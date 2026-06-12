//! End-to-end integration tests: in-process server (mock feed) on an
//! ephemeral port, exercised over real HTTP.
//!
//! The package has no lib target, so the server-core sources are compiled
//! directly into this test crate via `#[path]` includes.

#![allow(dead_code)]

#[path = "../src/config.rs"]
mod config;
#[path = "../src/dispatch.rs"]
mod dispatch;
#[path = "../src/handlers/mod.rs"]
mod handlers;
#[path = "../src/mapping.rs"]
mod mapping;
#[path = "../src/state.rs"]
mod state;
#[path = "../src/webhook/mod.rs"]
mod webhook;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use serde_json::{json, Value};

use config::{
    AuthToken, BufferSettings, EventModeling, FeedKind, FeedSettings, PollSettings, PushSettings,
    QueryConfig, ServerConfig, WebhookSettings,
};
use state::AppState;

const EVENT: &str = "high-value-orders.changed";

fn test_config() -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: 0,
        auth_tokens: vec![AuthToken {
            token: "devtoken".into(),
            principal: "dev@example.com".into(),
        }],
        event_modeling: EventModeling::Single,
        buffer: BufferSettings {
            max_events_per_type: 1000,
            max_age_ms: Some(600_000),
        },
        feed: FeedSettings {
            kind: FeedKind::Mock,
            query_id: Some("high-value-orders".into()),
            interval_ms: Some(25),
            url: None,
        },
        queries: vec![QueryConfig {
            id: "high-value-orders".into(),
            description: Some("test query".into()),
            payload_schema: None,
        }],
        push: PushSettings {
            heartbeat_interval_ms: 150,
        },
        poll: PollSettings { next_poll_ms: 50 },
        webhook: WebhookSettings {
            enabled: false,
            ..WebhookSettings::default()
        },
    }
}

async fn spawn_server(cfg: ServerConfig) -> (String, Arc<AppState>) {
    let state = AppState::new(cfg).expect("building state");
    mapping::spawn_feed_pipeline(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let app = dispatch::router(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serving");
    });
    (format!("http://{addr}/mcp"), state)
}

async fn rpc_response(
    client: &reqwest::Client,
    url: &str,
    id: i64,
    method: &str,
    params: Value,
) -> reqwest::Response {
    client
        .post(url)
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
        .send()
        .await
        .expect("request send")
}

async fn rpc(client: &reqwest::Client, url: &str, id: i64, method: &str, params: Value) -> Value {
    let resp = rpc_response(client, url, id, method, params).await;
    assert_eq!(resp.status(), 200, "method {method}");
    let body: Value = resp.json().await.expect("json body");
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], json!(id));
    body
}

/// Re-polls `name` from `cursor` until at least `min` events accumulate.
async fn wait_for_events(
    client: &reqwest::Client,
    url: &str,
    name: &str,
    cursor: &str,
    min: usize,
) -> Vec<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let body = rpc(
            client,
            url,
            999,
            "events/poll",
            json!({"name": name, "cursor": cursor}),
        )
        .await;
        let events = body["result"]["events"]
            .as_array()
            .expect("events array")
            .clone();
        if events.len() >= min {
            return events;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {min} events of {name} (have {})",
            events.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Reads SSE frames (each `data:` line parsed as JSON) until `done` is
/// satisfied or the deadline passes.
async fn collect_frames(
    resp: reqwest::Response,
    timeout: Duration,
    done: impl Fn(&[Value]) -> bool,
) -> Vec<Value> {
    let mut frames: Vec<Value> = Vec::new();
    let mut buf = String::new();
    let mut stream = resp.bytes_stream();
    let deadline = tokio::time::Instant::now() + timeout;
    while !done(&frames) {
        let next = tokio::time::timeout_at(deadline, stream.next())
            .await
            .unwrap_or_else(|_| panic!("timed out collecting SSE frames; got: {frames:#?}"));
        let chunk = next
            .expect("SSE stream ended early")
            .expect("SSE stream error");
        buf.push_str(std::str::from_utf8(&chunk).expect("utf8 chunk"));
        while let Some(idx) = buf.find("\n\n") {
            let frame: String = buf.drain(..idx + 2).collect();
            for line in frame.lines() {
                if let Some(data) = line.strip_prefix("data:") {
                    frames.push(serde_json::from_str(data.trim_start()).expect("frame JSON"));
                }
            }
        }
    }
    frames
}

#[tokio::test]
async fn initialize_list_poll_flow() {
    let (url, _state) = spawn_server(test_config()).await;
    let client = reqwest::Client::new();

    // --- initialize: version, capabilities, session header ---
    let resp = rpc_response(
        &client,
        &url,
        1,
        "initialize",
        json!({
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": { "name": "integration-test", "version": "0.0.0" }
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("mcp-session-id").is_some(),
        "initialize must issue Mcp-Session-Id"
    );
    let body: Value = resp.json().await.expect("init body");
    assert_eq!(body["result"]["protocolVersion"], "2025-11-25");
    assert_eq!(
        body["result"]["capabilities"]["events"]["listChanged"],
        json!(false)
    );
    assert!(body["result"]["serverInfo"]["name"].is_string());

    // --- notifications/initialized: 202, empty body ---
    let resp = client
        .post(&url)
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        .send()
        .await
        .expect("notification send");
    assert_eq!(resp.status(), 202);
    assert!(resp.bytes().await.expect("body").is_empty());

    // --- ping ---
    let body = rpc(&client, &url, 2, "ping", json!({})).await;
    assert_eq!(body["result"], json!({}));

    // --- events/list ---
    let body = rpc(&client, &url, 3, "events/list", json!({})).await;
    let events = body["result"]["events"].as_array().expect("definitions");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["name"], EVENT);
    let delivery = events[0]["delivery"].as_array().expect("delivery");
    assert!(delivery.contains(&json!("poll")));
    assert!(delivery.contains(&json!("push")));
    assert!(!delivery.contains(&json!("webhook")), "webhook disabled");
    assert_eq!(
        events[0]["inputSchema"]["properties"]["changeType"]["enum"],
        json!(["added", "updated", "deleted"])
    );
    assert!(events[0]["payloadSchema"].is_object());
    assert!(
        body["result"].get("nextCursor").is_none(),
        "no pagination => nextCursor absent"
    );

    // --- null-cursor poll: fresh cursor, no events ---
    let body = rpc(
        &client,
        &url,
        4,
        "events/poll",
        json!({"name": EVENT, "cursor": null}),
    )
    .await;
    let result = &body["result"];
    assert_eq!(result["events"], json!([]));
    assert_eq!(result["truncated"], json!(false));
    assert_eq!(result["hasMore"], json!(false));
    assert_eq!(result["nextPollMs"], json!(50));
    let c0 = result["cursor"].as_str().expect("fresh cursor").to_string();

    // --- events flow ---
    let events = wait_for_events(&client, &url, EVENT, &c0, 6).await;
    for e in &events {
        assert_eq!(e["name"], EVENT);
        assert!(e["eventId"].as_str().expect("eventId").starts_with("order-"));
        let change = e["data"]["changeType"].as_str().expect("changeType");
        assert!(["added", "updated", "deleted"].contains(&change));
        assert!(
            chrono::DateTime::parse_from_rfc3339(e["timestamp"].as_str().expect("ts")).is_ok()
        );
        assert!(
            e.get("cursor").is_none(),
            "poll occurrences carry no per-event cursor"
        );
    }
    // The mock scenario starts with inserts.
    assert_eq!(events[0]["data"]["changeType"], "added");
    assert!(events[0]["data"].get("before").is_none());
    assert!(events[0]["data"]["after"].is_object());

    // --- maxEvents / hasMore pagination ---
    let body = rpc(
        &client,
        &url,
        5,
        "events/poll",
        json!({"name": EVENT, "cursor": c0, "maxEvents": 1}),
    )
    .await;
    let result = &body["result"];
    assert_eq!(result["events"].as_array().expect("page").len(), 1);
    assert_eq!(result["hasMore"], json!(true));
    assert_eq!(result["events"][0]["eventId"], events[0]["eventId"]);
    let c1 = result["cursor"].as_str().expect("page cursor").to_string();
    assert_ne!(c1, c0, "cursor advances past the partial batch");

    let body = rpc(
        &client,
        &url,
        6,
        "events/poll",
        json!({"name": EVENT, "cursor": c1, "maxEvents": 1}),
    )
    .await;
    assert_eq!(body["result"]["events"][0]["eventId"], events[1]["eventId"]);

    // --- changeType filter param, end to end ---
    assert!(
        events.iter().any(|e| e["data"]["changeType"] != "added"),
        "scenario should contain non-added events by now"
    );
    let body = rpc(
        &client,
        &url,
        7,
        "events/poll",
        json!({"name": EVENT, "cursor": c0, "params": {"changeType": "added"}}),
    )
    .await;
    let filtered = body["result"]["events"].as_array().expect("filtered");
    assert!(!filtered.is_empty());
    assert!(filtered.iter().all(|e| e["data"]["changeType"] == "added"));

    // Invalid filter value -> InvalidParams.
    let body = rpc(
        &client,
        &url,
        8,
        "events/poll",
        json!({"name": EVENT, "cursor": null, "params": {"changeType": "bogus"}}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32602));

    // --- bogus cursor -> truncated ---
    let body = rpc(
        &client,
        &url,
        9,
        "events/poll",
        json!({"name": EVENT, "cursor": "definitely-not-a-cursor"}),
    )
    .await;
    assert_eq!(body["result"]["truncated"], json!(true));
    assert!(body["result"]["cursor"].is_string());

    // --- unknown event name -> -32011 {kind: "event"} ---
    let body = rpc(
        &client,
        &url,
        10,
        "events/poll",
        json!({"name": "no.such.event", "cursor": null}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32011));
    assert_eq!(body["error"]["data"]["kind"], "event");

    // --- unknown method -> -32601 ---
    let body = rpc(&client, &url, 11, "events/nope", json!({})).await;
    assert_eq!(body["error"]["code"], json!(-32601));

    // --- missing required param -> -32602 ---
    let body = rpc(&client, &url, 12, "events/poll", json!({})).await;
    assert_eq!(body["error"]["code"], json!(-32602));
}

#[tokio::test]
async fn stream_replay_live_heartbeats_and_meta_routing() {
    let (url, _state) = spawn_server(test_config()).await;
    let client = reqwest::Client::new();

    // Bootstrap a cursor, then let some events accumulate to be replayed.
    let body = rpc(
        &client,
        &url,
        1,
        "events/poll",
        json!({"name": EVENT, "cursor": null}),
    )
    .await;
    let c0 = body["result"]["cursor"].as_str().expect("c0").to_string();
    let polled = wait_for_events(&client, &url, EVENT, &c0, 2).await;

    let resp = rpc_response(
        &client,
        &url,
        42,
        "events/stream",
        json!({"name": EVENT, "cursor": c0}),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .expect("content-type")
        .to_str()
        .expect("ct str")
        .to_string();
    assert!(
        content_type.starts_with("text/event-stream"),
        "{content_type}"
    );

    let frames = collect_frames(resp, Duration::from_secs(15), |frames| {
        let events = frames
            .iter()
            .filter(|f| f["method"] == "notifications/events/event")
            .count();
        let heartbeats = frames
            .iter()
            .filter(|f| f["method"] == "notifications/events/heartbeat")
            .count();
        events >= 3 && heartbeats >= 1
    })
    .await;

    // First frame: active confirmation with cursor + truncated.
    assert_eq!(frames[0]["method"], "notifications/events/active");
    assert_eq!(frames[0]["params"]["truncated"], json!(false));
    assert!(frames[0]["params"]["cursor"].is_string());

    // _meta routing: every notification frame carries the request id.
    for f in &frames {
        assert!(f["method"].is_string(), "unexpected non-notification: {f}");
        assert_eq!(
            f["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
            json!(42),
            "frame missing subscription id: {f}"
        );
    }

    let events: Vec<&Value> = frames
        .iter()
        .filter(|f| f["method"] == "notifications/events/event")
        .collect();
    assert!(events.len() >= 3);
    // Replay matches what poll saw from the same cursor.
    assert_eq!(events[0]["params"]["eventId"], polled[0]["eventId"]);
    assert_eq!(events[1]["params"]["eventId"], polled[1]["eventId"]);
    let mut cursors = Vec::new();
    for e in &events {
        assert_eq!(e["params"]["name"], EVENT);
        let cursor = e["params"]["cursor"]
            .as_str()
            .expect("push events carry a per-event cursor")
            .to_string();
        assert!(e["params"]["data"]["changeType"].is_string());
        cursors.push(cursor);
    }
    cursors.dedup();
    assert_eq!(
        cursors.len(),
        events.len(),
        "per-event cursors must advance"
    );

    let heartbeat = frames
        .iter()
        .find(|f| f["method"] == "notifications/events/heartbeat")
        .expect("heartbeat frame");
    assert!(heartbeat["params"]["cursor"].is_string());

    // Filtered stream: only "added" changes are delivered.
    let resp = rpc_response(
        &client,
        &url,
        43,
        "events/stream",
        json!({"name": EVENT, "cursor": c0, "params": {"changeType": "added"}}),
    )
    .await;
    let frames = collect_frames(resp, Duration::from_secs(15), |frames| {
        frames
            .iter()
            .filter(|f| f["method"] == "notifications/events/event")
            .count()
            >= 2
    })
    .await;
    for f in frames
        .iter()
        .filter(|f| f["method"] == "notifications/events/event")
    {
        assert_eq!(f["params"]["data"]["changeType"], "added");
        assert_eq!(
            f["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
            json!(43)
        );
    }

    // Invalid subscription: immediate JSON-RPC error, no stream opened.
    let resp = rpc_response(
        &client,
        &url,
        44,
        "events/stream",
        json!({"name": "no.such.event"}),
    )
    .await;
    assert!(resp
        .headers()
        .get("content-type")
        .expect("ct")
        .to_str()
        .expect("ct str")
        .starts_with("application/json"));
    let body: Value = resp.json().await.expect("error body");
    assert_eq!(body["error"]["code"], json!(-32011));
    assert_eq!(body["error"]["data"]["kind"], "event");

    // Invalid params: immediate -32602.
    let resp = rpc_response(
        &client,
        &url,
        45,
        "events/stream",
        json!({"name": EVENT, "params": {"changeType": "bogus"}}),
    )
    .await;
    let body: Value = resp.json().await.expect("error body");
    assert_eq!(body["error"]["code"], json!(-32602));
}

#[tokio::test]
async fn per_change_modeling_three_event_types_end_to_end() {
    let mut cfg = test_config();
    cfg.event_modeling = EventModeling::PerChange;
    let (url, _state) = spawn_server(cfg).await;
    let client = reqwest::Client::new();

    let body = rpc(&client, &url, 1, "events/list", json!({})).await;
    let names: Vec<&str> = body["result"]["events"]
        .as_array()
        .expect("defs")
        .iter()
        .map(|e| e["name"].as_str().expect("name"))
        .collect();
    assert_eq!(
        names,
        vec![
            "high-value-orders.added",
            "high-value-orders.updated",
            "high-value-orders.deleted"
        ]
    );

    // .added events carry the row directly.
    let body = rpc(
        &client,
        &url,
        2,
        "events/poll",
        json!({"name": "high-value-orders.added", "cursor": null}),
    )
    .await;
    let c_added = body["result"]["cursor"].as_str().expect("cursor").to_string();
    let added = wait_for_events(&client, &url, "high-value-orders.added", &c_added, 1).await;
    assert!(added[0]["data"]["id"].is_number());
    assert!(added[0]["data"]["customer"].is_string());
    assert!(added[0]["data"].get("changeType").is_none());

    // .updated events carry {before, after}.
    let body = rpc(
        &client,
        &url,
        3,
        "events/poll",
        json!({"name": "high-value-orders.updated", "cursor": null}),
    )
    .await;
    let c_upd = body["result"]["cursor"].as_str().expect("cursor").to_string();
    let updated = wait_for_events(&client, &url, "high-value-orders.updated", &c_upd, 1).await;
    assert!(updated[0]["data"]["before"].is_object());
    assert!(updated[0]["data"]["after"].is_object());
    assert_ne!(updated[0]["data"]["before"], updated[0]["data"]["after"]);
}

#[test]
fn example_configs_parse() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");

    let mock = ServerConfig::load(&dir.join("mock.yaml")).expect("mock.yaml");
    assert_eq!(mock.feed.kind, FeedKind::Mock);
    assert_eq!(mock.feed.interval_ms, Some(1500));
    assert_eq!(mock.event_modeling, EventModeling::Single);
    assert!(mock.webhook.enabled);
    assert!(mock.webhook.allow_insecure_urls);
    assert_eq!(
        mock.principal_for_token("devtoken").as_deref(),
        Some("dev@example.com")
    );

    let drasi = ServerConfig::load(&dir.join("drasi.yaml")).expect("drasi.yaml");
    assert_eq!(drasi.feed.kind, FeedKind::DrasiSse);
    assert_eq!(drasi.feed.url.as_deref(), Some("http://localhost:8081/events"));
    assert_eq!(drasi.queries[0].id, "high-value-orders");
    assert!(drasi.webhook.enabled);
    assert!(!drasi.webhook.allow_insecure_urls);
}
