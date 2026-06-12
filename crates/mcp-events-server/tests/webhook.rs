//! Webhook end-to-end tests: in-process server (mock feed) plus an
//! in-process axum webhook receiver, both on loopback (`allowInsecureUrls`).
//!
//! The package has no lib target, so the server sources are compiled
//! directly into this test crate via `#[path]` includes (same pattern as
//! tests/integration.rs).

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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use chrono::Utc;
use mcp_events_engine::{canonical_json, SubKey};
use mcp_events_wire as wire;
use serde_json::{json, Value};

use config::{
    AuthToken, BufferSettings, EventModeling, FeedKind, FeedSettings, PollSettings, PushSettings,
    QueryConfig, ServerConfig, WebhookSettings,
};
use state::AppState;

const EVENT: &str = "high-value-orders.changed";
const PRINCIPAL: &str = "dev@example.com";
const TOKEN: &str = "devtoken";

fn webhook_settings() -> WebhookSettings {
    WebhookSettings {
        enabled: true,
        ttl_cap_ms: 600_000,
        min_ttl_ms: 1_000,
        max_subscriptions_per_principal: 16,
        allow_insecure_urls: true,
        suspend_after_failures: 5,
    }
}

fn test_config(webhook: WebhookSettings) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: 0,
        auth_tokens: vec![AuthToken {
            token: TOKEN.into(),
            principal: PRINCIPAL.into(),
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
        webhook,
    }
}

async fn spawn_server(cfg: ServerConfig) -> (String, Arc<AppState>) {
    let state = AppState::new(cfg).expect("building state");
    mapping::spawn_feed_pipeline(state.clone());
    if state.config.webhook.enabled {
        webhook::worker::spawn_delivery_worker(state.clone());
    }
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

// ---------------------------------------------------------------- receiver

#[derive(Clone, Debug)]
struct Delivery {
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Delivery {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map(String::as_str).unwrap_or("")
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }

    fn is_verification(&self) -> bool {
        self.json().get("type").and_then(Value::as_str) == Some("verification")
    }

    fn is_event(&self) -> bool {
        self.json().get("eventId").is_some()
    }

    fn verifies_with(&self, secret: &[u8]) -> bool {
        let ts: i64 = self.header("webhook-timestamp").parse().unwrap_or(0);
        wire::verify_standard_webhooks(
            secret,
            self.header("webhook-id"),
            ts,
            &self.body,
            self.header("webhook-signature"),
        )
    }
}

#[derive(Clone, Default)]
struct Receiver {
    deliveries: Arc<Mutex<Vec<Delivery>>>,
    fail_events: Arc<AtomicBool>,
    wrong_challenge: Arc<AtomicBool>,
}

impl Receiver {
    fn all(&self) -> Vec<Delivery> {
        self.deliveries.lock().unwrap().clone()
    }

    fn verifications(&self) -> Vec<Delivery> {
        self.all().into_iter().filter(Delivery::is_verification).collect()
    }

    fn events_for(&self, sub_id: &str) -> Vec<Delivery> {
        self.all()
            .into_iter()
            .filter(|d| d.is_event() && d.header("x-mcp-subscription-id") == sub_id)
            .collect()
    }

    fn event_count(&self) -> usize {
        self.all().iter().filter(|d| d.is_event()).count()
    }
}

async fn hook(
    State(rx): State<Receiver>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut hmap = HashMap::new();
    for (k, v) in headers.iter() {
        if let Ok(s) = v.to_str() {
            hmap.insert(k.as_str().to_owned(), s.to_owned());
        }
    }
    rx.deliveries.lock().unwrap().push(Delivery {
        path: uri.path().to_owned(),
        headers: hmap,
        body: body.to_vec(),
    });
    let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    if v.get("type").and_then(Value::as_str) == Some("verification") {
        let challenge = if rx.wrong_challenge.load(Ordering::SeqCst) {
            json!("not-the-nonce")
        } else {
            v["challenge"].clone()
        };
        return (StatusCode::OK, Json(json!({ "challenge": challenge }))).into_response();
    }
    if rx.fail_events.load(Ordering::SeqCst) {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    (StatusCode::OK, Json(json!({}))).into_response()
}

async fn spawn_receiver() -> (String, Receiver) {
    let rx = Receiver::default();
    let app = Router::new()
        .route("/hooks/{name}", post(hook))
        .with_state(rx.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding receiver");
    let addr = listener.local_addr().expect("receiver addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serving receiver");
    });
    (format!("http://{addr}"), rx)
}

// ----------------------------------------------------------------- helpers

fn make_secret(seed: u8) -> String {
    format!("whsec_{}", B64.encode([seed; 32]))
}

async fn rpc(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let mut req = client
        .post(url)
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let resp = req.send().await.expect("request send");
    assert_eq!(resp.status(), 200, "method {method}");
    let body: Value = resp.json().await.expect("json body");
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], json!(id));
    body
}

async fn wait_for<T>(timeout_ms: u64, what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn subscribe_body(url: &str, secret: &str) -> Value {
    json!({
        "name": EVENT,
        "delivery": { "mode": "webhook", "url": url, "secret": secret },
        "cursor": null,
        "ttlMs": 60_000
    })
}

fn sub_key(url: &str) -> SubKey {
    SubKey {
        principal: PRINCIPAL.into(),
        url: url.into(),
        name: EVENT.into(),
        params_canonical: canonical_json(&Value::Null),
    }
}

fn ms_until(iso: &str) -> i64 {
    let t = chrono::DateTime::parse_from_rfc3339(iso).expect("rfc3339 refreshBefore");
    (t.with_timezone(&Utc) - Utc::now()).num_milliseconds()
}

fn seq_of(cursor: &str) -> u64 {
    cursor
        .split(':')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("internal cursor format: {cursor}"))
}

// ------------------------------------------------------------------- tests

#[tokio::test]
async fn webhook_methods_require_principal() {
    let (url, _state) = spawn_server(test_config(webhook_settings())).await;
    let client = reqwest::Client::new();
    let (hook_base, _rx) = spawn_receiver().await;
    let hook = format!("{hook_base}/hooks/auth");

    // Missing Authorization.
    let body = rpc(
        &client,
        &url,
        None,
        1,
        "events/subscribe",
        subscribe_body(&hook, &make_secret(1)),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32012));

    // Unrecognized bearer token = anonymous.
    let body = rpc(
        &client,
        &url,
        Some("wrong-token"),
        2,
        "events/subscribe",
        subscribe_body(&hook, &make_secret(1)),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32012));

    let body = rpc(
        &client,
        &url,
        None,
        3,
        "events/unsubscribe",
        json!({"name": EVENT, "delivery": {"url": hook}}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32012));
}

#[tokio::test]
async fn static_validation_errors() {
    let (url, _state) = spawn_server(test_config(webhook_settings())).await;
    let client = reqwest::Client::new();
    let auth = Some(TOKEN);
    // None of these reach the network: all fail before the challenge.
    let hook = "https://unreachable.invalid/hooks/x";

    // Missing secret.
    let body = rpc(
        &client,
        &url,
        auth,
        1,
        "events/subscribe",
        json!({"name": EVENT, "delivery": {"mode": "webhook", "url": hook}, "cursor": null}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32602));

    // Malformed secrets.
    for (i, secret) in ["nope", "whsec_!!!notbase64", &format!("whsec_{}", B64.encode([7u8; 8]))]
        .iter()
        .enumerate()
    {
        let body = rpc(
            &client,
            &url,
            auth,
            10 + i as i64,
            "events/subscribe",
            subscribe_body(hook, secret),
        )
        .await;
        assert_eq!(body["error"]["code"], json!(-32602), "secret {secret:?}");
    }

    // Malformed URL.
    let body = rpc(
        &client,
        &url,
        auth,
        20,
        "events/subscribe",
        subscribe_body("not a url", &make_secret(1)),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32602));

    // delivery.mode other than webhook.
    let body = rpc(
        &client,
        &url,
        auth,
        21,
        "events/subscribe",
        json!({"name": EVENT, "delivery": {"mode": "push", "url": hook, "secret": make_secret(1)}}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32014));
    assert_eq!(body["error"]["data"]["feature"], "deliveryMode");
    assert_eq!(body["error"]["data"]["value"], "push");

    // delivery.mode absent.
    let body = rpc(
        &client,
        &url,
        auth,
        22,
        "events/subscribe",
        json!({"name": EVENT, "delivery": {"url": hook, "secret": make_secret(1)}}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32602));

    // Unknown event name.
    let body = rpc(
        &client,
        &url,
        auth,
        23,
        "events/subscribe",
        json!({"name": "no.such.event", "delivery": {"mode": "webhook", "url": hook, "secret": make_secret(1)}}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32011));
    assert_eq!(body["error"]["data"]["kind"], "event");

    // Invalid params for the event's inputSchema.
    let body = rpc(
        &client,
        &url,
        auth,
        24,
        "events/subscribe",
        json!({"name": EVENT, "params": {"changeType": "bogus"},
               "delivery": {"mode": "webhook", "url": hook, "secret": make_secret(1)}}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32602));

    // Unsubscribe for a key that never existed.
    let body = rpc(
        &client,
        &url,
        auth,
        25,
        "events/unsubscribe",
        json!({"name": EVENT, "delivery": {"url": hook}}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32011));
    assert_eq!(body["error"]["data"]["kind"], "subscription");

    // http:// rejected when allowInsecureUrls is off.
    let mut secure = webhook_settings();
    secure.allow_insecure_urls = false;
    let (secure_url, _s) = spawn_server(test_config(secure)).await;
    let body = rpc(
        &client,
        &secure_url,
        auth,
        30,
        "events/subscribe",
        subscribe_body("http://example.com/hooks/x", &make_secret(1)),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32602));

    // Webhook delivery not offered when disabled.
    let mut disabled = webhook_settings();
    disabled.enabled = false;
    let (disabled_url, _s) = spawn_server(test_config(disabled)).await;
    let body = rpc(
        &client,
        &disabled_url,
        auth,
        31,
        "events/subscribe",
        subscribe_body("https://example.com/hooks/x", &make_secret(1)),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32014));
}

#[tokio::test]
async fn subscribe_challenge_delivery_rotation_unsubscribe() {
    let (url, _state) = spawn_server(test_config(webhook_settings())).await;
    let client = reqwest::Client::new();
    let auth = Some(TOKEN);
    let (hook_base, rx) = spawn_receiver().await;
    let hook = format!("{hook_base}/hooks/main");

    let secret1 = make_secret(1);
    let secret1_bytes = wire::parse_whsec(&secret1).expect("secret1");

    // --- subscribe: verification challenge round-trip ---
    let body = rpc(&client, &url, auth, 1, "events/subscribe", subscribe_body(&hook, &secret1)).await;
    let result = &body["result"];
    let sub_id = result["id"].as_str().expect("subscription id").to_string();
    assert!(sub_id.starts_with("sub_"), "{sub_id}");
    assert!(result["refreshBefore"].is_string(), "finite grant");
    assert!(result["cursor"].is_string(), "fresh watermark cursor");
    assert_eq!(result["truncated"], json!(false));
    assert!(
        result.get("deliveryStatus").is_none(),
        "no deliveryStatus on create"
    );

    let verifications = rx.verifications();
    assert_eq!(verifications.len(), 1, "exactly one challenge");
    let v = &verifications[0];
    assert!(v.header("webhook-id").starts_with("msg_verification_"));
    assert_eq!(v.header("x-mcp-subscription-id"), sub_id);
    assert!(v.header("content-type").starts_with("application/json"));
    assert!(
        v.verifies_with(&secret1_bytes),
        "verification POST must be signed with the subscription secret"
    );
    assert!(v.json()["challenge"].is_string());

    // --- events delivered, signed, watermark cursors advance ---
    let deliveries = wait_for(15_000, "3 event deliveries", || {
        let evs = rx.events_for(&sub_id);
        (evs.len() >= 3).then_some(evs)
    })
    .await;
    let mut last_seq = 0u64;
    for d in &deliveries {
        assert!(d.verifies_with(&secret1_bytes), "signature must verify");
        let body = d.json();
        assert_eq!(body["name"], EVENT);
        assert_eq!(
            d.header("webhook-id"),
            body["eventId"].as_str().expect("eventId"),
            "webhook-id carries the eventId"
        );
        assert_eq!(d.header("x-mcp-subscription-id"), sub_id);
        let ts: i64 = d.header("webhook-timestamp").parse().expect("unix ts");
        assert!((Utc::now().timestamp() - ts).abs() < 300);
        let cursor = body["cursor"].as_str().expect("watermark cursor");
        let seq = seq_of(cursor);
        assert!(seq > last_seq, "FIFO watermark must advance");
        last_seq = seq;
        assert!(body["data"]["changeType"].is_string());
    }

    // --- second subscription, same URL, filtered params: no new challenge ---
    let secret2 = make_secret(2);
    let secret2_bytes = wire::parse_whsec(&secret2).expect("secret2");
    let body = rpc(
        &client,
        &url,
        auth,
        2,
        "events/subscribe",
        json!({
            "name": EVENT,
            "params": {"changeType": "added"},
            "delivery": {"mode": "webhook", "url": hook, "secret": secret2},
            "cursor": null
        }),
    )
    .await;
    let sub2 = body["result"]["id"].as_str().expect("sub2 id").to_string();
    assert_ne!(sub2, sub_id, "different params = different subscription");
    assert_eq!(
        rx.verifications().len(),
        1,
        "verification cached per (principal, url)"
    );
    let filtered = wait_for(15_000, "2 filtered deliveries", || {
        let evs = rx.events_for(&sub2);
        (evs.len() >= 2).then_some(evs)
    })
    .await;
    for d in &filtered {
        assert!(d.verifies_with(&secret2_bytes));
        assert_eq!(d.json()["data"]["changeType"], "added");
    }

    // --- refresh: same id, deliveryStatus, secret rotation ---
    let secret3 = make_secret(3);
    let secret3_bytes = wire::parse_whsec(&secret3).expect("secret3");
    let body = rpc(&client, &url, auth, 3, "events/subscribe", subscribe_body(&hook, &secret3)).await;
    let result = &body["result"];
    assert_eq!(result["id"].as_str(), Some(sub_id.as_str()), "idempotent key");
    assert!(result["refreshBefore"].is_string(), "TTL re-granted");
    assert!(result["cursor"].is_string(), "watermark in refresh response");
    let status = &result["deliveryStatus"];
    assert_eq!(status["active"], json!(true));
    assert!(status["lastDeliveryAt"].is_string(), "deliveries succeeded");
    assert!(status["lastError"].is_null());

    wait_for(15_000, "delivery signed with rotated secret", || {
        rx.events_for(&sub_id)
            .iter()
            .any(|d| d.verifies_with(&secret3_bytes))
            .then_some(())
    })
    .await;

    // --- unsubscribe stops deliveries ---
    let body = rpc(
        &client,
        &url,
        auth,
        4,
        "events/unsubscribe",
        json!({"name": EVENT, "delivery": {"url": hook}}),
    )
    .await;
    assert_eq!(body["result"], json!({}), "empty ack");
    let body = rpc(
        &client,
        &url,
        auth,
        5,
        "events/unsubscribe",
        json!({"name": EVENT, "params": {"changeType": "added"}, "delivery": {"url": hook}}),
    )
    .await;
    assert_eq!(body["result"], json!({}));

    // Unknown after removal.
    let body = rpc(
        &client,
        &url,
        auth,
        6,
        "events/unsubscribe",
        json!({"name": EVENT, "delivery": {"url": hook}}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32011));
    assert_eq!(body["error"]["data"]["kind"], "subscription");

    tokio::time::sleep(Duration::from_millis(600)).await; // drain in-flight
    let settled = rx.event_count();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        rx.event_count(),
        settled,
        "no deliveries after unsubscribe (feed still emitting)"
    );
}

#[tokio::test]
async fn quota_enforced_before_challenge() {
    let mut settings = webhook_settings();
    settings.max_subscriptions_per_principal = 1;
    let (url, _state) = spawn_server(test_config(settings)).await;
    let client = reqwest::Client::new();
    let (hook_base, rx) = spawn_receiver().await;

    let body = rpc(
        &client,
        &url,
        Some(TOKEN),
        1,
        "events/subscribe",
        subscribe_body(&format!("{hook_base}/hooks/a"), &make_secret(1)),
    )
    .await;
    assert!(body["result"]["id"].is_string());

    let body = rpc(
        &client,
        &url,
        Some(TOKEN),
        2,
        "events/subscribe",
        subscribe_body(&format!("{hook_base}/hooks/b"), &make_secret(2)),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32013));
    assert_eq!(body["error"]["data"]["limit"], "subscriptions");
    assert_eq!(body["error"]["data"]["max"], json!(1));
    assert_eq!(
        rx.verifications().len(),
        1,
        "the over-quota subscribe must not POST a challenge"
    );
}

#[tokio::test]
async fn ttl_negotiation_clamps_and_refuses_no_expiry() {
    let mut settings = webhook_settings();
    settings.ttl_cap_ms = 120_000;
    settings.min_ttl_ms = 30_000;
    let (url, _state) = spawn_server(test_config(settings)).await;
    let client = reqwest::Client::new();
    let (hook_base, _rx) = spawn_receiver().await;
    let hook = format!("{hook_base}/hooks/ttl");
    let secret = make_secret(9);

    let grant = |ttl: Value| {
        let mut params = subscribe_body(&hook, &secret);
        match ttl {
            Value::Null => params["ttlMs"] = Value::Null,
            Value::Number(n) => params["ttlMs"] = Value::Number(n),
            _ => {
                params.as_object_mut().expect("object").remove("ttlMs");
            }
        }
        params
    };

    // Absent => server default (the cap).
    let body = rpc(&client, &url, Some(TOKEN), 1, "events/subscribe", grant(json!("absent"))).await;
    let rb = body["result"]["refreshBefore"].as_str().expect("grant");
    assert!((105_000..=125_000).contains(&ms_until(rb)), "{rb}");

    // null => no-expiry requested, server grants the finite cap instead.
    let body = rpc(&client, &url, Some(TOKEN), 2, "events/subscribe", grant(Value::Null)).await;
    let rb = body["result"]["refreshBefore"].as_str().expect("finite grant");
    assert!((105_000..=125_000).contains(&ms_until(rb)), "{rb}");

    // Tiny suggestion clamped up to the server minimum.
    let body = rpc(&client, &url, Some(TOKEN), 3, "events/subscribe", grant(json!(1))).await;
    let rb = body["result"]["refreshBefore"].as_str().expect("grant");
    assert!((15_000..=35_000).contains(&ms_until(rb)), "{rb}");

    // Huge suggestion clamped down to the cap.
    let body = rpc(&client, &url, Some(TOKEN), 4, "events/subscribe", grant(json!(999_999_999u64))).await;
    let rb = body["result"]["refreshBefore"].as_str().expect("grant");
    assert!((105_000..=125_000).contains(&ms_until(rb)), "{rb}");
}

#[tokio::test]
async fn challenge_failures_yield_callback_endpoint_error() {
    let (url, _state) = spawn_server(test_config(webhook_settings())).await;
    let client = reqwest::Client::new();

    // Reachable endpoint that echoes the wrong nonce.
    let (hook_base, rx) = spawn_receiver().await;
    rx.wrong_challenge.store(true, Ordering::SeqCst);
    let hook = format!("{hook_base}/hooks/bad");
    let body = rpc(&client, &url, Some(TOKEN), 1, "events/subscribe", subscribe_body(&hook, &make_secret(1))).await;
    assert_eq!(body["error"]["code"], json!(-32015));
    assert_eq!(body["error"]["data"]["reason"], "challenge_failed");

    // The failed subscription must not linger.
    let body = rpc(
        &client,
        &url,
        Some(TOKEN),
        2,
        "events/unsubscribe",
        json!({"name": EVENT, "delivery": {"url": hook}}),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32011));

    // Unreachable endpoint (bound then dropped => connection refused).
    let dead_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
        l.local_addr().expect("probe addr").port()
    };
    let body = rpc(
        &client,
        &url,
        Some(TOKEN),
        3,
        "events/subscribe",
        subscribe_body(&format!("http://127.0.0.1:{dead_port}/hooks/x"), &make_secret(1)),
    )
    .await;
    assert_eq!(body["error"]["code"], json!(-32015));
    assert_eq!(body["error"]["data"]["reason"], "connection_refused");
}

#[tokio::test]
async fn failing_endpoint_suspends_and_refresh_reports_and_reactivates() {
    let mut settings = webhook_settings();
    settings.suspend_after_failures = 2;
    let (url, state) = spawn_server(test_config(settings)).await;
    let client = reqwest::Client::new();
    let (hook_base, rx) = spawn_receiver().await;
    rx.fail_events.store(true, Ordering::SeqCst); // verification still succeeds
    let hook = format!("{hook_base}/hooks/failing");

    let body = rpc(&client, &url, Some(TOKEN), 1, "events/subscribe", subscribe_body(&hook, &make_secret(4))).await;
    assert!(body["result"]["id"].is_string(), "challenge succeeds: {body}");

    // Two consecutive failed attempts (initial + 1s retry) suspend delivery.
    let key = sub_key(&hook);
    wait_for(15_000, "suspension after consecutive failures", || {
        state
            .subs
            .get(&key)
            .filter(|s| !s.active)
            .map(|_| ())
    })
    .await;

    // Refresh surfaces the pre-refresh status and reactivates.
    let body = rpc(&client, &url, Some(TOKEN), 2, "events/subscribe", subscribe_body(&hook, &make_secret(4))).await;
    let status = &body["result"]["deliveryStatus"];
    assert_eq!(status["active"], json!(false), "{body}");
    assert_eq!(status["lastError"], "http_5xx");
    assert!(status["failedSince"].is_string());
    assert!(status["lastDeliveryAt"].is_null(), "no delivery ever succeeded");

    let reactivated = state.subs.get(&key).expect("still subscribed");
    assert!(reactivated.active, "refresh reactivates delivery");
}
