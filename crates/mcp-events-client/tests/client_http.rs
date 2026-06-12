//! End-to-end tests of `EventsClient` against an in-process mock MCP server
//! (axum on an ephemeral port). Covers the initialize handshake (incl. the
//! 202 for notifications/initialized and session-id propagation), unary
//! calls, typed RPC errors, and the SSE stream path with frames delivered in
//! deliberately awkward chunk sizes.

use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use futures::StreamExt;
use mcp_events_client::{EventsClient, RpcError, StreamFrame};
use mcp_events_wire as wire;
use serde_json::{json, Value};

const SESSION_ID: &str = "sess-test-1";

fn json_response(id: Option<wire::RequestId>, result: Value) -> Response {
    let resp = wire::JsonRpcResponse {
        jsonrpc: "2.0".to_owned(),
        id,
        result: Some(result),
        error: None,
    };
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_vec(&resp).unwrap(),
    )
        .into_response()
}

fn error_response(id: Option<wire::RequestId>, error: wire::JsonRpcError) -> Response {
    let resp = wire::JsonRpcResponse::failure(id, error);
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_vec(&resp).unwrap(),
    )
        .into_response()
}

async fn mcp_handler(headers: HeaderMap, body: Bytes) -> Response {
    let req: wire::JsonRpcRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => return (StatusCode::BAD_REQUEST, "bad json").into_response(),
    };
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !accept.contains("application/json") || !accept.contains("text/event-stream") {
        return (StatusCode::BAD_REQUEST, "missing accept header").into_response();
    }
    let has_session = headers
        .get("mcp-session-id")
        .map(|v| v.as_bytes() == SESSION_ID.as_bytes())
        .unwrap_or(false);
    match req.method.as_str() {
        "initialize" => {
            let result = json!({
                "protocolVersion": "2025-11-25",
                "capabilities": { "events": { "listChanged": false } },
                "serverInfo": { "name": "mock-server", "version": "0.0.0" }
            });
            let resp = wire::JsonRpcResponse {
                jsonrpc: "2.0".to_owned(),
                id: req.id,
                result: Some(result),
                error: None,
            };
            (
                StatusCode::OK,
                [
                    ("content-type", "application/json"),
                    ("mcp-session-id", SESSION_ID),
                ],
                serde_json::to_vec(&resp).unwrap(),
            )
                .into_response()
        }
        "notifications/initialized" => {
            if !has_session {
                return (StatusCode::BAD_REQUEST, "missing session id").into_response();
            }
            StatusCode::ACCEPTED.into_response()
        }
        "events/list" => {
            if !has_session {
                return (StatusCode::BAD_REQUEST, "missing session id").into_response();
            }
            json_response(
                req.id,
                json!({
                    "events": [{
                        "name": "orders.changed",
                        "description": "order rows changing",
                        "delivery": ["poll", "push"]
                    }]
                }),
            )
        }
        "events/poll" => {
            if !has_session {
                return (StatusCode::BAD_REQUEST, "missing session id").into_response();
            }
            let params: wire::PollEventsParams =
                serde_json::from_value(req.params.unwrap_or(Value::Null)).unwrap();
            if params.cursor.is_none() {
                // start-from-now bootstrap
                json_response(
                    req.id,
                    json!({
                        "events": [],
                        "cursor": "c1",
                        "truncated": false,
                        "hasMore": false,
                        "nextPollMs": 1000
                    }),
                )
            } else {
                json_response(
                    req.id,
                    json!({
                        "events": [{
                            "eventId": "evt-1",
                            "name": "orders.changed",
                            "timestamp": "2026-06-11T00:00:00Z",
                            "data": { "changeType": "added", "after": { "id": 1 } }
                        }],
                        "cursor": "c2",
                        "truncated": false,
                        "hasMore": false,
                        "nextPollMs": 2000
                    }),
                )
            }
        }
        "events/subscribe" => error_response(req.id, wire::JsonRpcError::forbidden("Forbidden")),
        "events/unsubscribe" => json_response(req.id, json!({})),
        "events/stream" => {
            let id_json = serde_json::to_value(req.id.clone().unwrap()).unwrap();
            let meta = json!({ "io.modelcontextprotocol/subscriptionId": id_json });
            let frames = [
                json!({"jsonrpc":"2.0","method":"notifications/events/active",
                       "params":{"cursor":"s1","truncated":false,"_meta":meta}}),
                json!({"jsonrpc":"2.0","method":"notifications/events/event",
                       "params":{"eventId":"evt-9","name":"orders.changed",
                                 "timestamp":"2026-06-11T00:00:01Z",
                                 "data":{"changeType":"updated"},
                                 "cursor":"s2","_meta":meta}}),
                json!({"jsonrpc":"2.0","method":"notifications/events/heartbeat",
                       "params":{"cursor":"s3","_meta":meta}}),
                json!({"jsonrpc":"2.0","id":id_json,"result":{"_meta":{}}}),
            ];
            let mut sse = String::new();
            for f in &frames {
                sse.push_str("data: ");
                sse.push_str(&f.to_string());
                sse.push_str("\n\n");
            }
            // Deliver in 7-byte chunks so frames are split mid-line/mid-CRLF.
            let chunks: Vec<Result<Bytes, std::io::Error>> = sse
                .into_bytes()
                .chunks(7)
                .map(|c| Ok(Bytes::copy_from_slice(c)))
                .collect();
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(futures::stream::iter(chunks)))
                .unwrap()
        }
        other => error_response(req.id, wire::JsonRpcError::method_not_found(other)),
    }
}

async fn spawn_mock() -> String {
    let app = Router::new().route("/mcp", post(mcp_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}/mcp")
}

#[tokio::test]
async fn initialize_list_and_poll() {
    let url = spawn_mock().await;
    let mut client = EventsClient::new(url);
    let init = client.initialize().await.unwrap();
    assert_eq!(init.protocol_version, "2025-11-25");
    assert!(init.capabilities.events.is_some());

    // session id captured during initialize must reach subsequent calls
    let list = client.list_events().await.unwrap();
    assert_eq!(list.events.len(), 1);
    assert_eq!(list.events[0].name, "orders.changed");
    assert!(list.next_cursor.is_none());

    let bootstrap = client
        .poll(&wire::PollEventsParams {
            name: "orders.changed".into(),
            params: None,
            cursor: None,
            max_age_ms: None,
            max_events: None,
        })
        .await
        .unwrap();
    assert!(bootstrap.events.is_empty());
    assert_eq!(bootstrap.cursor.as_deref(), Some("c1"));

    let next = client
        .poll(&wire::PollEventsParams {
            name: "orders.changed".into(),
            params: None,
            cursor: bootstrap.cursor,
            max_age_ms: None,
            max_events: None,
        })
        .await
        .unwrap();
    assert_eq!(next.events.len(), 1);
    assert_eq!(next.events[0].event_id, "evt-1");
    // poll occurrences carry no per-event cursor
    assert_eq!(next.events[0].cursor, None);
    assert_eq!(next.cursor.as_deref(), Some("c2"));
}

#[tokio::test]
async fn rpc_errors_are_typed() {
    let url = spawn_mock().await;
    let mut client = EventsClient::new(url);
    client.initialize().await.unwrap();
    let err = client
        .subscribe(&wire::SubscribeParams {
            name: "orders.changed".into(),
            params: None,
            delivery: wire::DeliverySpec {
                mode: "webhook".into(),
                url: "https://example.com/hook".into(),
                secret: Some("whsec_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into()),
            },
            cursor: None,
            max_age_ms: None,
            ttl_ms: None,
        })
        .await
        .unwrap_err();
    let rpc = err.downcast_ref::<RpcError>().expect("typed RpcError");
    assert_eq!(rpc.code, wire::FORBIDDEN);
    assert_eq!(rpc.code_name(), "Forbidden");
}

#[tokio::test]
async fn unsubscribe_accepts_any_ack() {
    let url = spawn_mock().await;
    let mut client = EventsClient::new(url);
    client.initialize().await.unwrap();
    client
        .unsubscribe(&wire::UnsubscribeParams {
            name: "orders.changed".into(),
            params: None,
            delivery: wire::DeliverySpec {
                mode: String::new(),
                url: "https://example.com/hook".into(),
                secret: None,
            },
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn stream_parses_chunked_sse_frames() {
    let url = spawn_mock().await;
    let mut client = EventsClient::new(url);
    client.initialize().await.unwrap();
    let stream = client
        .stream(&wire::StreamEventsParams {
            name: "orders.changed".into(),
            params: None,
            cursor: None,
            max_age_ms: None,
        })
        .await
        .unwrap();
    let frames: Vec<StreamFrame> = stream.map(|f| f.unwrap()).collect().await;
    assert_eq!(frames.len(), 4, "{frames:?}");
    match &frames[0] {
        StreamFrame::Active(a) => {
            assert_eq!(a.cursor.as_deref(), Some("s1"));
            assert!(!a.truncated);
        }
        other => panic!("expected active, got {other:?}"),
    }
    match &frames[1] {
        StreamFrame::Event(ev) => {
            assert_eq!(ev.event_id, "evt-9");
            assert_eq!(ev.cursor, Some(Some("s2".to_owned())));
        }
        other => panic!("expected event, got {other:?}"),
    }
    match &frames[2] {
        StreamFrame::Heartbeat(h) => assert_eq!(h.cursor.as_deref(), Some("s3")),
        other => panic!("expected heartbeat, got {other:?}"),
    }
    assert!(matches!(frames[3], StreamFrame::Result));
}
