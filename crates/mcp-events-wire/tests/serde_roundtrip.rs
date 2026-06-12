//! Serde round-trips against JSON literals copied verbatim from the design
//! sketch examples (jsonc comments stripped), plus null-vs-absent matrix tests.

use mcp_events_wire::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

/// Deserialize `literal` into `T`, re-serialize, and assert value equality
/// with the original literal.
fn round_trip<T: Serialize + DeserializeOwned>(literal: &str) -> T {
    let parsed: Value = serde_json::from_str(literal).unwrap();
    let typed: T = serde_json::from_str(literal).unwrap();
    let back = serde_json::to_value(&typed).unwrap();
    assert_eq!(back, parsed, "re-serialized JSON differs from literal");
    typed
}

/// Round-trip the `params` member of a JSON-RPC envelope as `T`.
fn round_trip_params<T: Serialize + DeserializeOwned>(envelope: &JsonRpcRequest) -> T {
    let params = envelope.params.clone().unwrap();
    let typed: T = serde_json::from_value(params.clone()).unwrap();
    let back = serde_json::to_value(&typed).unwrap();
    assert_eq!(back, params, "re-serialized params differ from literal");
    typed
}

// ---------------------------------------------------------------- events/list

#[test]
fn event_definition_literal() {
    let def: EventDefinition = round_trip(
        r#"{
          "name": "email.received",
          "description": "Fires when a new email arrives in the inbox",
          "delivery": ["poll"],
          "inputSchema": {
            "type": "object",
            "properties": {
              "from": { "type": "string", "description": "Glob pattern for sender address" },
              "subject_contains": { "type": "string" },
              "redact_pii": { "type": "boolean", "default": false, "description": "Strip PII from event payloads" },
              "include_body_preview": { "type": "boolean", "default": true, "description": "Include a snippet of the email body" }
            }
          },
          "payloadSchema": {
            "type": "object",
            "properties": {
              "messageId": { "type": "string" },
              "from": { "type": "string" },
              "subject": { "type": "string" },
              "receivedAt": { "type": "string", "format": "date-time" }
            }
          }
        }"#,
    );
    assert_eq!(def.name, "email.received");
    assert_eq!(def.delivery, vec![DeliveryMode::Poll]);
    assert!(def.meta.is_none());
}

#[test]
fn list_events_result_next_cursor_absent_when_no_more_pages() {
    let res = ListEventsResult {
        events: vec![],
        next_cursor: None,
    };
    let v = serde_json::to_value(&res).unwrap();
    assert_eq!(v, json!({ "events": [] }));
}

#[test]
fn capability_declaration_literal() {
    let caps: ServerCapabilities = round_trip(r#"{ "events": { "listChanged": true } }"#);
    assert_eq!(caps.events.unwrap().list_changed, Some(true));
    assert!(caps.extra.is_empty());
}

#[test]
fn server_capabilities_passes_through_unknown_capabilities() {
    let caps: ServerCapabilities = round_trip(
        r#"{ "events": { "listChanged": false }, "tools": { "listChanged": true } }"#,
    );
    assert!(caps.extra.contains_key("tools"));
}

// ---------------------------------------------------------------- events/poll

#[test]
fn poll_request_literal() {
    let params: PollEventsParams = round_trip(
        r#"{
          "name": "email.received",
          "params": {
            "from": "*@anthropic.com",
            "redact_pii": true
          },
          "cursor": null,
          "maxAgeMs": 300000,
          "maxEvents": 50
        }"#,
    );
    assert_eq!(params.name, "email.received");
    assert_eq!(params.cursor, None);
    assert_eq!(params.max_age_ms, Some(300_000));
    assert_eq!(params.max_events, Some(50));
}

#[test]
fn poll_response_literal() {
    let res: PollEventsResult = round_trip(
        r#"{
          "events": [
            {
              "eventId": "evt_001",
              "name": "email.received",
              "timestamp": "2026-02-19T15:30:00Z",
              "data": {
                "messageId": "msg_xyz",
                "from": "dsp@anthropic.com",
                "subject": "MCP spec review",
                "receivedAt": "2026-02-19T15:29:58Z"
              }
            }
          ],
          "cursor": "historyId_99842",
          "truncated": false,
          "hasMore": false,
          "nextPollMs": 30000
        }"#,
    );
    assert_eq!(res.events.len(), 1);
    // Poll occurrences carry no per-event cursor: absent, not null.
    assert_eq!(res.events[0].cursor, None);
    assert_eq!(res.cursor.as_deref(), Some("historyId_99842"));
    assert!(!res.truncated);
    assert!(!res.has_more);
    assert_eq!(res.next_poll_ms, 30_000);
}

// ------------------------------------------------------------- events/stream

#[test]
fn stream_request_envelope_literal() {
    let req: JsonRpcRequest = round_trip(
        r#"{
          "jsonrpc": "2.0",
          "method": "events/stream",
          "id": 1,
          "params": {
            "name": "email.received",
            "params": { "from": "*@anthropic.com", "redact_pii": true },
            "cursor": null,
            "maxAgeMs": 300000
          }
        }"#,
    );
    assert_eq!(req.method, METHOD_EVENTS_STREAM);
    assert_eq!(req.id, Some(RequestId::Num(1)));
    let params: StreamEventsParams = round_trip_params(&req);
    assert_eq!(params.cursor, None);
    assert_eq!(params.max_age_ms, Some(300_000));
}

#[test]
fn notification_active_literal() {
    let req: JsonRpcRequest = round_trip(
        r#"{"jsonrpc":"2.0","method":"notifications/events/active","params":{"cursor":"historyId_99840","truncated":false,"_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
    );
    assert!(req.is_notification());
    assert_eq!(req.method, NOTIF_EVENTS_ACTIVE);
    let params: EventsActiveParams = round_trip_params(&req);
    assert_eq!(params.cursor.as_deref(), Some("historyId_99840"));
    assert!(!params.truncated);
    assert_eq!(params.meta[META_SUBSCRIPTION_ID], json!(1));
}

#[test]
fn notification_event_literal() {
    let req: JsonRpcRequest = round_trip(
        r#"{"jsonrpc":"2.0","method":"notifications/events/event","params":{"eventId":"evt_001","name":"email.received","timestamp":"2026-02-19T15:30:00Z","data":{"messageId":"msg_xyz","from":"dsp@anthropic.com","subject":"MCP spec review"},"cursor":"historyId_99842","_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
    );
    assert_eq!(req.method, NOTIF_EVENTS_EVENT);
    let occ: EventOccurrence = round_trip_params(&req);
    assert_eq!(occ.cursor, Some(Some("historyId_99842".to_owned())));
    assert_eq!(occ.meta.unwrap()[META_SUBSCRIPTION_ID], json!(1));
}

#[test]
fn notification_error_literal() {
    let req: JsonRpcRequest = round_trip(
        r#"{"jsonrpc":"2.0","method":"notifications/events/error","params":{"error":{"code":-32603,"message":"UpstreamError","data":{"reason":"Gmail API 503"}},"_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
    );
    assert_eq!(req.method, NOTIF_EVENTS_ERROR);
    let params: EventsErrorParams = round_trip_params(&req);
    assert_eq!(params.error.code, INTERNAL_ERROR);
    assert_eq!(params.error.message, "UpstreamError");
    assert_eq!(params.meta[META_SUBSCRIPTION_ID], json!(1));
}

#[test]
fn notification_heartbeat_literal() {
    let req: JsonRpcRequest = round_trip(
        r#"{"jsonrpc":"2.0","method":"notifications/events/heartbeat","params":{"cursor":"historyId_99850","_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
    );
    assert_eq!(req.method, NOTIF_EVENTS_HEARTBEAT);
    let params: EventsHeartbeatParams = round_trip_params(&req);
    assert_eq!(params.cursor.as_deref(), Some("historyId_99850"));
}

#[test]
fn notification_terminated_literal() {
    let req: JsonRpcRequest = round_trip(
        r#"{"jsonrpc":"2.0","method":"notifications/events/terminated","params":{"error":{"code":-32012,"message":"Forbidden","data":{"reason":"Access revoked"}},"_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
    );
    assert_eq!(req.method, NOTIF_EVENTS_TERMINATED);
    let params: EventsTerminatedParams = round_trip_params(&req);
    assert_eq!(params.error.code, FORBIDDEN);
    assert_eq!(params.meta[META_SUBSCRIPTION_ID], json!(1));
}

#[test]
fn stream_final_frame_literal() {
    let res: JsonRpcResponse = round_trip(r#"{"jsonrpc":"2.0","id":1,"result":{"_meta":{}}}"#);
    assert_eq!(res.id, Some(RequestId::Num(1)));
    assert_eq!(res.result, Some(json!({ "_meta": {} })));
    assert!(res.error.is_none());
}

// ---------------------------------------------------------- events/subscribe

#[test]
fn subscribe_request_envelope_literal() {
    let req: JsonRpcRequest = round_trip(
        r#"{
          "jsonrpc": "2.0",
          "method": "events/subscribe",
          "id": 2,
          "params": {
            "name": "incident.created",
            "params": { "severity": "P1" },
            "delivery": {
              "mode": "webhook",
              "url": "https://proxy.example.com/hooks/client123",
              "secret": "whsec_<base64-of-24-to-64-random-bytes>"
            },
            "cursor": null,
            "maxAgeMs": 300000,
            "ttlMs": 3600000
          }
        }"#,
    );
    assert_eq!(req.method, METHOD_EVENTS_SUBSCRIBE);
    let params: SubscribeParams = round_trip_params(&req);
    assert_eq!(params.delivery.mode, "webhook");
    assert_eq!(params.delivery.url, "https://proxy.example.com/hooks/client123");
    assert!(params.delivery.secret.is_some());
    assert_eq!(params.cursor, None);
    assert_eq!(params.ttl_ms, Some(Some(3_600_000)));
}

#[test]
fn subscribe_response_literal() {
    let res: SubscribeResult = round_trip(
        r#"{
          "id": "sub_a3f1c8e2b0d49f7e",
          "refreshBefore": "2026-02-19T16:30:00Z",
          "cursor": "cursor_start_001",
          "truncated": false
        }"#,
    );
    assert_eq!(res.id, "sub_a3f1c8e2b0d49f7e");
    assert_eq!(res.refresh_before.as_deref(), Some("2026-02-19T16:30:00Z"));
    assert_eq!(res.cursor.as_deref(), Some("cursor_start_001"));
    assert!(res.delivery_status.is_none());
}

#[test]
fn subscribe_refresh_healthy_literal() {
    let res: SubscribeResult = round_trip(
        r#"{
          "id": "sub_a3f1c8e2b0d49f7e",
          "refreshBefore": "2026-02-19T17:00:00Z",
          "cursor": "cursor_xyz",
          "truncated": false,
          "deliveryStatus": {
            "active": true,
            "lastDeliveryAt": "2026-02-19T16:28:00Z",
            "lastError": null
          }
        }"#,
    );
    let ds = res.delivery_status.unwrap();
    assert!(ds.active);
    assert_eq!(ds.last_delivery_at.as_deref(), Some("2026-02-19T16:28:00Z"));
    assert_eq!(ds.last_error, None);
    assert_eq!(ds.failed_since, None);
}

#[test]
fn subscribe_refresh_failing_literal() {
    let res: SubscribeResult = round_trip(
        r#"{
          "id": "sub_a3f1c8e2b0d49f7e",
          "refreshBefore": "2026-02-19T17:00:00Z",
          "cursor": "cursor_xyz",
          "truncated": false,
          "deliveryStatus": {
            "active": false,
            "lastDeliveryAt": "2026-02-19T15:45:00Z",
            "lastError": "http_4xx",
            "failedSince": "2026-02-19T15:50:00Z"
          }
        }"#,
    );
    let ds = res.delivery_status.unwrap();
    assert!(!ds.active);
    assert_eq!(ds.last_error.as_deref(), Some("http_4xx"));
    assert_eq!(ds.failed_since.as_deref(), Some("2026-02-19T15:50:00Z"));
}

#[test]
fn unsubscribe_request_envelope_literal() {
    let req: JsonRpcRequest = round_trip(
        r#"{
          "jsonrpc": "2.0",
          "method": "events/unsubscribe",
          "id": 3,
          "params": {
            "name": "incident.created",
            "params": { "severity": "P1" },
            "delivery": { "url": "https://proxy.example.com/hooks/client123" }
          }
        }"#,
    );
    assert_eq!(req.method, METHOD_EVENTS_UNSUBSCRIBE);
    let params: UnsubscribeParams = round_trip_params(&req);
    // Unsubscribe `delivery` is url-only: no mode, no secret.
    assert!(params.delivery.mode.is_empty());
    assert!(params.delivery.secret.is_none());
    assert_eq!(params.delivery.url, "https://proxy.example.com/hooks/client123");
}

// ------------------------------------------------------- webhook POST bodies

#[test]
fn webhook_event_occurrence_body_literal() {
    let occ: EventOccurrence = round_trip(
        r#"{
          "eventId": "evt_789",
          "name": "incident.created",
          "timestamp": "2026-02-19T16:00:00Z",
          "data": {
            "incidentId": "INC-1234",
            "title": "Database connection pool exhausted",
            "severity": "P1"
          },
          "cursor": "cursor_xyz"
        }"#,
    );
    assert_eq!(occ.event_id, "evt_789");
    assert_eq!(occ.cursor, Some(Some("cursor_xyz".to_owned())));
    assert!(occ.meta.is_none());
}

#[test]
fn control_envelope_gap_literal() {
    let body: WebhookControlBody = round_trip(r#"{"type":"gap","cursor":"<fresh>"}"#);
    assert_eq!(
        body,
        WebhookControlBody::Gap {
            cursor: Some("<fresh>".to_owned())
        }
    );
}

#[test]
fn control_envelope_terminated_literal() {
    let body: WebhookControlBody = round_trip(
        r#"{"type":"terminated","error":{"code":-32012,"message":"Forbidden","data":{"reason":"Access revoked"}}}"#,
    );
    match body {
        WebhookControlBody::Terminated { error } => {
            assert_eq!(error.code, FORBIDDEN);
            assert_eq!(error.data, Some(json!({ "reason": "Access revoked" })));
        }
        other => panic!("expected terminated, got {other:?}"),
    }
}

#[test]
fn control_envelope_verification_literal() {
    let body: WebhookControlBody = round_trip(r#"{"type":"verification","challenge":"<nonce>"}"#);
    assert_eq!(
        body,
        WebhookControlBody::Verification {
            challenge: "<nonce>".to_owned()
        }
    );
}

// ------------------------------------------------------ null-vs-absent matrix

#[test]
fn occurrence_cursor_absent_vs_null_vs_value() {
    let base = r#"{"eventId":"e1","name":"n","timestamp":"2026-01-01T00:00:00Z","data":{}}"#;

    let absent: EventOccurrence = serde_json::from_str(base).unwrap();
    assert_eq!(absent.cursor, None);
    let v = serde_json::to_value(&absent).unwrap();
    assert!(v.get("cursor").is_none(), "absent cursor must not serialize");

    let null: EventOccurrence = serde_json::from_str(
        r#"{"eventId":"e1","name":"n","timestamp":"2026-01-01T00:00:00Z","data":{},"cursor":null}"#,
    )
    .unwrap();
    assert_eq!(null.cursor, Some(None));
    let v = serde_json::to_value(&null).unwrap();
    assert_eq!(v.get("cursor"), Some(&Value::Null), "explicit null must round-trip");

    let val: EventOccurrence = serde_json::from_str(
        r#"{"eventId":"e1","name":"n","timestamp":"2026-01-01T00:00:00Z","data":{},"cursor":"c1"}"#,
    )
    .unwrap();
    assert_eq!(val.cursor, Some(Some("c1".to_owned())));
    let v = serde_json::to_value(&val).unwrap();
    assert_eq!(v.get("cursor"), Some(&json!("c1")));
}

#[test]
fn subscribe_ttl_ms_absent_vs_null_vs_value() {
    let base = r#"{"name":"n","delivery":{"mode":"webhook","url":"https://x.example/h","secret":"whsec_x"},"cursor":null}"#;

    let absent: SubscribeParams = serde_json::from_str(base).unwrap();
    assert_eq!(absent.ttl_ms, None, "absent ttlMs = server default");
    let v = serde_json::to_value(&absent).unwrap();
    assert!(v.get("ttlMs").is_none(), "absent ttlMs must not serialize");

    let null: SubscribeParams = serde_json::from_str(
        r#"{"name":"n","delivery":{"mode":"webhook","url":"https://x.example/h","secret":"whsec_x"},"cursor":null,"ttlMs":null}"#,
    )
    .unwrap();
    assert_eq!(null.ttl_ms, Some(None), "ttlMs:null = request no expiry");
    let v = serde_json::to_value(&null).unwrap();
    assert_eq!(v.get("ttlMs"), Some(&Value::Null));

    let val: SubscribeParams = serde_json::from_str(
        r#"{"name":"n","delivery":{"mode":"webhook","url":"https://x.example/h","secret":"whsec_x"},"cursor":null,"ttlMs":5000}"#,
    )
    .unwrap();
    assert_eq!(val.ttl_ms, Some(Some(5000)));
}

#[test]
fn poll_result_cursor_always_serialized_even_when_null() {
    let res = PollEventsResult {
        events: vec![],
        cursor: None,
        truncated: false,
        has_more: false,
        next_poll_ms: 2000,
    };
    let v = serde_json::to_value(&res).unwrap();
    assert_eq!(v.get("cursor"), Some(&Value::Null));

    let res: PollEventsResult = serde_json::from_value(v).unwrap();
    assert_eq!(res.cursor, None);
}

#[test]
fn subscribe_result_nullable_fields_always_serialized() {
    let res = SubscribeResult {
        id: "sub_x".to_owned(),
        refresh_before: None,
        cursor: None,
        truncated: false,
        delivery_status: None,
    };
    let v = serde_json::to_value(&res).unwrap();
    assert_eq!(v.get("refreshBefore"), Some(&Value::Null), "no-expiry grant is explicit null");
    assert_eq!(v.get("cursor"), Some(&Value::Null), "no-replay cursor is explicit null");
    assert!(v.get("deliveryStatus").is_none(), "optional deliveryStatus omitted");
}

#[test]
fn request_cursor_absent_is_accepted_as_start_from_now() {
    // The sketch only ever shows "cursor": null; absent is treated identically.
    let p: PollEventsParams = serde_json::from_str(r#"{"name":"n"}"#).unwrap();
    assert_eq!(p.cursor, None);
    let v = serde_json::to_value(&p).unwrap();
    assert_eq!(v.get("cursor"), Some(&Value::Null), "request cursor serializes as explicit null");
}

#[test]
fn heartbeat_and_active_cursor_null_serialized_for_no_replay_types() {
    let hb = EventsHeartbeatParams {
        cursor: None,
        meta: json!({ META_SUBSCRIPTION_ID: 7 }),
    };
    let v = serde_json::to_value(&hb).unwrap();
    assert_eq!(v.get("cursor"), Some(&Value::Null));

    let active = EventsActiveParams {
        cursor: None,
        truncated: false,
        meta: json!({ META_SUBSCRIPTION_ID: "req-9" }),
    };
    let v = serde_json::to_value(&active).unwrap();
    assert_eq!(v.get("cursor"), Some(&Value::Null));
    assert_eq!(v["_meta"][META_SUBSCRIPTION_ID], json!("req-9"));
}

// ----------------------------------------------------------------- misc wire

#[test]
fn request_id_string_and_number() {
    let n: RequestId = serde_json::from_str("42").unwrap();
    assert_eq!(n, RequestId::Num(42));
    let s: RequestId = serde_json::from_str(r#""abc""#).unwrap();
    assert_eq!(s, RequestId::Str("abc".to_owned()));
    assert_eq!(serde_json::to_string(&n).unwrap(), "42");
    assert_eq!(serde_json::to_string(&s).unwrap(), r#""abc""#);
}

#[test]
fn delivery_mode_lowercase() {
    assert_eq!(serde_json::to_value(DeliveryMode::Poll).unwrap(), json!("poll"));
    assert_eq!(serde_json::to_value(DeliveryMode::Push).unwrap(), json!("push"));
    assert_eq!(serde_json::to_value(DeliveryMode::Webhook).unwrap(), json!("webhook"));
    let m: DeliveryMode = serde_json::from_value(json!("webhook")).unwrap();
    assert_eq!(m, DeliveryMode::Webhook);
}

#[test]
fn error_constructor_data_payloads() {
    let e = JsonRpcError::not_found("event", "NotFound");
    assert_eq!(e.code, NOT_FOUND);
    assert_eq!(e.data, Some(json!({ "kind": "event" })));

    let e = JsonRpcError::forbidden("Forbidden");
    assert_eq!(e.code, FORBIDDEN);
    assert_eq!(e.data, None);

    let e = JsonRpcError::resource_exhausted("subscriptions", Some(16));
    assert_eq!(e.code, RESOURCE_EXHAUSTED);
    assert_eq!(e.data, Some(json!({ "limit": "subscriptions", "max": 16 })));

    let e = JsonRpcError::resource_exhausted("subscriptions", None);
    assert_eq!(e.data, Some(json!({ "limit": "subscriptions" })));

    let e = JsonRpcError::unsupported("deliveryMode", "push");
    assert_eq!(e.code, UNSUPPORTED);
    assert_eq!(e.data, Some(json!({ "feature": "deliveryMode", "value": "push" })));

    let e = JsonRpcError::callback_endpoint_error("challenge_failed");
    assert_eq!(e.code, CALLBACK_ENDPOINT_ERROR);
    assert_eq!(e.data, Some(json!({ "reason": "challenge_failed" })));
}

#[test]
fn initialize_result_shape() {
    let res = InitializeResult {
        protocol_version: PROTOCOL_VERSION.to_owned(),
        capabilities: ServerCapabilities {
            events: Some(EventsCapability {
                list_changed: Some(false),
            }),
            extra: serde_json::Map::new(),
        },
        server_info: Implementation {
            name: "mcp-events-server".to_owned(),
            version: "0.1.0".to_owned(),
            title: None,
        },
        instructions: None,
    };
    let v = serde_json::to_value(&res).unwrap();
    assert_eq!(
        v,
        json!({
            "protocolVersion": "2025-11-25",
            "capabilities": { "events": { "listChanged": false } },
            "serverInfo": { "name": "mcp-events-server", "version": "0.1.0" }
        })
    );
    let back: InitializeResult = serde_json::from_value(v).unwrap();
    assert_eq!(back, res);
}
