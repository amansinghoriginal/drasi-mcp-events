//! `EventsClient`: MCP Events over Streamable HTTP (single POST endpoint).
//!
//! Every JSON-RPC message is one HTTP POST with
//! `Accept: application/json, text/event-stream`. Unary methods expect an
//! `application/json` response; `events/stream` expects `text/event-stream`
//! whose `data:` frames are JSON-RPC messages, the last being the request's
//! response (`StreamFrame::Result`).

use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _};
use futures::{Stream, StreamExt};
use mcp_events_wire as wire;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tracing::{debug, warn};

use crate::error::RpcError;
use crate::sse::SseParser;

const UNARY_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_BOTH: &str = "application/json, text/event-stream";
const HEADER_SESSION: &str = "mcp-session-id";
const HEADER_PROTOCOL_VERSION: &str = "mcp-protocol-version";

/// One frame of an `events/stream` response.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamFrame {
    Active(wire::EventsActiveParams),
    Event(wire::EventOccurrence),
    Heartbeat(wire::EventsHeartbeatParams),
    Error(wire::EventsErrorParams),
    Terminated(wire::EventsTerminatedParams),
    /// Final JSON-RPC result frame: the server closed the stream.
    Result,
}

pub struct EventStream {
    inner: Pin<Box<dyn Stream<Item = anyhow::Result<StreamFrame>> + Send>>,
}

impl Stream for EventStream {
    type Item = anyhow::Result<StreamFrame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

pub struct EventsClient {
    base_url: String,
    bearer: Option<String>,
    // Construction is infallible per the contract; a TLS-init failure is
    // deferred and surfaced on first use instead of panicking.
    http: Result<reqwest::Client, String>,
    session_id: Option<String>,
    initialized: bool,
    next_id: AtomicI64,
}

impl EventsClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| e.to_string());
        Self {
            base_url: base_url.into(),
            bearer: None,
            http,
            session_id: None,
            initialized: false,
            next_id: AtomicI64::new(1),
        }
    }

    pub fn with_bearer(mut self, token: impl Into<String>) -> Self {
        self.bearer = Some(token.into());
        self
    }

    fn next_id(&self) -> i64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn builder(&self) -> anyhow::Result<reqwest::RequestBuilder> {
        let http = self
            .http
            .as_ref()
            .map_err(|e| anyhow!("failed to construct HTTP client: {e}"))?;
        let mut rb = http
            .post(&self.base_url)
            .header(reqwest::header::ACCEPT, ACCEPT_BOTH);
        if let Some(token) = &self.bearer {
            rb = rb.bearer_auth(token);
        }
        if let Some(sid) = &self.session_id {
            rb = rb.header(HEADER_SESSION, sid);
        }
        if self.initialized {
            rb = rb.header(HEADER_PROTOCOL_VERSION, wire::PROTOCOL_VERSION);
        }
        Ok(rb)
    }

    /// MCP handshake: `initialize`, then `notifications/initialized` (which
    /// must be acknowledged with HTTP 202). Captures the `Mcp-Session-Id`
    /// response header for all subsequent requests.
    pub async fn initialize(&mut self) -> anyhow::Result<wire::InitializeResult> {
        let params = wire::InitializeParams {
            protocol_version: wire::PROTOCOL_VERSION.to_owned(),
            capabilities: json!({}),
            client_info: Some(wire::Implementation {
                name: "mcp-events-client".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                title: None,
            }),
        };
        let id = self.next_id();
        let req = wire::JsonRpcRequest::request(
            id,
            wire::METHOD_INITIALIZE,
            Some(serde_json::to_value(&params)?),
        );
        let resp = self
            .builder()?
            .timeout(UNARY_TIMEOUT)
            .json(&req)
            .send()
            .await
            .context("initialize request failed")?;
        if let Some(sid) = resp
            .headers()
            .get(HEADER_SESSION)
            .and_then(|v| v.to_str().ok())
        {
            debug!(session_id = sid, "captured Mcp-Session-Id");
            self.session_id = Some(sid.to_owned());
        }
        let result: wire::InitializeResult =
            parse_unary(resp, &wire::RequestId::Num(id), wire::METHOD_INITIALIZE).await?;
        if result.protocol_version != wire::PROTOCOL_VERSION {
            warn!(
                server = %result.protocol_version,
                client = wire::PROTOCOL_VERSION,
                "server negotiated a different protocol version"
            );
        }
        if result.capabilities.events.is_none() {
            warn!("server did not advertise the events capability");
        }
        self.initialized = true;
        let notif = wire::JsonRpcRequest::notification(wire::NOTIF_INITIALIZED, None);
        let resp = self
            .builder()?
            .timeout(UNARY_TIMEOUT)
            .json(&notif)
            .send()
            .await
            .context("notifications/initialized request failed")?;
        if resp.status() != reqwest::StatusCode::ACCEPTED {
            bail!(
                "notifications/initialized: expected HTTP 202 Accepted, got {}",
                resp.status()
            );
        }
        Ok(result)
    }

    pub async fn list_events(&self) -> anyhow::Result<wire::ListEventsResult> {
        self.call(wire::METHOD_EVENTS_LIST, None).await
    }

    pub async fn poll(&self, params: &wire::PollEventsParams) -> anyhow::Result<wire::PollEventsResult> {
        self.call(wire::METHOD_EVENTS_POLL, Some(serde_json::to_value(params)?))
            .await
    }

    pub async fn subscribe(
        &self,
        params: &wire::SubscribeParams,
    ) -> anyhow::Result<wire::SubscribeResult> {
        self.call(
            wire::METHOD_EVENTS_SUBSCRIBE,
            Some(serde_json::to_value(params)?),
        )
        .await
    }

    pub async fn unsubscribe(&self, params: &wire::UnsubscribeParams) -> anyhow::Result<()> {
        // The sketch does not define the ack's result shape; accept any JSON.
        let _ack: Value = self
            .call(
                wire::METHOD_EVENTS_UNSUBSCRIBE,
                Some(serde_json::to_value(params)?),
            )
            .await?;
        Ok(())
    }

    /// Opens an `events/stream` request. An immediate JSON-RPC error (invalid
    /// subscription) is returned as `Err` carrying [`RpcError`]; otherwise the
    /// SSE response is parsed incrementally into [`StreamFrame`]s, ending with
    /// `StreamFrame::Result` when the server closes the stream.
    pub async fn stream(&self, params: &wire::StreamEventsParams) -> anyhow::Result<EventStream> {
        let id = self.next_id();
        let req = wire::JsonRpcRequest::request(
            id,
            wire::METHOD_EVENTS_STREAM,
            Some(serde_json::to_value(params)?),
        );
        // Deliberately no request timeout: heartbeats are the liveness signal.
        let resp = self
            .builder()?
            .json(&req)
            .send()
            .await
            .context("events/stream request failed")?;
        let status = resp.status();
        let ct = content_type(resp.headers());
        if ct.starts_with("application/json") {
            let body = resp.bytes().await.context("events/stream: reading body")?;
            let rpc: wire::JsonRpcResponse = serde_json::from_slice(&body)
                .context("events/stream: parsing JSON response")?;
            if let Some(err) = rpc.error {
                return Err(RpcError::from(err).into());
            }
            if rpc.result.is_some() {
                // Degenerate single-response stream: already complete.
                return Ok(EventStream {
                    inner: Box::pin(futures::stream::iter([Ok(StreamFrame::Result)])),
                });
            }
            bail!("events/stream: JSON response with neither result nor error (HTTP {status})");
        }
        if !ct.starts_with("text/event-stream") {
            bail!("events/stream: unexpected response (HTTP {status}, content-type {ct:?})");
        }
        if !status.is_success() {
            bail!("events/stream: HTTP {status} with SSE content-type");
        }
        let expected = wire::RequestId::Num(id);
        let mut body = Box::pin(resp.bytes_stream());
        let frames = async_stream::stream! {
            let mut parser = SseParser::new();
            loop {
                let chunk = match body.next().await {
                    Some(Ok(c)) => c,
                    Some(Err(e)) => {
                        yield Err(anyhow::Error::new(e).context("reading events/stream body"));
                        return;
                    }
                    None => break,
                };
                for ev in parser.push(&chunk) {
                    match parse_stream_frame(&ev.data, &expected) {
                        FrameOutcome::Frame(f) => {
                            let done = matches!(f, StreamFrame::Result);
                            yield Ok(f);
                            if done {
                                return;
                            }
                        }
                        FrameOutcome::Fatal(e) => {
                            yield Err(e);
                            return;
                        }
                        FrameOutcome::Skip => {}
                    }
                }
            }
            if let Some(ev) = parser.finish() {
                match parse_stream_frame(&ev.data, &expected) {
                    FrameOutcome::Frame(f) => yield Ok(f),
                    FrameOutcome::Fatal(e) => yield Err(e),
                    FrameOutcome::Skip => {}
                }
            }
        };
        Ok(EventStream {
            inner: Box::pin(frames),
        })
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Option<Value>,
    ) -> anyhow::Result<T> {
        let id = self.next_id();
        let req = wire::JsonRpcRequest::request(id, method, params);
        let resp = self
            .builder()?
            .timeout(UNARY_TIMEOUT)
            .json(&req)
            .send()
            .await
            .with_context(|| format!("{method} request failed"))?;
        parse_unary(resp, &wire::RequestId::Num(id), method).await
    }
}

async fn parse_unary<T: DeserializeOwned>(
    resp: reqwest::Response,
    id: &wire::RequestId,
    method: &str,
) -> anyhow::Result<T> {
    let status = resp.status();
    let ct = content_type(resp.headers());
    let body = resp
        .bytes()
        .await
        .with_context(|| format!("{method}: reading response body"))?;
    // Base Streamable HTTP permits a server to answer ANY POST — including a
    // unary request — with a `text/event-stream` body carrying the single
    // JSON-RPC response (and possibly server-initiated messages first). We
    // advertise `Accept: application/json, text/event-stream`, so we must
    // accept both: parse JSON directly, or pull the matching response frame
    // out of the SSE body.
    let rpc: wire::JsonRpcResponse = if ct.starts_with("text/event-stream") {
        unary_response_from_sse(&body, id)
            .with_context(|| format!("{method}: reading JSON-RPC response from SSE body"))?
    } else if ct.starts_with("application/json") || ct.is_empty() {
        serde_json::from_slice(&body)
            .with_context(|| format!("{method}: parsing JSON-RPC response"))?
    } else {
        bail!(
            "{method}: unexpected response (HTTP {status}, content-type {ct:?}, {} body bytes)",
            body.len()
        );
    };
    if let Some(err) = rpc.error {
        return Err(RpcError::from(err).into());
    }
    if !status.is_success() {
        bail!("{method}: HTTP {status}");
    }
    if rpc.id.as_ref() != Some(id) {
        warn!(method, expected = %id, got = ?rpc.id, "response id does not match request id");
    }
    let result = rpc
        .result
        .ok_or_else(|| anyhow!("{method}: response has neither result nor error"))?;
    serde_json::from_value(result).with_context(|| format!("{method}: deserializing result"))
}

/// Extract the JSON-RPC response with the given `id` from an SSE-framed unary
/// body. A compliant server sends exactly one response frame; any preceding
/// frames are server-initiated requests/notifications, which a unary caller
/// ignores. Falls back to the last parseable response if none matches `id`.
fn unary_response_from_sse(
    body: &[u8],
    id: &wire::RequestId,
) -> anyhow::Result<wire::JsonRpcResponse> {
    let mut parser = crate::sse::SseParser::new();
    let mut events = parser.push(body);
    events.extend(parser.finish());
    let mut last: Option<wire::JsonRpcResponse> = None;
    for ev in events {
        if let Ok(rpc) = serde_json::from_str::<wire::JsonRpcResponse>(&ev.data) {
            if rpc.id.as_ref() == Some(id) {
                return Ok(rpc);
            }
            last = Some(rpc);
        }
    }
    last.ok_or_else(|| anyhow!("no JSON-RPC response frame found in SSE body"))
}

fn content_type(headers: &reqwest::header::HeaderMap) -> String {
    headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase()
}

enum FrameOutcome {
    Frame(StreamFrame),
    Fatal(anyhow::Error),
    Skip,
}

/// Maps one SSE `data:` payload (a JSON-RPC message) to a [`StreamFrame`].
/// Unknown notification methods and malformed-but-recognized frames are
/// skipped with a warning so a single bad frame does not kill the stream.
fn parse_stream_frame(data: &str, expected: &wire::RequestId) -> FrameOutcome {
    let v: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "skipping non-JSON SSE frame");
            return FrameOutcome::Skip;
        }
    };
    if let Some(method) = v.get("method").and_then(Value::as_str).map(str::to_owned) {
        let params = v.get("params").cloned().unwrap_or(Value::Null);
        check_subscription_id(&params, expected, &method);
        let frame = match method.as_str() {
            wire::NOTIF_EVENTS_ACTIVE => serde_json::from_value(params).map(StreamFrame::Active),
            wire::NOTIF_EVENTS_EVENT => serde_json::from_value(params).map(StreamFrame::Event),
            wire::NOTIF_EVENTS_HEARTBEAT => {
                serde_json::from_value(params).map(StreamFrame::Heartbeat)
            }
            wire::NOTIF_EVENTS_ERROR => serde_json::from_value(params).map(StreamFrame::Error),
            wire::NOTIF_EVENTS_TERMINATED => {
                serde_json::from_value(params).map(StreamFrame::Terminated)
            }
            other => {
                warn!(method = other, "skipping unknown notification on events/stream");
                return FrameOutcome::Skip;
            }
        };
        match frame {
            Ok(f) => FrameOutcome::Frame(f),
            Err(e) => {
                warn!(method = %method, error = %e, "skipping malformed notification params");
                FrameOutcome::Skip
            }
        }
    } else if v.get("result").is_some() || v.get("error").is_some() {
        match serde_json::from_value::<wire::JsonRpcResponse>(v) {
            Ok(rpc) => {
                if rpc.id.as_ref() != Some(expected) {
                    warn!(expected = %expected, got = ?rpc.id, "final stream frame id mismatch");
                }
                if let Some(err) = rpc.error {
                    FrameOutcome::Fatal(RpcError::from(err).into())
                } else {
                    FrameOutcome::Frame(StreamFrame::Result)
                }
            }
            Err(e) => FrameOutcome::Fatal(
                anyhow::Error::new(e).context("malformed final response frame on events/stream"),
            ),
        }
    } else {
        warn!("skipping unrecognized frame on events/stream");
        FrameOutcome::Skip
    }
}

fn check_subscription_id(params: &Value, expected: &wire::RequestId, method: &str) {
    let got = params
        .get("_meta")
        .and_then(|m| m.get(wire::META_SUBSCRIPTION_ID));
    let matches = match (got, expected) {
        (Some(Value::Number(n)), wire::RequestId::Num(e)) => n.as_i64() == Some(*e),
        (Some(Value::String(s)), wire::RequestId::Str(e)) => s == e,
        _ => false,
    };
    if !matches {
        warn!(
            method,
            expected = %expected,
            got = ?got,
            "subscriptionId missing or mismatched on stream frame"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected() -> wire::RequestId {
        wire::RequestId::Num(1)
    }

    fn frame(data: &str) -> Option<StreamFrame> {
        match parse_stream_frame(data, &expected()) {
            FrameOutcome::Frame(f) => Some(f),
            _ => None,
        }
    }

    #[test]
    fn unary_response_extracted_from_sse_body() {
        // A peer (e.g. the TS SDK / mcpkit default) answers a unary POST with
        // a text/event-stream body instead of application/json; the single
        // response frame must still be recovered.
        let body = b"event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n\n";
        let rpc = unary_response_from_sse(body, &expected()).unwrap();
        assert_eq!(rpc.id, Some(wire::RequestId::Num(1)));
        assert_eq!(rpc.result.unwrap()["ok"], serde_json::json!(true));
    }

    #[test]
    fn unary_response_skips_preceding_server_messages_in_sse() {
        // Server-initiated notification first, then the actual response.
        let body = b"data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\",\"params\":{}}\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"v\":7}}\n\n";
        let rpc = unary_response_from_sse(body, &expected()).unwrap();
        assert_eq!(rpc.result.unwrap()["v"], serde_json::json!(7));
    }

    // Frames below are taken from the design sketch examples (§Push-Based Delivery).
    #[test]
    fn parses_active_frame() {
        let f = frame(
            r#"{"jsonrpc":"2.0","method":"notifications/events/active","params":{"cursor":"historyId_99840","truncated":false,"_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
        );
        match f {
            Some(StreamFrame::Active(a)) => {
                assert_eq!(a.cursor.as_deref(), Some("historyId_99840"));
                assert!(!a.truncated);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_event_frame() {
        let f = frame(
            r#"{"jsonrpc":"2.0","method":"notifications/events/event","params":{"eventId":"evt_001","name":"email.received","timestamp":"2026-02-19T15:30:00Z","data":{"subject":"MCP spec review"},"cursor":"historyId_99842","_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
        );
        match f {
            Some(StreamFrame::Event(ev)) => {
                assert_eq!(ev.event_id, "evt_001");
                assert_eq!(ev.cursor, Some(Some("historyId_99842".to_owned())));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_heartbeat_with_null_cursor() {
        let f = frame(
            r#"{"jsonrpc":"2.0","method":"notifications/events/heartbeat","params":{"cursor":null,"_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
        );
        match f {
            Some(StreamFrame::Heartbeat(h)) => assert_eq!(h.cursor, None),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_error_and_terminated_frames() {
        let f = frame(
            r#"{"jsonrpc":"2.0","method":"notifications/events/error","params":{"error":{"code":-32603,"message":"UpstreamError","data":{"reason":"Gmail API 503"}},"_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
        );
        assert!(matches!(f, Some(StreamFrame::Error(_))));
        let f = frame(
            r#"{"jsonrpc":"2.0","method":"notifications/events/terminated","params":{"error":{"code":-32012,"message":"Forbidden","data":{"reason":"Access revoked"}},"_meta":{"io.modelcontextprotocol/subscriptionId":1}}}"#,
        );
        match f {
            Some(StreamFrame::Terminated(t)) => assert_eq!(t.error.code, wire::FORBIDDEN),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_final_result_frame() {
        let f = frame(r#"{"jsonrpc":"2.0","id":1,"result":{"_meta":{}}}"#);
        assert!(matches!(f, Some(StreamFrame::Result)));
    }

    #[test]
    fn error_response_frame_is_fatal_rpc_error() {
        let out = parse_stream_frame(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32011,"message":"NotFound","data":{"kind":"event"}}}"#,
            &expected(),
        );
        match out {
            FrameOutcome::Fatal(e) => {
                let rpc = e.downcast_ref::<RpcError>().expect("RpcError");
                assert_eq!(rpc.code, wire::NOT_FOUND);
            }
            _ => panic!("expected fatal"),
        }
    }

    #[test]
    fn unknown_method_and_garbage_skipped() {
        assert!(matches!(
            parse_stream_frame(
                r#"{"jsonrpc":"2.0","method":"notifications/something/else","params":{}}"#,
                &expected()
            ),
            FrameOutcome::Skip
        ));
        assert!(matches!(
            parse_stream_frame("not json", &expected()),
            FrameOutcome::Skip
        ));
        assert!(matches!(
            parse_stream_frame(r#"{"jsonrpc":"2.0"}"#, &expected()),
            FrameOutcome::Skip
        ));
    }

    #[test]
    fn malformed_known_notification_skipped_not_fatal() {
        // events/event missing required fields → skip, stream stays alive.
        assert!(matches!(
            parse_stream_frame(
                r#"{"jsonrpc":"2.0","method":"notifications/events/event","params":{"nope":true}}"#,
                &expected()
            ),
            FrameOutcome::Skip
        ));
    }
}
