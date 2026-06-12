//! Hand-rolled SSE consumer for the Drasi Server SSE reaction.
//!
//! Wire format per `drasi/SSE-FORMAT.md` (verified against
//! `core/components/reactions/sse/src/sse.rs`): anonymous events with a single
//! `data:` line carrying minified JSON, `: keep-alive` comments, and
//! application-level `{"type":"heartbeat","ts":...}` data frames. No `id:`/
//! `event:` fields and no replay.

use std::time::Duration;

use anyhow::Context as _;
use futures::StreamExt as _;
use tokio::sync::mpsc;

use crate::{ChangeType, FeedEvent};

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
// Guard against a stream that never terminates a line/frame.
const MAX_BUFFERED_BYTES: usize = 4 * 1024 * 1024;

/// One dispatched SSE event (post line-protocol reassembly).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SseFrame {
    pub data: String,
    pub event: Option<String>,
    pub id: Option<String>,
}

/// Incremental SSE line-protocol parser. Feed it raw byte chunks (arbitrary
/// boundaries); it yields complete frames at blank-line dispatch points.
#[derive(Debug, Default)]
pub(crate) struct SseParser {
    buf: Vec<u8>,
    data_lines: Vec<String>,
    event_type: Option<String>,
    last_event_id: Option<String>,
}

impl SseParser {
    pub fn push(&mut self, chunk: &[u8]) -> Vec<SseFrame> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
            let raw: Vec<u8> = self.buf.drain(..=nl).collect();
            let mut line = &raw[..raw.len() - 1];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            let line = String::from_utf8_lossy(line);
            if line.is_empty() {
                if let Some(frame) = self.dispatch() {
                    out.push(frame);
                }
            } else {
                self.process_line(&line);
            }
        }
        if self.buf.len() > MAX_BUFFERED_BYTES {
            tracing::warn!(
                buffered = self.buf.len(),
                "SSE input exceeded buffer cap without a line terminator; discarding pending data"
            );
            self.buf.clear();
            self.data_lines.clear();
            self.event_type = None;
        }
        out
    }

    fn process_line(&mut self, line: &str) {
        if line.starts_with(':') {
            return; // comment (e.g. ": keep-alive")
        }
        let (name, value) = match line.find(':') {
            Some(idx) => {
                let value = &line[idx + 1..];
                // SSE spec: strip at most one leading space from the value.
                (&line[..idx], value.strip_prefix(' ').unwrap_or(value))
            }
            None => (line, ""),
        };
        match name {
            "data" => self.data_lines.push(value.to_string()),
            "event" => self.event_type = Some(value.to_string()),
            "id" => self.last_event_id = Some(value.to_string()),
            // "retry" and unknown fields are ignored.
            _ => {}
        }
    }

    fn dispatch(&mut self) -> Option<SseFrame> {
        let event = self.event_type.take();
        if self.data_lines.is_empty() {
            return None;
        }
        let data = std::mem::take(&mut self.data_lines).join("\n");
        Some(SseFrame {
            data,
            event,
            id: self.last_event_id.clone(),
        })
    }
}

/// Maps one frame payload to zero or more `FeedEvent`s. Malformed or
/// unrecognized payloads are logged and skipped (never an error).
pub(crate) fn feed_events_from_payload(payload: &str) -> Vec<FeedEvent> {
    if payload.trim().is_empty() {
        return Vec::new();
    }
    let value: serde_json::Value = match serde_json::from_str(payload) {
        Ok(v) => v,
        Err(error) => {
            tracing::warn!(%error, payload, "skipping malformed SSE JSON payload");
            return Vec::new();
        }
    };
    let Some(obj) = value.as_object() else {
        tracing::warn!(payload, "skipping non-object SSE payload");
        return Vec::new();
    };
    if obj.get("type").and_then(|v| v.as_str()) == Some("heartbeat") {
        tracing::trace!("drasi SSE heartbeat");
        return Vec::new();
    }
    let query_id = obj.get("queryId").and_then(|v| v.as_str());
    let results = obj.get("results").and_then(|v| v.as_array());
    let (Some(query_id), Some(results)) = (query_id, results) else {
        tracing::warn!(payload, "skipping SSE payload of unrecognized shape");
        return Vec::new();
    };
    let timestamp = obj
        .get("timestamp")
        .and_then(|v| v.as_i64())
        .and_then(chrono::DateTime::from_timestamp_millis);
    results
        .iter()
        .filter_map(|diff| feed_event_from_diff(query_id, timestamp, diff))
        .collect()
}

fn feed_event_from_diff(
    query_id: &str,
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
    diff: &serde_json::Value,
) -> Option<FeedEvent> {
    let Some(obj) = diff.as_object() else {
        tracing::warn!(?diff, "skipping non-object diff in SSE results");
        return None;
    };
    // JSON null is treated the same as absent.
    let field = |key: &str| obj.get(key).filter(|v| !v.is_null()).cloned();
    let (change, before, after) = match obj.get("type").and_then(|v| v.as_str()) {
        Some("ADD") => {
            let Some(data) = field("data") else {
                tracing::warn!(?diff, "skipping ADD diff with missing data");
                return None;
            };
            (ChangeType::Added, None, Some(data))
        }
        Some("UPDATE") => {
            // `data` duplicates `after` in drasi's encoding; fall back to it.
            let Some(after) = field("after").or_else(|| field("data")) else {
                tracing::warn!(?diff, "skipping UPDATE diff with missing after/data");
                return None;
            };
            (ChangeType::Updated, field("before"), Some(after))
        }
        Some("DELETE") => {
            let Some(data) = field("data") else {
                tracing::warn!(?diff, "skipping DELETE diff with missing data");
                return None;
            };
            (ChangeType::Deleted, Some(data), None)
        }
        // The reaction routes aggregations through its "updated" path; do the same.
        Some("aggregation") => {
            let Some(after) = field("after") else {
                tracing::warn!(?diff, "skipping aggregation diff with missing after");
                return None;
            };
            (ChangeType::Updated, field("before"), Some(after))
        }
        Some("noop") => return None,
        other => {
            tracing::warn!(diff_type = ?other, "skipping diff with unknown type");
            return None;
        }
    };
    Some(FeedEvent {
        query_id: query_id.to_string(),
        change,
        before,
        after,
        timestamp,
        // Drasi SSE frames carry no stable per-diff identifier.
        upstream_id: None,
    })
}

enum StreamEnd {
    ChannelClosed,
    ServerClosed,
}

async fn stream_once(
    client: &reqwest::Client,
    url: &str,
    tx: &mpsc::Sender<FeedEvent>,
    backoff: &mut Duration,
) -> anyhow::Result<StreamEnd> {
    let resp = client
        .get(url)
        .send()
        .await
        .context("connecting to drasi SSE endpoint")?;
    let resp = resp
        .error_for_status()
        .context("drasi SSE endpoint returned error status")?;
    // Successful connection: reset the reconnect backoff.
    *backoff = BACKOFF_MIN;
    tracing::info!(url = %url, "connected to drasi SSE stream");
    let mut stream = resp.bytes_stream();
    let mut parser = SseParser::default();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("reading drasi SSE stream")?;
        for frame in parser.push(&chunk) {
            for ev in feed_events_from_payload(&frame.data) {
                if tx.send(ev).await.is_err() {
                    return Ok(StreamEnd::ChannelClosed);
                }
            }
        }
    }
    Ok(StreamEnd::ServerClosed)
}

/// Consumes a Drasi Server SSE reaction endpoint, forwarding parsed
/// [`FeedEvent`]s to `tx`. Reconnects forever with capped exponential backoff
/// (1s..30s, reset on successful connect); returns `Ok(())` once the receiver
/// side of `tx` is dropped. Malformed frames are logged and skipped.
pub async fn run_drasi_sse_feed(
    url: String,
    tx: mpsc::Sender<FeedEvent>,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .build()
        .context("building HTTP client for drasi SSE feed")?;
    let mut backoff = BACKOFF_MIN;
    loop {
        if tx.is_closed() {
            return Ok(());
        }
        match stream_once(&client, &url, &tx, &mut backoff).await {
            Ok(StreamEnd::ChannelClosed) => return Ok(()),
            Ok(StreamEnd::ServerClosed) => {
                tracing::info!(url = %url, "drasi SSE stream closed by server; reconnecting");
            }
            Err(error) => {
                tracing::warn!(url = %url, error = format!("{error:#}"), "drasi SSE connection failed; reconnecting");
            }
        }
        tracing::debug!(backoff_ms = backoff.as_millis() as u64, "waiting before SSE reconnect");
        tokio::select! {
            _ = tx.closed() => return Ok(()),
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Frames taken verbatim from drasi/SSE-FORMAT.md §4.
    const ADD_FRAME: &str = "data: {\"queryId\":\"high-value-orders\",\"results\":[{\"data\":{\"customer\":\"erin\",\"id\":5,\"status\":\"open\",\"total\":5000.0},\"type\":\"ADD\"}],\"timestamp\":1749600000123}\n\n";
    const UPDATE_FRAME: &str = "data: {\"queryId\":\"high-value-orders\",\"results\":[{\"after\":{\"customer\":\"alice\",\"id\":1,\"status\":\"open\",\"total\":1800.0},\"before\":{\"customer\":\"alice\",\"id\":1,\"status\":\"open\",\"total\":1500.0},\"data\":{\"customer\":\"alice\",\"id\":1,\"status\":\"open\",\"total\":1800.0},\"type\":\"UPDATE\"}],\"timestamp\":1749600005456}\n\n";
    const DELETE_FRAME: &str = "data: {\"queryId\":\"high-value-orders\",\"results\":[{\"data\":{\"customer\":\"erin\",\"id\":5,\"status\":\"open\",\"total\":5000.0},\"type\":\"DELETE\"}],\"timestamp\":1749600010789}\n\n";
    const HEARTBEAT_FRAME: &str = "data: {\"ts\":1749600015000,\"type\":\"heartbeat\"}\n\n";
    const KEEPALIVE_COMMENT: &str = ": keep-alive\n\n";
    const MULTI_DIFF_FRAME: &str = "data: {\"queryId\":\"high-value-orders\",\"results\":[{\"data\":{\"customer\":\"alice\",\"id\":1,\"status\":\"open\",\"total\":1500.0},\"type\":\"ADD\"},{\"data\":{\"customer\":\"carol\",\"id\":3,\"status\":\"shipped\",\"total\":2200.5},\"type\":\"ADD\"}],\"timestamp\":1749599990000}\n\n";

    fn parse_chunked(input: &[u8], chunk_size: usize) -> Vec<FeedEvent> {
        let mut parser = SseParser::default();
        let mut events = Vec::new();
        for chunk in input.chunks(chunk_size) {
            for frame in parser.push(chunk) {
                events.extend(feed_events_from_payload(&frame.data));
            }
        }
        events
    }

    #[test]
    fn parses_add_frame() {
        let events = parse_chunked(ADD_FRAME.as_bytes(), ADD_FRAME.len());
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.query_id, "high-value-orders");
        assert_eq!(ev.change, ChangeType::Added);
        assert_eq!(ev.before, None);
        let after = ev.after.as_ref().unwrap();
        assert_eq!(after["customer"], "erin");
        assert_eq!(after["id"], 5);
        assert_eq!(after["total"], 5000.0);
        assert_eq!(
            ev.timestamp.unwrap(),
            chrono::DateTime::from_timestamp_millis(1749600000123).unwrap()
        );
        assert_eq!(ev.upstream_id, None);
    }

    #[test]
    fn parses_update_frame_with_before_and_after() {
        let events = parse_chunked(UPDATE_FRAME.as_bytes(), UPDATE_FRAME.len());
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.change, ChangeType::Updated);
        assert_eq!(ev.before.as_ref().unwrap()["total"], 1500.0);
        assert_eq!(ev.after.as_ref().unwrap()["total"], 1800.0);
    }

    #[test]
    fn parses_delete_frame() {
        let events = parse_chunked(DELETE_FRAME.as_bytes(), DELETE_FRAME.len());
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.change, ChangeType::Deleted);
        assert_eq!(ev.before.as_ref().unwrap()["customer"], "erin");
        assert_eq!(ev.after, None);
    }

    #[test]
    fn incremental_parsing_is_chunk_boundary_independent() {
        let mut combined = String::new();
        combined.push_str(ADD_FRAME);
        combined.push_str(KEEPALIVE_COMMENT);
        combined.push_str(UPDATE_FRAME);
        combined.push_str(HEARTBEAT_FRAME);
        combined.push_str(DELETE_FRAME);
        combined.push_str(MULTI_DIFF_FRAME);
        let whole = parse_chunked(combined.as_bytes(), combined.len());
        // ADD + UPDATE + DELETE + 2x ADD from the multi-diff batch.
        assert_eq!(whole.len(), 5);
        assert_eq!(whole[3].after.as_ref().unwrap()["customer"], "alice");
        assert_eq!(whole[4].after.as_ref().unwrap()["customer"], "carol");
        for chunk_size in [1, 2, 3, 5, 7, 16, 64] {
            let chunked = parse_chunked(combined.as_bytes(), chunk_size);
            assert_eq!(chunked, whole, "chunk size {chunk_size}");
        }
    }

    #[test]
    fn heartbeats_and_comments_produce_no_events() {
        let input = format!("{KEEPALIVE_COMMENT}{HEARTBEAT_FRAME}{KEEPALIVE_COMMENT}");
        assert!(parse_chunked(input.as_bytes(), 4).is_empty());
    }

    #[test]
    fn malformed_frame_is_skipped_and_stream_continues() {
        let input = format!("data: {{this is not json\n\n{ADD_FRAME}");
        let events = parse_chunked(input.as_bytes(), input.len());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].change, ChangeType::Added);
    }

    #[test]
    fn unknown_diff_types_and_noops_are_skipped_aggregations_tolerated() {
        let payload = "data: {\"queryId\":\"q\",\"results\":[\
            {\"type\":\"noop\"},\
            {\"type\":\"SOMETHING_NEW\",\"data\":{}},\
            {\"after\":{\"count\":2},\"before\":null,\"type\":\"aggregation\"},\
            {\"after\":{\"id\":1},\"before\":{\"id\":1},\"data\":{\"id\":1},\"grouping_keys\":[\"id\"],\"type\":\"UPDATE\",\"futureField\":true}\
            ],\"timestamp\":1749600000000}\n\n";
        let events = parse_chunked(payload.as_bytes(), payload.len());
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].change, ChangeType::Updated); // aggregation
        assert_eq!(events[0].before, None); // null before -> None
        assert_eq!(events[0].after.as_ref().unwrap()["count"], 2);
        assert_eq!(events[1].change, ChangeType::Updated);
    }

    #[test]
    fn crlf_lines_and_event_id_fields_are_tolerated() {
        let input = format!(
            "event: message\r\nid: 42\r\nretry: 100\r\n{}",
            ADD_FRAME.replace('\n', "\r\n")
        );
        let events = parse_chunked(input.as_bytes(), 3);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].change, ChangeType::Added);
    }

    #[test]
    fn multiple_data_lines_join_with_newline() {
        let mut parser = SseParser::default();
        let frames = parser.push(b"data: first\ndata:second\n\n");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, "first\nsecond");
    }

    #[test]
    fn blank_line_without_data_dispatches_nothing() {
        let mut parser = SseParser::default();
        assert!(parser.push(b"event: ping\n\n\n\n").is_empty());
    }

    async fn serve_one_connection(listener: &tokio::net::TcpListener, body: &str) {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 2048];
        let _ = sock.read(&mut buf).await.unwrap();
        let resp = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{body}"
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
        sock.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sse_feed_delivers_and_reconnects_after_server_close() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            serve_one_connection(&listener, ADD_FRAME).await;
            serve_one_connection(&listener, DELETE_FRAME).await;
        });

        let (tx, mut rx) = mpsc::channel(8);
        let feed = tokio::spawn(run_drasi_sse_feed(format!("http://{addr}/events"), tx));

        let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.change, ChangeType::Added);

        // Second event arrives only after a reconnect (~1s backoff).
        let second = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.change, ChangeType::Deleted);

        drop(rx);
        let result = tokio::time::timeout(Duration::from_secs(5), feed)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok());
        server.await.unwrap();
    }
}
