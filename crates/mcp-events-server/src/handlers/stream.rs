//! `events/stream`: long-lived SSE response per the design sketch's
//! Push-Based Delivery section.
//!
//! Frame sequence: `notifications/events/active` (cursor + truncated), then a
//! replay of retained events from the request cursor, then a live tail fed by
//! the buffer's broadcast channel, with `notifications/events/heartbeat`
//! every `push.heartbeatIntervalMs`. Every notification carries
//! `_meta["io.modelcontextprotocol/subscriptionId"]` = the request id. On
//! broadcast lag the stream re-syncs from the buffer and emits a fresh
//! `active {truncated: true}`. A server-side close (broadcast channel closed)
//! writes the final `{"jsonrpc":"2.0","id":...,"result":{"_meta":{}}}` frame;
//! a client abort simply drops the stream.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use futures::Stream;
use mcp_events_wire::{
    DeliveryMode, EventsActiveParams, EventsHeartbeatParams, JsonRpcError, JsonRpcRequest,
    JsonRpcResponse, RequestId, StreamEventsParams, META_SUBSCRIPTION_ID, NOTIF_EVENTS_ACTIVE,
    NOTIF_EVENTS_EVENT, NOTIF_EVENTS_HEARTBEAT,
};
use serde_json::{json, Value};
use tokio::sync::broadcast::error::RecvError;

use crate::state::AppState;

/// Validates the subscription; invalid requests yield an immediate JSON-RPC
/// error (no stream is opened), valid ones an SSE response.
pub async fn handle(
    state: Arc<AppState>,
    id: RequestId,
    params: Option<Value>,
) -> Result<Response, JsonRpcError> {
    let p: StreamEventsParams = super::parse_params(params)?;
    let def = state.registry.get(&p.name).ok_or_else(|| {
        JsonRpcError::not_found("event", format!("unknown event type \"{}\"", p.name))
    })?;
    if !def.delivery.contains(&DeliveryMode::Push) {
        return Err(JsonRpcError::unsupported("deliveryMode", "push"));
    }
    crate::mapping::validate_event_params(&state.filters, &p.name, p.params.as_ref())?;
    Ok(Sse::new(event_stream(state, id, p)).into_response())
}

fn data_frame<T: serde::Serialize>(msg: &T) -> Option<Event> {
    match serde_json::to_string(msg) {
        // Serialized JSON-RPC is single-line; safe for one `data:` field.
        Ok(s) => Some(Event::default().data(s)),
        Err(error) => {
            tracing::error!(%error, "failed to serialize SSE frame; dropping it");
            None
        }
    }
}

fn notif<T: serde::Serialize>(method: &str, params: &T) -> Option<Event> {
    let params = match serde_json::to_value(params) {
        Ok(v) => v,
        Err(error) => {
            tracing::error!(%error, method, "failed to serialize notification params");
            return None;
        }
    };
    data_frame(&JsonRpcRequest::notification(method, Some(params)))
}

/// Extracts the seq component of an engine cursor (`"<epoch>:<seq>"`, pinned
/// by ARCHITECTURE.md) so replay and the live broadcast can be stitched
/// together without duplicates.
fn seq_of(cursor: &str) -> u64 {
    cursor
        .split_once(':')
        .and_then(|(_, s)| s.parse().ok())
        .unwrap_or(0)
}

fn event_stream(
    state: Arc<AppState>,
    id: RequestId,
    p: StreamEventsParams,
) -> impl Stream<Item = Result<Event, Infallible>> {
    async_stream::stream! {
        let meta = json!({ META_SUBSCRIPTION_ID: id });
        let filter = state.filters.get(&p.name).cloned();

        // Subscribe to the live channel before probing the buffer so no event
        // can fall between replay and tail; overlap is deduped by seq below.
        let mut rx = state.buffer.live(&p.name);

        // maxEvents: 0 probe: effective start position + truncation + whether
        // a backlog exists, without consuming events.
        let probe = state.buffer.read(
            &p.name,
            p.cursor.as_deref(),
            p.max_age_ms,
            Some(0),
            p.params.as_ref(),
            filter.as_deref(),
        );
        let mut cur = probe.cursor;
        let active = EventsActiveParams {
            cursor: Some(cur.clone()),
            truncated: probe.truncated,
            meta: meta.clone(),
        };
        if let Some(ev) = notif(NOTIF_EVENTS_ACTIVE, &active) {
            yield Ok::<Event, Infallible>(ev);
        }

        // Replay retained backlog one event per read so each push frame
        // carries its own position-after cursor.
        let mut pending = probe.has_more;
        while pending {
            let read = state.buffer.read(
                &p.name,
                Some(&cur),
                p.max_age_ms,
                Some(1),
                p.params.as_ref(),
                filter.as_deref(),
            );
            pending = read.has_more;
            cur = read.cursor;
            let Some(mut occ) = read.events.into_iter().next() else {
                break;
            };
            occ.cursor = Some(Some(cur.clone()));
            occ.meta = Some(meta.clone());
            if let Some(ev) = notif(NOTIF_EVENTS_EVENT, &occ) {
                yield Ok(ev);
            }
        }
        let mut last_seq = seq_of(&cur);

        let hb_period = Duration::from_millis(state.config.push.heartbeat_interval_ms.max(1));
        let mut heartbeat = tokio::time::interval(hb_period);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await; // consume the immediate first tick

        loop {
            // Frames are built inside select arms and yielded after, since
            // `yield` cannot live inside `tokio::select!`.
            let mut frames: Vec<Event> = Vec::new();
            let mut closed = false;
            tokio::select! {
                received = rx.recv() => match received {
                    Ok(live) => {
                        if live.seq > last_seq {
                            last_seq = live.seq;
                            // Cursor advances past filtered events too, so
                            // heartbeats keep the client's position fresh.
                            if let Some(c) = live.occurrence.cursor.clone().flatten() {
                                cur = c;
                            }
                            let deliver = match (p.params.as_ref(), filter.as_deref()) {
                                (Some(sub_params), Some(f)) => f(sub_params, &live.occurrence.data),
                                _ => true,
                            };
                            if deliver {
                                let mut occ = live.occurrence;
                                occ.meta = Some(meta.clone());
                                if let Some(ev) = notif(NOTIF_EVENTS_EVENT, &occ) {
                                    frames.push(ev);
                                }
                            }
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, name = %p.name, "events/stream receiver lagged; re-syncing from buffer");
                        let active = EventsActiveParams {
                            cursor: Some(cur.clone()),
                            truncated: true,
                            meta: meta.clone(),
                        };
                        if let Some(ev) = notif(NOTIF_EVENTS_ACTIVE, &active) {
                            frames.push(ev);
                        }
                        let mut more = true;
                        while more {
                            let read = state.buffer.read(
                                &p.name,
                                Some(&cur),
                                None,
                                Some(1),
                                p.params.as_ref(),
                                filter.as_deref(),
                            );
                            more = read.has_more;
                            cur = read.cursor;
                            let Some(mut occ) = read.events.into_iter().next() else {
                                break;
                            };
                            occ.cursor = Some(Some(cur.clone()));
                            occ.meta = Some(meta.clone());
                            if let Some(ev) = notif(NOTIF_EVENTS_EVENT, &occ) {
                                frames.push(ev);
                            }
                        }
                        last_seq = last_seq.max(seq_of(&cur));
                    }
                    Err(RecvError::Closed) => {
                        closed = true;
                    }
                },
                _ = heartbeat.tick() => {
                    let hb = EventsHeartbeatParams {
                        cursor: Some(cur.clone()),
                        meta: meta.clone(),
                    };
                    if let Some(ev) = notif(NOTIF_EVENTS_HEARTBEAT, &hb) {
                        frames.push(ev);
                    }
                }
            }
            for ev in frames {
                yield Ok(ev);
            }
            if closed {
                // Server-side termination: final response frame, then EOF.
                let result = JsonRpcResponse::success(id.clone(), json!({ "_meta": {} }));
                if let Some(ev) = data_frame(&result) {
                    yield Ok(ev);
                }
                break;
            }
        }
    }
}
