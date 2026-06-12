//! Background webhook delivery worker (design sketch §Webhook Event
//! Delivery; ARCHITECTURE.md):
//!
//! - one broadcast receiver per event type fans live events out to
//!   per-subscription FIFO queues; broadcast lag queues a `gap` control
//!   envelope with a fresh cursor;
//! - one task per subscription delivers its queue sequentially: backfill
//!   from the stored watermark first (subscribe-time replay), then queued
//!   live events; the payload `cursor` is the safe watermark — with FIFO
//!   delivery, the event's own position, since everything lower is already
//!   acked or abandoned;
//! - each POST retries with 1s/5s/25s backoff, every failed attempt is
//!   recorded (suspension after `suspendAfterFailures` consecutive
//!   failures), then the event is abandoned and the watermark advances;
//!   `413` is non-retryable;
//! - a 5s sweep expires lapsed subscriptions and reconciles delivery tasks.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use chrono::Utc;
use mcp_events_engine::{error_category, ParamFilter, SubKey, WebhookSub};
use mcp_events_wire::{EventOccurrence, WebhookControlBody};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};
use url::Url;

use super::{signer, ssrf};
use crate::state::AppState;

const RETRY_BACKOFF: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(25),
];
const TTL_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
/// How often a paused delivery task re-checks a suspended subscription.
const SUSPEND_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Bound on each subscription's delivery queue; overflow is surfaced as a
/// `gap` control envelope rather than unbounded buffering.
const DELIVERY_QUEUE_CAPACITY: usize = 1024;

#[derive(Clone)]
enum QueueItem {
    Event { seq: u64, occurrence: EventOccurrence },
    Gap { seq: u64, cursor: String },
}

struct SubHandle {
    key: SubKey,
    tx: mpsc::Sender<QueueItem>,
}

#[derive(Clone, Default)]
struct Tasks(Arc<Mutex<HashMap<String, SubHandle>>>);

impl Tasks {
    fn lock(&self) -> MutexGuard<'_, HashMap<String, SubHandle>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn remove(&self, sub_id: &str) {
        self.lock().remove(sub_id);
    }
}

pub fn spawn_delivery_worker(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let tasks = Tasks::default();
        for def in state.registry.list() {
            tokio::spawn(fan_out(state.clone(), def.name.clone(), tasks.clone()));
        }
        let mut tick = tokio::time::interval(TTL_SWEEP_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            for sub in state.subs.expire_lapsed(Utc::now()) {
                tracing::info!(sub_id = %sub.id, event = %sub.key.name, "webhook subscription lapsed (TTL)");
                tasks.remove(&sub.id);
            }
            reconcile(&state, &tasks);
        }
    })
}

/// Drops handles for removed/finished subscriptions and ensures every
/// verified subscription has a delivery task (covers subscribe-time replay
/// when no live event arrives to trigger lazy creation).
fn reconcile(state: &Arc<AppState>, tasks: &Tasks) {
    tasks
        .lock()
        .retain(|_, h| !h.tx.is_closed() && state.subs.get(&h.key).is_some());
    for def in state.registry.list() {
        for sub in state.subs.list_for_event(&def.name) {
            if sub.verified {
                ensure_task(state, tasks, &sub);
            }
        }
    }
}

fn ensure_task(
    state: &Arc<AppState>,
    tasks: &Tasks,
    sub: &WebhookSub,
) -> mpsc::Sender<QueueItem> {
    let mut map = tasks.lock();
    if let Some(h) = map.get(&sub.id) {
        if !h.tx.is_closed() {
            return h.tx.clone();
        }
    }
    let (tx, rx) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
    map.insert(
        sub.id.clone(),
        SubHandle {
            key: sub.key.clone(),
            tx: tx.clone(),
        },
    );
    tokio::spawn(run_delivery(
        state.clone(),
        sub.key.clone(),
        sub.id.clone(),
        sub.params.clone(),
        rx,
    ));
    tx
}

/// Per event type: consume `buffer.live()` and enqueue to every verified,
/// active subscription's FIFO queue. Unverified subscriptions are skipped —
/// the sketch forbids delivery before endpoint verification; events emitted
/// in the handshake window are recovered by the task's watermark backfill.
///
/// Memory is bounded two ways: suspended (`active == false`) subscriptions
/// are not enqueued to at all (their delivery task is parked and would never
/// drain), and each queue is bounded at `DELIVERY_QUEUE_CAPACITY`. In both
/// cases the skipped span is surfaced to the receiver as a `gap` control
/// envelope with a fresh cursor once the queue accepts again — the same
/// resync contract as broadcast lag.
async fn fan_out(state: Arc<AppState>, name: String, tasks: Tasks) {
    let mut rx = state.buffer.live(&name);
    // Subscriptions that missed one or more items (suspension or full
    // queue) and are owed a gap envelope before further deliveries.
    let mut pending_gap: HashSet<String> = HashSet::new();
    loop {
        let item = match rx.recv().await {
            Ok(live) => QueueItem::Event {
                seq: live.seq,
                occurrence: live.occurrence,
            },
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                tracing::warn!(event = %name, missed, "delivery fan-out lagged; queueing gap envelopes");
                let cursor = state.buffer.current_cursor(&name);
                let seq = parse_seq(&cursor).unwrap_or(u64::MAX);
                QueueItem::Gap { seq, cursor }
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        for sub in state.subs.list_for_event(&name) {
            if !sub.verified {
                continue;
            }
            if !sub.active {
                // Parked delivery task; enqueuing would only grow the queue.
                // Owe a gap so the receiver learns of the skipped span after
                // reactivation.
                pending_gap.insert(sub.id.clone());
                continue;
            }
            let tx = ensure_task(&state, &tasks, &sub);
            if pending_gap.contains(&sub.id) {
                // The current item falls inside the skipped span; the gap's
                // fresh head cursor covers it.
                let cursor = state.buffer.current_cursor(&name);
                let seq = parse_seq(&cursor).unwrap_or(u64::MAX);
                if try_enqueue(&state, &tasks, &sub, &tx, QueueItem::Gap { seq, cursor }) {
                    pending_gap.remove(&sub.id);
                }
                continue;
            }
            if !try_enqueue(&state, &tasks, &sub, &tx, item.clone()) {
                pending_gap.insert(sub.id.clone());
            }
        }
    }
}

/// Non-blocking enqueue; recreates the delivery task once if it has exited
/// (e.g. re-subscribe after removal). Returns false when the queue is full —
/// the caller owes the subscription a gap envelope.
fn try_enqueue(
    state: &Arc<AppState>,
    tasks: &Tasks,
    sub: &WebhookSub,
    tx: &mpsc::Sender<QueueItem>,
    item: QueueItem,
) -> bool {
    match tx.try_send(item) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!(sub_id = %sub.id, "delivery queue full; deferring to gap envelope");
            false
        }
        Err(mpsc::error::TrySendError::Closed(item)) => {
            tasks.remove(&sub.id);
            let tx = ensure_task(state, tasks, sub);
            match tx.try_send(item) {
                Ok(()) => true,
                Err(_) => false,
            }
        }
    }
}

/// One subscription's sequential delivery loop.
async fn run_delivery(
    state: Arc<AppState>,
    key: SubKey,
    sub_id: String,
    params: Value,
    mut rx: mpsc::Receiver<QueueItem>,
) {
    let name = key.name.clone();
    let Ok(url) = Url::parse(&key.url) else {
        tracing::error!(sub_id = %sub_id, url = %key.url, "stored callback URL does not parse; delivery disabled");
        return;
    };
    let filter: Option<Arc<ParamFilter>> = state.filters.get(&name).cloned();
    let params_opt: Option<Value> = if params.is_null() { None } else { Some(params) };
    let Some(sub) = state.subs.get(&key) else {
        return;
    };
    let mut pos = sub
        .cursor
        .unwrap_or_else(|| state.buffer.current_cursor(&name));

    // Backfill: replay retained events from the persisted watermark (covers
    // subscribe-time replay and the verification-handshake window).
    loop {
        let read = state.buffer.read(
            &name,
            Some(&pos),
            None,
            Some(1),
            params_opt.as_ref(),
            filter.as_deref(),
        );
        if read.truncated {
            // The watermark fell out of retention: tell the endpoint via a
            // gap envelope carrying the reset position, then resume from it.
            let reset = state
                .buffer
                .read(&name, Some(&pos), None, Some(0), params_opt.as_ref(), filter.as_deref())
                .cursor;
            if deliver_control(
                &state,
                &key,
                &sub_id,
                &url,
                WebhookControlBody::Gap {
                    cursor: Some(reset.clone()),
                },
            )
            .await
            .is_none()
            {
                return;
            }
            pos = reset;
            state.subs.update_cursor(&sub_id, Some(pos.clone()));
            continue;
        }
        match read.events.into_iter().next() {
            Some(mut occurrence) => {
                occurrence.cursor = Some(Some(read.cursor.clone()));
                if deliver_event(&state, &key, &sub_id, &url, &occurrence)
                    .await
                    .is_none()
                {
                    return;
                }
                pos = read.cursor;
                state.subs.update_cursor(&sub_id, Some(pos.clone()));
            }
            None => {
                if read.cursor != pos {
                    // Only filtered events remained; the watermark may still
                    // advance past them.
                    pos = read.cursor;
                    state.subs.update_cursor(&sub_id, Some(pos.clone()));
                }
                break;
            }
        }
    }

    let mut last_done = parse_seq(&pos).unwrap_or(0);
    while let Some(item) = rx.recv().await {
        if state.subs.get(&key).is_none() {
            return;
        }
        match item {
            QueueItem::Event { seq, occurrence } => {
                if seq <= last_done {
                    continue; // already covered by backfill
                }
                let cursor = occurrence
                    .cursor
                    .clone()
                    .flatten()
                    .unwrap_or_else(|| state.buffer.current_cursor(&name));
                let filtered_out = match (params_opt.as_ref(), filter.as_deref()) {
                    (Some(p), Some(f)) => !f(p, &occurrence.data),
                    _ => false,
                };
                if !filtered_out {
                    let mut occurrence = occurrence;
                    occurrence.cursor = Some(Some(cursor.clone()));
                    if deliver_event(&state, &key, &sub_id, &url, &occurrence)
                        .await
                        .is_none()
                    {
                        return;
                    }
                }
                last_done = seq;
                state.subs.update_cursor(&sub_id, Some(cursor));
            }
            QueueItem::Gap { seq, cursor } => {
                if seq <= last_done {
                    continue;
                }
                if deliver_control(
                    &state,
                    &key,
                    &sub_id,
                    &url,
                    WebhookControlBody::Gap {
                        cursor: Some(cursor.clone()),
                    },
                )
                .await
                .is_none()
                {
                    return;
                }
                last_done = seq;
                state.subs.update_cursor(&sub_id, Some(cursor));
            }
        }
    }
}

/// `Some(())` = acked or abandoned (advance the watermark); `None` = the
/// subscription is gone and the caller must stop.
async fn deliver_event(
    state: &Arc<AppState>,
    key: &SubKey,
    sub_id: &str,
    url: &Url,
    occurrence: &EventOccurrence,
) -> Option<()> {
    let body = match serde_json::to_vec(occurrence) {
        Ok(b) => b,
        Err(error) => {
            tracing::error!(sub_id, %error, "serializing event occurrence; skipping");
            return Some(());
        }
    };
    deliver(state, key, sub_id, url, &occurrence.event_id, &body).await
}

async fn deliver_control(
    state: &Arc<AppState>,
    key: &SubKey,
    sub_id: &str,
    url: &Url,
    body: WebhookControlBody,
) -> Option<()> {
    let kind = match &body {
        WebhookControlBody::Gap { .. } => "gap",
        WebhookControlBody::Terminated { .. } => "terminated",
        WebhookControlBody::Verification { .. } => "verification",
    };
    let bytes = match serde_json::to_vec(&body) {
        Ok(b) => b,
        Err(error) => {
            tracing::error!(sub_id, %error, "serializing control envelope; skipping");
            return Some(());
        }
    };
    deliver(state, key, sub_id, url, &signer::control_msg_id(kind), &bytes).await
}

enum Attempt {
    Acked,
    Fail {
        category: &'static str,
        retryable: bool,
    },
}

/// POSTs one message with the retry/suspension policy. Returns `Some(())`
/// when the message is acked or abandoned, `None` when the subscription no
/// longer exists. Delivery pauses (without burning attempts) while the
/// subscription is suspended and resumes when a refresh reactivates it.
async fn deliver(
    state: &Arc<AppState>,
    key: &SubKey,
    sub_id: &str,
    url: &Url,
    msg_id: &str,
    body: &[u8],
) -> Option<()> {
    let suspend_after = state.config.webhook.suspend_after_failures;
    let mut attempt = 0usize;
    loop {
        // Fetch the secret fresh per attempt (rotation) and hold while
        // suspended.
        let secret = loop {
            match state.subs.get(key) {
                None => return None,
                Some(s) if s.active => break s.secret,
                Some(_) => tokio::time::sleep(SUSPEND_POLL_INTERVAL).await,
            }
        };
        match attempt_post(state, url, sub_id, msg_id, &secret, body).await {
            Attempt::Acked => {
                state.subs.mark_delivery_ok(sub_id, Utc::now());
                return Some(());
            }
            Attempt::Fail {
                category,
                retryable,
            } => {
                state
                    .subs
                    .mark_delivery_failed(sub_id, category, Utc::now(), suspend_after);
                if !retryable || attempt >= RETRY_BACKOFF.len() {
                    tracing::warn!(sub_id, msg_id, category, retryable, "abandoning webhook delivery");
                    return Some(());
                }
                tokio::time::sleep(RETRY_BACKOFF[attempt]).await;
                attempt += 1;
            }
        }
    }
}

async fn attempt_post(
    state: &Arc<AppState>,
    url: &Url,
    sub_id: &str,
    msg_id: &str,
    secret: &[u8],
    body: &[u8],
) -> Attempt {
    let client = match ssrf::client_for(state, url).await {
        Ok(c) => c,
        Err(error) => {
            tracing::warn!(sub_id, %error, "webhook delivery blocked before connect");
            return Attempt::Fail {
                category: error.category(),
                retryable: true,
            };
        }
    };
    match signer::signed_post(&client, url.as_str(), sub_id, msg_id, secret, body.to_vec())
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                return Attempt::Acked;
            }
            tracing::debug!(sub_id, msg_id, %status, "webhook delivery got non-2xx");
            Attempt::Fail {
                // 1xx/3xx have no sketch-defined category; folded into
                // http_4xx (recorded as a spec-gap finding).
                category: if status.is_server_error() {
                    error_category::HTTP_5XX
                } else {
                    error_category::HTTP_4XX
                },
                retryable: status != reqwest::StatusCode::PAYLOAD_TOO_LARGE,
            }
        }
        Err(error) => {
            let category = ssrf::classify_send_error(&error);
            tracing::debug!(sub_id, msg_id, %error, category, "webhook delivery send failed");
            Attempt::Fail {
                category,
                retryable: true,
            }
        }
    }
}

fn parse_seq(cursor: &str) -> Option<u64> {
    cursor.split_once(':').and_then(|(_, seq)| seq.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_seq_reads_the_internal_cursor_format() {
        assert_eq!(parse_seq("12345:7"), Some(7));
        assert_eq!(parse_seq("0:0"), Some(0));
        assert_eq!(parse_seq("garbage"), None);
        assert_eq!(parse_seq("1:x"), None);
    }
}
