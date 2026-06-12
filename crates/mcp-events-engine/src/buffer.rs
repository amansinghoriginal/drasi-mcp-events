//! Per-event-type ring buffer with `<epoch>:<seq>` cursors and live broadcast
//! fan-out (design sketch §Cursor Lifecycle, §Emit-only event types).
//!
//! Cursor model: `epoch` is a random `u64` chosen at buffer construction
//! (process-scoped — a restart invalidates all cursors); `seq` is a per-event-
//! type counter starting at 1. A cursor value is the seq of the last consumed
//! event ("position after"), `0` = before the first event.
//!
//! Lifecycle decisions taken where the sketch is silent (see SPEC-GAPS
//! findings): a foreign-epoch or unparseable cursor replays from the oldest
//! retained event (at-least-once bias; dedup by `eventId` absorbs duplicates);
//! a same-epoch cursor beyond the current head resets to the head with no
//! replay; both set `truncated: true`. The `maxAgeMs` floor sets `truncated`
//! only when retained events were actually skipped by it.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Duration, Utc};
use mcp_events_wire::EventOccurrence;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::iso8601;

/// An event handed to the buffer by the feed/mapping layer.
#[derive(Clone, Debug)]
pub struct EmittedEvent {
    pub name: String,
    pub event_id: String,
    pub timestamp: DateTime<Utc>,
    pub data: Value,
}

/// Param-filtering hook: `(subscription params, event data) -> deliver?`.
/// Applied by `read()` only when both a filter and params are supplied;
/// absent params means an unfiltered stream.
pub type ParamFilter = dyn Fn(&Value, &Value) -> bool + Send + Sync;

#[derive(Clone, Debug)]
pub struct BufferConfig {
    /// Ring capacity per event type; older events are evicted beyond it.
    pub max_events_per_type: usize,
    /// Retention window; events older than this are evicted on emit/read.
    pub max_age: Option<Duration>,
}

/// Event as broadcast to live (push/webhook) receivers; `occurrence.cursor`
/// carries `Some(Some("<epoch>:<seq>"))`.
#[derive(Clone, Debug)]
pub struct LiveEvent {
    pub seq: u64,
    pub occurrence: EventOccurrence,
}

/// Result of a poll-style `read()`. Occurrences carry no per-event cursor
/// (poll places the cursor at the response level).
#[derive(Clone, Debug)]
pub struct ReadResult {
    pub events: Vec<EventOccurrence>,
    pub cursor: String,
    pub truncated: bool,
    pub has_more: bool,
}

struct Stored {
    seq: u64,
    event_id: String,
    timestamp: DateTime<Utc>,
    data: Value,
}

struct TypeState {
    last_seq: u64,
    entries: VecDeque<Stored>,
    tx: broadcast::Sender<LiveEvent>,
}

impl TypeState {
    fn new(chan_capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(chan_capacity);
        Self {
            last_seq: 0,
            entries: VecDeque::new(),
            tx,
        }
    }
}

struct Inner {
    epoch: u64,
    cfg: BufferConfig,
    chan_capacity: usize,
    types: Mutex<HashMap<String, TypeState>>,
}

/// Thread-safe, cheap-clone event buffer.
#[derive(Clone)]
pub struct EventBuffer(Arc<Inner>);

fn parse_cursor(s: &str) -> Option<(u64, u64)> {
    let (epoch, seq) = s.split_once(':')?;
    Some((epoch.parse().ok()?, seq.parse().ok()?))
}

fn floor_from_max_age_ms(now: DateTime<Utc>, ms: u64) -> DateTime<Utc> {
    // `ms` comes straight off the wire (events/poll, events/stream,
    // events/subscribe); a huge value must clamp, not overflow chrono's
    // datetime range and panic the handler.
    let delta = Duration::milliseconds(i64::try_from(ms).unwrap_or(i64::MAX));
    now.checked_sub_signed(delta).unwrap_or(DateTime::<Utc>::MIN_UTC)
}

impl EventBuffer {
    pub fn new(cfg: BufferConfig) -> Self {
        let chan_capacity = cfg.max_events_per_type.clamp(1, 1024);
        Self(Arc::new(Inner {
            epoch: rand::random(),
            cfg,
            chan_capacity,
            types: Mutex::new(HashMap::new()),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, TypeState>> {
        self.0.types.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn cursor_str(&self, seq: u64) -> String {
        format!("{}:{}", self.0.epoch, seq)
    }

    fn prune(cfg: &BufferConfig, st: &mut TypeState, now: DateTime<Utc>) {
        while st.entries.len() > cfg.max_events_per_type {
            st.entries.pop_front();
        }
        if let Some(max_age) = cfg.max_age {
            let cutoff = now
                .checked_sub_signed(max_age)
                .unwrap_or(DateTime::<Utc>::MIN_UTC);
            while st.entries.front().is_some_and(|e| e.timestamp < cutoff) {
                st.entries.pop_front();
            }
        }
    }

    /// Assigns the next seq for the event's type, stores it (subject to
    /// retention), and broadcasts it to live receivers. Returns the seq.
    pub fn emit(&self, ev: EmittedEvent) -> u64 {
        let now = Utc::now();
        let mut types = self.lock();
        let st = types
            .entry(ev.name.clone())
            .or_insert_with(|| TypeState::new(self.0.chan_capacity));
        st.last_seq += 1;
        let seq = st.last_seq;
        st.entries.push_back(Stored {
            seq,
            event_id: ev.event_id.clone(),
            timestamp: ev.timestamp,
            data: ev.data.clone(),
        });
        Self::prune(&self.0.cfg, st, now);
        let occurrence = EventOccurrence {
            event_id: ev.event_id,
            name: ev.name,
            timestamp: iso8601(ev.timestamp),
            data: ev.data,
            cursor: Some(Some(self.cursor_str(seq))),
            meta: None,
        };
        // No receivers is fine — poll-only operation.
        let _ = st.tx.send(LiveEvent { seq, occurrence });
        seq
    }

    /// Poll-style read implementing the cursor lifecycle (see module docs).
    pub fn read(
        &self,
        name: &str,
        cursor: Option<&str>,
        max_age_ms: Option<u64>,
        max_events: Option<u32>,
        params: Option<&Value>,
        filter: Option<&ParamFilter>,
    ) -> ReadResult {
        let now = Utc::now();
        let mut types = self.lock();
        let st = types
            .entry(name.to_owned())
            .or_insert_with(|| TypeState::new(self.0.chan_capacity));
        Self::prune(&self.0.cfg, st, now);

        // Null cursor = "start from now": no events, fresh cursor, no
        // truncation; maxAgeMs is ignored (sketch §Bounding replay).
        let Some(cursor) = cursor else {
            return ReadResult {
                events: Vec::new(),
                cursor: self.cursor_str(st.last_seq),
                truncated: false,
                has_more: false,
            };
        };

        let mut truncated = false;
        let oldest = st.entries.front().map(|e| e.seq);
        let start_after = match parse_cursor(cursor) {
            Some((epoch, seq)) if epoch == self.0.epoch && seq <= st.last_seq => match oldest {
                // Events in (seq, oldest) were evicted: gap.
                Some(o) if seq + 1 < o => {
                    truncated = true;
                    o - 1
                }
                // Buffer fully evicted but events were emitted past seq.
                None if seq < st.last_seq => {
                    truncated = true;
                    st.last_seq
                }
                _ => seq,
            },
            // Same epoch but beyond head: not a servable position; reset to
            // head rather than replaying events the cursor claims to be past.
            Some((epoch, _)) if epoch == self.0.epoch => {
                truncated = true;
                st.last_seq
            }
            // Foreign epoch (e.g. pre-restart) or unparseable: replay from
            // the oldest retained event.
            _ => {
                truncated = true;
                oldest.map(|o| o - 1).unwrap_or(st.last_seq)
            }
        };

        let floor = max_age_ms.map(|ms| floor_from_max_age_ms(now, ms));
        let cap = max_events.map(|n| n as usize);
        let mut events = Vec::new();
        let mut pos = start_after;
        let mut has_more = false;
        for e in st.entries.iter().filter(|e| e.seq > start_after) {
            if floor.is_some_and(|f| e.timestamp < f) {
                // Skipped by the replay floor: events were dropped, but the
                // cursor may safely advance past them.
                truncated = true;
                pos = e.seq;
                continue;
            }
            if let (Some(p), Some(f)) = (params, filter) {
                if !f(p, &e.data) {
                    pos = e.seq;
                    continue;
                }
            }
            if cap.is_some_and(|c| events.len() >= c) {
                // A further matching event exists beyond the batch cap; the
                // cursor must not advance past it.
                has_more = true;
                break;
            }
            events.push(EventOccurrence {
                event_id: e.event_id.clone(),
                name: name.to_owned(),
                timestamp: iso8601(e.timestamp),
                data: e.data.clone(),
                cursor: None, // poll carries the cursor at the response level
                meta: None,
            });
            pos = e.seq;
        }

        ReadResult {
            events,
            cursor: self.cursor_str(pos),
            truncated,
            has_more,
        }
    }

    /// Subscribes to live events for one event type. Only events emitted
    /// after this call are received; filtering is the receiver's concern.
    pub fn live(&self, name: &str) -> broadcast::Receiver<LiveEvent> {
        let mut types = self.lock();
        types
            .entry(name.to_owned())
            .or_insert_with(|| TypeState::new(self.0.chan_capacity))
            .tx
            .subscribe()
    }

    /// Current head position (`"<epoch>:<last_seq>"`) for an event type;
    /// `"<epoch>:0"` for a type with no emissions yet.
    pub fn current_cursor(&self, name: &str) -> String {
        let types = self.lock();
        let last = types.get(name).map(|s| s.last_seq).unwrap_or(0);
        self.cursor_str(last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn buf(cap: usize) -> EventBuffer {
        EventBuffer::new(BufferConfig {
            max_events_per_type: cap,
            max_age: None,
        })
    }

    fn ev(name: &str, id: &str, ts: DateTime<Utc>) -> EmittedEvent {
        EmittedEvent {
            name: name.to_owned(),
            event_id: id.to_owned(),
            timestamp: ts,
            data: json!({ "id": id }),
        }
    }

    fn epoch_of(b: &EventBuffer) -> u64 {
        let c = b.current_cursor("__epoch_probe__");
        parse_cursor(&c).unwrap().0
    }

    fn ids(r: &ReadResult) -> Vec<&str> {
        r.events.iter().map(|e| e.event_id.as_str()).collect()
    }

    #[test]
    fn null_cursor_starts_from_now_on_empty_buffer() {
        let b = buf(10);
        let r = b.read("t", None, None, None, None, None);
        assert!(r.events.is_empty());
        assert!(!r.truncated);
        assert!(!r.has_more);
        assert_eq!(r.cursor, format!("{}:0", epoch_of(&b)));
        assert_eq!(r.cursor, b.current_cursor("t"));
    }

    #[test]
    fn null_cursor_skips_history_and_resumes_forward() {
        let b = buf(10);
        let now = Utc::now();
        b.emit(ev("t", "e1", now));
        b.emit(ev("t", "e2", now));
        let r = b.read("t", None, None, None, None, None);
        assert!(r.events.is_empty());
        assert!(!r.truncated);
        assert_eq!(r.cursor, format!("{}:2", epoch_of(&b)));

        b.emit(ev("t", "e3", now));
        let r2 = b.read("t", Some(&r.cursor), None, None, None, None);
        assert_eq!(ids(&r2), vec!["e3"]);
        assert!(!r2.truncated);
        assert!(!r2.has_more);
        assert_eq!(r2.cursor, format!("{}:3", epoch_of(&b)));
    }

    #[test]
    fn resume_returns_events_in_order_then_drains() {
        let b = buf(10);
        let c0 = b.read("t", None, None, None, None, None).cursor;
        let now = Utc::now();
        for i in 1..=3 {
            b.emit(ev("t", &format!("e{i}"), now));
        }
        let r = b.read("t", Some(&c0), None, None, None, None);
        assert_eq!(ids(&r), vec!["e1", "e2", "e3"]);
        assert!(!r.truncated);
        assert!(!r.has_more);
        let r2 = b.read("t", Some(&r.cursor), None, None, None, None);
        assert!(r2.events.is_empty());
        assert_eq!(r2.cursor, r.cursor);
        assert!(!r2.truncated);
    }

    #[test]
    fn poll_occurrences_have_no_cursor_and_iso8601_timestamps() {
        let b = buf(10);
        let c0 = b.read("t", None, None, None, None, None).cursor;
        b.emit(ev("t", "e1", Utc::now()));
        let r = b.read("t", Some(&c0), None, None, None, None);
        let occ = &r.events[0];
        assert_eq!(occ.cursor, None);
        assert_eq!(occ.name, "t");
        assert_eq!(occ.data, json!({"id": "e1"}));
        assert!(chrono::DateTime::parse_from_rfc3339(&occ.timestamp).is_ok());
        assert!(occ.timestamp.ends_with('Z'));
    }

    #[test]
    fn foreign_epoch_truncates_and_replays_from_oldest_retained() {
        let b = buf(10);
        let now = Utc::now();
        for i in 1..=3 {
            b.emit(ev("t", &format!("e{i}"), now));
        }
        let foreign = format!("{}:2", epoch_of(&b).wrapping_add(1));
        let r = b.read("t", Some(&foreign), None, None, None, None);
        assert!(r.truncated);
        assert_eq!(ids(&r), vec!["e1", "e2", "e3"]);
        assert_eq!(r.cursor, format!("{}:3", epoch_of(&b)));
    }

    #[test]
    fn foreign_epoch_on_empty_buffer_resets_to_now() {
        let b = buf(10);
        let foreign = format!("{}:7", epoch_of(&b).wrapping_add(1));
        let r = b.read("t", Some(&foreign), None, None, None, None);
        assert!(r.truncated);
        assert!(r.events.is_empty());
        assert_eq!(r.cursor, format!("{}:0", epoch_of(&b)));
    }

    #[test]
    fn unparseable_cursors_truncate() {
        let b = buf(10);
        b.emit(ev("t", "e1", Utc::now()));
        for bogus in ["garbage", "", "12", ":", "1:2:3", "abc:def", "1:def", "-1:2"] {
            let r = b.read("t", Some(bogus), None, None, None, None);
            assert!(r.truncated, "cursor {bogus:?} should truncate");
            assert_eq!(ids(&r), vec!["e1"], "cursor {bogus:?} replays from oldest");
        }
    }

    #[test]
    fn evicted_cursor_truncates_and_resumes_from_oldest() {
        let b = buf(3);
        let c0 = b.read("t", None, None, None, None, None).cursor; // E:0
        let now = Utc::now();
        for i in 1..=5 {
            b.emit(ev("t", &format!("e{i}"), now));
        }
        // Retained: e3..e5. Position 0 → e1, e2 evicted.
        let r = b.read("t", Some(&c0), None, None, None, None);
        assert!(r.truncated);
        assert_eq!(ids(&r), vec!["e3", "e4", "e5"]);
        assert_eq!(r.cursor, format!("{}:5", epoch_of(&b)));

        // Position 2 is contiguous with the oldest retained event (3): no gap.
        let c2 = format!("{}:2", epoch_of(&b));
        let r2 = b.read("t", Some(&c2), None, None, None, None);
        assert!(!r2.truncated);
        assert_eq!(ids(&r2), vec!["e3", "e4", "e5"]);

        // Position 1: event 2 was evicted → gap.
        let c1 = format!("{}:1", epoch_of(&b));
        let r3 = b.read("t", Some(&c1), None, None, None, None);
        assert!(r3.truncated);
        assert_eq!(ids(&r3), vec!["e3", "e4", "e5"]);
    }

    #[test]
    fn fully_age_evicted_buffer_with_stale_cursor_truncates_to_head() {
        let b = EventBuffer::new(BufferConfig {
            max_events_per_type: 10,
            max_age: Some(Duration::seconds(1)),
        });
        let c0 = b.read("t", None, None, None, None, None).cursor;
        let old = Utc::now() - Duration::hours(1);
        b.emit(ev("t", "e1", old));
        b.emit(ev("t", "e2", old));
        let r = b.read("t", Some(&c0), None, None, None, None);
        assert!(r.truncated);
        assert!(r.events.is_empty());
        assert_eq!(r.cursor, format!("{}:2", epoch_of(&b)));
        // At-head cursor stays clean.
        let r2 = b.read("t", Some(&r.cursor), None, None, None, None);
        assert!(!r2.truncated);
        assert!(r2.events.is_empty());
    }

    #[test]
    fn max_age_floor_skips_old_events_and_truncates() {
        let b = buf(10);
        let c0 = b.read("t", None, None, None, None, None).cursor;
        let now = Utc::now();
        b.emit(ev("t", "old", now - Duration::minutes(10)));
        b.emit(ev("t", "new", now - Duration::minutes(1)));
        let r = b.read("t", Some(&c0), Some(5 * 60 * 1000), None, None, None);
        assert!(r.truncated);
        assert_eq!(ids(&r), vec!["new"]);
        assert_eq!(r.cursor, format!("{}:2", epoch_of(&b)));
    }

    #[test]
    fn gigantic_max_age_ms_clamps_instead_of_panicking() {
        // Regression: maxAgeMs is attacker-controlled wire input; values
        // large enough to overflow chrono's datetime range previously
        // panicked the handler task (DateTime - TimeDelta overflow).
        let b = buf(10);
        let c0 = b.read("t", None, None, None, None, None).cursor;
        b.emit(ev("t", "e1", Utc::now()));
        for ms in [u64::MAX, 9_000_000_000_000_000_000, i64::MAX as u64] {
            let r = b.read("t", Some(&c0), Some(ms), None, None, None);
            // Floor clamps to the epoch floor => no constraint, no panic.
            assert!(!r.truncated);
            assert_eq!(ids(&r), vec!["e1"]);
        }
    }

    #[test]
    fn max_age_floor_without_skipped_events_does_not_truncate() {
        // Floor is past the cursor's wall-clock time, but no retained event
        // after the cursor falls below it — nothing was skipped (documented
        // reading of §Bounding replay vs §Gaps and truncated).
        let b = buf(10);
        let now = Utc::now();
        b.emit(ev("t", "old", now - Duration::minutes(10)));
        b.emit(ev("t", "new", now - Duration::minutes(1)));
        let c1 = format!("{}:1", epoch_of(&b)); // already past "old"
        let r = b.read("t", Some(&c1), Some(5 * 60 * 1000), None, None, None);
        assert!(!r.truncated);
        assert_eq!(ids(&r), vec!["new"]);
    }

    #[test]
    fn max_age_ignored_when_cursor_is_null() {
        let b = buf(10);
        b.emit(ev("t", "old", Utc::now() - Duration::hours(2)));
        let r = b.read("t", None, Some(1), None, None, None);
        assert!(!r.truncated);
        assert!(r.events.is_empty());
        assert_eq!(r.cursor, format!("{}:1", epoch_of(&b)));
    }

    #[test]
    fn max_age_floor_applies_after_invalid_cursor_reset() {
        let b = buf(10);
        let now = Utc::now();
        b.emit(ev("t", "old", now - Duration::minutes(10)));
        b.emit(ev("t", "new", now - Duration::minutes(1)));
        let foreign = format!("{}:0", epoch_of(&b).wrapping_add(1));
        let r = b.read("t", Some(&foreign), Some(5 * 60 * 1000), None, None, None);
        assert!(r.truncated);
        assert_eq!(ids(&r), vec!["new"]);
    }

    #[test]
    fn future_cursor_same_epoch_resets_to_head() {
        let b = buf(10);
        let now = Utc::now();
        b.emit(ev("t", "e1", now));
        b.emit(ev("t", "e2", now));
        let future = format!("{}:99", epoch_of(&b));
        let r = b.read("t", Some(&future), None, None, None, None);
        assert!(r.truncated);
        assert!(r.events.is_empty());
        assert_eq!(r.cursor, format!("{}:2", epoch_of(&b)));
    }

    #[test]
    fn max_events_paginates_with_has_more_and_intermediate_cursors() {
        let b = buf(10);
        let mut cursor = b.read("t", None, None, None, None, None).cursor;
        let now = Utc::now();
        for i in 1..=5 {
            b.emit(ev("t", &format!("e{i}"), now));
        }
        let mut seen = Vec::new();
        let mut rounds = 0;
        loop {
            let r = b.read("t", Some(&cursor), None, Some(2), None, None);
            assert!(!r.truncated);
            seen.extend(r.events.iter().map(|e| e.event_id.clone()));
            cursor = r.cursor;
            rounds += 1;
            if !r.has_more {
                break;
            }
            assert!(rounds < 10, "pagination must terminate");
        }
        assert_eq!(seen, vec!["e1", "e2", "e3", "e4", "e5"]);
        assert_eq!(rounds, 3);
        assert_eq!(cursor, format!("{}:5", epoch_of(&b)));
    }

    #[test]
    fn max_events_zero_returns_empty_batch_with_has_more() {
        let b = buf(10);
        let c0 = b.read("t", None, None, None, None, None).cursor;
        b.emit(ev("t", "e1", Utc::now()));
        let r = b.read("t", Some(&c0), None, Some(0), None, None);
        assert!(r.events.is_empty());
        assert!(r.has_more);
        assert_eq!(r.cursor, c0);
    }

    fn change_type_filter() -> Box<ParamFilter> {
        Box::new(|params: &Value, data: &Value| match params.get("changeType") {
            Some(want) => data.get("changeType") == Some(want),
            None => true,
        })
    }

    fn change_ev(name: &str, id: &str, kind: &str) -> EmittedEvent {
        EmittedEvent {
            name: name.to_owned(),
            event_id: id.to_owned(),
            timestamp: Utc::now(),
            data: json!({ "id": id, "changeType": kind }),
        }
    }

    #[test]
    fn filter_excludes_events_and_advances_cursor_past_them() {
        let b = buf(10);
        let c0 = b.read("t", None, None, None, None, None).cursor;
        b.emit(change_ev("t", "e1", "added"));
        b.emit(change_ev("t", "e2", "updated"));
        b.emit(change_ev("t", "e3", "added"));
        let filter = change_type_filter();
        let params = json!({ "changeType": "added" });
        let r = b.read("t", Some(&c0), None, None, Some(&params), Some(filter.as_ref()));
        assert_eq!(ids(&r), vec!["e1", "e3"]);
        assert!(!r.has_more);
        assert!(!r.truncated);
        assert_eq!(r.cursor, format!("{}:3", epoch_of(&b)));
    }

    #[test]
    fn filtered_events_do_not_count_toward_max_events() {
        let b = buf(10);
        let c0 = b.read("t", None, None, None, None, None).cursor;
        b.emit(change_ev("t", "e1", "added"));
        b.emit(change_ev("t", "e2", "updated"));
        b.emit(change_ev("t", "e3", "added"));
        b.emit(change_ev("t", "e4", "added"));
        let filter = change_type_filter();
        let params = json!({ "changeType": "added" });
        let r = b.read("t", Some(&c0), None, Some(2), Some(&params), Some(filter.as_ref()));
        assert_eq!(ids(&r), vec!["e1", "e3"]);
        assert!(r.has_more); // e4 still matches
        assert_eq!(r.cursor, format!("{}:3", epoch_of(&b)));
        let r2 = b.read("t", Some(&r.cursor), None, Some(2), Some(&params), Some(filter.as_ref()));
        assert_eq!(ids(&r2), vec!["e4"]);
        assert!(!r2.has_more);
    }

    #[test]
    fn trailing_filtered_events_do_not_set_has_more() {
        let b = buf(10);
        let c0 = b.read("t", None, None, None, None, None).cursor;
        b.emit(change_ev("t", "e1", "added"));
        b.emit(change_ev("t", "e2", "added"));
        b.emit(change_ev("t", "e3", "updated"));
        let filter = change_type_filter();
        let params = json!({ "changeType": "added" });
        let r = b.read("t", Some(&c0), None, Some(2), Some(&params), Some(filter.as_ref()));
        assert_eq!(ids(&r), vec!["e1", "e2"]);
        assert!(!r.has_more);
        // Cursor advanced past the trailing filtered event.
        assert_eq!(r.cursor, format!("{}:3", epoch_of(&b)));
    }

    #[test]
    fn filter_not_applied_when_params_absent() {
        let b = buf(10);
        let c0 = b.read("t", None, None, None, None, None).cursor;
        b.emit(change_ev("t", "e1", "added"));
        b.emit(change_ev("t", "e2", "updated"));
        let filter = change_type_filter();
        let r = b.read("t", Some(&c0), None, None, None, Some(filter.as_ref()));
        assert_eq!(ids(&r), vec!["e1", "e2"]);
    }

    #[test]
    fn event_types_are_independent() {
        let b = buf(10);
        let now = Utc::now();
        assert_eq!(b.emit(ev("a", "a1", now)), 1);
        assert_eq!(b.emit(ev("b", "b1", now)), 1);
        assert_eq!(b.emit(ev("a", "a2", now)), 2);
        assert_eq!(b.current_cursor("a"), format!("{}:2", epoch_of(&b)));
        assert_eq!(b.current_cursor("b"), format!("{}:1", epoch_of(&b)));
        let c0 = format!("{}:0", epoch_of(&b));
        let r = b.read("a", Some(&c0), None, None, None, None);
        assert_eq!(ids(&r), vec!["a1", "a2"]);
    }

    #[test]
    fn current_cursor_for_unknown_type_is_position_zero() {
        let b = buf(10);
        assert_eq!(b.current_cursor("nope"), format!("{}:0", epoch_of(&b)));
    }

    #[tokio::test]
    async fn live_receivers_get_cursor_bearing_occurrences() {
        let b = buf(10);
        let mut rx = b.live("t");
        let seq = b.emit(ev("t", "e1", Utc::now()));
        assert_eq!(seq, 1);
        let live = rx.recv().await.unwrap();
        assert_eq!(live.seq, 1);
        assert_eq!(live.occurrence.event_id, "e1");
        assert_eq!(
            live.occurrence.cursor,
            Some(Some(format!("{}:1", epoch_of(&b))))
        );
        // Buffered read of the same event carries no per-event cursor.
        let c0 = format!("{}:0", epoch_of(&b));
        let r = b.read("t", Some(&c0), None, None, None, None);
        assert_eq!(r.events[0].cursor, None);
    }

    #[tokio::test]
    async fn live_subscription_only_sees_later_emissions() {
        let b = buf(10);
        b.emit(ev("t", "before", Utc::now()));
        let mut rx = b.live("t");
        b.emit(ev("t", "after", Utc::now()));
        let live = rx.recv().await.unwrap();
        assert_eq!(live.occurrence.event_id, "after");
        assert!(rx.try_recv().is_err());
    }
}
