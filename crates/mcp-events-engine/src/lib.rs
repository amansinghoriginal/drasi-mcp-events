//! Engine for the MCP Events prototype (design sketch, emit-only model):
//! event-type registry, per-type ring buffer with `<epoch>:<seq>` cursors and
//! broadcast fan-out, canonical JSON, and the webhook subscription store.
//!
//! Protocol behavior follows `docs/design-sketch-proposal.md`; the public API
//! is pinned by `docs/ARCHITECTURE.md`.

mod buffer;
mod canon;
mod registry;
mod store;

pub use buffer::{BufferConfig, EmittedEvent, EventBuffer, LiveEvent, ParamFilter, ReadResult};
pub use canon::canonical_json;
pub use registry::Registry;
pub use store::{
    derive_sub_id, error_category, StoreError, SubKey, SubscriptionStore, UpsertOutcome,
    WebhookSub,
};

use chrono::{DateTime, SecondsFormat, Utc};

/// Canonical ISO-8601 (RFC 3339, UTC `Z`, millisecond precision) rendering for
/// all wire timestamps produced by this engine.
pub fn iso8601(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn iso8601_renders_utc_z_millis() {
        let ts = Utc.with_ymd_and_hms(2026, 2, 19, 15, 30, 0).unwrap();
        assert_eq!(iso8601(ts), "2026-02-19T15:30:00.000Z");
    }
}
