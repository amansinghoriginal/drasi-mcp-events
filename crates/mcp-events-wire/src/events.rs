//! Events-extension wire types (design sketch: events/list, events/poll,
//! events/stream, events/subscribe, events/unsubscribe, notification params).
//!
//! null-vs-absent conventions:
//! - `Option<Option<T>>` where absent and `null` differ on the wire
//!   (`EventOccurrence.cursor`, `SubscribeParams.ttl_ms`): absent → `None`,
//!   `null` → `Some(None)`, value → `Some(Some(v))`.
//! - Plain always-serialized `Option<T>` where the field is always present but
//!   nullable (`PollEventsResult.cursor`, `SubscribeResult.refresh_before`/`cursor`,
//!   notification `cursor` fields).
//! - Request-level `cursor` (poll/stream/subscribe) is plain `Option<String>`:
//!   absent is treated as `null` ("start from now") and serialized as `null`,
//!   matching the sketch examples.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Deserializes a present field (including `null`) into `Some(inner)`;
/// combined with `#[serde(default)]`, an absent field stays `None`.
pub(crate) fn double_option<'de, T, D>(de: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Option::<T>::deserialize(de).map(Some)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryMode {
    Poll,
    Push,
    Webhook,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventDefinition {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub delivery: Vec<DeliveryMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_schema: Option<Value>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListEventsParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListEventsResult {
    pub events: Vec<EventDefinition>,
    /// Present only when more pages are available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventOccurrence {
    pub event_id: String,
    pub name: String,
    /// ISO 8601.
    pub timestamp: String,
    pub data: Value,
    /// Absent in poll results (cursor is at the response level);
    /// `null` = event type does not support replay.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "double_option"
    )]
    pub cursor: Option<Option<String>>,
    #[serde(rename = "_meta", default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollEventsParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    /// `null` = start from now. Always serialized (absent ≡ null on receive).
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollEventsResult {
    pub events: Vec<EventOccurrence>,
    /// Always serialized; `null` = event type does not support replay.
    pub cursor: Option<String>,
    pub truncated: bool,
    pub has_more: bool,
    pub next_poll_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamEventsParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    /// `null` = start from now. Always serialized (absent ≡ null on receive).
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_ms: Option<u64>,
}

/// Params of `notifications/events/active`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventsActiveParams {
    /// Always serialized; `null` = no replay support.
    pub cursor: Option<String>,
    pub truncated: bool,
    #[serde(rename = "_meta")]
    pub meta: Value,
}

/// Params of `notifications/events/error` (transient; subscription stays active).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventsErrorParams {
    pub error: crate::jsonrpc::JsonRpcError,
    #[serde(rename = "_meta")]
    pub meta: Value,
}

/// Params of `notifications/events/terminated` (subscription has ended).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventsTerminatedParams {
    pub error: crate::jsonrpc::JsonRpcError,
    #[serde(rename = "_meta")]
    pub meta: Value,
}

/// Params of `notifications/events/heartbeat`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventsHeartbeatParams {
    /// Always serialized; `null` = no replay support.
    pub cursor: Option<String>,
    #[serde(rename = "_meta")]
    pub meta: Value,
}

/// `delivery` object in subscribe/unsubscribe params.
///
/// The sketch's `events/unsubscribe` example carries `{ "url": ... }` only —
/// no `mode` — so `mode` is empty-string when absent and skipped when empty.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliverySpec {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mode: String,
    pub url: String,
    /// REQUIRED on subscribe (validated server-side), absent on unsubscribe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    pub delivery: DeliverySpec,
    /// `null` = start from now. Always serialized (absent ≡ null on receive).
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_ms: Option<u64>,
    /// Absent = server default; `null` = request no expiry; value = suggested TTL.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "double_option"
    )]
    pub ttl_ms: Option<Option<u64>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryStatus {
    pub active: bool,
    #[serde(default)]
    pub last_delivery_at: Option<String>,
    /// Category string (`connection_refused` | `timeout` | `tls_error` |
    /// `http_4xx` | `http_5xx` | `challenge_failed`), never raw endpoint output.
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_since: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeResult {
    pub id: String,
    /// Always serialized; ISO 8601 expiry, `null` = no expiry granted.
    pub refresh_before: Option<String>,
    /// Always serialized; safe-to-persist watermark, `null` = no replay support.
    pub cursor: Option<String>,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_status: Option<DeliveryStatus>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsubscribeParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    /// `url` only — no `mode`/`secret`.
    pub delivery: DeliverySpec,
}
