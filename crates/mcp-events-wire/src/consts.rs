//! Protocol string constants: method names, notification names, headers, `_meta` keys.

/// MCP protocol version implemented by this prototype.
pub const PROTOCOL_VERSION: &str = "2025-11-25";

/// JSON-RPC version string carried in every message.
pub const JSONRPC_VERSION: &str = "2.0";

// MCP base methods / notifications.
pub const METHOD_INITIALIZE: &str = "initialize";
pub const METHOD_PING: &str = "ping";
pub const NOTIF_INITIALIZED: &str = "notifications/initialized";

// Events extension methods.
pub const METHOD_EVENTS_LIST: &str = "events/list";
pub const METHOD_EVENTS_POLL: &str = "events/poll";
pub const METHOD_EVENTS_STREAM: &str = "events/stream";
pub const METHOD_EVENTS_SUBSCRIBE: &str = "events/subscribe";
pub const METHOD_EVENTS_UNSUBSCRIBE: &str = "events/unsubscribe";

// Events extension notifications.
pub const NOTIF_EVENTS_ACTIVE: &str = "notifications/events/active";
pub const NOTIF_EVENTS_EVENT: &str = "notifications/events/event";
pub const NOTIF_EVENTS_HEARTBEAT: &str = "notifications/events/heartbeat";
pub const NOTIF_EVENTS_ERROR: &str = "notifications/events/error";
pub const NOTIF_EVENTS_TERMINATED: &str = "notifications/events/terminated";
pub const NOTIF_EVENTS_LIST_CHANGED: &str = "notifications/events/list_changed";

/// `_meta` key carrying the parent `events/stream` request id on every
/// `notifications/events/*` frame (SEP-2575 correlation convention).
pub const META_SUBSCRIPTION_ID: &str = "io.modelcontextprotocol/subscriptionId";

// Webhook delivery headers (HTTP header names are case-insensitive; lowercase canonical).
pub const HEADER_WEBHOOK_ID: &str = "webhook-id";
pub const HEADER_WEBHOOK_TIMESTAMP: &str = "webhook-timestamp";
pub const HEADER_WEBHOOK_SIGNATURE: &str = "webhook-signature";
pub const HEADER_MCP_SUBSCRIPTION_ID: &str = "x-mcp-subscription-id";
