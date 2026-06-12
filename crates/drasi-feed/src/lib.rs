//! Feed sources for the MCP Events prototype.
//!
//! Produces a stream of [`FeedEvent`]s either from a live Drasi Server SSE
//! reaction ([`run_drasi_sse_feed`]) or from a deterministic synthetic
//! "orders" scenario ([`run_mock_feed`]). These types are internal plumbing
//! between the feed and the server's event-mapping layer; they are not wire
//! types.

mod mock;
mod sse;

pub use mock::run_mock_feed;
pub use sse::run_drasi_sse_feed;

/// Kind of change a continuous-query result row underwent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeType {
    Added,
    Updated,
    Deleted,
}

/// One row-level change observed on a continuous query.
#[derive(Clone, Debug, PartialEq)]
pub struct FeedEvent {
    pub query_id: String,
    pub change: ChangeType,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub upstream_id: Option<String>,
}
