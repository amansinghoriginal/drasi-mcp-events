//! Client library for the MCP Events extension over Streamable HTTP
//! (single `POST /mcp` endpoint), plus shared helpers used by the
//! `events-harness` binary (incremental SSE parser, cursor state files,
//! bounded eventId dedup).

mod client;
mod dedup;
mod error;
mod sse;
mod state;

pub use client::{EventStream, EventsClient, StreamFrame};
pub use dedup::LruSet;
pub use error::RpcError;
pub use sse::{SseEvent, SseParser};
pub use state::{load_state, save_state, CursorState};
