//! `events/list`: returns the full registry in one page (`nextCursor` absent).

use mcp_events_wire::{JsonRpcError, ListEventsParams, ListEventsResult};
use serde_json::Value;

use crate::state::AppState;

pub fn handle(state: &AppState, params: Option<Value>) -> Result<Value, JsonRpcError> {
    // A pagination cursor is accepted but ignored: the registry fits one page.
    let _p: ListEventsParams = super::parse_params(params)?;
    let result = ListEventsResult {
        events: state.registry.list(),
        next_cursor: None,
    };
    serde_json::to_value(result).map_err(|e| JsonRpcError::internal_error(e.to_string()))
}
