//! `events/poll`: cursor-based read of the emit buffer.

use mcp_events_wire::{DeliveryMode, JsonRpcError, PollEventsParams, PollEventsResult};
use serde_json::Value;

use crate::state::AppState;

pub fn handle(state: &AppState, params: Option<Value>) -> Result<Value, JsonRpcError> {
    let p: PollEventsParams = super::parse_params(params)?;
    let def = state.registry.get(&p.name).ok_or_else(|| {
        JsonRpcError::not_found("event", format!("unknown event type \"{}\"", p.name))
    })?;
    if !def.delivery.contains(&DeliveryMode::Poll) {
        return Err(JsonRpcError::unsupported("deliveryMode", "poll"));
    }
    crate::mapping::validate_event_params(&state.filters, &p.name, p.params.as_ref())?;
    let filter = state.filters.get(&p.name).cloned();
    let read = state.buffer.read(
        &p.name,
        p.cursor.as_deref(),
        p.max_age_ms,
        p.max_events,
        p.params.as_ref(),
        filter.as_deref(),
    );
    let result = PollEventsResult {
        events: read.events,
        cursor: Some(read.cursor),
        truncated: read.truncated,
        has_more: read.has_more,
        next_poll_ms: state.config.poll.next_poll_ms,
    };
    serde_json::to_value(result).map_err(|e| JsonRpcError::internal_error(e.to_string()))
}
