//! `initialize`: protocol/capability negotiation + `Mcp-Session-Id` issuance.

use mcp_events_wire::{
    EventsCapability, Implementation, InitializeParams, InitializeResult, JsonRpcError,
    ServerCapabilities, PROTOCOL_VERSION,
};
use serde_json::Value;

/// Returns `(result, session_id)`; the dispatcher attaches the session id as
/// the `Mcp-Session-Id` response header.
pub fn handle(params: Option<Value>) -> Result<(Value, String), JsonRpcError> {
    let p: InitializeParams = super::parse_params(params)?;
    if p.protocol_version != PROTOCOL_VERSION {
        // Version negotiation: reply with the (single) version we support.
        tracing::debug!(requested = %p.protocol_version, "client requested a different protocol version; offering ours");
    }
    let result = InitializeResult {
        protocol_version: PROTOCOL_VERSION.to_owned(),
        capabilities: ServerCapabilities {
            events: Some(EventsCapability {
                list_changed: Some(false),
            }),
            extra: serde_json::Map::new(),
        },
        server_info: Implementation {
            name: env!("CARGO_PKG_NAME").to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            title: Some("MCP Events prototype server (Drasi-backed)".to_owned()),
        },
        instructions: None,
    };
    // Prototype shortcut (documented): the session id is issued but not
    // enforced on subsequent requests.
    let session_id = uuid::Uuid::new_v4().to_string();
    let value =
        serde_json::to_value(result).map_err(|e| JsonRpcError::internal_error(e.to_string()))?;
    Ok((value, session_id))
}
