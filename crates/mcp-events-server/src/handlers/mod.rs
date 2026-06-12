//! JSON-RPC method handlers (server-core: initialize, ping handled inline,
//! events/list, events/poll, events/stream).

pub mod initialize;
pub mod list;
pub mod poll;
pub mod stream;

use mcp_events_wire::JsonRpcError;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Decodes request params; absent and `null` are treated as an empty object
/// so structs whose fields are all optional accept param-less requests.
pub(crate) fn parse_params<T: DeserializeOwned>(params: Option<Value>) -> Result<T, JsonRpcError> {
    let value = match params {
        None | Some(Value::Null) => Value::Object(serde_json::Map::new()),
        Some(v) => v,
    };
    serde_json::from_value(value)
        .map_err(|e| JsonRpcError::invalid_params(format!("invalid params: {e}")))
}
