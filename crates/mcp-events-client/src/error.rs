//! Typed wrapper for JSON-RPC error responses so callers (and the harness)
//! can branch on / print code names while still flowing through `anyhow`.

use std::fmt;

use mcp_events_wire as wire;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcError {
    /// Human-readable name for the codes defined by JSON-RPC and the events
    /// extension (design sketch §Error Codes).
    pub fn code_name(&self) -> &'static str {
        match self.code {
            wire::PARSE_ERROR => "ParseError",
            wire::INVALID_REQUEST => "InvalidRequest",
            wire::METHOD_NOT_FOUND => "MethodNotFound",
            wire::INVALID_PARAMS => "InvalidParams",
            wire::INTERNAL_ERROR => "InternalError",
            wire::NOT_FOUND => "NotFound",
            wire::FORBIDDEN => "Forbidden",
            wire::RESOURCE_EXHAUSTED => "ResourceExhausted",
            wire::UNSUPPORTED => "Unsupported",
            wire::CALLBACK_ENDPOINT_ERROR => "CallbackEndpointError",
            c if (-32099..=-32000).contains(&c) => "ServerError",
            _ => "Error",
        }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.code_name(), self.code, self.message)?;
        if let Some(data) = &self.data {
            write!(f, " data={data}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RpcError {}

impl From<wire::JsonRpcError> for RpcError {
    fn from(e: wire::JsonRpcError) -> Self {
        Self {
            code: e.code,
            message: e.message,
            data: e.data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_names() {
        let cases = [
            (wire::NOT_FOUND, "NotFound"),
            (wire::FORBIDDEN, "Forbidden"),
            (wire::RESOURCE_EXHAUSTED, "ResourceExhausted"),
            (wire::UNSUPPORTED, "Unsupported"),
            (wire::CALLBACK_ENDPOINT_ERROR, "CallbackEndpointError"),
            (wire::INVALID_PARAMS, "InvalidParams"),
            (-32050, "ServerError"),
            (42, "Error"),
        ];
        for (code, name) in cases {
            let e = RpcError {
                code,
                message: "m".into(),
                data: None,
            };
            assert_eq!(e.code_name(), name);
        }
    }

    #[test]
    fn display_includes_code_name_and_data() {
        let e = RpcError::from(wire::JsonRpcError::not_found("event", "no such event"));
        let s = e.to_string();
        assert!(s.contains("NotFound"), "{s}");
        assert!(s.contains("-32011"), "{s}");
        assert!(s.contains("\"kind\""), "{s}");
    }

    #[test]
    fn downcasts_through_anyhow() {
        let err: anyhow::Error = RpcError {
            code: wire::FORBIDDEN,
            message: "nope".into(),
            data: None,
        }
        .into();
        assert_eq!(err.downcast_ref::<RpcError>().map(|e| e.code), Some(wire::FORBIDDEN));
    }
}
