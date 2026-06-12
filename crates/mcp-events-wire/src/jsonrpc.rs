//! JSON-RPC 2.0 message types and the error-code surface defined by the
//! MCP Events design sketch (§Error Codes).

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::consts::JSONRPC_VERSION;

// Standard JSON-RPC 2.0 codes.
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;

// General-purpose codes defined by the events extension (server range [-32000, -32099]).
pub const NOT_FOUND: i64 = -32011;
pub const FORBIDDEN: i64 = -32012;
pub const RESOURCE_EXHAUSTED: i64 = -32013;
pub const UNSUPPORTED: i64 = -32014;
pub const CALLBACK_ENDPOINT_ERROR: i64 = -32015;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Num(i64),
    Str(String),
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::Num(n) => write!(f, "{n}"),
            RequestId::Str(s) => write!(f, "{s}"),
        }
    }
}

impl From<i64> for RequestId {
    fn from(n: i64) -> Self {
        RequestId::Num(n)
    }
}

impl From<String> for RequestId {
    fn from(s: String) -> Self {
        RequestId::Str(s)
    }
}

impl From<&str> for RequestId {
    fn from(s: &str) -> Self {
        RequestId::Str(s.to_owned())
    }
}

/// A JSON-RPC request or notification (`id` absent ⇒ notification).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcRequest {
    pub fn request(id: impl Into<RequestId>, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id: Some(id.into()),
            method: method.into(),
            params,
        }
    }

    pub fn notification(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id: None,
            method: method.into(),
            params,
        }
    }

    pub fn is_notification(&self) -> bool {
        self.id.is_none()
    }
}

/// A JSON-RPC response: `result` XOR `error`. `id` is always serialized
/// (JSON-RPC requires it; `null` is only legal when the request id was unparseable).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<RequestId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    pub fn success(id: impl Into<RequestId>, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id: Some(id.into()),
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(id: Option<RequestId>, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcError {
    pub fn new(code: i64, message: impl Into<String>, data: Option<Value>) -> Self {
        Self {
            code,
            message: message.into(),
            data,
        }
    }

    pub fn parse_error(message: impl Into<String>) -> Self {
        Self::new(PARSE_ERROR, message, None)
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(INVALID_REQUEST, message, None)
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(
            METHOD_NOT_FOUND,
            "MethodNotFound",
            Some(serde_json::json!({ "method": method })),
        )
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(INVALID_PARAMS, message, None)
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new(INTERNAL_ERROR, message, None)
    }

    /// `-32011 NotFound`; `kind` is `"event"` or `"subscription"` (design sketch §Error Codes).
    pub fn not_found(kind: &str, message: impl Into<String>) -> Self {
        Self::new(NOT_FOUND, message, Some(serde_json::json!({ "kind": kind })))
    }

    /// `-32012 Forbidden`.
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(FORBIDDEN, message, None)
    }

    /// `-32013 ResourceExhausted`; `limit` names the limit, `max` optionally gives the ceiling.
    pub fn resource_exhausted(limit: &str, max: Option<u64>) -> Self {
        let mut data = serde_json::Map::new();
        data.insert("limit".to_owned(), Value::from(limit));
        if let Some(max) = max {
            data.insert("max".to_owned(), Value::from(max));
        }
        Self::new(RESOURCE_EXHAUSTED, "ResourceExhausted", Some(Value::Object(data)))
    }

    /// `-32014 Unsupported`; e.g. `feature: "deliveryMode", value: "push"`.
    pub fn unsupported(feature: &str, value: &str) -> Self {
        Self::new(
            UNSUPPORTED,
            "Unsupported",
            Some(serde_json::json!({ "feature": feature, "value": value })),
        )
    }

    /// `-32015 CallbackEndpointError`; `reason` is one of the `lastError` categories.
    pub fn callback_endpoint_error(reason: &str) -> Self {
        Self::new(
            CALLBACK_ENDPOINT_ERROR,
            "CallbackEndpointError",
            Some(serde_json::json!({ "reason": reason })),
        )
    }
}
