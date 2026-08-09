//! HTTP surface: hybrid `POST /mcp` dispatcher + `GET /healthz`.
//!
//! Route-1 hybrid (one endpoint, two dispatch targets): the five draft
//! `events/*` extension methods are served by the hand-written handlers in
//! this crate; every other request — initialize back-compat, `server/discover`,
//! `tools/*`, the whole 2026-07-28 stateless lifecycle — is forwarded into the
//! official SDK's `StreamableHttpService` (rmcp). Method sniffing prefers the
//! SEP-2243 `Mcp-Method` header (required on the wire for protocol >=
//! 2026-07-28) and falls back to parsing the JSON-RPC body, so pre-2026
//! clients work too.
//!
//! Events-method rules follow the design sketch: client notifications get
//! `202 Accepted`; requests get `application/json` — except `events/stream`,
//! which (when valid) returns `text/event-stream`. Bearer tokens resolve to
//! principals via config for the webhook methods.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use http_body_util::Full;
use mcp_events_wire as wire;
use mcp_events_wire::{JsonRpcError, JsonRpcRequest, JsonRpcResponse, RequestId};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use serde_json::{json, Value};

use crate::config::ServerConfig;
use crate::handlers;
use crate::mcp_service::OrdersMcp;
use crate::state::AppState;

pub type McpCoreService = StreamableHttpService<OrdersMcp, LocalSessionManager>;

/// Builds the official-SDK core service: rmcp serves initialize back-compat,
/// `server/discover`, `tools/*`, and the 2026-07-28 stateless lifecycle.
/// Sessions exist only for legacy (< 2026-07-28) clients; 2026-07-28 requests
/// are always served statelessly (SEP-2567). `json_response` keeps unary
/// responses plain JSON so curl/log output stays readable in the demo.
pub fn build_mcp_core(postgres_url: String) -> Arc<McpCoreService> {
    let db = Arc::new(crate::tools_db::ToolsDb::new(postgres_url));
    let orders = OrdersMcp::new(db);
    Arc::new(StreamableHttpService::new(
        move || Ok(orders.clone()),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default().with_json_response(true),
    ))
}

/// Combined router state: the events-extension state plus the rmcp core service.
#[derive(Clone)]
pub struct HybridState {
    pub app: Arc<AppState>,
    pub mcp: Arc<McpCoreService>,
}

pub fn router(app: Arc<AppState>, mcp: Arc<McpCoreService>) -> Router {
    Router::new()
        .route("/mcp", any(handle_hybrid))
        .route("/healthz", get(healthz))
        .with_state(HybridState { app, mcp })
}

async fn healthz() -> &'static str {
    "ok"
}

fn success(id: RequestId, result: Value) -> Response {
    Json(JsonRpcResponse::success(id, result)).into_response()
}

fn failure(id: RequestId, error: JsonRpcError) -> Response {
    Json(JsonRpcResponse::failure(Some(id), error)).into_response()
}

/// Success path for events methods: stamp the SEP-2322 `resultType`
/// discriminator (2026-07-28 requires it on every result; clients on older
/// protocols ignore the extra field).
fn respond(id: RequestId, outcome: Result<Value, JsonRpcError>) -> Response {
    match outcome {
        Ok(mut result) => {
            if let Value::Object(map) = &mut result {
                map.entry("resultType").or_insert_with(|| json!("complete"));
            }
            success(id, result)
        }
        Err(error) => failure(id, error),
    }
}

/// Resolves `Authorization: Bearer <token>` to a configured principal.
/// Unknown or absent tokens yield `None` (anonymous): poll/push remain open,
/// webhook methods reject anonymous callers with `-32012`.
fn principal_from_headers(config: &ServerConfig, headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let principal = config.principal_for_token(token.trim());
    if principal.is_none() {
        tracing::warn!("unrecognized bearer token; treating request as unauthenticated");
    }
    principal
}

/// Matches rmcp's default `max_request_body_bytes` (4 MiB) so fronting it
/// with this buffering layer does not silently tighten the transport limit.
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// SEP-2243 / 2026-07-28 `HeaderMismatchError` (-32020), mapped to HTTP 400.
const HEADER_MISMATCH: i64 = -32020;

async fn handle_hybrid(
    State(state): State<HybridState>,
    req: Request<axum::body::Body>,
) -> Response {
    // GET (SSE channel) and DELETE (session teardown) carry no JSON-RPC body —
    // they belong to the rmcp transport.
    if req.method() != Method::POST {
        return state.mcp.handle(req).await.into_response();
    }

    let (parts, body) = req.into_parts();
    let bytes: Bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(JsonRpcResponse::failure(
                    None,
                    JsonRpcError::invalid_request(format!(
                        "request body exceeds {MAX_BODY_BYTES} bytes or could not be read"
                    )),
                )),
            )
                .into_response();
        }
    };

    // Sniff the JSON-RPC method: prefer the SEP-2243 `Mcp-Method` header
    // (2026-07-28 clients must send it), fall back to the body.
    let method = parts
        .headers
        .get("mcp-method")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or_else(|| {
            serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|v| v.get("method").and_then(|m| m.as_str()).map(str::to_owned))
        });

    match method.as_deref() {
        Some(m) if m.starts_with("events/") => {
            let header_method = parts
                .headers
                .get("mcp-method")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            handle_events_rpc(&state.app, &parts.headers, header_method, &bytes).await
        }
        _ => {
            // Replay the buffered body into the official SDK untouched — rmcp
            // reads Mcp-Session-Id / MCP-Protocol-Version / Accept / Host from
            // the original parts and applies its own lifecycle validation.
            let req = Request::from_parts(parts, Full::new(bytes));
            state.mcp.handle(req).await.into_response()
        }
    }
}

async fn handle_events_rpc(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    header_method: Option<String>,
    body: &Bytes,
) -> Response {
    let principal = principal_from_headers(&state.config, headers);

    let value: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(JsonRpcResponse::failure(
                    None,
                    JsonRpcError::parse_error(format!("invalid JSON: {error}")),
                )),
            )
                .into_response();
        }
    };
    let req: JsonRpcRequest = match serde_json::from_value(value.clone()) {
        Ok(r) => r,
        Err(error) => {
            let id = value
                .get("id")
                .cloned()
                .and_then(|v| serde_json::from_value::<RequestId>(v).ok());
            return Json(JsonRpcResponse::failure(
                id,
                JsonRpcError::invalid_request(format!("not a JSON-RPC request: {error}")),
            ))
            .into_response();
        }
    };

    let Some(id) = req.id.clone() else {
        // Client notification: 202, no body.
        tracing::debug!(method = %req.method, "client notification accepted");
        return StatusCode::ACCEPTED.into_response();
    };

    // SEP-2243: when the Mcp-Method header is present it MUST match the body
    // method — routing trusted the header, so enforce the pairing here.
    if let Some(header) = header_method {
        if header != req.method {
            return (
                StatusCode::BAD_REQUEST,
                Json(JsonRpcResponse::failure(
                    Some(id),
                    JsonRpcError {
                        code: HEADER_MISMATCH,
                        message: format!(
                            "Mcp-Method header {header:?} does not match body method {:?}",
                            req.method
                        ),
                        data: None,
                    },
                )),
            )
                .into_response();
        }
    }

    tracing::debug!(method = %req.method, %id, "dispatching events request");
    match req.method.as_str() {
        wire::METHOD_EVENTS_LIST => respond(id, handlers::list::handle(state, req.params)),
        wire::METHOD_EVENTS_POLL => respond(id, handlers::poll::handle(state, req.params)),
        wire::METHOD_EVENTS_STREAM => {
            match handlers::stream::handle(state.clone(), id.clone(), req.params).await {
                Ok(sse) => sse,
                Err(error) => failure(id, error),
            }
        }
        wire::METHOD_EVENTS_SUBSCRIBE => {
            let params = match handlers::parse_params::<wire::SubscribeParams>(req.params) {
                Ok(p) => p,
                Err(error) => return failure(id, error),
            };
            match crate::webhook::handlers::handle_subscribe(state.clone(), principal, params)
                .await
            {
                Ok(result) => respond(
                    id,
                    serde_json::to_value(result)
                        .map_err(|e| JsonRpcError::internal_error(e.to_string())),
                ),
                Err(error) => failure(id, error),
            }
        }
        wire::METHOD_EVENTS_UNSUBSCRIBE => {
            let params = match handlers::parse_params::<wire::UnsubscribeParams>(req.params) {
                Ok(p) => p,
                Err(error) => return failure(id, error),
            };
            match crate::webhook::handlers::handle_unsubscribe(state.clone(), principal, params)
                .await
            {
                Ok(result) => respond(id, Ok(result)),
                Err(error) => failure(id, error),
            }
        }
        other => failure(id, JsonRpcError::method_not_found(other)),
    }
}
