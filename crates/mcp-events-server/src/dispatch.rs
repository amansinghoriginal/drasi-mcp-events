//! HTTP surface: `POST /mcp` JSON-RPC dispatcher + `GET /healthz`.
//!
//! Base-protocol rules (docs/mcp-reference.md §2.2): client notifications get
//! `202 Accepted` with no body; requests get a single `application/json`
//! JSON-RPC response — except `events/stream`, which (when valid) returns
//! `text/event-stream`. Bearer tokens resolve to principals via config; an
//! `Mcp-Session-Id` header is issued on `initialize` but, as a documented
//! prototype shortcut, not enforced afterwards.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mcp_events_wire as wire;
use mcp_events_wire::{JsonRpcError, JsonRpcRequest, JsonRpcResponse, RequestId};
use serde_json::{json, Value};

use crate::config::ServerConfig;
use crate::handlers;
use crate::state::AppState;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/mcp", post(handle_mcp))
        .route("/healthz", get(healthz))
        .with_state(state)
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

fn respond(id: RequestId, outcome: Result<Value, JsonRpcError>) -> Response {
    match outcome {
        Ok(result) => success(id, result),
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

async fn handle_mcp(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let principal = principal_from_headers(&state.config, &headers);

    let value: Value = match serde_json::from_slice(&body) {
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
        // Client notification (e.g. notifications/initialized): 202, no body.
        tracing::debug!(method = %req.method, "client notification accepted");
        return StatusCode::ACCEPTED.into_response();
    };

    tracing::debug!(method = %req.method, %id, "dispatching request");
    match req.method.as_str() {
        wire::METHOD_INITIALIZE => match handlers::initialize::handle(req.params) {
            Ok((result, session_id)) => {
                let mut resp = success(id, result);
                match HeaderValue::from_str(&session_id) {
                    Ok(v) => {
                        resp.headers_mut().insert("mcp-session-id", v);
                    }
                    Err(error) => tracing::error!(%error, "session id is not header-safe"),
                }
                resp
            }
            Err(error) => failure(id, error),
        },
        wire::METHOD_PING => success(id, json!({})),
        wire::METHOD_EVENTS_LIST => respond(id, handlers::list::handle(&state, req.params)),
        wire::METHOD_EVENTS_POLL => respond(id, handlers::poll::handle(&state, req.params)),
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
                Ok(result) => success(id, result),
                Err(error) => failure(id, error),
            }
        }
        other => failure(id, JsonRpcError::method_not_found(other)),
    }
}
