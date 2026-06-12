//! Endpoint-verification handshake (design sketch §Webhook Security →
//! Endpoint verification): before activating delivery to an unverified
//! `(principal, url)`, POST a signed `verification` control envelope with a
//! single-use uuid-v4 nonce and require a 2xx body echoing
//! `{"challenge":"<nonce>"}`, compared in constant time.

use mcp_events_engine::error_category;
use mcp_events_wire::WebhookControlBody;
use subtle::ConstantTimeEq as _;
use url::Url;

use super::{signer, ssrf};
use crate::state::AppState;

/// Cap on the echoed body we are willing to read; anything larger cannot be
/// a well-formed challenge echo.
const MAX_ECHO_BYTES: usize = 64 * 1024;

/// Runs the verification handshake once (no retries). On failure returns the
/// `lastError` category to surface as `-32015 data.reason`: connection-level
/// failures map to `connection_refused`/`timeout`/`tls_error`, non-2xx
/// responses to `http_4xx`/`http_5xx`, and a 2xx that does not echo the nonce
/// to `challenge_failed`. Per the sketch, raw endpoint output is never
/// propagated.
pub async fn verify_endpoint(
    state: &AppState,
    url: &Url,
    sub_id: &str,
    secret: &[u8],
) -> Result<(), &'static str> {
    let client = ssrf::client_for(state, url).await.map_err(|error| {
        tracing::warn!(%url, %error, "verification blocked before connect");
        error.category()
    })?;

    let nonce = uuid::Uuid::new_v4().to_string();
    let body = serde_json::to_vec(&WebhookControlBody::Verification {
        challenge: nonce.clone(),
    })
    .map_err(|error| {
        tracing::error!(%error, "serializing verification envelope");
        error_category::CHALLENGE_FAILED
    })?;
    let msg_id = signer::control_msg_id("verification");

    let response = signer::signed_post(&client, url.as_str(), sub_id, &msg_id, secret, body)
        .send()
        .await
        .map_err(|error| {
            let category = ssrf::classify_send_error(&error);
            tracing::warn!(%url, %error, category, "verification POST failed");
            category
        })?;

    let status = response.status();
    if !status.is_success() {
        tracing::warn!(%url, %status, "verification POST got non-2xx");
        return Err(if status.is_server_error() {
            error_category::HTTP_5XX
        } else {
            error_category::HTTP_4XX
        });
    }

    let bytes = response.bytes().await.map_err(|error| {
        tracing::warn!(%url, %error, "reading verification echo failed");
        error_category::CHALLENGE_FAILED
    })?;
    if bytes.len() > MAX_ECHO_BYTES {
        return Err(error_category::CHALLENGE_FAILED);
    }
    let echoed = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|v| {
            v.get("challenge")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
    match echoed {
        Some(e) if bool::from(e.as_bytes().ct_eq(nonce.as_bytes())) => Ok(()),
        _ => Err(error_category::CHALLENGE_FAILED),
    }
}
