//! Standard Webhooks signing helpers and webhook control-envelope bodies
//! (design sketch §Webhook Security → Signature scheme, §Non-event webhook bodies).

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::jsonrpc::JsonRpcError;

pub const WHSEC_PREFIX: &str = "whsec_";
pub const WHSEC_MIN_BYTES: usize = 24;
pub const WHSEC_MAX_BYTES: usize = 64;

/// Signature scheme version prefix for symmetric HMAC signatures.
pub const SIGNATURE_VERSION_PREFIX: &str = "v1,";

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WhsecError {
    #[error("secret must start with \"whsec_\"")]
    MissingPrefix,
    #[error("secret is not valid base64: {0}")]
    InvalidBase64(String),
    #[error("decoded secret must be {WHSEC_MIN_BYTES}..={WHSEC_MAX_BYTES} bytes, got {0}")]
    BadLength(usize),
}

/// Parses a Standard Webhooks symmetric secret: `whsec_` + base64 of 24..=64 bytes.
/// Standard base64 alphabet, padding required where applicable.
pub fn parse_whsec(s: &str) -> Result<Vec<u8>, WhsecError> {
    let b64 = s.strip_prefix(WHSEC_PREFIX).ok_or(WhsecError::MissingPrefix)?;
    let bytes = B64
        .decode(b64)
        .map_err(|e| WhsecError::InvalidBase64(e.to_string()))?;
    if !(WHSEC_MIN_BYTES..=WHSEC_MAX_BYTES).contains(&bytes.len()) {
        return Err(WhsecError::BadLength(bytes.len()));
    }
    Ok(bytes)
}

fn hmac_sha256(secret: &[u8], msg_id: &str, timestamp_secs: i64, body: &[u8]) -> Vec<u8> {
    // HMAC-SHA256 accepts keys of any length; new_from_slice cannot fail here.
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(msg_id.as_bytes());
    mac.update(b".");
    mac.update(timestamp_secs.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    mac.finalize().into_bytes().to_vec()
}

/// Computes the `webhook-signature` header value for one signature:
/// `"v1," + base64(HMAC-SHA256(secret, "<msg_id>.<timestamp>.<body>"))`.
/// `body` is the raw HTTP body bytes exactly as sent.
pub fn sign_standard_webhooks(secret: &[u8], msg_id: &str, timestamp_secs: i64, body: &[u8]) -> String {
    let tag = hmac_sha256(secret, msg_id, timestamp_secs, body);
    format!("{SIGNATURE_VERSION_PREFIX}{}", B64.encode(tag))
}

/// Verifies a `webhook-signature` header against one secret. The header may
/// carry multiple space-delimited signatures (secret rotation); verification
/// succeeds if any `v1,` entry matches. Non-`v1,` entries (e.g. `v1a,`
/// asymmetric signatures) and undecodable entries are ignored. Comparison is
/// constant-time per candidate.
pub fn verify_standard_webhooks(
    secret: &[u8],
    msg_id: &str,
    timestamp_secs: i64,
    body: &[u8],
    signature_header: &str,
) -> bool {
    let expected = hmac_sha256(secret, msg_id, timestamp_secs, body);
    let mut ok = false;
    for token in signature_header.split_whitespace() {
        let Some(b64) = token.strip_prefix(SIGNATURE_VERSION_PREFIX) else {
            continue;
        };
        let Ok(candidate) = B64.decode(b64) else {
            continue;
        };
        // No early return: check every candidate to keep timing uniform.
        ok |= bool::from(candidate.as_slice().ct_eq(expected.as_slice()));
    }
    ok
}

/// Non-event control envelope POSTed to a webhook endpoint. A body with a
/// top-level `type` field is a control envelope; one without is an
/// [`crate::EventOccurrence`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WebhookControlBody {
    /// Gap detected between refreshes; client persists `cursor` and treats it
    /// as `truncated: true`.
    Gap { cursor: Option<String> },
    /// Subscription has ended; it no longer exists server-side.
    Terminated { error: JsonRpcError },
    /// Endpoint-verification challenge; endpoint echoes `challenge` in a 2xx body.
    Verification { challenge: String },
}
