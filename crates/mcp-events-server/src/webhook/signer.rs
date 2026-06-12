//! Thin layer over the wire crate's Standard Webhooks helpers: builds signed
//! delivery POSTs and control-envelope message ids. Timestamp and signature
//! are regenerated on every call, so each retry attempt is freshly signed
//! (design sketch §Webhook Security → Signature scheme).

use mcp_events_wire as wire;

/// `webhook-id` for control envelopes: `msg_<type>_<random>` (design sketch
/// §Non-event webhook bodies). Stable per message; reused across retries so
/// receivers can dedup.
pub fn control_msg_id(kind: &str) -> String {
    format!("msg_{kind}_{}", uuid::Uuid::new_v4().simple())
}

/// Signed Standard Webhooks POST builder. The signature covers the exact
/// `body` bytes attached to the request.
pub fn signed_post(
    client: &reqwest::Client,
    url: &str,
    sub_id: &str,
    msg_id: &str,
    secret: &[u8],
    body: Vec<u8>,
) -> reqwest::RequestBuilder {
    let timestamp = chrono::Utc::now().timestamp();
    let signature = wire::sign_standard_webhooks(secret, msg_id, timestamp, &body);
    client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(wire::HEADER_WEBHOOK_ID, msg_id)
        .header(wire::HEADER_WEBHOOK_TIMESTAMP, timestamp.to_string())
        .header(wire::HEADER_WEBHOOK_SIGNATURE, signature)
        .header(wire::HEADER_MCP_SUBSCRIPTION_ID, sub_id)
        .body(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_msg_ids_are_typed_and_unique() {
        let a = control_msg_id("gap");
        let b = control_msg_id("gap");
        assert!(a.starts_with("msg_gap_"));
        assert!(a.len() > "msg_gap_".len());
        assert_ne!(a, b);
        assert!(control_msg_id("verification").starts_with("msg_verification_"));
    }
}
