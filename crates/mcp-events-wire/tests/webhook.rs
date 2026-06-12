//! whsec parsing bounds and Standard Webhooks signature known-answer tests.

use mcp_events_wire::*;

// base64 fixtures generated once (standard alphabet, padded).
const WHSEC_23: &str = "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRY=";
const WHSEC_24: &str = "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYX";
const WHSEC_64: &str =
    "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+Pw==";
const WHSEC_65: &str =
    "whsec_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

#[test]
fn whsec_bounds() {
    assert_eq!(parse_whsec(WHSEC_23), Err(WhsecError::BadLength(23)));

    let k24 = parse_whsec(WHSEC_24).unwrap();
    assert_eq!(k24.len(), 24);
    assert_eq!(k24[0], 0);
    assert_eq!(k24[23], 23);

    let k64 = parse_whsec(WHSEC_64).unwrap();
    assert_eq!(k64.len(), 64);

    assert_eq!(parse_whsec(WHSEC_65), Err(WhsecError::BadLength(65)));
}

#[test]
fn whsec_bad_prefix_rejected() {
    assert_eq!(
        parse_whsec("AAECAwQFBgcICQoLDA0ODxAREhMUFRYX"),
        Err(WhsecError::MissingPrefix)
    );
    assert_eq!(
        parse_whsec("whsec-AAECAwQFBgcICQoLDA0ODxAREhMUFRYX"),
        Err(WhsecError::MissingPrefix)
    );
    assert_eq!(
        parse_whsec("WHSEC_AAECAwQFBgcICQoLDA0ODxAREhMUFRYX"),
        Err(WhsecError::MissingPrefix)
    );
}

#[test]
fn whsec_bad_base64_rejected() {
    assert!(matches!(
        parse_whsec("whsec_!!!not-base64!!!"),
        Err(WhsecError::InvalidBase64(_))
    ));
}

// Known-answer test, computed once externally with
// HMAC-SHA256(secret, "evt_789.1739980800.<body>") and pinned:
//   secret  = b"MCP-events-wire-known-answer-32B" (32 bytes)
//   whsec   = "whsec_" + base64(secret)
const KAT_WHSEC: &str = "whsec_TUNQLWV2ZW50cy13aXJlLWtub3duLWFuc3dlci0zMkI=";
const KAT_MSG_ID: &str = "evt_789";
const KAT_TS: i64 = 1739980800;
const KAT_BODY: &[u8] = br#"{"eventId":"evt_789","name":"incident.created","timestamp":"2026-02-19T16:00:00Z","data":{"incidentId":"INC-1234","title":"Database connection pool exhausted","severity":"P1"},"cursor":"cursor_xyz"}"#;
const KAT_SIG: &str = "v1,C58Y3Bm+leDlL4KEu1xinLp1QQKqaroJDZHLqvMtLuE=";

#[test]
fn signature_known_answer() {
    let secret = parse_whsec(KAT_WHSEC).unwrap();
    assert_eq!(secret.as_slice(), b"MCP-events-wire-known-answer-32B");
    let sig = sign_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS, KAT_BODY);
    assert_eq!(sig, KAT_SIG);
}

#[test]
fn verify_accepts_known_answer() {
    let secret = parse_whsec(KAT_WHSEC).unwrap();
    assert!(verify_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS, KAT_BODY, KAT_SIG));
}

#[test]
fn verify_rejects_tampering() {
    let secret = parse_whsec(KAT_WHSEC).unwrap();
    // Wrong timestamp.
    assert!(!verify_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS + 1, KAT_BODY, KAT_SIG));
    // Wrong msg id.
    assert!(!verify_standard_webhooks(&secret, "evt_790", KAT_TS, KAT_BODY, KAT_SIG));
    // Body mutated.
    let mut body = KAT_BODY.to_vec();
    body[0] = b' ';
    assert!(!verify_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS, &body, KAT_SIG));
    // Wrong secret.
    let other = parse_whsec(WHSEC_24).unwrap();
    assert!(!verify_standard_webhooks(&other, KAT_MSG_ID, KAT_TS, KAT_BODY, KAT_SIG));
    // Empty header.
    assert!(!verify_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS, KAT_BODY, ""));
}

#[test]
fn verify_is_multi_signature_aware() {
    let secret = parse_whsec(KAT_WHSEC).unwrap();
    // Rotation: a non-matching v1 signature followed by the valid one.
    let header = format!("v1,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= {KAT_SIG}");
    assert!(verify_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS, KAT_BODY, &header));
    // Valid one first also works.
    let header = format!("{KAT_SIG} v1,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    assert!(verify_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS, KAT_BODY, &header));
}

#[test]
fn verify_ignores_unknown_schemes_and_garbage() {
    let secret = parse_whsec(KAT_WHSEC).unwrap();
    // Asymmetric (v1a,) entries are not verified by the symmetric helper.
    let v1a = format!("v1a,{}", &KAT_SIG[3..]);
    assert!(!verify_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS, KAT_BODY, &v1a));
    // ...but their presence alongside a valid v1 entry is fine.
    let header = format!("{v1a} {KAT_SIG}");
    assert!(verify_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS, KAT_BODY, &header));
    // Undecodable v1 entries are skipped, not fatal.
    let header = format!("v1,%%% {KAT_SIG}");
    assert!(verify_standard_webhooks(&secret, KAT_MSG_ID, KAT_TS, KAT_BODY, &header));
}

#[test]
fn sign_verify_round_trip_fresh_secret() {
    let secret: Vec<u8> = (0u8..32).collect();
    let body = br#"{"type":"verification","challenge":"a2f0"}"#;
    let sig = sign_standard_webhooks(&secret, "msg_verification_x1", 1760000000, body);
    assert!(sig.starts_with("v1,"));
    assert!(verify_standard_webhooks(&secret, "msg_verification_x1", 1760000000, body, &sig));
    // Retry regenerates timestamp → old signature must not verify for new ts.
    assert!(!verify_standard_webhooks(&secret, "msg_verification_x1", 1760000001, body, &sig));
}
