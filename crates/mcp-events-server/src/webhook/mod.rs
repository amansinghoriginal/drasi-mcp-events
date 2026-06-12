//! Webhook delivery subsystem (design sketch §Webhook-Based Delivery,
//! §Webhook Security, §Subscription TTL/Identity): subscribe/unsubscribe
//! handlers, the background delivery worker, Standard Webhooks signing,
//! SSRF-hardened egress, and the endpoint-verification challenge.

pub mod challenge;
pub mod handlers;
pub mod signer;
pub mod ssrf;
pub mod worker;
