//! `events/subscribe` / `events/unsubscribe` handlers (design sketch
//! §Webhook-Based Delivery, §Subscription Identity, §Subscription TTL,
//! §Webhook Security). Both require an authenticated principal; the
//! subscription key is `(principal, delivery.url, name, canonical params)`.

use std::sync::Arc;

use chrono::Utc;
use mcp_events_engine::{canonical_json, iso8601, StoreError, SubKey, UpsertOutcome};
use mcp_events_wire::{
    parse_whsec, DeliveryMode, JsonRpcError, SubscribeParams, SubscribeResult, UnsubscribeParams,
};
use serde_json::{json, Value};
use url::Url;

use super::challenge;
use crate::config::WebhookSettings;
use crate::state::AppState;

pub async fn handle_subscribe(
    state: Arc<AppState>,
    principal: Option<String>,
    params: SubscribeParams,
) -> Result<SubscribeResult, JsonRpcError> {
    let Some(principal) = principal else {
        return Err(JsonRpcError::forbidden(
            "events/subscribe requires an authenticated principal",
        ));
    };
    let SubscribeParams {
        name,
        params: sub_params,
        delivery,
        cursor,
        max_age_ms,
        ttl_ms,
    } = params;

    if delivery.mode.is_empty() {
        return Err(JsonRpcError::invalid_params(
            "delivery.mode is required on events/subscribe and must be \"webhook\"",
        ));
    }
    if delivery.mode != "webhook" {
        return Err(JsonRpcError::unsupported("deliveryMode", &delivery.mode));
    }
    if !state.config.webhook.enabled {
        return Err(JsonRpcError::unsupported("deliveryMode", "webhook"));
    }
    let Some(def) = state.registry.get(&name) else {
        return Err(JsonRpcError::not_found(
            "event",
            format!("unknown event type \"{name}\""),
        ));
    };
    if !def.delivery.contains(&DeliveryMode::Webhook) {
        return Err(JsonRpcError::unsupported("deliveryMode", "webhook"));
    }
    crate::mapping::validate_event_params(&state.filters, &name, sub_params.as_ref())?;
    let url = parse_callback_url(&delivery.url, state.config.webhook.allow_insecure_urls)?;
    let secret_value = delivery.secret.as_deref().ok_or_else(|| {
        JsonRpcError::invalid_params("delivery.secret is required for webhook subscriptions")
    })?;
    let secret = parse_whsec(secret_value)
        .map_err(|error| JsonRpcError::invalid_params(format!("delivery.secret: {error}")))?;

    let granted_ms = grant_ttl_ms(&state.config.webhook, ttl_ms);
    let refresh_at =
        Utc::now() + chrono::Duration::milliseconds(i64::try_from(granted_ms).unwrap_or(i64::MAX));

    // Normalize the requested start position into an internal cursor and
    // detect truncation (stale cursor / maxAgeMs floor / retention ceiling)
    // without consuming any deliverable events (max_events = 0).
    let read = state.buffer.read(
        &name,
        cursor.as_deref(),
        max_age_ms,
        Some(0),
        sub_params.as_ref(),
        state.filters.get(&name).map(|f| f.as_ref()),
    );

    let params_value = sub_params.unwrap_or(Value::Null);
    let key = SubKey {
        principal: principal.clone(),
        url: delivery.url.clone(),
        name: name.clone(),
        params_canonical: canonical_json(&params_value),
    };
    let outcome = state
        .subs
        .upsert(
            key.clone(),
            params_value,
            secret.clone(),
            Some(refresh_at),
            Some(read.cursor.clone()),
        )
        .map_err(|StoreError::QuotaExceeded { max }| {
            JsonRpcError::resource_exhausted("subscriptions", Some(max as u64))
        })?;
    let sub_id = outcome.sub().id.clone();

    // Endpoint verification before first activation, cached per
    // (principal, url). The worker only delivers to verified subscriptions,
    // so the just-upserted record stays dormant until this succeeds.
    if !state.subs.is_verified(&principal, &delivery.url) {
        if let Err(reason) = challenge::verify_endpoint(&state, &url, &sub_id, &secret).await {
            state.subs.remove(&key);
            tracing::warn!(sub_id = %sub_id, url = %delivery.url, reason, "endpoint verification failed");
            return Err(JsonRpcError::callback_endpoint_error(reason));
        }
        state.subs.set_verified(&principal, &delivery.url);
        tracing::info!(sub_id = %sub_id, url = %delivery.url, "endpoint verified");
    }

    let refresh_before = Some(iso8601(refresh_at));
    match outcome {
        UpsertOutcome::Created(sub) => {
            tracing::info!(sub_id = %sub.id, event = %name, "webhook subscription created");
            Ok(SubscribeResult {
                id: sub.id,
                refresh_before,
                cursor: Some(read.cursor),
                truncated: read.truncated,
                delivery_status: None,
            })
        }
        UpsertOutcome::Refreshed(snapshot) => {
            tracing::debug!(sub_id = %snapshot.id, event = %name, "webhook subscription refreshed");
            // Live refresh: the supplied cursor is a no-op; report the
            // stored safe watermark so the client's cursor advances during
            // quiet periods.
            let delivery_status = Some(snapshot.delivery_status());
            Ok(SubscribeResult {
                id: snapshot.id,
                refresh_before,
                cursor: Some(snapshot.cursor.unwrap_or(read.cursor)),
                truncated: false,
                delivery_status,
            })
        }
    }
}

pub async fn handle_unsubscribe(
    state: Arc<AppState>,
    principal: Option<String>,
    params: UnsubscribeParams,
) -> Result<serde_json::Value, JsonRpcError> {
    let Some(principal) = principal else {
        return Err(JsonRpcError::forbidden(
            "events/unsubscribe requires an authenticated principal",
        ));
    };
    if state.registry.get(&params.name).is_none() {
        return Err(JsonRpcError::not_found(
            "event",
            format!("unknown event type \"{}\"", params.name),
        ));
    }
    let params_value = params.params.unwrap_or(Value::Null);
    let key = SubKey {
        principal,
        url: params.delivery.url.clone(),
        name: params.name.clone(),
        params_canonical: canonical_json(&params_value),
    };
    match state.subs.remove(&key) {
        Some(sub) => {
            tracing::info!(sub_id = %sub.id, event = %sub.key.name, "webhook subscription removed");
            Ok(json!({}))
        }
        None => Err(JsonRpcError::not_found(
            "subscription",
            "no subscription matches the supplied (principal, url, name, params) key",
        )),
    }
}

/// TTL negotiation (sketch §Subscription TTL, ARCHITECTURE.md): absent ⇒
/// server default (the cap); `null` ⇒ no-expiry requested but this server is
/// unwilling and grants the finite cap; a value is clamped into
/// `[minTtlMs, ttlCapMs]`.
fn grant_ttl_ms(cfg: &WebhookSettings, ttl_ms: Option<Option<u64>>) -> u64 {
    let cap = cfg.ttl_cap_ms;
    let floor = cfg.min_ttl_ms.min(cap);
    match ttl_ms {
        None | Some(None) => cap,
        Some(Some(v)) => v.clamp(floor, cap),
    }
}

/// `delivery.url` static validation (sketch §Webhook Security → TLS
/// requirement): https only, unless `allowInsecureUrls` also permits http.
fn parse_callback_url(raw: &str, allow_insecure: bool) -> Result<Url, JsonRpcError> {
    let url = Url::parse(raw)
        .map_err(|error| JsonRpcError::invalid_params(format!("delivery.url: {error}")))?;
    match url.scheme() {
        "https" => {}
        "http" if allow_insecure => {}
        other => {
            return Err(JsonRpcError::invalid_params(format!(
                "delivery.url must use https (got \"{other}\")"
            )))
        }
    }
    if url.host().is_none() {
        return Err(JsonRpcError::invalid_params(
            "delivery.url must include a host",
        ));
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(min_ttl_ms: u64, ttl_cap_ms: u64) -> WebhookSettings {
        WebhookSettings {
            enabled: true,
            ttl_cap_ms,
            min_ttl_ms,
            max_subscriptions_per_principal: 16,
            allow_insecure_urls: false,
            suspend_after_failures: 5,
        }
    }

    #[test]
    fn ttl_grant_clamps_into_min_cap_window() {
        let cfg = settings(10_000, 1_800_000);
        assert_eq!(grant_ttl_ms(&cfg, None), 1_800_000, "absent = default cap");
        assert_eq!(
            grant_ttl_ms(&cfg, Some(None)),
            1_800_000,
            "null = no-expiry refused, finite cap granted"
        );
        assert_eq!(grant_ttl_ms(&cfg, Some(Some(1))), 10_000, "clamped up");
        assert_eq!(grant_ttl_ms(&cfg, Some(Some(60_000))), 60_000);
        assert_eq!(
            grant_ttl_ms(&cfg, Some(Some(u64::MAX))),
            1_800_000,
            "clamped down"
        );
    }

    #[test]
    fn ttl_grant_survives_min_above_cap_misconfig() {
        let cfg = settings(5_000, 1_000);
        assert_eq!(grant_ttl_ms(&cfg, Some(Some(2))), 1_000);
        assert_eq!(grant_ttl_ms(&cfg, None), 1_000);
    }

    #[test]
    fn callback_url_requires_https_unless_insecure_allowed() {
        assert!(parse_callback_url("https://example.com/hooks", false).is_ok());
        let err = parse_callback_url("http://example.com/hooks", false).unwrap_err();
        assert_eq!(err.code, mcp_events_wire::INVALID_PARAMS);
        assert!(parse_callback_url("http://example.com/hooks", true).is_ok());
        assert!(parse_callback_url("ftp://example.com/x", true).is_err());
        assert!(parse_callback_url("not a url", true).is_err());
        assert!(parse_callback_url("https://", true).is_err());
    }
}
