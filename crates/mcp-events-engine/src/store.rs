//! Webhook subscription store (design sketch §Subscription Identity,
//! §Subscription TTL, §Webhook Event Delivery, §Webhook Delivery Status).
//!
//! Subscriptions are keyed by `(principal, url, name, params_canonical)` with
//! a deterministic derived id. Decisions taken where the sketch is silent
//! (see SPEC-GAPS findings): an upsert against a lapsed-but-unswept key is a
//! fresh create (adopts the supplied cursor); the per-principal quota counts
//! only non-lapsed subscriptions; a refresh never moves a live subscription's
//! watermark cursor; refresh/reactivate resets the consecutive-failure streak
//! but `lastError`/`failedSince` are only cleared by a successful delivery.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::iso8601;

/// Webhook delivery `lastError` categories (design sketch §Webhook Delivery
/// Status) — never raw endpoint output.
pub mod error_category {
    pub const CONNECTION_REFUSED: &str = "connection_refused";
    pub const TIMEOUT: &str = "timeout";
    pub const TLS_ERROR: &str = "tls_error";
    pub const HTTP_4XX: &str = "http_4xx";
    pub const HTTP_5XX: &str = "http_5xx";
    pub const CHALLENGE_FAILED: &str = "challenge_failed";
}

/// Compound subscription key: `(principal, delivery.url, name, params)` with
/// params in canonical-JSON form (see [`crate::canonical_json`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubKey {
    pub principal: String,
    pub url: String,
    pub name: String,
    pub params_canonical: String,
}

/// `"sub_"` + first 16 hex chars of SHA-256 over the newline-joined key
/// components. Stable across refreshes and restarts.
pub fn derive_sub_id(key: &SubKey) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.principal.as_bytes());
    hasher.update(b"\n");
    hasher.update(key.url.as_bytes());
    hasher.update(b"\n");
    hasher.update(key.name.as_bytes());
    hasher.update(b"\n");
    hasher.update(key.params_canonical.as_bytes());
    let digest = hasher.finalize();
    let hex = hex::encode(digest);
    format!("sub_{}", &hex[..16])
}

#[derive(Clone, Debug)]
pub struct WebhookSub {
    pub id: String,
    pub key: SubKey,
    pub params: Value,
    pub secret: Vec<u8>,
    /// `None` = no expiry granted.
    pub refresh_before: Option<DateTime<Utc>>,
    /// Safe-to-persist watermark maintained by delivery bookkeeping
    /// (`update_cursor`); seeded from the subscribe-time cursor on create.
    pub cursor: Option<String>,
    pub active: bool,
    pub last_delivery_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub failed_since: Option<DateTime<Utc>>,
    pub verified: bool,
}

impl WebhookSub {
    pub fn is_lapsed(&self, now: DateTime<Utc>) -> bool {
        self.refresh_before.is_some_and(|t| t <= now)
    }

    /// Wire-shaped delivery status snapshot for subscribe/refresh responses.
    pub fn delivery_status(&self) -> mcp_events_wire::DeliveryStatus {
        mcp_events_wire::DeliveryStatus {
            active: self.active,
            last_delivery_at: self.last_delivery_at.map(iso8601),
            last_error: self.last_error.clone(),
            failed_since: self.failed_since.map(iso8601),
        }
    }
}

/// Outcome of an idempotent upsert. `Refreshed` carries the updated record
/// except that `active` reports the PRE-refresh value, so callers can build a
/// `deliveryStatus` that reflects a suspension the refresh just lifted
/// (sketch §Webhook Delivery Status).
#[derive(Clone, Debug)]
pub enum UpsertOutcome {
    Created(WebhookSub),
    Refreshed(WebhookSub),
}

impl UpsertOutcome {
    pub fn sub(&self) -> &WebhookSub {
        match self {
            UpsertOutcome::Created(s) | UpsertOutcome::Refreshed(s) => s,
        }
    }

    pub fn is_created(&self) -> bool {
        matches!(self, UpsertOutcome::Created(_))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("subscription quota exceeded for principal (max {max})")]
    QuotaExceeded { max: usize },
}

struct StoreState {
    subs: HashMap<String, WebhookSub>,
    /// Consecutive delivery-failure streak per subscription id.
    failures: HashMap<String, u32>,
    /// Endpoint-verification cache per `(principal, url)`.
    verified: HashSet<(String, String)>,
}

struct StoreInner {
    max_per_principal: usize,
    state: Mutex<StoreState>,
}

/// Thread-safe, cheap-clone webhook subscription store.
#[derive(Clone)]
pub struct SubscriptionStore(Arc<StoreInner>);

impl SubscriptionStore {
    pub fn new(max_per_principal: usize) -> Self {
        Self(Arc::new(StoreInner {
            max_per_principal,
            state: Mutex::new(StoreState {
                subs: HashMap::new(),
                failures: HashMap::new(),
                verified: HashSet::new(),
            }),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, StoreState> {
        self.0.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Idempotent upsert (design sketch §Subscribing / §Subscription
    /// Identity). Existing live key → refresh: secret replaced, TTL
    /// re-granted, reactivated, supplied cursor ignored (live no-op).
    /// Absent or lapsed key → fresh create adopting the supplied cursor,
    /// subject to the per-principal quota.
    pub fn upsert(
        &self,
        key: SubKey,
        params: Value,
        secret: Vec<u8>,
        refresh_before: Option<DateTime<Utc>>,
        cursor: Option<String>,
    ) -> Result<UpsertOutcome, StoreError> {
        let id = derive_sub_id(&key);
        let now = Utc::now();
        let mut st = self.lock();
        let verified = st
            .verified
            .contains(&(key.principal.clone(), key.url.clone()));

        let existing_is_live = st
            .subs
            .get(&id)
            .is_some_and(|s| !s.is_lapsed(now));

        if existing_is_live {
            st.failures.insert(id.clone(), 0);
            if let Some(existing) = st.subs.get_mut(&id) {
                let prev_active = existing.active;
                existing.params = params;
                existing.secret = secret;
                existing.refresh_before = refresh_before;
                existing.active = true;
                existing.verified = verified;
                // Live subscription: supplied cursor is a no-op; the stored
                // watermark stays authoritative.
                let mut snapshot = existing.clone();
                snapshot.active = prev_active;
                return Ok(UpsertOutcome::Refreshed(snapshot));
            }
        }

        // Fresh create (also replaces a lapsed record in place).
        let in_use = st
            .subs
            .values()
            .filter(|s| s.key.principal == key.principal && s.id != id && !s.is_lapsed(now))
            .count();
        if in_use >= self.0.max_per_principal {
            return Err(StoreError::QuotaExceeded {
                max: self.0.max_per_principal,
            });
        }
        let sub = WebhookSub {
            id: id.clone(),
            key,
            params,
            secret,
            refresh_before,
            cursor,
            active: true,
            last_delivery_at: None,
            last_error: None,
            failed_since: None,
            verified,
        };
        st.failures.insert(id.clone(), 0);
        st.subs.insert(id, sub.clone());
        Ok(UpsertOutcome::Created(sub))
    }

    pub fn remove(&self, key: &SubKey) -> Option<WebhookSub> {
        let id = derive_sub_id(key);
        let mut st = self.lock();
        st.failures.remove(&id);
        st.subs.remove(&id)
    }

    pub fn get(&self, key: &SubKey) -> Option<WebhookSub> {
        let id = derive_sub_id(key);
        self.lock().subs.get(&id).cloned()
    }

    /// All subscriptions (including suspended ones — callers filter on
    /// `active`) for one event name.
    pub fn list_for_event(&self, name: &str) -> Vec<WebhookSub> {
        self.lock()
            .subs
            .values()
            .filter(|s| s.key.name == name)
            .cloned()
            .collect()
    }

    /// Removes and returns every subscription whose grant has lapsed
    /// (`refresh_before <= now`). No-expiry subscriptions never lapse.
    pub fn expire_lapsed(&self, now: DateTime<Utc>) -> Vec<WebhookSub> {
        let mut st = self.lock();
        let ids: Vec<String> = st
            .subs
            .values()
            .filter(|s| s.is_lapsed(now))
            .map(|s| s.id.clone())
            .collect();
        let mut removed = Vec::with_capacity(ids.len());
        for id in ids {
            st.failures.remove(&id);
            if let Some(sub) = st.subs.remove(&id) {
                tracing::debug!(sub_id = %sub.id, name = %sub.key.name, "webhook subscription lapsed");
                removed.push(sub);
            }
        }
        removed
    }

    /// Records a successful delivery: clears the failure streak and the
    /// `lastError`/`failedSince` bookkeeping.
    pub fn mark_delivery_ok(&self, id: &str, at: DateTime<Utc>) {
        let mut st = self.lock();
        st.failures.insert(id.to_owned(), 0);
        if let Some(s) = st.subs.get_mut(id) {
            s.last_delivery_at = Some(at);
            s.last_error = None;
            s.failed_since = None;
        }
    }

    /// Records a failed delivery. `failed_since` marks the start of the
    /// current streak. After `suspend_after` consecutive failures delivery is
    /// suspended (`active = false`); `suspend_after == 0` disables suspension.
    pub fn mark_delivery_failed(
        &self,
        id: &str,
        error_category: &str,
        at: DateTime<Utc>,
        suspend_after: u32,
    ) {
        let mut st = self.lock();
        let state = &mut *st;
        if let Some(s) = state.subs.get_mut(id) {
            s.last_error = Some(error_category.to_owned());
            if s.failed_since.is_none() {
                s.failed_since = Some(at);
            }
            let count = state.failures.entry(id.to_owned()).or_insert(0);
            *count += 1;
            if suspend_after > 0 && *count >= suspend_after && s.active {
                s.active = false;
                tracing::warn!(
                    sub_id = id,
                    failures = *count,
                    category = error_category,
                    "suspending webhook delivery after consecutive failures"
                );
            }
        }
    }

    /// Marks `(principal, url)` as verified and flips the flag on every
    /// matching stored subscription.
    pub fn set_verified(&self, principal: &str, url: &str) {
        let mut st = self.lock();
        st.verified.insert((principal.to_owned(), url.to_owned()));
        for s in st.subs.values_mut() {
            if s.key.principal == principal && s.key.url == url {
                s.verified = true;
            }
        }
    }

    pub fn is_verified(&self, principal: &str, url: &str) -> bool {
        self.lock()
            .verified
            .contains(&(principal.to_owned(), url.to_owned()))
    }

    /// Resumes delivery for a suspended subscription and resets its
    /// consecutive-failure streak.
    pub fn reactivate(&self, id: &str) {
        let mut st = self.lock();
        st.failures.insert(id.to_owned(), 0);
        if let Some(s) = st.subs.get_mut(id) {
            s.active = true;
        }
    }

    /// Updates the safe-to-persist watermark cursor (delivery bookkeeping).
    pub fn update_cursor(&self, id: &str, cursor: Option<String>) {
        if let Some(s) = self.lock().subs.get_mut(id) {
            s.cursor = cursor;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use serde_json::json;

    fn key(principal: &str, url: &str, name: &str, params: &Value) -> SubKey {
        SubKey {
            principal: principal.to_owned(),
            url: url.to_owned(),
            name: name.to_owned(),
            params_canonical: crate::canonical_json(params),
        }
    }

    fn upsert_simple(
        store: &SubscriptionStore,
        k: &SubKey,
        secret: &[u8],
        rb: Option<DateTime<Utc>>,
        cursor: Option<&str>,
    ) -> Result<UpsertOutcome, StoreError> {
        store.upsert(
            k.clone(),
            json!({}),
            secret.to_vec(),
            rb,
            cursor.map(str::to_owned),
        )
    }

    fn future() -> Option<DateTime<Utc>> {
        Some(Utc::now() + Duration::hours(1))
    }

    #[test]
    fn id_is_sub_prefixed_16_hex_and_deterministic_over_key() {
        let k = key("dev@example.com", "https://h/x", "orders.changed", &json!({"a":1}));
        let id1 = derive_sub_id(&k);
        let id2 = derive_sub_id(&k);
        assert_eq!(id1, id2);
        let hex_part = id1.strip_prefix("sub_").expect("sub_ prefix");
        assert_eq!(hex_part.len(), 16);
        assert!(hex_part.chars().all(|c| c.is_ascii_hexdigit()));

        for other in [
            key("other@example.com", "https://h/x", "orders.changed", &json!({"a":1})),
            key("dev@example.com", "https://h/y", "orders.changed", &json!({"a":1})),
            key("dev@example.com", "https://h/x", "orders.deleted", &json!({"a":1})),
            key("dev@example.com", "https://h/x", "orders.changed", &json!({"a":2})),
        ] {
            assert_ne!(derive_sub_id(&other), id1);
        }
    }

    #[test]
    fn upsert_creates_with_supplied_cursor_and_grant() {
        let store = SubscriptionStore::new(4);
        let k = key("p", "https://h/cb", "n", &json!({"severity":"P1"}));
        let rb = future();
        let out = upsert_simple(&store, &k, b"secret-a", rb, Some("c0")).unwrap();
        assert!(out.is_created());
        let s = out.sub();
        assert_eq!(s.id, derive_sub_id(&k));
        assert_eq!(s.secret, b"secret-a");
        assert_eq!(s.refresh_before, rb);
        assert_eq!(s.cursor.as_deref(), Some("c0"));
        assert!(s.active);
        assert!(!s.verified);
        assert!(s.last_delivery_at.is_none());
        let got = store.get(&k).expect("stored");
        assert_eq!(got.id, s.id);
    }

    #[test]
    fn refresh_rotates_secret_regrants_ttl_and_ignores_supplied_cursor() {
        let store = SubscriptionStore::new(4);
        let k = key("p", "https://h/cb", "n", &json!({}));
        let out = upsert_simple(&store, &k, b"old-secret", future(), Some("c0")).unwrap();
        let id = out.sub().id.clone();
        // Delivery bookkeeping advanced the watermark.
        store.update_cursor(&id, Some("w9".into()));

        let rb2 = Some(Utc::now() + Duration::hours(2));
        let out2 = upsert_simple(&store, &k, b"new-secret", rb2, Some("stale-cursor")).unwrap();
        assert!(!out2.is_created());
        let snap = out2.sub();
        assert_eq!(snap.secret, b"new-secret");
        assert_eq!(snap.refresh_before, rb2);
        // Live subscription: supplied cursor is a no-op.
        assert_eq!(snap.cursor.as_deref(), Some("w9"));
        let stored = store.get(&k).unwrap();
        assert_eq!(stored.cursor.as_deref(), Some("w9"));
        assert_eq!(stored.secret, b"new-secret");
    }

    #[test]
    fn refresh_reports_pre_refresh_active_but_reactivates_store_state() {
        let store = SubscriptionStore::new(4);
        let k = key("p", "https://h/cb", "n", &json!({}));
        let id = upsert_simple(&store, &k, b"s", future(), None).unwrap().sub().id.clone();
        let t = Utc::now();
        for _ in 0..3 {
            store.mark_delivery_failed(&id, error_category::HTTP_5XX, t, 3);
        }
        assert!(!store.get(&k).unwrap().active);

        let out = upsert_simple(&store, &k, b"s2", future(), None).unwrap();
        match &out {
            UpsertOutcome::Refreshed(snap) => {
                assert!(!snap.active, "snapshot reports pre-refresh suspension");
                assert_eq!(snap.last_error.as_deref(), Some(error_category::HTTP_5XX));
                assert!(snap.failed_since.is_some());
                let status = snap.delivery_status();
                assert!(!status.active);
                assert_eq!(status.last_error.as_deref(), Some("http_5xx"));
            }
            other => panic!("expected Refreshed, got {other:?}"),
        }
        assert!(store.get(&k).unwrap().active, "store state reactivated");
        // Failure streak was reset by the refresh: 2 more failures < 3 keep it active.
        store.mark_delivery_failed(&id, error_category::TIMEOUT, t, 3);
        store.mark_delivery_failed(&id, error_category::TIMEOUT, t, 3);
        assert!(store.get(&k).unwrap().active);
    }

    #[test]
    fn quota_applies_per_principal_to_new_keys_only() {
        let store = SubscriptionStore::new(2);
        let k1 = key("p", "https://h/1", "n", &json!({}));
        let k2 = key("p", "https://h/2", "n", &json!({}));
        let k3 = key("p", "https://h/3", "n", &json!({}));
        upsert_simple(&store, &k1, b"s", future(), None).unwrap();
        upsert_simple(&store, &k2, b"s", future(), None).unwrap();
        let err = upsert_simple(&store, &k3, b"s", future(), None).unwrap_err();
        assert_eq!(err, StoreError::QuotaExceeded { max: 2 });

        // Refreshing an existing key does not hit the quota.
        assert!(!upsert_simple(&store, &k1, b"s2", future(), None).unwrap().is_created());

        // A different principal has its own quota.
        let other = key("q", "https://h/1", "n", &json!({}));
        assert!(upsert_simple(&store, &other, b"s", future(), None).unwrap().is_created());
    }

    #[test]
    fn lapsed_subscriptions_do_not_count_toward_quota() {
        let store = SubscriptionStore::new(1);
        let lapsed_key = key("p", "https://h/old", "n", &json!({}));
        upsert_simple(&store, &lapsed_key, b"s", Some(Utc::now() - Duration::hours(1)), None)
            .unwrap();
        let fresh = key("p", "https://h/new", "n", &json!({}));
        assert!(upsert_simple(&store, &fresh, b"s", future(), None).unwrap().is_created());
    }

    #[test]
    fn upsert_on_lapsed_key_is_a_fresh_create_adopting_the_cursor() {
        let store = SubscriptionStore::new(4);
        let k = key("p", "https://h/cb", "n", &json!({}));
        let id = upsert_simple(&store, &k, b"s", Some(Utc::now() - Duration::seconds(1)), Some("old"))
            .unwrap()
            .sub()
            .id
            .clone();
        store.update_cursor(&id, Some("w5".into()));
        store.mark_delivery_failed(&id, error_category::HTTP_4XX, Utc::now(), 5);

        let out = upsert_simple(&store, &k, b"s2", future(), Some("client-cursor")).unwrap();
        assert!(out.is_created(), "expired key re-subscribes as fresh");
        let s = out.sub();
        assert_eq!(s.cursor.as_deref(), Some("client-cursor"));
        assert!(s.last_error.is_none());
        assert!(s.failed_since.is_none());
        assert!(s.active);
    }

    #[test]
    fn expire_lapsed_removes_only_lapsed_grants() {
        let store = SubscriptionStore::new(8);
        let now = Utc::now();
        let lapsed = key("p", "https://h/1", "n", &json!({}));
        let live = key("p", "https://h/2", "n", &json!({}));
        let forever = key("p", "https://h/3", "n", &json!({}));
        upsert_simple(&store, &lapsed, b"s", Some(now - Duration::seconds(5)), None).unwrap();
        upsert_simple(&store, &live, b"s", Some(now + Duration::hours(1)), None).unwrap();
        upsert_simple(&store, &forever, b"s", None, None).unwrap();

        let removed = store.expire_lapsed(now);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].key, lapsed);
        assert!(store.get(&lapsed).is_none());
        assert!(store.get(&live).is_some());
        assert!(store.get(&forever).is_some());
        assert!(store.expire_lapsed(now).is_empty());

        // Boundary: a grant exactly at `now` is lapsed.
        let edge = key("p", "https://h/4", "n", &json!({}));
        upsert_simple(&store, &edge, b"s", Some(now), None).unwrap();
        assert_eq!(store.expire_lapsed(now).len(), 1);
    }

    #[test]
    fn suspension_after_n_consecutive_failures_and_recovery() {
        let store = SubscriptionStore::new(4);
        let k = key("p", "https://h/cb", "n", &json!({}));
        let id = upsert_simple(&store, &k, b"s", future(), None).unwrap().sub().id.clone();
        let t1 = Utc::now();
        let t2 = t1 + Duration::seconds(10);

        store.mark_delivery_failed(&id, error_category::CONNECTION_REFUSED, t1, 3);
        store.mark_delivery_failed(&id, error_category::TIMEOUT, t2, 3);
        let s = store.get(&k).unwrap();
        assert!(s.active, "below threshold stays active");
        assert_eq!(s.last_error.as_deref(), Some(error_category::TIMEOUT));
        assert_eq!(s.failed_since, Some(t1), "failedSince is start of streak");

        // A success clears the streak and the bookkeeping.
        store.mark_delivery_ok(&id, t2);
        let s = store.get(&k).unwrap();
        assert_eq!(s.last_delivery_at, Some(t2));
        assert!(s.last_error.is_none());
        assert!(s.failed_since.is_none());

        // Fresh streak of 3 suspends.
        for _ in 0..3 {
            store.mark_delivery_failed(&id, error_category::HTTP_5XX, t2, 3);
        }
        assert!(!store.get(&k).unwrap().active);

        // Reactivation resets the streak.
        store.reactivate(&id);
        assert!(store.get(&k).unwrap().active);
        store.mark_delivery_failed(&id, error_category::HTTP_5XX, t2, 3);
        store.mark_delivery_failed(&id, error_category::HTTP_5XX, t2, 3);
        assert!(store.get(&k).unwrap().active);
        store.mark_delivery_failed(&id, error_category::HTTP_5XX, t2, 3);
        assert!(!store.get(&k).unwrap().active);
    }

    #[test]
    fn suspend_after_zero_disables_suspension() {
        let store = SubscriptionStore::new(4);
        let k = key("p", "https://h/cb", "n", &json!({}));
        let id = upsert_simple(&store, &k, b"s", future(), None).unwrap().sub().id.clone();
        for _ in 0..10 {
            store.mark_delivery_failed(&id, error_category::HTTP_5XX, Utc::now(), 0);
        }
        assert!(store.get(&k).unwrap().active);
    }

    #[test]
    fn verification_cache_is_scoped_to_principal_and_url() {
        let store = SubscriptionStore::new(8);
        assert!(!store.is_verified("p", "https://h/cb"));
        let k = key("p", "https://h/cb", "n", &json!({"a":1}));
        upsert_simple(&store, &k, b"s", future(), None).unwrap();
        assert!(!store.get(&k).unwrap().verified);

        store.set_verified("p", "https://h/cb");
        assert!(store.is_verified("p", "https://h/cb"));
        assert!(!store.is_verified("q", "https://h/cb"), "other principal not covered");
        assert!(!store.is_verified("p", "https://h/other"), "other url not covered");
        assert!(store.get(&k).unwrap().verified, "existing sub flipped");

        // Varying params/name reuses the (principal, url) verification.
        let k2 = key("p", "https://h/cb", "other.event", &json!({"b":2}));
        let out = upsert_simple(&store, &k2, b"s", future(), None).unwrap();
        assert!(out.sub().verified);
    }

    #[test]
    fn remove_returns_the_subscription_once() {
        let store = SubscriptionStore::new(4);
        let k = key("p", "https://h/cb", "n", &json!({}));
        upsert_simple(&store, &k, b"s", future(), None).unwrap();
        let removed = store.remove(&k).expect("present");
        assert_eq!(removed.key, k);
        assert!(store.get(&k).is_none());
        assert!(store.remove(&k).is_none());
    }

    #[test]
    fn list_for_event_returns_matching_subs_including_suspended() {
        let store = SubscriptionStore::new(8);
        let ka = key("p", "https://h/1", "orders.changed", &json!({}));
        let kb = key("q", "https://h/2", "orders.changed", &json!({"x":1}));
        let kc = key("p", "https://h/1", "other.event", &json!({}));
        upsert_simple(&store, &ka, b"s", future(), None).unwrap();
        let id_b = upsert_simple(&store, &kb, b"s", future(), None).unwrap().sub().id.clone();
        upsert_simple(&store, &kc, b"s", future(), None).unwrap();
        store.mark_delivery_failed(&id_b, error_category::HTTP_4XX, Utc::now(), 1);

        let mut got: Vec<String> = store
            .list_for_event("orders.changed")
            .into_iter()
            .map(|s| s.key.url.clone())
            .collect();
        got.sort();
        assert_eq!(got, vec!["https://h/1", "https://h/2"]);
        assert!(store
            .list_for_event("orders.changed")
            .iter()
            .any(|s| !s.active));
        assert!(store.list_for_event("nope").is_empty());
    }

    #[test]
    fn update_cursor_sets_and_clears_the_watermark() {
        let store = SubscriptionStore::new(4);
        let k = key("p", "https://h/cb", "n", &json!({}));
        let id = upsert_simple(&store, &k, b"s", future(), None).unwrap().sub().id.clone();
        store.update_cursor(&id, Some("w1".into()));
        assert_eq!(store.get(&k).unwrap().cursor.as_deref(), Some("w1"));
        store.update_cursor(&id, None);
        assert!(store.get(&k).unwrap().cursor.is_none());
        // Unknown id is a no-op.
        store.update_cursor("sub_ffffffffffffffff", Some("x".into()));
    }

    #[test]
    fn params_canonicalization_makes_key_order_irrelevant() {
        let store = SubscriptionStore::new(4);
        let p1: Value = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
        let p2: Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        let k1 = key("p", "https://h/cb", "n", &p1);
        let k2 = key("p", "https://h/cb", "n", &p2);
        assert_eq!(derive_sub_id(&k1), derive_sub_id(&k2));
        store.upsert(k1, p1, b"s".to_vec(), future(), None).unwrap();
        let out = store.upsert(k2, p2, b"s2".to_vec(), future(), None).unwrap();
        assert!(!out.is_created(), "same canonical params = same subscription");
    }

    #[test]
    fn delivery_status_renders_wire_shape() {
        let store = SubscriptionStore::new(4);
        let k = key("p", "https://h/cb", "n", &json!({}));
        let id = upsert_simple(&store, &k, b"s", future(), None).unwrap().sub().id.clone();
        let at = Utc::now();
        store.mark_delivery_ok(&id, at);
        let status = store.get(&k).unwrap().delivery_status();
        assert!(status.active);
        assert_eq!(status.last_delivery_at, Some(crate::iso8601(at)));
        assert!(status.last_error.is_none());
        assert!(status.failed_since.is_none());
    }
}
