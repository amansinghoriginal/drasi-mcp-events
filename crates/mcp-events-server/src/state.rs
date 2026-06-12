//! Shared application state (pinned by ARCHITECTURE.md; used by both the
//! server-core and webhook components).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use mcp_events_engine::{BufferConfig, EventBuffer, ParamFilter, Registry, SubscriptionStore};

use crate::config::ServerConfig;

pub struct AppState {
    pub config: ServerConfig,
    pub registry: Registry,
    pub buffer: EventBuffer,
    // Read by the webhook component (separate task); allow until it lands.
    #[allow(dead_code)]
    pub subs: SubscriptionStore,
    /// Per event name (`Arc<dyn Fn>`).
    pub filters: HashMap<String, Arc<ParamFilter>>,
    /// Outbound client: redirects disabled (webhook SSRF hardening),
    /// connect timeout 5s, request timeout 10s.
    #[allow(dead_code)]
    pub http: reqwest::Client,
}

impl AppState {
    pub fn new(config: ServerConfig) -> anyhow::Result<Arc<Self>> {
        config.validate()?;
        let (defs, filters) = crate::mapping::build_event_model(&config);
        let registry = Registry::new(defs);
        let max_age = config
            .buffer
            .max_age_ms
            .map(|ms| chrono::Duration::milliseconds(i64::try_from(ms).unwrap_or(i64::MAX)));
        let buffer = EventBuffer::new(BufferConfig {
            max_events_per_type: config.buffer.max_events_per_type,
            max_age,
        });
        let subs = SubscriptionStore::new(config.webhook.max_subscriptions_per_principal);
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .context("building outbound HTTP client")?;
        Ok(Arc::new(Self {
            config,
            registry,
            buffer,
            subs,
            filters,
            http,
        }))
    }
}
