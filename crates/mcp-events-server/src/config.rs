//! YAML server configuration (camelCase keys; schema pinned by
//! `docs/ARCHITECTURE.md` §crate: mcp-events-server).

use std::path::Path;

use anyhow::Context as _;
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// Bearer token -> principal mapping; empty = unauthenticated server.
    #[serde(default)]
    pub auth_tokens: Vec<AuthToken>,
    #[serde(default)]
    pub event_modeling: EventModeling,
    #[serde(default)]
    pub buffer: BufferSettings,
    pub feed: FeedSettings,
    pub queries: Vec<QueryConfig>,
    #[serde(default)]
    pub push: PushSettings,
    #[serde(default)]
    pub poll: PollSettings,
    #[serde(default)]
    pub webhook: WebhookSettings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthToken {
    pub token: String,
    pub principal: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EventModeling {
    #[default]
    Single,
    PerChange,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BufferSettings {
    #[serde(default = "default_max_events_per_type")]
    pub max_events_per_type: usize,
    /// Retention window; explicit `null` disables age-based eviction.
    #[serde(default = "default_max_age_ms")]
    pub max_age_ms: Option<u64>,
}

impl Default for BufferSettings {
    fn default() -> Self {
        Self {
            max_events_per_type: default_max_events_per_type(),
            max_age_ms: default_max_age_ms(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FeedKind {
    Mock,
    DrasiSse,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FeedSettings {
    pub kind: FeedKind,
    /// mock only; defaults to the first configured query id.
    #[serde(default)]
    pub query_id: Option<String>,
    /// mock only.
    #[serde(default)]
    pub interval_ms: Option<u64>,
    /// drasiSse only: full SSE reaction URL (e.g. `http://localhost:8081/events`).
    #[serde(default)]
    pub url: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryConfig {
    pub id: String,
    #[serde(default)]
    pub description: Option<String>,
    /// JSON Schema of the query's projected row (RETURN aliases).
    #[serde(default)]
    pub payload_schema: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PushSettings {
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
}

impl Default for PushSettings {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PollSettings {
    #[serde(default = "default_next_poll_ms")]
    pub next_poll_ms: u64,
}

impl Default for PollSettings {
    fn default() -> Self {
        Self {
            next_poll_ms: default_next_poll_ms(),
        }
    }
}

// Several knobs are read only by the webhook component (separate task).
#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebhookSettings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_ttl_cap_ms")]
    pub ttl_cap_ms: u64,
    #[serde(default = "default_min_ttl_ms")]
    pub min_ttl_ms: u64,
    #[serde(default = "default_max_subscriptions_per_principal")]
    pub max_subscriptions_per_principal: usize,
    /// true => permit http:// and private IPs (LOCAL TESTING ONLY, nonconformant).
    #[serde(default)]
    pub allow_insecure_urls: bool,
    #[serde(default = "default_suspend_after_failures")]
    pub suspend_after_failures: u32,
}

impl Default for WebhookSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            ttl_cap_ms: default_ttl_cap_ms(),
            min_ttl_ms: default_min_ttl_ms(),
            max_subscriptions_per_principal: default_max_subscriptions_per_principal(),
            allow_insecure_urls: false,
            suspend_after_failures: default_suspend_after_failures(),
        }
    }
}

fn default_host() -> String {
    "127.0.0.1".to_owned()
}
fn default_port() -> u16 {
    8090
}
fn default_max_events_per_type() -> usize {
    10_000
}
fn default_max_age_ms() -> Option<u64> {
    Some(600_000)
}
fn default_heartbeat_interval_ms() -> u64 {
    15_000
}
fn default_next_poll_ms() -> u64 {
    2_000
}
fn default_ttl_cap_ms() -> u64 {
    1_800_000
}
fn default_min_ttl_ms() -> u64 {
    10_000
}
fn default_max_subscriptions_per_principal() -> usize {
    16
}
fn default_suspend_after_failures() -> u32 {
    5
}

impl ServerConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        let cfg: Self = serde_yaml::from_str(&raw)
            .with_context(|| format!("parsing config file {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.queries.is_empty(),
            "config must declare at least one query"
        );
        if self.feed.kind == FeedKind::DrasiSse {
            anyhow::ensure!(
                self.feed.url.is_some(),
                "feed.kind = drasiSse requires feed.url"
            );
        }
        Ok(())
    }

    pub fn principal_for_token(&self, token: &str) -> Option<String> {
        self.auth_tokens
            .iter()
            .find(|t| t.token == token)
            .map(|t| t.principal.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The sample from ARCHITECTURE.md, verbatim modulo the commented-out url.
    const SAMPLE: &str = r#"
host: 127.0.0.1
port: 8090
authTokens:
  - { token: devtoken, principal: dev@example.com }
eventModeling: single
buffer: { maxEventsPerType: 10000, maxAgeMs: 600000 }
feed:
  kind: mock
  queryId: high-value-orders
  intervalMs: 2000
queries:
  - id: high-value-orders
    description: "Rows entering/leaving/changing in the high-value-orders continuous query"
    payloadSchema: {}
push: { heartbeatIntervalMs: 15000 }
poll: { nextPollMs: 2000 }
webhook:
  enabled: true
  ttlCapMs: 1800000
  minTtlMs: 10000
  maxSubscriptionsPerPrincipal: 16
  allowInsecureUrls: false
  suspendAfterFailures: 5
"#;

    #[test]
    fn parses_architecture_sample() {
        let cfg: ServerConfig = serde_yaml::from_str(SAMPLE).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 8090);
        assert_eq!(cfg.event_modeling, EventModeling::Single);
        assert_eq!(cfg.buffer.max_events_per_type, 10_000);
        assert_eq!(cfg.buffer.max_age_ms, Some(600_000));
        assert_eq!(cfg.feed.kind, FeedKind::Mock);
        assert_eq!(cfg.feed.interval_ms, Some(2000));
        assert_eq!(cfg.queries.len(), 1);
        assert_eq!(cfg.queries[0].id, "high-value-orders");
        assert_eq!(cfg.queries[0].payload_schema, Some(serde_json::json!({})));
        assert_eq!(cfg.push.heartbeat_interval_ms, 15_000);
        assert_eq!(cfg.poll.next_poll_ms, 2_000);
        assert!(cfg.webhook.enabled);
        assert!(!cfg.webhook.allow_insecure_urls);
        assert_eq!(cfg.webhook.suspend_after_failures, 5);
        assert_eq!(
            cfg.principal_for_token("devtoken").as_deref(),
            Some("dev@example.com")
        );
        assert_eq!(cfg.principal_for_token("nope"), None);
    }

    #[test]
    fn minimal_config_uses_defaults() {
        let cfg: ServerConfig = serde_yaml::from_str(
            "feed: { kind: mock }\nqueries:\n  - id: q1\n",
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 8090);
        assert_eq!(cfg.event_modeling, EventModeling::Single);
        assert_eq!(cfg.buffer.max_events_per_type, 10_000);
        assert!(!cfg.webhook.enabled);
        assert_eq!(cfg.webhook.ttl_cap_ms, 1_800_000);
        assert!(cfg.auth_tokens.is_empty());
    }

    #[test]
    fn per_change_and_drasi_sse_parse() {
        let cfg: ServerConfig = serde_yaml::from_str(
            "eventModeling: perChange\nfeed: { kind: drasiSse, url: \"http://localhost:8081/events\" }\nqueries:\n  - id: q1\n",
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.event_modeling, EventModeling::PerChange);
        assert_eq!(cfg.feed.kind, FeedKind::DrasiSse);
    }

    #[test]
    fn drasi_sse_without_url_is_rejected() {
        let cfg: ServerConfig =
            serde_yaml::from_str("feed: { kind: drasiSse }\nqueries:\n  - id: q1\n").unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn empty_queries_rejected_and_unknown_fields_rejected() {
        let cfg: ServerConfig =
            serde_yaml::from_str("feed: { kind: mock }\nqueries: []\n").unwrap();
        assert!(cfg.validate().is_err());
        let err = serde_yaml::from_str::<ServerConfig>(
            "feed: { kind: mock }\nqueries:\n  - id: q1\nbogusField: 1\n",
        );
        assert!(err.is_err());
    }

    #[test]
    fn null_max_age_disables_retention_window() {
        let cfg: ServerConfig = serde_yaml::from_str(
            "feed: { kind: mock }\nqueries:\n  - id: q1\nbuffer: { maxEventsPerType: 5, maxAgeMs: null }\n",
        )
        .unwrap();
        assert_eq!(cfg.buffer.max_events_per_type, 5);
        assert_eq!(cfg.buffer.max_age_ms, None);
    }
}
