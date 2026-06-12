# Architecture & API Contract

This document is the **binding contract** for all crates in this workspace. Implementers MUST
conform to the signatures and conventions here so independently-built crates integrate cleanly.
The protocol behavior itself is specified by `docs/design-sketch-proposal.md` (the draft MCP
Events extension) and `docs/mcp-reference.md` (MCP base protocol, 2025-11-25). Where this
document and the design sketch conflict on wire behavior, **the design sketch wins** — and the
conflict should be reported as a finding.

## Ground rules (all implementers)

1. **Clean-room discipline**: the ONLY protocol sources are the two docs in `docs/`. Do not
   consult other MCP SDK implementations or any external events-extension code. Reading Drasi
   source (`/Users/aman/proto/server`, `/Users/aman/proto/core`) is allowed for Drasi
   integration details only.
2. **Log every "the spec didn't tell me" moment**: whenever the design sketch is ambiguous,
   underspecified, contradictory, or forces an arbitrary choice, record it (section, what was
   unclear, what you assumed). These findings are a primary deliverable of this project.
3. Wire JSON uses **camelCase** field names. Rust uses snake_case + `#[serde(rename_all = "camelCase")]`.
4. **null vs absent matters** in several places (`cursor`, `ttlMs`, `refreshBefore`). Convention:
   `Option<Option<T>>` + `#[serde(default, skip_serializing_if = "Option::is_none")]` where the
   distinction is meaningful (absent → `None`, `null` → `Some(None)`, value → `Some(Some(v))`).
   Where a field is always present but nullable (e.g. `refreshBefore` in `SubscribeResult`,
   `cursor` in `PollEventsResult`), use plain `Option<T>` and always serialize it.
5. Scope: **Streamable HTTP transport only** (single `POST /mcp` endpoint). stdio is out of
   scope for this prototype. Protocol version string: `"2025-11-25"`.
6. Each crate must have unit tests; run `cargo test -p <crate>` before declaring done.
7. Do not edit files owned by another component (ownership table below). Do not edit the
   workspace `Cargo.toml` or any crate `Cargo.toml` — they are pre-pinned. If you need a new
   dependency, it must already be in `[workspace.dependencies]`; report if something is missing.

## Workspace layout

```
crates/
  mcp-events-wire/     wire types: JSON-RPC 2.0 + MCP base subset + events extension (serde)
  mcp-events-engine/   event buffer (ring + cursors), registry, webhook subscription store
  drasi-feed/          Drasi SSE reaction consumer + mock feed generator
  mcp-events-server/   axum server: initialize, events/list|poll|stream|subscribe|unsubscribe
  mcp-events-client/   client library + `events-harness` binary
drasi/                 Drasi Server demo environment (docker compose, config, seed, runbook)
docs/                  specs + this contract
```

Dependency graph: `wire` ← `engine`, `feed`, `client`; `server` ← `wire` + `engine` + `feed`.
`feed` and `client` MUST NOT depend on `engine`.

---

## crate: mcp-events-wire

JSON-RPC:

```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)] #[serde(untagged)]
pub enum RequestId { Num(i64), Str(String) }

pub struct JsonRpcRequest { pub jsonrpc: String, #[serde(skip_serializing_if="Option::is_none")] pub id: Option<RequestId>, pub method: String, #[serde(skip_serializing_if="Option::is_none")] pub params: Option<serde_json::Value> }
pub struct JsonRpcResponse { ... } // result XOR error, standard JSON-RPC 2.0
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcError { pub code: i64, pub message: String, #[serde(skip_serializing_if="Option::is_none")] pub data: Option<serde_json::Value> }
```

Error code constants (i64): `PARSE_ERROR=-32700, INVALID_REQUEST=-32600, METHOD_NOT_FOUND=-32601,
INVALID_PARAMS=-32602, INTERNAL_ERROR=-32603, NOT_FOUND=-32011, FORBIDDEN=-32012,
RESOURCE_EXHAUSTED=-32013, UNSUPPORTED=-32014, CALLBACK_ENDPOINT_ERROR=-32015`.
Provide constructor helpers: `JsonRpcError::not_found(kind: &str, msg)`, `::forbidden(msg)`,
`::resource_exhausted(limit: &str, max: Option<u64>)`, `::unsupported(feature: &str, value: &str)`,
`::callback_endpoint_error(reason: &str)` — `data` payloads per design sketch §Error Codes.

MCP base subset (only what the server needs): `InitializeParams`, `InitializeResult`
(`protocol_version`, `capabilities`, `server_info`, optional `instructions`), `Implementation
{ name, version, optional title }`, `ServerCapabilities { events: Option<EventsCapability> , #[serde(flatten)] extra: Map }`,
`EventsCapability { list_changed: Option<bool> }`. Method/notification name constants:
`"initialize"`, `"ping"`, `"notifications/initialized"`.

Events extension types (per design sketch — field names/optionality exactly as specified there):

```rust
pub enum DeliveryMode { Poll, Push, Webhook }   // serde lowercase
pub struct EventDefinition { name, description: Option<String>, delivery: Vec<DeliveryMode>, input_schema: Option<Value>, payload_schema: Option<Value>, _meta: Option<Value> }
pub struct ListEventsParams { cursor: Option<String> }
pub struct ListEventsResult { events: Vec<EventDefinition>, next_cursor: Option<String> }
pub struct EventOccurrence { event_id, name, timestamp: String /*ISO8601*/, data: Value,
                             cursor: Option<Option<String>> /* absent for poll; null = no replay */,
                             _meta: Option<Value> }
pub struct PollEventsParams { name, params: Option<Value>, cursor: Option<String>, max_age_ms: Option<u64>, max_events: Option<u32> }
pub struct PollEventsResult { events: Vec<EventOccurrence>, cursor: Option<String> /* always serialized, nullable */, truncated: bool, has_more: bool, next_poll_ms: u64 }
pub struct StreamEventsParams { name, params: Option<Value>, cursor: Option<String>, max_age_ms: Option<u64> }
// notifications params:
pub struct EventsActiveParams { cursor: Option<String>, truncated: bool, _meta: Value }
pub struct EventsErrorParams { error: JsonRpcError, _meta: Value }
pub struct EventsTerminatedParams { error: JsonRpcError, _meta: Value }
pub struct EventsHeartbeatParams { cursor: Option<String>, _meta: Value }
pub struct DeliverySpec { mode: String /* "webhook" */, url: String, secret: Option<String> /* required on subscribe, absent on unsubscribe */ }
pub struct SubscribeParams { name, params: Option<Value>, delivery: DeliverySpec, cursor: Option<String>, max_age_ms: Option<u64>, ttl_ms: Option<Option<u64>> /* absent=server default, null=request no expiry */ }
pub struct DeliveryStatus { active: bool, last_delivery_at: Option<String>, last_error: Option<String>, failed_since: Option<String> }
pub struct SubscribeResult { id: String, refresh_before: Option<String> /* always serialized, nullable */, cursor: Option<String> /* always serialized, nullable */, truncated: bool, delivery_status: Option<DeliveryStatus> }
pub struct UnsubscribeParams { name, params: Option<Value>, delivery: DeliverySpec /* url only */ }
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WebhookControlBody { Gap { cursor: Option<String> }, Terminated { error: JsonRpcError }, Verification { challenge: String } }
```

Constants: method names `events/list`, `events/poll`, `events/stream`, `events/subscribe`,
`events/unsubscribe`; notification methods `notifications/events/active|event|heartbeat|error|terminated|list_changed`;
`META_SUBSCRIPTION_ID = "io.modelcontextprotocol/subscriptionId"`; webhook header names
(lowercase) `webhook-id`, `webhook-timestamp`, `webhook-signature`, `x-mcp-subscription-id`.

Standard Webhooks helpers (shared by server signer and harness verifier):

```rust
pub fn parse_whsec(s: &str) -> Result<Vec<u8>, WhsecError>;       // "whsec_" + base64(24..=64 bytes)
pub fn sign_standard_webhooks(secret: &[u8], msg_id: &str, timestamp_secs: i64, body: &[u8]) -> String;  // "v1,<base64 hmac-sha256>"
pub fn verify_standard_webhooks(secret: &[u8], msg_id: &str, timestamp_secs: i64, body: &[u8], signature_header: &str) -> bool; // space-delimited multi-sig aware, constant-time compare
```

Tests: serde round-trips against JSON literals taken verbatim from the design sketch examples
(poll request/response, subscribe request/response, notification frames, control envelopes),
null-vs-absent behavior for `cursor`/`ttlMs`/`refreshBefore`, whsec validation bounds, signature
known-answer test.

---

## crate: mcp-events-engine

```rust
pub struct EmittedEvent { pub name: String, pub event_id: String, pub timestamp: chrono::DateTime<chrono::Utc>, pub data: serde_json::Value }

pub struct Registry;     // cheap-clone (Arc inside)
impl Registry { pub fn new(defs: Vec<EventDefinition>) -> Self; pub fn list(&self) -> Vec<EventDefinition>; pub fn get(&self, name: &str) -> Option<EventDefinition>; }

pub struct LiveEvent { pub seq: u64, pub occurrence: EventOccurrence }  // occurrence.cursor populated ("<epoch>:<seq>")

pub struct BufferConfig { pub max_events_per_type: usize, pub max_age: Option<chrono::Duration> }
pub struct ReadResult { pub events: Vec<EventOccurrence>, pub cursor: String, pub truncated: bool, pub has_more: bool }

pub struct EventBuffer;  // cheap-clone (Arc inside); thread-safe
impl EventBuffer {
    pub fn new(cfg: BufferConfig) -> Self;
    pub fn emit(&self, ev: EmittedEvent) -> u64;     // assigns seq, stores, broadcasts to live receivers
    pub fn read(&self, name: &str, cursor: Option<&str>, max_age_ms: Option<u64>, max_events: Option<u32>,
                params: Option<&serde_json::Value>, filter: Option<&ParamFilter>) -> ReadResult;
    pub fn live(&self, name: &str) -> tokio::sync::broadcast::Receiver<LiveEvent>;
    pub fn current_cursor(&self, name: &str) -> String;
}
pub type ParamFilter = dyn Fn(&serde_json::Value /*params*/, &serde_json::Value /*event data*/) -> bool + Send + Sync;
```

Cursor semantics (per design sketch §Cursor Lifecycle, emit-only model):
- Format `"<epoch>:<seq>"`; `epoch` is a random `u64` chosen at `EventBuffer` construction
  (process-scoped). Cursors are opaque to clients; this format is internal.
- `cursor: None` (request) → "start from now": return no events, fresh cursor, `truncated=false`.
- Foreign epoch, unparseable cursor, or seq evicted from retention → start from oldest retained
  (or now, subject to `max_age_ms` floor) and set `truncated=true`.
- `max_age_ms` → replay floor `now - max_age_ms`; if floor advances past the supplied cursor,
  `truncated=true`. Ignored when `cursor` is `None`.
- `max_events` cap → partial batch + intermediate cursor + `has_more=true`.
- In poll reads, `EventOccurrence.cursor` is absent (`None`); response-level cursor carries position.
  In `live()` events, `occurrence.cursor = Some(Some("<epoch>:<seq>"))`.

Webhook subscription store:

```rust
pub struct SubKey { pub principal: String, pub url: String, pub name: String, pub params_canonical: String }
pub fn canonical_json(v: &serde_json::Value) -> String;   // recursive key-sort, no whitespace
pub struct WebhookSub { pub id: String, pub key: SubKey, pub params: serde_json::Value, pub secret: Vec<u8>,
    pub refresh_before: Option<chrono::DateTime<chrono::Utc>>,  // None = no expiry
    pub cursor: Option<String>, pub active: bool,
    pub last_delivery_at: Option<chrono::DateTime<chrono::Utc>>, pub last_error: Option<String>,
    pub failed_since: Option<chrono::DateTime<chrono::Utc>>, pub verified: bool }
pub enum UpsertOutcome { Created(WebhookSub), Refreshed(WebhookSub) }
pub struct SubscriptionStore;   // cheap-clone, thread-safe
impl SubscriptionStore {
    pub fn new(max_per_principal: usize) -> Self;
    pub fn upsert(&self, key: SubKey, params: serde_json::Value, secret: Vec<u8>,
                  refresh_before: Option<chrono::DateTime<chrono::Utc>>, cursor: Option<String>) -> Result<UpsertOutcome, StoreError>; // StoreError::QuotaExceeded
    pub fn remove(&self, key: &SubKey) -> Option<WebhookSub>;
    pub fn get(&self, key: &SubKey) -> Option<WebhookSub>;
    pub fn list_for_event(&self, name: &str) -> Vec<WebhookSub>;
    pub fn expire_lapsed(&self, now: chrono::DateTime<chrono::Utc>) -> Vec<WebhookSub>;
    pub fn mark_delivery_ok(&self, id: &str, at: chrono::DateTime<chrono::Utc>);
    pub fn mark_delivery_failed(&self, id: &str, error_category: &str, at: chrono::DateTime<chrono::Utc>, suspend_after: u32);
    pub fn set_verified(&self, principal: &str, url: &str);          // verification cache per (principal, url)
    pub fn is_verified(&self, principal: &str, url: &str) -> bool;
    pub fn reactivate(&self, id: &str);
    pub fn update_cursor(&self, id: &str, cursor: Option<String>);
}
```

`id` = `"sub_"` + first 16 hex chars of SHA-256 over `principal\n url\n name\n params_canonical`.
Error categories (strings, per sketch): `connection_refused`, `timeout`, `tls_error`, `http_4xx`,
`http_5xx`, `challenge_failed`.

Tests: cursor lifecycle (start-from-now, resume, foreign epoch → truncated, eviction → truncated,
max_age floor, has_more pagination), filter application, upsert idempotency + secret rotation +
quota, TTL expiry, suspension after N failures.

---

## crate: drasi-feed

```rust
pub enum ChangeType { Added, Updated, Deleted }
pub struct FeedEvent { pub query_id: String, pub change: ChangeType,
    pub before: Option<serde_json::Value>, pub after: Option<serde_json::Value>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>, pub upstream_id: Option<String> }

pub async fn run_drasi_sse_feed(url: String, tx: tokio::sync::mpsc::Sender<FeedEvent>) -> anyhow::Result<()>;
pub async fn run_mock_feed(tx: tokio::sync::mpsc::Sender<FeedEvent>, query_id: String, interval: std::time::Duration) -> anyhow::Result<()>;
```

- `run_drasi_sse_feed` connects to a Drasi Server **SSE reaction** endpoint, parses its frames
  into `FeedEvent`s, reconnects with capped exponential backoff, runs until cancelled. Determine
  the exact frame format from the Drasi sources / `drasi/SSE-FORMAT.md` (written by the Drasi
  env task) and parse defensively (unknown fields ignored; malformed frames logged + skipped).
- `run_mock_feed` generates a deterministic synthetic scenario against a virtual `orders` table:
  cycle of insert (Added) → 2× update (Updated, with before/after) → delete (Deleted), multiple
  concurrent order ids, stable `upstream_id` per row revision (`"order-<n>-rev-<k>"`).
- Hand-roll SSE parsing on `reqwest` byte streams (`data:` lines, blank-line dispatch, ignore
  `:` comments and `event:`/`id:` fields beyond capture). No extra SSE dependency.

---

## crate: mcp-events-server

Binary `mcp-events-server`. CLI: `--config <path>` (YAML).

Config (serde, camelCase):

```yaml
host: 127.0.0.1
port: 8090
authTokens:                      # optional; bearer token -> principal
  - { token: devtoken, principal: dev@example.com }
eventModeling: single            # single | perChange
buffer: { maxEventsPerType: 10000, maxAgeMs: 600000 }
feed:
  kind: mock                     # mock | drasiSse
  queryId: high-value-orders     # mock only
  intervalMs: 2000               # mock only
  # url: http://localhost:8081/events   # drasiSse only
queries:
  - id: high-value-orders
    description: "Rows entering/leaving/changing in the high-value-orders continuous query"
    payloadSchema: {}            # optional
push: { heartbeatIntervalMs: 15000 }
poll: { nextPollMs: 2000 }
webhook:
  enabled: true
  ttlCapMs: 1800000
  minTtlMs: 10000
  maxSubscriptionsPerPrincipal: 16
  allowInsecureUrls: false       # true => permit http:// + private IPs (LOCAL TESTING ONLY, nonconformant)
  suspendAfterFailures: 5
```

Event modeling (`src/mapping.rs`): for each configured query, register event type(s) and map
`FeedEvent` → `EmittedEvent`:
- `single` → name `<query_id>.changed`, data `{"changeType": "added|updated|deleted", "before": ..., "after": ...}`
  (omit null sides); `inputSchema` advertises optional `{"changeType": {"enum": [...]}}` filter
  param, enforced via a `ParamFilter`.
- `perChange` → `<query_id>.added` (data = after), `<query_id>.updated` (data = `{before, after}`),
  `<query_id>.deleted` (data = before).
- `event_id`: `upstream_id` if present else UUIDv4. `delivery`: poll+push always; webhook iff
  `webhook.enabled`.

HTTP surface (axum): `POST /mcp` JSON-RPC dispatcher; `GET /healthz` → `200 ok`. Client
notifications (e.g. `notifications/initialized`) → `202` empty body. Requests yield either
`application/json` (single response) or, for `events/stream` only, `text/event-stream` where
each SSE `data:` frame is a JSON-RPC message and the final frame is the request's response
(`{"jsonrpc":"2.0","id":...,"result":{"_meta":{}}}`) when the server terminates; client abort =
TCP close. Bearer auth: `Authorization: Bearer <token>` resolved via config to a principal.
`initialize` returns capabilities `{"events": {"listChanged": false}}` and issues an
`Mcp-Session-Id` response header (accepted but not enforced subsequently — prototype shortcut,
document it). Implement `ping` → `{}`.

Handlers: `initialize`, `ping`, `events/list` (registry, no pagination → `nextCursor` absent),
`events/poll`, `events/stream` (active frame → live loop with heartbeats every
`heartbeatIntervalMs`, `_meta[META_SUBSCRIPTION_ID]` = request id on every frame; on broadcast
lag (RecvError::Lagged) re-sync from buffer and emit fresh `active {truncated: true}`),
`events/subscribe`, `events/unsubscribe`; unknown method → `-32601`; unknown event name →
`-32011 {data:{kind:"event"}}`; delivery mode not offered → `-32014`.

**File ownership — server-core component**: `src/main.rs`, `src/config.rs`, `src/state.rs`,
`src/dispatch.rs`, `src/mapping.rs`, `src/handlers/{mod,initialize,list,poll,stream}.rs`,
`tests/integration.rs`, `examples/mock.yaml`, `examples/drasi.yaml`.
**File ownership — webhook component**: `src/webhook/{mod,handlers,worker,signer,ssrf,challenge}.rs`,
`tests/webhook.rs`.

Shared state (defined by server-core in `src/state.rs`, used by both):

```rust
pub struct AppState {
    pub config: ServerConfig,
    pub registry: mcp_events_engine::Registry,
    pub buffer: mcp_events_engine::EventBuffer,
    pub subs: mcp_events_engine::SubscriptionStore,
    pub filters: std::collections::HashMap<String, std::sync::Arc<mcp_events_engine::ParamFilter>>, // per event name... (Arc<dyn Fn>)
    pub http: reqwest::Client,    // redirect(Policy::none()), connect timeout 5s, request timeout 10s
}
```

Pinned cross-component signatures (dispatch/main call these; webhook component implements):

```rust
// src/webhook/handlers.rs
pub async fn handle_subscribe(state: Arc<AppState>, principal: Option<String>, params: SubscribeParams) -> Result<SubscribeResult, JsonRpcError>;
pub async fn handle_unsubscribe(state: Arc<AppState>, principal: Option<String>, params: UnsubscribeParams) -> Result<serde_json::Value, JsonRpcError>;
// src/webhook/worker.rs — spawned once from main
pub fn spawn_delivery_worker(state: Arc<AppState>) -> tokio::task::JoinHandle<()>;
```

Webhook behavior (design sketch §Webhook-Based Delivery, §Webhook Security, §Subscription TTL):
- subscribe/unsubscribe REQUIRE an authenticated principal → else `-32012`.
- Validate `delivery.url` (https unless `allowInsecureUrls`), `delivery.secret` via `parse_whsec`
  → else `-32602`. TTL grant: `min(requested, ttlCapMs)` clamped up to `minTtlMs`;
  `ttlMs: null` → grant finite `ttlCapMs` (server unwilling to grant no-expiry);
  absent → default `ttlCapMs`. Returns `refreshBefore` ISO-8601.
- Endpoint verification before first activation per `(principal, url)`: POST signed
  `{"type":"verification","challenge":"<uuid>"}`, expect 2xx echoing `{"challenge":"<same>"}`
  (constant-time compare) → else `-32015 {data:{reason:"challenge_failed" | <connection category>}}`.
- SSRF guard (`src/webhook/ssrf.rs`): resolve host at delivery time, reject non-globally-routable
  IPs (loopback, RFC1918, link-local, CGNAT 100.64/10, ULA fc00::/7, fe80::/10, unspecified,
  multicast/broadcast) unless `allowInsecureUrls`; connect to the validated IP (use
  `reqwest::ClientBuilder::resolve`) ; redirects disabled.
- Delivery worker: one broadcast receiver per event type; per-subscription FIFO queue; POST each
  `EventOccurrence` with Standard Webhooks headers (`webhook-id` = eventId,
  `webhook-timestamp` = now, fresh signature per attempt, `x-mcp-subscription-id`); retry
  backoff 1s → 5s → 25s then mark failed; suspend after `suspendAfterFailures` consecutive
  failures (`active=false`, resume on refresh); watermark cursor = highest seq such that all
  lower seqs for that subscription are acked/abandoned — include in payload `cursor` and in
  refresh responses; TTL expiry tick every 5s removes lapsed subs.
- Refresh (idempotent upsert on same key): replace secret, re-grant TTL, treat supplied cursor
  per sketch (no-op if at/behind in-flight position; replay point if lapsed), set active=true,
  include `deliveryStatus`.

Integration test (`tests/integration.rs`): spawn server (mock feed, in-process on an ephemeral
port), then: initialize → events/list (assert definitions) → poll with `cursor: null` (assert
fresh cursor, empty) → wait for emissions → poll (assert events + cursor advance) → poll with
`maxEvents: 1` (assert `hasMore`) → stream (collect active + ≥1 event frame, assert `_meta`
subscription id routing) → bogus cursor (assert `truncated`). Webhook test (`tests/webhook.rs`):
in-process axum receiver (loopback, `allowInsecureUrls: true`): subscribe with missing auth →
`-32012`; with auth → challenge arrives, echo it; events delivered with valid signatures
(verify via `verify_standard_webhooks`); bad-secret subscribe → `-32602`; refresh returns
`deliveryStatus`; unsubscribe stops delivery.

---

## crate: mcp-events-client

Library:

```rust
pub struct EventsClient;   // base_url ("http://host:port/mcp"), optional bearer token
impl EventsClient {
    pub fn new(base_url: impl Into<String>) -> Self;
    pub fn with_bearer(self, token: impl Into<String>) -> Self;
    pub async fn initialize(&mut self) -> anyhow::Result<InitializeResult>;   // also sends notifications/initialized
    pub async fn list_events(&self) -> anyhow::Result<ListEventsResult>;
    pub async fn poll(&self, params: &PollEventsParams) -> anyhow::Result<PollEventsResult>;
    pub async fn stream(&self, params: &StreamEventsParams) -> anyhow::Result<EventStream>;  // EventStream: futures::Stream<Item = anyhow::Result<StreamFrame>>
    pub async fn subscribe(&self, params: &SubscribeParams) -> anyhow::Result<SubscribeResult>;
    pub async fn unsubscribe(&self, params: &UnsubscribeParams) -> anyhow::Result<()>;
}
pub enum StreamFrame { Active(EventsActiveParams), Event(EventOccurrence), Heartbeat(EventsHeartbeatParams), Error(EventsErrorParams), Terminated(EventsTerminatedParams), Result }
```

Errors: JSON-RPC error responses surface as `anyhow` errors carrying code+message (define a
typed `RpcError` wrapper so the harness can print code names).

Binary `events-harness` (clap subcommands):
- `list` — initialize + list, pretty-print event types.
- `poll --name <n> [--params <json>] [--state-file <path>] [--follow] [--max-events N] [--max-age-ms N]`
  — poll loop honoring `nextPollMs`/`hasMore`; persists `{cursor}` JSON to state file; dedups by
  `eventId` (bounded LRU set, warn on duplicate); prints events + `truncated` warnings.
- `stream --name <n> [--params <json>] [--state-file <path>]` — consume SSE frames, persist
  cursor from events AND heartbeats, print everything; reconnect with last cursor on drop.
- `subscribe --name <n> --url <u> [--params <json>] [--ttl-ms N|null] [--secret <whsec>] [--state-file <path>]`
  — generate `whsec_` secret if not given (print it), subscribe, print grant; `--refresh-loop`
  re-subscribes before `refreshBefore`.
- `unsubscribe --name <n> --url <u> [--params <json>]`
- `webhook-recv --port <p> --secret <whsec>` — axum receiver: verifies Standard Webhooks
  signatures (reject >5min skew), answers `verification` challenges, dedups on `webhook-id`,
  prints events/control envelopes, always 2xx on valid.

All harness output human-readable; one event per line: `[<name>] <eventId> <changeType?> <data JSON>`.

---

## drasi/ demo environment

`docker-compose.yml` (postgres:16 + `ghcr.io/drasi-project/drasi-server:0.1.0`), `server.yaml`
(postgres source with bootstrap provider; continuous query `high-value-orders` (Cypher, e.g.
orders with `total > 1000`); SSE reaction on port 8081), `seed.sql` (orders table +
`drasi_slot`/publication prerequisites + sample rows), `RUNBOOK.md` (bring-up, verify via curl,
psql commands that trigger Added/Updated/Deleted, teardown), `SSE-FORMAT.md` (documented frame
format of the Drasi SSE reaction, derived from Drasi sources — this feeds the `drasi-feed`
implementation). Validate YAML against the drasi-server README config reference
(`/Users/aman/proto/server/README.md`).
