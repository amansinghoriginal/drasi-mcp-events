# Conformance Notes

Deviations recorded by two adversarial spec-conformance reviews (poll/push/cursor and
webhook) plus a defect-focused code review, run against this implementation after the build.
**No `real` (wire-visible) nonconformances were found by either spec review.** The two `real`
defects found by the code review (a `maxAgeMs` overflow panic and unbounded webhook delivery
queues) were fixed in the commit that introduced this file; everything below is `minor`
(SHOULD-level / edge case) or `info` (documented prototype shortcut), kept here as an honest
backlog.


## Poll / Push / Cursor review

_Conformance review of poll + push + cursor semantics for the clean-room MCP Events prototype at /Users/aman/proto/drasi-mcp-events against docs/design-sketch-proposal.md. Wire shapes were checked field-by-field against every JSON example in the in-scope sections: capability declaration, events/list request/response (incl. _meta and nextCursor), events/poll request/response, the EventOccurrence table, the events/stream envelope, and the active/event/heartbeat/error/terminated notification params plus the final StreamEventsResult frame all match the sketch exactly (camelCase names, null-vs-absent conventions, per-event cursors on push only, response-level cursor on poll, _meta["io.modelcontextprotocol/subscriptionId"] on every stream frame, SSE data: frames with no comment keepalives). Core cursor lifecycle behavior conforms: null cursor = start-from-now with fresh cursor and no truncation, maxAgeMs ignored on null cursor, maxEvents/hasMore pagination with intermediate cursors, cursor advancement past filtered events, heartbeat-carried cursor, per-subscription in-order delivery with replay/live-tail stitching deduped by seq, immediate JSON-RPC error (no stream) for invalid subscriptions, NotFound/Unsupported/InvalidParams used as specified. No real (wire-visible, unconditional) nonconformance was found; nine minor (SHOULD-level or edge-case/race) deviations and one info-level cluster of documented prototype shortcuts are reported, the most interesting being: truncated is suppressed when the maxAgeMs floor passes the cursor but nothing was actually dropped (a sketch self-contradiction), a mid-replay buffer eviction race can swallow a real gap on the push stream, broadcast-lag re-sync sends a spurious truncated:true even when every event is recovered, error `message` strings deviate from the sketch's canonical names for NotFound/Forbidden/InvalidParams, and the harness's poll floor defaults to 250 ms instead of the sketch's 1000 ms. Eleven spec gaps surfaced by this code are reported, headlined by: completely unspecified invalid/foreign/future cursor handling, the §Bounding-replay vs §Gaps truncated contradiction, the client-side dead-stream rule referencing a heartbeat interval that is never communicated on the wire, undefined maxEvents:0 semantics, unstated absent-vs-null request-cursor equivalence, and ambiguous emit-only restart replay semantics._

- **[minor]** `engine buffer: maxAgeMs floor vs truncated (crates/mcp-events-engine/src/buffer.rs read(), lines 229-241; test max_age_floor_without_skipped_events_does_not_truncate)`
  - Spec: If the floor advances past the cursor, the server SHOULD set `truncated: true` on the first response (poll result / `notifications/events/active` / webhook subscribe response) so the client knows older events were skipped.
  - Behavior: The buffer sets truncated only when a retained event after the cursor was actually discarded by the floor. A poll/stream whose maxAgeMs floor is later than the cursor's wall-clock position, but where no events exist in the skipped interval, returns truncated:false. This is a deliberate, documented narrowing (the §Gaps definition reading: nothing was skipped) but contradicts the literal SHOULD in §
  - Suggested fix: Either keep as-is and surface the contradiction upstream (see specGaps), or conservatively set truncated whenever floor > cursor position regardless of whether retained events were dropped.

- **[minor]** `push stream: mid-replay gap swallowed (crates/mcp-events-server/src/handlers/stream.rs, backlog replay loop lines 117-136 and lagged re-sync loop lines 182-204)`
  - Spec: Push | A `notifications/events/active {cursor:<fresh>, truncated:true, _meta.subscriptionId}` is sent (initially, and again mid-stream if a gap occurs); delivery continues on the same stream.
  - Behavior: The one-event-per-read replay loop discards ReadResult.truncated. If ring eviction races the replay (e.g. SSE backpressure stalls the generator while the feed emits past the ring/age limits), buffer.read() detects the gap and sets truncated, but no fresh active{truncated:true} is emitted — the client silently loses events with no gap signal. The initial probe and the Lagged arm do signal truncatio
  - Suggested fix: In both replay loops, if read.truncated is true, emit a fresh notifications/events/active {cursor:<read.cursor>, truncated:true, _meta} before continuing.

- **[minor]** `push stream: spurious truncated on broadcast lag (crates/mcp-events-server/src/handlers/stream.rs, RecvError::Lagged arm lines 172-204)`
  - Spec: `truncated: true` is the single signal that the server started delivery from a position later than the cursor the client supplied — i.e., events were skipped.
  - Behavior: On tokio broadcast lag the handler unconditionally sends active{truncated:true} and then re-syncs from the ring buffer. Because ring retention (default 10,000 / 10 min) is >= the broadcast channel capacity (clamped to 1024), the re-sync read almost always recovers every lagged event — so the client is told events were skipped when none were, prompting unnecessary 're-fetch authoritative state' beh
  - Suggested fix: Perform the catch-up read first and set the active frame's truncated from read.truncated (i.e., only when the buffer itself could not serve the missed range).

- **[minor]** `error message strings (crates/mcp-events-wire/src/jsonrpc.rs not_found()/forbidden()/invalid_params(); used by crates/mcp-events-server/src/handlers/poll.rs:11, handlers/stream.rs:41, mapping.rs:208-216)`
  - Spec: | `-32011` | `NotFound` | A referenced entity does not exist ... | ... example: {"code":-32012,"message":"Forbidden","data":{"reason":"Access revoked"}}
  - Behavior: resource_exhausted/unsupported/callback_endpoint_error use the canonical message names from the sketch's table, but not_found, forbidden, and invalid_params put free-form prose in `message` (e.g. message: "unknown event type \"x\"" for -32011, "invalid params: ..." for -32602) instead of the canonical "NotFound"/"Forbidden"/"InvalidParams" with detail in data. Wire-visible inconsistency with the s
  - Suggested fix: Set message to the canonical name and carry the human-readable detail in data (e.g. data: {kind:"event", name:"x"}).

- **[minor]** `client poll floor default (crates/mcp-events-client/src/bin/events-harness.rs DEFAULT_POLL_FLOOR_MS = 250, line 31)`
  - Spec: Clients SHOULD apply a configurable floor (default 1000 ms) to guard against a misbehaving server inducing a tight loop.
  - Behavior: The floor is configurable (--floor-ms) and applied to nextPollMs sleeps, but the default is 250 ms, not the sketch's stated default of 1000 ms. (Skipping the floor entirely when hasMore is true is conformant: the sketch says to re-poll immediately and ignore nextPollMs.)
  - Suggested fix: Change DEFAULT_POLL_FLOOR_MS to 1000.

- **[minor]** `client push path lacks eventId dedup (crates/mcp-events-client/src/bin/events-harness.rs cmd_stream, lines 365-461)`
  - Spec: `eventId` for client-side deduplication — the client can detect and discard duplicates that arise during reconnection.
  - Behavior: cmd_poll dedups via LruSet, but cmd_stream prints every StreamFrame::Event without eventId dedup. After a server restart the foreign-epoch cursor triggers the server's documented at-least-once replay-from-oldest, so the stream harness emits duplicate events to its output on reconnect.
  - Suggested fix: Apply the same LruSet(DEDUP_CAPACITY) check in cmd_stream before print_event.

- **[minor]** `events/list pagination cursor ignored (crates/mcp-events-server/src/handlers/list.rs:9-14)`
  - Spec: // params (optional)
{ "cursor": "..." }  // pagination ... "nextCursor": "..."   // present when more pages are available; same semantics as tools/list etc.
  - Behavior: Any cursor value — including garbage or a stale cursor — is accepted and silently answered with the full single page. Under MCP's tools/list pagination semantics (which the sketch adopts by reference) an invalid cursor should be rejected with -32602 InvalidParams. Returning nextCursor absent for a single page is conformant.
  - Suggested fix: Reject non-empty cursors the server never issued with InvalidParams (or document the prototype's single-page leniency).

- **[minor]** `perChange event types: no inputSchema, params silently ignored (crates/mcp-events-server/src/mapping.rs build_event_model PerChange arm lines 94-129, validate_event_params lines 198-221)`
  - Spec: A server declares event types via `events/list` (each with a name, an `inputSchema` for subscription params, a `payloadSchema`, and the delivery modes it supports). ... `-32602 InvalidParams` — params don't match the event's inputSchema
  - Behavior: In perChange modeling the EventDefinitions omit inputSchema entirely, and validate_event_params only checks that params is an object — arbitrary subscription params (e.g. {"changeType":"added"} on q1.added) are accepted and never applied as filters, rather than rejected. A client can believe it subscribed with a narrowing filter while receiving the unfiltered stream. (Single mode validates and enf
  - Suggested fix: Advertise an explicit empty-object inputSchema ({"type":"object","properties":{},"additionalProperties":false}) for perChange types and reject unknown params with InvalidParams, or document that unrecognized params are ignored.

- **[minor]** `poll: no server-side default maxEvents limit (crates/mcp-events-server/src/handlers/poll.rs:18-25; crates/mcp-events-engine/src/buffer.rs read())`
  - Spec: If omitted, the server uses its own default limit.
  - Behavior: When maxEvents is omitted the server imposes no batch limit and returns the entire retained backlog in one response — up to buffer.maxEventsPerType (default 10,000) occurrences in a single JSON body. The only bound is the ring capacity itself, which is arguably not a 'default limit' in the sketch's sense.
  - Suggested fix: Add a configurable server default (e.g. 256) applied when maxEvents is absent, with hasMore pagination.

- **[info]** `heartbeat interval configurability (crates/mcp-events-server/src/config.rs PushSettings, default_heartbeat_interval_ms = 15000; handlers/stream.rs:139)`
  - Spec: The server SHOULD send a heartbeat at least every 30 seconds.
  - Behavior: The default (15 s) conforms, but push.heartbeatIntervalMs is accepted unbounded — an operator can configure e.g. 120000 and the server will silently violate the SHOULD (and outlive the client guidance's 2x-interval dead-stream window). No clamp or warning at config load.
  - Suggested fix: Warn or clamp at config validation when heartbeatIntervalMs > 30000.

- **[info]** `documented prototype shortcuts (crates/mcp-events-server/src/dispatch.rs module docs + lines 110-124; handlers/initialize.rs:32-34; mapping.rs/registry)`
  - Spec: stdio: The `events/stream` request is sent on stdin. ... the client cancels by sending `notifications/cancelled` ... [SSE stream] is independent of, and does not replace, the transport's existing GET-based SSE stream ... If the set of available event types changes at runtime ... the server sends a `
  - Behavior: Known, documented prototype scope cuts visible in the reviewed code: (1) Streamable HTTP only — stdio push delivery and notifications/cancelled handling are absent (client notifications are 202-accepted and dropped); (2) no GET /mcp SSE channel for non-event notifications; (3) Mcp-Session-Id is issued on initialize but never enforced; (4) capabilities advertise events.listChanged:false and the reg
  - Suggested fix: —


## Webhook review

_Reviewed webhook mode end-to-end against docs/design-sketch-proposal.md. Core wire mechanics are conformant: the Standard Webhooks signature formula was independently verified byte-for-byte (HMAC-SHA256 over `webhook-id + "." + webhook-timestamp + "." + raw-body`, base64, `v1,` prefix; secret = decoded bytes after `whsec_` with 24-64B bounds enforced via -32602); all four required headers plus Content-Type are on every POST including control envelopes; timestamps/signatures are regenerated per retry attempt while webhook-id stays stable (eventId for events, msg_<type>_<random> for control envelopes); the challenge is a signed verification envelope with a single-use nonce, constant-time compared, cached per (principal, url) so varying params/names never re-challenge, with no delivery to unverified subscriptions (worker fan-out and reconcile both gate on `verified`, and the handshake window is recovered via watermark backfill); TTL negotiation clamps into [min, cap], grants the cap for omitted ttlMs, refuses no-expiry with a finite grant (never returns refreshBefore:null, satisfying the MUST), and clamping is self-announcing; the subscription key is (principal, url, name, canonical-params) with deterministic sub_<16-hex> id, idempotent upsert (live refresh: secret replaced, TTL re-granted, reactivated, supplied cursor a no-op; lapsed/absent: fresh create adopting the cursor), Forbidden for unauthenticated callers on both methods, and unsubscribe resolving by the same key with NotFound kind=event|subscription. The watermark-cursor safety claim HOLDS: deliveries are strict per-subscription FIFO, the payload cursor is the event's own position (everything lower acked/abandoned), the stored watermark advances only after ack/abandon/filtered-skip/gap-signal, created-subscribe responses return the client's own normalized position, and refresh responses return the stored watermark — I found no path that advances a persisted cursor past an unacked, unabandoned event (the lapse/re-subscribe dual-task race produces duplicates, not skips). Error-code mapping (-32602/-32011/-32012/-32013/-32014/-32015 with kind/limit+max/feature+value/reason data) matches the table, with reasons confined to the six categories and no endpoint output leaked. Deviations found are SHOULD-level or prototype shortcuts: non-2xx challenge responses report http_4xx/5xx where §Endpoint verification implies challenge_failed (the sketch itself is contradictory here); no dual-sign rotation grace window; no per-destination rate limiting or negative caching of challenges (weakening the one-POST-per-(principal,url) anti-flooding bound); the verification cache never expires; the SSRF blocklist misses several IANA registry entries (notably NAT64 64:ff9b::/96 and 6to4 2002::/16, both of which can embed private IPv4 targets); the 256 KiB body SHOULD is unenforced; delivery is FIFO-with-blocking-retries rather than independent per-event retry; and webhook is advertised in events/list even on a server with no configured principals. 14 spec gaps recorded, the sharpest being undefined canonical JSON, absent-params/{}/null key identity, URL equality, the challenge_failed-vs-http_4xx contradiction, missing lastError categories for DNS/SSRF-policy/3xx failures, verification-cache lifetime, the truncated-on-maxAgeMs-floor ambiguity, and unspecified failed-verification/lapsed-key/quota state transitions._

- **[minor]** `Endpoint verification — data.reason for reachable non-2xx challenge responses (crates/mcp-events-server/src/webhook/challenge.rs:55-63)`
  - Spec: A reachable endpoint that fails to echo yields `-32015 CallbackEndpointError` with `data.reason: "challenge_failed"`; an unreachable one yields the same code with the relevant connection-failure category (`connection_refused`, `timeout`, or `tls_error`).
  - Behavior: verify_endpoint maps a non-2xx challenge response to http_4xx/http_5xx, reserving challenge_failed for a 2xx body that fails to echo the nonce. Per the §Endpoint verification sentence, a reachable endpoint returning 404/500 'fails to echo' and should yield challenge_failed; only connection categories are sanctioned otherwise. (The -32015 row in §Error Codes lists all six categories, so the impl re
  - Suggested fix: Either map every reachable-but-non-echoing endpoint (including non-2xx) to challenge_failed, or keep the current mapping and seek clarification of the sketch's two pulls.

- **[minor]** `Secret rotation — no dual-sign grace window (crates/mcp-events-engine/src/store.rs upsert; crates/mcp-events-server/src/webhook/signer.rs)`
  - Spec: the server SHOULD dual-sign (Standard Webhooks multi-signature) with old and new for a short grace window so in-flight deliveries verify under either.
  - Behavior: upsert replaces the secret atomically; signed_post emits exactly one `v1,` signature with the current secret (fetched fresh per attempt). The wire crate's verify helper supports multi-signature headers, but the server never produces them, so there is no grace window during rotation.
  - Suggested fix: Retain the previous secret with a short expiry in WebhookSub and emit space-delimited `v1,` signatures for both during that window.

- **[minor]** `Endpoint verification — no per-destination rate limiting / no negative caching of failed handshakes (crates/mcp-events-server/src/webhook/challenge.rs, handlers.rs)`
  - Spec: The verification POST MUST use the same SSRF-hardened path as deliveries (delivery-time IP validation, no redirects) and SHOULD be rate-limited per destination host ... bounds attacker-induced traffic to one POST per (principal, victim-url)
  - Behavior: The SSRF-hardened path is used (conformant), but there is no rate limiting anywhere: every subscribe call against an unverified (principal, url) fires one challenge POST, failed handshakes are not negatively cached, and concurrent subscribes with varying params each launch their own challenge. Attacker-induced traffic at a victim URL is bounded only by the attacker's subscribe request rate, not by
  - Suggested fix: Add a per-destination-host token bucket on challenge POSTs and a short negative cache / in-flight coalescing keyed on (principal, url).

- **[minor]** `Endpoint verification — cache never expires (crates/mcp-events-engine/src/store.rs: StoreState.verified)`
  - Spec: The cache is in-memory TTL-scoped soft state like the subscription itself; after a restart the server re-verifies on the next subscribe.
  - Behavior: The `verified: HashSet<(principal, url)>` has no eviction at all: entries survive unsubscribe and TTL lapse of every subscription to that URL, for the whole process lifetime. A (principal, url) verified once is never re-challenged, so if the URL's ownership changes (e.g. reusable tunnel/relay hostnames) the same principal can resume flooding it without a new consent handshake. 'TTL-scoped like the
  - Suggested fix: Expire verification entries when the last covering subscription lapses (or after a bounded TTL), forcing a fresh handshake on the next subscribe.

- **[minor]** `SSRF — incomplete IANA special-purpose registry coverage (crates/mcp-events-server/src/webhook/ssrf.rs: ipv4_is_global/ipv6_is_global)`
  - Spec: Servers SHOULD reject URLs whose resolved IP is not globally routable per the IANA IPv4 and IPv6 Special-Purpose Address Registries
  - Behavior: The hand-rolled check misses several registry entries: IPv6 64:ff9b::/96 (NAT64 well-known prefix — 64:ff9b::10.0.0.1 embeds a private IPv4 and passes as global, reaching internal hosts on NAT64-capable networks), 2002::/16 (6to4 with embedded IPv4), 100::/64 (discard-only), 2001::/23 (IETF protocol assignments incl. Teredo 2001::/32), ::/96 (deprecated IPv4-compatible, ::10.0.0.1 passes); IPv4 19
  - Suggested fix: Add the missing prefixes (especially 64:ff9b::/96 and 2002::/16, which can encode private IPv4 targets) to ipv6_is_global/ipv4_is_global.

- **[minor]** `Delivery profile — 256 KiB body bound not enforced (crates/mcp-events-server/src/webhook/worker.rs: deliver_event)`
  - Spec: Body size: servers SHOULD keep delivery bodies at or under 256 KiB, consistent with Payload Minimality.
  - Behavior: Event occurrences are serialized and POSTed without any size check; feed-provided `data` of arbitrary size goes out as-is. (The companion MUST — treating 413 as non-retryable — is correctly implemented via `retryable: status != PAYLOAD_TOO_LARGE`.)
  - Suggested fix: Check serialized body length before POSTing; log-and-abandon (or truncate payload fields) past 256 KiB.

- **[minor]** `Delivery model — strict FIFO with blocking retries instead of independent per-event retry (crates/mcp-events-server/src/webhook/worker.rs: run_delivery/deliver)`
  - Spec: Delivery model. The server retries each event independently with exponential backoff on non-2xx responses ... Concurrent deliveries and retries may therefore arrive out of order
  - Behavior: Each subscription has one sequential delivery task: a failing event head-of-line blocks every subsequent event for the full retry schedule (1s/5s/25s, 4 attempts ≈ 31s) before being abandoned and the watermark advanced. This narrowing is what makes the payload cursor (the event's own position) a valid safe watermark, and in-order delivery is a permitted subset of best-effort ordering — but it is a
  - Suggested fix: Document the FIFO model as the implementation's profile, or lengthen the retry schedule; if independent retry is ever adopted, the watermark computation must change to a min-unacked position.

- **[minor]** `events/list advertises webhook on a server with no configured principals (crates/mcp-events-server/src/mapping.rs: build_event_model)`
  - Spec: Unauthenticated MCP servers may offer poll and push, but not webhook.
  - Behavior: DeliveryMode::Webhook is added to every event's `delivery` array whenever `webhook.enabled` is true, regardless of whether any authTokens are configured. A token-less server therefore advertises webhook while every events/subscribe deterministically fails -32012, so no client can ever use the advertised mode.
  - Suggested fix: Suppress webhook from `delivery` (or refuse the config) when `webhook.enabled && auth_tokens.is_empty()`.

- **[info]** `Authorization — flat permission model; terminated envelope unreachable (crates/mcp-events-server/src/webhook/handlers.rs; worker.rs deliver_control)`
  - Spec: the server MUST verify the principal has permission to subscribe to the requested event type with the given params ... The server SHOULD periodically re-verify permissions. If the user's access is revoked ... the server terminates the subscription [via a] Signed {"type":"terminated",...} envelope
  - Behavior: Any authenticated principal may subscribe to any event/params (the permission check is vacuous — all principals are permitted everything), there is no delivery-time re-verification, and the `terminated` control envelope is never emitted (the wire type and worker match arm exist but no code path constructs one; no revocation model exists). Documented prototype shortcut consistent with the static be
  - Suggested fix: —

- **[info]** `allowInsecureUrls bypasses TLS MUST and all SSRF checks (crates/mcp-events-server/src/webhook/handlers.rs parse_callback_url; ssrf.rs client_for)`
  - Spec: Callback URLs MUST use `https://`. Servers MUST reject `events/subscribe` with a non-`https` `delivery.url` (`-32602 InvalidParams`). ... this validation MUST be performed at delivery time
  - Behavior: With `webhook.allowInsecureUrls: true`, http:// URLs are accepted and `client_for` returns the shared client with no IP validation or pinning at all. Explicitly labeled 'LOCAL TESTING ONLY, nonconformant' in config.rs and module docs; defaults to false.
  - Suggested fix: —

- **[info]** `In-memory state; no durability guard for long TTL grants; sessions not enforced (crates/mcp-events-engine/src/store.rs; crates/mcp-events-server/src/config.rs, dispatch.rs)`
  - Spec: A server configured to grant long or no-expiry TTLs MUST instead persist subscriptions for the lifetime it grants ... SDKs SHOULD refuse a no-expiry cap unless the author has wired up durable storage.
  - Behavior: Subscription store, verification cache, and event buffer are in-memory (sanctioned for the default 30-minute cap; refresh-cycle recovery applies). No-expiry is never granted, but `ttlCapMs` is operator-configurable to arbitrarily large values with no guard or warning about volatile storage, so an operator can configure the server into the long-TTL regime the MUST forbids without persistence. Mcp-S
  - Suggested fix: —

- **[info]** `JSON-RPC error.message strings vs the table's canonical names (crates/mcp-events-wire/src/jsonrpc.rs; webhook/handlers.rs)`
  - Spec: | `-32012` | `Forbidden` | The authenticated principal is not permitted ... | (§Error Codes table, Message column)
  - Behavior: Some errors carry descriptive sentences instead of the canonical message token: forbidden() and not_found() set message to e.g. "events/subscribe requires an authenticated principal" rather than "Forbidden"/"NotFound" (ResourceExhausted/Unsupported/CallbackEndpointError use the canonical names). The sketch tells clients to branch by code/method/data, so this is informational, but the examples (e.g
  - Suggested fix: —

- **[info]** `Broadcast-lag handled as a gap even when events are still retained (crates/mcp-events-server/src/webhook/worker.rs: fan_out Lagged arm)`
  - Spec: `gap` ... Sent when: A gap is detected between refreshes ... In all cases the server resets to a position it can serve from, returns that position as the fresh cursor alongside truncated: true, and continues
  - Behavior: When the internal broadcast channel lags, the worker enqueues a gap envelope with the head cursor and skips queued events at or below it — even though those events typically still sit in the retention ring buffer and could be recovered by re-reading (as the backfill path does). Conformant (a server-side ceiling signaled via gap is sanctioned) but abandons recoverable events.
  - Suggested fix: —

- **[info]** `Lapse/re-subscribe race can run two delivery tasks for one sub id (crates/mcp-events-server/src/webhook/worker.rs: sweep + ensure_task)`
  - Spec: All three modes provide at-least-once delivery ... Exactly-once requires application-level deduplication via eventId.
  - Behavior: When the 5s sweep removes a lapsed subscription's task handle while its task is still draining, an immediate re-subscribe (same derived id) spawns a second task; both may deliver briefly, producing duplicate POSTs and interleaved update_cursor calls. Each watermark value written is individually safe (covers only events that writer acked/abandoned), and duplicates are permitted under at-least-once 
  - Suggested fix: —


## Code review (defects)

_Read all six crates (wire, engine, drasi-feed, server, client, webhook subsystem). The codebase is generally careful (constant-time HMAC compares, SSRF hardening, poison-tolerant locks, no MutexGuards held across awaits, dedup by seq to stitch replay+live). I found one clear panic-on-untrusted-input DoS (DateTime subtraction overflow from attacker-controlled maxAgeMs), one unbounded-memory issue (per-subscription unbounded mpsc queue that grows without bound while an endpoint is slow or a subscription is suspended), and several lower-severity issues (non-constant-time bearer-token comparison, verification echo body read before size cap, failure counter incremented per retry attempt rather than per delivery). Findings are filed in deviations[]._

- **[real]** `crates/mcp-events-engine/src/buffer.rs:106-108 (used at :229)` — **FIXED** in this commit
  - Spec: fn floor_from_max_age_ms(now: DateTime<Utc>, ms: u64) -> DateTime<Utc> { now - Duration::milliseconds(i64::try_from(ms).unwrap_or(i64::MAX)) }
  - Behavior: Panic on untrusted wire input (DoS). `maxAgeMs` is an attacker-controlled u64 from events/poll, events/stream and events/subscribe params and is never validated/clamped. For any non-null cursor, read() computes floor = now - Duration::milliseconds(ms). chrono's `DateTime - TimeDelta` calls .expect("`DateTime - TimeDelta` overflowed") and panics when the result leaves chrono's NaiveDate range (roug
  - Suggested fix: Use a checked/saturating subtraction: `now.checked_sub_signed(Duration::try_milliseconds(ms.min(i64::MAX as u64) as i64).unwrap_or(TimeDelta::MAX)).unwrap_or(DateTime::<Utc>::MIN_UTC)`, or clamp/validate maxAgeMs (e.g. reject or cap at the retention window) in validate_event_params before it reaches

- **[real]** `crates/mcp-events-server/src/webhook/worker.rs:111 (with fan_out :149-160 and deliver suspend-loop :369-375)` — **FIXED** in this commit
  - Spec: let (tx, rx) = mpsc::unbounded_channel();
  - Behavior: Unbounded per-subscription memory growth / no backpressure. Each subscription's delivery queue is an unbounded mpsc. fan_out() pushes every live event to EVERY verified sub returned by list_for_event() — which includes suspended (active=false) subs. A suspended sub's run_delivery task parks inside deliver()'s `loop { match state.subs.get(key) { Some(s) if s.active => break, Some(_) => sleep(200ms)
  - Suggested fix: Bound the queue (mpsc::channel(N)) and on overflow drop-and-resynchronize via a Gap envelope + watermark (the design already supports gap-based resync), or skip enqueuing to subs whose active==false and rely on watermark backfill when they reactivate (note: backfill currently only runs at task start

- **[minor]** `crates/mcp-events-server/src/config.rs:219-224`
  - Spec: self.auth_tokens.iter().find(|t| t.token == token).map(|t| t.principal.clone())
  - Behavior: Bearer tokens (secrets) are compared with String `==`, which short-circuits on the first differing byte and is not constant-time. This is a timing side-channel on the authentication token that gates all webhook subscribe/unsubscribe operations (principal resolution in dispatch.rs:61).
  - Suggested fix: Compare with a constant-time primitive (the workspace already depends on `subtle`): iterate tokens and accumulate `subtle::ConstantTimeEq::ct_eq` over equal-length byte slices, avoiding early return; or hash tokens and compare digests in constant time.

- **[minor]** `crates/mcp-events-server/src/webhook/challenge.rs:65-71`
  - Spec: let bytes = response.bytes().await...; if bytes.len() > MAX_ECHO_BYTES { return Err(CHALLENGE_FAILED); }
  - Behavior: The MAX_ECHO_BYTES (64 KiB) cap on the verification echo is checked only AFTER response.bytes().await has buffered the entire response body into memory. A malicious or buggy callback endpoint can stream an arbitrarily large body (bounded only by the 10s request timeout) before the size check runs, allowing transient large-allocation pressure during the subscribe handshake.
  - Suggested fix: Stream the body with a running byte budget (e.g. read chunks via response.chunk().await, abort once the accumulated length exceeds MAX_ECHO_BYTES), or short-circuit on Content-Length before reading.

- **[minor]** `crates/mcp-events-server/src/webhook/worker.rs:381-394`
  - Spec: Attempt::Fail { category, retryable } => { state.subs.mark_delivery_failed(sub_id, category, Utc::now(), suspend_after); ... attempt += 1; }
  - Behavior: mark_delivery_failed (which increments the consecutive-failure counter feeding suspendAfterFailures) is called on every retry ATTEMPT of a single message, not once per failed delivery. One persistently-failing message contributes up to 4 increments (initial + 3 backoff retries), so with suspendAfterFailures=5 a subscription is suspended after roughly 1-2 failed messages rather than 5 consecutive f
  - Suggested fix: Count one failure per abandoned message: only call mark_delivery_failed once when the message is finally abandoned (or track attempts vs. deliveries separately) so the suspension threshold matches the documented per-delivery semantics.

- **[info]** `crates/mcp-events-engine/src/store.rs:124-126,313-321`
  - Spec: verified: HashSet<(String, String)>
  - Behavior: The endpoint-verification cache (keyed by principal+url) is inserted into but never evicted or bounded; entries persist for the process lifetime. Over many distinct (principal, url) pairs this set grows without bound (slow leak). Unlike subs/failures, it is not cleaned up on unsubscribe or expire_lapsed.
  - Suggested fix: Evict verification entries when the last subscription for a (principal, url) is removed/lapsed, or bound the cache with an LRU.
