# Spec Gaps & Findings — MCP Events Design Sketch

Findings from an independent, clean-room Rust implementation of the draft MCP **Events**
extension ([design sketch, PR #1 on
`modelcontextprotocol/experimental-ext-triggers-events`](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/pull/1),
revision of 2026-06-11; vendored at `docs/design-sketch-proposal.md`).

## Methodology

The implementation ([this repo](https://github.com/amansinghoriginal/drasi-mcp-events)) was
built **from the sketch text alone** — no other Events implementation (TypeScript SDK branch,
mcpkit, …) was consulted at any point, so every finding below reflects what the document
itself does or does not pin down. Scope: all three delivery modes (poll, push/`events/stream`,
webhook incl. Standard-Webhooks signing, endpoint verification, TTL negotiation and watermark
cursors), the full cursor lifecycle, a CLI client/harness, and a [Drasi](https://drasi.io)
continuous-query bridge as a real durable upstream. 223 tests; e2e-verified for all three modes.

Each "the spec didn't tell me" moment was logged at the point it was hit, then deduplicated,
re-rated, and extended by an adversarial completeness pass over the whole sketch.

**Severity:** **high** = two reasonable implementations will be wire-incompatible or fail to
interoperate; **medium** = divergent-but-recoverable behavior, or a mandated thing that cannot
be implemented as written; **low** = editorial / clarity.

**Totals:** 3 high · 33 medium · 22 low


---

## High

### 1. The delivery object has no schema; delivery.mode is present on subscribe but absent in the unsubscribe example

**Sketch section:** Subscribing: events/subscribe / Unsubscribing: events/unsubscribe (delivery object)  
**Kind:** Contradiction

The delivery object ({mode, url, secret}) is defined only by example and never given a field table. The subscribe example carries all three fields, but the events/unsubscribe example carries delivery: {url} only, and the prose never says whether mode and secret are optional, forbidden, ignored, or validated on unsubscribe. A schema-deriving implementer who makes mode a required field of the delivery object will reject unsubscribe requests from clients that follow the example verbatim — a hard interop failure on a core method. Relatedly, the error for events/subscribe with mode push/poll or with mode absent is undefined: -32014 Unsupported ("a delivery mode the event type does not offer") does not quite fit, since push may be offered, just not via subscribe.

**What this implementation assumed:** Implementations kept a required mode field internally but made it wire-optional (absent deserializes to empty; empty is omitted on serialize) so both sketch literals round-trip; unsubscribe sends {url} only and ignores extra delivery fields; subscribe with non-webhook mode returns -32014 {feature:"deliveryMode"}, absent mode returns -32602.

### 2. "Canonical-JSON equality" for params is undefined, including absent vs {} vs null

**Sketch section:** Subscription Identity (key composition)  
**Kind:** Underspecified

The subscription key compares params "by canonical-JSON equality," but canonical JSON is never defined: key ordering, number normalization (1 vs 1.0 vs 1e0), string escaping, and — most practically — whether an omitted params field, params: {}, and params: null address the same subscription. A client whose JSON serializer does not preserve key order, talking to a server that compares params textually, will silently create a new subscription on every "refresh" instead of refreshing the existing one — duplicate deliveries, quota exhaustion, and orphaned subscriptions until TTL. The same question applies to the params field on events/poll and events/stream requests (and to the canonicalHash(params) poll-lease key in Server SDK Guidance).

**What this implementation assumed:** Implemented recursive lexicographic key sort with compact separators and no float normalization; absent params canonicalize as null, so params: {} is a distinct subscription from omitted params, and clients must pass a byte-identical params shape on refresh/unsubscribe.

### 3. base64 alphabet and padding for whsec_ secrets and v1 signatures are unspecified

**Sketch section:** Webhook Security → Signature scheme / Secret generation  
**Kind:** Underspecified

The sketch says secrets are whsec_ + "base64 of 24–64 random bytes" and signatures are "encoded as base64," but never specifies standard vs URL-safe alphabet or whether padding is required, accepted, or forbidden. A 64-byte secret encodes with a padding character, so the choice is directly observable on the wire; a sender using URL-safe or unpadded encoding produces signatures that off-the-shelf Standard Webhooks verifiers (which the sketch promises "work without modification") will reject, so every delivery fails verification. The referenced Standard Webhooks spec resolves this, but the sketch's restatement should pin it explicitly (standard alphabet, with padding) or cite the exact normative clause.

**What this implementation assumed:** Standard base64 alphabet with padding for both whsec_ decoding and signature encoding; non-canonical input (bad padding, URL-safe characters) is rejected at subscribe time and treated as non-matching during verification.


---

## Medium

### 1. Behavior for a present-but-invalid bearer token on non-webhook methods is unspecified

**Sketch section:** Summary → State and identity / Subscription Identity (Authentication required)  
**Kind:** Underspecified

Authorization uses "the same MCP principal as for tools," webhook methods require an authenticated principal, and unauthenticated servers may offer poll and push — but nothing says what a server should do on events/poll, events/stream, or events/list when a bearer token is present but invalid: reject at the HTTP layer (401), reject the RPC (-32012 Forbidden), or degrade to anonymous. Degrading to anonymous silently changes which events/params a caller may see, which is security-relevant divergence.

**What this implementation assumed:** Unknown/malformed tokens resolve to an anonymous principal with a warning; poll/push/list remain usable, and webhook methods reject anonymous callers with -32012.

### 2. Semantics of the events capability beyond the single listChanged example are unstated

**Sketch section:** Capability Declaration  
**Kind:** Underspecified

Only {"events": {"listChanged": true}} is shown. The sketch does not say whether an empty {"events": {}} validly advertises event support, what absent listChanged implies, whether clients must declare anything events-related in their own initialize capabilities to call events/*, or what a client should do if the server omits the events capability yet still answers events/list (proceed or refuse). Two client implementations can diverge on whether events are usable against the same server.

**What this implementation assumed:** listChanged modeled as optional (absent = unknown/false); clients declare nothing events-specific, log a warning when the capability is absent, and proceed.

### 3. notifications/events/list_changed collides with the rules stated for all notifications/events/* messages

**Sketch section:** Dynamic Event Types: notifications/events/list_changed / Push-Based Delivery → Event Delivery  
**Kind:** Contradiction

The sketch says "Every notifications/events/* message carries the JSON-RPC id of the parent events/stream request in params._meta[...subscriptionId]" and "The events/stream response carries only notifications/events/* messages" — but notifications/events/list_changed is itself a notifications/events/* method with no parent stream and no subscription id. Read literally, a server must either send list_changed on open event streams (where subscription-id routing would drop it) or violate the quoted rules; a client that routes all notifications/events/* by _meta.subscriptionId will discard list_changed entirely. The sketch should state that list_changed travels on the transport's standard notification channel (GET stream / stdout) and carve it out of both rules.

**What this implementation assumed:** Not implemented — review finding (the demo sends list_changed on the standard channel without _meta.subscriptionId, which violates the letter of both quoted rules).

### 4. Absent vs explicit-null request cursor is never distinguished

**Sketch section:** Poll-Based Delivery → Request: events/poll (also events/stream, events/subscribe)  
**Kind:** Underspecified

Every request example shows "cursor": null with the comment "null = start from now," but the sketch never states whether the cursor field may be omitted entirely, and if so whether omission is equivalent to null or invalid. The field is not marked required anywhere. A strict server that requires the field will reject clients that omit it; the same question applies to events/stream and events/subscribe requests. (Merged from four independent reports.)

**What this implementation assumed:** Absent treated as equivalent to null ("start from now") on receive; clients always serialize the field explicitly (null when starting from now) to match the examples byte-for-byte.

### 5. Optionality of poll response fields (cursor, truncated, hasMore, nextPollMs) is never tabulated

**Sketch section:** Poll-Based Delivery → Response  
**Kind:** Underspecified

EventOccurrence gets a field table with Required columns, but the poll response envelope does not: the sketch never says which of cursor, truncated, hasMore, and nextPollMs must be present in every response. A server that omits nextPollMs (expecting the client to use its own default) or omits truncated when false will break a client that parses the response with all four fields required — and the client's poll cadence is undefined without nextPollMs.

**What this implementation assumed:** Not implemented — review finding (the demo always emits all four fields, matching the examples; tolerant clients should treat truncated/hasMore as default-false and apply a local interval when nextPollMs is absent).

### 6. Interaction of maxEvents/hasMore/cursor with param filtering is undefined

**Sketch section:** Poll-Based Delivery (emit-only buffer + match/transform hooks)  
**Kind:** Underspecified

The SDK applies author match()/transform() hooks when events/poll reads the emit-fed ring buffer, but the sketch never says whether maxEvents counts events before or after filtering, whether the response cursor may advance past filtered-out events, or whether hasMore is true only when a further MATCHING event exists. A server that does not advance the cursor past trailing non-matching events can stall a poll loop (repeated empty batches at the same position); one that counts pre-filter returns chronically short batches.

**What this implementation assumed:** maxEvents caps post-filter events; filtered-out events advance the cursor without counting toward the cap; hasMore is true only when a further matching event exists; trailing non-matching events are consumed so the cursor never stalls.

### 7. hasMore: true bypasses the anti-tight-loop floor entirely

**Sketch section:** Poll-Based Delivery → Response notes / Client SDK Guidance (poll mode)  
**Kind:** SDK-guidance gap

Clients SHOULD apply a configurable floor (default 1000 ms) to nextPollMs "to guard against a misbehaving server inducing a tight loop" — but the same section instructs clients to poll again immediately, ignoring nextPollMs, whenever hasMore is true. A misbehaving or buggy server can therefore induce exactly the tight loop the floor was meant to prevent by always returning hasMore: true. The sketch gives no bound (drain budget, max consecutive immediate polls, or a smaller hasMore floor) on the immediate-drain path. One implementation also noted the floor default itself drives divergent client cadences when left configurable.

**What this implementation assumed:** One harness drains hasMore batches with no floor at all, exactly as written, and is therefore vulnerable to the loop; floor applied only to nextPollMs waits.

### 8. No guidance when the upstream provides no stable event id

**Sketch section:** Poll-Based Delivery → EventOccurrence schema (eventId)  
**Kind:** SDK-guidance gap

eventId SHOULD be the upstream's stable identifier so dedup works across delivery paths, with SDK auto-generation as the fallback — but many real upstreams (e.g., change-feed/SSE sources that emit no per-diff identifier) provide nothing stable. The sketch never discusses the consequence that randomly generated ids break cross-path and replay dedup for such upstreams, nor suggests deriving a deterministic id (e.g., a hash of source id + row content + position). Different servers will pick UUIDs vs content hashes, with materially different dedup behavior for the same upstream.

**What this implementation assumed:** The demo's real upstream feed sets no upstream id and falls back to UUIDv4, accepting that re-deliveries of the same upstream change cannot be deduplicated; the mock feed supplies stable ids.

### 9. The client's dead-stream rule depends on a heartbeat interval it cannot learn

**Sketch section:** Push-Based Delivery → Lifecycle (Heartbeat)  
**Kind:** Underspecified

The client SHOULD treat the stream as dead after silence "more than twice the heartbeat interval," but the interval is server-chosen, never carried on the wire (not in events/list metadata, not in the active frame), and not negotiable. The mandated client rule is uncomputable as written except by assuming the "SHOULD send at least every 30 seconds" ceiling — which a server may legitimately exceed in either direction. Either the active frame should carry the interval or the client rule should be restated against the 30 s ceiling. (Merged from three reports.)

**What this implementation assumed:** Clients assume the 30 s ceiling and use a fixed 60 s idle watchdog, reconnecting with the last persisted cursor on expiry; the demo server uses a 15 s (or 5 s for demos) configured interval and does not advertise it.

### 10. What position the initial active.cursor represents in the non-truncated case is never defined

**Sketch section:** Push-Based Delivery → Event Delivery (notifications/events/active)  
**Kind:** Underspecified

The confirmation notifications/events/active carries a cursor before any events flow, but the sketch only defines it for the truncated case ("returns that position as the fresh cursor"). When the client supplied a valid cursor and truncated is false, it is unstated whether active.cursor is the supplied value echoed back, the validated/clamped start position, or the current head. Clients persist this value, so a server that echoes the head while honoring replay can cause a client crash-restart to skip the backlog it was owed.

**What this implementation assumed:** active.cursor = the effective start position after applying the maxAgeMs floor / invalid-cursor reset; equals the supplied cursor when replay is honored and the current head when the request cursor was null.

### 11. Per-event cursors during reconnect replay are implied but never specified

**Sketch section:** Push-Based Delivery → Cursor Advancement / Reconnection after failure  
**Kind:** Underspecified

Push puts the cursor on each event ("position after this event"), but the sketch says nothing about how replay after reconnect interacts with that: a server reading backlog in batches naturally has only a batch-final position, so either every replayed event needs an individually computed cursor or some replayed events must omit it. The schema marks cursor optional, but the cursor-advancement rules implicitly require it on every event; servers will diverge on whether replayed events carry per-event cursors, changing the client's crash-recovery granularity mid-replay.

**What this implementation assumed:** Every replayed event carries its own position-after cursor (buffer paged one event per read); the stream never emits an event frame without a cursor for replay-capable types.

### 12. Client behavior after a graceful server-initiated close is unspecified; no reconnect backoff guidance

**Sketch section:** Push-Based Delivery → Lifecycle (Stream termination / Reconnection after failure)  
**Kind:** Underspecified

"Reconnection after failure" covers dropped connections, but the sketch never says what a client should do when the server gracefully closes the stream with the final StreamEventsResult frame: reconnect immediately, back off, or treat the subscription as over. One client implementation treating a routine server close (e.g., a deploy) as end-of-subscription silently stops receiving events while another reconnects forever. No backoff guidance is given for either case, so reconnect storms against a restarting server are unconstrained.

**What this implementation assumed:** Server-initiated close treated like a drop: reconnect with the last persisted cursor using exponential backoff 1 s → 30 s (reset on any received frame); only notifications/events/terminated ends the subscription.

### 13. No guidance for unknown or malformed frames on the event stream

**Sketch section:** Push-Based Delivery → Event Delivery  
**Kind:** SDK-guidance gap

The stream carries only notifications/events/* messages, but the sketch does not say what a client should do on an unknown notification method (e.g., from a newer server revision), a notification with malformed params, or a non-JSON data: frame — kill the stream, skip, or surface an error. Strict clients that kill the stream will break against any future additive extension of the notifications/events/* namespace; a tolerance rule (ignore unknown methods, warn on malformed frames) would preserve forward compatibility.

**What this implementation assumed:** Clients warn and skip unknown/malformed notification frames (stream stays alive); only a malformed final response frame or a JSON-RPC error response is fatal.

### 14. Mid-stream gap re-sync: truncated may be forced true when nothing was lost, and active-vs-backlog ordering is unspecified

**Sketch section:** Push-Based Delivery / Cursor Lifecycle → Gaps and truncated  
**Kind:** Contradiction

A mid-stream gap produces a fresh active {cursor:<fresh>, truncated:true} and delivery "continues," but truncated is elsewhere defined as "events were skipped." A server whose live fan-out channel lagged but whose retention buffer still holds everything can re-sync with zero loss — sending truncated: true is then a false positive that triggers unnecessary client re-fetch of authoritative state. The sketch also never specifies the ordering of the fresh active frame relative to any retained backlog the server can still deliver, which determines how the client should interpret cursors on the surrounding frames.

**What this implementation assumed:** On internal lag, emit active{cursor: current position, truncated: true} first, then drain still-retained events, then resume the live tail — accepting that truncated may be a false positive when retention covered the lag.

### 15. The response cursor at create time, and truncated on a live no-op refresh, are undefined

**Sketch section:** Subscribing: events/subscribe (response cursor / truncated)  
**Kind:** Underspecified

The response cursor is defined as a safe-to-persist watermark (everything at-or-before it acked or abandoned), but at create time with a requested replay cursor nothing has been delivered yet, so no position satisfies that definition; the sketch never says what the initial watermark is or how maxAgeMs/retention clamping is reflected in it. Conversely, on a live refresh where the supplied cursor is treated as a no-op, truncated is defined as "delivery started later than the supplied cursor" — a literal implementation comparing the supplied (stale-by-design) cursor against the current watermark would return truncated: true on every routine refresh, a persistent false gap signal.

**What this implementation assumed:** Create returns the validated requested position (supplied cursor clamped forward past the maxAgeMs floor/retention boundary, never past a deliverable event) with truncated reflecting that clamping, and seeds the stored watermark from it; live refreshes compute truncated as false (no-op cursor is not a gap) — the refresh aspect is otherwise not implemented — review finding.

### 16. Lapsed-but-unswept subscriptions: refresh vs fresh create, and quota accounting

**Sketch section:** Subscription TTL / Subscribing: events/subscribe  
**Kind:** Underspecified

"If the subscription has expired ... the server creates a fresh subscription using the provided cursor" — but with periodic expiry sweeps, a TTL-expired record can still be physically present when the re-subscribe arrives. The sketch doesn't say the upsert must treat it as absent (adopting the supplied cursor and resetting delivery bookkeeping), nor whether expired-but-unswept subscriptions count toward the ResourceExhausted subscription quota. Servers diverge on whether a re-subscribe after expiry replays from the supplied cursor or silently continues the stale record, and on whether dead records block new subscriptions.

**What this implementation assumed:** A lapsed record is treated as absent: outcome is a fresh create, supplied cursor adopted, failure bookkeeping reset; the per-principal quota counts only non-lapsed subscriptions.

### 17. A refresh cursor ahead of the server's in-flight watermark has no defined behavior

**Sketch section:** Subscription Identity (idempotent upsert field table)  
**Kind:** Underspecified

The cursor row of the upsert table covers "at or behind the current in-flight position" (no-op) and "lapsed or server restarted" (replay point). A live subscription refreshed with a cursor AHEAD of the server's watermark — impossible for a well-behaved client but trivially constructible (bug, forged value, state restored from a different machine) — has no defined behavior. A server that skips forward silently drops unacked events; one that ignores it does not.

**What this implementation assumed:** A refresh never moves a live subscription's stored watermark (any supplied cursor is a no-op); supplied cursors only seed newly created or lapsed-recreated subscriptions.

### 18. delivery.url equality/normalization for the subscription key is unspecified

**Sketch section:** Subscription Identity (key composition)  
**Kind:** Underspecified

params get canonical-JSON equality, but nothing is said about how delivery.url is compared: trailing slash, host case, percent-encoding, default port (https://h/ vs https://h:443/). The choice changes whether a refresh hits the same key (vs creating a duplicate subscription), how the per-principal quota counts, and whether the per-(principal, url) verification cache applies — so client-side URL normalization meeting server-side byte comparison silently forks subscriptions.

**What this implementation assumed:** Raw byte-for-byte string equality on delivery.url as supplied; no normalization. Clients must echo the identical string on refresh/unsubscribe.

### 19. When an event is "abandoned" is never defined, yet abandonment is load-bearing for the watermark cursor

**Sketch section:** Webhook Event Delivery (delivery model / safe watermark)  
**Kind:** Underspecified

The server "retries each event independently with exponential backoff," and the safe-to-persist cursor advances only past events that have been "acknowledged ... or abandoned" — but the sketch never defines when a server gives up on an event (max attempts, max age, or never). An implementation that never abandons stalls the watermark indefinitely behind one permanently failing event, freezing the client's cursor and eventually pushing it outside the upstream retention window; one that abandons quickly silently drops events. At minimum the spec should say servers MUST bound retries per event and SHOULD signal abandonment-induced skips (gap/truncated).

**What this implementation assumed:** Review finding surfaced from implementation: the demo retries 1 s/5 s/25 s, then abandons the event, advances the watermark past it, and records the failure category.

### 20. Unit of the suspension failure counter is ambiguous (failed attempt vs abandoned event)

**Sketch section:** Webhook Event Delivery / Webhook Delivery Status  
**Kind:** Ambiguity

"After repeated failures (server-defined threshold), the server MAY suspend delivery" never defines whether a "failure" is one failed POST attempt or one event abandoned after all its retries. Counting only abandoned events multiplies the time-to-suspend by the full per-event retry envelope; counting attempts suspends much sooner. The two readings produce materially different flood behavior against a dead endpoint, and shape what lastError reflects.

**What this implementation assumed:** Every failed POST attempt counts toward the consecutive-failure streak (and updates lastError); the final failed attempt coincides with abandoning the event, satisfying both readings.

### 21. eventId is placed verbatim in the webhook-id HTTP header with no character-set or length constraint

**Sketch section:** Webhook Event Delivery (headers) / EventOccurrence schema (eventId)  
**Kind:** Underspecified

webhook-id carries the eventId for event deliveries, and eventId SHOULD be the upstream's stable identifier — an arbitrary, upstream-controlled string. The sketch never constrains eventId's character set or length, so an upstream id containing CR/LF, non-ASCII bytes, or excessive length is injected directly into an HTTP header: at best the delivery fails, at worst it is a header-injection vector through the signing path (the signed string also concatenates webhook-id with "."-delimiters, so a "."-bearing id is fine but a header-illegal one is not). The spec should require header-safe eventIds or define an encoding for the header.

**What this implementation assumed:** Not implemented — review finding (the demo's ids are header-safe by construction: UUIDs or <epoch>:<seq>-derived).

### 22. Receiver HTTP status codes for invalid deliveries are unspecified

**Sketch section:** Webhook Event Delivery / Webhook Security → Signature scheme (receiver behavior)  
**Kind:** Underspecified

The sketch defines 2xx as ack, 413 as non-retryable, and 503/425 for the subscribe/delivery race, but never says what a receiver should return for a bad signature, stale timestamp, missing required headers, or an unparseable body. The choice matters: a receiver returning 5xx for forged/invalid deliveries induces full retry cycles and skews the server's http_5xx failure accounting and suspension behavior, while 4xx does not. Recommended receiver statuses (e.g., 401 for signature failure, 400 for malformed/stale) would align retry and diagnostics behavior across implementations.

**What this implementation assumed:** Receiver returns 401 for signature failure, 400 for stale/unparseable timestamp, missing headers, or malformed body, 405 for non-POST — all 4xx so the server categorizes http_4xx.

### 23. webhook-id deduplication conflicts with verification-challenge retries

**Sketch section:** Non-event webhook bodies (webhook-id dedup) / Webhook Security → Endpoint verification  
**Kind:** Ambiguity

Receivers SHOULD dedup on webhook-id, and control envelopes carry msg_<type>_<random> ids precisely "so receivers can dedup retries" — but a receiver that dedup-drops a retried verification envelope (e.g., because its first 2xx echo was lost in transit) returns 2xx without echoing the challenge, and verification fails permanently for that attempt. The interaction is unaddressed; the spec should state that the challenge echo is idempotent and MUST be repeated on duplicate verification deliveries.

**What this implementation assumed:** Receiver always echoes the challenge (idempotent), using dedup only to suppress duplicate processing/logging of gap/terminated/event bodies.

### 24. Control-envelope bookkeeping and the gap envelope's cursor position are unspecified

**Sketch section:** Non-event webhook bodies  
**Kind:** Underspecified

Several behaviors around control envelopes are undefined: whether an acked gap/verification POST counts as a "delivery" for deliveryStatus.lastDeliveryAt and the consecutive-failure streak; what position the gap envelope's "<fresh>" cursor denotes (head at lag-detection time, or some replayable boundary) — which determines exactly how many events the client gives up on; whether the gap envelope is ordered through the same delivery queue as events; whether a terminated envelope is retried on failure; and the format/entropy of the <random> component of msg_<type>_<random> (and whether it is stable across retries of the same control message, which dedup requires).

**What this implementation assumed:** Hyphenless UUIDv4 for <random>, stable per message across retries; any acked POST updates lastDeliveryAt and clears the streak; gap cursor = buffer head at lag detection, with the envelope ordered through the same per-subscription FIFO.

### 25. deliveryStatus snapshot timing, reactivation side-effects, and failedSince lifecycle are undefined

**Sketch section:** Webhook Delivery Status  
**Kind:** Ambiguity

The failing-refresh example shows active: false with a note that "the refresh that returned this status has just reactivated it" — i.e., the response reflects PRE-refresh state while server state is already active, which is easy to implement backwards. Unspecified follow-ons: whether reactivation resets the consecutive-failure counter (if not, the next single failure instantly re-suspends), whether lastError/failedSince are cleared by the refresh or only by the next successful delivery, and failedSince itself appears only in example JSON with no prose defining when it is set (first failure of the current streak?) or cleared. (Merged from two reports.)

**What this implementation assumed:** Refresh responses snapshot pre-refresh state while the store reactivates; reactivation resets the failure streak; failedSince is set at the first failure of the current streak and, with lastError, cleared only by the next successful delivery.

### 26. The closed lastError/data.reason category list is incomplete and inconsistently applied

**Sketch section:** Webhook Delivery Status / Error Codes (-32015) / Webhook Security  
**Kind:** Underspecified

lastError "MUST be ... one of" six categories, but mandated failure modes have no value: DNS resolution failure and SSRF-policy rejection (both required detections at delivery time), and 1xx/3xx responses (redirects MUST NOT be followed, making 3xx a failure that is neither http_4xx nor http_5xx). Separately, §Endpoint verification maps failure to challenge_failed or connection categories only, while §Error Codes says -32015 data.reason spans all six — leaving non-2xx verification responses (e.g., a 500) unclassified between challenge_failed and http_5xx. As written, a conforming implementation literally cannot label some mandatory failures. (Merged from three reports.)

**What this implementation assumed:** DNS/SSRF rejections map to connection_refused (preserving the no-response-oracle property); 1xx/3xx fold into http_4xx; non-2xx verification responses map to http_4xx/http_5xx with challenge_failed reserved for 2xx-with-bad-echo.

### 27. The exact string form of webhook-timestamp inside the signed string is unstated

**Sketch section:** Webhook Security → Signature scheme  
**Kind:** Underspecified

HMAC-SHA256(secret, webhook-id + "." + webhook-timestamp + "." + body) concatenates the timestamp as text, but the sketch never pins the textual form (decimal, no leading zeros, no sign) nor states that the receiver MUST use the header value verbatim rather than a parsed-and-reformatted integer. A receiver that parses and re-serializes the timestamp (or a sender emitting a padded form) produces signature mismatches that are extremely hard to debug.

**What this implementation assumed:** Sender formats seconds as plain decimal (default integer rendering); receiver verifies using the raw header string, never a re-serialized value.

### 28. Multi-signature header parsing and malformed-entry handling are loosely specified

**Sketch section:** Webhook Security → Signature scheme (multi-signature)  
**Kind:** Underspecified

The header "MAY contain multiple space-delimited signatures (v1,<sigA> v1,<sigB>)"; the sketch does not say whether exactly one ASCII space is required, whether other whitespace is tolerated, or how a receiver should treat undecodable entries or unknown-prefix entries (the sketch does say endpoints ignore v1a, but says nothing about other unknown prefixes). A receiver that hard-errors on any unrecognized entry will reject dual-signed rotations or future scheme additions.

**What this implementation assumed:** Receiver splits on whitespace, skips entries without a v1, prefix and entries whose base64 fails to decode, and accepts if any remaining candidate matches in constant time with no early return.

### 29. The freshness window is one-sided: future-dated timestamps are unaddressed

**Sketch section:** Webhook Security → Signature scheme (freshness)  
**Kind:** Underspecified

Receivers "SHOULD reject deliveries where webhook-timestamp is more than 5 minutes old," but nothing is said about timestamps in the future. Under clock skew this matters for replay protection: an attacker who captures a delivery pre-dated into the future extends its replay life arbitrarily against receivers that only check staleness. Standard practice (and Standard Webhooks guidance) is a symmetric window.

**What this implementation assumed:** Receivers enforce a symmetric ±5-minute window (|now − ts| > 300 s rejected).

### 30. Verification sequencing, the record's fate on challenge failure, handshake-window events, and nonce/retry policy are unspecified

**Sketch section:** Webhook Security → Endpoint verification  
**Kind:** Underspecified

The sketch mandates verification before delivery activation but leaves the surrounding state machine open: (1) whether the challenge POST happens before or after the quota check and record creation, and whether a subscription record survives a failed challenge (-32015) — determining whether a failed subscribe leaves residual state and whether an over-quota subscribe can still trigger a POST at the victim URL; (2) whether events emitted between the subscribe-time cursor computation and challenge completion are owed to the subscription, or silently skipped; (3) the challenge nonce's concrete lifetime and whether the server retries the verification POST before failing the subscribe. (Merged from three reports.)

**What this implementation assumed:** Quota enforced before any challenge POST; on challenge failure the record is removed and -32015 returned (no residual state); handshake-window events are backfilled from the stored watermark once verification succeeds; exactly one verification attempt per subscribe, nonce scoped to that request, client re-subscribes to retry.

### 31. The unsubscribe success-response shape is never defined

**Sketch section:** Unsubscribing: events/unsubscribe  
**Kind:** Underspecified

The sequence diagram shows "Server-->>SDK: (ack)" and the prose never specifies the result payload of a successful events/unsubscribe — empty object, typed result, or _meta-bearing envelope. Every other method has an example response. Strictly typed clients and servers can disagree on the wire shape of a core method's result.

**What this implementation assumed:** Clients accept and discard any JSON result value; the demo server returns an empty object.

### 32. The reset position for unserveable cursors (foreign, unparseable, or ahead of the head) is unspecified

**Sketch section:** Cursor Lifecycle → Gaps and truncated  
**Kind:** Ambiguity

For a cursor the server cannot serve from, the sketch only says the server "resets to a position it can serve from" and returns it with truncated: true. It never says whether that position is the oldest retained event (replay all retained history) or the current head (skip everything) — two reasonable servers deliver wildly different volumes for the same input. It also never says whether maxAgeMs still bounds replay after such a reset (maxAgeMs is defined only relative to a valid supplied cursor), and the case of a syntactically valid cursor pointing PAST the newest event (forged, corrupted, or from a buffer that lost writes) is never mentioned at all. (Merged from two reports.)

**What this implementation assumed:** Foreign/unparseable cursors replay from the oldest retained event with the maxAgeMs floor still applied (at-least-once bias; eventId dedup absorbs duplicates); a cursor ahead of the head resets to the current head with no events and truncated: true.

### 33. truncated when the floor passes the cursor through empty space: two definitions conflict

**Sketch section:** Cursor Lifecycle → Bounding replay with maxAgeMs / Gaps and truncated  
**Kind:** Contradiction

§Bounding replay says "If the floor advances past the cursor, the server SHOULD set truncated: true," while §Gaps and truncated defines truncated as the signal that "events were skipped." When the maxAgeMs floor is later than the cursor position but no events exist in the interval, the first rule says true and the second says false. Since truncated: true instructs clients to re-fetch authoritative state, false positives have a real cost; the spec should pick one definition (events-actually-skipped is implementable for buffer-backed servers, position-based is cheaper for others) or explicitly permit the conservative answer.

**What this implementation assumed:** truncated set only when retained events after the cursor were actually dropped by the floor; a floor passing through empty space yields truncated: false.


---

## Low

### 1. Behavior for an invalid events/list pagination cursor is unstated

**Sketch section:** Listing Available Events (pagination)  
**Kind:** Editorial

events/list takes an optional pagination cursor with "same semantics as tools/list etc.," but the behavior for a stale or garbage pagination cursor — especially on a server that never paginates — is unstated (base MCP hints invalid cursor → -32602; this sketch says nothing).

**What this implementation assumed:** Server accepts and ignores any cursor param, always returning the full single-page list with nextCursor absent.

### 2. Whether error `message` must equal the canonical code name is unclear

**Sketch section:** Error Codes  
**Kind:** Ambiguity

The error table's Message column gives PascalCase names (NotFound, Forbidden, ...) and the examples use them verbatim with human detail relegated to data, but the sketch never states whether message MUST be the canonical name or may be free-form text. Clients should branch on code/data, so this is editorial, but the examples invite string-matching on message.

**What this implementation assumed:** Canonical name used as message except where a caller-supplied message exists; clients never branch on message.

### 3. No precedence among error codes when several apply, and unsubscribe's data.kind for unknown event names is ambiguous

**Sketch section:** Error Codes  
**Kind:** Ambiguity

When a request is simultaneously NotFound (unknown name), Unsupported (mode not offered), Forbidden (no principal), and InvalidParams (bad params/secret/url, over quota), the sketch defines no check ordering, so different servers surface different codes for identical requests. For unsubscribe with an unknown event name, "the method implies which" suggests data.kind=subscription, but the actually-missing entity is the event type. Diagnostic divergence only — clients must handle all codes anyway. (Merged from two reports.)

**What this implementation assumed:** One implementation's order: principal → delivery mode → webhook offered → event exists → params → url → secret → quota → verification; unsubscribe returns kind="event" for unknown event names, kind="subscription" otherwise.

### 4. Required depth of inputSchema enforcement for subscription params is unspecified

**Sketch section:** Error Codes (-32602) / Listing Available Events (inputSchema)  
**Kind:** SDK-guidance gap

-32602 is defined for "params don't match the event's inputSchema," implying full JSON Schema validation, but the sketch elsewhere says param semantics are entirely server-defined. How deep validation must go (types, required, unknown keys) is unstated, so the same subscribe may be accepted by one server and rejected by another.

**What this implementation assumed:** Targeted hand-rolled validation of declared filter fields; unknown keys accepted per default JSON Schema semantics.

### 5. No recommended default or upper bound when maxEvents is omitted

**Sketch section:** Poll-Based Delivery → Response notes (maxEvents)  
**Kind:** Underspecified

"If omitted, the server uses its own default limit" gives no recommended default or ceiling, so an omitted maxEvents against a deep buffer can legally return the entire retention window (potentially tens of thousands of events) in one response.

**What this implementation assumed:** No extra cap applied when absent; the batch is bounded only by per-type retention (default 10k in the demo).

### 6. Cursor portability across changed params (or event types) is unspecified

**Sketch section:** Poll-Based Delivery / Cursor Lifecycle  
**Kind:** Underspecified

Each poll is self-contained with (name, params, cursor), but the sketch never says whether a cursor obtained under one params value remains valid if the client re-polls with different params (or whether servers may bind cursors to a subscription identity and reject foreign ones). Servers that encode a params hash into the cursor will reject what others accept.

**What this implementation assumed:** Not implemented — review finding (the demo treats cursors as positions in the per-event-type buffer, valid under any params).

### 7. timestamp format and semantics are underdefined (canonical ISO 8601 form; occurrence vs processing time)

**Sketch section:** Poll-Based Delivery → EventOccurrence schema (timestamp)  
**Kind:** Underspecified

timestamp is "string (ISO 8601), when the event occurred" with Z-suffixed second-precision examples, but the sketch constrains neither the offset form, fractional seconds, nor UTC — relevant because webhook receivers are told to use timestamp for ordering, and mixed forms mis-order under naive comparison. It also gives no guidance when the true occurrence time is unknown and only an upstream processing time is available (common for change-feed sources). (Merged from two reports.)

**What this implementation assumed:** Timestamps passed through opaquely; upstream processing time used as the best available approximation of occurrence time.

### 8. SSE framing details beyond "data: frames" are unspecified

**Sketch section:** Push-Based Delivery (Streamable HTTP framing)  
**Kind:** Underspecified

Notifications are "SSE data: frames," but the sketch does not say whether event:/id: fields may be used, whether exactly one JSON-RPC message per SSE event is guaranteed, or whether a terminating blank line precedes server close after the final result frame. Lenient SSE parsers absorb most variants, but strict ones can diverge.

**What this implementation assumed:** Parser ignores event:/id: fields, treats each dispatched data payload as one JSON-RPC message, and leniently flushes a final unterminated frame at end-of-stream.

### 9. Interplay with the base transport's SSE resumability (id: / Last-Event-ID) is unaddressed

**Sketch section:** Push-Based Delivery (Streamable HTTP) / MCP base transport  
**Kind:** Underspecified

Base Streamable HTTP defines SSE stream resumability via id: fields and Last-Event-ID; the sketch defines its own cursor-based reconnect for events/stream and never says whether servers may emit SSE id: fields on the event stream, whether clients may attempt Last-Event-ID resumption, or that the cursor mechanism supersedes it. A client attempting base-transport resumption against a cursor-only server gets undefined behavior.

**What this implementation assumed:** Not implemented — review finding (the demo emits bare data: frames; the client reconnects with its cursor only).

### 10. Whether the final StreamEventsResult frame carries the subscription id in _meta

**Sketch section:** Push-Based Delivery → Lifecycle (Stream termination)  
**Kind:** Editorial

The sketch requires the subscription id on "every notifications/events/* message," and the final-frame example is result:{"_meta":{}} with empty _meta. Strictly the final frame is a response, not a notification, so there is no formal conflict — but summaries elsewhere phrase the rule as "every frame," and implementers stamped the id on the result too. One clarifying sentence (the response is routed by JSON-RPC id, not _meta) would settle it.

**What this implementation assumed:** Final frame is exactly {"jsonrpc":"2.0","id":<id>,"result":{"_meta":{}}} with no subscription id.

### 11. ttlMs value-range semantics (0, negative) are unaddressed

**Sketch section:** Subscription TTL (ttlMs)  
**Kind:** Underspecified

ttlMs is a nullable-integer suggestion in milliseconds with server-side clamping, but the sketch never says how a server should treat ttlMs: 0 or a negative value — reject as InvalidParams, or clamp up to the server minimum like any other short suggestion.

**What this implementation assumed:** Unsigned wire type, so negatives fail deserialization (effectively InvalidParams); 0 is clamped up to the server's minimum TTL.

### 12. Expiry boundary at exactly t == refreshBefore is unstated

**Sketch section:** Subscription TTL (refreshBefore)  
**Kind:** Editorial

refreshBefore is "when the subscription will expire" and clients must refresh "before refreshBefore" — whether the subscription is still alive at exactly that instant is unstated. Matters only for expiry sweeps and the upsert's lapsed check at the boundary.

**What this implementation assumed:** Inclusive expiry: lapsed when now >= refreshBefore.

### 13. Refresh-time meaning of maxAgeMs on a live subscription is unstated

**Sketch section:** Subscription TTL / Subscription Identity (refresh)  
**Kind:** Editorial

On a live refresh the supplied cursor is a defined no-op, but the sketch doesn't say what maxAgeMs means in that case — ignored, or applied to in-flight delivery. Only the create/lapsed path gives it a natural meaning (bounding the replay point).

**What this implementation assumed:** maxAgeMs ignored on a live refresh; it shapes the replay point only when a cursor is actually adopted.

### 14. Header-name casing is inconsistent within the sketch

**Sketch section:** Webhook Event Delivery (headers)  
**Kind:** Editorial

Standard Webhooks headers are written lowercase (webhook-id, webhook-timestamp, webhook-signature) while the MCP-specific header is mixed-case (X-MCP-Subscription-Id). HTTP headers are case-insensitive so this is cosmetic, but a spec should pick one convention; WAF-rule authors copying literally (per the Delivery profile section, which invites exact-header matching) could be confused.

**What this implementation assumed:** All four header constants emitted lowercase.

### 15. Dual-sign grace-window length is undefined

**Sketch section:** Webhook Security → Secret rotation  
**Kind:** Underspecified

"The server SHOULD dual-sign deliveries with both the old and new secrets for a short grace window" — the window's duration, and behavior under multiple rotations in quick succession (sign with all? last two?), are unspecified. Receivers verifying with the latest secret are unaffected since every retry re-signs, so impact is limited to receivers mid-rotation.

**What this implementation assumed:** Not implemented in the demo (single signature with the current secret, re-fetched per attempt); recorded as an open issue.

### 16. Null-vs-absent conventions for optional fields are only inferable from examples

**Sketch section:** Non-event webhook bodies / Webhook Delivery Status (serialization)  
**Kind:** Underspecified

The healthy deliveryStatus example serializes lastError: null explicitly but omits failedSince entirely; the failing example includes all four fields; no prose states which fields are always-present-nullable vs omittable. Likewise the gap envelope is shown only as {"type":"gap","cursor":"<fresh>"} — whether cursor may be null/omitted, and whether gap envelopes can occur at all for event types without replay support (presumably never, but unstated), is left to inference. Strict schema validation on either side can reject the other's output. (Merged from two reports.)

**What this implementation assumed:** lastDeliveryAt/lastError always serialized (nullable), failedSince omitted when absent, reproducing both literals; gap cursor modeled nullable; all tolerate absence on deserialize; gap envelopes never fire for non-replay types.

### 17. Unsubscribe is non-idempotent under retry: NotFound after a lost response is indistinguishable from never-existed

**Sketch section:** Unsubscribing: events/unsubscribe (idempotency)  
**Kind:** Underspecified

events/unsubscribe returns -32011 NotFound when "no subscription matching the key" exists, so a client that retries after a network failure (its first call may have succeeded with the response lost) receives an error for an operation that achieved its goal. The sketch gives no guidance that clients SHOULD treat NotFound on unsubscribe as success, or that servers MAY make unsubscribe idempotent.

**What this implementation assumed:** Not implemented — review finding.

### 18. No guidance on cursor encoding for restart detection

**Sketch section:** Server SDK Guidance → Emit-only event types  
**Kind:** SDK-guidance gap

The emit-buffer cursor is "a buffer-local sequence number ... process-scoped — a server restart invalidates all cursors," but there is no guidance on how a server recognizes a pre-restart cursor: a bare sequence number from a previous process can look perfectly valid to the new one and silently alias a different position, defeating the promised truncated-and-fresh-cursor behavior. A non-normative hint (embed a per-process epoch/nonce in the cursor) would close it.

**What this implementation assumed:** Cursors carry a random per-process epoch (<epoch>:<seq>) so pre-restart cursors are detectably foreign and routed through the truncated-reset path.

### 19. Watermark bookkeeping is specified for a concurrency model the SDK need not use

**Sketch section:** Webhook Event Delivery / Note on Consistency and Ordering (SDK guidance)  
**Kind:** SDK-guidance gap

The cursor_N rule ("does not include cursor_N in event N's payload until events at positions < N have been acked or given up on") and its in-flight-set bookkeeping presume concurrent per-event retries. With sequential per-subscription FIFO delivery — which the sketch permits and is the simpler implementation — the safe watermark degenerates to the event's own position. The sketch never tells SDK authors that serializing deliveries makes the mandated bookkeeping trivial, so they may build the complex machinery unnecessarily.

**What this implementation assumed:** Sequential FIFO delivery per subscription; payload cursor = the event's own position; the stored watermark advances on ack/abandon and past filtered-out events.

### 20. No guidance on refresh scheduling margin, refresh-failure retry, or the no-expiry health-check cadence

**Sketch section:** Client SDK Guidance → Webhook mode / Subscription TTL  
**Kind:** SDK-guidance gap

The SDK must refresh "before refreshBefore," but no safety margin (network latency, clock skew) or transient-failure retry policy is suggested — a refresh that fails once near the deadline can let the subscription lapse before the next attempt. For refreshBefore: null grants, the loop "drops to an occasional health-check cadence" with no number, range, or basis — yet that cadence determines how quickly the client notices suspended delivery via deliveryStatus. (Merged from three reports.)

**What this implementation assumed:** Refresh at ~80% of remaining lifetime (min 1 s); transient failures retried every 5 s, JSON-RPC errors fatal; 60 s health-check cadence for no-expiry grants in the demo.

### 21. The eventId dedup window is unbounded by the spec

**Sketch section:** Client SDK Guidance (deduplication)  
**Kind:** SDK-guidance gap

The SDK is told to deduplicate by eventId, but nothing bounds the dedup memory (event streams are unbounded over time) or says how long an id must be remembered to be effective across reconnect/replay overlap. Implementations will pick arbitrary, divergent windows.

**What this implementation assumed:** Bounded LRU set of 4096 ids per subscription; a replay deeper than that can re-surface duplicates.

### 22. HTTP status mapping for malformed POST bodies and missing/mismatched Accept headers is undefined in the base-protocol reference

**Sketch section:** MCP base protocol §2.2 Streamable HTTP (docs/mcp-reference.md — outside this sketch)  
**Kind:** SDK-guidance gap

Findings against the base transport, not the events sketch: (1) no HTTP status is defined for a POST body that is not valid JSON or not a JSON-RPC message, nor whether JSON-RPC error responses ride on HTTP 200 or an error status; (2) the client MUST send Accept: application/json, text/event-stream, but no server behavior (406? ignore?) is specified when it is missing or excludes the content type the response requires. Both shape interop for any server implementing events over Streamable HTTP. (Merged from two reports.)

**What this implementation assumed:** Unparseable JSON → HTTP 400 with -32700 body; non-request JSON → HTTP 200 with -32600; method-level errors on HTTP 200; Accept header ignored, content type chosen by method.


---

## Appendix: gaps surfaced by cross-implementation interop

These are spec ambiguities confirmed by *observed divergence between independent
implementations* — the strongest evidence class. Each was adjudicated against the sketch text.


### events/stream final result frame: {} vs {"_meta":{}} (dup: ts-agent, wire-diff)

Confirmed: TS _handleStream resolves {} mapped to EmptyResultSchema (events.ts:1164-1167); ours and mcpkit always emit {"_meta":{}} (mcpkit StreamEventsResult.Meta deliberately has no omitempty 'so the wire shape matches the spec example exactly', stream.go:42-46). The frame is defined to carry no information and _meta is optional on every MCP result type, so calling the TS shape a bug overreads a parenthetical example; but the fact that one implementer pinned the example exactly and another used {} shows the text is ambiguous. Static finding — no run ever exercised a server-initiated close.


> The StreamEventsResult is an empty typed result ({"_meta": {}}). It carries no information (sketch §Push-Based Delivery, Lifecycle)


**Recommendation:** add-to-spec-gaps — state that {} and {"_meta":{}} are equivalent empty results (or pin one normatively).


### events capability advertisement: listChanged semantics (ts-agent)

Confirmed: TS registers events:{listChanged: existing ?? true} unconditionally (events.ts:881-885) even for servers that never change their list; our mock advertises listChanged:false (handlers/initialize.rs:20-22); their client gates only on presence of the events key. Interop worked both ways (run1). The sketch genuinely never says whether events:{} is valid, what listChanged defaults to when absent, or that false is permitted — both implementations guessed compatibly but a third might not.


> Servers advertise event support in their capabilities: {"capabilities":{"events":{"listChanged":true}}} (sketch §Capability Declaration — sole example, no prose on false/absent/default)


**Recommendation:** add-to-spec-gaps — define listChanged default/absence and whether bare events:{} is a valid capability.


### whsec_ secret base64 alphabet/padding (dup: mcpkit-agent, wire-diff)

Confirmed: mcpkit generates whsec_ + base64.RawURLEncoding (URL-safe, unpadded; secret.go:44) and deliberately accepts both alphabets on input ('the SDKs don't agree on which they emit', secret.go:57-58); our parse_whsec requires the standard alphabet with padding (webhook.rs:4,30-41) and rejects their auto-generated secrets with -32602 before delivery is ever attempted; TS's regex accepts standard alphabet with optional padding (eventWebhook.ts:60). All three are defensible readings of 'base64'. Standard Webhooks examples use standard base64, which argues for pinning that, but the sketch text does not say so — every implementation guessed.


> the literal prefix whsec_ followed by base64 of 24–64 random bytes. Servers MUST reject values that do not satisfy this with InvalidParams (sketch §events/subscribe notes) — alphabet and padding never specified.


**Recommendation:** add-to-spec-gaps — pin the alphabet/padding (recommend standard base64, accept-both-on-input as a SHOULD); consider relaxing parse_whsec to accept URL-safe/unpadded input as interop hardening.


### events/poll request cursor: absent vs null (wire-diff)

Confirmed: TS requires the key (cursor: z.string().nullable(), not .optional(), schemas.ts:2229) so an omitting client gets InvalidParams; we and mcpkit treat absent ≡ null (events.rs:92-95 serde default; mcpkit *string). All examples in the sketch show the key present but no prose addresses omission. We always serialize the key (events.rs comment 'Always serialized'), so ours->TS works; this only bites third-party lenient clients.


> Poll request example: '"cursor": null // null = start from now' (sketch §events/poll) — never states whether the key may be omitted.


**Recommendation:** add-to-spec-gaps — state whether absent ≡ null on poll/stream/subscribe cursor (recommend yes, matching JSON-RPC conventions).


### events/subscribe refresh with a cursor ahead of/unrelated to the live position (wire-diff)

Confirmed both readings in code: our refresh treats any supplied cursor as a no-op and returns the stored watermark (webhook/handlers.rs:129-141 'Live refresh: the supplied cursor is a no-op'); TS documents and implements 'on refresh ... a non-null value replaces it' with _replayAfterCursor backlog re-delivery (schemas.ts:2362-2365, events.ts:1332-1344). The sketch's conditional only covers at-or-behind cursors, so both behaviors are defensible; observable difference is backlog re-POSTs from TS vs nothing from us.


> If the subscription is live and the cursor is at or behind the current in-flight position, this is a no-op (sketch §Subscription Identity field table) — silent on a cursor ahead of, or unrelated to, the in-flight position.


**Recommendation:** add-to-spec-gaps — define refresh semantics for a cursor ahead of/unrelated to the in-flight position (replace-and-replay vs ignore).


### deliveryStatus field null-vs-absent presence rules (wire-diff)

Confirmed: we always serialize lastDeliveryAt/lastError (null when none) and omit failedSince when absent (events.rs:203-213); TS is close to us; mcpkit omits all empty fields and the whole object when there is nothing to report (events.go:930-960 'Omitted on first subscribe — wire bloat'). All three agree on the load-bearing part (omitted on first subscribe, present on refresh) and the differences are absorbed by optional-field decoding. Minor.


> Healthy-refresh example shows '"lastError": null' present (sketch §Webhook Delivery Status); the prose never states presence rules for lastDeliveryAt/lastError/failedSince or for the object itself.


**Recommendation:** add-to-spec-gaps — one line stating null and absent are equivalent for deliveryStatus members (low priority).


### Error message wording for shared codes: literal 'NotFound' vs descriptive text (mcpkit-agent)

Confirmed: codes and typed data payloads agree between us and mcpkit (both -32011 + {kind}), but mcpkit sends the literal table token ('NotFound', 'Forbidden') as the message while we send descriptive strings ('unknown event type "nope"'). The sketch's own design intent ('clients still distinguish every case they branch on (by code, by method, or by a typed data discriminator)') implies messages are non-normative, but it never says so, and a message-matching client breaks one way or the other.


> Error table: | -32011 | NotFound | A referenced entity does not exist... (sketch §Error Codes) — the table never says whether the Message column is normative literal wire text.


**Recommendation:** add-to-spec-gaps — state that the Message column is a label, messages are non-normative free text, and clients MUST branch on code/data only.
