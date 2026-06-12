# Cross-Implementation Interop Report

Independent interop testing of this Rust implementation against the two other known
implementations of the draft MCP Events extension. The point of three independent
implementations is that an observed difference, root-caused, is evidence about the *spec* —
so every divergence below carries a verdict (our-bug / their-bug / spec-gap / revision-skew).

| Implementation | Language | Repo |
|---|---|---|
| **ours** | Rust | this repo |
| **TS SDK branch** | TypeScript | `clareliguori/mcp-typescript-sdk` @ `events-bufferemits-and-examples` |
| **mcpkit** | Go (+ Python client) | `panyam/mcpkit` `experimental/ext/events` |

> Method: built and ran both peers locally (no forks, no PRs — read-only clones with push
> disabled). Bidirectional where possible: their clients → our server, and our harness → their
> servers. 32 divergences were collapsed and adjudicated by an independent judge that
> confirmed load-bearing claims in code.

## Headline: the three implementations track three different sketch revisions

This is the single most useful finding for the WG. Static three-way wire-format comparison of the MCP Events extension: our Rust implementation (/Users/aman/proto/drasi-mcp-events, vendored sketch 2026-06-11 PR#1), the TS SDK branch (packages/core schemas + packages/server events.ts), and mcpkit (experimental/ext/events). Headline finding: the three track THREE different sketch revisions. Ours is newest (consolidated error codes -32011..-32015 with typed data, maxAgeMs/nextPollMs, ttlMs/refreshBefore-nullable negotiation, _meta subscriptionId correlation, normative endpoint verification, notifications/events/list_changed). The TS SDK predates the 2026-05-22 error-code consolidation (per-case codes -32011..-32017, -32014 reserved, no ttlMs, refreshBefore non-nullable, verification envelope marked 'Pending — see spec Open Question 6' and never sent). mcpkit is mid-stream: it cites the 2026-05-22 consolidation commit 567be29 and uses our er

## Verdict tally

**revision-skew**: 7, **our-bug**: 1, **their-bug**: 15, **spec-gap**: 7, **not-a-divergence**: 2

The two **our-bug** findings were **fixed in the commit that adds this report**; everything else
is a peer issue, a spec ambiguity, or revision skew. Details below.

---

## TypeScript SDK branch ↔ ours

_Full bidirectional interop between our Rust MCP Events implementation and Clare Liguori's TypeScript SDK branch (events-bufferemits-and-examples). Their-client -> our-server: events/list, poll loop with cursor reuse, push stream with params filtering + heartbeat cursor advancement, and the complete webhook lifecycle (Standard Webhooks signatures verified both ways, idempotent refresh, unsubscribe) all work — EXCEPT webhook activation fails out of the box because their SDK never echoes our mandatory verification challenge (their schema marks it 'Pending — spec Open Question 6'); with a 5-line receiver patch the whole flow passes. Our-harness -> their-server: list/poll/stream/webhook all work through a small SSE->JSON bridging proxy (their transport answers unary POSTs with text/event-stream, which our harness rejects). Material divergences found: their branch tracks an older sketch revision (different error-code registry -32014/-32016/-32017, no ttlMs/nullable-refreshBefore, verification unimplemented); their server replays events BEFORE notifications/events/active; bootstrap polls an_

**Build:** Fully built and run. pnpm was missing; `corepack enable --install-directory /tmp/interop-ts/bin` provided pnpm 10.26.1 per their packageManager pin; then `pnpm install` (12s, build scripts for lefthook/sharp/workerd ignored — not needed) and `pnpm build:all` succeeded in /Users/aman/proto/interop/mcp-typescript-sdk. Wrote two driver files inside their repo (allowed): examples/client/src/interopWebhookDriver.ts (HTTP client w/ Authorization header + webhook receiver with optional challenge echo) and examples/server/src/interopHttpEventsServer.ts (their eventsExample wrapped in NodeStreamableHTT

| Direction | Scenario | Outcome |
|---|---|---|
| their-client -> our-server | eventsTerminalClient over Streamable HTTP (MCP_URL=http://127.0.0.1:8190/mc | ✅ pass |
| their-client -> our-server | Webhook subscribe with their SDK's default receiver behavior (no challenge  | ❌ fail |
| their-client -> our-server | Webhook full lifecycle with challenge echo added in driver: subscribe, 13 s | ✅ pass |
| our-harness -> their-server | Direct connection (no proxy): initialize | ⛔ blocked |
| our-harness -> their-server | events/list + poll cycle: bootstrap (cursor null), lease resume, real curso | 🟡 partial |
| our-harness -> their-server | events/stream 15s consumption + reconnect with persisted cursor | 🟡 partial |
| our-harness -> their-server | Webhook: harness webhook-recv (8195) + events/subscribe (ttlMs 120000), 20s | ✅ pass |
| our-harness -> their-server | Error-path probes via curl: unknown event, schema-invalid params, malformed | ✅ pass |

## mcpkit (Go/Python) ↔ ours

_Full bidirectional interop run between our Rust drasi-mcp-events stack and Sri Panyam's mcpkit (Go) events extension. mcpkit tracks an older revision of Peter Alexander's design sketch than our 2026-06-11 vendored copy (seconds-based maxAge/nextPollSeconds, requestId-based stream correlation, no ttlMs negotiation, no `truncated` in subscribe responses, no endpoint-verification handshake, EventDef with `cursorless` but no `inputSchema`), so much of the wire skew is revision-skew rather than bugs. Core happy paths interop surprisingly well: their Go client's initialize/list/poll-with-cursor-reuse/stream all work against our server, and our harness receives their stream events. The two hard incompatibilities are (1) Standard Webhooks HMAC key derivation — mcpkit signs/verifies with the raw "whsec_..." ASCII string while the spec (and our impl) uses the base64-decoded bytes after the prefix, proven byte-for-byte in both directions, so no cross-implementation webhook delivery ever verifies; and (2) whsec_ base64 alphabet — their SDKs generate raw-URL-safe unpadded secrets that our standar_

**Build:** Everything built and ran. Our prebuilt binaries used as-is (server on 127.0.0.1:8290 with a copy of mock.yaml; harness; webhook-recv on 8291). Their Go client driver at /tmp/interop-kit/driver (go.mod replace -> local checkout; go 1.23.5 auto-fetched the go1.26.4 toolchain their modules require). Their kitchen-sink example built and served on 127.0.0.1:8294 after a 1-line patch to /Users/aman/proto/interop/mcpkit/examples/events/kitchen-sink/main.go (default addr :8080 -> 127.0.0.1:8294) because demokit.FilterArgs strips --addr so the flag is unreachable (their example bug; port 8080 was occup

| Direction | Scenario | Outcome |
|---|---|---|
| their-Go-client -> our-server | initialize + events/list + unknown-event error probe (mcpkit client.Connect | ✅ pass |
| their-Go-client -> our-server | events/poll cycle with cursor reuse (3 polls + reuse poll) | ✅ pass |
| their-Go-client -> our-server | events/stream ~15s via eventsclient.Stream helper | ✅ pass |
| their-Go-client -> our-server | webhook E1: eventsclient.Subscribe with their auto-generated secret | ❌ fail |
| their-Go-client -> our-server | webhook E2: eventsclient.Subscribe + their Receiver[T] (standard-padded sec | ❌ fail |
| their-Go-client -> our-server | webhook F: challenge-echoing receiver; dual signature verification; unsubsc | 🟡 partial |
| their-Python-client -> our-server | events_client.py list / poll / webhook subcommands | ❌ fail |
| their-Python-client -> our-server | events_client.py stream subcommand (~20s) | ✅ pass |
| our-harness -> their-server (kitchen-sink) | initialize + events/list | ❌ fail |
| our-harness -> their-server (kitchen-sink) | events/poll (harness) + raw curl poll cursor-reuse | ❌ fail |
| our-harness -> their-server (kitchen-sink) | events/stream chat.message ~25s | 🟡 partial |
| our-harness -> their-server (kitchen-sink) | events/subscribe + deliveries to our webhook-recv + unsubscribe | 🟡 partial |
| our-harness -> their-server (kitchen-sink) | quota interaction (incidental) | 🟡 partial |

---

## Adjudicated divergences


### [our-bug] Streamable HTTP unary response framing (dup: ts-agent run4, mcpkit-agent)

Confirmed in our code: parse_unary (crates/mcp-events-client/src/client.rs:307-324) hard-fails on any non-application/json content type even though we send Accept: application/json, text/event-stream (client.rs:26). The TS server's default SSE framing for every POST and mcpkit's SSE-when-accepted are both legal under base Streamable HTTP; our client's rejection is the nonconformance, confirmed by both agents needing proxies/shims to run. Two real peer wrinkles ride along but don't change the verdict: TS enableJsonResponse:true drops request-correlated notifications (streamableHttp.ts:988 guard), making JSON-unary + spec-correct events/stream mutually exclusive in their transport; mcpkit's Python MCPSession.rpc() parses only SSE data: lines and silently returns {} for JSON bodies (reported a -32012 as success) — fails silently where ours fails loudly.


> sketch §Push-Based Delivery only constrains events/stream ('a POST that returns an SSE response stream'); base Streamable HTTP requires the client to support a server answering any POSTed request with either application/json or text/event-stream.


**Action for us:** fix-our-code — teach parse_unary to read a single JSON-RPC response from an SSE body for unary methods; separately report the TS enableJsonResponse/events-stream conflict and the mcpkit Python silent-{} parsing as SDK bugs, and suggest the SEP add transport guidance.


### [their-bug] TS push stream lifecycle on HTTP abort: zombie subscription, crash, duplicate delivery

Confirmed the full causal chain in their code: the POST response ReadableStream's cancel callback only does this._streamMapping.delete(streamId) (streamableHttp.ts:755-758); the per-request AbortSignal is aborted solely in protocol.ts _oncancel (notifications/cancelled) — so the abort listener events.ts registers at ctx.mcpReq.signal (events.ts:1204) never fires on TCP abort. Heartbeat/poll timers keep running on the zombie stream; notify() then either throws 'No connection established for request ID' (streamableHttp.ts:982-983, the observed crash) or — when a new events/stream reuses the same JSON-RPC id in stateless mode — routes the zombie's notifications onto the new connection via _requestToStreamMapping keyed on raw request id (the observed INC-9 duplicate). Their own client masks it by also sending notifications/cancelled, so SDK-to-SDK testing never sees it. This is a genuine bug under any revision: their vendored draft also treats stream close as terminating the subscription.


> On Streamable HTTP, the client aborts the request stream ... In both cases, the server MUST stop delivering events and release any associated resources (sketch §Push-Based Delivery, Cancellation); 'the abort is the terminal signal'


**Action for us:** report-to-them — POST-stream cancel must abort the request's AbortController (and stateless streams should not be keyed by raw JSON-RPC id); nothing to fix on our side (our server tears down on abort).


### [their-bug] TS order of notifications/events/active vs replayed events

Confirmed in events.ts _openStream: 'for (const occ of replay.events) this._deliverToPush(stream, occ);' precedes 'this._sendActiveNotification(stream, stream.sub.cursor, replay.truncated)' (events.ts:1214-1217). This contradicts both the sketch's sequence and their own vendored schema JSDoc, so it cannot be revision-skew. Effect: clients that gate event processing on receiving active drop or buffer the replayed backlog, and the active cursor is the post-replay head rather than the position at subscribe.


> Their own schemas.ts:2520-2521: 'Confirms a subscription on an events/stream request is active. Sent once before any events'; sketch sequence diagram sends active (step 2) before the event loop.


**Action for us:** report-to-them — send active (with the resume/truncation status) before replay delivery; our harness already tolerates either order, so no change for us.


### [their-bug] TS bootstrap cursor:null returned for replayable event types (poll + webhook subscribe)

Confirmed in events.ts _handlePoll: newCursor = occurrences.at(-1)?.cursor ?? (truncated ? ... : wireCursor) — on a quiet bootstrap (no occurrences, not truncated) this falls back to wireCursor, which is the client's null, so the response is {cursor:null} for an event type that mints real cursors. The same fallthrough affects fresh webhook subscribes. Their own schema doc reserves cursor:null for 'does not support replay', so this conflates 'no position yet' with 'no replay' under their own revision too — not skew. A client cannot persist a resume position until the first event; a crash in that window loses events.


> In the request, null means 'start from now' — the server returns no events and provides a fresh cursor for subsequent polls ... an event type that ever returns a non-null cursor SHOULD always do so (sketch §EventOccurrence notes / §Cursor Lifecycle)


**Action for us:** report-to-them — bootstrap should return the current log head (event.log head or a minted position) instead of echoing null; ours already returns a fresh cursor.


### [their-bug] TS cursor:null poll semantics with an existing server-side lease

Confirmed in events.ts:1072-1074: 'if (wireCursor === null) { replayFromSeq = lease.lastSeenSeq; }' — with a live lease (keyed principal+name+params), cursor:null resumes from the lease position instead of bootstrapping from now, which is exactly the wire-semantics use of lease state the sketch carves out as not allowed (leases are for lifecycle hooks only). Observable: run7 received events 46-50 on a null-cursor poll; two clients sharing a principal/params entangle (one's bootstrap drains the other's backlog), and first vs subsequent null polls behave differently. Their own vendored revision carries the same self-contained-poll language (their code cites it), so not skew.


> Each poll request is self-contained ... The server does not need to 'remember' previous poll requests to answer them. (The SDK MAY hold ephemeral derived state — a poll-lease table to drive on_subscribe/on_unsubscribe ... but neither is required to answer a poll) (sketch §Poll notes); 'null = start from now'


**Action for us:** report-to-them — cursor:null must always bootstrap ('start from now') regardless of lease state; lease may only drive on_subscribe/on_unsubscribe.


### [their-bug] Per-occurrence cursor field in poll results (dup: ts-agent, mcpkit-agent, wire-diff)

Confirmed: TS builds poll occurrences via the same path that sets cursor on every occurrence (the poll handler reads occurrences.at(-1)?.cursor, and run8 raw output shows per-event cursors), directly contradicting its own vendored schema comment — so their-bug for TS, not skew. mcpkit's Event has Cursor with no omitempty (events.go:37, 'intentionally no omitempty'), reusing one envelope across all three modes. Benign in practice: it is additive data, our decoder tolerates it via double_option (events.rs:75-82), and it even gives poll clients intermediate resume points. Reported by all three agents — one verdict.


> cursor ... Subscription position after this event (push/webhook only; poll carries cursor at the response level) (sketch §EventOccurrence schema); TS's own schemas.ts:2167-2168 says the same.


**Action for us:** report-to-them — both SDKs deviate from the field's documented scoping; also suggest the SEP either bless or forbid per-occurrence cursors in poll results (blessing is arguably useful).


### [their-bug] mcpkit missing events capability in initialize (dup: mcpkit-agent, capability half of wire-diff list_changed entry)

Confirmed: no capability-registration code exists anywhere in mcpkit's events package (the only 'capability' grep hit is an unrelated comment in stream.go), and the captured kitchen-sink initialize result has no events key. The wire-diff agent suggested revision-skew, but mcpkit's spec line citations (L139-149 poll shape, L361 auth, L425-460 deliveryStatus, etc.) align structurally with our copy, where §Capability Declaration sits at the top — every revision had it. Their registry.go comment ('the spec-shaped notifications/events/list_changed ... does not exist in the events SEP today', justifying the events.topology meta-source) excuses only the notification, not the capability key. Effect: any capability-gated client never calls events/* against mcpkit servers.


> Servers advertise event support in their capabilities: {"capabilities":{"events":{...}}} (sketch §Capability Declaration)


**Action for us:** report-to-them — events.Register must inject capabilities.events; also flag the SDK-reserved 'events.topology' name and absent list_changed for the SEP's reserved-prefix discussion.


### [their-bug] Webhook HMAC key derivation: raw whsec_ string vs decoded bytes (dup: mcpkit-agent, wire-diff)

Confirmed in their code: signStandardWebhooks does hmac.New(sha256.New, []byte(secret)) (headers.go:144) and VerifyStandardWebhooksSignature mirrors it (headers.go:217), with the stored Secret documented as the full 'whsec_...' string (webhook.go:269) and passed verbatim at delivery (webhook.go:1167, control.go:201). Ours (webhook.rs parse_whsec + sign) and TS (eventWebhook.ts computeWebhookSignature -> decodeWebhookSecret) both key on the decoded bytes. Even if mcpkit's snapshot predates the sketch's explicit decoded-bytes sentence, their header-mode comment shows they adopted Standard Webhooks deliberately, and Standard Webhooks itself defines the key as the decoded portion — so their-bug, not skew. This single bug 401s every cross-implementation webhook delivery in both directions (proven by both agents' offline recomputation).


> secret is the base64-decoded bytes of the value after the whsec_ prefix ... Off-the-shelf Standard Webhooks verifiers (e.g., Svix libraries) work without modification (sketch §Signature scheme)


**Action for us:** report-to-them — highest-priority mcpkit fix: decode the base64 after whsec_ before keying HMAC in headers.go (sign and verify, Go and Python); no change for us.


### [their-bug] Poll result key presence: events/truncated omitted by Go omitempty (dup: mcpkit-agent, wire-diff)

Confirmed: pollResultWire has Events []Event json:"events,omitempty" and Truncated bool json:"truncated,omitempty" (events.go:500-505), so the most common response — nothing happened — omits the events key entirely; even their own Python client defends with result.get('events', []), showing the omission is a Go-marshalling artifact rather than a deliberate wire shape of their revision (their revision's poll example also showed events:[], per their L139-149 citation). Our side has a real robustness gap stacked on top: PollEventsResult declares events/truncated/hasMore/nextPollMs with no serde defaults (events.rs:104-111), so we hard-fail with 'missing field events' instead of degrading.


> Empty events array means nothing happened — this is the common case and should be cheap (sketch §Poll notes); response example always carries {events, cursor, truncated, hasMore, nextPollMs}


**Action for us:** fix-our-code — add serde defaults (events=[], truncated=false, and tolerate missing hasMore/nextPollMs) in PollEventsResult; also report the omitempty artifact to mcpkit since it breaks any strict decoder.


### [their-bug] mcpkit events/stream ignores resume cursor: no backlog replay, no truncated signal (replay half of wire-diff stream entry)

Confirmed: registerStream resolves the initial cursor (non-null client cursor 'passes through unchanged', stream.go:174-184) then subscribes only to the source's live channel; the cursor is echoed in active but never used to replay, and MaxAge is parsed but unused. Their own sources support replay (poll mode replays from cursor), so this is not a capability limit; a reconnecting push client silently loses every event between its cursor and 'now', without even truncated:true — the one signal the sketch (any revision: their poll code implements the same truncated contract) requires when delivery starts later than the supplied cursor.


> Reconnection after failure: ... the client sends a new events/stream with the same subscription and its last-known cursor (sketch §Push-Based Delivery, Lifecycle) — i.e., the server resumes from there.


**Action for us:** report-to-them — events/stream must replay from the supplied cursor (or send truncated:true on the active frame when it cannot).


### [their-bug] mcpkit events/subscribe response missing truncated and staleness check (dup: mcpkit-agent, wire-diff)

Confirmed: respBody = {id, cursor, refreshBefore} + conditional deliveryStatus (events.go:913-926), no truncated key and no backlog/staleness evaluation on subscribe. The mcpkit agent suggested revision-skew, but their adjacent line citations (§Subscription Identity L361-390, §Webhook Delivery Status L425-460 — whose examples include truncated:false) match our copy's structure, and they implement the gap control envelope whose whole purpose complements subscribe-time truncated — their revision had the field. Stacked failure mode on our side: SubscribeResult requires truncated (events.rs:222), so our harness errored after their server had already created the subscription, leaving a live 1h subscription delivering to a receiver that rejects everything.


> Response: {id, refreshBefore, cursor, truncated} — '"truncated": false // true if delivery started later than the supplied cursor' (sketch §Subscribing: events/subscribe)


**Action for us:** report-to-them — restore truncated (and a stale-cursor check) on subscribe; additionally default truncated=false in our SubscribeResult decode so a missing field cannot strand a half-created subscription.


### [their-bug] Control envelope shapes: mcpkit terminated drops error.data, gap cursor cannot be null (wire-diff)

Confirmed: mcpkit's ControlError is {code, message} only (control.go:34-37) and controlEnvelope.Cursor is string omitempty (control.go:44-47), so a terminated envelope cannot carry the sketch's data.reason payload (which our and TS envelopes emit) and a gap envelope cannot express cursor:null — notable since mcpkit itself ships a cursorless feature. Their own stream-side error shape has the same {code,message}-only payload. Lossy but parseable for lenient receivers; TS's envelope schemas match ours exactly (schemas.ts:2598-2620, verified gap nullable + error.data).


> {"type":"terminated","error":{"code":...,"message":...,"data":...}} (sketch §Non-event webhook bodies); the Authorization example carries data.reason.


**Action for us:** report-to-them — add the optional data field to ControlError and make gap cursor nullable; no change for us.


### [their-bug] Unsubscribe of nonexistent subscription: mcpkit silent success (wire-diff; TS -32016 half folded into error-registry verdict)

Confirmed: registerUnsubscribe calls webhooks.Unregister(canonical) — documented and implemented as a found-or-not no-op (webhook.go:750-757) — and returns {} unconditionally (events.go:993-995). Their own errors.go documents the NotFound kind="subscription" discriminator for precisely this case, so it is a bug under their own tracked revision, not skew. Silent success hides client-side key-derivation/canonicalization bugs the error exists to surface. (TS's -32016 for the same case is pre-consolidation skew, covered by the error-registry verdict.) Ours returns -32011 kind=subscription (webhook/handlers.rs:168-178) — verified.


> -32011 NotFound — ... no subscription matching the key on events/unsubscribe (sketch §Error Codes); mcpkit's own errors.go:50-51: '"subscription" for an unsubscribe target that doesn't exist'


**Action for us:** report-to-them — mcpkit unsubscribe should return -32011 with data.kind='subscription' when no key matches.


### [their-bug] mcpkit kitchen-sink --addr flag unusable (mcpkit-agent)

Confirmed: kitchen-sink/main.go passes demokit.ValueFlag("--addr") to demokit.FilterArgs, and FilterArgs (demokit@v0.0.29/args.go) adds extras to its strip set and excludes them from the returned slice — so --addr never reaches flag.CommandLine.Parse and the server always binds the default (the interop agent had to patch the default to 127.0.0.1:8294, comment still visible at main.go:58). The ExtraFlag mechanism strips flags; it does not pass them through, contradicting the repo's own CLAUDE.md note.


> (n/a — example tooling, not spec surface)


**Action for us:** report-to-them — kitchen-sink should not list --addr (or other flags it wants parsed) as FilterArgs extras; cosmetic but blocks out-of-the-box interop testing.


### [their-bug] mcpkit EventDef advertised with delivery: null (events.topology meta-source) (mcpkit-agent)

Confirmed: the self-registered topology source is constructed as EventDef{Name, Description} with no Delivery (registry.go:120-123), and EventDef.Delivery is []string json:"delivery" without omitempty (events.go:70), so the captured events/list (/tmp/interop-kit/their-events-list.json) shows "delivery": null — violating the non-empty-subset rule under any revision. One malformed auto-injected def poisons the whole list for strict clients; our all-or-nothing ListEventsResult decode (events.rs Vec<EventDefinition> with required delivery) amplifies it.


> delivery lists the delivery modes this event type supports — any non-empty subset of "poll", "push", "webhook" (sketch §Listing notes)


**Action for us:** report-to-them — the auto-registered topology source must declare its real delivery modes; we may also decode events/list per-entry-tolerantly so one bad def doesn't void the list.


### [their-bug] EventDef field set: inputSchema missing, cursorless added (mcpkit); title added (TS) (dup: mcpkit-agent, wire-diff)

Confirmed: mcpkit's EventDef has Name/Description/Delivery/PayloadSchema/Cursorless/Meta and hook funcs — no inputSchema anywhere (events.go:67-89), so clients cannot discover valid subscription params for Match/Transform-driven sources; and it advertises a nonstandard cursorless flag in events/list, duplicating the spec's per-delivery cursor:null signal as out-of-spec list surface. inputSchema has been core to the sketch since the 2026-02-19 Summary, so revision-skew does not explain its absence — this is an SDK design gap. TS's additive title (BaseMetadata, matching base-MCP conventions) and always-emitting a default inputSchema are harmless and not divergences.


> inputSchema is a JSON Schema describing valid subscription parameters ... This mirrors the inputSchema on tools for consistency (sketch §Listing notes; also in the Summary since the first draft: 'each with a name, an inputSchema for subscription params, a payloadSchema')


**Action for us:** report-to-them — mcpkit: add inputSchema to EventDef; suggest the SEP weigh in on a subscribe-time cursorless/replay capability flag since mcpkit clearly wanted one (worth an Open Question).


### [spec-gap] events/stream final result frame: {} vs {"_meta":{}} (dup: ts-agent, wire-diff)

Confirmed: TS _handleStream resolves {} mapped to EmptyResultSchema (events.ts:1164-1167); ours and mcpkit always emit {"_meta":{}} (mcpkit StreamEventsResult.Meta deliberately has no omitempty 'so the wire shape matches the spec example exactly', stream.go:42-46). The frame is defined to carry no information and _meta is optional on every MCP result type, so calling the TS shape a bug overreads a parenthetical example; but the fact that one implementer pinned the example exactly and another used {} shows the text is ambiguous. Static finding — no run ever exercised a server-initiated close.


> The StreamEventsResult is an empty typed result ({"_meta": {}}). It carries no information (sketch §Push-Based Delivery, Lifecycle)


**Action for us:** add-to-spec-gaps — state that {} and {"_meta":{}} are equivalent empty results (or pin one normatively).


### [spec-gap] events capability advertisement: listChanged semantics (ts-agent)

Confirmed: TS registers events:{listChanged: existing ?? true} unconditionally (events.ts:881-885) even for servers that never change their list; our mock advertises listChanged:false (handlers/initialize.rs:20-22); their client gates only on presence of the events key. Interop worked both ways (run1). The sketch genuinely never says whether events:{} is valid, what listChanged defaults to when absent, or that false is permitted — both implementations guessed compatibly but a third might not.


> Servers advertise event support in their capabilities: {"capabilities":{"events":{"listChanged":true}}} (sketch §Capability Declaration — sole example, no prose on false/absent/default)


**Action for us:** add-to-spec-gaps — define listChanged default/absence and whether bare events:{} is a valid capability.


### [spec-gap] whsec_ secret base64 alphabet/padding (dup: mcpkit-agent, wire-diff)

Confirmed: mcpkit generates whsec_ + base64.RawURLEncoding (URL-safe, unpadded; secret.go:44) and deliberately accepts both alphabets on input ('the SDKs don't agree on which they emit', secret.go:57-58); our parse_whsec requires the standard alphabet with padding (webhook.rs:4,30-41) and rejects their auto-generated secrets with -32602 before delivery is ever attempted; TS's regex accepts standard alphabet with optional padding (eventWebhook.ts:60). All three are defensible readings of 'base64'. Standard Webhooks examples use standard base64, which argues for pinning that, but the sketch text does not say so — every implementation guessed.


> the literal prefix whsec_ followed by base64 of 24–64 random bytes. Servers MUST reject values that do not satisfy this with InvalidParams (sketch §events/subscribe notes) — alphabet and padding never specified.


**Action for us:** add-to-spec-gaps — pin the alphabet/padding (recommend standard base64, accept-both-on-input as a SHOULD); consider relaxing parse_whsec to accept URL-safe/unpadded input as interop hardening.


### [spec-gap] events/poll request cursor: absent vs null (wire-diff)

Confirmed: TS requires the key (cursor: z.string().nullable(), not .optional(), schemas.ts:2229) so an omitting client gets InvalidParams; we and mcpkit treat absent ≡ null (events.rs:92-95 serde default; mcpkit *string). All examples in the sketch show the key present but no prose addresses omission. We always serialize the key (events.rs comment 'Always serialized'), so ours->TS works; this only bites third-party lenient clients.


> Poll request example: '"cursor": null // null = start from now' (sketch §events/poll) — never states whether the key may be omitted.


**Action for us:** add-to-spec-gaps — state whether absent ≡ null on poll/stream/subscribe cursor (recommend yes, matching JSON-RPC conventions).


### [spec-gap] events/subscribe refresh with a cursor ahead of/unrelated to the live position (wire-diff)

Confirmed both readings in code: our refresh treats any supplied cursor as a no-op and returns the stored watermark (webhook/handlers.rs:129-141 'Live refresh: the supplied cursor is a no-op'); TS documents and implements 'on refresh ... a non-null value replaces it' with _replayAfterCursor backlog re-delivery (schemas.ts:2362-2365, events.ts:1332-1344). The sketch's conditional only covers at-or-behind cursors, so both behaviors are defensible; observable difference is backlog re-POSTs from TS vs nothing from us.


> If the subscription is live and the cursor is at or behind the current in-flight position, this is a no-op (sketch §Subscription Identity field table) — silent on a cursor ahead of, or unrelated to, the in-flight position.


**Action for us:** add-to-spec-gaps — define refresh semantics for a cursor ahead of/unrelated to the in-flight position (replace-and-replay vs ignore).


### [spec-gap] deliveryStatus field null-vs-absent presence rules (wire-diff)

Confirmed: we always serialize lastDeliveryAt/lastError (null when none) and omit failedSince when absent (events.rs:203-213); TS is close to us; mcpkit omits all empty fields and the whole object when there is nothing to report (events.go:930-960 'Omitted on first subscribe — wire bloat'). All three agree on the load-bearing part (omitted on first subscribe, present on refresh) and the differences are absorbed by optional-field decoding. Minor.


> Healthy-refresh example shows '"lastError": null' present (sketch §Webhook Delivery Status); the prose never states presence rules for lastDeliveryAt/lastError/failedSince or for the object itself.


**Action for us:** add-to-spec-gaps — one line stating null and absent are equivalent for deliveryStatus members (low priority).


### [spec-gap] Error message wording for shared codes: literal 'NotFound' vs descriptive text (mcpkit-agent)

Confirmed: codes and typed data payloads agree between us and mcpkit (both -32011 + {kind}), but mcpkit sends the literal table token ('NotFound', 'Forbidden') as the message while we send descriptive strings ('unknown event type "nope"'). The sketch's own design intent ('clients still distinguish every case they branch on (by code, by method, or by a typed data discriminator)') implies messages are non-normative, but it never says so, and a message-matching client breaks one way or the other.


> Error table: | -32011 | NotFound | A referenced entity does not exist... (sketch §Error Codes) — the table never says whether the Message column is normative literal wire text.


**Action for us:** add-to-spec-gaps — state that the Message column is a label, messages are non-normative free text, and clients MUST branch on code/data only.


### [revision-skew] Webhook endpoint verification handshake (dup: ts-agent, mcpkit-agent, wire-diff)

Verified three ways: our copy's Open Questions section (5 questions) no longer lists verification — it is a normative MUST in §Webhook Security, and our crates/mcp-events-server/src/webhook/handlers.rs:104-115 implements it with -32015 challenge_failed. TS schemas.ts:2616 annotates the verification envelope '(Pending — see spec Open Question 6.)' and _handleSubscribe activates delivery immediately with no verification POST; the TS client's deliverWebhookPayload explicitly ignores verification ('Receiver-side challenge response is the gateway's concern', client/events.ts:337). mcpkit has no verification envelope at all (control.go knows only gap/terminated). Both peers track drafts where this was unsettled; ours tracks the 2026-06-11 revision where it is settled. Reported by all three agents — one verdict.


> a server MUST NOT begin delivering to a callback URL until the endpoint's intent to receive deliveries is confirmed ... the endpoint proves intent by echoing the nonce in a 2xx body ({"challenge":"<nonce>"}) (sketch §Webhook Security, Endpoint verification)


**Action for us:** add-to-spec-gaps — SEP must flag that settling Open Question 6 breaks strict-server -> old-receiver interop at the first events/subscribe (receivers need a ~5-line challenge echo); also note to both SDKs that they need the handshake on rebase.


### [revision-skew] Events error-code registry: -32011..-32017 vs consolidated five codes (dup: ts-agent, wire-diff error-surface, wire-diff -32013 discriminator)

Verified: TS constants.ts:26-42 defines the pre-consolidation seven-code set (EVENT_NOT_FOUND -32011 ... SUBSCRIPTION_NOT_FOUND -32016, DELIVERY_MODE_UNSUPPORTED -32017, deprecated CURSOR_EXPIRED -32014) with message-only ProtocolErrors and no typed data. mcpkit errors.go implements the consolidated set identical to ours (jsonrpc.rs:19-23) including data.kind/data.limit/data.max conventions, and its own comment dates the consolidation, proving TS simply predates it. Practical breaks (unsubscribe-miss -32016 vs -32011+kind, delivery-mode -32017 vs -32014) disappear when TS rebases. mcpkit's extra use of -32013 for on_subscribe rejection (data.max absent) is within the sketch's letter ('a server-imposed limit') — not a divergence.


> mcpkit errors.go:12-14: 'The MCP Events spec consolidated its error surface on 2026-05-22 (design-sketch commit 567be29): seven events-specific codes collapsed into five reusable codes carrying typed data discriminators.'


**Action for us:** report-to-them — TS SDK should rebase constants.ts/enums.ts onto the 2026-05-22 consolidated codes and attach typed data payloads; no change for us or mcpkit.


### [revision-skew] Webhook TTL negotiation: ttlMs request field and nullable refreshBefore (dup: ts-agent, wire-diff, ttlMs half of mcpkit time-unit entry)

Verified: TS SubscribeEventRequestParamsSchema (schemas.ts:2348-2372) has no ttlMs and refreshBefore is non-nullable z.string() (schemas.ts:2433); the server grants a fixed config TTL (events.ts:1298,1407). mcpkit likewise has no ttlMs (events.go subscribe struct) and pins TTL to a server-configured [5min,24h] envelope citing 'WG guidance (Peter, 2026-06-05 in #triggers-events-wg)' (webhook.go:48-73), with refreshBefore always RFC3339 (events.go:915). Our §Subscription TTL negotiation (2026-06-11 copy) postdates both snapshots. Wire-safe today via the clamping-is-self-announcing rule, but two forward-compat hazards on their side: zod/Go silently strip ttlMs (a no-expiry request gets a 60s lease with zero signal), and TS's client does new Date(result.refreshBefore) which yields NaN timers on a spec-conformant refreshBefore:null.


> ttlMs (request, optional, nullable integer) is the client's suggested lifetime ... refreshBefore (response) is the grant. It SHOULD be less than or equal to the suggestion ... refreshBefore: null grants no expiry (sketch §Subscription TTL)


**Action for us:** report-to-them — both SDKs: add ttlMs on rebase and make refreshBefore null-tolerant (TS new Date(null-grant) = NaN timers; mcpkit Subscription helper errors on null); our implementation already conforms.


### [revision-skew] maxAge (seconds) vs maxAgeMs, nextPollSeconds vs nextPollMs (dup: mcpkit-agent time-unit entry, wire-diff)

Confirmed: mcpkit parses MaxAge int json:"maxAge" in seconds on poll/subscribe/stream and returns NextPollSeconds hardcoded 5 (events.go:504,703, pinned in wire_shape_test.go); their code and README cite their spec revision for both names, and TS matches our ms spellings — so the rename/unit change happened between their snapshot and ours. The danger is the failure mode, not the blame: unknown-key dropping makes our maxAgeMs silently inert against them, and if a client ever sent maxAge to a *Ms-era server (or vice versa with the same name), 300000 would be read as ~3.5 days vs 5 minutes with no error.


> mcpkit events.go:537-538/757: 'MaxAge per spec §Cursor Lifecycle → Bounding replay with maxAge L529' (seconds) — their vendored revision named the field maxAge; our copy: 'maxAgeMs (integer milliseconds)' and poll response 'nextPollMs: 30000'.


**Action for us:** report-to-them — rebase to maxAgeMs/nextPollMs; also worth a SEP note that unit-bearing field renames across draft revisions fail silently (suggest the SEP changelog call out maxAge->maxAgeMs explicitly).


### [revision-skew] Stream notification correlation: top-level requestId vs _meta subscriptionId (dup: mcpkit-agent, wire-diff)

Confirmed: mcpkit's activeNotifParams/eventNotifParams/heartbeatNotifParams/errorNotifParams all carry top-level RequestID and no _meta correlation (stream.go:72-115), and their comments quote their vendored spec text using requestId — their revision predates the SEP-2575 _meta convention our copy and TS both implement (consts.rs:31, TS constants SUBSCRIPTION_ID_META_KEY confirmed in events.ts heartbeat params). Tolerable on per-POST Streamable HTTP SSE (no demux needed) but breaks stdio multi-stream routing cross-implementation.


> Every notifications/events/* message carries the JSON-RPC id of the parent events/stream request in params._meta["io.modelcontextprotocol/subscriptionId"] (per SEP-2575's correlation convention) (sketch §Push-Based Delivery); mcpkit stream.go:295: 'Spec L285: the server sends a fresh notifications/events/active {requestId, cursor:<fresh>, truncated:true}'


**Action for us:** report-to-them — rebase to the _meta[io.modelcontextprotocol/subscriptionId] convention; no change for us (TS already matches us).


### [revision-skew] active/heartbeat params field presence: truncated omitempty/_meta absent vs our hard-required fields (dup: mcpkit-agent, presence half of wire-diff stream entry)

Confirmed both sides: mcpkit's active is {requestId, cursor, truncated omitempty} and heartbeat {requestId, cursor} (stream.go:72-97), with their comment explicitly matching their vendored example; our EventsActiveParams requires truncated and _meta and EventsHeartbeatParams requires _meta with no defaults (events.rs:130-163), so their frames fail decode and our harness silently loses cursor advancement from active/heartbeat (events still flow because EventOccurrence decode is lenient). The shape difference is skew (resolves on rebase along with requestId->_meta), but our hard-required decode is a self-inflicted robustness gap worth fixing regardless.


> mcpkit stream.go:71-72: 'Truncated is omitted when false to match the spec example payload' — their revision's active example omitted it; our copy's example: {"cursor":"historyId_99840","truncated":false,"_meta":{...}}


**Action for us:** fix-our-code — default truncated=false and make _meta optional when decoding notifications in the harness (keep always-serializing on the server side).


### [revision-skew] Non-https / policy-rejected callback URL error code: -32015 vs -32602 (wire-diff; -32015 meaning note in ts-agent error entry)

Confirmed: TS throws INVALID_CALLBACK_URL (-32015) for all isSafeWebhookUrl failures (events.ts:1310-1313) — fully coherent under its pre-consolidation revision where -32015 meant exactly that, so skew that resolves on rebase. Ours uses -32602 for static URL validation (webhook/handlers.rs parse_callback_url) and reserves -32015+data.reason for verification/reachability. The mcpkit variant is worse than skew: it tracks the consolidated revision yet returns -32015 with data.reason='connection_refused' for scheme/loopback/parse failures, with a comment admitting 'none map cleanly onto the runtime DeliveryErrorBucket categories' (events.go:775-783) — a knowing miscode of a static-validation failure that its own revision's table assigns to InvalidParams.


> -32602 InvalidParams — ... the callback delivery.url is malformed or non-https (sketch §Error Codes); TS constants.ts:37-38 defines -32015 as INVALID_CALLBACK_URL under its older revision.


**Action for us:** report-to-them — mcpkit should return -32602 for static URL validation (their consolidated revision already says so); TS resolves with the error-registry rebase.


### [not-a-divergence] Session / lifecycle enforcement: Mcp-Session-Id and notifications/initialized strictness (mcpkit-agent)

Confirmed both sides: our server issues but does not enforce the session id, explicitly documented as a prototype shortcut (handlers/initialize.rs:32-34 comment; CONFORMANCE-NOTES.md item 3), and our harness client performs the full handshake including notifications/initialized (client.rs:110-167) — so no actual interop failure exists between conformant parties. mcpkit's hard enforcement is permitted (a server MAY require the session header and reject pre-initialization requests); our leniency is also permitted. The only victim is a client that itself skips the mandatory initialized notification, which is that client's nonconformance. Not an events-spec matter at all.


> (Events sketch is silent — base-protocol territory; base Streamable HTTP makes session-id enforcement a server option, and requiring initialization before serving requests is spec-faithful.)


**Action for us:** none — already documented as a prototype scope cut; optionally enforce sessions if this prototype graduates.


### [not-a-divergence] Webhook delivery headers: casing, Host pinning, mcpkit legacy MCPHeaders mode (wire-diff)

Confirmed: all three default modes emit the same four headers (header-name casing is HTTP-insensitive; TS's SSRF Host pinning is sketch-sanctioned). mcpkit's MCPHeaders mode (X-MCP-Signature/X-MCP-Timestamp, different HMAC base) is wholly off-spec but is opt-in, non-default, and self-documented as the pre-WG-alignment legacy shape (headers.go:15-37, citing WG PR#1 comment r3167245184 for the Standard Webhooks default) — no on-by-default wire divergence exists.


> Required headers: webhook-id, webhook-timestamp, webhook-signature, and X-MCP-Subscription-Id are present on every delivery (sketch §Delivery profile)


**Action for us:** none — optionally suggest mcpkit deprecate MCPHeaders so nobody enables it expecting interop; no spec or code action.

