# drasi-mcp-events

An **independent, clean-room Rust implementation** of the draft [MCP **Events**
extension](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/pull/1)
(MCP Triggers & Events Working Group), bridged to [Drasi](https://drasi.io) — a CNCF
continuous-query engine — as the first *real, durable, replayable* upstream for the proposed
primitive.

> **Status: prototype against a draft spec.** Built to stress-test the design sketch and
> produce Working-Group feedback, not for production. The spec WILL change; this repo tracks
> the sketch revision vendored at [`docs/design-sketch-proposal.md`](docs/design-sketch-proposal.md)
> (2026-06-11).

## Why

The Triggers & Events WG is gathering prototype implementations that stress-test the Events
design sketch. Existing prototypes (TypeScript, Go) wrap simulated SaaS feeds. This one is:

- **Independent** — implemented from the sketch text alone; no other Events implementation
  was consulted. Every divergence from sibling implementations is therefore evidence about
  the *spec*, not shared code. The full ambiguity log is the headline deliverable:
  **[SPEC-GAPS.md](SPEC-GAPS.md)** — 58 findings from the build plus 7 confirmed by
  cross-implementation interop, and **[docs/INTEROP.md](docs/INTEROP.md)** — bidirectional
  interop vs the TypeScript SDK branch and mcpkit (Go).
- **Backed by a real change engine** — [Drasi](https://github.com/drasi-project) continuous
  queries turn database WAL streams into semantic Added/Updated/Deleted diffs over a standing
  query's result set (a SQL `UPDATE` that drops a row below a query threshold arrives as a
  semantic *delete*). That exercises delta payloads, cursor replay, and subscription
  provisioning in ways synthetic feeds cannot.

## What's here

```
Postgres ──WAL──▶ Drasi Server ──SSE reaction──▶ drasi-feed ──▶ event buffer ──▶ MCP Events server
                  (drasi/ docker env)                            (epoch:seq cursors)   POST /mcp
                                                                                        │
                                              events/list · events/poll · events/stream (SSE)
                                              events/subscribe · events/unsubscribe (webhooks,
                                              Standard-Webhooks signed, challenge-verified, TTL)
```

| Crate | What it is |
|---|---|
| `mcp-events-wire` | Wire types: JSON-RPC 2.0 + MCP base subset + the full Events extension, Standard Webhooks sign/verify |
| `mcp-events-engine` | Per-event-type ring buffer with the sketch's full cursor lifecycle (`truncated`, `maxAgeMs`, `hasMore`), webhook subscription store (compound identity, TTL, quotas, suspension, verification cache) |
| `drasi-feed` | Drasi SSE-reaction consumer (format: [`drasi/SSE-FORMAT.md`](drasi/SSE-FORMAT.md), live-verified) + deterministic mock feed |
| `mcp-events-server` | Axum server: `POST /mcp` dispatcher, all five `events/*` methods, push streams with heartbeats, webhook delivery worker (SSRF-guarded, watermark cursors, gap envelopes) |
| `mcp-events-client` | Client library + `events-harness` CLI: `list` / `poll` / `stream` / `subscribe` / `unsubscribe` / `webhook-recv`, with cursor persistence and `eventId` dedup |

**Verification:** 227 tests across the workspace; scripted e2e covering poll cursor
persistence/resume, push streams (active/event/heartbeat frames), and the full webhook loop
(challenge → signed deliveries → unsubscribe). Two adversarial spec-conformance reviews found
no wire-visible nonconformance; their SHOULD-level notes are in
[`docs/CONFORMANCE-NOTES.md`](docs/CONFORMANCE-NOTES.md).

## Quickstart (mock feed — no Drasi needed)

```bash
cargo build --workspace

# Terminal 1: server with a synthetic orders feed
cargo run -p mcp-events-server -- --config crates/mcp-events-server/examples/mock.yaml

# Terminal 2: discover and consume
cargo run -p mcp-events-client --bin events-harness -- list
cargo run -p mcp-events-client --bin events-harness -- poll   --name high-value-orders.changed --follow --state-file /tmp/cursor.json
cargo run -p mcp-events-client --bin events-harness -- stream --name high-value-orders.changed

# Webhook mode (requires the bearer principal from mock.yaml)
cargo run -p mcp-events-client --bin events-harness -- webhook-recv --port 8099 --secret <whsec_…>
cargo run -p mcp-events-client --bin events-harness -- subscribe --name high-value-orders.changed \
    --url http://127.0.0.1:8099/hook --secret <whsec_…> --bearer devtoken
```

Kill the harness, re-run `poll` with the same `--state-file`: it resumes from the persisted
cursor. Restart the *server* instead: the next poll gets `truncated: true` and a fresh cursor
(process-scoped cursors, exactly the sketch's emit-only model).

## Live Drasi mode

```bash
cd drasi
docker compose run --rm plugin-install   # one-time: installs source/postgres, bootstrap/postgres, reaction/sse
docker compose up -d                     # postgres:16 + drasi-server 0.1.6
cd ..
cargo run -p mcp-events-server -- --config crates/mcp-events-server/examples/drasi.yaml
```

Then trigger real changes (see [`drasi/RUNBOOK.md`](drasi/RUNBOOK.md)):

```bash
docker exec drasi-demo-postgres psql -U demo -d demo \
  -c "INSERT INTO orders (customer, total, status) VALUES ('erin', 5000, 'open');"
```

…and watch them arrive as MCP events in the harness — `added` on insert, `updated` (with
before/after) on in-set updates, `deleted` when an update crosses below the query threshold.

## Deliverables for the Working Group

1. **[SPEC-GAPS.md](SPEC-GAPS.md)** — 58 deduplicated, severity-rated ambiguity findings from
   the clean-room build, each with the assumption this implementation made.
2. **A third independent implementation** (after the TypeScript SDK branch and mcpkit/Go),
   with a full bidirectional interop report ([docs/INTEROP.md](docs/INTEROP.md)) whose headline
   is that the three implementations track three different sketch revisions — concrete evidence
   for pinning the spec.
3. **The Drasi bridge** as evidence for the sketch's open questions: delta-shaped payloads
   (`{before, after}`), durable-upstream cursor replay, multi-event-type feeds (open question
   4), and result-set-as-resource vs. diffs-as-events (open question 2).

## Known prototype shortcuts

Streamable HTTP only (no stdio); sessions issued but not enforced; in-memory state throughout
(by design — the sketch's short-TTL soft-state model); `allowInsecureUrls` config flag exists
solely for loopback webhook e2e and is nonconformant with the sketch's TLS MUST; JSON Schema
`inputSchema` params are advertised but only shallowly validated. See
[`docs/CONFORMANCE-NOTES.md`](docs/CONFORMANCE-NOTES.md) for the full honest list.

## Provenance & license

Implementation: Apache-2.0. `docs/design-sketch-proposal.md` is a verbatim vendored copy of
the WG design sketch (author: Peter Alexander, Anthropic) from PR #1 of
`modelcontextprotocol/experimental-ext-triggers-events`, included for reference. This is a
community prototype by a Drasi maintainer; it is not an official artifact of the MCP project,
Anthropic, Microsoft, or the Drasi project.
