# drasi-mcp-events

**A working, end-to-end demo of the draft [MCP Events
extension](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/pull/1)**
(MCP Triggers & Events Working Group): an agent that is given a *task*, discovers the event
streams a server offers, **chooses its own subscription**, sleeps until the world changes,
and reacts — verifying through MCP tools and acting through one. Events are powered by
[Drasi](https://drasi.io), a CNCF continuous-query engine, as a *real, durable, replayable*
upstream — plus an on-demand incident stream for scripted demos.

> **Start here: [DEMO.md](DEMO.md)** — the full scripted walkthrough, with a captured live
> transcript ([`docs/demo-transcript.txt`](docs/demo-transcript.txt)) in which a real model
> reads the catalog, picks `incidents.created` with `{"priority": "P1"}`, ignores a P2,
> escalates a P1, runs the verify-then-flag tool loop on a suspicious order, and replays an
> event that fired while it was dead.

The MCP core is served by the **official Rust SDK (rmcp 3.1.x)** on **MCP 2026-07-28**
(stateless lifecycle, `server/discover`); the extension is declared in the standard
`extensions` capability map as `io.modelcontextprotocol/events`. To a stock MCP client this
is an ordinary tools server; to an events-aware client, the same endpoint is a full Events
server — which is the extension-mechanism story in one process.

## What the demo shows

1. **Subscription is a reasoning step, not config.** `events/list` descriptions and typed
   `inputSchema`s are model-readable, exactly like tool schemas. Given
   `--task "You are on-call: watch for P1 incidents"`, the agent picks the stream *and* the
   filter arguments itself, with a stated rationale.
2. **Events are conditions, not writes.** A SQL `UPDATE` arrives as a semantic `deleted`
   when a row leaves the watched result set — and the `stuck-orders` stream
   (`drasi.trueFor`) fires with **no triggering write at all**: the continuous query
   notices time passing.
3. **Thin events, authoritative tools.** Payloads carry triage data; the agent verifies via
   `get_order` / `get_customer_history` and acts via `flag_order` — the events × tools
   composition the extension is designed around.
4. **Nothing is lost when the consumer dies.** Client-owned cursors + server-side retention
   give at-least-once replay from primitives the sketch already defines; `truncated: true`
   is the honest "you missed things, re-verify" signal.

## Architecture

```
Postgres ──WAL──▶ Drasi Server ──SSE reaction──▶ drasi-feed ──▶ event buffer ──▶ hybrid MCP server (POST /mcp)
                  (drasi/ docker env,                            (epoch:seq cursors)   │
                   2 continuous queries)  scripts/fire-incident.sh ──▶ POST /inject ───┤
                          ┌────────────────────────────────────────────────────────────┤
                          │ official Rust SDK (rmcp 3.1.x): server/discover, 2026-07-28│
                          │ stateless lifecycle, tools/* (get_order, get_customer_     │
                          │ history, flag_order), legacy initialize back-compat        │
                          │ draft Events extension (this repo): events/list · poll ·   │
                          │ stream (SSE) · subscribe/unsubscribe (Standard-Webhooks    │
                          │ signed, challenge-verified, TTL)                           │
                          └──────────────────────────────────────────────△─────────────┘
                                                                         │ wakes
                        drasi-agent (crates/mcp-events-agent): task ▶ discover ▶ choose ▶
                        subscribe ▶ background work ▶ react (LLM or deterministic policy)
```

| Crate | What it is |
|---|---|
| `mcp-events-wire` | Wire types: JSON-RPC 2.0 + MCP base subset + the full Events extension, Standard Webhooks sign/verify |
| `mcp-events-engine` | Per-event-type ring buffer with the sketch's full cursor lifecycle (`truncated`, `maxAgeMs`, `hasMore`), webhook subscription store (compound identity, TTL, quotas, suspension, verification cache) |
| `drasi-feed` | Drasi SSE-reaction consumer (format: [`drasi/SSE-FORMAT.md`](drasi/SSE-FORMAT.md), live-verified) + deterministic mock feed |
| `mcp-events-server` | Hybrid server: rmcp (official SDK) serves the MCP core + three Postgres-backed demo tools; an axum dispatcher in front serves the five `events/*` methods — push streams with heartbeats, webhook delivery worker (SSRF-guarded, watermark cursors, gap envelopes), demo `/inject` endpoint for on-demand streams |
| `mcp-events-client` | Client library + `events-harness` CLI: `discover` / `list` / `poll` / `stream` / `subscribe` / `unsubscribe` / `webhook-recv`, 2026-07-28 stateless lifecycle (SEP-2243 headers + SEP-2575 `_meta`), cursor persistence, `eventId` dedup |
| `mcp-events-agent` | `drasi-agent`: task-driven discovery + the events × tools loop. LLM brains for three wire dialects (Anthropic API / OpenAI chat completions / OpenAI Responses — see `.env.example`), or a deterministic policy so everything runs with no credentials |

**Verification:** 244 tests across the workspace, live end-to-end runs (including against an
Azure-hosted model), and three rounds of adversarial review with every confirmed finding
fixed. SHOULD-level conformance notes: [`docs/CONFORMANCE-NOTES.md`](docs/CONFORMANCE-NOTES.md).

## Quickstart

The full demo (Drasi + tools + task-driven agent) is scripted in **[DEMO.md](DEMO.md)**.
Shortest path:

```bash
cargo build --workspace
cd drasi && docker compose run --rm plugin-install && docker compose up -d && cd ..
cp .env.example .env    # optional: LLM credentials; without them a deterministic brain runs

# Terminal A
cargo run -p mcp-events-server -- --config crates/mcp-events-server/examples/drasi.yaml
# Terminal B
cargo run -p mcp-events-agent -- --task "You are on-call: watch for P1 incidents and escalate them"
# Terminal C
scripts/fire-incident.sh P1 checkout-service "Checkout returning 500s"
```

No Docker? `--config crates/mcp-events-server/examples/mock.yaml` serves a synthetic orders
feed, and the `events-harness` CLI exercises every delivery mode directly
(`discover` / `list` / `poll --state-file …` / `stream` / `subscribe` for signed webhooks —
see the flags in each subcommand's `--help`).

## Working-Group background

This repo began as an independent clean-room implementation of the design sketch (vendored
at [`docs/design-sketch-proposal.md`](docs/design-sketch-proposal.md)) built to stress-test
it. Artifacts from that phase remain for WG reference:

- [`docs/INTEROP.md`](docs/INTEROP.md) — bidirectional interop against the TypeScript SDK
  branch and mcpkit (Go); its headline (three implementations tracking three different
  sketch revisions) argued for pinning the spec.
- [`SPEC-GAPS.md`](SPEC-GAPS.md) — ambiguity findings logged during the original clean-room
  build. *Historical artifact: not independently verified; read critically.*
- The Drasi bridge doubles as evidence on the sketch's open questions: delta-shaped
  payloads, durable-upstream cursor replay, multi-event-type feeds (open question 4), and
  result-set-as-resource vs. diffs-as-events (open question 2).

The demo itself adds newer evidence: the extension runs cleanly on the 2026-07-28 stateless
core, declared through the standard extensions capability map, next to an official-SDK MCP
core — with the events methods dispatched in front of the SDK because no SDK ships
extension hooks yet (the gap an Extensions-Track SEP would close).

## Known limitations

Streamable HTTP only (no stdio); in-memory event buffer and subscription state by design
(the sketch's short-TTL soft-state model — cursors are process-scoped, so a server restart
yields `truncated: true`); `/inject` and `allowInsecureUrls` are loopback demo conveniences,
nonconformant with the sketch's TLS MUST; JSON Schema `inputSchema` params are advertised
but only shallowly validated. See [`docs/CONFORMANCE-NOTES.md`](docs/CONFORMANCE-NOTES.md).

## Provenance & license

Implementation: Apache-2.0. `docs/design-sketch-proposal.md` is a verbatim vendored copy of
the WG design sketch (author: Peter Alexander, Anthropic) from PR #1 of
`modelcontextprotocol/experimental-ext-triggers-events`, included for reference. This is a
community demo by a Drasi maintainer; it is not an official artifact of the MCP project,
Anthropic, Microsoft, or the Drasi project.
