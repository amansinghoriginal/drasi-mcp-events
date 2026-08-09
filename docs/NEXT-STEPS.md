# Taking this work forward

State as of 2026-08-10: the repo is a working, reviewed, live-verified demo of the draft
MCP Events extension (see [README](../README.md) and [DEMO.md](../DEMO.md); a captured
transcript with a real Azure-hosted model is in `demo-transcript.txt`). Everything below is
scoped follow-on work, roughly ordered by value.

## Demo & polish

- **Fresh transcript.** `demo-transcript.txt` was captured just before the last round of
  review fixes (task-contextualized reaction prompts, per-stream playbooks). A re-capture
  makes Act 3's verdicts read as ops language instead of fraud language.
- **Polling-cost comparison.** Run the same scenario with a tool-polling agent vs. the
  event-driven one; a calls/tokens/latency table is the quantitative argument for the
  primitive (slide material, not live).
- **MCP Inspector screenshot.** Point Inspector at `:8090/mcp` — it sees the tools and
  discover surface (legacy initialize back-compat) and, by design, not the `events/*`
  methods. Good visual for "extensions degrade gracefully".

## Protocol / WG work

- **Durable cursors.** The event buffer is in-memory with process-scoped `epoch:seq`
  cursors (a server restart yields `truncated: true`). Deriving cursors from upstream
  positions (Postgres WAL LSN / Drasi bootstrap) would demonstrate the design sketch's
  durable-upstream cursor story end to end. This is the single most WG-valuable extension
  of this codebase.
- **SDK extension hooks (Route 2).** The `events/*` methods are dispatched *in front of*
  rmcp because SDKs have no custom-method surface; consequently they bypass the SDK's
  `_meta`/lifecycle validation (partially reimplemented in `dispatch.rs`, e.g. the -32020
  header-mismatch guard). Contributing extension-registration hooks to
  `modelcontextprotocol/rust-sdk` — and turning this repo's handlers into the reference
  events extension — is the natural Extensions-Track SEP companion.
- **Sketch rebase tracking.** `design-sketch-proposal.md` (vendored 2026-06-11) predates
  the 2026-07-28 stateless core; the WG will rebase it (removing the GET-SSE stream, etc.).
  When that lands, re-check: stream framing, `subscriptionId` correlation, resultType
  stamping, and the webhook control envelopes.
- **Webhook / remote variant.** Webhook delivery (Standard-Webhooks signed,
  challenge-verified, TTL) is implemented and tested but not in the stage demo — its
  topology (server can't reach a connected client) needs a remote deployment. Deploying
  Postgres+Drasi+server to a cloud host and running only the agent locally makes the
  "close the laptop" story literal and showcases webhook mode. Note: rmcp's DNS-rebinding
  guard defaults to loopback `allowed_hosts`; a remote deployment must configure
  `with_allowed_hosts(...)`. The companion repo
  [`drasi-mcp-agent`](https://github.com/amansinghoriginal/drasi-mcp-agent) (branch
  `demo/rmcp-2026-07-28`) holds a Dapr scale-to-zero webhook consumer (currently blocked
  on a missing local `dapr-agents` checkout) and an untested Python standalone agent
  (official Python SDK; blocked at the time by a PyPI CDN outage).

## Code quality / hardening

- **Real JSON Schema validation.** `inputSchema` params are only shallowly validated
  server-side (changeType enum for diff types; equality filters for injected types).
- **Events-side `_meta` validation.** The extension dispatcher accepts requests leniently
  (any or no `_meta`); strict 2026-07-28 validation would match rmcp's behavior for core
  methods.
- **stdio transport** is absent throughout (Streamable HTTP only).
- **`/inject` is demo machinery**: unauthenticated by design, guarded only by the server's
  loopback bind. Don't ship it anywhere real without auth.

## Housekeeping

- `SPEC-GAPS.md` is an unaudited artifact of the original clean-room build — verify before
  citing (its 58 findings were never adversarially reviewed, unlike the code).
- `docs/ARCHITECTURE.md` is historical (see its banner); the live architecture is in the
  README. `CONFORMANCE-NOTES.md` and `INTEROP.md` describe the pre-rebuild state.
