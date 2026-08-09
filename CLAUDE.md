# Working on drasi-mcp-events

A working demo of the draft MCP Events extension: Drasi continuous queries + an injected
incident stream feed a hybrid MCP server (official rmcp SDK core + hand-dispatched
`events/*` methods, MCP 2026-07-28), consumed by a task-driven agent that discovers the
catalog, chooses its own subscription, and reacts via MCP tools. Read `README.md` for
architecture, `DEMO.md` for the demo script, `docs/NEXT-STEPS.md` for the roadmap.

## Commands

```bash
cargo build --workspace && cargo test --workspace   # 244 tests, no external services needed
scripts/demo-setup.sh        # one-time: build + Drasi docker env (idempotent)
scripts/demo-reset.sh        # between demo runs: seed rows, no flags, no cursors
scripts/demo-hard-reset.sh   # rebuild containers+volumes (needed after editing
                             # drasi/server.yaml or drasi/seed.sql)
scripts/fire-incident.sh P1 <service> "<title>"     # fire an injected incident
```

Server: `cargo run -p mcp-events-server -- --config crates/mcp-events-server/examples/drasi.yaml`
(`mock.yaml` = no-Docker synthetic feed). Agent: `cargo run -p mcp-events-agent -- --task "..."`.
Harness: `cargo run -p mcp-events-client --bin events-harness -- <discover|list|poll|stream|subscribe>`.

## Gotchas that will bite you

- **`mcp-events-server` has no lib target**: its integration tests compile `src/*.rs`
  directly via `#[path]` includes. Adding a module or a `ServerConfig` field means updating
  `tests/integration.rs` and `tests/webhook.rs` (module list + config literals) too.
- **`events/*` requests never reach rmcp**: `dispatch.rs` routes them (by `Mcp-Method`
  header, body fallback) to the extension handlers *before* the SDK. Anything the SDK
  normally validates (headers, `_meta`, sessions) must be handled there explicitly.
- **Cursors are process-scoped** (`epoch:seq` in an in-memory ring buffer). Server restart
  ⇒ clients get `truncated: true` + fresh cursor. Agent cursor files are per-stream:
  `/tmp/drasi-agent-cursor-<stream>.json`.
- **Drasi config/seed changes need `demo-hard-reset.sh`** — `server.yaml` is bind-mounted
  read-only and `seed.sql` only runs on an empty Postgres volume.
- **Seed rows must never be status `'open'`** — the `stuck-orders` temporal query
  (`drasi.trueFor`, 45s) would fire on them after every reset.
- **LLM config** comes from untracked `.env` (see `.env.example`): three dialects —
  `anthropic`, `openai` (chat completions), and OpenAI Responses (auto-detected from a
  `/responses` URL). No credentials ⇒ deterministic policy brain, same demo beats.
- **The agent treats LLM output as untrusted**: chosen subscription arguments are validated
  against the stream's `inputSchema` (case-coerced enums), truncated tool calls bail, and
  a failing event is retried via cursor replay 3× then skipped loudly. Keep these
  properties when extending.
- `docs/ARCHITECTURE.md` is **historical** (pre-rebuild contract); don't follow its rules.
  `SPEC-GAPS.md` is unaudited — don't cite it as fact.

## Conventions

Wire JSON is camelCase (serde rename_all); `null` vs absent is meaningful for `cursor` /
`ttlMs` / `refreshBefore` (`Option<Option<T>>` pattern). Every behavior change ships with a
test pinning it (the adversarial reviews repeatedly found bugs the suite missed — when
fixing a bug, add the test that would have caught it).
