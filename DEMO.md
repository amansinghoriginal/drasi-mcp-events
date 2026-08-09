# Demo: the MCP Events primitive, end to end

**What this shows:** the loop the draft [MCP Events extension](docs/design-sketch-proposal.md)
exists to enable — *something happens in the world → an agent that was doing nothing wakes up,
verifies through MCP tools, and acts* — running on the **official Rust SDK (rmcp 3.1.x)** and
**MCP 2026-07-28** (stateless lifecycle, `server/discover`, extensions capability map).

```
psql INSERT/UPDATE ─▶ Postgres WAL ─▶ Drasi continuous query ─▶ SSE ─▶ hybrid MCP server
                                    "row entered/changed/left            │ rmcp core: server/discover, tools/*
                                     the high-value result set"          │ extension: events/list|poll|stream|subscribe
                                                                         ▼
                                              drasi-agent: events/stream wakes it
                                                → get_order / get_customer_history (official SDK client)
                                                → flag_order  (writes order_flags, outside the WAL feed)
```

Everything is terminal-based: three terminals, no UI.

## 0. One-time setup

```bash
cargo build --workspace                              # builds server, harness, agent
cd drasi
docker compose run --rm plugin-install               # one-time: Drasi plugins (runs as root)
docker compose up -d                                 # postgres:16 + drasi-server 0.1.6
curl -fsS http://localhost:8080/health               # wait until {"status":"ok",...}
cd ..
```

## 1. Terminal A — the hybrid server

```bash
cargo run -p mcp-events-server -- --config crates/mcp-events-server/examples/drasi.yaml
```

> **Say:** one endpoint, `POST /mcp`. The MCP core — discover, lifecycle, tools — is the
> official Rust SDK; the five `events/*` methods are the draft extension, dispatched by
> method name in front of it. To a client it's one server.

## 2. Terminal B — discovery beat (the SEP story)

```bash
cargo run -p mcp-events-client --bin events-harness -- discover
```

> **Say:** `server/discover` is the 2026-07-28 stateless entry point — no initialize, no
> session. Note `capabilities.extensions["io.modelcontextprotocol/events"]`: the extension
> is declared exactly the way Tasks or MCP Apps are, alongside `tools`. This is what "Events
> as an MCP extension" looks like on the current protocol.

Optionally show the event catalog (typed filters + payload schema):

```bash
cargo run -p mcp-events-client --bin events-harness -- list
```

## 3. Terminal B — start the agent

```bash
# Deterministic (no key needed):
cargo run -p mcp-events-agent
# Or let Claude decide (same loop, model chooses the tool calls):
ANTHROPIC_API_KEY=sk-... cargo run -p mcp-events-agent
```

> **Say:** the agent subscribed with `events/stream` and is now doing *nothing*. No polling
> loop in the agent, no tokens burning. Event payloads are semantic diffs
> `{changeType, before, after}` — triage data. The truth stays behind the tools.

## 4. Terminal C — the three-act scenario

**Act 1 — anomalous order arrives** (watch Terminal B wake):

```bash
docker exec drasi-demo-postgres psql -U demo -d demo \
  -c "INSERT INTO orders (customer, total, status) VALUES ('ivy', 7200, 'open');"
```

Agent: wakes on `ADDED` → `get_order` → `get_customer_history` (first-ever order) →
`flag_order`. A row appears in `order_flags`.

**Act 2 — routine change, no action** (the agent can say no):

```bash
docker exec drasi-demo-postgres psql -U demo -d demo \
  -c "UPDATE orders SET total = 2350 WHERE customer = 'carol';"
```

Agent: wakes on `UPDATED`, sees a ~7% bump, does nothing. *(An update that jumps ≥3× in one
step gets flagged instead — try `total = 9000`.)*

**Act 3 — the row leaves the result set** (Drasi's semantic diff):

```bash
docker exec drasi-demo-postgres psql -U demo -d demo \
  -c "UPDATE orders SET total = 400 WHERE customer = 'ivy';"
```

> **Say:** that was a SQL `UPDATE`, but the agent received `DELETED` — the *condition*
> "total > 1000" stopped being true. That's a continuous query talking, not a table trigger.
> No raw webhook can express this.

**Finale — close the laptop.** Kill the agent (Ctrl-C in Terminal B), make a change while
nothing is listening, restart it:

```bash
docker exec drasi-demo-postgres psql -U demo -d demo \
  -c "INSERT INTO orders (customer, total, status) VALUES ('jack', 12000, 'open');"
cargo run -p mcp-events-agent    # resumes from the persisted cursor, replays jack's order
```

> **Say:** the cursor is client-owned state (a file). The upstream is durable, so the missed
> event replays. This is the "kick off, disconnect, get woken later" story the WG's Tasks
> and Events work both circle around.

Audit trail:

```bash
docker exec drasi-demo-postgres psql -U demo -d demo \
  -c "SELECT order_id, reason, flagged_by, flagged_at FROM order_flags ORDER BY id;"
```

## 5. Reset between runs

```bash
docker exec drasi-demo-postgres psql -U demo -d demo -c "TRUNCATE order_flags RESTART IDENTITY;"
rm -f /tmp/drasi-agent-cursor.json
```

Full environment reset (`down -v` deletes the plugin volume, so the install step must run
again before `up`):

```bash
cd drasi && docker compose down -v && docker compose run --rm plugin-install && docker compose up -d
```

## Notes

- **MCP Inspector / other MCP clients** connect to `http://127.0.0.1:8090/mcp` and see the
  tools and discover surface via the official SDK (legacy initialize still works for older
  clients; sessions exist only for them per SEP-2567). They won't see `events/*` — no
  released client speaks the draft extension yet, which is the point of this prototype.
- **Delivery modes:** the agent uses push (`events/stream`). Poll and webhook (Standard
  Webhooks signed, challenge-verified) are also live — see `events-harness poll/subscribe`.
- A sample transcript of the full run is in [`docs/demo-transcript.txt`](docs/demo-transcript.txt).
