# Demo: an agent that *chooses* what to watch

**The claim this demo proves:** give an agent a task, and it can *discover* the event
streams a server offers (MCP `events/list`), **decide for itself** which one its task needs,
subscribe with fitting arguments, go do other work — and react with judgment when the world
changes. Discovery, subscription, wake, verification, and action all happen **in one
protocol**: MCP with the draft [Events extension](docs/design-sketch-proposal.md), served by
the official Rust SDK on MCP 2026-07-28.

```
                          ┌──────────────── hybrid MCP server (POST /mcp) ────────────────┐
 Postgres ─WAL▶ Drasi ───▶│ events/list ─── the catalog (LLM-readable descriptions)       │
   two continuous queries │   high-value-orders.changed   (condition: total > $1,000)     │
                          │   stuck-orders.changed        (temporal: open for 45s —       │
                          │                                fires with NO write at all)    │
 fire-incident.sh ──────▶ │   incidents.created           (on-demand, P1–P4 filterable)   │
                          │ events/stream ── wakes the agent   tools/* ── verify + act    │
                          └───────────────────────────────△──────────────△────────────────┘
                                       task ▶ agent: discover ▶ choose ▶ subscribe ▶ react
```

Three terminals: **A** server · **B** agent · **C** where you make things happen.

## 0. One-time setup

```bash
scripts/demo-setup.sh      # builds binaries, installs Drasi plugins, starts docker,
                           # waits for health (idempotent — safe to re-run)
cp .env.example .env       # optional: LLM credentials; without them the
                           # deterministic brain runs the same beats
```

Only Postgres and the Drasi server live in Docker; the MCP server and agent are host
binaries you run in terminals below.

Terminal A: `cargo run -p mcp-events-server -- --config crates/mcp-events-server/examples/drasi.yaml`

## Act 1 — the agent picks its own subscription (the headline)

Terminal B — give it an on-call task:

```bash
cargo run -p mcp-events-agent -- --task \
  "You are on-call: watch for P1 incidents and escalate them"
```

Watch the log, it narrates the whole argument:

```
discovering event streams via events/list — 3 available:
  · high-value-orders.changed — Fires when an order enters, changes within, or leaves …
  · stuck-orders.changed — Fires when an order has remained in status 'open' for more …
  · incidents.created — Fires when an operational incident is reported. Payload: {prio…
chose incidents.created (arguments: {"priority":"P1"}) — This stream directly reports
  operational incidents and supports filtering to only P1 incidents, matching the
  on-call escalation task exactly.
subscribing to incidents.created via events/stream …
[background] routine batch work continues … (tick 1)
```

> **Say:** *"the model read the catalog — the same way it reads tool schemas — and chose
> a stream AND a filter argument to match its task. Subscription is a reasoning step, not
> config. Now it's doing other work; the subscription costs nothing."*

Terminal C — prove the filter is real. Fire a P2 first:

```bash
scripts/fire-incident.sh P2 email-service "Bounce rate elevated"
```

**Nothing happens in B** — the server filtered it out; the agent wasn't even woken. Then:

```bash
scripts/fire-incident.sh P1 checkout-service "Checkout returning 500s"
```

B wakes mid-tick and escalates in its own words:

```
EVENT incidents.created P1 on checkout-service — Checkout returning 500s — waking agent
  agent: Paging the P1 incident escalation immediately because checkout-service is
  returning 500s and the incident is marked P1, indicating a critical outage.
```

## Act 2 — same binary, different task, different choice (+ the tools loop)

Restart B with a different goal (`rm -f /tmp/drasi-agent-cursor.json` first):

```bash
cargo run -p mcp-events-agent -- --task \
  "Monitor high-value payment activity and flag anything that looks like possible fraud"
```

It chooses `high-value-orders.changed` this time — *the subscription followed the task.*
Terminal C:

```bash
docker exec drasi-demo-postgres psql -U demo -d demo \
  -c "INSERT INTO orders (customer, total, status) VALUES ('mallory', 9800, 'open');"
```

B wakes and runs the full events × tools loop — verify through MCP tools, act through one:

```
EVENT ADDED order 8 (mallory, $9800) — waking agent
  tool → get_order({"order_id":8})
  tool → get_customer_history({"customer":"mallory"})
  tool → flag_order({"order_id":8,"reason":"First order unusually high-value …"})
  agent: … I flagged it for human review as an unusually high-value first order.
```

> **Say:** *"the event carried triage data; the truth stayed behind the tools. The model
> verified before acting — and wrote its action durably via a tool."* Show the audit trail:
> `SELECT order_id, reason FROM order_flags ORDER BY id;`

## Act 3 — the event with no write at all (Drasi's beat)

Keep the fraud agent from Act 2 running? No — restart B with:

```bash
cargo run -p mcp-events-agent -- --task \
  "Watch fulfillment for orders that get stuck without being processed"
```

It chooses `stuck-orders.changed`. Terminal C — create an order and **touch nothing**:

```bash
docker exec drasi-demo-postgres psql -U demo -d demo \
  -c "INSERT INTO orders (customer, total, status) VALUES ('leo', 300, 'open');"
```

Narrate the silence. ~45 seconds later B wakes — **no second write ever happened**:

```
EVENT ADDED order 9 (leo, $300) — waking agent          ← fired by time passing
```

> **Say:** *"that event exists because a condition — 'open for 45 seconds straight' —
> became true. There is no cron job, no poller, no trigger anywhere in this system. The
> continuous query noticed time passing."* (`drasi.trueFor` in drasi/server.yaml.)
> Bonus: `UPDATE orders SET status='shipped' WHERE customer='leo'` → the agent gets
> `DELETED` — "unstuck" — from a plain UPDATE. *Caveat: the model sometimes narrows its
> subscription to `{"changeType": "added"}` (it did in our recorded run — a reasonable
> reading of the task), which filters the unstuck event out. If you want this bonus beat
> guaranteed, run the agent with the manual override instead:
> `--event stuck-orders.changed`.*

## Finale — the consumer dies and misses nothing

Ctrl-C the agent. Fire an event it cares about while it's dead. Restart with the *same
task* — it re-chooses, resumes from its persisted cursor, and **replays the missed event**:

```
resuming from persisted cursor Some("…")
EVENT … — waking agent            ← the one that fired while it was down
```

> **Say:** *"client-owned cursor + server-side retention = at-least-once, from primitives
> the extension already defines. A server restart instead yields `truncated: true` — the
> protocol's honest 'you missed things, re-verify via tools' signal."*
>
> Mechanics: cursor files are keyed **per stream** (`/tmp/drasi-agent-cursor-<stream>.json`),
> so replay works when the restarted agent lands on the same stream. The LLM re-choice is
> not strictly deterministic — with the same task it reliably picks the same *stream* (args
> may vary, which is harmless: the cursor belongs to the stream, and arguments only filter).
> For a zero-variance stage finale, pin the restart with
> `--event incidents.created --params '{"priority":"P1"}'`.

## For the WG cut, add (2 min)

- `events-harness discover` before Act 1: the extension declared in the 2026-07-28
  `extensions` capability map, served by rmcp, next to `tools`.
- After the finale: *"push is what you watched; poll and Standard-Webhooks-signed webhook
  delivery run against the same buffer, cursors, and filters."*

## Reset between runs

```bash
scripts/demo-reset.sh      # stops the MCP server, restores the 4 seed rows,
                           # clears order_flags and all agent cursor files
```

Then start Terminal A (server) and Terminal B (agent) fresh. Docker containers keep
running across resets. If you changed `drasi/server.yaml`/`seed.sql`, Drasi is wedged,
or you want a known-good world before going on stage: `scripts/demo-hard-reset.sh`
rebuilds the containers and volumes from scratch (~a minute).

No LLM credentials? Everything above runs with the deterministic brain: the chooser falls
back to keyword matching over the same catalog (still extracts `{"priority":"P1"}` from the
task) and reactions use fixed rules — same beats, canned prose. A captured live transcript
(Azure `gpt-5.4` via the Responses API) is in [`docs/demo-transcript.txt`](docs/demo-transcript.txt).
