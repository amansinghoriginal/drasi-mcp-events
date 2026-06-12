# Drasi Demo Environment — Runbook

Demo stack for the MCP Events prototype: PostgreSQL 16 (logical replication) + Drasi
Server 0.1.0 running the `high-value-orders` continuous query (`orders` rows with
`total > 1000`) and an SSE reaction on port 8081.

Prerequisites: Docker (with compose v2), `curl`. `psql` is optional — every database
command below also works via `docker compose exec postgres ...`.

All commands run from this directory (`drasi/`).

## 1. Bring-up

```bash
docker compose up -d
docker compose logs -f drasi-server   # Ctrl-C to stop following
```

Expected in the logs: the postgres source connecting and starting replication, the
`high-value-orders` query bootstrapping (2 seed rows match: alice 1500.00, carol 2200.50),
and `Starting SSE server on 0.0.0.0:8081 with CORS enabled`.

Ports on the host:

| Port | What |
|---|---|
| 5433 | PostgreSQL (`demo`/`demo`, db `demo`) — 5433 to avoid clashing with a local 5432 |
| 8080 | Drasi REST API |
| 8081 | SSE reaction (`GET /events`) |

## 2. Health checks

```bash
# Drasi server alive?
curl -fsS http://localhost:8080/health

# Source / query / reaction registered?
curl -fsS http://localhost:8080/api/v1/sources | python3 -m json.tool
curl -fsS http://localhost:8080/api/v1/queries | python3 -m json.tool
curl -fsS http://localhost:8080/api/v1/reactions | python3 -m json.tool

# Current materialized result set (should contain alice and carol after bootstrap):
curl -fsS http://localhost:8080/api/v1/queries/high-value-orders/results | python3 -m json.tool

# SSE endpoint reachable (streams forever; --max-time bails out after 35s, long
# enough to observe at least one heartbeat frame / keep-alive comment):
curl -N --max-time 35 http://localhost:8081/events

# Postgres reachable from the host:
psql "postgresql://demo:demo@localhost:5433/demo" -c "TABLE orders;"
# ... or without a local psql:
docker compose exec postgres psql -U demo -d demo -c "TABLE orders;"
```

## 3. Trigger query result changes

Keep a stream open in one terminal:

```bash
curl -N http://localhost:8081/events
```

Then fire these one-liners in another (see `SSE-FORMAT.md` for the exact frames):

```bash
PSQL='psql postgresql://demo:demo@localhost:5433/demo'
# no local psql? use:  PSQL='docker compose exec postgres psql -U demo -d demo'

# ADD — insert a new order above the threshold (row enters the result set):
$PSQL -c "INSERT INTO orders (customer, total, status) VALUES ('erin', 5000, 'open');"

# UPDATE — change a row that is already in the result set (stays above 1000):
$PSQL -c "UPDATE orders SET total = 1800 WHERE customer = 'alice';"

# ADD via threshold crossing — bob goes from 250 to 1200 (enters the result set):
$PSQL -c "UPDATE orders SET total = 1200 WHERE customer = 'bob';"

# DELETE via threshold crossing — bob drops back below 1000 (leaves the result set):
$PSQL -c "UPDATE orders SET total = 250 WHERE customer = 'bob';"

# DELETE — remove a row that is in the result set:
$PSQL -c "DELETE FROM orders WHERE customer = 'erin';"
```

Expected frame per command (all on the open `/events` stream, `"queryId":"high-value-orders"`):

1. `results: [{"type":"ADD", "data": {erin/5000 row}}]`
2. `results: [{"type":"UPDATE", "before": {alice/1500}, "after": {alice/1800}, "data": {alice/1800}}]`
3. `results: [{"type":"ADD", "data": {bob/1200 row}}]` — an UPDATE in SQL, but an ADD to the query result
4. `results: [{"type":"DELETE", "data": {bob/1200 row}}]` — leaves the result set
5. `results: [{"type":"DELETE", "data": {erin/5000 row}}]`

Interleaved with `{"ts":...,"type":"heartbeat"}` data frames (every 30s) and `: keep-alive`
comment lines (every 30s).

Cross-check the materialized state at any point:

```bash
curl -fsS http://localhost:8080/api/v1/queries/high-value-orders/results | python3 -m json.tool
```

## 4. Teardown

```bash
docker compose down -v
```

`-v` also removes postgres's (anonymous) data volume so the next `up -d` re-runs
`seed.sql` from scratch (fresh table, publication, and replication slot). Without `-v`
the database — including the `drasi_slot` replication slot and any rows you mutated —
survives restarts.

## 5. Troubleshooting

- **Port already in use** — something on the host occupies 5433/8080/8081. Edit the host
  side of the `ports:` mappings in `docker-compose.yml` (container ports must stay 5432 /
  8080 / 8081 to match `server.yaml`).
- **No frames on /events although SQL ran** — confirm the source started
  (`curl http://localhost:8080/api/v1/sources`) and check
  `docker compose logs drasi-server` for replication errors. The publication is created by
  `seed.sql`; if you removed the volume without `-v` semantics getting applied, re-create
  with `docker compose down -v && docker compose up -d`.
- **Bootstrap ADDs not visible** — they are emitted at server start; an SSE client that
  connects later never sees them (no replay). Use the `/results` endpoint instead.
- **Connecting to 5433 from inside another container** — use `postgres:5432` on the
  compose network instead; 5433 only exists on the host.
- **API mutations rejected** — `server.yaml` is mounted read-only and `persistConfig` is
  false; this stack is configured declaratively. Edit `server.yaml` and
  `docker compose restart drasi-server` instead.
