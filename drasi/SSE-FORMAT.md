# Drasi SSE Reaction — Wire Format

This document specifies the exact wire format emitted by the Drasi Server SSE reaction
(`kind: sse`), as configured by `drasi/server.yaml` in this repo. It was derived by reading
the reaction implementation in drasi-core:

- `core/components/reactions/sse/src/sse.rs` (frame construction, broadcast, HTTP server)
- `core/components/reactions/sse/src/config.rs` (config + defaults)
- `core/lib/src/channels/events.rs` (`QueryResult`, `ResultDiff` serde shapes)
- `core/lib/src/queries/manager.rs` (how diffs are populated from query evaluation)
- axum 0.7 `response/sse.rs` (byte-level SSE encoding used by the reaction)

It is intended to be sufficient, on its own, to implement a parser (`drasi-feed`).

## 1. Endpoint

| Item | Value |
|---|---|
| Method | `GET` |
| URL (this demo) | `http://localhost:8081/events` |
| Path | the reaction's `ssePath` config (default `/events`); demo uses `/events` |
| Port | reaction `port` config (default `8080`); demo uses `8081` |
| Response status | `200` with an unbounded body; **`404`** for any path with no registered stream |
| Response headers | `content-type: text/event-stream`, `cache-control: no-cache` |
| CORS | permissive (`*` origins, `GET`/`OPTIONS`) |
| Auth | none |
| Request headers | none required (no `Accept` negotiation, **no `Last-Event-ID` support**) |

The HTTP server uses a catch-all route and looks the request path up in a map of
broadcasters. With custom per-query templates (not used in this demo) additional sub-paths
can exist (e.g. `/events/<custom>`); with the default configuration the only valid path is
the base `ssePath`.

## 2. SSE framing

Every message is one anonymous SSE event consisting of a single `data:` line:

```
data: <minified single-line JSON>\n
\n
```

Precise rules (from axum's encoder, which the reaction uses):

- Field encoding is `data` + `:` + one space + payload + `\n`; the event is terminated by
  one extra `\n` (so the frame ends in `\n\n`). Lines are `\n`-delimited, not `\r\n`.
- The reaction **never** sets `event:`, `id:` or `retry:` fields. Parsers must dispatch on
  the JSON content only; there is nothing to resume from (`Last-Event-ID` is meaningless).
- JSON payloads are produced by `serde_json::Value::to_string()`: minified, no embedded
  newlines, so there is always exactly one `data:` line per event in practice. (A defensive
  parser should still concatenate multiple consecutive `data:` lines with `\n` per the SSE
  spec — multi-line frames are only reachable via custom Handlebars templates containing
  newlines, which this demo does not configure.)
- **Keep-alive comment lines** are emitted every **30 seconds** (hard-coded in the reaction,
  independent of `heartbeatIntervalMs`):

  ```
  : keep-alive\n
  \n
  ```

  Per the SSE spec, lines starting with `:` are comments and must be ignored. Note the
  space after the colon.
- JSON **object key order is alphabetical** (drasi-core builds payloads through
  `serde_json`'s default BTreeMap-backed `Map`; `preserve_order` is not enabled). Do not
  rely on key order either way.

## 3. Frame payloads (default format — what this demo emits)

When the reaction has no `routes` / `defaultTemplate` configured (our case), exactly two
JSON payload shapes appear on the stream:

### 3.1 Result frames

One frame per `QueryResult` batch dequeued from the query. A batch corresponds to one
source change (or one bootstrap chunk) and can contain **one or more** diffs:

```json
{"queryId":"<query id>","results":[ <diff>, ... ],"timestamp":<int>}
```

| Field | Type | Meaning |
|---|---|---|
| `queryId` | string | The continuous query id (`"high-value-orders"` here). A reaction subscribed to multiple queries multiplexes them on one stream, distinguished only by this field. |
| `results` | array | One or more diff objects (below). Order within the array is meaningful (evaluation order). |
| `timestamp` | integer | Unix **epoch milliseconds** at the time the *reaction processed* the batch (not the query-evaluation or DB-commit time). |

The `QueryResult`'s internal `metadata` and `profiling` fields are **not** included in the
SSE payload.

### 3.2 Diff objects (elements of `results`)

Internally-tagged on `"type"`. Five variants exist:

| `type` | Other fields | Semantics for a continuous query |
|---|---|---|
| `"ADD"` | `data`: object | Row **entered** the result set (insert matching the WHERE, or update crossing into it, or initial bootstrap row). `data` = the row, keyed by the query's RETURN aliases. |
| `"UPDATE"` | `before`: object, `after`: object, `data`: object, optional `grouping_keys`: string[] | Row was already in the result set and changed. `before`/`after` are the old/new projected rows. `data` duplicates `after`. `grouping_keys` is omitted when absent (drasi-lib currently never sets it — treat as optional and ignorable; note the **snake_case** name). |
| `"DELETE"` | `data`: object | Row **left** the result set (delete, or update crossing out of the WHERE). `data` = the last projected row. |
| `"aggregation"` | `before`: object **or null** (always present), `after`: object | Aggregate-result change (lowercase tag!). Not produced by the non-aggregating demo query, but parsers should tolerate it. |
| `"noop"` | none | Evaluation produced no observable change. Can appear inside `results`; skip it. |

> **Live-verified addendum (image 0.1.6 / reaction-sse plugin 0.3.2):** each `ADD`/`UPDATE`/`DELETE`
> diff additionally carries `row_signature` (JSON integer, u64 content hash of the projected row).
> It is a *content* signature, not an event id — an ADD and a later DELETE of an identical row carry
> the same value — so it MUST NOT be used for event deduplication. Ignore it.

Row objects (`data`/`before`/`after`) are keyed by the **RETURN aliases** of the Cypher
query. For `high-value-orders`:

```json
{"customer":"alice","id":1,"status":"open","total":1500.0}
```

- `id` (serial/int4) → JSON integer.
- `total` (numeric) → JSON number. The textual form may carry a trailing `.0` (decimals are
  routed through f64); parse as a JSON number, do not string-match.
- `customer`, `status` (text) → JSON strings. SQL `NULL` would surface as JSON `null`
  (nulls are also dropped from node properties at the source, so a NULL column may simply
  be absent from the projected row — handle both).

### 3.3 Heartbeat frames

Application-level heartbeats are broadcast to **every** path each `heartbeatIntervalMs`
(demo config: 30000 ms; default 30000 ms). They are regular `data:` events (distinct from
the `: keep-alive` comments) and interleave with result frames:

```
data: {"ts":1749600015000,"type":"heartbeat"}
```

| Field | Type | Meaning |
|---|---|---|
| `type` | string | Always `"heartbeat"`. |
| `ts` | integer | Unix epoch milliseconds at emit time. |

Dispatch rule for a parser: a payload object with `"type":"heartbeat"` is a heartbeat;
a payload object with `queryId` + `results` is a result batch; anything else should be
logged and skipped.

## 4. Concrete example frames

Raw bytes on the wire (each block ends with a blank line). Captured semantics for the demo
query `MATCH (o:orders) WHERE o.total > 1000 RETURN o.id AS id, o.customer AS customer,
o.total AS total, o.status AS status`.

**Added** — `INSERT INTO orders (customer, total, status) VALUES ('erin', 5000, 'open');`

```
data: {"queryId":"high-value-orders","results":[{"data":{"customer":"erin","id":5,"status":"open","total":5000.0},"type":"ADD"}],"timestamp":1749600000123}

```

**Updated** — `UPDATE orders SET total = 1800 WHERE customer = 'alice';` (row stays in the
result set):

```
data: {"queryId":"high-value-orders","results":[{"after":{"customer":"alice","id":1,"status":"open","total":1800.0},"before":{"customer":"alice","id":1,"status":"open","total":1500.0},"data":{"customer":"alice","id":1,"status":"open","total":1800.0},"type":"UPDATE"}],"timestamp":1749600005456}

```

**Deleted** — `DELETE FROM orders WHERE customer = 'erin';` (also produced by an UPDATE
that drops `total` to ≤ 1000):

```
data: {"queryId":"high-value-orders","results":[{"data":{"customer":"erin","id":5,"status":"open","total":5000.0},"type":"DELETE"}],"timestamp":1749600010789}

```

**Heartbeat** (every `heartbeatIntervalMs`):

```
data: {"ts":1749600015000,"type":"heartbeat"}

```

**Keep-alive comment** (every 30 s, fixed):

```
: keep-alive

```

**Multi-diff batch** (e.g. one transaction touching several rows; also typical of the
bootstrap snapshot at query start):

```
data: {"queryId":"high-value-orders","results":[{"data":{"customer":"alice","id":1,"status":"open","total":1500.0},"type":"ADD"},{"data":{"customer":"carol","id":3,"status":"shipped","total":2200.5},"type":"ADD"}],"timestamp":1749599990000}

```

## 5. Delivery semantics / caveats for the consumer

- **No replay, no backfill.** Each connection joins a per-path tokio broadcast channel
  (capacity 1024) and receives only frames broadcast *after* it connected. There is no
  `id:`/`Last-Event-ID` resume mechanism. Reconnect = fresh subscription with a gap.
- **Silent loss under lag.** A slow consumer that falls more than 1024 messages behind has
  the lagged frames silently dropped (broadcast-stream errors are filtered out server-side;
  no gap marker is emitted).
- **Bootstrap frames are easy to miss.** The query's initial bootstrap (seed rows with
  `total > 1000`) is dispatched as `ADD` diffs when the query starts at server boot; if no
  SSE client is connected at that moment the frames go nowhere. Use the management API to
  read the current result set instead: `GET http://localhost:8080/api/v1/queries/high-value-orders/results`.
- **No terminal/control event.** Server shutdown simply closes the TCP stream. The only
  "control" traffic is the heartbeat data frame (§3.3) and the keep-alive comment (§2).
- Unknown JSON fields and unknown `type` values should be tolerated and ignored (future
  drasi versions may extend the shapes).

## 6. Live verification (2026-06-11)

This format was verified against a running stack (`drasi-server:0.1.6`, plugins
`source/postgres`, `bootstrap/postgres`, `reaction/sse 0.3.2`) by capturing
`curl -N http://localhost:8081/events` while executing the RUNBOOK's psql scenarios:

- INSERT above threshold → `ADD` frame ✓ (erin, total 5000)
- UPDATE within result set → `UPDATE` frame with correct `before`/`after` ✓ (alice 1500→1800)
- UPDATE crossing below threshold → `DELETE` frame ✓ (carol — SQL UPDATE, semantic delete)
- DELETE of in-set row → `DELETE` frame ✓ (erin)
- heartbeat data frames interleave as documented ✓

Deviations found and folded into §3.2: the undocumented `row_signature` field; numeric
values from WAL decoding may serialize without a fractional part (`5000`) while
bootstrap-sourced values carry one (`1500.0`) — parse as JSON numbers.

## 7. Alternate format: custom templates (NOT used by this demo)

For completeness — if the reaction were configured with `routes` or `defaultTemplate`
(per-query Handlebars templates), the stream changes shape: one frame **per diff** (not per
batch), rendered from the template with context `{before, after, query_name, operation,
timestamp}` where `operation` ∈ `ADD|UPDATE|DELETE|AGGREGATION` (aggregations route through
the `updated` template; noops are dropped). An empty template string falls back to a
per-diff JSON `{"queryId":...,"result":<diff>,"timestamp":...}` (singular `result`).
Templates may also route to custom sub-paths. A parser targeting this repo's `server.yaml`
only needs the default format in §3.
