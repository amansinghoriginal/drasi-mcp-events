<!-- Local reference notes on the MCP base protocol (spec 2025-11-25), used as the
     base-protocol source for this clean-room prototype. -->

# Model Context Protocol (MCP) — Comprehensive Reference

**Spec version**: `2025-11-25` (latest)
**Schema**: [schema.ts](https://github.com/modelcontextprotocol/specification/blob/main/schema/2025-11-25/schema.ts)
**Wire format**: JSON-RPC 2.0 over UTF-8
**Prior versions**: `2024-11-05` → `2025-03-26` → `2025-06-18` → `2025-11-25`

---

## 1. Architecture

Three roles:

| Role | Description |
|------|-------------|
| **Host** | LLM application that initiates connections (IDE, chat UI, agent runtime) |
| **Client** | Connector within the host; speaks MCP to exactly one server |
| **Server** | Service providing context and capabilities |

Communication is **stateful** with bidirectional capability negotiation at initialization.

---

## 2. Transports

### 2.1 stdio

Client spawns server as subprocess. Communication via stdin/stdout.

- Messages are newline-delimited JSON-RPC. MUST NOT contain embedded newlines.
- Server MUST NOT write non-MCP data to stdout.
- Server MAY write UTF-8 logs to stderr.
- Shutdown: client closes stdin → waits → SIGTERM → SIGKILL.
- Single-client only. No networking, no auth needed.
- **Clients SHOULD support stdio whenever possible** (per spec).

### 2.2 Streamable HTTP

Server runs as independent HTTP service. Replaces old HTTP+SSE transport from `2024-11-05`.

**Single endpoint** (e.g., `https://example.com/mcp`) supports both POST and GET.

#### Client → Server (POST)

- Every JSON-RPC message = one HTTP POST.
- Client MUST include `Accept: application/json, text/event-stream`.
- Notifications/responses from client: server returns `202 Accepted` (no body).
- Requests from client: server returns either:
  - `Content-Type: application/json` — single JSON-RPC response, OR
  - `Content-Type: text/event-stream` — SSE stream carrying the response + optional server-initiated messages.

#### Server → Client (GET)

- Client MAY open GET to the endpoint to receive an SSE stream for server-initiated messages.
- Server returns `text/event-stream` or `405 Method Not Allowed`.
- Server MAY send JSON-RPC requests/notifications on this stream.
- Server MUST NOT send JSON-RPC responses on GET streams (unless resuming a prior POST stream).

#### Session Management

- Server MAY assign `MCP-Session-Id` header during initialization.
- Client MUST include it on all subsequent requests.
- Session ID MUST be cryptographically secure (UUID, JWT, or hash). Visible ASCII only (0x21–0x7E).
- Server MAY terminate sessions; responds `404 Not Found` to expired session IDs.
- Client sends HTTP DELETE + session ID to end a session.
- Client MUST start new initialization on receiving `404`.

#### Protocol Version Header

Client MUST send `MCP-Protocol-Version: 2025-11-25` on all HTTP requests after initialization.

#### Resumability

- Server MAY assign SSE event IDs (globally unique within session/client).
- Client reconnects with `Last-Event-ID` header via GET to resume.
- Server MAY replay missed messages on the original stream only.
- Event IDs SHOULD encode stream identity for correlation.

#### Multiple Connections

- Client MAY maintain multiple SSE streams simultaneously.
- Server MUST NOT broadcast the same message across multiple streams.

#### Security

- Server MUST validate `Origin` header (403 Forbidden if invalid) — prevents DNS rebinding.
- Local servers SHOULD bind to `127.0.0.1`, not `0.0.0.0`.
- Servers SHOULD implement authentication.

#### Backwards Compatibility (with 2024-11-05 HTTP+SSE)

- Clients: POST `InitializeRequest`; if 400/404/405, fall back to old SSE-first transport.
- Servers: host both old and new endpoints side-by-side.

### 2.3 Custom Transports

Any channel supporting bidirectional message exchange. MUST preserve JSON-RPC format and MCP lifecycle.

---

## 3. Lifecycle

Three phases: **Initialization → Operation → Shutdown**.

### 3.1 Initialization

Client sends `initialize` request with:
- `protocolVersion` (SHOULD be latest supported)
- `capabilities` (client capabilities)
- `clientInfo` (name, title, version, description, icons, websiteUrl)

Server responds with:
- `protocolVersion` (same if supported, otherwise server's latest)
- `capabilities` (server capabilities)
- `serverInfo` (name, title, version, description, icons, websiteUrl)
- `instructions` (optional — natural language guidance for the client)

Client then sends `notifications/initialized`. No other requests (except ping) before this exchange completes.

#### Version Negotiation

- Client sends version it supports. Server responds with same or its own.
- If client doesn't support server's version, it SHOULD disconnect.

#### Capability Negotiation

| Side | Capability | Description |
|------|-----------|-------------|
| Client | `roots` | Filesystem root exposure |
| Client | `sampling` | LLM sampling requests |
| Client | `elicitation` | Server-initiated user input |
| Client | `tasks` | Task-augmented client requests |
| Server | `prompts` | Prompt templates |
| Server | `resources` | Readable resources |
| Server | `tools` | Callable tools |
| Server | `logging` | Structured log messages |
| Server | `completions` | Argument autocompletion |
| Server | `tasks` | Task-augmented server requests |

Sub-capabilities: `listChanged` (prompts, resources, tools), `subscribe` (resources only).

### 3.2 Operation

Both parties respect negotiated version and capabilities. Only use features that were successfully negotiated.

### 3.3 Shutdown

- **stdio**: Client closes stdin → waits → SIGTERM → SIGKILL. Server MAY initiate by closing stdout and exiting.
- **HTTP**: Close associated HTTP connections. Client MAY send DELETE with session ID.

### 3.4 Timeouts

- Implementations SHOULD set timeouts on all requests.
- On timeout: send cancellation notification, stop waiting.
- MAY reset timeout clock on progress notifications.
- SHOULD enforce maximum timeout regardless of progress.

---

## 4. Server Primitives

### 4.1 Resources

Provide **contextual data** (files, schemas, configs) to the LLM. Identified by URIs. **Application-controlled** — the host decides when/how to include them.

#### Capability

```json
{ "capabilities": { "resources": { "subscribe": true, "listChanged": true } } }
```

Both `subscribe` and `listChanged` are optional.

#### Protocol Messages

| Method | Direction | Description |
|--------|-----------|-------------|
| `resources/list` | Client → Server | Discover resources (paginated) |
| `resources/read` | Client → Server | Retrieve resource contents by URI |
| `resources/templates/list` | Client → Server | List parameterized resource templates (RFC 6570 URI templates) |
| `resources/subscribe` | Client → Server | Subscribe to changes on a specific URI |
| `resources/unsubscribe` | Client → Server | Unsubscribe |
| `notifications/resources/list_changed` | Server → Client | Resource list changed |
| `notifications/resources/updated` | Server → Client | Specific subscribed resource changed (includes URI) |

#### Resource Definition

- `uri`: Unique identifier (RFC 3986)
- `name`: Resource name
- `title`: Optional display name
- `description`: Optional
- `mimeType`: Optional MIME type
- `size`: Optional size in bytes
- `annotations`: `audience` (`["user"]`, `["assistant"]`, or both), `priority` (0.0–1.0), `lastModified` (ISO 8601)

#### Content Types

- **Text**: `{ "uri": "...", "mimeType": "text/plain", "text": "..." }`
- **Binary**: `{ "uri": "...", "mimeType": "image/png", "blob": "base64..." }`

#### URI Schemes

- `https://` — only if client can fetch directly from web
- `file://` — filesystem-like resources (need not map to actual filesystem)
- `git://` — version control
- Custom schemes — MUST follow RFC 3986

#### Errors

- Resource not found: `-32002`
- Internal errors: `-32603`

### 4.2 Tools

Expose **executable functions** for the LLM. **Model-controlled** — the LLM discovers and invokes them.

#### Capability

```json
{ "capabilities": { "tools": { "listChanged": true } } }
```

#### Protocol Messages

| Method | Direction | Description |
|--------|-----------|-------------|
| `tools/list` | Client → Server | Discover tools (paginated) |
| `tools/call` | Client → Server | Invoke a tool with arguments |
| `notifications/tools/list_changed` | Server → Client | Tool list changed |

#### Tool Definition

- `name`: Unique identifier
- `title`: Optional display name
- `description`: Human-readable (LLM uses this to decide invocation)
- `inputSchema`: JSON Schema for parameters
- `outputSchema`: Optional JSON Schema for structured output
- `annotations`: Behavior hints — **clients MUST treat as untrusted** unless from trusted server
- `execution.taskSupport`: `"required"` | `"optional"` | `"forbidden"` (for task-augmented calls)

#### Tool Results

Unstructured content in `content` array:

| Type | Fields |
|------|--------|
| `text` | `text` |
| `image` | `data` (base64), `mimeType` |
| `audio` | `data` (base64), `mimeType` |
| `resource_link` | `uri`, `name`, `description`, `mimeType` |
| `resource` | `resource: { uri, mimeType, text/blob }` (embedded) |

Structured content in `structuredContent` field (JSON object). For backwards compatibility, also return serialized JSON in a `text` content block.

#### Error Handling

Two mechanisms:
1. **Protocol errors**: JSON-RPC errors (unknown tool: `-32602`, server error: `-32603`)
2. **Tool execution errors**: `isError: true` in result (API failures, bad input, business logic)

Input validation errors SHOULD be tool execution errors (not protocol errors) to enable model self-correction.

#### Security

- Servers MUST: validate inputs, implement access controls, rate limit, sanitize outputs.
- Clients SHOULD: prompt user confirmation, show inputs before calling, validate results, implement timeouts, log usage.

### 4.3 Prompts

Server-provided **prompt templates**. **User-controlled** — users explicitly select them (e.g., slash commands).

#### Capability

```json
{ "capabilities": { "prompts": { "listChanged": true } } }
```

#### Protocol Messages

| Method | Direction | Description |
|--------|-----------|-------------|
| `prompts/list` | Client → Server | List prompts (paginated) |
| `prompts/get` | Client → Server | Get prompt content with arguments |
| `notifications/prompts/list_changed` | Server → Client | Prompt list changed |

#### Prompt Definition

- `name`: Unique identifier
- `title`: Optional display name
- `description`: Optional
- `arguments`: List of `{ name, description, required }` for customization

#### PromptMessage

- `role`: `"user"` or `"assistant"`
- `content`: text, image, audio, or embedded resource

---

## 5. Client Primitives

### 5.1 Sampling

Allows servers to request **LLM completions** through the client. Server needs no API keys.

#### Capability

```json
{ "capabilities": { "sampling": { "tools": {}, "context": {} } } }
```

`tools`: client supports tool use in sampling. `context`: supports context inclusion (soft-deprecated).

#### Protocol

- Server sends `sampling/createMessage` with `messages`, `modelPreferences`, `systemPrompt`, `maxTokens`.
- Client returns `CreateMessageResult` with `role`, `content`, `model`, `stopReason`.

#### Model Preferences

- `hints`: Array of `{ name }` — substring-matched against model names (e.g., `"claude-3-sonnet"`)
- `costPriority`, `speedPriority`, `intelligencePriority`: 0.0–1.0 normalized priorities
- Client makes final model selection; hints are advisory.

#### Tool Use in Sampling

- Server includes `tools` array and optional `toolChoice` (`auto` | `required` | `none`) in request.
- Response with `stopReason: "toolUse"` contains `ToolUseContent` blocks.
- Server executes tools, sends follow-up `sampling/createMessage` with tool results appended.
- Multi-turn loop until `stopReason: "endTurn"`.

#### Message Constraints

- Tool result messages MUST contain ONLY tool results (no mixing with text/image/audio).
- Every `ToolUseContent` block MUST be matched by a `ToolResultContent` with same `toolUseId`.

#### Human-in-the-Loop

- Users SHOULD approve sampling requests, review prompts, and approve responses before delivery.

### 5.2 Roots

Clients expose **filesystem boundaries** (`file://` URIs) to servers.

#### Capability

```json
{ "capabilities": { "roots": { "listChanged": true } } }
```

#### Protocol

| Method | Direction | Description |
|--------|-----------|-------------|
| `roots/list` | Server → Client | Get available roots |
| `notifications/roots/list_changed` | Client → Server | Root list changed |

#### Root Definition

- `uri`: MUST be `file://` URI
- `name`: Optional display name

### 5.3 Elicitation

Servers request **user input** through the client. Two modes:

#### Capability

```json
{ "capabilities": { "elicitation": { "form": {}, "url": {} } } }
```

Must support at least one mode. Empty object = form-only (backwards compat).

#### Form Mode

- Structured data collection with JSON Schema validation.
- Schema restricted to flat objects with primitive types: string, number/integer, boolean, enum (single/multi-select).
- Supported string formats: `email`, `uri`, `date`, `date-time`.
- All types support optional `default` values.
- **MUST NOT request sensitive data** (passwords, API keys, tokens, payment credentials).

#### URL Mode (new in 2025-11-25)

- Directs user to external URL for sensitive/out-of-band interactions (OAuth, payments, API keys).
- Includes `url`, `elicitationId`, `message`.
- Client MUST NOT auto-fetch URL, MUST show full URL, MUST get explicit user consent.
- Client MUST open URL in secure browser context (not embedded webview).
- Server MAY send `notifications/elicitation/complete` when out-of-band interaction finishes.
- `URLElicitationRequiredError` (`-32042`): server returns this when a request can't proceed without URL elicitation.

#### Response Actions

- `accept`: User approved + submitted data (form: `content` field has data; URL: no content)
- `decline`: User explicitly said no
- `cancel`: User dismissed without choosing (closed dialog, pressed Escape)

---

## 6. Utilities

### 6.1 Progress

Optional tracking for long-running operations.

- Requester includes `progressToken` (string or integer, unique across active requests) in `_meta`.
- Receiver sends `notifications/progress` with `progressToken`, `progress` (must increase), optional `total`, optional `message`.
- Stop notifications after completion.
- Both parties SHOULD rate-limit and track active tokens.

### 6.2 Cancellation

Either side can cancel in-progress requests via `notifications/cancelled`.

- Includes `requestId` and optional `reason`.
- `initialize` MUST NOT be cancelled.
- Receiver SHOULD stop work and free resources. MAY ignore if already completed.
- Invalid/unknown request IDs silently ignored.
- Both sides must handle race conditions (cancellation arrives after response sent).
- For tasks: use `tasks/cancel` instead.

### 6.3 Logging

Servers emit structured log messages via `notifications/message`.

#### Capability

```json
{ "capabilities": { "logging": {} } }
```

#### Log Levels (RFC 5424 syslog severity)

`debug` < `info` < `notice` < `warning` < `error` < `critical` < `alert` < `emergency`

#### Protocol

- Client sets minimum level: `logging/setLevel` (e.g., `"info"`)
- Server sends: `notifications/message` with `level`, `logger` (name), `data` (arbitrary JSON)
- Log messages MUST NOT contain credentials, PII, or exploitable system details.

### 6.4 Completion

Autocompletion for prompt arguments and resource template URIs.

#### Capability

```json
{ "capabilities": { "completions": {} } }
```

#### Protocol

- Client sends `completion/complete` with:
  - `ref`: `{ type: "ref/prompt", name }` or `{ type: "ref/resource", uri }`
  - `argument`: `{ name, value }` (current partial input)
  - `context.arguments`: previously completed argument values (for multi-arg prompts)
- Server returns `completion.values` (max 100, ranked by relevance), `total`, `hasMore`.

#### Best Practices

- Servers: sort by relevance, implement fuzzy matching, rate limit, validate inputs.
- Clients: debounce requests, cache results.

### 6.5 Pagination

Cursor-based pagination for list operations.

- Server includes `nextCursor` in response if more results exist.
- Client sends `cursor` in next request to continue.
- Cursors are **opaque tokens** — clients MUST NOT parse, modify, or persist them across sessions.
- Page size determined by server.

Paginated operations: `resources/list`, `resources/templates/list`, `prompts/list`, `tools/list`, `tasks/list`.

### 6.6 Tasks (Experimental — new in 2025-11-25)

Durable state machines for long-running requests. Enable polling and deferred result retrieval.

#### Roles

- **Requestor**: sender of task-augmented request (client or server)
- **Receiver**: executor of the task (client or server)

#### Capability

```json
// Server
{ "capabilities": { "tasks": { "list": {}, "cancel": {}, "requests": { "tools": { "call": {} } } } } }

// Client
{ "capabilities": { "tasks": { "list": {}, "cancel": {}, "requests": { "sampling": { "createMessage": {} }, "elicitation": { "create": {} } } } } }
```

#### Tool-Level Negotiation

Tools declare `execution.taskSupport`:
- `"forbidden"` (default): MUST NOT use task augmentation
- `"optional"`: MAY use tasks or normal requests
- `"required"`: MUST use tasks; server returns `-32601` otherwise

#### Protocol

| Method | Description |
|--------|-------------|
| `tools/call` (+ `task` param) | Create task-augmented request |
| `tasks/get` | Poll task status |
| `tasks/result` | Retrieve final result (blocks until terminal) |
| `tasks/list` | List tasks (paginated) |
| `tasks/cancel` | Cancel a task |
| `notifications/tasks/status` | Optional status change notification |

#### Creating a Task

Include `task: { ttl: 60000 }` in request params. Server returns `CreateTaskResult` with task metadata (NOT the operation result). Optional `io.modelcontextprotocol/model-immediate-response` in `_meta` for immediate model feedback.

#### Task Status Lifecycle

```
working → input_required → working → completed
       → completed
       → failed
       → cancelled
```

Terminal states: `completed`, `failed`, `cancelled`. Once terminal, MUST NOT transition.

#### Getting Results

- `tasks/result` blocks until terminal status, then returns what the underlying request would have returned.
- For `completed`: the actual operation result.
- For `failed`: JSON-RPC error or result with `isError: true`.

#### Polling

- Respect `pollInterval` (milliseconds) from `tasks/get` responses.
- Continue polling via `tasks/get` even after calling `tasks/result`.
- DO NOT rely on `notifications/tasks/status` — it's optional.

#### TTL and Cleanup

- `createdAt` and `lastUpdatedAt`: ISO 8601 timestamps in all task responses.
- `ttl`: milliseconds from creation before receiver MAY delete task and results.
- Receiver MAY override requested TTL.

#### Related Task Metadata

All task-related messages MUST include `io.modelcontextprotocol/related-task: { taskId }` in `_meta`. Exception: `tasks/get`, `tasks/list`, `tasks/cancel` use the `taskId` parameter directly.

#### Security

- Task IDs MUST be cryptographically secure with sufficient entropy.
- Bind tasks to authorization context when available.
- Reject cross-context access to tasks.
- Enforce limits on concurrent tasks per requestor.
- Enforce maximum TTL durations.
- Log task lifecycle events.

### 6.7 Ping

- Either side sends `ping` request (no params).
- Receiver MUST respond promptly with `{}`.
- Used for connection health checks.
- Frequency SHOULD be configurable. Avoid excessive pinging.

---

## 7. Notifications

All notifications are **one-way JSON-RPC messages** — no `id` field, no response expected. Fire and forget. However, notifications **can carry arbitrary data** in their `params` field. They fall into two categories:

### 7.1 Signal-Only Notifications (no data payload)

Pure "something changed" signals with **no `params`**. Client/server must re-fetch to learn what changed.

| Notification | Direction |
|---|---|
| `notifications/initialized` | Client → Server |
| `notifications/tools/list_changed` | Server → Client |
| `notifications/resources/list_changed` | Server → Client |
| `notifications/prompts/list_changed` | Server → Client |
| `notifications/roots/list_changed` | Client → Server |

These are deliberately minimal — they trigger a re-fetch of the corresponding list (`tools/list`, `resources/list`, etc.). The notification itself carries zero information about what was added, removed, or modified.

### 7.2 Data-Carrying Notifications (structured `params`)

These include structured data in `params`. Some carry identifiers only; others carry rich, arbitrary payloads.

| Notification | Direction | `params` contents |
|---|---|---|
| `notifications/resources/updated` | Server → Client | `{ uri }` — identifier only; client must re-read |
| `notifications/cancelled` | Either | `{ requestId, reason? }` |
| `notifications/progress` | Either | `{ progressToken, progress, total?, message? }` — numeric progress + human-readable message |
| `notifications/message` | Server → Client | `{ level, logger?, data }` — **`data` is arbitrary JSON** (full structured log entry) |
| `notifications/tasks/status` | Receiver → Requestor | **Full `Task` object**: `{ taskId, status, statusMessage?, createdAt, lastUpdatedAt, ttl, pollInterval? }` |
| `notifications/elicitation/complete` | Server → Client | `{ elicitationId }` — identifier only |

#### Detail: `notifications/message` (Logging)

The richest notification. `data` field accepts **any JSON-serializable value** — objects, arrays, nested structures:

```jsonc
{
  "method": "notifications/message",
  "params": {
    "level": "error",
    "logger": "database",
    "data": {
      "error": "Connection failed",
      "details": { "host": "localhost", "port": 5432, "retries": 3 }
    }
  }
}
```

#### Detail: `notifications/tasks/status`

Carries the **complete task state** — no need for a follow-up `tasks/get`:

```jsonc
{
  "method": "notifications/tasks/status",
  "params": {
    "taskId": "786512e2-...",
    "status": "completed",
    "statusMessage": "Weather data retrieved.",
    "createdAt": "2025-11-25T10:30:00Z",
    "lastUpdatedAt": "2025-11-25T10:50:00Z",
    "ttl": 60000
  }
}
```

However, this notification is **optional** — requestors MUST NOT rely on receiving it and SHOULD continue polling via `tasks/get`.

#### Detail: `notifications/progress`

Incremental updates with monotonically increasing `progress` value:

```jsonc
{
  "method": "notifications/progress",
  "params": {
    "progressToken": "abc123",
    "progress": 50,
    "total": 100,
    "message": "Processing records..."
  }
}
```

`total` and `message` are optional. `progress` MUST increase with each notification.

### 7.3 Key Rules

- After `list_changed`: always re-fetch the full list. Don't assume what changed.
- After `resources/updated`: re-read the resource. Notification only says *that* it changed, not *how*.
- `notifications/message` can carry **arbitrarily complex JSON** in `data`.
- `notifications/tasks/status` carries full task state but is **optional** — don't rely on it.
- Rate-limit progress notifications to prevent flooding.
- Handle cancellation race conditions gracefully (may arrive after response sent).
- Ignore unknown notification types for forward compatibility.

---

## 8. Resources vs. Tools — When to Use Which

| Dimension | Resources | Tools |
|---|---|---|
| **Nature** | Data/context (read-only) | Executable functions (side effects) |
| **Controlled by** | Application/host | LLM/model |
| **Interaction** | `list` → `read` | `list` → `call` with arguments |
| **Side effects** | None | Yes |
| **Identified by** | URI | Name string |
| **Parameters** | URI templates | `inputSchema` (JSON Schema) |
| **Subscriptions** | Yes (`resources/subscribe`) | No |
| **Human confirmation** | Not typically needed | SHOULD have human-in-the-loop |

**Use resources for**: database schemas, documentation, config files, project structure — anything the host decides to include as context.

**Use tools for**: running queries, calling APIs, sending notifications, computations — anything with arguments and/or side effects that the LLM should reason about.

**Bridge**: Tools can return `resource_link` or embedded `resource` in results, creating references that can be independently tracked via `resources/read` and `resources/subscribe`.

---

## 9. Authorization

MCP supports OAuth 2.1-based authorization for HTTP transports.

### Key Points

- Servers MAY require authorization. Clients discover auth requirements via:
  - `WWW-Authenticate` header with `resource_metadata` URL
  - Protected Resource Metadata (RFC 9728) at `.well-known` endpoint
  - OpenID Connect Discovery 1.0
- Dynamic client registration supported.
- OAuth Client ID Metadata Documents recommended for client registration.
- Incremental scope consent via `WWW-Authenticate` challenges.
- Token passthrough is **explicitly forbidden** — servers MUST NOT accept tokens not issued to them.
- Third-party API authorization uses URL-mode elicitation (not client's MCP token).

---

## 10. Security Best Practices

### Trust & Safety Principles

1. **User Consent**: Users must explicitly consent to data access and tool invocation.
2. **Data Privacy**: Hosts must not transmit resource data without user consent.
3. **Tool Safety**: Tool annotations are untrusted unless from trusted server. Human-in-the-loop for all tool invocations.
4. **Sampling Controls**: Users approve all sampling requests and control what the server sees.

### Attack Vectors & Mitigations

| Attack | Mitigation |
|--------|-----------|
| **Confused Deputy** | Per-client consent before third-party OAuth flows. Validate `redirect_uri` exactly. CSRF protection via `state` parameter. |
| **Token Passthrough** | Servers MUST NOT accept tokens not explicitly issued for them. |
| **SSRF** | Enforce HTTPS. Block private IP ranges. Validate redirect targets. Use egress proxies. Pin DNS resolution. |
| **Session Hijacking** | Cryptographically secure session IDs. Bind sessions to user identity. Verify auth on all requests. Never use sessions for authentication alone. |
| **Local Server Compromise** | Show exact commands before execution. Sandbox with minimal privileges. Use stdio transport for local servers. |
| **Scope Over-Grant** | Minimal initial scopes. Incremental elevation via `WWW-Authenticate`. Accept reduced scope tokens. |

### Server Security Checklist

- Validate all inputs (tool arguments, resource URIs, prompt arguments).
- Implement access controls and rate limiting.
- Sanitize outputs — no credentials, PII, or exploitable details in logs.
- Validate `Origin` header on HTTP connections.
- Bind to localhost for local servers.
- Use secure session IDs; never use sessions as sole authentication.

### Client Security Checklist

- Prompt user for confirmation on tool invocations and sampling requests.
- Show tool inputs before sending to server.
- Validate tool results before passing to LLM.
- Implement request timeouts.
- Log tool usage for audit.
- Block private IP ranges during OAuth discovery (SSRF prevention).
- Do not auto-fetch elicitation URLs.

---

## 11. JSON-RPC Error Codes

| Code | Meaning | Used By |
|------|---------|---------|
| `-1` | User rejected (sampling) | Client |
| `-32002` | Resource not found | Server |
| `-32042` | URL elicitation required | Server |
| `-32600` | Invalid request | Either |
| `-32601` | Method not found | Either |
| `-32602` | Invalid params (unknown tool, missing args, invalid cursor, invalid task ID) | Either |
| `-32603` | Internal error | Either |

---

## 12. Key Changes in 2025-11-25

**Major:**
- URL-mode elicitation (OAuth flows, sensitive data via external URLs)
- Tool calling in sampling (`tools` + `toolChoice` params)
- Tasks (experimental) — durable state machines for long-running operations
- OAuth enhancements (OpenID Connect Discovery, incremental scope consent, Client ID Metadata Documents)
- Icons for tools, resources, resource templates, prompts
- Tool name guidance formalized
- Elicitation schema: titled/untitled enums, single/multi-select, default values

**Minor:**
- SSE stream polling (server can disconnect, client reconnects)
- Resumption always via GET with `Last-Event-ID`
- JSON Schema 2020-12 as default dialect
- Input validation errors → tool execution errors (enables model self-correction)
- Clarified stderr usage in stdio
- Updated security best practices (SSRF, session hijacking, scope minimization)

---

## Appendix: Quick Protocol Message Reference

### Server Features

```
prompts/list          → { prompts[], nextCursor? }
prompts/get           → { description?, messages[] }
resources/list        → { resources[], nextCursor? }
resources/read        → { contents[] }
resources/templates/list → { resourceTemplates[], nextCursor? }
resources/subscribe   → {}
resources/unsubscribe → {}
tools/list            → { tools[], nextCursor? }
tools/call            → { content[], structuredContent?, isError? }
completion/complete   → { completion: { values[], total?, hasMore } }
logging/setLevel      → {}
```

### Client Features

```
sampling/createMessage → { role, content, model, stopReason }
roots/list             → { roots[] }
elicitation/create     → { action, content? }
```

### Utilities

```
ping                  → {}
tasks/get             → Task
tasks/result          → (original request result type)
tasks/list            → { tasks[], nextCursor? }
tasks/cancel          → Task
```

### Notifications (no response)

```
notifications/initialized
notifications/cancelled
notifications/progress
notifications/message
notifications/tools/list_changed
notifications/resources/list_changed
notifications/resources/updated
notifications/prompts/list_changed
notifications/roots/list_changed
notifications/tasks/status
notifications/elicitation/complete
```
