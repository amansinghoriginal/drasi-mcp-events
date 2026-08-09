//! `drasi-agent`: standalone event-driven MCP agent — the events × tools loop.
//!
//! One process, one MCP server, the full loop the draft Events extension
//! exists to enable:
//!
//! ```text
//! events/stream (draft extension) ──wakes──▶ agent ──reasons──▶ tools/* (MCP core, official SDK)
//!                                              │                  get_order
//!                                              │                  get_customer_history
//!                                              ▼                  flag_order
//!                                         terminal log
//! ```
//!
//! * The **tools** side is the **official Rust SDK** (rmcp client, 2026-07-28
//!   `server/discover` lifecycle — no initialize handshake).
//! * The **events** side speaks the draft extension directly
//!   (`events/stream` with SEP-2243 headers + SEP-2575 `_meta`), because no
//!   SDK ships it yet — that asymmetry is the point of the prototype.
//! * With `ANTHROPIC_API_KEY` set, a Claude tool-use loop decides what to do.
//!   Without it, a deterministic policy performs the *same* tool calls, so the
//!   wake→verify→act shape is identical either way.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context as _, Result};
use clap::Parser;
use futures::StreamExt as _;
use mcp_events_client::{load_state, save_state, CursorState, EventsClient, StreamFrame};
use mcp_events_wire as wire;
use rmcp::model::{
    CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation, ProtocolVersion,
};
use rmcp::service::{ClientLifecycleMode, ClientServiceExt as _};
use rmcp::transport::StreamableHttpClientTransport;
use serde_json::{json, Map, Value};

const MAX_BRAIN_TURNS: usize = 8;
/// Stream considered dead after this long without any frame (sketch: twice the
/// heartbeat interval; the server's SHOULD is <= 30 s, so 60 s).
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// A poison event is skipped (with a loud log) after this many failed attempts.
const MAX_EVENT_ATTEMPTS: u32 = 3;
const ANTHROPIC_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// LLM connection resolved from the environment (optionally seeded from an
/// untracked `.env`). Two wire dialects cover every deployment we care about:
/// `anthropic` (api.anthropic.com, Claude in Microsoft Foundry) and `openai`
/// (Azure OpenAI / any OpenAI-compatible chat-completions endpoint).
#[derive(Clone, Debug, PartialEq)]
struct LlmConfig {
    provider: Provider,
    /// Full URL to POST (for Azure, paste the portal's full endpoint —
    /// including any `?api-version=` — so URL-shape differences never matter).
    chat_url: String,
    api_key: String,
    model: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Provider {
    Anthropic,
    OpenAi,
}

impl LlmConfig {
    /// Resolution order: explicit `LLM_*` variables win; a bare
    /// `ANTHROPIC_API_KEY` keeps working as the zero-config path.
    fn from_env(default_model: &str) -> Option<Self> {
        let get = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        let provider = match get("LLM_PROVIDER").as_deref() {
            Some("openai") => Provider::OpenAi,
            Some("anthropic") | None => Provider::Anthropic,
            Some(other) => {
                tracing::warn!(provider = other, "unknown LLM_PROVIDER; expected anthropic|openai");
                return None;
            }
        };
        let api_key = get("LLM_API_KEY").or_else(|| get("ANTHROPIC_API_KEY"))?;
        let chat_url = get("LLM_CHAT_URL").unwrap_or_else(|| ANTHROPIC_URL.to_owned());
        if provider == Provider::OpenAi && chat_url == ANTHROPIC_URL {
            tracing::warn!("LLM_PROVIDER=openai requires LLM_CHAT_URL");
            return None;
        }
        let model = get("LLM_MODEL").unwrap_or_else(|| default_model.to_owned());
        Some(Self { provider, chat_url, api_key, model })
    }
}

/// Loads KEY=VALUE lines from an env file into the process environment.
/// Real environment variables win over file entries; `#` comments and blank
/// lines are skipped; surrounding single/double quotes are stripped.
fn load_env_file(path: &std::path::Path) -> usize {
    let Ok(content) = std::fs::read_to_string(path) else {
        return 0;
    };
    let mut loaded = 0;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim().trim_matches('"').trim_matches('\'');
        if !key.is_empty() && std::env::var_os(key).is_none() {
            std::env::set_var(key, value);
            loaded += 1;
        }
    }
    loaded
}

const SYSTEM_PROMPT: &str = "You are an autonomous order-review agent. You are woken only when \
the high-value-orders continuous query changes (a row entered, changed within, or left the \
result set). You are not running continuously.\n\
For each change: verify current state with get_order, judge whether the order is routine or \
anomalous for this customer with get_customer_history, and if it warrants human review call \
flag_order with a concise reason. Routine changes and rows leaving the result set (changeType \
\"deleted\") normally need no action. Never flag the same situation twice — check priorFlags. \
End with one sentence: what happened and what you did.";

#[derive(Debug, Parser)]
#[command(name = "drasi-agent", about = "Event-driven MCP agent: events wake it, tools act")]
struct Cli {
    /// MCP server endpoint (Streamable HTTP).
    #[arg(long, default_value = "http://127.0.0.1:8090/mcp")]
    server: String,
    /// Event type to subscribe to.
    #[arg(long, default_value = "high-value-orders.changed")]
    event: String,
    /// auto = llm when credentials are configured (env or .env), else policy.
    #[arg(long, default_value = "auto", value_parser = ["auto", "llm", "claude", "policy"])]
    mode: String,
    /// Model for llm mode (overridden by LLM_MODEL).
    #[arg(long, default_value = "claude-sonnet-5")]
    model: String,
    /// Untracked env file with LLM connection details (LLM_PROVIDER,
    /// LLM_CHAT_URL, LLM_API_KEY, LLM_MODEL). Real env vars win.
    #[arg(long, default_value = ".env")]
    env_file: PathBuf,
    /// Persists {"cursor": ...} between runs (durable replay across restarts).
    #[arg(long, default_value = "/tmp/drasi-agent-cursor.json")]
    state_file: PathBuf,
}

fn log(msg: impl AsRef<str>) {
    println!("[{}] {}", chrono::Local::now().format("%H:%M:%S"), msg.as_ref());
}

fn log2(msg: impl AsRef<str>) {
    log(format!("  {}", msg.as_ref()));
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let loaded = load_env_file(&cli.env_file);
    if loaded > 0 {
        log(format!("loaded {loaded} variable(s) from {}", cli.env_file.display()));
    }
    let llm = LlmConfig::from_env(&cli.model);
    let mode = match (cli.mode.as_str(), &llm) {
        ("auto", Some(_)) => "llm",
        ("auto", None) => "policy",
        ("llm" | "claude", None) => anyhow::bail!(
            "--mode llm requires credentials: set ANTHROPIC_API_KEY, or LLM_PROVIDER/\
             LLM_CHAT_URL/LLM_API_KEY (directly or in {})",
            cli.env_file.display()
        ),
        ("llm" | "claude", Some(_)) => "llm",
        (m, _) => m,
    };

    // Events-extension view of the server: stateless discover (no handshake).
    let events_client = EventsClient::new(cli.server.clone());
    let discovered = events_client.discover().await.context("server/discover")?;
    let info = &discovered["_meta"][wire::META_SERVER_INFO];
    let has_ext = discovered["capabilities"]["extensions"]
        .get("io.modelcontextprotocol/events")
        .is_some();
    log(format!(
        "discovered {} v{} (stateless {}, no handshake) — events extension: {}",
        info["name"].as_str().unwrap_or("?"),
        info["version"].as_str().unwrap_or("?"),
        wire::PROTOCOL_VERSION_2026_07_28,
        if has_ext { "yes" } else { "MISSING" },
    ));

    // Tools via the official SDK, server/discover lifecycle (SEP-2575).
    let transport = StreamableHttpClientTransport::from_uri(cli.server.clone());
    let client_info = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::new("drasi-agent", env!("CARGO_PKG_VERSION")),
    );
    let mcp = client_info
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .context("connecting rmcp client (discover lifecycle)")?;
    let listed = mcp.list_tools(Default::default()).await?;
    let tool_defs: Vec<Value> = listed
        .tools
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description.as_deref().unwrap_or(""),
                "input_schema": t.input_schema,
            })
        })
        .collect();
    log2(format!(
        "tools (official Rust SDK): {}",
        listed
            .tools
            .iter()
            .map(|t| t.name.as_ref())
            .collect::<Vec<_>>()
            .join(", ")
    ));
    log(format!(
        "brain: {mode}{}",
        match (&llm, mode) {
            (Some(cfg), "llm") => format!(
                " ({:?} dialect, model {}, {})",
                cfg.provider,
                cfg.model,
                if cfg.chat_url == ANTHROPIC_URL { "api.anthropic.com" } else { "custom endpoint" }
            ),
            _ => " (deterministic)".to_string(),
        }
    ));

    let http = reqwest::Client::new();
    let mut cursor = load_state(&cli.state_file)?.unwrap_or_default().cursor;
    if cursor.is_some() {
        log(format!("resuming from persisted cursor {cursor:?}"));
    }
    let mut backoff = Duration::from_secs(1);
    // (event_id, attempt count) for the event currently failing, if any.
    let mut failures: Option<(String, u32)> = None;
    log(format!("subscribing to {} via events/stream …", cli.event));

    loop {
        let params = wire::StreamEventsParams {
            name: cli.event.clone(),
            params: None,
            cursor: cursor.clone(),
            max_age_ms: None,
        };
        let mut stream = match events_client.stream(&params).await {
            Ok(s) => s,
            Err(error) => {
                log(format!("stream open failed ({error:#}); retrying in {backoff:?}"));
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        loop {
            // Heartbeat watchdog: the stream deliberately has no request
            // timeout, so heartbeats are the only liveness signal. The sketch
            // says a client seeing neither an event nor a heartbeat for twice
            // the heartbeat interval should treat the stream as dead.
            let frame = match tokio::time::timeout(IDLE_TIMEOUT, stream.next()).await {
                Err(_) => {
                    log(format!(
                        "no frames for {IDLE_TIMEOUT:?} — treating stream as dead; \
                         reconnecting with cursor {cursor:?}"
                    ));
                    break;
                }
                Ok(None) => break,
                Ok(Some(frame)) => frame,
            };
            match frame {
                Ok(StreamFrame::Active(active)) => {
                    backoff = Duration::from_secs(1);
                    if active.truncated {
                        log("stream active with truncated=true — events were skipped; \
                             tools remain the source of truth");
                    } else {
                        log(format!("stream active (cursor {:?})", active.cursor));
                    }
                    persist(&cli.state_file, &mut cursor, active.cursor);
                }
                Ok(StreamFrame::Event(event)) => {
                    let new_cursor = event.cursor.clone().flatten();
                    match handle_event(&event, mode, llm.as_ref(), &mcp, &tool_defs, &http).await {
                        Ok(verdict) => {
                            log2(format!("agent: {verdict}"));
                            log("agent idle — waiting for next event");
                            failures = None;
                            persist(&cli.state_file, &mut cursor, new_cursor);
                        }
                        Err(error) => {
                            // At-least-once: do NOT persist the cursor — break so
                            // the reconnect replays this event — unless the same
                            // event has now failed MAX_EVENT_ATTEMPTS times
                            // (poison event: skip it loudly instead of looping).
                            let attempts = match &mut failures {
                                Some((id, n)) if *id == event.event_id => {
                                    *n += 1;
                                    *n
                                }
                                _ => {
                                    failures = Some((event.event_id.clone(), 1));
                                    1
                                }
                            };
                            if attempts >= MAX_EVENT_ATTEMPTS {
                                log(format!(
                                    "agent error handling event {} ({error:#}); giving up \
                                     after {attempts} attempts and SKIPPING it — review manually",
                                    event.event_id
                                ));
                                failures = None;
                                persist(&cli.state_file, &mut cursor, new_cursor);
                            } else {
                                log(format!(
                                    "agent error handling event {} ({error:#}); reconnecting \
                                     to retry (attempt {attempts}/{MAX_EVENT_ATTEMPTS})",
                                    event.event_id
                                ));
                                break;
                            }
                        }
                    }
                }
                Ok(StreamFrame::Heartbeat(hb)) => {
                    persist(&cli.state_file, &mut cursor, hb.cursor);
                }
                Ok(StreamFrame::Terminated(t)) => {
                    log(format!("subscription terminated by server: {:?}", t.error));
                    return Ok(());
                }
                Ok(StreamFrame::Error(e)) => {
                    log2(format!("transient upstream error (stream stays open): {:?}", e.error));
                }
                Ok(StreamFrame::Result) => {
                    log("server closed the stream gracefully; reconnecting");
                    break;
                }
                Err(error) => {
                    log(format!("stream error ({error:#}); reconnecting with cursor {cursor:?}"));
                    break;
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn persist(path: &PathBuf, cursor: &mut Option<String>, new: Option<String>) {
    if let Some(c) = new {
        *cursor = Some(c.clone());
        if let Err(error) = save_state(path, &CursorState { cursor: Some(c) }) {
            tracing::warn!(%error, "failed to persist cursor");
        }
    }
}

async fn handle_event(
    event: &wire::EventOccurrence,
    mode: &str,
    llm: Option<&LlmConfig>,
    mcp: &impl ToolCaller,
    tool_defs: &[Value],
    http: &reqwest::Client,
) -> Result<String> {
    let data = &event.data;
    let row = data.get("after").or_else(|| data.get("before")).cloned().unwrap_or(Value::Null);
    log(format!(
        "EVENT {} order {} ({}, ${}) — waking agent",
        data.get("changeType").and_then(Value::as_str).unwrap_or("?").to_uppercase(),
        row.get("id").and_then(Value::as_i64).unwrap_or(-1),
        row.get("customer").and_then(Value::as_str).unwrap_or("?"),
        row.get("total").and_then(Value::as_f64).unwrap_or(0.0),
    ));
    match (mode, llm) {
        ("llm", Some(cfg)) => match cfg.provider {
            Provider::Anthropic => anthropic_brain(event, cfg, mcp, tool_defs, http).await,
            Provider::OpenAi => openai_brain(event, cfg, mcp, tool_defs, http).await,
        },
        _ => policy_brain(event, mcp).await,
    }
}

/// Narrow interface over the rmcp client so brains are testable.
trait ToolCaller: Sync {
    fn call(&self, name: &str, args: Value) -> impl std::future::Future<Output = Result<Value>> + Send;
}

impl<S> ToolCaller for rmcp::service::RunningService<rmcp::RoleClient, S>
where
    S: rmcp::Service<rmcp::RoleClient>,
{
    async fn call(&self, name: &str, args: Value) -> Result<Value> {
        log2(format!("tool → {name}({args})"));
        let arguments: Map<String, Value> = serde_json::from_value(args)?;
        let result = self
            .call_tool(CallToolRequestParams::new(name.to_owned()).with_arguments(arguments))
            .await?;
        let payload = if result.is_error.unwrap_or(false) {
            json!({"error": text_of(&result.content)})
        } else if let Some(structured) = result.structured_content {
            structured
        } else {
            let text = text_of(&result.content);
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        let shown = payload.to_string();
        log2(format!("  ← {}", truncate_at_char_boundary(&shown, 200)));
        Ok(payload)
    }
}

/// Byte-bounded truncation that never slices inside a multi-byte character
/// (a plain `&s[..max]` panics when `max` lands mid-character — and tool
/// payloads contain non-ASCII like `×` and `—`).
fn truncate_at_char_boundary(s: &str, max: usize) -> &str {
    let mut end = s.len().min(max);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn text_of(content: &[rmcp::model::ContentBlock]) -> String {
    content
        .iter()
        .filter_map(|block| {
            serde_json::to_value(block)
                .ok()
                .and_then(|v| v.get("text").and_then(Value::as_str).map(str::to_owned))
        })
        .collect::<Vec<_>>()
        .join("")
}

/// No-key fallback: the same wake→verify→act loop, deterministic rules.
async fn policy_brain(event: &wire::EventOccurrence, mcp: &impl ToolCaller) -> Result<String> {
    let data = &event.data;
    let change = data.get("changeType").and_then(Value::as_str).unwrap_or("?");
    let row = data.get("after").or_else(|| data.get("before")).cloned().unwrap_or(Value::Null);
    let order_id = row.get("id").and_then(Value::as_i64).unwrap_or(-1);
    let customer = row.get("customer").and_then(Value::as_str).unwrap_or("?").to_owned();
    if change == "deleted" {
        return Ok(format!(
            "Order {order_id} ({customer}) left the high-value set; no action needed."
        ));
    }
    let order = mcp.call("get_order", json!({"order_id": order_id})).await?;
    if order.is_null() || order.get("error").is_some() {
        return Ok(format!("Order {order_id} no longer exists; nothing to review."));
    }
    let history = mcp
        .call("get_customer_history", json!({"customer": customer}))
        .await?;
    let total = order.get("total").and_then(Value::as_f64).unwrap_or(0.0);
    let empty = vec![];
    let orders = history.get("orders").and_then(Value::as_array).unwrap_or(&empty);
    let prior_flags = history.get("priorFlags").and_then(Value::as_array).unwrap_or(&empty);
    if prior_flags
        .iter()
        .any(|f| f.get("orderId").and_then(Value::as_i64) == Some(order_id))
    {
        return Ok(format!("Order {order_id} is already flagged; not flagging again."));
    }
    let others: Vec<f64> = orders
        .iter()
        .filter(|o| o.get("id").and_then(Value::as_i64) != Some(order_id))
        .filter_map(|o| o.get("total").and_then(Value::as_f64))
        .collect();
    let mut reasons: VecDeque<String> = VecDeque::new();
    if change == "updated" {
        // Judge an update by its before→after jump, not by order history.
        let before = data
            .get("before")
            .and_then(|b| b.get("total"))
            .and_then(Value::as_f64)
            .unwrap_or(total);
        if before > 0.0 && total >= 3.0 * before {
            reasons.push_back(format!(
                "total jumped {:.1}× in one update (${before:.0} → ${total:.0})",
                total / before
            ));
        }
    } else if others.is_empty() {
        reasons.push_back(format!("first-ever order for {customer} at ${total:.0}"));
    } else {
        let peak = others.iter().cloned().fold(f64::MIN, f64::max);
        if total >= 3.0 * peak {
            reasons.push_back(format!(
                "${total:.0} is ≥3× {customer}'s previous peak (${peak:.0})"
            ));
        }
    }
    // Corroborating signal only: prior flags strengthen an existing anomaly
    // reason but never trigger a flag by themselves — otherwise one flag would
    // taint every future routine order for that customer.
    if !reasons.is_empty() && !prior_flags.is_empty() {
        reasons.push_back(format!("customer has {} prior flag(s)", prior_flags.len()));
    }
    if reasons.is_empty() {
        return Ok(format!(
            "Order {order_id} ({customer}, ${total:.0}) looks routine for this customer; \
             no action taken."
        ));
    }
    let reason = reasons.into_iter().collect::<Vec<_>>().join("; ");
    let flag = mcp
        .call("flag_order", json!({"order_id": order_id, "reason": reason}))
        .await?;
    Ok(format!(
        "Flagged order {order_id} for review (flag #{}): {reason}.",
        flag.get("flagId").and_then(Value::as_i64).unwrap_or(-1)
    ))
}

/// Claude tool-use loop over the raw Messages API dialect (api.anthropic.com
/// or an Anthropic-compatible endpoint such as Claude in Microsoft Foundry).
async fn anthropic_brain(
    event: &wire::EventOccurrence,
    cfg: &LlmConfig,
    mcp: &impl ToolCaller,
    tool_defs: &[Value],
    http: &reqwest::Client,
) -> Result<String> {
    let mut messages = vec![json!({
        "role": "user",
        "content": format!(
            "A watched-query change event just arrived:\n{}",
            serde_json::to_string_pretty(&serde_json::to_value(event)?)?
        ),
    })];
    for _ in 0..MAX_BRAIN_TURNS {
        let resp = http
            .post(&cfg.chat_url)
            // x-api-key is the Anthropic header; api-key is what Azure-hosted
            // gateways expect. Sending both is harmless either way.
            .header("x-api-key", &cfg.api_key)
            .header("api-key", &cfg.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&json!({
                "model": cfg.model,
                // Roomy cap: on Claude 5 models adaptive thinking is on by
                // default and max_tokens bounds thinking + text + tool_use
                // together — a tight cap truncates turns mid-thought.
                "max_tokens": 8192,
                "system": SYSTEM_PROMPT,
                "tools": tool_defs,
                "messages": messages,
            }))
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            anyhow::bail!("anthropic api {status}: {body}");
        }
        let response: Value = serde_json::from_str(&body)?;
        let content = response["content"].as_array().cloned().unwrap_or_default();
        let text = content
            .iter()
            .filter_map(|b| (b["type"] == "text").then(|| b["text"].as_str().unwrap_or("")))
            .collect::<Vec<_>>()
            .join("");
        match response["stop_reason"].as_str() {
            Some("tool_use") => {}
            Some("end_turn") | Some("stop_sequence") => return Ok(text),
            // max_tokens, refusal, … must surface as errors, not silently
            // pass for a final answer with the actions dropped.
            other => anyhow::bail!(
                "unexpected stop_reason {other:?} from the model (partial text: {text:?})"
            ),
        }
        messages.push(json!({"role": "assistant", "content": content}));
        let mut results = vec![];
        for block in &content {
            if block["type"] == "tool_use" {
                let payload = mcp
                    .call(
                        block["name"].as_str().unwrap_or(""),
                        block["input"].clone(),
                    )
                    .await
                    .unwrap_or_else(|e| json!({"error": e.to_string()}));
                results.push(json!({
                    "type": "tool_result",
                    "tool_use_id": block["id"],
                    "content": payload.to_string(),
                }));
            }
        }
        messages.push(json!({"role": "user", "content": results}));
    }
    Ok("(stopped: exceeded max tool-use turns)".to_owned())
}

/// Converts Anthropic-shaped tool defs ({name, description, input_schema})
/// to OpenAI chat-completions function tools.
fn openai_tool_defs(tool_defs: &[Value]) -> Vec<Value> {
    tool_defs
        .iter()
        .map(|d| {
            json!({
                "type": "function",
                "function": {
                    "name": d["name"],
                    "description": d["description"],
                    "parameters": d["input_schema"],
                }
            })
        })
        .collect()
}

/// Tool-use loop over the OpenAI chat-completions dialect (Azure OpenAI or
/// any OpenAI-compatible endpoint). `cfg.chat_url` is the full URL to POST —
/// for Azure, paste the deployment endpoint including `?api-version=`.
async fn openai_brain(
    event: &wire::EventOccurrence,
    cfg: &LlmConfig,
    mcp: &impl ToolCaller,
    tool_defs: &[Value],
    http: &reqwest::Client,
) -> Result<String> {
    let tools = openai_tool_defs(tool_defs);
    let mut messages = vec![
        json!({"role": "system", "content": SYSTEM_PROMPT}),
        json!({
            "role": "user",
            "content": format!(
                "A watched-query change event just arrived:\n{}",
                serde_json::to_string_pretty(&serde_json::to_value(event)?)?
            ),
        }),
    ];
    for _ in 0..MAX_BRAIN_TURNS {
        let resp = http
            .post(&cfg.chat_url)
            // api-key is Azure's header; Authorization covers OpenAI-compatible
            // endpoints that expect a bearer token. Sending both is harmless.
            .header("api-key", &cfg.api_key)
            .header("authorization", format!("Bearer {}", cfg.api_key))
            .json(&json!({
                "model": cfg.model,
                "messages": messages,
                "tools": tools,
            }))
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            anyhow::bail!("llm api {status}: {body}");
        }
        let response: Value = serde_json::from_str(&body)?;
        let message = response["choices"][0]["message"].clone();
        let finish = response["choices"][0]["finish_reason"].as_str().unwrap_or("");
        let tool_calls = message["tool_calls"].as_array().cloned().unwrap_or_default();
        if tool_calls.is_empty() {
            let text = message["content"].as_str().unwrap_or("").to_owned();
            return match finish {
                "stop" => Ok(text),
                other => anyhow::bail!(
                    "unexpected finish_reason {other:?} from the model (partial text: {text:?})"
                ),
            };
        }
        messages.push(message.clone());
        for call in &tool_calls {
            let name = call["function"]["name"].as_str().unwrap_or("");
            let args: Value = call["function"]["arguments"]
                .as_str()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_else(|| json!({}));
            let payload = mcp
                .call(name, args)
                .await
                .unwrap_or_else(|e| json!({"error": e.to_string()}));
            messages.push(json!({
                "role": "tool",
                "tool_call_id": call["id"],
                "content": payload.to_string(),
            }));
        }
    }
    Ok("(stopped: exceeded max tool-use turns)".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockTools {
        calls: Mutex<Vec<String>>,
        order_total: f64,
        history: Value,
    }

    impl ToolCaller for MockTools {
        async fn call(&self, name: &str, _args: Value) -> Result<Value> {
            self.calls.lock().unwrap().push(name.to_owned());
            Ok(match name {
                "get_order" => json!({
                    "id": 6, "customer": "ivy", "total": self.order_total, "status": "open"
                }),
                "get_customer_history" => self.history.clone(),
                "flag_order" => json!({"flagId": 9}),
                _ => Value::Null,
            })
        }
    }

    fn added_event(total: f64) -> wire::EventOccurrence {
        serde_json::from_value(json!({
            "eventId": "e1",
            "name": "high-value-orders.changed",
            "timestamp": "2026-08-09T00:00:00Z",
            "data": {
                "changeType": "added",
                "after": {"id": 6, "customer": "ivy", "total": total, "status": "open"}
            },
        }))
        .expect("event")
    }

    fn history_with_flag_on_other_order() -> Value {
        json!({
            "customer": "ivy", "orderCount": 2, "lifetimeTotal": 9200.0,
            "orders": [
                {"id": 5, "total": 7200.0, "status": "open"},
                {"id": 6, "total": 2000.0, "status": "open"}
            ],
            "priorFlags": [
                {"orderId": 5, "reason": "first-ever order", "flaggedBy": "t", "flaggedAt": "x"}
            ],
        })
    }

    #[tokio::test]
    async fn prior_flag_alone_does_not_flag_a_routine_order() {
        // ivy has a flag on order 5; a routine $2000 order (< 3x her $7200
        // peak) must NOT be flagged just because that other flag exists.
        let tools = MockTools {
            calls: Mutex::new(vec![]),
            order_total: 2000.0,
            history: history_with_flag_on_other_order(),
        };
        let verdict = policy_brain(&added_event(2000.0), &tools).await.expect("verdict");
        assert!(verdict.contains("routine"), "unexpected verdict: {verdict}");
        assert!(
            !tools.calls.lock().unwrap().iter().any(|c| c == "flag_order"),
            "routine order was flagged"
        );
    }

    #[test]
    fn env_file_parsing_and_precedence() {
        let dir = std::env::temp_dir().join("drasi-agent-env-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".env");
        std::fs::write(
            &path,
            "# comment\nTEST_ENVFILE_A=hello\nTEST_ENVFILE_B=\"quoted value\"\n\nnot a pair\nTEST_ENVFILE_C=x=y\n",
        )
        .unwrap();
        std::env::set_var("TEST_ENVFILE_A", "real-env-wins");
        let loaded = load_env_file(&path);
        assert_eq!(loaded, 2, "A is preset, B and C load");
        assert_eq!(std::env::var("TEST_ENVFILE_A").unwrap(), "real-env-wins");
        assert_eq!(std::env::var("TEST_ENVFILE_B").unwrap(), "quoted value");
        assert_eq!(std::env::var("TEST_ENVFILE_C").unwrap(), "x=y");
    }

    #[test]
    fn openai_tool_def_conversion() {
        let defs = vec![json!({
            "name": "get_order",
            "description": "d",
            "input_schema": {"type": "object", "properties": {"order_id": {"type": "integer"}}}
        })];
        let out = openai_tool_defs(&defs);
        assert_eq!(out[0]["type"], "function");
        assert_eq!(out[0]["function"]["name"], "get_order");
        assert_eq!(
            out[0]["function"]["parameters"]["properties"]["order_id"]["type"],
            "integer"
        );
    }

    #[tokio::test]
    async fn prior_flag_corroborates_a_real_anomaly() {
        // $25000 is >= 3x the $7200 peak: flag, citing both the anomaly and
        // the prior flag as supporting context.
        let tools = MockTools {
            calls: Mutex::new(vec![]),
            order_total: 25000.0,
            history: history_with_flag_on_other_order(),
        };
        let verdict = policy_brain(&added_event(25000.0), &tools).await.expect("verdict");
        assert!(verdict.contains("Flagged"), "unexpected verdict: {verdict}");
        assert!(verdict.contains("prior flag"), "missing corroboration: {verdict}");
        assert!(tools.calls.lock().unwrap().iter().any(|c| c == "flag_order"));
    }
}
