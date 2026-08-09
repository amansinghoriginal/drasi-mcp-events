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
const ANTHROPIC_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";

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
    /// auto = claude when ANTHROPIC_API_KEY is set, else policy.
    #[arg(long, default_value = "auto", value_parser = ["auto", "claude", "policy"])]
    mode: String,
    /// Model for claude mode.
    #[arg(long, default_value = "claude-sonnet-5")]
    model: String,
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
    let mode = match cli.mode.as_str() {
        "auto" => {
            if std::env::var("ANTHROPIC_API_KEY").is_ok() {
                "claude"
            } else {
                "policy"
            }
        }
        "claude" if std::env::var("ANTHROPIC_API_KEY").is_err() => {
            anyhow::bail!("--mode claude requires ANTHROPIC_API_KEY")
        }
        m => m,
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
        if mode == "claude" {
            format!(" ({})", cli.model)
        } else {
            " (deterministic)".to_string()
        }
    ));

    let http = reqwest::Client::new();
    let mut cursor = load_state(&cli.state_file)?.unwrap_or_default().cursor;
    if cursor.is_some() {
        log(format!("resuming from persisted cursor {cursor:?}"));
    }
    let mut backoff = Duration::from_secs(1);
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
        while let Some(frame) = stream.next().await {
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
                    let verdict = handle_event(&event, mode, &cli.model, &mcp, &tool_defs, &http)
                        .await
                        .unwrap_or_else(|e| format!("(agent error: {e:#})"));
                    log2(format!("agent: {verdict}"));
                    log("agent idle — waiting for next event");
                    persist(&cli.state_file, &mut cursor, new_cursor);
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
    model: &str,
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
    if mode == "claude" {
        claude_brain(event, model, mcp, tool_defs, http).await
    } else {
        policy_brain(event, mcp).await
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
        log2(format!(
            "  ← {}",
            if shown.len() > 200 { &shown[..200] } else { &shown }
        ));
        Ok(payload)
    }
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

/// Claude tool-use loop over the raw Messages API (no SDK dependency).
async fn claude_brain(
    event: &wire::EventOccurrence,
    model: &str,
    mcp: &impl ToolCaller,
    tool_defs: &[Value],
    http: &reqwest::Client,
) -> Result<String> {
    let api_key = std::env::var("ANTHROPIC_API_KEY")?;
    let mut messages = vec![json!({
        "role": "user",
        "content": format!(
            "A watched-query change event just arrived:\n{}",
            serde_json::to_string_pretty(&serde_json::to_value(event)?)?
        ),
    })];
    for _ in 0..MAX_BRAIN_TURNS {
        let resp = http
            .post(ANTHROPIC_URL)
            .header("x-api-key", &api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&json!({
                "model": model,
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
