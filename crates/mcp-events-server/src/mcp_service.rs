//! The official-SDK half of the hybrid server.
//!
//! `OrdersMcp` is an rmcp `ServerHandler`: it serves the MCP core protocol
//! (initialize back-compat, `server/discover`, 2026-07-28 stateless lifecycle)
//! and three Postgres-backed demo tools. The draft Events extension is
//! advertised here through the 2026-07-28 `extensions` capability map; its
//! `events/*` methods are dispatched by `dispatch.rs` before requests reach
//! rmcp, so to a client the whole thing looks like one MCP server.

use std::sync::Arc;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars, tool, tool_handler, tool_router, ErrorData as McpError,
};

use crate::tools_db::ToolsDb;

/// Extension identifier under which the draft Events extension is advertised.
/// The WG design sketch predates extension ids; this follows the official
/// `io.modelcontextprotocol/tasks` naming pattern for official extensions.
pub const EVENTS_EXTENSION_ID: &str = "io.modelcontextprotocol/events";

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetOrderArgs {
    /// Order id, as carried in event payloads (`data.after.id` / `data.before.id`).
    pub order_id: i32,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CustomerHistoryArgs {
    /// Customer name, as carried in event payloads (`data.after.customer`).
    pub customer: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct FlagOrderArgs {
    /// Order id to flag for human review.
    pub order_id: i32,
    /// Short human-readable justification for the flag.
    pub reason: String,
}

#[derive(Clone)]
pub struct OrdersMcp {
    db: Arc<ToolsDb>,
    // Read by #[tool_handler]-generated code; rustc's dead-code analysis
    // misses the macro use (the SDK's own examples carry the same allow).
    #[allow(dead_code)]
    tool_router: ToolRouter<OrdersMcp>,
}

fn tool_error(error: anyhow::Error) -> McpError {
    // Tool-execution errors (not protocol errors) so the model can self-correct.
    McpError::internal_error(error.to_string(), None)
}

fn json_result(value: serde_json::Value) -> CallToolResult {
    CallToolResult::success(vec![ContentBlock::text(value.to_string())])
}

#[tool_router]
impl OrdersMcp {
    pub fn new(db: Arc<ToolsDb>) -> Self {
        Self {
            db,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Fetch the current authoritative state of one order (id, customer, total, status). Event payloads are triage-only; call this for the source of truth."
    )]
    async fn get_order(
        &self,
        Parameters(GetOrderArgs { order_id }): Parameters<GetOrderArgs>,
    ) -> Result<CallToolResult, McpError> {
        let value = self.db.get_order(order_id).await.map_err(tool_error)?;
        Ok(json_result(value))
    }

    #[tool(
        description = "Fetch a customer's order history: order count, lifetime total, every order, and any prior review flags. Use this to judge whether a new high-value order is routine for this customer or anomalous."
    )]
    async fn get_customer_history(
        &self,
        Parameters(CustomerHistoryArgs { customer }): Parameters<CustomerHistoryArgs>,
    ) -> Result<CallToolResult, McpError> {
        let value = self
            .db
            .get_customer_history(&customer)
            .await
            .map_err(tool_error)?;
        Ok(json_result(value))
    }

    #[tool(
        description = "Flag an order for human review, recording a reason. Writes to the order_flags table (which is outside the watched publication, so flagging never re-triggers events). Errors if the order does not exist."
    )]
    async fn flag_order(
        &self,
        Parameters(FlagOrderArgs { order_id, reason }): Parameters<FlagOrderArgs>,
    ) -> Result<CallToolResult, McpError> {
        let value = self
            .db
            .flag_order(order_id, &reason, "mcp-events-demo-agent")
            .await
            .map_err(tool_error)?;
        Ok(json_result(value))
    }
}

#[tool_handler]
impl rmcp::ServerHandler for OrdersMcp {
    fn get_info(&self) -> ServerInfo {
        let mut capabilities = ServerCapabilities::builder().enable_tools().build();
        let mut extensions = ExtensionCapabilities::new();
        extensions.insert(
            EVENTS_EXTENSION_ID.to_string(),
            serde_json::from_value(serde_json::json!({
                "delivery": ["poll", "push", "webhook"],
                "methods": [
                    "events/list", "events/poll", "events/stream",
                    "events/subscribe", "events/unsubscribe"
                ],
            }))
            .expect("static extension settings object"),
        );
        capabilities.extensions = Some(extensions);

        let mut implementation = Implementation::from_build_env();
        implementation.name = "drasi-mcp-events".into();
        implementation.title =
            Some("Drasi-backed MCP server: orders tools + draft Events extension".into());
        implementation.version = env!("CARGO_PKG_VERSION").into();

        // No with_protocol_version override: rmcp negotiates per client request
        // (2026-07-28 clients get 2026-07-28 since it's in KNOWN_VERSIONS), and
        // the default LATEST is the safer fallback for unknown versions.
        ServerInfo::new(capabilities)
            .with_server_info(implementation)
            .with_instructions(
                "Orders demo server. Tools give authoritative order state and actions \
                 (get_order, get_customer_history, flag_order). The draft MCP Events \
                 extension (io.modelcontextprotocol/events) adds events/list, events/poll, \
                 events/stream, events/subscribe and events/unsubscribe on this same \
                 endpoint: subscribe to `high-value-orders.changed` to be notified when \
                 rows enter, change within, or leave the high-value-orders continuous \
                 query. Event payloads are semantic diffs {changeType, before, after} — \
                 triage from the event, verify via tools, act via flag_order."
                    .to_string(),
            )
    }
}
