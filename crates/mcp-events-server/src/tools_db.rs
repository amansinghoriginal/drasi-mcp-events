//! Postgres-backed data layer for the demo MCP tools.
//!
//! SDK-agnostic on purpose: these functions speak `serde_json::Value` so the
//! rmcp tool wrappers stay thin. They connect to the same database the Drasi
//! source watches, but `flag_order` writes to `order_flags`, which is outside
//! `drasi_publication` — agent actions never re-enter the event feed.

use std::sync::Arc;

use anyhow::Context as _;
use serde_json::{json, Value};
use tokio_postgres::{Client, NoTls};

/// Thin wrapper over a single tokio-postgres connection with reconnect-on-error.
///
/// A demo server doesn't need pooling; it needs to survive the Postgres
/// container restarting mid-demo. Every call validates the connection and
/// re-dials once on failure.
pub struct ToolsDb {
    conn_str: String,
    client: tokio::sync::Mutex<Option<Arc<Client>>>,
}

impl ToolsDb {
    pub fn new(conn_str: impl Into<String>) -> Self {
        Self {
            conn_str: conn_str.into(),
            client: tokio::sync::Mutex::new(None),
        }
    }

    async fn client(&self) -> anyhow::Result<Arc<Client>> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            if !client.is_closed() {
                return Ok(client.clone());
            }
        }
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .with_context(|| format!("connecting to {}", self.conn_str))?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(%error, "tools-db connection task ended");
            }
        });
        let client = Arc::new(client);
        *guard = Some(client.clone());
        Ok(client)
    }

    /// Full detail for one order, or `null` if it doesn't exist.
    pub async fn get_order(&self, order_id: i32) -> anyhow::Result<Value> {
        let client = self.client().await?;
        let row = client
            .query_opt(
                "SELECT id, customer, total::float8 AS total, status FROM orders WHERE id = $1",
                &[&order_id],
            )
            .await?;
        Ok(match row {
            Some(r) => json!({
                "id": r.get::<_, i32>("id"),
                "customer": r.get::<_, String>("customer"),
                "total": r.get::<_, f64>("total"),
                "status": r.get::<_, String>("status"),
            }),
            None => Value::Null,
        })
    }

    /// A customer's order history plus any prior flags — the context an agent
    /// needs to judge whether a new high-value order is routine or anomalous.
    pub async fn get_customer_history(&self, customer: &str) -> anyhow::Result<Value> {
        let client = self.client().await?;
        let orders = client
            .query(
                "SELECT id, total::float8 AS total, status FROM orders \
                 WHERE customer = $1 ORDER BY id",
                &[&customer],
            )
            .await?;
        let flags = client
            .query(
                "SELECT f.order_id, f.reason, f.flagged_by, f.flagged_at::text AS flagged_at \
                 FROM order_flags f JOIN orders o ON o.id = f.order_id \
                 WHERE o.customer = $1 ORDER BY f.id",
                &[&customer],
            )
            .await?;
        let order_count = orders.len();
        let lifetime_total: f64 = orders.iter().map(|r| r.get::<_, f64>("total")).sum();
        Ok(json!({
            "customer": customer,
            "orderCount": order_count,
            "lifetimeTotal": lifetime_total,
            "orders": orders.iter().map(|r| json!({
                "id": r.get::<_, i32>("id"),
                "total": r.get::<_, f64>("total"),
                "status": r.get::<_, String>("status"),
            })).collect::<Vec<_>>(),
            "priorFlags": flags.iter().map(|r| json!({
                "orderId": r.get::<_, i32>("order_id"),
                "reason": r.get::<_, String>("reason"),
                "flaggedBy": r.get::<_, String>("flagged_by"),
                "flaggedAt": r.get::<_, String>("flagged_at"),
            })).collect::<Vec<_>>(),
        }))
    }

    /// Record a review flag against an order. Errors if the order is unknown so
    /// the model gets a correctable tool error rather than silently flagging air.
    pub async fn flag_order(
        &self,
        order_id: i32,
        reason: &str,
        flagged_by: &str,
    ) -> anyhow::Result<Value> {
        let client = self.client().await?;
        let exists = client
            .query_opt("SELECT 1 FROM orders WHERE id = $1", &[&order_id])
            .await?;
        anyhow::ensure!(exists.is_some(), "order {order_id} does not exist");
        let row = client
            .query_one(
                "INSERT INTO order_flags (order_id, reason, flagged_by) \
                 VALUES ($1, $2, $3) RETURNING id, flagged_at::text AS flagged_at",
                &[&order_id, &reason, &flagged_by],
            )
            .await?;
        Ok(json!({
            "flagId": row.get::<_, i32>("id"),
            "orderId": order_id,
            "reason": reason,
            "flaggedBy": flagged_by,
            "flaggedAt": row.get::<_, String>("flagged_at"),
        }))
    }
}
