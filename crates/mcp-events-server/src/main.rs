//! `mcp-events-server`: axum prototype of the draft MCP Events extension,
//! backed by a Drasi SSE reaction or a deterministic mock feed.

mod config;
mod dispatch;
mod handlers;
mod mapping;
mod state;
mod webhook;

use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "mcp-events-server", about = "MCP Events extension prototype server")]
struct Cli {
    /// Path to the YAML config file.
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let cfg = config::ServerConfig::load(&cli.config)?;
    let state = state::AppState::new(cfg)?;

    mapping::spawn_feed_pipeline(state.clone());
    if state.config.webhook.enabled {
        webhook::worker::spawn_delivery_worker(state.clone());
    }

    let addr = format!("{}:{}", state.config.host, state.config.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(addr = %listener.local_addr()?, "mcp-events-server listening (POST /mcp, GET /healthz)");
    axum::serve(listener, dispatch::router(state))
        .await
        .context("serving HTTP")?;
    Ok(())
}
