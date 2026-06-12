//! `events-harness`: command-line exerciser for the MCP Events extension.
//!
//! Subcommands: list, poll, stream, subscribe, unsubscribe, webhook-recv.
//! Events are printed one per line: `[<name>] <eventId> <changeType?> <data JSON>`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context as _};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use mcp_events_client::{
    load_state, save_state, CursorState, EventsClient, LruSet, RpcError, StreamFrame,
};
use mcp_events_wire as wire;
use serde_json::{json, Value};
use tracing::{info, warn};

const DEDUP_CAPACITY: usize = 4096;
/// Floor applied to `nextPollMs` (the design sketch suggests a configurable
/// floor; this harness defaults it to 250 ms).
const DEFAULT_POLL_FLOOR_MS: u64 = 250;
/// Stream considered dead after this long without any frame (sketch: twice
/// the heartbeat interval; the interval is not communicated, so assume the
/// 30 s SHOULD → 60 s).
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);
/// Refresh cadence for no-expiry grants ("occasional health-check" per the sketch).
const NO_EXPIRY_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// Standard Webhooks freshness window: reject deliveries older than 5 minutes
/// (applied symmetrically to future-dated timestamps).
const MAX_TIMESTAMP_SKEW_SECS: i64 = 300;

#[derive(Parser, Debug)]
#[command(name = "events-harness", version, about = "MCP Events client harness")]
struct Cli {
    /// MCP server endpoint (Streamable HTTP).
    #[arg(long, global = true, default_value = "http://127.0.0.1:8090/mcp")]
    server: String,
    /// Bearer token for Authorization.
    #[arg(long, global = true)]
    bearer: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Initialize and pretty-print the advertised event types.
    List,
    /// Poll loop for one subscription, honoring nextPollMs/hasMore.
    Poll {
        #[arg(long)]
        name: String,
        /// Subscription params as a JSON object.
        #[arg(long)]
        params: Option<String>,
        /// Persists {"cursor": ...} between runs.
        #[arg(long)]
        state_file: Option<PathBuf>,
        /// Keep polling forever (default: drain once and exit).
        #[arg(long)]
        follow: bool,
        #[arg(long)]
        max_events: Option<u32>,
        #[arg(long)]
        max_age_ms: Option<u64>,
        /// Minimum wait between polls, guarding against tight loops.
        #[arg(long, default_value_t = DEFAULT_POLL_FLOOR_MS)]
        floor_ms: u64,
    },
    /// Open an events/stream and print every frame; reconnects with the last cursor.
    Stream {
        #[arg(long)]
        name: String,
        #[arg(long)]
        params: Option<String>,
        #[arg(long)]
        state_file: Option<PathBuf>,
    },
    /// Register (or refresh) a webhook subscription.
    Subscribe {
        #[arg(long)]
        name: String,
        /// Callback URL the server should POST events to.
        #[arg(long)]
        url: String,
        #[arg(long)]
        params: Option<String>,
        /// Suggested TTL in ms, or the literal "null" to request no expiry.
        #[arg(long, value_name = "MS|null")]
        ttl_ms: Option<String>,
        /// whsec_ secret; generated (and printed) if omitted.
        #[arg(long)]
        secret: Option<String>,
        #[arg(long)]
        state_file: Option<PathBuf>,
        /// Keep re-subscribing before each refreshBefore.
        #[arg(long)]
        refresh_loop: bool,
    },
    /// Remove a webhook subscription.
    Unsubscribe {
        #[arg(long)]
        name: String,
        #[arg(long)]
        url: String,
        #[arg(long)]
        params: Option<String>,
    },
    /// Run a Standard-Webhooks-verifying receiver endpoint.
    WebhookRecv {
        #[arg(long)]
        port: u16,
        #[arg(long)]
        secret: String,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        match e.downcast_ref::<RpcError>() {
            Some(rpc) => eprintln!("error: {rpc}"),
            None => eprintln!("error: {e:#}"),
        }
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.cmd {
        Cmd::List => cmd_list(&cli.server, &cli.bearer).await,
        Cmd::Poll {
            ref name,
            ref params,
            ref state_file,
            follow,
            max_events,
            max_age_ms,
            floor_ms,
        } => {
            cmd_poll(
                &cli.server, &cli.bearer, name, params, state_file, follow, max_events, max_age_ms,
                floor_ms,
            )
            .await
        }
        Cmd::Stream {
            ref name,
            ref params,
            ref state_file,
        } => cmd_stream(&cli.server, &cli.bearer, name, params, state_file).await,
        Cmd::Subscribe {
            ref name,
            ref url,
            ref params,
            ref ttl_ms,
            ref secret,
            ref state_file,
            refresh_loop,
        } => {
            cmd_subscribe(
                &cli.server, &cli.bearer, name, url, params, ttl_ms, secret, state_file,
                refresh_loop,
            )
            .await
        }
        Cmd::Unsubscribe {
            ref name,
            ref url,
            ref params,
        } => cmd_unsubscribe(&cli.server, &cli.bearer, name, url, params).await,
        Cmd::WebhookRecv { port, ref secret } => cmd_webhook_recv(port, secret).await,
    }
}

// ---------------------------------------------------------------- helpers

async fn connect(server: &str, bearer: &Option<String>) -> anyhow::Result<EventsClient> {
    let mut client = EventsClient::new(server.to_owned());
    if let Some(token) = bearer {
        client = client.with_bearer(token.clone());
    }
    let init = client.initialize().await?;
    info!(
        server_name = %init.server_info.name,
        server_version = %init.server_info.version,
        protocol = %init.protocol_version,
        "initialized"
    );
    Ok(client)
}

fn parse_params(s: &Option<String>) -> anyhow::Result<Option<Value>> {
    match s {
        None => Ok(None),
        Some(raw) => Ok(Some(
            serde_json::from_str(raw).context("--params must be valid JSON")?,
        )),
    }
}

fn parse_ttl(s: &Option<String>) -> anyhow::Result<Option<Option<u64>>> {
    match s.as_deref() {
        None => Ok(None),
        Some("null") => Ok(Some(None)),
        Some(v) => Ok(Some(Some(
            v.parse().context("--ttl-ms must be an integer or 'null'")?,
        ))),
    }
}

fn load_or_default(path: &Option<PathBuf>) -> anyhow::Result<CursorState> {
    match path {
        None => Ok(CursorState::default()),
        Some(p) => {
            let st = load_state(p)?.unwrap_or_default();
            if st.cursor.is_some() {
                info!(state_file = %p.display(), cursor = ?st.cursor, "resuming from persisted cursor");
            }
            Ok(st)
        }
    }
}

fn persist(path: &Option<PathBuf>, state: &CursorState) -> anyhow::Result<()> {
    if let Some(p) = path {
        save_state(p, state)?;
    }
    Ok(())
}

fn print_event(ev: &wire::EventOccurrence) {
    match ev.data.get("changeType").and_then(Value::as_str) {
        Some(change) => println!("[{}] {} {} {}", ev.name, ev.event_id, change, ev.data),
        None => println!("[{}] {} {}", ev.name, ev.event_id, ev.data),
    }
}

fn fmt_cursor(c: &Option<String>) -> &str {
    c.as_deref().unwrap_or("null")
}

fn mode_str(m: &wire::DeliveryMode) -> &'static str {
    match m {
        wire::DeliveryMode::Poll => "poll",
        wire::DeliveryMode::Push => "push",
        wire::DeliveryMode::Webhook => "webhook",
    }
}

// ---------------------------------------------------------------- list

async fn cmd_list(server: &str, bearer: &Option<String>) -> anyhow::Result<()> {
    let client = connect(server, bearer).await?;
    let res = client.list_events().await?;
    if res.events.is_empty() {
        println!("(no event types advertised)");
    }
    for def in &res.events {
        let modes: Vec<&str> = def.delivery.iter().map(mode_str).collect();
        println!("{} [{}]", def.name, modes.join(", "));
        if let Some(desc) = &def.description {
            println!("    {desc}");
        }
        if let Some(schema) = &def.input_schema {
            println!("    inputSchema: {schema}");
        }
        if let Some(schema) = &def.payload_schema {
            println!("    payloadSchema: {schema}");
        }
    }
    if res.next_cursor.is_some() {
        warn!("server returned nextCursor; additional pages were not fetched");
    }
    Ok(())
}

// ---------------------------------------------------------------- poll

#[allow(clippy::too_many_arguments)]
async fn cmd_poll(
    server: &str,
    bearer: &Option<String>,
    name: &str,
    params: &Option<String>,
    state_file: &Option<PathBuf>,
    follow: bool,
    max_events: Option<u32>,
    max_age_ms: Option<u64>,
    floor_ms: u64,
) -> anyhow::Result<()> {
    let params = parse_params(params)?;
    let mut state = load_or_default(state_file)?;
    let client = connect(server, bearer).await?;
    let mut dedup = LruSet::new(DEDUP_CAPACITY);
    let floor = Duration::from_millis(floor_ms);
    let mut transient_backoff = Duration::from_secs(1);
    loop {
        let req = wire::PollEventsParams {
            name: name.to_owned(),
            params: params.clone(),
            cursor: state.cursor.clone(),
            max_age_ms,
            max_events,
        };
        let res = match client.poll(&req).await {
            Ok(r) => r,
            Err(e) => {
                // JSON-RPC errors (NotFound/Forbidden/terminated...) are fatal;
                // transport hiccups are retried only in --follow mode.
                if e.downcast_ref::<RpcError>().is_some() || !follow {
                    return Err(e);
                }
                warn!(error = %format!("{e:#}"), "poll failed; retrying");
                tokio::time::sleep(transient_backoff).await;
                transient_backoff = (transient_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                continue;
            }
        };
        transient_backoff = Duration::from_secs(1);
        if res.truncated {
            warn!("truncated=true: events were skipped (stale cursor, maxAgeMs floor, or replay ceiling)");
        }
        for ev in &res.events {
            if dedup.check_and_insert(&ev.event_id) {
                warn!(event_id = %ev.event_id, "duplicate event suppressed");
                continue;
            }
            print_event(ev);
        }
        state.cursor = res.cursor.clone();
        persist(state_file, &state)?;
        if res.has_more {
            continue; // drain backlog immediately, ignoring nextPollMs
        }
        if !follow {
            break;
        }
        tokio::time::sleep(Duration::from_millis(res.next_poll_ms).max(floor)).await;
    }
    Ok(())
}

// ---------------------------------------------------------------- stream

async fn cmd_stream(
    server: &str,
    bearer: &Option<String>,
    name: &str,
    params: &Option<String>,
    state_file: &Option<PathBuf>,
) -> anyhow::Result<()> {
    let params = parse_params(params)?;
    let mut state = load_or_default(state_file)?;
    let client = connect(server, bearer).await?;
    let mut backoff = Duration::from_secs(1);
    loop {
        let sp = wire::StreamEventsParams {
            name: name.to_owned(),
            params: params.clone(),
            cursor: state.cursor.clone(),
            max_age_ms: None,
        };
        let mut stream = match client.stream(&sp).await {
            Ok(s) => s,
            Err(e) => {
                if e.downcast_ref::<RpcError>().is_some() {
                    return Err(e); // subscription rejected: fatal
                }
                warn!(error = %format!("{e:#}"), backoff = ?backoff, "stream connect failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
                continue;
            }
        };
        let mut server_closed = false;
        loop {
            match tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.next()).await {
                Err(_) => {
                    warn!(idle = ?STREAM_IDLE_TIMEOUT, "no frames within idle window; treating stream as dead");
                    break;
                }
                Ok(None) => {
                    warn!("stream ended without a final result frame; reconnecting");
                    break;
                }
                Ok(Some(Err(e))) => {
                    warn!(error = %format!("{e:#}"), "stream error; reconnecting");
                    break;
                }
                Ok(Some(Ok(frame))) => {
                    backoff = Duration::from_secs(1);
                    match frame {
                        StreamFrame::Active(a) => {
                            if a.truncated {
                                warn!("truncated=true on active: events were skipped");
                            }
                            println!(
                                "[stream] active cursor={} truncated={}",
                                fmt_cursor(&a.cursor),
                                a.truncated
                            );
                            state.cursor = a.cursor.clone();
                            persist(state_file, &state)?;
                        }
                        StreamFrame::Event(ev) => {
                            print_event(&ev);
                            // Absent cursor: leave position unchanged; null: no replay.
                            if let Some(c) = &ev.cursor {
                                state.cursor = c.clone();
                                persist(state_file, &state)?;
                            }
                        }
                        StreamFrame::Heartbeat(h) => {
                            println!("[stream] heartbeat cursor={}", fmt_cursor(&h.cursor));
                            state.cursor = h.cursor.clone();
                            persist(state_file, &state)?;
                        }
                        StreamFrame::Error(e) => {
                            let err = RpcError::from(e.error);
                            println!("[stream] error {err} (subscription stays active)");
                        }
                        StreamFrame::Terminated(t) => {
                            let err = RpcError::from(t.error);
                            println!("[stream] terminated {err}");
                            return Ok(());
                        }
                        StreamFrame::Result => {
                            println!("[stream] server closed the stream; reconnecting with last cursor");
                            server_closed = true;
                        }
                    }
                    if server_closed {
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
    }
}

// ---------------------------------------------------------------- subscribe

fn generate_whsec() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    format!("{}{}", wire::WHSEC_PREFIX, B64.encode(bytes))
}

fn print_grant(res: &wire::SubscribeResult) {
    println!("subscription id: {}", res.id);
    println!(
        "refreshBefore:   {}",
        res.refresh_before.as_deref().unwrap_or("null (no expiry)")
    );
    println!("cursor:          {}", fmt_cursor(&res.cursor));
    if res.truncated {
        warn!("truncated=true: delivery starts later than the supplied cursor");
    }
    if let Some(ds) = &res.delivery_status {
        println!(
            "deliveryStatus:  active={} lastDeliveryAt={} lastError={} failedSince={}",
            ds.active,
            ds.last_delivery_at.as_deref().unwrap_or("null"),
            ds.last_error.as_deref().unwrap_or("null"),
            ds.failed_since.as_deref().unwrap_or("null"),
        );
    }
}

/// Sleep until ~80% of the remaining lifetime has elapsed (margin is
/// unspecified by the sketch); no-expiry grants use a health-check cadence.
fn refresh_sleep(refresh_before: &Option<String>) -> anyhow::Result<Duration> {
    match refresh_before {
        None => Ok(NO_EXPIRY_REFRESH_INTERVAL),
        Some(rb) => {
            let when = chrono::DateTime::parse_from_rfc3339(rb)
                .with_context(|| format!("unparseable refreshBefore {rb:?}"))?;
            let remain = (when.with_timezone(&Utc) - Utc::now())
                .to_std()
                .unwrap_or(Duration::ZERO);
            Ok((remain * 4 / 5).max(Duration::from_secs(1)))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn cmd_subscribe(
    server: &str,
    bearer: &Option<String>,
    name: &str,
    url: &str,
    params: &Option<String>,
    ttl_ms: &Option<String>,
    secret: &Option<String>,
    state_file: &Option<PathBuf>,
    refresh_loop: bool,
) -> anyhow::Result<()> {
    let params = parse_params(params)?;
    let ttl = parse_ttl(ttl_ms)?;
    let secret = match secret {
        Some(s) => {
            wire::parse_whsec(s).map_err(|e| anyhow!("--secret: {e}"))?;
            s.clone()
        }
        None => {
            let s = generate_whsec();
            println!("generated webhook secret (pass to webhook-recv --secret): {s}");
            s
        }
    };
    let mut state = load_or_default(state_file)?;
    let client = connect(server, bearer).await?;
    let subscribe_once = |cursor: Option<String>| {
        let sp = wire::SubscribeParams {
            name: name.to_owned(),
            params: params.clone(),
            delivery: wire::DeliverySpec {
                mode: "webhook".to_owned(),
                url: url.to_owned(),
                secret: Some(secret.clone()),
            },
            cursor,
            max_age_ms: None,
            ttl_ms: ttl,
        };
        let client = &client;
        async move { client.subscribe(&sp).await }
    };
    let mut grant = subscribe_once(state.cursor.clone()).await?;
    print_grant(&grant);
    state.cursor = grant.cursor.clone();
    persist(state_file, &state)?;
    if !refresh_loop {
        return Ok(());
    }
    loop {
        let nap = refresh_sleep(&grant.refresh_before)?;
        info!(sleep = ?nap, "refresh loop sleeping");
        tokio::time::sleep(nap).await;
        match subscribe_once(state.cursor.clone()).await {
            Ok(res) => {
                println!("--- refresh @ {} ---", Utc::now().to_rfc3339());
                print_grant(&res);
                if let Some(ds) = &res.delivery_status {
                    if !ds.active {
                        warn!("delivery had been suspended; this refresh reactivates it");
                    }
                }
                state.cursor = res.cursor.clone();
                persist(state_file, &state)?;
                grant = res;
            }
            Err(e) => {
                if e.downcast_ref::<RpcError>().is_some() {
                    return Err(e); // e.g. Forbidden after revocation: fatal
                }
                warn!(error = %format!("{e:#}"), "refresh failed; retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// ---------------------------------------------------------------- unsubscribe

async fn cmd_unsubscribe(
    server: &str,
    bearer: &Option<String>,
    name: &str,
    url: &str,
    params: &Option<String>,
) -> anyhow::Result<()> {
    let params = parse_params(params)?;
    let client = connect(server, bearer).await?;
    let up = wire::UnsubscribeParams {
        name: name.to_owned(),
        params,
        // url only: no mode, no secret (per the sketch's unsubscribe example).
        delivery: wire::DeliverySpec {
            mode: String::new(),
            url: url.to_owned(),
            secret: None,
        },
    };
    client.unsubscribe(&up).await?;
    println!("unsubscribed {name} @ {url}");
    Ok(())
}

// ---------------------------------------------------------------- webhook-recv

struct RecvState {
    key: Vec<u8>,
    dedup: Mutex<LruSet>,
}

async fn cmd_webhook_recv(port: u16, secret: &str) -> anyhow::Result<()> {
    let key = wire::parse_whsec(secret).map_err(|e| anyhow!("--secret: {e}"))?;
    let state = Arc::new(RecvState {
        key,
        dedup: Mutex::new(LruSet::new(DEDUP_CAPACITY)),
    });
    let app = Router::new().fallback(recv_handler).with_state(state);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| format!("binding 127.0.0.1:{port}"))?;
    println!("webhook receiver listening on http://127.0.0.1:{port}/ (any path)");
    axum::serve(listener, app)
        .await
        .context("webhook receiver failed")?;
    Ok(())
}

fn bad(msg: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, msg).into_response()
}

async fn recv_handler(
    State(st): State<Arc<RecvState>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method != Method::POST {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let hdr = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let Some(msg_id) = hdr(wire::HEADER_WEBHOOK_ID) else {
        return bad("missing webhook-id header");
    };
    let Some(ts_raw) = hdr(wire::HEADER_WEBHOOK_TIMESTAMP) else {
        return bad("missing webhook-timestamp header");
    };
    let Some(signature) = hdr(wire::HEADER_WEBHOOK_SIGNATURE) else {
        return bad("missing webhook-signature header");
    };
    let sub_id = hdr(wire::HEADER_MCP_SUBSCRIPTION_ID);
    if sub_id.is_none() {
        warn!("delivery missing x-mcp-subscription-id header");
    }
    let Ok(ts) = ts_raw.parse::<i64>() else {
        return bad("unparseable webhook-timestamp");
    };
    let skew = Utc::now().timestamp() - ts;
    if skew.abs() > MAX_TIMESTAMP_SKEW_SECS {
        warn!(skew, %msg_id, "rejecting delivery outside the 5-minute freshness window");
        return bad("webhook-timestamp outside freshness window");
    }
    if !wire::verify_standard_webhooks(&st.key, &msg_id, ts, &body, &signature) {
        warn!(%msg_id, "rejecting delivery with invalid signature");
        return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
    }
    let value: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "validly signed body is not JSON");
            return bad("body is not JSON");
        }
    };
    let duplicate = {
        let mut dedup = st.dedup.lock().unwrap_or_else(|p| p.into_inner());
        dedup.check_and_insert(&msg_id)
    };
    let sub = sub_id.as_deref().unwrap_or("?");
    if value.get("type").is_some() {
        match serde_json::from_value::<wire::WebhookControlBody>(value) {
            // Always echo the challenge, even on a retried (duplicate) id —
            // dropping the echo would fail the server's verification.
            Ok(wire::WebhookControlBody::Verification { challenge }) => {
                println!("[control] verification challenge (sub={sub}); echoing");
                (StatusCode::OK, Json(json!({ "challenge": challenge }))).into_response()
            }
            Ok(wire::WebhookControlBody::Gap { cursor }) => {
                if duplicate {
                    warn!(%msg_id, "duplicate gap envelope suppressed");
                } else {
                    println!(
                        "[control] gap (sub={sub}): events were skipped; fresh cursor={}",
                        cursor.as_deref().unwrap_or("null")
                    );
                }
                StatusCode::OK.into_response()
            }
            Ok(wire::WebhookControlBody::Terminated { error }) => {
                if duplicate {
                    warn!(%msg_id, "duplicate terminated envelope suppressed");
                } else {
                    println!(
                        "[control] terminated (sub={sub}): {}",
                        RpcError::from(error)
                    );
                }
                StatusCode::OK.into_response()
            }
            Err(e) => {
                warn!(error = %e, "unknown control envelope");
                bad("unknown control envelope")
            }
        }
    } else {
        match serde_json::from_value::<wire::EventOccurrence>(value) {
            Ok(ev) => {
                if duplicate {
                    warn!(webhook_id = %msg_id, "duplicate delivery suppressed");
                } else {
                    print_event(&ev);
                }
                StatusCode::OK.into_response()
            }
            Err(e) => {
                warn!(error = %e, "body is neither a control envelope nor an EventOccurrence");
                bad("malformed event body")
            }
        }
    }
}

// ---------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn help_lists_all_subcommands() {
        let mut cmd = Cli::command();
        let help = cmd.render_long_help().to_string();
        for sub in ["list", "poll", "stream", "subscribe", "unsubscribe", "webhook-recv"] {
            assert!(help.contains(sub), "help missing subcommand {sub}: {help}");
        }
        let err = Cli::try_parse_from(["events-harness", "--help"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
    }

    #[test]
    fn subcommand_help_renders() {
        for sub in ["list", "poll", "stream", "subscribe", "unsubscribe", "webhook-recv"] {
            let err = Cli::try_parse_from(["events-harness", sub, "--help"]).unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp, "{sub}");
        }
    }

    #[test]
    fn parses_poll_flags() {
        let cli = Cli::try_parse_from([
            "events-harness",
            "poll",
            "--name",
            "orders.changed",
            "--params",
            "{\"changeType\":\"added\"}",
            "--state-file",
            "/tmp/x.json",
            "--follow",
            "--max-events",
            "5",
            "--max-age-ms",
            "60000",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Poll {
                name,
                follow,
                max_events,
                max_age_ms,
                floor_ms,
                ..
            } => {
                assert_eq!(name, "orders.changed");
                assert!(follow);
                assert_eq!(max_events, Some(5));
                assert_eq!(max_age_ms, Some(60000));
                assert_eq!(floor_ms, DEFAULT_POLL_FLOOR_MS);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn parses_subscribe_ttl_variants() {
        let cli = Cli::try_parse_from([
            "events-harness", "subscribe", "--name", "x", "--url", "https://h/cb", "--ttl-ms",
            "null",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Subscribe { ttl_ms, .. } => assert_eq!(parse_ttl(&ttl_ms).unwrap(), Some(None)),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(parse_ttl(&Some("3600000".into())).unwrap(), Some(Some(3600000)));
        assert_eq!(parse_ttl(&None).unwrap(), None);
        assert!(parse_ttl(&Some("soon".into())).is_err());
    }

    #[test]
    fn generated_secret_is_valid_whsec() {
        let s = generate_whsec();
        let bytes = wire::parse_whsec(&s).unwrap();
        assert_eq!(bytes.len(), 32);
    }

    #[test]
    fn refresh_sleep_handles_no_expiry_and_past_deadlines() {
        assert_eq!(refresh_sleep(&None).unwrap(), NO_EXPIRY_REFRESH_INTERVAL);
        let past = (Utc::now() - chrono::Duration::seconds(10)).to_rfc3339();
        assert_eq!(refresh_sleep(&Some(past)).unwrap(), Duration::from_secs(1));
        let future = (Utc::now() + chrono::Duration::seconds(100)).to_rfc3339();
        let nap = refresh_sleep(&Some(future)).unwrap();
        assert!(
            nap >= Duration::from_secs(70) && nap <= Duration::from_secs(85),
            "{nap:?}"
        );
        assert!(refresh_sleep(&Some("not-a-date".into())).is_err());
    }
}
