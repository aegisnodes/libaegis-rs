//! Basic example: a component that connects to Aegis, receives market data,
//! and resets cleanly on session restart (REBORN).

use libaegis::{Component, ComponentHandler, Config, DataStream, StreamMessage};
use libaegis::error::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;

use std::sync::Arc;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct MarketDataHandler {
    socket_path:  Arc<Mutex<String>>,
    component_id: Arc<Mutex<String>>,
    session_id:   Arc<Mutex<String>>,
}

impl MarketDataHandler {
    fn new() -> Self {
        Self {
            socket_path:  Arc::new(Mutex::new(String::new())),
            // Seeded from env at startup; the SDK overwrites these after
            // REGISTERED is received before on_running is ever called.
            component_id: Arc::new(Mutex::new(
                std::env::var("AEGIS_COMPONENT_ID").unwrap_or_default(),
            )),
            session_id: Arc::new(Mutex::new(
                std::env::var("AEGIS_SESSION_TOKEN").unwrap_or_default(),
            )),
        }
    }
}

impl ComponentHandler for MarketDataHandler {
    async fn on_configure(&self, socket_path: String, _topics: Vec<String>) -> Result<()> {
        info!("configure — socket={}", socket_path);
        // Topics are returned by the data stream handshake, not from this
        // payload — store only the socket path for use in on_running.
        *self.socket_path.lock().await = socket_path;
        Ok(())
    }

    async fn on_running(&self, shutdown: CancellationToken) -> Result<()> {
        info!("running — starting data stream worker");

        let socket       = self.socket_path.lock().await.clone();
        let component_id = self.component_id.lock().await.clone();
        let session_id   = self.session_id.lock().await.clone();

        // Spawn the data stream worker. It keeps running across session
        // restarts — REBORN does not cancel this token or call on_running
        // again. Reset per-run state in on_reborn() instead.
        tokio::spawn(async move {
            stream_worker(socket, component_id, session_id, shutdown).await;
        });

        Ok(())
    }

    async fn on_reborn(&self) {
        // Called when Aegis restarts the session (REBORN command).
        // Reset ALL state from the previous run here — counters, positions,
        // buffers, etc. on_running is NOT called again; the existing worker
        // task keeps running and will receive data from the new orchestrator.
        info!("reborn — resetting per-run state");
    }

    async fn on_ping(&self) {
        info!("ping");
    }

    async fn on_shutdown(&self) {
        info!("shutdown — releasing resources");
    }

    async fn on_error(&self, code: String, message: String) {
        tracing::error!("aegis error — code={} message={}", code, message);
    }
}

// ---------------------------------------------------------------------------
// Stream worker
// ---------------------------------------------------------------------------

async fn stream_worker(
    socket_path:  String,
    component_id: String,
    session_id:   String,
    shutdown:     CancellationToken,
) {
    'outer: loop {
        if shutdown.is_cancelled() { break; }

        // Connect and handshake. Retry on failure until shutdown.
        let mut stream = loop {
            if shutdown.is_cancelled() { break 'outer; }

            match DataStream::connect(&socket_path, &component_id, &session_id).await {
                Ok(s) => {
                    info!("data stream connected — topics={:?}", s.topics);
                    break s;
                }
                Err(e) => {
                    tracing::warn!("data stream connect error: {e} — retrying in 1s");
                    tokio::select! {
                        _ = shutdown.cancelled() => break 'outer,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                    }
                }
            }
        };

        // Read frames until the socket closes or shutdown is requested.
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break 'outer,
                result = stream.next() => {
                    match result {
                        Ok(msg)  => handle_message(msg).await,
                        Err(e)   => {
                            tracing::warn!("data stream closed: {e} — reconnecting");
                            break;
                        }
                    }
                }
            }
        }

        // Brief pause before reconnecting to avoid a tight loop when the
        // session has ended and the socket no longer exists.
        tokio::select! {
            _ = shutdown.cancelled() => break 'outer,
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
        }
    }

    info!("stream worker stopped");
}

async fn handle_message(msg: StreamMessage) {
    // topic format: "aegis.<session_id>.<stream>.<symbol>[.<timeframe>]"
    // skip the first two segments (aegis + session_id)
    let parts: Vec<&str> = msg.topic.split('.').collect();
    let parts = if parts.len() >= 3 { &parts[2..] } else { &parts[..] };

    match parts {
        ["aggTrades", sym] => {
            info!("aggTrade — symbol={} data={}", sym, msg.data);
        }
        ["klines", sym, tf] => {
            info!("kline — symbol={} timeframe={} data={}", sym, tf, msg.data);
        }
        _ => {
            tracing::debug!("unhandled topic: {}", msg.topic);
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("libaegis=debug,market_data=info")
        .init();

    let socket_path   = std::env::var("AEGIS_SOCKET")
        .unwrap_or_else(|_| "/tmp/aegis-components.sock".into());
    let session_token = std::env::var("AEGIS_SESSION_TOKEN")
        .unwrap_or_default();

    let mut cfg = Config::new(socket_path, session_token, "market_data");
    cfg.version                = "0.1.0".into();
    cfg.supported_symbols      = vec!["BTCUSDT".into(), "ETHUSDT".into()];
    cfg.supported_timeframes   = vec!["1m".into(), "5m".into()];
    cfg.requires_streams       = vec!["aggTrades".into(), "klines".into()];
    cfg.max_reconnect_attempts = 10;

    let component = Component::new(cfg, MarketDataHandler::new());

    tokio::select! {
        res = component.run() => {
            if let Err(e) = res {
                eprintln!("component stopped with error: {e}");
                std::process::exit(1);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c — shutting down");
        }
    }

    Ok(())
}