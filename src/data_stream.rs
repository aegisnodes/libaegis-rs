//! DataStream — client for the Aegis data stream Unix socket.
//!
//! The orchestrator publishes market data as newline-delimited JSON frames
//! delivered over a Unix socket. The component must complete a handshake
//! before data starts flowing:
//!
//!   Component → {"component_id": "...", "session_token": "<session_id>"}
//!   Server    → {"status": "ok", "topics": [...]}
//!
//! Usage:
//! ```rust
//! let mut stream = DataStream::connect(socket_path, component_id, session_id).await?;
//! loop {
//!     let msg = stream.next().await?;
//!     // msg.topic, msg.data
//! }
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::timeout,
};

use crate::error::{AegisError, Result};

// ─── Public types ─────────────────────────────────────────────────────────────

/// A single message received from the data stream.
#[derive(Debug, Clone)]
pub struct StreamMessage {
    /// Dot-separated topic, e.g. `"aggTrades.BTCUSDT"` or `"klines.BTCUSDT.1h"`.
    pub topic: String,
    /// Raw JSON payload — shape depends on the topic.
    pub data: Value,
}

/// Connected data stream session.
pub struct DataStream {
    /// Topics this stream is subscribed to (from the handshake response).
    pub topics: Vec<String>,
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
}

// ─── Wire format ──────────────────────────────────────────────────────────────

/// Incoming data frame from the orchestrator.
#[derive(Debug, Deserialize)]
struct Frame {
    topic: String,
    data:  Value,
}

/// Handshake sent by the component to identify itself.
/// `session_token` is the session ID (not the registration token).
#[derive(Serialize)]
struct Handshake<'a> {
    component_id:  &'a str,
    session_token: &'a str,
}

/// Handshake response from the data stream server.
#[derive(Deserialize)]
struct HandshakeResponse {
    status:  String,
    message: Option<String>,
    topics:  Option<Vec<String>>,
}

// ─── Read timeout ─────────────────────────────────────────────────────────────

/// How long to wait for the next frame before returning a timeout error.
/// The orchestrator sends data continuously during a session — a long silence
/// means the session ended or the socket was closed.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

// ─── Implementation ───────────────────────────────────────────────────────────

impl DataStream {
    /// Connect to the data stream socket and complete the handshake.
    ///
    /// `socket_path`  — from the CONFIGURE payload (`data_stream_socket`).
    /// `component_id` — from env var `AEGIS_COMPONENT_ID` or the stored ID
    ///                  received in the REGISTERED response.
    /// `session_id`   — the session ID received in the REGISTERED response
    ///                  (used as `session_token` in the handshake).
    pub async fn connect(
        socket_path:  &str,
        component_id: &str,
        session_id:   &str,
    ) -> Result<Self> {
        let stream = UnixStream::connect(socket_path).await
            .map_err(|e| AegisError::Connection(format!("data stream connect: {e}")))?;

        let (read_half, write_half) = stream.into_split();
        let mut writer = tokio::io::BufWriter::new(write_half);
        let mut reader = BufReader::new(read_half);

        // Send handshake so the server can identify this component and
        // build its topic subscription set.
        let hs = Handshake { component_id, session_token: session_id };
        let mut frame = serde_json::to_string(&hs).map_err(AegisError::Json)?;
        frame.push('\n');

        writer.write_all(frame.as_bytes()).await.map_err(AegisError::Io)?;
        writer.flush().await.map_err(AegisError::Io)?;

        // Read handshake response — contains the topic list and status.
        let mut line = String::new();
        timeout(Duration::from_secs(10), reader.read_line(&mut line))
            .await
            .map_err(|_| AegisError::Timeout)?
            .map_err(AegisError::Io)?;

        let resp: HandshakeResponse = serde_json::from_str(&line)
            .map_err(|e| AegisError::Connection(format!("data stream handshake response: {e}")))?;

        if resp.status != "ok" {
            return Err(AegisError::Connection(format!(
                "data stream handshake rejected: {}",
                resp.message.unwrap_or_else(|| "no message".into())
            )));
        }

        let topics = resp.topics.unwrap_or_default();

        tracing::debug!(?topics, "data stream connected");

        Ok(Self { topics, reader })
    }

    /// Read the next message from the stream.
    ///
    /// Blocks until a frame arrives, the socket closes, or `READ_TIMEOUT`
    /// elapses. Returns `Err(AegisError::Connection)` on EOF so the caller
    /// can reconnect.
    pub async fn next(&mut self) -> Result<StreamMessage> {
        let mut line = String::new();

        let n = timeout(READ_TIMEOUT, self.reader.read_line(&mut line))
            .await
            .map_err(|_| AegisError::Timeout)?
            .map_err(AegisError::Io)?;

        if n == 0 {
            return Err(AegisError::Connection("data stream closed".into()));
        }

        let frame: Frame = serde_json::from_str(&line).map_err(AegisError::Json)?;

        Ok(StreamMessage {
            topic: frame.topic,
            data:  frame.data,
        })
    }
}