//! DataStream — client for the Aegis data stream Unix socket.
//!
//! The orchestrator publishes market data as newline-delimited JSON [`Envelope`] frames
//! delivered over a Unix socket. The component must complete a handshake
//! before data starts flowing:
//!
//!   Component → {"component_id": "...", "session_token": "<session_id>"}
//!   Server    → {"status": "ok", "topics": [...]}
//!
//! Every frame is an [`Envelope`] with a `topic`, a unix-ms `ts`, and a typed
//! `data` payload. Use [`StreamMessage::parse`] to decode the payload into
//! a concrete [`MarketData`] variant.
//!
//! ## Topic format
//! Full NATS topic: `aegis.<session_id>.<data_type>.<symbol>[.<timeframe>]`
//! Use [`TopicParts::parse`] to break it down.
//!
//! ## AggTrade note
//! In **historical** mode `event_time == 0` and `symbol` is empty — these
//! fields are absent from Binance Vision CSV files. Use `transact_time` as
//! the canonical timestamp in both modes (it equals the envelope `ts`).
//!
//! ## OrderBook note
//! Binance USD-M futures depth streams include `event_time` (field `"T"`).
//! Spot depth streams do **not** — in that case the server falls back to
//! `last_update_id` as a monotonic proxy, which is **not** a unix timestamp.
//! [`OrderBook::has_real_timestamp`] lets callers detect this case.
//!
//! Usage:
//! ```rust
//! let mut stream = DataStream::connect(socket_path, component_id, session_id).await?;
//! loop {
//!     let msg = stream.next().await?;
//!     let parts = msg.topic_parts();
//!     match msg.parse()? {
//!         MarketData::AggTrade(t) => { /* t.transact_time is always valid */ }
//!         MarketData::Trade(t)    => { /* t.time is always valid */ }
//!         MarketData::Kline(k)    => { /* k.open_time is always valid */ }
//!         MarketData::OrderBook(ob) => {
//!             if ob.has_real_timestamp() { /* ob.event_time is unix ms */ }
//!         }
//!         MarketData::BookDepth(bd) => { /* bd.timestamp is always unix ms */ }
//!         MarketData::Metrics(m)    => { /* m.create_time is always unix ms */ }
//!         MarketData::Unknown { data_type, payload } => { /* forward-compat */ }
//!     }
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

// ─── Read timeout ─────────────────────────────────────────────────────────────

/// How long [`DataStream::next`] waits before returning [`AegisError::Timeout`].
/// The orchestrator sends data continuously during a session; a long silence
/// means the session ended or the socket was closed.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

// ─── Wire format ──────────────────────────────────────────────────────────────

/// The envelope the Go orchestrator writes for every row.
///
/// Go definition (publisher.go):
/// ```go
/// type Envelope struct {
///     SessionID string          `json:"session_id"`
///     Topic     string          `json:"topic"`
///     Ts        int64           `json:"ts"`
///     Data      json.RawMessage `json:"data"`
/// }
/// ```
#[derive(Debug, Deserialize)]
struct RawEnvelope {
    pub session_id: String,
    /// Full NATS topic: `aegis.<session_id>.<data_type>.<symbol>[.<timeframe>]`
    pub topic:      String,
    /// Canonical unix-ms timestamp for this row.
    /// For OrderBook on **spot** streams this may be `last_update_id` (not
    /// unix ms) when Binance does not include `"T"` in the depth event.
    /// Check [`OrderBook::has_real_timestamp`] before treating it as a clock value.
    pub ts:         i64,
    /// JSON payload; shape depends on the data type encoded in `topic`.
    pub data:       Value,
}

/// Handshake sent by the component to identify itself.
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

// ─── Public types ─────────────────────────────────────────────────────────────

/// A single decoded message from the data stream.
#[derive(Debug, Clone)]
pub struct StreamMessage {
    pub session_id: String,
    /// Full NATS topic: `aegis.<session_id>.<data_type>.<symbol>[.<timeframe>]`
    pub topic:      String,
    /// Canonical unix-ms timestamp for this row (see module docs for caveats).
    pub ts:         i64,
    /// Raw JSON payload.
    pub data:       Value,
}

impl StreamMessage {
    /// Parse the topic into its component parts.
    ///
    /// Returns `None` if the topic doesn't match the expected format.
    pub fn topic_parts(&self) -> Option<TopicParts> {
        TopicParts::parse(&self.topic)
    }

    /// Decode the `data` payload into a typed [`MarketData`] variant.
    ///
    /// Uses the data type encoded in the topic to select the right schema.
    /// Returns [`MarketData::Unknown`] for unrecognised data types so that
    /// new server-side types do not break existing components.
    pub fn parse(&self) -> Result<MarketData> {
        let parts = self
            .topic_parts()
            .ok_or_else(|| AegisError::Connection(format!("unparseable topic: {}", self.topic)))?;

        MarketData::decode(&parts.data_type, self.data.clone())
    }
}

/// The components of a full NATS topic string.
///
/// Format: `aegis.<session_id>.<data_type>.<symbol>[.<timeframe>]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicParts {
    pub session_id: String,
    pub data_type:  String,
    pub symbol:     String,
    /// Empty for flat data types (trades, aggTrades, bookDepth, metrics).
    pub timeframe:  Option<String>,
}

impl TopicParts {
    /// Parse a full NATS topic into its parts.
    ///
    /// Returns `None` if the topic has fewer than 4 dot-separated segments.
    pub fn parse(topic: &str) -> Option<Self> {
        // Expected: aegis . <session_id> . <data_type> . <symbol> [. <timeframe>]
        let parts: Vec<&str> = topic.splitn(6, '.').collect();
        if parts.len() < 4 {
            return None;
        }
        // parts[0] == "aegis"
        Some(TopicParts {
            session_id: parts[1].to_string(),
            data_type:  parts[2].to_string(),
            symbol:     parts[3].to_string(),
            timeframe:  parts.get(4).map(|s| s.to_string()),
        })
    }
}

// ─── Typed payloads ───────────────────────────────────────────────────────────

/// Aggregated trade from Binance.
///
/// **Historical vs realtime differences:**
/// - `event_time`: 0 in historical mode (not in CSV).
/// - `symbol`: empty string in historical mode.
/// - `normal_qty`: 0.0 in historical mode (RPI-adjusted qty, futures only).
///
/// Always use `transact_time` as the canonical timestamp — it equals the
/// envelope `ts` in both modes.
#[derive(Debug, Clone, Deserialize)]
pub struct AggTrade {
    /// Unix ms. Zero in historical mode — use `transact_time` instead.
    pub event_time:     i64,
    /// Empty in historical mode.
    pub symbol:         String,
    pub agg_trade_id:   i64,
    pub price:          f64,
    /// Total quantity including RPI orders.
    pub quantity:       f64,
    /// Quantity excluding RPI orders (futures only). Zero in historical mode.
    pub normal_qty:     f64,
    pub first_trade_id: i64,
    pub last_trade_id:  i64,
    /// Canonical timestamp in both historical and realtime modes.
    pub transact_time:  i64,
    pub is_buyer_maker: bool,
}

impl AggTrade {
    /// Returns `true` if this row came from a live WebSocket stream.
    /// Historical rows have `event_time == 0`.
    #[inline]
    pub fn is_realtime(&self) -> bool {
        self.event_time != 0
    }
}

/// Individual trade.
#[derive(Debug, Clone, Deserialize)]
pub struct Trade {
    pub id:             i64,
    pub price:          f64,
    pub qty:            f64,
    pub quote_qty:      f64,
    /// Unix ms. Always valid.
    pub time:           i64,
    pub is_buyer_maker: bool,
}

/// OHLCV kline / candlestick.
#[derive(Debug, Clone, Deserialize)]
pub struct Kline {
    /// Unix ms — open of the candle interval. Always valid.
    pub open_time:              i64,
    pub open:                   f64,
    pub high:                   f64,
    pub low:                    f64,
    pub close:                  f64,
    pub volume:                 f64,
    pub close_time:             i64,
    pub quote_volume:           f64,
    pub count:                  i64,
    pub taker_buy_volume:       f64,
    pub taker_buy_quote_volume: f64,
}

/// Partial-depth order book snapshot (realtime only, no CSV equivalent).
///
/// Binance USD-M futures depth streams include `event_time` (field `"T"`).
/// Binance **spot** depth streams do *not* — in that case the server stores
/// `last_update_id` in `event_time` as a monotonic proxy.
///
/// Use [`has_real_timestamp`][OrderBook::has_real_timestamp] to distinguish
/// the two cases before treating `event_time` as a wall-clock value.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderBook {
    pub last_update_id: i64,
    /// Unix ms for USD-M futures streams. For spot streams this is
    /// `last_update_id` — a monotonic counter, **not** a unix timestamp.
    /// Check [`has_real_timestamp`][OrderBook::has_real_timestamp] first.
    pub event_time:     i64,
    pub bids:           Vec<PriceLevel>,
    pub asks:           Vec<PriceLevel>,
}

impl OrderBook {
    /// Returns `true` when `event_time` is a real unix-ms timestamp.
    ///
    /// Binance spot depth streams omit the `"T"` field; the server falls
    /// back to `last_update_id`. Heuristic: a value larger than
    /// `1_000_000_000_000` (year 2001 in ms) is almost certainly a real
    /// timestamp; smaller values are `last_update_id` counters.
    #[inline]
    pub fn has_real_timestamp(&self) -> bool {
        self.event_time > 1_000_000_000_000
    }

    /// Best bid (highest price). Returns `None` if the snapshot is empty.
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.first()
    }

    /// Best ask (lowest price). Returns `None` if the snapshot is empty.
    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first()
    }

    /// Mid-price between best bid and best ask. Returns `None` if either side is empty.
    pub fn mid_price(&self) -> Option<f64> {
        let bid = self.best_bid()?.price;
        let ask = self.best_ask()?.price;
        Some((bid + ask) / 2.0)
    }
}

/// A single bid or ask level in an order book snapshot.
#[derive(Debug, Clone, Deserialize)]
pub struct PriceLevel {
    pub price:    f64,
    pub quantity: f64,
}

/// Aggregated order book depth at a given percentage from mid-price (historical only).
#[derive(Debug, Clone, Deserialize)]
pub struct BookDepth {
    /// Unix ms. Always valid.
    pub timestamp:  i64,
    pub percentage: f64,
    pub depth:      f64,
    pub notional:   f64,
}

/// Funding-rate / open-interest metrics snapshot (historical only).
#[derive(Debug, Clone, Deserialize)]
pub struct Metrics {
    /// Unix ms. Always valid.
    pub create_time:                      i64,
    pub symbol:                           String,
    pub sum_open_interest:                f64,
    pub sum_open_interest_value:          f64,
    pub count_toptrader_long_short_ratio: f64,
    pub sum_toptrader_long_short_ratio:   f64,
    pub count_long_short_ratio:           f64,
    pub sum_taker_long_short_vol_ratio:   f64,
}

/// Typed market data payload.
#[derive(Debug, Clone)]
pub enum MarketData {
    AggTrade(AggTrade),
    Trade(Trade),
    Kline(Kline),
    /// Realtime only — no CSV/historical equivalent.
    OrderBook(OrderBook),
    /// Historical only — no WebSocket/realtime equivalent.
    BookDepth(BookDepth),
    /// Historical only — no WebSocket/realtime equivalent.
    Metrics(Metrics),
    /// Forward-compatibility variant for data types added after this SDK version.
    Unknown {
        data_type: String,
        payload:   Value,
    },
}

impl MarketData {
    fn decode(data_type: &str, payload: Value) -> Result<Self> {
        match data_type {
            "aggTrades" => {
                let v = serde_json::from_value(payload).map_err(AegisError::Json)?;
                Ok(MarketData::AggTrade(v))
            }
            "trades" => {
                let v = serde_json::from_value(payload).map_err(AegisError::Json)?;
                Ok(MarketData::Trade(v))
            }
            "klines" => {
                let v = serde_json::from_value(payload).map_err(AegisError::Json)?;
                Ok(MarketData::Kline(v))
            }
            "orderBook" => {
                let v = serde_json::from_value(payload).map_err(AegisError::Json)?;
                Ok(MarketData::OrderBook(v))
            }
            "bookDepth" => {
                let v = serde_json::from_value(payload).map_err(AegisError::Json)?;
                Ok(MarketData::BookDepth(v))
            }
            "metrics" => {
                let v = serde_json::from_value(payload).map_err(AegisError::Json)?;
                Ok(MarketData::Metrics(v))
            }
            other => Ok(MarketData::Unknown {
                data_type: other.to_string(),
                payload,
            }),
        }
    }
}

// ─── DataStream ───────────────────────────────────────────────────────────────

/// Connected data stream session.
pub struct DataStream {
    /// Topics this stream is subscribed to (from the handshake response).
    pub topics: Vec<String>,
    reader:     BufReader<tokio::net::unix::OwnedReadHalf>,
}

impl DataStream {
    /// Connect to the data stream socket and complete the handshake.
    ///
    /// - `socket_path`  — from the CONFIGURE payload (`data_stream_socket`).
    /// - `component_id` — from `AEGIS_COMPONENT_ID` env var or stored REGISTERED response.
    /// - `session_id`   — session ID from the REGISTERED response (used as `session_token`).
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

        // Send handshake so the server can identify this component and build
        // its topic subscription set.
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

    /// Read the next raw message from the stream.
    ///
    /// Blocks until a frame arrives, the socket closes, or [`READ_TIMEOUT`]
    /// elapses. Returns `Err(AegisError::Connection)` on EOF so the caller
    /// can reconnect.
    pub async fn next(&mut self) -> Result<StreamMessage> {
        tracing::debug!("next(): waiting for line...");  // ADD

        let mut line = String::new();

        let n = timeout(READ_TIMEOUT, self.reader.read_line(&mut line))
            .await
            .map_err(|_| AegisError::Timeout)?
            .map_err(AegisError::Io)?;

        if n == 0 {
            return Err(AegisError::Connection("data stream closed".into()));
        }

        let env: RawEnvelope = serde_json::from_str(&line).map_err(AegisError::Json)?;

        tracing::debug!("next(): got {} bytes", n);  // ADD


        Ok(StreamMessage {
            session_id: env.session_id,
            topic:      env.topic,
            ts:         env.ts,
            data:       env.data,
        })
    }

    /// Convenience wrapper: read the next message and immediately parse it.
    ///
    /// Equivalent to `stream.next().await?.parse()`.
    pub async fn next_parsed(&mut self) -> Result<(StreamMessage, MarketData)> {
        let msg = self.next().await?;
        tracing::debug!("next_parsed(): topic={} ts={}", msg.topic, msg.ts); // ADD
        let data = msg.parse()?;
        Ok((msg, data))
    }
}