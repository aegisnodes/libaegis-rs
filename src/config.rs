use std::time::Duration;

/// Configuration for an Aegis component.
///
/// ## Topic construction
///
/// The server builds session topics by combining the stream names in
/// `requires_streams` with `supported_symbols`, `supported_timeframes`,
/// and `supported_orderbook_speeds`. **Never include symbols, timeframes,
/// or speeds inside `requires_streams`** — those belong in their
/// dedicated fields.
///
/// | `requires_streams` entry | Extra field used          | Resulting topic              |
/// |--------------------------|---------------------------|------------------------------|
/// | `"aggTrades"`            | `supported_symbols`       | `aggTrades.BTCUSDT`          |
/// | `"trades"`               | `supported_symbols`       | `trades.BTCUSDT`             |
/// | `"klines"`               | `supported_symbols`       | `klines.BTCUSDT.1m`          |
/// |                          | + `supported_timeframes`  |                              |
/// | `"orderBook"`            | `supported_symbols`       | `orderBook.BTCUSDT.100ms`    |
/// |                          | + `supported_orderbook_speeds` |                         |
/// | `"bookDepth"`            | `supported_symbols`       | `bookDepth.BTCUSDT`          |
/// | `"metrics"`              | `supported_symbols`       | `metrics.BTCUSDT`            |
#[derive(Debug, Clone)]
pub struct Config {
    // Connection
    pub socket_path: String,
 
    // Identity
    pub session_token:  String,
    pub component_name: String,
    pub version:        String,
 
    // Capabilities
    pub supported_symbols:         Vec<String>,
    pub supported_timeframes:      Vec<String>,
    /// Update speeds for the orderBook stream.
    /// Valid values: `"100ms"`, `"250ms"`, `"500ms"`.
    /// Defaults to `["100ms"]` when empty and `requires_streams` contains `"orderBook"`.
    pub supported_orderbook_speeds: Vec<String>,
    /// Stream names only — no symbols, timeframes, or speeds.
    /// Example: `vec!["aggTrades", "klines", "orderBook"]`
    pub requires_streams:          Vec<String>,
 
    // Reconnection
    pub reconnect:              bool,
    pub reconnect_delay:        Duration,
    pub max_reconnect_delay:    Duration,
    pub max_reconnect_attempts: u32, // 0 = unlimited
}
 

impl Config {
    pub fn new(
        socket_path:    impl Into<String>,
        session_token:  impl Into<String>,
        component_name: impl Into<String>,
    ) -> Self {
        Self {
            socket_path:                  socket_path.into(),
            session_token:                session_token.into(),
            component_name:               component_name.into(),
            version:                      "0.1.0".to_string(),
            supported_symbols:            vec![],
            supported_timeframes:         vec![],
            supported_orderbook_speeds:   vec![],
            requires_streams:             vec![],
            reconnect:                    true,
            reconnect_delay:              Duration::from_secs(3),
            max_reconnect_delay:          Duration::from_secs(60),
            max_reconnect_attempts:       0,
        }
    }
}
