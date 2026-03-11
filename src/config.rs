use std::time::Duration;

/// Configuration for an Aegis component.
#[derive(Debug, Clone)]
pub struct Config {
    // Connection
    pub socket_path: String,

    // Identity
    pub session_token:  String,
    pub component_name: String,
    pub version:        String,

    // Capabilities
    pub supported_symbols:    Vec<String>,
    pub supported_timeframes: Vec<String>,
    pub requires_streams:     Vec<String>,

    // Reconnection
    pub reconnect:              bool,
    pub reconnect_delay:        Duration,
    pub max_reconnect_delay:    Duration,
    pub max_reconnect_attempts: u32, // 0 = unlimited
}

impl Config {
    pub fn new(
        socket_path: impl Into<String>,
        session_token: impl Into<String>,
        component_name: impl Into<String>,
    ) -> Self {
        Self {
            socket_path:              socket_path.into(),
            session_token:            session_token.into(),
            component_name:           component_name.into(),
            version:                  "0.1.0".to_string(),
            supported_symbols:        vec![],
            supported_timeframes:     vec![],
            requires_streams:         vec![],
            reconnect:                true,
            reconnect_delay:          Duration::from_secs(3),
            max_reconnect_delay:      Duration::from_secs(60),
            max_reconnect_attempts:   0,
        }
    }
}
