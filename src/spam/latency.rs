//! Per-event-type delivery latency configuration.
//!
//! Controls the delay between a send-side event on one node and the
//! corresponding receive-side event on the destination node.

use std::collections::HashMap;
use std::time::Duration;

/// Configuration for delivery latency of receive-side events.
pub struct LatencyConfig {
    default_ms: u64,
    overrides: HashMap<u8, u64>,
}

impl LatencyConfig {
    /// Create a new LatencyConfig with the given default latency in milliseconds.
    pub fn new(default_ms: u64) -> Self {
        Self {
            default_ms,
            overrides: HashMap::new(),
        }
    }

    /// Set a per-event-type latency override.
    #[allow(dead_code)]
    pub fn with_override(mut self, event_id: u8, latency_ms: u64) -> Self {
        self.overrides.insert(event_id, latency_ms);
        self
    }

    /// Get the delivery latency for a given event type.
    pub fn latency_for(&self, event_id: u8) -> Duration {
        Duration::from_millis(
            self.overrides
                .get(&event_id)
                .copied()
                .unwrap_or(self.default_ms),
        )
    }
}
