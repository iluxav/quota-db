use std::time::Duration;

/// Configuration for anti-entropy system
#[derive(Debug, Clone)]
pub struct AntiEntropyConfig {
    /// Interval between status exchanges (Level 1)
    pub status_interval: Duration,
    /// Run digest check every N status cycles (Level 2)
    pub digest_every_n_cycles: u32,
    /// Number of consecutive mismatches before triggering snapshot
    pub snapshot_threshold: u8,
    /// Timeout for snapshot requests
    pub snapshot_timeout: Duration,
}

impl Default for AntiEntropyConfig {
    fn default() -> Self {
        Self {
            status_interval: Duration::from_secs(5),
            digest_every_n_cycles: 6, // 30 seconds
            snapshot_threshold: 2,
            snapshot_timeout: Duration::from_secs(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AntiEntropyConfig::default();
        assert_eq!(config.status_interval, Duration::from_secs(5));
        assert_eq!(config.digest_every_n_cycles, 6);
        assert_eq!(config.snapshot_threshold, 2);
        assert_eq!(config.snapshot_timeout, Duration::from_secs(10));
    }
}
