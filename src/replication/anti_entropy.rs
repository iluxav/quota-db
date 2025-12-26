use std::time::Duration;

use rustc_hash::FxHashSet;

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

/// Per-peer anti-entropy state
#[derive(Debug)]
pub struct PeerAntiEntropyState {
    /// Last known head_seq per shard from this peer
    pub remote_seqs: Vec<u64>,
    /// Digest mismatch count per shard
    pub mismatch_count: Vec<u8>,
    /// Shards with pending snapshot requests
    pub snapshot_pending: FxHashSet<u16>,
}

impl PeerAntiEntropyState {
    /// Create new state for a peer
    pub fn new(num_shards: usize) -> Self {
        Self {
            remote_seqs: vec![0; num_shards],
            mismatch_count: vec![0; num_shards],
            snapshot_pending: FxHashSet::default(),
        }
    }

    /// Update remote sequence for a shard
    pub fn update_remote_seq(&mut self, shard_id: u16, seq: u64) {
        if (shard_id as usize) < self.remote_seqs.len() {
            self.remote_seqs[shard_id as usize] = seq;
        }
    }

    /// Get remote sequence for a shard
    pub fn remote_seq(&self, shard_id: u16) -> u64 {
        self.remote_seqs.get(shard_id as usize).copied().unwrap_or(0)
    }

    /// Increment mismatch count for a shard, returns new count
    pub fn increment_mismatch(&mut self, shard_id: u16) -> u8 {
        if let Some(count) = self.mismatch_count.get_mut(shard_id as usize) {
            *count = count.saturating_add(1);
            *count
        } else {
            0
        }
    }

    /// Reset mismatch count for a shard
    pub fn reset_mismatch(&mut self, shard_id: u16) {
        if let Some(count) = self.mismatch_count.get_mut(shard_id as usize) {
            *count = 0;
        }
    }

    /// Get mismatch count for a shard
    pub fn mismatch_count(&self, shard_id: u16) -> u8 {
        self.mismatch_count.get(shard_id as usize).copied().unwrap_or(0)
    }

    /// Mark snapshot as pending for a shard
    pub fn mark_snapshot_pending(&mut self, shard_id: u16) {
        self.snapshot_pending.insert(shard_id);
    }

    /// Check if snapshot is pending for a shard
    pub fn is_snapshot_pending(&self, shard_id: u16) -> bool {
        self.snapshot_pending.contains(&shard_id)
    }

    /// Clear snapshot pending for a shard
    pub fn clear_snapshot_pending(&mut self, shard_id: u16) {
        self.snapshot_pending.remove(&shard_id);
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

    #[test]
    fn test_peer_state_new() {
        let state = PeerAntiEntropyState::new(4);
        assert_eq!(state.remote_seqs.len(), 4);
        assert_eq!(state.mismatch_count.len(), 4);
        assert!(state.snapshot_pending.is_empty());
    }

    #[test]
    fn test_peer_state_mismatch_tracking() {
        let mut state = PeerAntiEntropyState::new(4);

        assert_eq!(state.mismatch_count(0), 0);
        state.increment_mismatch(0);
        assert_eq!(state.mismatch_count(0), 1);
        state.increment_mismatch(0);
        assert_eq!(state.mismatch_count(0), 2);
        state.reset_mismatch(0);
        assert_eq!(state.mismatch_count(0), 0);
    }

    #[test]
    fn test_peer_state_snapshot_pending() {
        let mut state = PeerAntiEntropyState::new(4);

        assert!(!state.is_snapshot_pending(0));
        state.mark_snapshot_pending(0);
        assert!(state.is_snapshot_pending(0));
        state.clear_snapshot_pending(0);
        assert!(!state.is_snapshot_pending(0));
    }

    #[test]
    fn test_peer_state_remote_seq() {
        let mut state = PeerAntiEntropyState::new(4);

        assert_eq!(state.remote_seq(0), 0);
        state.update_remote_seq(0, 100);
        assert_eq!(state.remote_seq(0), 100);

        // Out of bounds should return 0
        assert_eq!(state.remote_seq(10), 0);
    }
}
