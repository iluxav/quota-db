use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustc_hash::FxHashSet;
use tracing::debug;

use crate::engine::ShardedDb;
use crate::replication::Message;

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

/// Anti-entropy task for gap detection and repair
pub struct AntiEntropyTask {
    /// Database reference
    db: Arc<ShardedDb>,
    /// Configuration
    config: AntiEntropyConfig,
    /// Number of shards
    num_shards: usize,
    /// Per-peer state
    peer_state: HashMap<SocketAddr, PeerAntiEntropyState>,
    /// Current cycle counter (for digest scheduling)
    cycle_count: u32,
}

impl AntiEntropyTask {
    /// Create a new anti-entropy task
    pub fn new(db: Arc<ShardedDb>, config: AntiEntropyConfig) -> Self {
        let num_shards = db.num_shards();
        Self {
            db,
            config,
            num_shards,
            peer_state: HashMap::new(),
            cycle_count: 0,
        }
    }

    /// Register a peer for anti-entropy tracking
    pub fn register_peer(&mut self, addr: SocketAddr) {
        use std::collections::hash_map::Entry;
        if let Entry::Vacant(e) = self.peer_state.entry(addr) {
            e.insert(PeerAntiEntropyState::new(self.num_shards));
            debug!("Registered peer {} for anti-entropy", addr);
        }
    }

    /// Unregister a peer
    pub fn unregister_peer(&mut self, addr: SocketAddr) {
        self.peer_state.remove(&addr);
        debug!("Unregistered peer {} from anti-entropy", addr);
    }

    /// Build status message with all shard sequences
    pub fn build_status_message(&self) -> Message {
        let shard_seqs: Vec<(u16, u64)> = (0..self.num_shards)
            .map(|i| (i as u16, self.db.shard_head_seq(i)))
            .collect();
        Message::status(shard_seqs)
    }

    /// Check if this cycle should include digest exchange
    pub fn should_check_digest(&self) -> bool {
        self.config.digest_every_n_cycles > 0
            && self.cycle_count % self.config.digest_every_n_cycles == 0
    }

    /// Increment cycle counter
    pub fn increment_cycle(&mut self) {
        self.cycle_count = self.cycle_count.wrapping_add(1);
    }

    /// Get the status interval
    pub fn status_interval(&self) -> Duration {
        self.config.status_interval
    }

    /// Get a reference to peer state
    pub fn peer_state(&self, addr: &SocketAddr) -> Option<&PeerAntiEntropyState> {
        self.peer_state.get(addr)
    }

    /// Get a mutable reference to peer state
    pub fn peer_state_mut(&mut self, addr: &SocketAddr) -> Option<&mut PeerAntiEntropyState> {
        self.peer_state.get_mut(addr)
    }

    /// Get all peer addresses
    pub fn peer_addrs(&self) -> impl Iterator<Item = &SocketAddr> {
        self.peer_state.keys()
    }

    /// Get the number of registered peers
    pub fn peer_count(&self) -> usize {
        self.peer_state.len()
    }

    /// Get the current cycle count
    pub fn cycle_count(&self) -> u32 {
        self.cycle_count
    }

    /// Get the database reference
    pub fn db(&self) -> &Arc<ShardedDb> {
        &self.db
    }

    /// Get the configuration
    pub fn config(&self) -> &AntiEntropyConfig {
        &self.config
    }

    /// Get the number of shards
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Get the snapshot threshold from configuration
    pub fn snapshot_threshold(&self) -> u8 {
        self.config.snapshot_threshold
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

    // ========== AntiEntropyTask Tests ==========

    fn create_test_db() -> Arc<ShardedDb> {
        use crate::config::Config;
        let config = Config {
            shards: 4,
            node_id: 1,
            ..Default::default()
        };
        Arc::new(ShardedDb::new(&config))
    }

    #[test]
    fn test_anti_entropy_task_new() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let task = AntiEntropyTask::new(db, config);

        assert_eq!(task.num_shards(), 4);
        assert_eq!(task.peer_count(), 0);
        assert_eq!(task.cycle_count(), 0);
    }

    #[test]
    fn test_anti_entropy_task_register_peer() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let addr1: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        assert_eq!(task.peer_count(), 0);

        task.register_peer(addr1);
        assert_eq!(task.peer_count(), 1);
        assert!(task.peer_state(&addr1).is_some());

        task.register_peer(addr2);
        assert_eq!(task.peer_count(), 2);

        // Re-registering same peer should not change count
        task.register_peer(addr1);
        assert_eq!(task.peer_count(), 2);
    }

    #[test]
    fn test_anti_entropy_task_unregister_peer() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let addr1: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        task.register_peer(addr1);
        task.register_peer(addr2);
        assert_eq!(task.peer_count(), 2);

        task.unregister_peer(addr1);
        assert_eq!(task.peer_count(), 1);
        assert!(task.peer_state(&addr1).is_none());
        assert!(task.peer_state(&addr2).is_some());

        // Unregistering non-existent peer is a no-op
        task.unregister_peer(addr1);
        assert_eq!(task.peer_count(), 1);
    }

    #[test]
    fn test_anti_entropy_task_build_status_message() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let task = AntiEntropyTask::new(db, config);

        let msg = task.build_status_message();
        match msg {
            Message::Status { shard_seqs } => {
                assert_eq!(shard_seqs.len(), 4);
                // All shards should start at seq 0
                for (shard_id, seq) in shard_seqs {
                    assert!(shard_id < 4);
                    assert_eq!(seq, 0);
                }
            }
            _ => panic!("Expected Status message"),
        }
    }

    #[test]
    fn test_anti_entropy_task_cycle_counting() {
        let db = create_test_db();
        let config = AntiEntropyConfig {
            digest_every_n_cycles: 3,
            ..Default::default()
        };
        let mut task = AntiEntropyTask::new(db, config);

        // Cycle 0: should check digest (0 % 3 == 0)
        assert!(task.should_check_digest());
        task.increment_cycle();
        assert_eq!(task.cycle_count(), 1);

        // Cycle 1: should not check digest
        assert!(!task.should_check_digest());
        task.increment_cycle();

        // Cycle 2: should not check digest
        assert!(!task.should_check_digest());
        task.increment_cycle();

        // Cycle 3: should check digest (3 % 3 == 0)
        assert!(task.should_check_digest());
    }

    #[test]
    fn test_anti_entropy_task_digest_disabled() {
        let db = create_test_db();
        let config = AntiEntropyConfig {
            digest_every_n_cycles: 0, // Disable digest checks
            ..Default::default()
        };
        let task = AntiEntropyTask::new(db, config);

        // With digest_every_n_cycles = 0, should never check digest
        assert!(!task.should_check_digest());
    }

    #[test]
    fn test_anti_entropy_task_status_interval() {
        let db = create_test_db();
        let config = AntiEntropyConfig {
            status_interval: Duration::from_secs(10),
            ..Default::default()
        };
        let task = AntiEntropyTask::new(db, config);

        assert_eq!(task.status_interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_anti_entropy_task_snapshot_threshold() {
        let db = create_test_db();
        let config = AntiEntropyConfig {
            snapshot_threshold: 5,
            ..Default::default()
        };
        let task = AntiEntropyTask::new(db, config);

        assert_eq!(task.snapshot_threshold(), 5);
    }

    #[test]
    fn test_anti_entropy_task_peer_state_mut() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        task.register_peer(addr);

        // Modify peer state through mutable reference
        if let Some(state) = task.peer_state_mut(&addr) {
            state.update_remote_seq(0, 100);
            state.increment_mismatch(1);
        }

        // Verify changes persisted
        if let Some(state) = task.peer_state(&addr) {
            assert_eq!(state.remote_seq(0), 100);
            assert_eq!(state.mismatch_count(1), 1);
        } else {
            panic!("Peer state should exist");
        }
    }

    #[test]
    fn test_anti_entropy_task_cycle_wrapping() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        // Set cycle to max - 1
        for _ in 0..(u32::MAX - 1) {
            task.increment_cycle();
            if task.cycle_count() == u32::MAX - 1 {
                break;
            }
        }

        // Force to max
        task.cycle_count = u32::MAX;
        task.increment_cycle();

        // Should wrap to 0
        assert_eq!(task.cycle_count(), 0);
    }
}
