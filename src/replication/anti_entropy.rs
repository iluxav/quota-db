use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustc_hash::FxHashSet;
use tracing::{debug, info, warn};

use crate::engine::ShardedDb;
use crate::replication::Message;
use crate::types::Key;

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

    // ========== Message Handling Methods ==========

    /// Handle incoming Status message from a peer
    /// Returns delta requests for shards where we're behind
    pub fn handle_status(&mut self, from: SocketAddr, shard_seqs: Vec<(u16, u64)>) -> Vec<Message> {
        let mut responses = Vec::new();

        let peer = self.peer_state.entry(from).or_insert_with(|| {
            PeerAntiEntropyState::new(self.num_shards)
        });

        for (shard_id, remote_seq) in shard_seqs {
            let shard_idx = shard_id as usize;
            if shard_idx >= self.num_shards {
                continue;
            }

            peer.update_remote_seq(shard_id, remote_seq);
            let local_seq = self.db.shard_head_seq(shard_idx);

            if local_seq < remote_seq {
                // We're behind - request missing deltas
                debug!(
                    "Shard {} behind: local={} remote={}, requesting deltas",
                    shard_id, local_seq, remote_seq
                );
                responses.push(Message::delta_request(shard_id, local_seq, remote_seq));
            }
        }

        responses
    }

    /// Handle incoming DigestExchange message from a peer
    /// Returns SnapshotRequest if threshold exceeded
    pub fn handle_digest_exchange(
        &mut self,
        from: SocketAddr,
        shard_id: u16,
        remote_seq: u64,
        remote_digest: u64,
    ) -> Option<Message> {
        let shard_idx = shard_id as usize;
        if shard_idx >= self.num_shards {
            return None;
        }

        let local_seq = self.db.shard_head_seq(shard_idx);
        let local_digest = self.db.shard_digest(shard_idx);

        let peer = self.peer_state.entry(from).or_insert_with(|| {
            PeerAntiEntropyState::new(self.num_shards)
        });

        // Only compare if sequences match
        if local_seq == remote_seq {
            if local_digest != remote_digest {
                // Digest mismatch
                let count = peer.increment_mismatch(shard_id);
                warn!(
                    "Shard {} digest mismatch with {} (count={}): local={:x} remote={:x}",
                    shard_id, from, count, local_digest, remote_digest
                );

                if count >= self.config.snapshot_threshold && !peer.is_snapshot_pending(shard_id) {
                    info!("Shard {} requesting snapshot from {} after {} mismatches",
                          shard_id, from, count);
                    peer.mark_snapshot_pending(shard_id);
                    return Some(Message::snapshot_request(shard_id));
                }
            } else {
                // Digests match - reset mismatch count
                peer.reset_mismatch(shard_id);
            }
        }

        None
    }

    /// Handle incoming Snapshot message - apply to local shard
    pub fn handle_snapshot(
        &mut self,
        from: SocketAddr,
        shard_id: u16,
        head_seq: u64,
        _digest: u64,
        entries: Vec<(Key, i64)>,
    ) {
        let shard_idx = shard_id as usize;
        if shard_idx >= self.num_shards {
            return;
        }

        info!(
            "Applying snapshot for shard {} from {}: {} entries, seq={}",
            shard_id, from, entries.len(), head_seq
        );

        self.db.apply_shard_snapshot(shard_idx, head_seq, entries);

        // Clear pending state
        if let Some(peer) = self.peer_state.get_mut(&from) {
            peer.clear_snapshot_pending(shard_id);
            peer.reset_mismatch(shard_id);
        }
    }

    /// Handle incoming SnapshotRequest - create and return snapshot
    pub fn handle_snapshot_request(&self, shard_id: u16) -> Option<Message> {
        let shard_idx = shard_id as usize;
        if shard_idx >= self.num_shards {
            return None;
        }

        if let Some((head_seq, digest, entries)) = self.db.create_shard_snapshot(shard_idx) {
            info!("Creating snapshot for shard {}: {} entries", shard_id, entries.len());
            Some(Message::snapshot(shard_id, head_seq, digest, entries))
        } else {
            None
        }
    }

    /// Build digest exchange messages for all shards
    pub fn build_digest_messages(&self) -> Vec<Message> {
        (0..self.num_shards)
            .map(|i| {
                let shard_id = i as u16;
                let head_seq = self.db.shard_head_seq(i);
                let digest = self.db.shard_digest(i);
                Message::digest_exchange(shard_id, head_seq, digest)
            })
            .collect()
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

    // ========== Message Handling Tests ==========

    #[test]
    fn test_handle_status_detects_gap() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // Peer is ahead on shard 0
        let responses = task.handle_status(peer, vec![(0, 100)]);

        // Should request deltas
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            Message::DeltaRequest { shard_id, from_seq, to_seq } => {
                assert_eq!(*shard_id, 0);
                assert_eq!(*from_seq, 0);
                assert_eq!(*to_seq, 100);
            }
            _ => panic!("Expected DeltaRequest"),
        }
    }

    #[test]
    fn test_handle_status_no_gap() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // Peer has same seq as us (0)
        let responses = task.handle_status(peer, vec![(0, 0)]);

        // Should not request any deltas
        assert!(responses.is_empty());
    }

    #[test]
    fn test_handle_status_multiple_shards() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // Peer is ahead on shards 0 and 2
        let responses = task.handle_status(peer, vec![(0, 50), (1, 0), (2, 75), (3, 0)]);

        // Should request deltas for shards 0 and 2
        assert_eq!(responses.len(), 2);

        // Check shard 0 request
        match &responses[0] {
            Message::DeltaRequest { shard_id, from_seq, to_seq } => {
                assert_eq!(*shard_id, 0);
                assert_eq!(*from_seq, 0);
                assert_eq!(*to_seq, 50);
            }
            _ => panic!("Expected DeltaRequest for shard 0"),
        }

        // Check shard 2 request
        match &responses[1] {
            Message::DeltaRequest { shard_id, from_seq, to_seq } => {
                assert_eq!(*shard_id, 2);
                assert_eq!(*from_seq, 0);
                assert_eq!(*to_seq, 75);
            }
            _ => panic!("Expected DeltaRequest for shard 2"),
        }
    }

    #[test]
    fn test_handle_status_out_of_bounds_shard() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // Peer reports a shard id that's out of bounds (we only have 4 shards)
        let responses = task.handle_status(peer, vec![(100, 50)]);

        // Should not request any deltas for out-of-bounds shard
        assert!(responses.is_empty());
    }

    #[test]
    fn test_handle_digest_mismatch_triggers_snapshot() {
        let db = create_test_db();
        let mut config = AntiEntropyConfig::default();
        config.snapshot_threshold = 2;
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // First mismatch - no snapshot yet
        let resp1 = task.handle_digest_exchange(peer, 0, 0, 0xDEAD);
        assert!(resp1.is_none());

        // Second mismatch - should trigger snapshot request
        let resp2 = task.handle_digest_exchange(peer, 0, 0, 0xDEAD);
        assert!(resp2.is_some());
        match resp2.unwrap() {
            Message::SnapshotRequest { shard_id } => assert_eq!(shard_id, 0),
            _ => panic!("Expected SnapshotRequest"),
        }
    }

    #[test]
    fn test_handle_digest_match_resets_mismatch() {
        let db = create_test_db();
        let mut config = AntiEntropyConfig::default();
        config.snapshot_threshold = 3;
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // Two mismatches
        task.handle_digest_exchange(peer, 0, 0, 0xDEAD);
        task.handle_digest_exchange(peer, 0, 0, 0xDEAD);

        // Now a match (digest = 0, which is what the empty shard has)
        let local_digest = task.db.shard_digest(0);
        task.handle_digest_exchange(peer, 0, 0, local_digest);

        // Mismatch count should be reset
        assert_eq!(task.peer_state(&peer).unwrap().mismatch_count(0), 0);

        // Another mismatch should not trigger snapshot (count reset)
        let resp = task.handle_digest_exchange(peer, 0, 0, 0xDEAD);
        assert!(resp.is_none());
    }

    #[test]
    fn test_handle_digest_seq_mismatch_no_compare() {
        let db = create_test_db();
        let mut config = AntiEntropyConfig::default();
        config.snapshot_threshold = 1;
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // Digest mismatch but sequences don't match - should not trigger snapshot
        // Local seq is 0, remote seq is 100
        let resp = task.handle_digest_exchange(peer, 0, 100, 0xDEAD);
        assert!(resp.is_none());

        // Mismatch count should not be incremented
        assert_eq!(task.peer_state(&peer).unwrap().mismatch_count(0), 0);
    }

    #[test]
    fn test_handle_digest_out_of_bounds_shard() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // Out of bounds shard
        let resp = task.handle_digest_exchange(peer, 100, 0, 0xDEAD);
        assert!(resp.is_none());
    }

    #[test]
    fn test_handle_snapshot_request() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let task = AntiEntropyTask::new(db, config);

        let response = task.handle_snapshot_request(0);
        assert!(response.is_some());
        match response.unwrap() {
            Message::Snapshot { shard_id, .. } => assert_eq!(shard_id, 0),
            _ => panic!("Expected Snapshot"),
        }
    }

    #[test]
    fn test_handle_snapshot_request_out_of_bounds() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let task = AntiEntropyTask::new(db, config);

        // Out of bounds shard
        let response = task.handle_snapshot_request(100);
        assert!(response.is_none());
    }

    #[test]
    fn test_handle_snapshot_applies_data() {
        use crate::types::Key;

        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        task.register_peer(peer);

        // Mark snapshot as pending to verify it gets cleared
        task.peer_state_mut(&peer).unwrap().mark_snapshot_pending(0);
        task.peer_state_mut(&peer).unwrap().increment_mismatch(0);

        // Apply a snapshot with some data
        let entries = vec![
            (Key::from("key1"), 100),
            (Key::from("key2"), 200),
        ];
        task.handle_snapshot(peer, 0, 50, 0xCAFE, entries);

        // Verify snapshot pending is cleared
        assert!(!task.peer_state(&peer).unwrap().is_snapshot_pending(0));

        // Verify mismatch count is reset
        assert_eq!(task.peer_state(&peer).unwrap().mismatch_count(0), 0);

        // Verify head_seq is updated
        assert_eq!(task.db.shard_head_seq(0), 50);
    }

    #[test]
    fn test_handle_snapshot_out_of_bounds_shard() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // Out of bounds shard - should be a no-op
        task.handle_snapshot(peer, 100, 50, 0xCAFE, vec![]);
    }

    #[test]
    fn test_build_digest_messages() {
        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let task = AntiEntropyTask::new(db, config);

        let messages = task.build_digest_messages();

        // Should have one message per shard
        assert_eq!(messages.len(), 4);

        for (i, msg) in messages.iter().enumerate() {
            match msg {
                Message::DigestExchange { shard_id, head_seq, digest } => {
                    assert_eq!(*shard_id, i as u16);
                    assert_eq!(*head_seq, 0);
                    assert_eq!(*digest, 0);
                }
                _ => panic!("Expected DigestExchange message"),
            }
        }
    }

    #[test]
    fn test_build_digest_messages_with_data() {
        use crate::types::Key;

        let db = create_test_db();
        let config = AntiEntropyConfig::default();
        let task = AntiEntropyTask::new(Arc::clone(&db), config);

        // Add some data to change the digest
        let key = Key::from("test_key");
        let (shard_id, _, _) = db.increment(key, 100);

        let messages = task.build_digest_messages();

        // The shard with data should have non-zero head_seq and digest
        for msg in messages {
            match msg {
                Message::DigestExchange { shard_id: sid, head_seq, digest } => {
                    if sid == shard_id {
                        assert_eq!(head_seq, 1);
                        assert_ne!(digest, 0);
                    }
                }
                _ => panic!("Expected DigestExchange message"),
            }
        }
    }

    #[test]
    fn test_snapshot_pending_prevents_duplicate_request() {
        let db = create_test_db();
        let mut config = AntiEntropyConfig::default();
        config.snapshot_threshold = 1;
        let mut task = AntiEntropyTask::new(db, config);

        let peer: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        // First mismatch - triggers snapshot request
        let resp1 = task.handle_digest_exchange(peer, 0, 0, 0xDEAD);
        assert!(resp1.is_some());

        // Second mismatch - should NOT trigger another request (pending)
        let resp2 = task.handle_digest_exchange(peer, 0, 0, 0xDEAD);
        assert!(resp2.is_none());
    }
}
