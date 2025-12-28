use bytes::Bytes;
use parking_lot::RwLock;

use crate::config::Config;

/// Simple glob pattern matching for Redis KEYS/SCAN commands.
/// Supports: * (match any), ? (match single char), [...] (character class), \ (escape)
fn glob_match(pattern: &str, text: &[u8]) -> bool {
    let text_str = String::from_utf8_lossy(text);
    glob_match_impl(pattern.as_bytes(), text_str.as_bytes())
}

fn glob_match_impl(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0; // pattern index
    let mut ti = 0; // text index
    let mut star_pi = None; // last * position in pattern
    let mut star_ti = None; // text position when we hit *

    while ti < text.len() {
        if pi < pattern.len() {
            match pattern[pi] {
                b'*' => {
                    // Match zero or more characters
                    star_pi = Some(pi);
                    star_ti = Some(ti);
                    pi += 1;
                    continue;
                }
                b'?' => {
                    // Match exactly one character
                    pi += 1;
                    ti += 1;
                    continue;
                }
                b'\\' if pi + 1 < pattern.len() => {
                    // Escaped character
                    pi += 1;
                    if pattern[pi] == text[ti] {
                        pi += 1;
                        ti += 1;
                        continue;
                    }
                }
                b'[' => {
                    // Character class
                    if let Some((matched, new_pi)) = match_char_class(&pattern[pi..], text[ti]) {
                        if matched {
                            pi = pi + new_pi;
                            ti += 1;
                            continue;
                        }
                    }
                }
                c if c == text[ti] => {
                    // Exact match
                    pi += 1;
                    ti += 1;
                    continue;
                }
                _ => {}
            }
        }

        // No match - backtrack to last * if available
        if let (Some(spi), Some(sti)) = (star_pi, star_ti) {
            pi = spi + 1;
            star_ti = Some(sti + 1);
            ti = sti + 1;
        } else {
            return false;
        }
    }

    // Check remaining pattern (should only be *s)
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Match a character class like [abc] or [a-z] or [^abc]
/// Returns (matched, bytes_consumed) or None if invalid
fn match_char_class(pattern: &[u8], ch: u8) -> Option<(bool, usize)> {
    if pattern.is_empty() || pattern[0] != b'[' {
        return None;
    }

    let mut i = 1;
    let negated = if i < pattern.len() && pattern[i] == b'^' {
        i += 1;
        true
    } else {
        false
    };

    let mut matched = false;
    let mut prev_char = None;

    while i < pattern.len() && pattern[i] != b']' {
        if pattern[i] == b'-' && prev_char.is_some() && i + 1 < pattern.len() && pattern[i + 1] != b']' {
            // Range: a-z
            let start = prev_char.unwrap();
            let end = pattern[i + 1];
            if ch >= start && ch <= end {
                matched = true;
            }
            i += 2;
            prev_char = Some(end);
        } else {
            if pattern[i] == ch {
                matched = true;
            }
            prev_char = Some(pattern[i]);
            i += 1;
        }
    }

    if i < pattern.len() && pattern[i] == b']' {
        Some((if negated { !matched } else { matched }, i + 1))
    } else {
        None // Unclosed bracket
    }
}
use crate::engine::shard::QuotaResult;
use crate::engine::Shard;
use crate::persistence::{recover_shard, PersistenceConfig, PersistenceHandle};
use crate::replication::Delta;
use crate::types::{Key, NodeId};

/// Thread-safe sharded database.
///
/// Each shard is protected by its own RwLock to minimize contention.
/// Reads are concurrent within a shard; writes acquire exclusive access.
///
/// Keys are routed to shards using: shard_index = key.shard_hash() % num_shards
pub struct ShardedDb {
    shards: Vec<RwLock<Shard>>,
    num_shards: usize,
    node_id: NodeId,
    /// Optional persistence handle for WAL logging
    persistence: Option<PersistenceHandle>,
}

impl ShardedDb {
    /// Create a new ShardedDb with the given configuration.
    pub fn new(config: &Config) -> Self {
        let node_id = config.node_id();
        let num_shards = config.shards;

        let mut shards = Vec::with_capacity(num_shards);
        for id in 0..num_shards {
            shards.push(RwLock::new(Shard::new(id, node_id)));
        }

        Self {
            shards,
            num_shards,
            node_id,
            persistence: None,
        }
    }

    /// Create a new ShardedDb with optional recovery from persistence.
    pub fn with_persistence(
        config: &Config,
        persistence_config: &PersistenceConfig,
        persistence_handle: Option<PersistenceHandle>,
    ) -> Self {
        let node_id = config.node_id();
        let num_shards = config.shards;

        let shards: Vec<_> = if persistence_config.enabled {
            (0..num_shards)
                .map(|i| {
                    match recover_shard(persistence_config, i as u16, node_id) {
                        Ok((shard, seq)) => {
                            tracing::info!("Shard {} recovered at seq {}", i, seq);
                            RwLock::new(shard)
                        }
                        Err(e) => {
                            tracing::warn!("Shard {} recovery failed: {}, starting fresh", i, e);
                            RwLock::new(Shard::new(i, node_id))
                        }
                    }
                })
                .collect()
        } else {
            (0..num_shards)
                .map(|i| RwLock::new(Shard::new(i, node_id)))
                .collect()
        };

        Self {
            shards,
            num_shards,
            node_id,
            persistence: persistence_handle,
        }
    }

    /// Get the number of shards.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Get this node's ID.
    #[inline]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Route a key to its shard index.
    #[inline]
    fn shard_index(&self, key: &Key) -> usize {
        (key.shard_hash() as usize) % self.num_shards
    }

    /// Increment a key by delta, returning (shard_id, new_value, replication_delta).
    #[inline]
    pub fn increment(&self, key: Key, delta: u64) -> (u16, i64, Delta) {
        let idx = self.shard_index(&key);
        let (value, rep_delta) = self.shards[idx].write().increment(key.clone(), delta);
        let shard_id = idx as u16;

        // Log to WAL if persistence is enabled
        if let Some(ref handle) = self.persistence {
            handle.log_incr(shard_id, rep_delta.seq, &key, self.node_id.as_u32(), delta);
        }

        (shard_id, value, rep_delta)
    }

    /// Decrement a key by delta, returning (shard_id, new_value, replication_delta).
    #[inline]
    pub fn decrement(&self, key: Key, delta: u64) -> (u16, i64, Delta) {
        let idx = self.shard_index(&key);
        let (value, rep_delta) = self.shards[idx].write().decrement(key.clone(), delta);
        let shard_id = idx as u16;

        // Log to WAL if persistence is enabled
        if let Some(ref handle) = self.persistence {
            handle.log_decr(shard_id, rep_delta.seq, &key, self.node_id.as_u32(), delta);
        }

        (shard_id, value, rep_delta)
    }

    /// Apply a remote delta from replication.
    pub fn apply_delta(&self, shard_id: u16, delta: &Delta) {
        if (shard_id as usize) < self.num_shards {
            self.shards[shard_id as usize].write().apply_delta(delta);
        }
    }

    /// Get the value of a key.
    #[inline]
    pub fn get(&self, key: &Key) -> Option<i64> {
        let idx = self.shard_index(key);
        self.shards[idx].read().get(key)
    }

    /// Set a counter key to a specific value.
    pub fn set(&self, key: Key, value: i64) {
        let idx = self.shard_index(&key);
        let shard_id = idx as u16;
        let seq = self.shards[idx].write().set(key.clone(), value);

        // Log to WAL if persistence is enabled
        if let Some(ref handle) = self.persistence {
            handle.log_set(shard_id, seq, &key, value);
        }
    }

    /// Set a string value for a key (for caching JSON, HTML, JS, etc.).
    pub fn set_string(&self, key: Key, value: Bytes) {
        let idx = self.shard_index(&key);
        let shard_id = idx as u16;
        let (seq, timestamp) = self.shards[idx].write().set_string(key.clone(), value.clone());

        // Log to WAL if persistence is enabled
        if let Some(ref handle) = self.persistence {
            handle.log_set_string(shard_id, seq, &key, &value, timestamp);
        }
    }

    /// Optimized SET path - shard_id pre-computed, returns timestamp for replication.
    /// Avoids redundant shard_index computation and extra clones.
    #[inline]
    pub fn set_string_fast(&self, shard_id: u16, key: Key, value: Bytes) -> u64 {
        let idx = shard_id as usize;
        let timestamp = self.shards[idx].write().set_string_inline(key.clone(), value.clone());

        // Log to WAL if persistence is enabled
        if let Some(ref handle) = self.persistence {
            let seq = self.shards[idx].read().head_seq();
            handle.log_set_string(shard_id, seq, &key, &value, timestamp);
        }

        timestamp
    }

    /// Get a string value for a key.
    #[inline]
    pub fn get_string(&self, key: &Key) -> Option<Bytes> {
        let idx = self.shard_index(key);
        self.shards[idx].read().get_string(key).cloned()
    }

    /// Set a string value with a specific timestamp (for replication with LWW semantics).
    #[inline]
    pub fn set_string_with_timestamp(&self, key: Key, value: Bytes, timestamp: u64) {
        let idx = self.shard_index(&key);
        self.shards[idx].write().set_string_with_timestamp(key, value, timestamp);
        // Note: No WAL logging here - this is a replicated write
    }

    /// Get entry type for a key ("counter", "quota", or "string").
    pub fn get_entry_type(&self, key: &Key) -> Option<&'static str> {
        let idx = self.shard_index(key);
        self.shards[idx].read().get_entry_type(key)
    }

    // ========== Quota Operations ==========

    /// Set up a quota for a key.
    pub fn quota_set(&self, key: Key, limit: u64, window_secs: u64) {
        let idx = self.shard_index(&key);
        let shard_id = idx as u16;
        let seq = self.shards[idx].write().quota_set(key.clone(), limit, window_secs);

        // Log to WAL if persistence is enabled
        if let Some(ref handle) = self.persistence {
            handle.log_quota_set(shard_id, seq, &key, limit, window_secs);
        }
    }

    /// Get quota info for a key: (limit, window_secs, remaining).
    pub fn quota_get(&self, key: &Key) -> Option<(u64, u64, i64)> {
        let idx = self.shard_index(key);
        self.shards[idx].read().quota_get(key)
    }

    /// Delete a quota.
    pub fn quota_del(&self, key: &Key) -> bool {
        let idx = self.shard_index(key);
        self.shards[idx].write().quota_del(key)
    }

    /// Check if a key is a quota.
    pub fn is_quota(&self, key: &Key) -> bool {
        let idx = self.shard_index(key);
        self.shards[idx].read().is_quota(key)
    }

    /// Try to consume tokens from a quota.
    /// For single-node operation, this also acts as the allocator.
    pub fn quota_consume(&self, key: &Key, amount: u64) -> QuotaResult {
        let idx = self.shard_index(key);
        let shard_id = idx as u16;
        let mut shard = self.shards[idx].write();

        // First try local consumption
        let result = shard.quota_consume(key, amount);

        match result {
            QuotaResult::NeedTokens => {
                // In single-node mode, we are the allocator
                // Request tokens from ourselves
                let batch_size = shard.quota_batch_size(key);
                let (granted, seq_opt) = shard.allocator_grant(key, self.node_id, batch_size);

                // Log grant to WAL if persistence is enabled and tokens were granted
                if let (Some(ref handle), Some(seq)) = (&self.persistence, seq_opt) {
                    handle.log_quota_grant(shard_id, seq, key, self.node_id.as_u32(), granted);
                }

                if granted > 0 {
                    // Add tokens to local balance
                    shard.quota_add_tokens(key, granted);
                    // Try consumption again
                    shard.quota_consume(key, amount)
                } else {
                    // No tokens available from allocator
                    QuotaResult::Denied
                }
            }
            other => other,
        }
    }

    /// Set expiration for a key.
    pub fn expire(&self, key: Key, expires_at: u64) {
        let idx = self.shard_index(&key);
        self.shards[idx].write().expire(key, expires_at);
    }

    /// Run TTL expiration across all shards.
    /// Returns the total number of expired entries.
    pub fn expire_all(&self, current_ts: u64) -> usize {
        self.shards
            .iter()
            .map(|s| s.write().expire_entries(current_ts))
            .sum()
    }

    /// Get total entry count across all shards.
    pub fn total_entries(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Get all keys matching a glob pattern.
    /// Pattern supports: * (match any), ? (match single char), [...] (character class)
    pub fn keys(&self, pattern: &[u8]) -> Vec<Key> {
        let mut result = Vec::new();
        let pattern_str = String::from_utf8_lossy(pattern);

        for shard in &self.shards {
            let shard_guard = shard.read();
            for key in shard_guard.keys() {
                if glob_match(&pattern_str, key.as_bytes()) {
                    result.push(key.clone());
                }
            }
        }

        result
    }

    /// Scan keys with cursor-based iteration.
    /// Returns (next_cursor, keys) where next_cursor is 0 when scan is complete.
    ///
    /// The cursor encodes: (shard_index << 32) | position_within_shard
    pub fn scan(
        &self,
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> (u64, Vec<Key>) {
        let mut result = Vec::with_capacity(count);

        // Decode cursor: high 32 bits = shard index, low 32 bits = position within shard
        let mut shard_idx = (cursor >> 32) as usize;
        let mut pos_in_shard = (cursor & 0xFFFFFFFF) as usize;

        let pattern_str = pattern.map(|p| String::from_utf8_lossy(p));

        while result.len() < count && shard_idx < self.num_shards {
            let shard_guard = self.shards[shard_idx].read();
            let keys: Vec<&Key> = shard_guard.keys().collect();

            // Skip to current position
            for key in keys.iter().skip(pos_in_shard) {
                let matches = match &pattern_str {
                    Some(pat) => glob_match(pat, key.as_bytes()),
                    None => true,
                };

                if matches {
                    result.push((*key).clone());
                    if result.len() >= count {
                        // Return cursor pointing to next position
                        pos_in_shard += 1;
                        let next_cursor = if pos_in_shard >= keys.len() {
                            // Move to next shard
                            let next_shard = shard_idx + 1;
                            if next_shard >= self.num_shards {
                                0 // Scan complete
                            } else {
                                (next_shard as u64) << 32
                            }
                        } else {
                            ((shard_idx as u64) << 32) | (pos_in_shard as u64)
                        };
                        return (next_cursor, result);
                    }
                }
                pos_in_shard += 1;
            }

            // Move to next shard
            shard_idx += 1;
            pos_in_shard = 0;
        }

        // Scan complete
        (0, result)
    }

    /// Get per-shard entry counts (for metrics).
    pub fn shard_sizes(&self) -> Vec<usize> {
        self.shards.iter().map(|s| s.read().len()).collect()
    }

    /// Get the batch size for quota token requests.
    pub fn quota_batch_size(&self, key: &Key) -> u64 {
        let idx = self.shard_index(key);
        self.shards[idx].read().quota_batch_size(key)
    }

    /// Grant tokens from allocator to a node.
    /// Returns the number of tokens granted.
    pub fn allocator_grant(&self, key: &Key, to_node: NodeId, requested: u64) -> u64 {
        let idx = self.shard_index(key);
        let shard_id = idx as u16;
        let (granted, seq_opt) = self.shards[idx].write().allocator_grant(key, to_node, requested);

        // Log to WAL if persistence is enabled and tokens were granted
        if let (Some(ref handle), Some(seq)) = (&self.persistence, seq_opt) {
            handle.log_quota_grant(shard_id, seq, key, to_node.as_u32(), granted);
        }

        granted
    }

    /// Add tokens to local quota balance (received from remote allocator).
    pub fn quota_add_tokens(&self, key: &Key, amount: u64) {
        let idx = self.shard_index(key);
        self.shards[idx].write().quota_add_tokens(key, amount);
    }

    // ========== Anti-Entropy Accessor Methods ==========

    /// Get the replication log head sequence for a shard
    pub fn shard_head_seq(&self, shard_idx: usize) -> u64 {
        if shard_idx < self.num_shards {
            self.shards[shard_idx].read().head_seq()
        } else {
            0
        }
    }

    /// Get the digest for a shard
    pub fn shard_digest(&self, shard_idx: usize) -> u64 {
        if shard_idx < self.num_shards {
            self.shards[shard_idx].read().digest()
        } else {
            0
        }
    }

    /// Create a snapshot of a shard
    /// Returns (head_seq, digest, entries) where entries is a list of (key, value) pairs.
    pub fn create_shard_snapshot(&self, shard_idx: usize) -> Option<(u64, u64, Vec<(Key, i64)>)> {
        if shard_idx < self.num_shards {
            Some(self.shards[shard_idx].read().create_snapshot())
        } else {
            None
        }
    }

    /// Apply a snapshot to a shard
    pub fn apply_shard_snapshot(&self, shard_idx: usize, head_seq: u64, entries: Vec<(Key, i64)>) {
        if shard_idx < self.num_shards {
            self.shards[shard_idx].write().apply_snapshot(head_seq, entries);
        }
    }

    /// Create a persistence snapshot for a shard (for WAL/snapshot persistence).
    pub fn create_persistence_snapshot(&self, shard_id: u16) -> Option<crate::persistence::ShardSnapshot> {
        let shard_idx = shard_id as usize;
        if shard_idx < self.num_shards {
            Some(self.shards[shard_idx].read().create_persistence_snapshot())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_db() -> ShardedDb {
        let config = Config {
            shards: 4,
            node_id: 1,
            ..Default::default()
        };
        ShardedDb::new(&config)
    }

    #[test]
    fn test_increment() {
        let db = create_db();
        let key = Key::from("counter");

        assert_eq!(db.get(&key), None);
        let (_, val, _) = db.increment(key.clone(), 5);
        assert_eq!(val, 5);
        assert_eq!(db.get(&key), Some(5));
    }

    #[test]
    fn test_decrement() {
        let db = create_db();
        let key = Key::from("counter");

        let _ = db.increment(key.clone(), 10);
        let (_, val, _) = db.decrement(key.clone(), 3);
        assert_eq!(val, 7);
    }

    #[test]
    fn test_set() {
        let db = create_db();
        let key = Key::from("counter");

        db.set(key.clone(), 42);
        assert_eq!(db.get(&key), Some(42));
    }

    #[test]
    fn test_multiple_keys() {
        let db = create_db();

        for i in 0..100 {
            let key = Key::from(format!("key{}", i));
            let _ = db.increment(key, 1);
        }

        assert_eq!(db.total_entries(), 100);
    }

    #[test]
    fn test_consistent_routing() {
        let db = create_db();
        let key = Key::from("test_key");

        // Increment and decrement should go to the same shard
        let _ = db.increment(key.clone(), 10);
        let (_, val, _) = db.decrement(key.clone(), 3);
        assert_eq!(val, 7);
        assert_eq!(db.get(&key), Some(7));
    }

    #[test]
    fn test_expire_all() {
        let db = create_db();

        for i in 0..10 {
            let key = Key::from(format!("key{}", i));
            let _ = db.increment(key.clone(), 1);
            db.expire(key, 1000);
        }

        assert_eq!(db.total_entries(), 10);
        assert_eq!(db.expire_all(500), 0);
        assert_eq!(db.total_entries(), 10);
        assert_eq!(db.expire_all(1000), 10);
        assert_eq!(db.total_entries(), 0);
    }

    #[test]
    fn test_shard_distribution() {
        let db = create_db();

        // Add enough keys to spread across shards
        for i in 0..1000 {
            let key = Key::from(format!("key_{}", i));
            let _ = db.increment(key, 1);
        }

        let sizes = db.shard_sizes();
        assert_eq!(sizes.len(), 4);

        // Each shard should have some keys (not all in one)
        for size in sizes {
            assert!(size > 0, "Shard should have some keys");
            assert!(size < 1000, "All keys shouldn't be in one shard");
        }
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let db = Arc::new(create_db());
        let key = Key::from("concurrent");

        let mut handles = vec![];

        // Spawn 10 threads, each incrementing 100 times
        for _ in 0..10 {
            let db_clone = db.clone();
            let key_clone = key.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _ = db_clone.increment(key_clone.clone(), 1);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(db.get(&key), Some(1000));
    }

    #[test]
    fn test_apply_delta() {
        let db = create_db();
        let key = Key::from("counter");

        // Get the shard index for this key
        let shard_idx = (key.shard_hash() as usize) % db.num_shards();

        // Apply a remote delta to the correct shard
        let delta = Delta::increment(0, key.clone(), crate::types::NodeId::new(2), 10);
        db.apply_delta(shard_idx as u16, &delta);

        assert_eq!(db.get(&key), Some(10));
    }

    // ========== Anti-Entropy Accessor Tests ==========

    #[test]
    fn test_shard_head_seq() {
        let db = create_db();
        let key = Key::from("counter");

        // Initially, head_seq should be 0
        for i in 0..4 {
            assert_eq!(db.shard_head_seq(i), 0);
        }

        // Increment a key (this should increase head_seq for the shard)
        let (shard_id, _, _) = db.increment(key.clone(), 10);
        assert_eq!(db.shard_head_seq(shard_id as usize), 1);

        // Another increment on the same key
        let _ = db.increment(key.clone(), 5);
        assert_eq!(db.shard_head_seq(shard_id as usize), 2);

        // Out of bounds should return 0
        assert_eq!(db.shard_head_seq(100), 0);
    }

    #[test]
    fn test_shard_digest() {
        let db = create_db();
        let key = Key::from("counter");

        // Initially, digest should be 0 for all shards
        for i in 0..4 {
            assert_eq!(db.shard_digest(i), 0);
        }

        // Increment a key (this should change the digest for that shard)
        let (shard_id, _, _) = db.increment(key.clone(), 10);
        let digest = db.shard_digest(shard_id as usize);
        assert_ne!(digest, 0);

        // Out of bounds should return 0
        assert_eq!(db.shard_digest(100), 0);
    }

    #[test]
    fn test_create_shard_snapshot() {
        let db = create_db();
        let key1 = Key::from("counter1");
        let key2 = Key::from("counter2");

        // Add some data
        let (shard_id1, _, _) = db.increment(key1.clone(), 100);
        let (shard_id2, _, _) = db.increment(key2.clone(), 200);

        // Create snapshot for the first shard
        let snapshot = db.create_shard_snapshot(shard_id1 as usize);
        assert!(snapshot.is_some());

        let (head_seq, digest, entries) = snapshot.unwrap();
        assert!(head_seq > 0);

        // If both keys ended up in the same shard, entries should have 2 items
        // Otherwise, entries should have 1 item
        if shard_id1 == shard_id2 {
            assert_eq!(entries.len(), 2);
            assert_ne!(digest, 0);
        } else {
            assert_eq!(entries.len(), 1);
            assert_ne!(digest, 0);
        }

        // Out of bounds should return None
        assert!(db.create_shard_snapshot(100).is_none());
    }

    #[test]
    fn test_apply_shard_snapshot() {
        let db1 = create_db();
        let db2 = create_db();

        let key = Key::from("snapshot_test");

        // Add data to db1
        let (shard_id, _, _) = db1.increment(key.clone(), 42);
        let (head_seq, digest1, entries) = db1.create_shard_snapshot(shard_id as usize).unwrap();

        // Apply snapshot to db2
        db2.apply_shard_snapshot(shard_id as usize, head_seq, entries);

        // Data should now exist in db2
        assert_eq!(db2.get(&key), Some(42));

        // Digests should match
        let digest2 = db2.shard_digest(shard_id as usize);
        assert_eq!(digest1, digest2);

        // Head sequences should match
        assert_eq!(db2.shard_head_seq(shard_id as usize), head_seq);
    }

    #[test]
    fn test_snapshot_round_trip_db_level() {
        let db1 = create_db();

        // Add multiple keys that will go to different shards
        let keys: Vec<Key> = (0..20).map(|i| Key::from(format!("key_{}", i))).collect();
        let mut key_values: Vec<(Key, i64)> = Vec::new();

        for key in &keys {
            let (_, value, _) = db1.increment(key.clone(), 10);
            key_values.push((key.clone(), value));
        }

        // Create snapshots for all shards and apply to a new db
        let db2 = create_db();

        for shard_idx in 0..4 {
            if let Some((head_seq, _, entries)) = db1.create_shard_snapshot(shard_idx) {
                db2.apply_shard_snapshot(shard_idx, head_seq, entries);
            }
        }

        // Verify all keys have the same values
        for (key, expected_value) in key_values {
            assert_eq!(db2.get(&key), Some(expected_value));
        }
    }

    // ========== Glob Pattern Matching Tests ==========

    #[test]
    fn test_glob_match_star() {
        assert!(super::glob_match("*", b"anything"));
        assert!(super::glob_match("*", b""));
        assert!(super::glob_match("foo*", b"foobar"));
        assert!(super::glob_match("*bar", b"foobar"));
        assert!(super::glob_match("foo*bar", b"fooXXXbar"));
        assert!(!super::glob_match("foo*", b"barfoo"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(super::glob_match("?", b"a"));
        assert!(!super::glob_match("?", b""));
        assert!(!super::glob_match("?", b"ab"));
        assert!(super::glob_match("fo?", b"foo"));
        assert!(super::glob_match("f??", b"foo"));
    }

    #[test]
    fn test_glob_match_char_class() {
        assert!(super::glob_match("[abc]", b"a"));
        assert!(super::glob_match("[abc]", b"b"));
        assert!(!super::glob_match("[abc]", b"d"));
        assert!(super::glob_match("[a-z]", b"m"));
        assert!(!super::glob_match("[a-z]", b"5"));
        assert!(super::glob_match("[^abc]", b"d"));
        assert!(!super::glob_match("[^abc]", b"a"));
    }

    #[test]
    fn test_glob_match_complex() {
        assert!(super::glob_match("user:*:count", b"user:123:count"));
        assert!(super::glob_match("user:[0-9]*", b"user:12345"));
        assert!(super::glob_match("key_?_*", b"key_1_value"));
    }

    // ========== KEYS and SCAN Tests ==========

    #[test]
    fn test_keys_all() {
        let db = create_db();

        // Add some keys
        for i in 0..10 {
            let key = Key::from(format!("key_{}", i));
            let _ = db.increment(key, 1);
        }

        // KEYS * should return all keys
        let keys = db.keys(b"*");
        assert_eq!(keys.len(), 10);
    }

    #[test]
    fn test_keys_pattern() {
        let db = create_db();

        // Add mixed keys
        let _ = db.increment(Key::from("user:1"), 1);
        let _ = db.increment(Key::from("user:2"), 1);
        let _ = db.increment(Key::from("post:1"), 1);
        let _ = db.increment(Key::from("post:2"), 1);

        let user_keys = db.keys(b"user:*");
        assert_eq!(user_keys.len(), 2);

        let post_keys = db.keys(b"post:*");
        assert_eq!(post_keys.len(), 2);

        let all_1_keys = db.keys(b"*:1");
        assert_eq!(all_1_keys.len(), 2);
    }

    #[test]
    fn test_scan_basic() {
        let db = create_db();

        // Add some keys
        for i in 0..20 {
            let key = Key::from(format!("key_{}", i));
            let _ = db.increment(key, 1);
        }

        // Scan with count 5
        let mut all_keys = Vec::new();
        let mut cursor = 0u64;

        loop {
            let (next_cursor, keys) = db.scan(cursor, None, 5);
            all_keys.extend(keys);
            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        assert_eq!(all_keys.len(), 20);
    }

    #[test]
    fn test_scan_with_pattern() {
        let db = create_db();

        // Add mixed keys
        for i in 0..10 {
            let _ = db.increment(Key::from(format!("user:{}", i)), 1);
            let _ = db.increment(Key::from(format!("post:{}", i)), 1);
        }

        // Scan only user:* keys
        let mut user_keys = Vec::new();
        let mut cursor = 0u64;

        loop {
            let (next_cursor, keys) = db.scan(cursor, Some(b"user:*"), 100);
            user_keys.extend(keys);
            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        assert_eq!(user_keys.len(), 10);
        for key in &user_keys {
            assert!(key.as_bytes().starts_with(b"user:"));
        }
    }
}
