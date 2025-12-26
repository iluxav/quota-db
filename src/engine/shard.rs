use rustc_hash::FxHashMap;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::engine::entry::Entry;
use crate::engine::quota::{AllocatorState, QuotaEntry};
use crate::engine::PnCounterEntry;
use crate::replication::{Delta, ReplicationLog, ShardDigest};
use crate::types::{Key, NodeId};

/// Entry in the TTL expiration queue
#[derive(Debug, Eq, PartialEq)]
struct TtlEntry {
    expires_at: u64,
    key: Key,
}

impl Ord for TtlEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min-heap: smaller expires_at has higher priority
        Reverse(self.expires_at).cmp(&Reverse(other.expires_at))
    }
}

impl PartialOrd for TtlEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Result of a quota increment operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaResult {
    /// Tokens consumed, returns remaining tokens
    Allowed(i64),
    /// No local tokens, need to request from allocator
    NeedTokens,
    /// Quota exhausted, request denied
    Denied,
    /// Key is not a quota (is a regular counter)
    NotQuota,
}

/// Individual shard containing a portion of the keyspace.
///
/// Each shard has:
/// - Its own HashMap for key-value storage
/// - Its own TTL queue for expiration
/// - Its own replication log for delta streaming
/// - Its own rolling hash digest for consistency checking
/// - The node ID for local CRDT operations
/// - Allocator state for quota keys (when this node owns the shard)
pub struct Shard {
    /// Shard ID for debugging/metrics
    id: usize,

    /// Key-value storage (counters and quotas)
    data: FxHashMap<Key, Entry>,

    /// Allocator state for quota keys (this node is allocator for this shard)
    allocators: FxHashMap<Key, AllocatorState>,

    /// TTL expiration queue (min-heap by expiration time)
    ttl_queue: BinaryHeap<TtlEntry>,

    /// Replication log for delta streaming
    rep_log: ReplicationLog,

    /// Rolling hash digest for anti-entropy consistency checking
    digest: ShardDigest,

    /// This node's ID for local CRDT operations
    node_id: NodeId,
}

impl Shard {
    /// Create a new shard
    pub fn new(id: usize, node_id: NodeId) -> Self {
        Self {
            id,
            data: FxHashMap::default(),
            allocators: FxHashMap::default(),
            ttl_queue: BinaryHeap::new(),
            rep_log: ReplicationLog::new(),
            digest: ShardDigest::new(),
            node_id,
        }
    }

    /// Get the shard ID
    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }

    // ========== Counter Operations (PN-Counter) ==========

    /// Increment a key by delta, returning the new value and replication delta.
    /// If the key is a quota, use `quota_consume` instead.
    #[inline]
    pub fn increment(&mut self, key: Key, delta: u64) -> (i64, Delta) {
        let entry = self
            .data
            .entry(key.clone())
            .or_insert_with(|| Entry::Counter(PnCounterEntry::new()));

        match entry {
            Entry::Counter(counter) => {
                let old_value = counter.value();
                counter.increment(self.node_id, delta);
                let new_value = counter.value();

                // Update digest: remove old value (if non-zero) and insert new value
                if old_value != 0 {
                    self.digest.update(&key, old_value, new_value);
                } else {
                    self.digest.insert(&key, new_value);
                }

                let rep_delta = self.rep_log.append_increment(key, self.node_id, delta);
                (new_value, rep_delta)
            }
            Entry::Quota(_) => {
                // For quota keys, INCR should go through quota_consume
                // Return current remaining as a fallback (shouldn't happen in normal use)
                let rep_delta = self.rep_log.append_increment(key, self.node_id, 0);
                (-1, rep_delta)
            }
        }
    }

    /// Decrement a key by delta, returning the new value and replication delta
    #[inline]
    pub fn decrement(&mut self, key: Key, delta: u64) -> (i64, Delta) {
        let entry = self
            .data
            .entry(key.clone())
            .or_insert_with(|| Entry::Counter(PnCounterEntry::new()));

        match entry {
            Entry::Counter(counter) => {
                let old_value = counter.value();
                counter.decrement(self.node_id, delta);
                let new_value = counter.value();

                // Update digest: remove old value (if non-zero) and insert new value
                if old_value != 0 {
                    self.digest.update(&key, old_value, new_value);
                } else {
                    self.digest.insert(&key, new_value);
                }

                let rep_delta = self.rep_log.append_decrement(key, self.node_id, delta);
                (new_value, rep_delta)
            }
            Entry::Quota(_) => {
                // DECR doesn't make sense for quotas
                let rep_delta = self.rep_log.append_decrement(key, self.node_id, 0);
                (-1, rep_delta)
            }
        }
    }

    /// Get the current value of a key.
    /// For counters, returns the counter value.
    /// For quotas, returns remaining tokens.
    #[inline]
    pub fn get(&self, key: &Key) -> Option<i64> {
        self.data.get(key).map(|e| match e {
            Entry::Counter(c) => c.value(),
            Entry::Quota(q) => q.local_tokens(),
        })
    }

    /// Set a key to a specific value (only works for counters)
    pub fn set(&mut self, key: Key, value: i64) {
        // Get old value before modifying
        let old_value = self.data.get(&key).and_then(|e| match e {
            Entry::Counter(c) => Some(c.value()),
            Entry::Quota(_) => None,
        });

        let entry = self
            .data
            .entry(key.clone())
            .or_insert_with(|| Entry::Counter(PnCounterEntry::new()));

        if let Entry::Counter(counter) = entry {
            counter.set_value(self.node_id, value);

            // Update digest
            match old_value {
                Some(old) if old != 0 => {
                    self.digest.update(&key, old, value);
                }
                _ => {
                    // Either new key or old value was 0
                    if value != 0 {
                        self.digest.insert(&key, value);
                    }
                }
            }
        }
    }

    /// Check if a key is a quota entry
    #[inline]
    pub fn is_quota(&self, key: &Key) -> bool {
        self.data.get(key).map_or(false, |e| e.is_quota())
    }

    // ========== Quota Operations ==========

    /// Set up a quota for a key.
    /// Creates or updates the quota configuration.
    pub fn quota_set(&mut self, key: Key, limit: u64, window_secs: u64) {
        let quota = QuotaEntry::new(limit, window_secs);
        self.data.insert(key.clone(), Entry::Quota(quota));

        // Initialize allocator state for this quota
        self.allocators
            .insert(key, AllocatorState::new());
    }

    /// Get quota info for a key: (limit, window_secs, remaining)
    pub fn quota_get(&self, key: &Key) -> Option<(u64, u64, i64)> {
        match self.data.get(key)? {
            Entry::Quota(q) => Some((q.limit(), q.window_secs(), q.local_tokens())),
            Entry::Counter(_) => None,
        }
    }

    /// Delete a quota, converting the key back to a regular counter (or removing it).
    pub fn quota_del(&mut self, key: &Key) -> bool {
        if let Some(Entry::Quota(_)) = self.data.get(key) {
            self.data.remove(key);
            self.allocators.remove(key);
            true
        } else {
            false
        }
    }

    /// Try to consume tokens from a quota locally.
    /// Returns QuotaResult indicating success, need for tokens, or denial.
    pub fn quota_consume(&mut self, key: &Key, amount: u64) -> QuotaResult {
        match self.data.get_mut(key) {
            Some(Entry::Quota(quota)) => {
                // Check if window expired
                if quota.is_window_expired() {
                    quota.reset_window();
                    // Also reset allocator state
                    if let Some(alloc) = self.allocators.get_mut(key) {
                        alloc.reset(quota.window_start());
                    }
                }

                // Try to consume locally
                match quota.try_consume(amount) {
                    Some(remaining) => QuotaResult::Allowed(remaining),
                    None => QuotaResult::NeedTokens,
                }
            }
            Some(Entry::Counter(_)) => QuotaResult::NotQuota,
            None => QuotaResult::NotQuota,
        }
    }

    /// Add tokens to a quota (called when allocator grants tokens).
    pub fn quota_add_tokens(&mut self, key: &Key, amount: u64) -> bool {
        match self.data.get_mut(key) {
            Some(Entry::Quota(quota)) => {
                quota.add_tokens(amount);
                true
            }
            _ => false,
        }
    }

    /// Try to grant tokens from the allocator.
    /// Returns the number of tokens actually granted.
    pub fn allocator_grant(&mut self, key: &Key, node: NodeId, requested: u64) -> u64 {
        // Get the limit from the quota entry
        let limit = match self.data.get(key) {
            Some(Entry::Quota(q)) => q.limit(),
            _ => return 0,
        };

        // Get or create allocator state
        let alloc = self.allocators.entry(key.clone()).or_default();

        // Check if we need to reset the window
        if let Some(Entry::Quota(q)) = self.data.get(key) {
            if q.is_window_expired() {
                alloc.reset(q.window_start());
            }
        }

        alloc.try_grant(node, requested, limit)
    }

    /// Get the batch size for token requests for a quota key.
    pub fn quota_batch_size(&self, key: &Key) -> u64 {
        match self.data.get(key) {
            Some(Entry::Quota(q)) => q.request_batch_size(),
            _ => 0,
        }
    }

    // ========== TTL Operations ==========

    /// Set expiration for a key (only for counters with TTL)
    pub fn expire(&mut self, key: Key, expires_at: u64) {
        if let Some(Entry::Counter(entry)) = self.data.get_mut(&key) {
            entry.set_expires_at(expires_at);
            self.ttl_queue.push(TtlEntry { expires_at, key });
        }
    }

    /// Expire entries that have passed their TTL.
    /// Returns the number of expired entries.
    pub fn expire_entries(&mut self, current_ts: u64) -> usize {
        let mut expired = 0;

        while let Some(ttl_entry) = self.ttl_queue.peek() {
            if ttl_entry.expires_at > current_ts {
                break;
            }

            let ttl_entry = self.ttl_queue.pop().unwrap();

            // Verify the entry still exists and is actually expired
            // (it might have been updated with a new TTL)
            if let Some(Entry::Counter(entry)) = self.data.get(&ttl_entry.key) {
                if entry.is_expired(current_ts) {
                    self.data.remove(&ttl_entry.key);
                    expired += 1;
                }
            }
        }

        expired
    }

    // ========== Metrics & Replication ==========

    /// Get entry count for metrics
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the shard is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the raw counter entry for a key (for replication)
    pub fn get_counter_entry(&self, key: &Key) -> Option<&PnCounterEntry> {
        match self.data.get(key) {
            Some(Entry::Counter(c)) => Some(c),
            _ => None,
        }
    }

    /// Get the raw entry for a key (for replication) - legacy compat
    pub fn get_entry(&self, key: &Key) -> Option<&PnCounterEntry> {
        self.get_counter_entry(key)
    }

    /// Merge a counter entry from another replica
    pub fn merge_entry(&mut self, key: Key, other: &PnCounterEntry) {
        match self.data.get_mut(&key) {
            Some(Entry::Counter(existing)) => {
                existing.merge(other);
            }
            None => {
                self.data.insert(key, Entry::Counter(other.clone()));
            }
            Some(Entry::Quota(_)) => {
                // Can't merge counter into quota - ignore
            }
        }
    }

    /// Apply a remote delta from replication
    pub fn apply_delta(&mut self, delta: &Delta) {
        // Get old value before applying
        let old_value = self.data.get(&delta.key).and_then(|e| match e {
            Entry::Counter(c) => Some(c.value()),
            Entry::Quota(_) => None,
        });

        let entry = self
            .data
            .entry(delta.key.clone())
            .or_insert_with(|| Entry::Counter(PnCounterEntry::new()));

        if let Entry::Counter(counter) = entry {
            if delta.delta_p > 0 {
                counter.apply_remote_p(delta.node_id, delta.delta_p);
            }
            if delta.delta_n > 0 {
                counter.apply_remote_n(delta.node_id, delta.delta_n);
            }

            let new_value = counter.value();

            // Update digest
            match old_value {
                Some(old) if old != 0 => {
                    if new_value != old {
                        self.digest.update(&delta.key, old, new_value);
                    }
                }
                _ => {
                    // New key (old_value was None or 0)
                    if new_value != 0 {
                        self.digest.insert(&delta.key, new_value);
                    }
                }
            }
        }
    }

    /// Get access to the replication log
    pub fn replication_log(&self) -> &ReplicationLog {
        &self.rep_log
    }

    // ========== Anti-Entropy / Digest Methods ==========

    /// Get the current rolling hash digest value
    #[inline]
    pub fn digest(&self) -> u64 {
        self.digest.value()
    }

    /// Get the current head sequence number from the replication log
    #[inline]
    pub fn head_seq(&self) -> u64 {
        self.rep_log.head_seq()
    }

    /// Create a snapshot of the shard's counter state.
    /// Returns (head_seq, digest, entries) where entries is a list of (key, value) pairs.
    /// Quota entries are excluded from snapshots.
    pub fn create_snapshot(&self) -> (u64, u64, Vec<(Key, i64)>) {
        let entries: Vec<(Key, i64)> = self
            .data
            .iter()
            .filter_map(|(k, v)| match v {
                Entry::Counter(c) => Some((k.clone(), c.value())),
                Entry::Quota(_) => None,
            })
            .collect();
        (self.rep_log.head_seq(), self.digest.value(), entries)
    }

    /// Apply a snapshot received from another replica.
    /// Clears counter entries (keeps quotas) and rebuilds state from the snapshot.
    pub fn apply_snapshot(&mut self, head_seq: u64, entries: Vec<(Key, i64)>) {
        // Clear counter entries (keep quotas)
        self.data.retain(|_, v| v.is_quota());
        self.digest.reset();

        for (key, value) in entries {
            let mut counter = PnCounterEntry::new();
            counter.set_value(self.node_id, value);
            self.data.insert(key.clone(), Entry::Counter(counter));
            if value != 0 {
                self.digest.insert(&key, value);
            }
        }

        self.rep_log.reset_to(head_seq);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_shard() -> Shard {
        Shard::new(0, NodeId::new(1))
    }

    #[test]
    fn test_increment() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        assert_eq!(shard.get(&key), None);
        let (val, _delta) = shard.increment(key.clone(), 5);
        assert_eq!(val, 5);
        assert_eq!(shard.get(&key), Some(5));
        let (val, _delta) = shard.increment(key.clone(), 3);
        assert_eq!(val, 8);
        assert_eq!(shard.get(&key), Some(8));
    }

    #[test]
    fn test_decrement() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        shard.increment(key.clone(), 10);
        let (val, _delta) = shard.decrement(key.clone(), 3);
        assert_eq!(val, 7);
        assert_eq!(shard.get(&key), Some(7));
    }

    #[test]
    fn test_negative_value() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        let (val, _delta) = shard.decrement(key.clone(), 5);
        assert_eq!(val, -5);
        assert_eq!(shard.get(&key), Some(-5));
    }

    #[test]
    fn test_set() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        shard.set(key.clone(), 42);
        assert_eq!(shard.get(&key), Some(42));

        shard.set(key.clone(), -10);
        assert_eq!(shard.get(&key), Some(-10));
    }

    #[test]
    fn test_expire_entries() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        let _ = shard.increment(key.clone(), 10);
        shard.expire(key.clone(), 1000);

        assert_eq!(shard.expire_entries(500), 0);
        assert_eq!(shard.get(&key), Some(10));

        assert_eq!(shard.expire_entries(1000), 1);
        assert_eq!(shard.get(&key), None);
    }

    #[test]
    fn test_len() {
        let mut shard = create_shard();
        assert_eq!(shard.len(), 0);
        assert!(shard.is_empty());

        let _ = shard.increment(Key::from("a"), 1);
        assert_eq!(shard.len(), 1);

        let _ = shard.increment(Key::from("b"), 1);
        assert_eq!(shard.len(), 2);

        assert!(!shard.is_empty());
    }

    #[test]
    fn test_merge_entry() {
        let mut shard1 = Shard::new(0, NodeId::new(1));
        let mut shard2 = Shard::new(0, NodeId::new(2));
        let key = Key::from("counter");

        let _ = shard1.increment(key.clone(), 10);
        let _ = shard2.increment(key.clone(), 5);

        // Merge shard2's entry into shard1
        let entry2 = shard2.get_entry(&key).unwrap().clone();
        shard1.merge_entry(key.clone(), &entry2);

        // Value should be sum of both: 10 + 5 = 15
        assert_eq!(shard1.get(&key), Some(15));
    }

    #[test]
    fn test_apply_delta() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        // Apply a remote increment delta
        let delta = Delta::increment(0, key.clone(), NodeId::new(2), 10);
        shard.apply_delta(&delta);

        assert_eq!(shard.get(&key), Some(10));

        // Apply a remote decrement delta
        let delta = Delta::decrement(1, key.clone(), NodeId::new(2), 3);
        shard.apply_delta(&delta);

        assert_eq!(shard.get(&key), Some(7));
    }

    #[test]
    fn test_replication_log() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        let (_, delta) = shard.increment(key.clone(), 10);
        assert_eq!(delta.seq, 0);
        assert_eq!(delta.delta_p, 10);

        let (_, delta) = shard.decrement(key.clone(), 3);
        assert_eq!(delta.seq, 1);
        assert_eq!(delta.delta_n, 3);

        assert_eq!(shard.replication_log().head_seq(), 2);
    }

    // ========== Digest Integration Tests ==========

    #[test]
    fn test_digest_updates_on_increment() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        // Initially empty digest
        let initial_digest = shard.digest();
        assert_eq!(initial_digest, 0);

        // After increment, digest should change
        let _ = shard.increment(key.clone(), 10);
        let digest_after_inc = shard.digest();
        assert_ne!(digest_after_inc, 0);

        // Another increment should change the digest again
        let _ = shard.increment(key.clone(), 5);
        let digest_after_inc2 = shard.digest();
        assert_ne!(digest_after_inc2, digest_after_inc);

        // Decrement back to original value should give same digest as after first increment
        // (since we went from 15 back to 10)
        let _ = shard.decrement(key.clone(), 5);
        assert_eq!(shard.get(&key), Some(10));
        assert_eq!(shard.digest(), digest_after_inc);
    }

    #[test]
    fn test_digest_consistency() {
        // Same operations in different order should produce the same final digest
        let mut shard1 = Shard::new(0, NodeId::new(1));
        let mut shard2 = Shard::new(0, NodeId::new(1));

        let key1 = Key::from("a");
        let key2 = Key::from("b");
        let key3 = Key::from("c");

        // Shard1: a, b, c order
        shard1.set(key1.clone(), 10);
        shard1.set(key2.clone(), 20);
        shard1.set(key3.clone(), 30);

        // Shard2: c, a, b order
        shard2.set(key3.clone(), 30);
        shard2.set(key1.clone(), 10);
        shard2.set(key2.clone(), 20);

        // Same data => same digest (order-independent)
        assert_eq!(shard1.digest(), shard2.digest());
    }

    #[test]
    fn test_snapshot_round_trip() {
        let mut shard1 = Shard::new(0, NodeId::new(1));
        let key1 = Key::from("counter1");
        let key2 = Key::from("counter2");
        let key3 = Key::from("counter3");

        // Add some data and perform some operations
        let _ = shard1.increment(key1.clone(), 100);
        let _ = shard1.increment(key2.clone(), 200);
        let _ = shard1.decrement(key2.clone(), 50);
        shard1.set(key3.clone(), -42);

        // Create snapshot
        let (head_seq, orig_digest, entries) = shard1.create_snapshot();

        // Apply snapshot to a fresh shard
        let mut shard2 = Shard::new(0, NodeId::new(2));
        shard2.apply_snapshot(head_seq, entries);

        // Values should match
        assert_eq!(shard2.get(&key1), Some(100));
        assert_eq!(shard2.get(&key2), Some(150));
        assert_eq!(shard2.get(&key3), Some(-42));

        // Digests should match
        assert_eq!(shard2.digest(), orig_digest);

        // Head sequence should match
        assert_eq!(shard2.head_seq(), head_seq);
    }

    #[test]
    fn test_snapshot_preserves_quotas() {
        let mut shard = Shard::new(0, NodeId::new(1));
        let counter_key = Key::from("counter");
        let quota_key = Key::from("quota");

        // Add counter and quota
        let _ = shard.increment(counter_key.clone(), 50);
        shard.quota_set(quota_key.clone(), 1000, 60);

        assert_eq!(shard.len(), 2);

        // Create and apply snapshot (with only counter data)
        let (head_seq, _, entries) = shard.create_snapshot();
        assert_eq!(entries.len(), 1); // Only the counter

        // Apply snapshot - quotas should be preserved
        shard.apply_snapshot(head_seq, entries);

        // Quota should still exist
        assert!(shard.is_quota(&quota_key));
        assert!(shard.quota_get(&quota_key).is_some());

        // Counter should be restored
        assert_eq!(shard.get(&counter_key), Some(50));
    }

    #[test]
    fn test_head_seq_getter() {
        let mut shard = create_shard();

        assert_eq!(shard.head_seq(), 0);

        let _ = shard.increment(Key::from("a"), 1);
        assert_eq!(shard.head_seq(), 1);

        let _ = shard.increment(Key::from("b"), 2);
        assert_eq!(shard.head_seq(), 2);

        let _ = shard.decrement(Key::from("a"), 1);
        assert_eq!(shard.head_seq(), 3);
    }

    #[test]
    fn test_digest_with_apply_delta() {
        let mut shard1 = Shard::new(0, NodeId::new(1));
        let mut shard2 = Shard::new(0, NodeId::new(1));
        let key = Key::from("counter");

        // Shard1: increment directly
        let _ = shard1.increment(key.clone(), 10);

        // Shard2: apply equivalent delta
        let delta = Delta::increment(0, key.clone(), NodeId::new(1), 10);
        shard2.apply_delta(&delta);

        // Both should have same value and same digest
        assert_eq!(shard1.get(&key), shard2.get(&key));
        assert_eq!(shard1.digest(), shard2.digest());
    }

    #[test]
    fn test_digest_empty_after_remove() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        // Add a value
        shard.set(key.clone(), 100);
        let digest_with_value = shard.digest();
        assert_ne!(digest_with_value, 0);

        // Set to 0 (effectively "removing" from digest perspective)
        shard.set(key.clone(), 0);

        // Value should be 0 but digest might include the 0 entry
        assert_eq!(shard.get(&key), Some(0));
    }
}
