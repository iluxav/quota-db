use rustc_hash::FxHashMap;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::engine::entry::Entry;
use crate::engine::quota::{AllocatorState, QuotaEntry};
use crate::engine::PnCounterEntry;
use crate::replication::{Delta, ReplicationLog};
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
                counter.increment(self.node_id, delta);
                let rep_delta = self.rep_log.append_increment(key, self.node_id, delta);
                (counter.value(), rep_delta)
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
                counter.decrement(self.node_id, delta);
                let rep_delta = self.rep_log.append_decrement(key, self.node_id, delta);
                (counter.value(), rep_delta)
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
        let entry = self
            .data
            .entry(key)
            .or_insert_with(|| Entry::Counter(PnCounterEntry::new()));

        if let Entry::Counter(counter) = entry {
            counter.set_value(self.node_id, value);
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
        }
    }

    /// Get access to the replication log
    pub fn replication_log(&self) -> &ReplicationLog {
        &self.rep_log
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
}
