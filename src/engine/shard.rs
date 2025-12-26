use rustc_hash::FxHashMap;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

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

/// Individual shard containing a portion of the keyspace.
///
/// Each shard has:
/// - Its own HashMap for key-value storage
/// - Its own TTL queue for expiration
/// - Its own replication log for delta streaming
/// - The node ID for local CRDT operations
pub struct Shard {
    /// Shard ID for debugging/metrics
    id: usize,

    /// Key-value storage
    data: FxHashMap<Key, PnCounterEntry>,

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

    /// Increment a key by delta, returning the new value and replication delta
    #[inline]
    pub fn increment(&mut self, key: Key, delta: u64) -> (i64, Delta) {
        let entry = self.data.entry(key.clone()).or_default();
        entry.increment(self.node_id, delta);
        let rep_delta = self.rep_log.append_increment(key, self.node_id, delta);
        (entry.value(), rep_delta)
    }

    /// Decrement a key by delta, returning the new value and replication delta
    #[inline]
    pub fn decrement(&mut self, key: Key, delta: u64) -> (i64, Delta) {
        let entry = self.data.entry(key.clone()).or_default();
        entry.decrement(self.node_id, delta);
        let rep_delta = self.rep_log.append_decrement(key, self.node_id, delta);
        (entry.value(), rep_delta)
    }

    /// Get the current value of a key
    #[inline]
    pub fn get(&self, key: &Key) -> Option<i64> {
        self.data.get(key).map(|e| e.value())
    }

    /// Set a key to a specific value
    pub fn set(&mut self, key: Key, value: i64) {
        let entry = self.data.entry(key).or_default();
        entry.set_value(self.node_id, value);
    }

    /// Set expiration for a key
    pub fn expire(&mut self, key: Key, expires_at: u64) {
        if let Some(entry) = self.data.get_mut(&key) {
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
            if let Some(entry) = self.data.get(&ttl_entry.key) {
                if entry.is_expired(current_ts) {
                    self.data.remove(&ttl_entry.key);
                    expired += 1;
                }
            }
        }

        expired
    }

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

    /// Get the raw entry for a key (for replication)
    pub fn get_entry(&self, key: &Key) -> Option<&PnCounterEntry> {
        self.data.get(key)
    }

    /// Merge an entry from another replica
    pub fn merge_entry(&mut self, key: Key, other: &PnCounterEntry) {
        self.data
            .entry(key)
            .and_modify(|e| e.merge(other))
            .or_insert_with(|| other.clone());
    }

    /// Apply a remote delta from replication
    pub fn apply_delta(&mut self, delta: &Delta) {
        let entry = self.data.entry(delta.key.clone()).or_default();
        if delta.delta_p > 0 {
            entry.apply_remote_p(delta.node_id, delta.delta_p);
        }
        if delta.delta_n > 0 {
            entry.apply_remote_n(delta.node_id, delta.delta_n);
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
