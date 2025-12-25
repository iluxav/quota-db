use parking_lot::RwLock;

use crate::config::Config;
use crate::engine::Shard;
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

    /// Increment a key by delta, returning the new value.
    #[inline]
    pub fn increment(&self, key: Key, delta: u64) -> i64 {
        let idx = self.shard_index(&key);
        self.shards[idx].write().increment(key, delta)
    }

    /// Decrement a key by delta, returning the new value.
    #[inline]
    pub fn decrement(&self, key: Key, delta: u64) -> i64 {
        let idx = self.shard_index(&key);
        self.shards[idx].write().decrement(key, delta)
    }

    /// Get the value of a key.
    #[inline]
    pub fn get(&self, key: &Key) -> Option<i64> {
        let idx = self.shard_index(key);
        self.shards[idx].read().get(key)
    }

    /// Set a key to a specific value.
    pub fn set(&self, key: Key, value: i64) {
        let idx = self.shard_index(&key);
        self.shards[idx].write().set(key, value);
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

    /// Get per-shard entry counts (for metrics).
    pub fn shard_sizes(&self) -> Vec<usize> {
        self.shards.iter().map(|s| s.read().len()).collect()
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
        assert_eq!(db.increment(key.clone(), 5), 5);
        assert_eq!(db.get(&key), Some(5));
    }

    #[test]
    fn test_decrement() {
        let db = create_db();
        let key = Key::from("counter");

        db.increment(key.clone(), 10);
        assert_eq!(db.decrement(key.clone(), 3), 7);
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
            db.increment(key, 1);
        }

        assert_eq!(db.total_entries(), 100);
    }

    #[test]
    fn test_consistent_routing() {
        let db = create_db();
        let key = Key::from("test_key");

        // Increment and decrement should go to the same shard
        db.increment(key.clone(), 10);
        assert_eq!(db.decrement(key.clone(), 3), 7);
        assert_eq!(db.get(&key), Some(7));
    }

    #[test]
    fn test_expire_all() {
        let db = create_db();

        for i in 0..10 {
            let key = Key::from(format!("key{}", i));
            db.increment(key.clone(), 1);
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
            db.increment(key, 1);
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
                    db_clone.increment(key_clone.clone(), 1);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(db.get(&key), Some(1000));
    }
}
