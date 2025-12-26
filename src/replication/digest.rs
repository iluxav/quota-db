//! Rolling hash digest for shard consistency checking.
//!
//! The `ShardDigest` provides an order-independent, reversible hash
//! for comparing shard state between replicas. It uses XOR to combine
//! entry hashes, allowing O(1) updates on insert, remove, and update.
//!
//! Properties:
//! - Order-independent: same entries = same digest regardless of insertion order
//! - Reversible: insert then remove = original digest
//! - O(1) per update using XOR

use crate::types::Key;
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

/// Rolling hash digest for shard consistency checking.
///
/// Uses XOR to combine entry hashes, making it:
/// - Order-independent (XOR is commutative and associative)
/// - Reversible (XOR is its own inverse)
/// - O(1) per update
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardDigest {
    hash: u64,
}

impl ShardDigest {
    /// Create a new empty digest.
    #[inline]
    pub fn new() -> Self {
        Self { hash: 0 }
    }

    /// Get the current digest value.
    #[inline]
    pub fn value(&self) -> u64 {
        self.hash
    }

    /// Compute the hash for a single entry (key, value pair).
    #[inline]
    fn entry_hash(key: &Key, value: i64) -> u64 {
        let mut hasher = FxHasher::default();
        key.as_bytes().hash(&mut hasher);
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Add an entry to the digest.
    #[inline]
    pub fn insert(&mut self, key: &Key, value: i64) {
        self.hash ^= Self::entry_hash(key, value);
    }

    /// Remove an entry from the digest.
    ///
    /// This is the inverse of insert - calling insert then remove
    /// with the same key/value returns the digest to its original state.
    #[inline]
    pub fn remove(&mut self, key: &Key, value: i64) {
        self.hash ^= Self::entry_hash(key, value);
    }

    /// Update an entry in the digest (remove old value, insert new value).
    ///
    /// This is equivalent to calling remove(key, old_value) then insert(key, new_value)
    /// but slightly more efficient as it combines the operations.
    #[inline]
    pub fn update(&mut self, key: &Key, old_value: i64, new_value: i64) {
        self.hash ^= Self::entry_hash(key, old_value);
        self.hash ^= Self::entry_hash(key, new_value);
    }

    /// Reset the digest to empty state.
    #[inline]
    pub fn reset(&mut self) {
        self.hash = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_digest() {
        let digest = ShardDigest::new();
        assert_eq!(digest.value(), 0);

        let default_digest = ShardDigest::default();
        assert_eq!(default_digest.value(), 0);
        assert_eq!(digest, default_digest);
    }

    #[test]
    fn test_insert_changes_digest() {
        let mut digest = ShardDigest::new();
        let key = Key::from("test_key");

        digest.insert(&key, 100);
        assert_ne!(digest.value(), 0);
    }

    #[test]
    fn test_insert_remove_returns_to_zero() {
        let mut digest = ShardDigest::new();
        let key = Key::from("test_key");
        let value = 42;

        digest.insert(&key, value);
        assert_ne!(digest.value(), 0);

        digest.remove(&key, value);
        assert_eq!(digest.value(), 0);
    }

    #[test]
    fn test_order_independence() {
        let key1 = Key::from("key1");
        let key2 = Key::from("key2");
        let key3 = Key::from("key3");

        // Insert in order: key1, key2, key3
        let mut digest1 = ShardDigest::new();
        digest1.insert(&key1, 10);
        digest1.insert(&key2, 20);
        digest1.insert(&key3, 30);

        // Insert in order: key3, key1, key2
        let mut digest2 = ShardDigest::new();
        digest2.insert(&key3, 30);
        digest2.insert(&key1, 10);
        digest2.insert(&key2, 20);

        // Insert in order: key2, key3, key1
        let mut digest3 = ShardDigest::new();
        digest3.insert(&key2, 20);
        digest3.insert(&key3, 30);
        digest3.insert(&key1, 10);

        // All should produce the same digest
        assert_eq!(digest1.value(), digest2.value());
        assert_eq!(digest2.value(), digest3.value());
    }

    #[test]
    fn test_update() {
        let key = Key::from("counter");

        // Method 1: Use update
        let mut digest1 = ShardDigest::new();
        digest1.insert(&key, 100);
        digest1.update(&key, 100, 200);

        // Method 2: Manual remove + insert
        let mut digest2 = ShardDigest::new();
        digest2.insert(&key, 100);
        digest2.remove(&key, 100);
        digest2.insert(&key, 200);

        // Both methods should produce the same result
        assert_eq!(digest1.value(), digest2.value());

        // And should be equivalent to just inserting the new value
        let mut digest3 = ShardDigest::new();
        digest3.insert(&key, 200);
        assert_eq!(digest1.value(), digest3.value());
    }

    #[test]
    fn test_different_values_different_digests() {
        let key = Key::from("same_key");

        let mut digest1 = ShardDigest::new();
        digest1.insert(&key, 100);

        let mut digest2 = ShardDigest::new();
        digest2.insert(&key, 200);

        // Same key with different values should produce different digests
        assert_ne!(digest1.value(), digest2.value());
    }

    #[test]
    fn test_different_keys_different_digests() {
        let key1 = Key::from("key_a");
        let key2 = Key::from("key_b");

        let mut digest1 = ShardDigest::new();
        digest1.insert(&key1, 100);

        let mut digest2 = ShardDigest::new();
        digest2.insert(&key2, 100);

        // Different keys with same value should produce different digests
        assert_ne!(digest1.value(), digest2.value());
    }

    #[test]
    fn test_reset() {
        let mut digest = ShardDigest::new();
        let key = Key::from("test");

        digest.insert(&key, 42);
        assert_ne!(digest.value(), 0);

        digest.reset();
        assert_eq!(digest.value(), 0);
    }

    #[test]
    fn test_clone() {
        let mut digest = ShardDigest::new();
        let key = Key::from("test");
        digest.insert(&key, 42);

        let cloned = digest.clone();
        assert_eq!(digest.value(), cloned.value());

        // Modifying original shouldn't affect clone
        digest.insert(&key, 100);
        assert_ne!(digest.value(), cloned.value());
    }

    #[test]
    fn test_multiple_inserts_removes() {
        let mut digest = ShardDigest::new();
        let key1 = Key::from("k1");
        let key2 = Key::from("k2");
        let key3 = Key::from("k3");

        // Insert all
        digest.insert(&key1, 1);
        digest.insert(&key2, 2);
        digest.insert(&key3, 3);
        let with_all = digest.value();

        // Remove middle one
        digest.remove(&key2, 2);
        let without_k2 = digest.value();
        assert_ne!(with_all, without_k2);

        // Re-insert it
        digest.insert(&key2, 2);
        assert_eq!(digest.value(), with_all);

        // Remove all in different order
        digest.remove(&key3, 3);
        digest.remove(&key1, 1);
        digest.remove(&key2, 2);
        assert_eq!(digest.value(), 0);
    }
}
