# Phase 4: Anti-Entropy Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add gap detection and repair to QuotaDB's replication layer with watermark comparison and rolling hash digests.

**Architecture:** Hybrid anti-entropy with Level 1 (sequence watermarks every 5s) and Level 2 (rolling hash digests every 30s). Threshold-based snapshot resync after 2 consecutive mismatches.

**Tech Stack:** Rust, tokio, rustc-hash (FxHash), bytes

---

## Task 1: Rolling Hash Digest

**Files:**
- Create: `src/replication/digest.rs`
- Modify: `src/replication/mod.rs`

**Step 1: Write the failing test**

Create `src/replication/digest.rs`:

```rust
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

use crate::types::Key;

/// Rolling XOR hash digest for shard consistency checking.
///
/// Properties:
/// - Order-independent: same entries = same digest regardless of insertion order
/// - Reversible: insert then remove = original digest
/// - O(1) per update
#[derive(Debug, Clone, Default)]
pub struct ShardDigest {
    hash: u64,
}

impl ShardDigest {
    /// Create a new empty digest
    pub fn new() -> Self {
        Self { hash: 0 }
    }

    /// Get the current digest value
    #[inline]
    pub fn value(&self) -> u64 {
        self.hash
    }

    /// Compute hash for a key-value pair
    #[inline]
    fn entry_hash(key: &Key, value: i64) -> u64 {
        let mut hasher = FxHasher::default();
        key.as_bytes().hash(&mut hasher);
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Insert a new entry (XOR in)
    #[inline]
    pub fn insert(&mut self, key: &Key, value: i64) {
        self.hash ^= Self::entry_hash(key, value);
    }

    /// Remove an entry (XOR out)
    #[inline]
    pub fn remove(&mut self, key: &Key, value: i64) {
        // XOR is its own inverse
        self.hash ^= Self::entry_hash(key, value);
    }

    /// Update an entry (XOR out old, XOR in new)
    #[inline]
    pub fn update(&mut self, key: &Key, old_value: i64, new_value: i64) {
        self.hash ^= Self::entry_hash(key, old_value);
        self.hash ^= Self::entry_hash(key, new_value);
    }

    /// Reset digest to empty state
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
    }

    #[test]
    fn test_insert_changes_digest() {
        let mut digest = ShardDigest::new();
        let key = Key::from("test");

        digest.insert(&key, 100);
        assert_ne!(digest.value(), 0);
    }

    #[test]
    fn test_insert_remove_returns_to_zero() {
        let mut digest = ShardDigest::new();
        let key = Key::from("test");

        digest.insert(&key, 100);
        digest.remove(&key, 100);
        assert_eq!(digest.value(), 0);
    }

    #[test]
    fn test_order_independence() {
        let key1 = Key::from("key1");
        let key2 = Key::from("key2");
        let key3 = Key::from("key3");

        let mut digest1 = ShardDigest::new();
        digest1.insert(&key1, 10);
        digest1.insert(&key2, 20);
        digest1.insert(&key3, 30);

        let mut digest2 = ShardDigest::new();
        digest2.insert(&key3, 30);
        digest2.insert(&key1, 10);
        digest2.insert(&key2, 20);

        assert_eq!(digest1.value(), digest2.value());
    }

    #[test]
    fn test_update() {
        let mut digest = ShardDigest::new();
        let key = Key::from("test");

        digest.insert(&key, 100);
        let after_insert = digest.value();

        digest.update(&key, 100, 200);
        assert_ne!(digest.value(), after_insert);

        // Updating back should give original
        digest.update(&key, 200, 100);
        assert_eq!(digest.value(), after_insert);
    }

    #[test]
    fn test_different_values_different_digests() {
        let key = Key::from("test");

        let mut digest1 = ShardDigest::new();
        digest1.insert(&key, 100);

        let mut digest2 = ShardDigest::new();
        digest2.insert(&key, 101);

        assert_ne!(digest1.value(), digest2.value());
    }
}
```

**Step 2: Update mod.rs exports**

Add to `src/replication/mod.rs`:

```rust
mod digest;

pub use digest::ShardDigest;
```

**Step 3: Run tests to verify they pass**

Run: `cargo test replication::digest --lib`
Expected: All 6 tests PASS

**Step 4: Commit**

```bash
git add src/replication/digest.rs src/replication/mod.rs
git commit -m "feat(anti-entropy): add rolling hash digest for shard consistency"
```

---

## Task 2: Protocol Message Types

**Files:**
- Modify: `src/replication/protocol.rs`

**Step 1: Add new message types to MessageType enum**

In `src/replication/protocol.rs`, add to `MessageType` enum (after line 22):

```rust
    /// Status exchange with per-shard sequence numbers
    Status = 8,
    /// Request missing deltas for a shard
    DeltaRequest = 9,
    /// Digest exchange for consistency check
    DigestExchange = 10,
    /// Request full shard snapshot
    SnapshotRequest = 11,
    /// Full shard snapshot response
    Snapshot = 12,
```

**Step 2: Update from_u8 match**

Add to `from_u8` match (after line 34):

```rust
            8 => Some(Self::Status),
            9 => Some(Self::DeltaRequest),
            10 => Some(Self::DigestExchange),
            11 => Some(Self::SnapshotRequest),
            12 => Some(Self::Snapshot),
```

**Step 3: Add new Message variants**

Add to `Message` enum (after QuotaSync variant, around line 80):

```rust
    /// Status exchange with per-shard sequence numbers (anti-entropy Level 1)
    Status {
        /// (shard_id, head_seq) pairs
        shard_seqs: Vec<(u16, u64)>,
    },
    /// Request missing deltas for gap fill
    DeltaRequest {
        shard_id: u16,
        from_seq: u64,
        to_seq: u64,
    },
    /// Digest exchange for consistency check (anti-entropy Level 2)
    DigestExchange {
        shard_id: u16,
        head_seq: u64,
        digest: u64,
    },
    /// Request full shard snapshot
    SnapshotRequest {
        shard_id: u16,
    },
    /// Full shard snapshot response
    Snapshot {
        shard_id: u16,
        head_seq: u64,
        digest: u64,
        /// (key, value) pairs
        entries: Vec<(Key, i64)>,
    },
```

**Step 4: Add encode cases**

Add to `encode` match in `Message::encode` (before the closing brace of the match):

```rust
            Message::Status { shard_seqs } => {
                buf.put_u8(MessageType::Status as u8);
                buf.put_u16_le(shard_seqs.len() as u16);
                for (shard_id, seq) in shard_seqs {
                    buf.put_u16_le(*shard_id);
                    buf.put_u64_le(*seq);
                }
            }
            Message::DeltaRequest { shard_id, from_seq, to_seq } => {
                buf.put_u8(MessageType::DeltaRequest as u8);
                buf.put_u16_le(*shard_id);
                buf.put_u64_le(*from_seq);
                buf.put_u64_le(*to_seq);
            }
            Message::DigestExchange { shard_id, head_seq, digest } => {
                buf.put_u8(MessageType::DigestExchange as u8);
                buf.put_u16_le(*shard_id);
                buf.put_u64_le(*head_seq);
                buf.put_u64_le(*digest);
            }
            Message::SnapshotRequest { shard_id } => {
                buf.put_u8(MessageType::SnapshotRequest as u8);
                buf.put_u16_le(*shard_id);
            }
            Message::Snapshot { shard_id, head_seq, digest, entries } => {
                buf.put_u8(MessageType::Snapshot as u8);
                buf.put_u16_le(*shard_id);
                buf.put_u64_le(*head_seq);
                buf.put_u64_le(*digest);
                buf.put_u32_le(entries.len() as u32);
                for (key, value) in entries {
                    let key_bytes = key.as_bytes();
                    buf.put_u16_le(key_bytes.len() as u16);
                    buf.put_slice(key_bytes);
                    buf.put_i64_le(*value);
                }
            }
```

**Step 5: Add decode cases**

Add to `decode` match in `Message::decode` (before the closing brace of the match):

```rust
            MessageType::Status => {
                if buf.remaining() < 2 {
                    return None;
                }
                let count = buf.get_u16_le() as usize;
                if buf.remaining() < count * 10 {
                    return None;
                }
                let mut shard_seqs = Vec::with_capacity(count);
                for _ in 0..count {
                    let shard_id = buf.get_u16_le();
                    let seq = buf.get_u64_le();
                    shard_seqs.push((shard_id, seq));
                }
                Some(Message::Status { shard_seqs })
            }
            MessageType::DeltaRequest => {
                if buf.remaining() < 18 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                let from_seq = buf.get_u64_le();
                let to_seq = buf.get_u64_le();
                Some(Message::DeltaRequest { shard_id, from_seq, to_seq })
            }
            MessageType::DigestExchange => {
                if buf.remaining() < 18 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                let head_seq = buf.get_u64_le();
                let digest = buf.get_u64_le();
                Some(Message::DigestExchange { shard_id, head_seq, digest })
            }
            MessageType::SnapshotRequest => {
                if buf.remaining() < 2 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                Some(Message::SnapshotRequest { shard_id })
            }
            MessageType::Snapshot => {
                if buf.remaining() < 22 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                let head_seq = buf.get_u64_le();
                let digest = buf.get_u64_le();
                let count = buf.get_u32_le() as usize;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    if buf.remaining() < 2 {
                        return None;
                    }
                    let key_len = buf.get_u16_le() as usize;
                    if buf.remaining() < key_len + 8 {
                        return None;
                    }
                    let key = Key::new(buf.copy_to_bytes(key_len));
                    let value = buf.get_i64_le();
                    entries.push((key, value));
                }
                Some(Message::Snapshot { shard_id, head_seq, digest, entries })
            }
```

**Step 6: Add constructor methods**

Add after the existing constructor methods (around line 320):

```rust
    /// Create a Status message
    pub fn status(shard_seqs: Vec<(u16, u64)>) -> Self {
        Message::Status { shard_seqs }
    }

    /// Create a DeltaRequest message
    pub fn delta_request(shard_id: u16, from_seq: u64, to_seq: u64) -> Self {
        Message::DeltaRequest { shard_id, from_seq, to_seq }
    }

    /// Create a DigestExchange message
    pub fn digest_exchange(shard_id: u16, head_seq: u64, digest: u64) -> Self {
        Message::DigestExchange { shard_id, head_seq, digest }
    }

    /// Create a SnapshotRequest message
    pub fn snapshot_request(shard_id: u16) -> Self {
        Message::SnapshotRequest { shard_id }
    }

    /// Create a Snapshot message
    pub fn snapshot(shard_id: u16, head_seq: u64, digest: u64, entries: Vec<(Key, i64)>) -> Self {
        Message::Snapshot { shard_id, head_seq, digest, entries }
    }
```

**Step 7: Add tests for new message types**

Add to the tests module:

```rust
    #[test]
    fn test_status_encode_decode() {
        let msg = Message::status(vec![(0, 100), (1, 200), (2, 150)]);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::Status { shard_seqs } => {
                assert_eq!(shard_seqs.len(), 3);
                assert_eq!(shard_seqs[0], (0, 100));
                assert_eq!(shard_seqs[1], (1, 200));
                assert_eq!(shard_seqs[2], (2, 150));
            }
            _ => panic!("Expected Status message"),
        }
    }

    #[test]
    fn test_delta_request_encode_decode() {
        let msg = Message::delta_request(5, 100, 200);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::DeltaRequest { shard_id, from_seq, to_seq } => {
                assert_eq!(shard_id, 5);
                assert_eq!(from_seq, 100);
                assert_eq!(to_seq, 200);
            }
            _ => panic!("Expected DeltaRequest message"),
        }
    }

    #[test]
    fn test_digest_exchange_encode_decode() {
        let msg = Message::digest_exchange(3, 500, 0xDEADBEEF);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::DigestExchange { shard_id, head_seq, digest } => {
                assert_eq!(shard_id, 3);
                assert_eq!(head_seq, 500);
                assert_eq!(digest, 0xDEADBEEF);
            }
            _ => panic!("Expected DigestExchange message"),
        }
    }

    #[test]
    fn test_snapshot_request_encode_decode() {
        let msg = Message::snapshot_request(7);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::SnapshotRequest { shard_id } => {
                assert_eq!(shard_id, 7);
            }
            _ => panic!("Expected SnapshotRequest message"),
        }
    }

    #[test]
    fn test_snapshot_encode_decode() {
        let entries = vec![
            (Key::from("key1"), 100),
            (Key::from("key2"), -50),
            (Key::from("key3"), 0),
        ];
        let msg = Message::snapshot(2, 1000, 0xCAFEBABE, entries);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::Snapshot { shard_id, head_seq, digest, entries } => {
                assert_eq!(shard_id, 2);
                assert_eq!(head_seq, 1000);
                assert_eq!(digest, 0xCAFEBABE);
                assert_eq!(entries.len(), 3);
                assert_eq!(entries[0].0.as_bytes(), b"key1");
                assert_eq!(entries[0].1, 100);
                assert_eq!(entries[1].1, -50);
            }
            _ => panic!("Expected Snapshot message"),
        }
    }
```

**Step 8: Run tests**

Run: `cargo test replication::protocol --lib`
Expected: All tests PASS

**Step 9: Commit**

```bash
git add src/replication/protocol.rs
git commit -m "feat(anti-entropy): add Status, DeltaRequest, DigestExchange, Snapshot message types"
```

---

## Task 3: Integrate Digest into Shard

**Files:**
- Modify: `src/engine/shard.rs`

**Step 1: Add digest field to Shard struct**

Add import at top of `src/engine/shard.rs`:

```rust
use crate::replication::ShardDigest;
```

Add field to `Shard` struct (after `node_id` field, around line 69):

```rust
    /// Rolling hash digest for anti-entropy
    digest: ShardDigest,
```

**Step 2: Initialize digest in Shard::new**

In `Shard::new`, add to the struct initialization:

```rust
            digest: ShardDigest::new(),
```

**Step 3: Update increment to maintain digest**

In `increment` method, after `counter.increment(self.node_id, delta)` and before creating rep_delta:

```rust
                // Update digest
                let old_value = counter.value() - delta as i64;
                if old_value == 0 {
                    self.digest.insert(&key, counter.value());
                } else {
                    self.digest.update(&key, old_value, counter.value());
                }
```

Wait, this is wrong. Let me reconsider. The counter.value() is already the new value after increment. Let me fix:

Actually, we need to track properly. Let's get value before and after:

Replace the `Entry::Counter` arm in `increment`:

```rust
            Entry::Counter(counter) => {
                let old_value = counter.value();
                counter.increment(self.node_id, delta);
                let new_value = counter.value();

                // Update digest
                if old_value == 0 && self.data.get(&key).is_some() {
                    // Entry existed with value 0
                    self.digest.update(&key, old_value, new_value);
                } else if old_value == 0 {
                    // New entry
                    self.digest.insert(&key, new_value);
                } else {
                    self.digest.update(&key, old_value, new_value);
                }

                let rep_delta = self.rep_log.append_increment(key, self.node_id, delta);
                (new_value, rep_delta)
            }
```

Hmm, this is getting complex. Let me simplify - the entry always exists at this point because we just called `entry()`. So:

```rust
            Entry::Counter(counter) => {
                let old_value = counter.value();
                counter.increment(self.node_id, delta);
                let new_value = counter.value();

                // Update digest (XOR out old, XOR in new)
                if old_value != 0 {
                    self.digest.remove(&key, old_value);
                }
                self.digest.insert(&key, new_value);

                let rep_delta = self.rep_log.append_increment(key, self.node_id, delta);
                (new_value, rep_delta)
            }
```

**Step 4: Update decrement to maintain digest**

Similar change in `decrement`:

```rust
            Entry::Counter(counter) => {
                let old_value = counter.value();
                counter.decrement(self.node_id, delta);
                let new_value = counter.value();

                // Update digest
                if old_value != 0 {
                    self.digest.remove(&key, old_value);
                }
                self.digest.insert(&key, new_value);

                let rep_delta = self.rep_log.append_decrement(key, self.node_id, delta);
                (new_value, rep_delta)
            }
```

**Step 5: Update set to maintain digest**

In `set` method:

```rust
    pub fn set(&mut self, key: Key, value: i64) {
        let old_value = self.get(&key);

        let entry = self
            .data
            .entry(key.clone())
            .or_insert_with(|| Entry::Counter(PnCounterEntry::new()));

        if let Entry::Counter(counter) = entry {
            counter.set_value(self.node_id, value);

            // Update digest
            if let Some(old) = old_value {
                self.digest.remove(&key, old);
            }
            self.digest.insert(&key, value);
        }
    }
```

**Step 6: Update apply_delta to maintain digest**

In `apply_delta` method:

```rust
    pub fn apply_delta(&mut self, delta: &Delta) {
        let old_value = self.get(&delta.key);

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
            if let Some(old) = old_value {
                if old != new_value {
                    self.digest.remove(&delta.key, old);
                    self.digest.insert(&delta.key, new_value);
                }
            } else {
                self.digest.insert(&delta.key, new_value);
            }
        }
    }
```

**Step 7: Add digest getter method**

Add new method:

```rust
    /// Get the current shard digest for anti-entropy
    #[inline]
    pub fn digest(&self) -> u64 {
        self.digest.value()
    }
```

**Step 8: Add methods for snapshot support**

Add new methods:

```rust
    /// Create a snapshot of the shard for anti-entropy resync
    pub fn create_snapshot(&self) -> (u64, u64, Vec<(Key, i64)>) {
        let entries: Vec<(Key, i64)> = self.data
            .iter()
            .filter_map(|(k, v)| match v {
                Entry::Counter(c) => Some((k.clone(), c.value())),
                Entry::Quota(_) => None, // Don't include quotas in snapshot
            })
            .collect();

        (self.rep_log.head_seq(), self.digest.value(), entries)
    }

    /// Apply a snapshot from another node, replacing local counter state
    pub fn apply_snapshot(&mut self, head_seq: u64, entries: Vec<(Key, i64)>) {
        // Clear counter entries (keep quotas)
        self.data.retain(|_, v| v.is_quota());
        self.digest.reset();

        // Insert snapshot entries
        for (key, value) in entries {
            let mut counter = PnCounterEntry::new();
            counter.set_value(self.node_id, value);
            self.data.insert(key.clone(), Entry::Counter(counter));
            self.digest.insert(&key, value);
        }

        // Reset replication log to snapshot position
        self.rep_log.reset_to(head_seq);
    }

    /// Get the replication log head sequence
    #[inline]
    pub fn head_seq(&self) -> u64 {
        self.rep_log.head_seq()
    }
```

**Step 9: Add reset_to method to ReplicationLog**

In `src/replication/log.rs`, add method:

```rust
    /// Reset the log to a specific sequence (used after snapshot apply)
    pub fn reset_to(&mut self, seq: u64) {
        self.head_seq = seq;
        // Clear the buffer
        for slot in self.buffer.iter_mut() {
            *slot = None;
        }
    }
```

**Step 10: Add test for digest integration**

Add to shard.rs tests:

```rust
    #[test]
    fn test_digest_updates_on_increment() {
        let mut shard = create_shard();
        let key = Key::from("counter");

        let initial_digest = shard.digest();
        shard.increment(key.clone(), 10);

        assert_ne!(shard.digest(), initial_digest);
    }

    #[test]
    fn test_digest_consistency() {
        let mut shard1 = Shard::new(0, NodeId::new(1));
        let mut shard2 = Shard::new(0, NodeId::new(2));

        // Same operations, different order
        shard1.increment(Key::from("a"), 10);
        shard1.increment(Key::from("b"), 20);

        shard2.increment(Key::from("b"), 20);
        shard2.increment(Key::from("a"), 10);

        // Digests should match (order independent)
        assert_eq!(shard1.digest(), shard2.digest());
    }

    #[test]
    fn test_snapshot_round_trip() {
        let mut shard = create_shard();
        shard.increment(Key::from("a"), 10);
        shard.increment(Key::from("b"), 20);
        shard.decrement(Key::from("a"), 3);

        let (head_seq, digest, entries) = shard.create_snapshot();

        // Create new shard and apply snapshot
        let mut shard2 = Shard::new(0, NodeId::new(2));
        shard2.apply_snapshot(head_seq, entries);

        // Values and digest should match
        assert_eq!(shard2.get(&Key::from("a")), Some(7));
        assert_eq!(shard2.get(&Key::from("b")), Some(20));
        assert_eq!(shard2.digest(), digest);
    }
```

**Step 11: Run tests**

Run: `cargo test engine::shard --lib`
Expected: All tests PASS

**Step 12: Commit**

```bash
git add src/engine/shard.rs src/replication/log.rs
git commit -m "feat(anti-entropy): integrate rolling hash digest into Shard"
```

---

## Task 4: Anti-Entropy Configuration

**Files:**
- Create: `src/replication/anti_entropy.rs`
- Modify: `src/replication/mod.rs`

**Step 1: Create anti_entropy.rs with config**

Create `src/replication/anti_entropy.rs`:

```rust
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
    }
}
```

**Step 2: Update mod.rs exports**

Add to `src/replication/mod.rs`:

```rust
mod anti_entropy;

pub use anti_entropy::AntiEntropyConfig;
```

**Step 3: Run tests**

Run: `cargo test replication::anti_entropy --lib`
Expected: PASS

**Step 4: Commit**

```bash
git add src/replication/anti_entropy.rs src/replication/mod.rs
git commit -m "feat(anti-entropy): add AntiEntropyConfig"
```

---

## Task 5: Anti-Entropy Peer State

**Files:**
- Modify: `src/replication/anti_entropy.rs`

**Step 1: Add peer state tracking**

Add to `src/replication/anti_entropy.rs`:

```rust
use rustc_hash::FxHashSet;

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

    /// Increment mismatch count for a shard
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
```

**Step 2: Add tests**

Add to tests module:

```rust
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
```

**Step 3: Run tests**

Run: `cargo test replication::anti_entropy --lib`
Expected: All tests PASS

**Step 4: Commit**

```bash
git add src/replication/anti_entropy.rs
git commit -m "feat(anti-entropy): add PeerAntiEntropyState for tracking"
```

---

## Task 6: Anti-Entropy Task Structure

**Files:**
- Modify: `src/replication/anti_entropy.rs`
- Modify: `src/replication/mod.rs`

**Step 1: Add AntiEntropyTask struct**

Add to `src/replication/anti_entropy.rs` (add imports first):

```rust
use crate::engine::ShardedDb;
use crate::replication::Message;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
```

Add the task struct:

```rust
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
    /// Channel to send messages to replication manager
    msg_tx: mpsc::Sender<(SocketAddr, Message)>,
}

impl AntiEntropyTask {
    /// Create a new anti-entropy task
    pub fn new(
        db: Arc<ShardedDb>,
        config: AntiEntropyConfig,
        msg_tx: mpsc::Sender<(SocketAddr, Message)>,
    ) -> Self {
        let num_shards = db.num_shards();
        Self {
            db,
            config,
            num_shards,
            peer_state: HashMap::new(),
            cycle_count: 0,
            msg_tx,
        }
    }

    /// Register a peer for anti-entropy tracking
    pub fn register_peer(&mut self, addr: SocketAddr) {
        if !self.peer_state.contains_key(&addr) {
            self.peer_state.insert(addr, PeerAntiEntropyState::new(self.num_shards));
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
}
```

**Step 2: Add shard_head_seq to ShardedDb**

In `src/engine/db.rs`, add method:

```rust
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
    pub fn create_shard_snapshot(&self, shard_idx: usize) -> Option<(u64, u64, Vec<(crate::types::Key, i64)>)> {
        if shard_idx < self.num_shards {
            Some(self.shards[shard_idx].read().create_snapshot())
        } else {
            None
        }
    }

    /// Apply a snapshot to a shard
    pub fn apply_shard_snapshot(&self, shard_idx: usize, head_seq: u64, entries: Vec<(crate::types::Key, i64)>) {
        if shard_idx < self.num_shards {
            self.shards[shard_idx].write().apply_snapshot(head_seq, entries);
        }
    }
```

**Step 3: Update mod.rs exports**

Update `src/replication/mod.rs`:

```rust
pub use anti_entropy::{AntiEntropyConfig, AntiEntropyTask, PeerAntiEntropyState};
```

**Step 4: Run tests**

Run: `cargo test --lib`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add src/replication/anti_entropy.rs src/engine/db.rs src/replication/mod.rs
git commit -m "feat(anti-entropy): add AntiEntropyTask structure"
```

---

## Task 7: Anti-Entropy Message Handling

**Files:**
- Modify: `src/replication/anti_entropy.rs`

**Step 1: Add handle_status method**

Add to `AntiEntropyTask` impl:

```rust
    /// Handle incoming Status message from a peer
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
                // Digest mismatch - potential corruption
                let count = peer.increment_mismatch(shard_id);
                warn!(
                    "Shard {} digest mismatch with {} (count={}): local={:x} remote={:x}",
                    shard_id, from, count, local_digest, remote_digest
                );

                if count >= self.config.snapshot_threshold && !peer.is_snapshot_pending(shard_id) {
                    // Threshold reached - request snapshot
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

    /// Handle incoming Snapshot message
    pub fn handle_snapshot(
        &mut self,
        from: SocketAddr,
        shard_id: u16,
        head_seq: u64,
        _digest: u64,
        entries: Vec<(crate::types::Key, i64)>,
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

    /// Handle incoming SnapshotRequest - returns Snapshot message
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
```

**Step 2: Run compilation check**

Run: `cargo check`
Expected: No errors

**Step 3: Commit**

```bash
git add src/replication/anti_entropy.rs
git commit -m "feat(anti-entropy): add message handling methods to AntiEntropyTask"
```

---

## Task 8: Integrate Anti-Entropy into ReplicationManager

**Files:**
- Modify: `src/replication/manager.rs`

**Step 1: Add anti-entropy fields to ReplicationManager**

Add import at top:

```rust
use crate::replication::{AntiEntropyConfig, AntiEntropyTask};
use tokio::time::Interval;
```

Add fields to `ReplicationManager` struct (after `num_nodes`):

```rust
    /// Anti-entropy task
    anti_entropy: Option<AntiEntropyTask>,
    /// Anti-entropy configuration
    anti_entropy_config: AntiEntropyConfig,
```

**Step 2: Update ReplicationManager::new**

Add to struct initialization:

```rust
            anti_entropy: None,
            anti_entropy_config: AntiEntropyConfig::default(),
```

**Step 3: Add method to initialize anti-entropy**

Add method:

```rust
    /// Initialize anti-entropy with the database reference
    pub fn init_anti_entropy(&mut self) {
        if let Some(ref db) = self.db {
            let (msg_tx, _) = mpsc::channel(64); // We'll handle messages inline
            self.anti_entropy = Some(AntiEntropyTask::new(
                db.clone(),
                self.anti_entropy_config.clone(),
                msg_tx,
            ));
            info!("Anti-entropy initialized");
        }
    }
```

**Step 4: Update run() to include anti-entropy loop**

In the `run` method, add anti-entropy interval setup after `flush_interval`:

```rust
        let mut anti_entropy_interval = interval(self.anti_entropy_config.status_interval);
```

Add new select branch in the main loop (after flush_interval tick):

```rust
                // Anti-entropy cycle
                _ = anti_entropy_interval.tick() => {
                    if let Some(ref mut ae) = self.anti_entropy {
                        ae.increment_cycle();

                        // Build and broadcast status
                        let status_msg = ae.build_status_message();
                        for (addr, writer) in &mut self.writers {
                            if let Err(e) = writer.send(&status_msg).await {
                                warn!("Failed to send status to {}: {}", addr, e);
                            }
                        }

                        // If digest cycle, also send digests
                        if ae.should_check_digest() {
                            let digest_msgs = ae.build_digest_messages();
                            for (addr, writer) in &mut self.writers {
                                for msg in &digest_msgs {
                                    if let Err(e) = writer.send(msg).await {
                                        warn!("Failed to send digest to {}: {}", addr, e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
```

**Step 5: Handle new message types in handle_peer_message**

Add cases to the match in `handle_peer_message`:

```rust
            Message::Status { shard_seqs } => {
                if let Some(ref mut ae) = self.anti_entropy {
                    let responses = ae.handle_status(addr, shard_seqs);
                    // Send delta requests back
                    if let Some(writer) = self.writers.get_mut(&addr) {
                        for resp in responses {
                            if let Err(e) = writer.send(&resp).await {
                                warn!("Failed to send delta request to {}: {}", addr, e);
                            }
                        }
                    }
                }
                None
            }
            Message::DeltaRequest { shard_id, from_seq, to_seq } => {
                // Handle delta request - send requested deltas
                debug!("Received DeltaRequest for shard {} from {} to {}", shard_id, from_seq, to_seq);
                // TODO: Fetch deltas from replication log and send as DeltaBatch
                None
            }
            Message::DigestExchange { shard_id, head_seq, digest } => {
                if let Some(ref mut ae) = self.anti_entropy {
                    ae.handle_digest_exchange(addr, shard_id, head_seq, digest)
                } else {
                    None
                }
            }
            Message::SnapshotRequest { shard_id } => {
                if let Some(ref ae) = self.anti_entropy {
                    ae.handle_snapshot_request(shard_id)
                } else {
                    None
                }
            }
            Message::Snapshot { shard_id, head_seq, digest, entries } => {
                if let Some(ref mut ae) = self.anti_entropy {
                    ae.handle_snapshot(addr, shard_id, head_seq, digest, entries);
                }
                None
            }
```

Note: The `handle_peer_message` method signature needs to be async for sending responses. This requires refactoring. For now, we'll return the response and handle sending in the caller.

**Step 6: Update handle_peer_message signature**

The method already returns `Option<Message>`, so we can use that for single responses. For multiple responses (like Status), we need to handle differently.

Actually, looking at the existing code, responses are sent in the select loop after calling `handle_peer_message`. Let's keep that pattern but handle multiple responses.

Change return type to `Vec<Message>`:

```rust
    fn handle_peer_message(&mut self, addr: SocketAddr, msg: Message) -> Vec<Message> {
```

Update all existing returns to use `vec![]` or `vec![response]`.

**Step 7: Update caller in run()**

Change the message handling branch:

```rust
                // Receive messages from peers
                Some((addr, msg)) = msg_rx.recv() => {
                    let responses = self.handle_peer_message(addr, msg);
                    if !responses.is_empty() {
                        if let Some(writer) = self.writers.get_mut(&addr) {
                            for resp in responses {
                                if let Err(e) = writer.send(&resp).await {
                                    error!("Failed to send response to {}: {}", addr, e);
                                    break;
                                }
                            }
                        }
                    }
                }
```

**Step 8: Register/unregister peers in anti-entropy**

In the connection established handler:

```rust
                    if let Some(ref mut ae) = self.anti_entropy {
                        ae.register_peer(addr);
                    }
```

In the disconnect handler:

```rust
                    if let Some(ref mut ae) = self.anti_entropy {
                        ae.unregister_peer(addr);
                    }
```

**Step 9: Run compilation check**

Run: `cargo check`
Expected: Compilation may have errors that need fixing. Address them.

**Step 10: Commit**

```bash
git add src/replication/manager.rs
git commit -m "feat(anti-entropy): integrate AntiEntropyTask into ReplicationManager"
```

---

## Task 9: Run All Tests

**Step 1: Run full test suite**

Run: `cargo test --lib`
Expected: All tests PASS

**Step 2: Fix any failing tests**

If tests fail, fix the issues.

**Step 3: Commit any fixes**

```bash
git add -A
git commit -m "fix: address test failures from anti-entropy integration"
```

---

## Task 10: Benchmark Anti-Entropy Overhead

**Files:**
- No new files, use docker-compose

**Step 1: Start services**

```bash
docker compose -f docker/docker-compose.yml up -d
sleep 3
```

**Step 2: Run INCR benchmark**

```bash
redis-benchmark -p 6380 -c 50 -n 100000 -q -t incr
```

**Step 3: Compare with previous baseline**

Compare results with Phase 3 benchmark results.
Target: <2% overhead from anti-entropy.

**Step 4: Stop services**

```bash
docker compose -f docker/docker-compose.yml down
```

**Step 5: Document results**

Record benchmark results in the design doc or a separate benchmark results file.

**Step 6: Commit**

```bash
git add docs/
git commit -m "docs: add Phase 4 benchmark results"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Rolling hash digest | digest.rs |
| 2 | Protocol message types | protocol.rs |
| 3 | Integrate digest into Shard | shard.rs, log.rs |
| 4 | AntiEntropyConfig | anti_entropy.rs |
| 5 | PeerAntiEntropyState | anti_entropy.rs |
| 6 | AntiEntropyTask structure | anti_entropy.rs, db.rs |
| 7 | Message handling | anti_entropy.rs |
| 8 | Manager integration | manager.rs |
| 9 | Run all tests | - |
| 10 | Benchmark overhead | - |
