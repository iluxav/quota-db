# Phase 4: Anti-Entropy System Design

## Overview

The anti-entropy system provides gap detection and repair for QuotaDB's replication layer. It runs as a background task alongside the replication manager, performing periodic consistency checks at two levels:

1. **Watermark Comparison** - Fast sequence-based gap detection
2. **Digest Verification** - Rolling hash comparison for corruption detection

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Primary concern | Hybrid (availability + consistency) | Balance fast gap recovery with corruption detection |
| Digest algorithm | Rolling XOR hash | O(1) per update, minimal hot-path overhead |
| Resync trigger | Threshold-based (2 cycles) | Avoid unnecessary snapshots during brief partitions |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  ReplicationManager                      │
│  ┌─────────────────┐    ┌─────────────────────────────┐ │
│  │  Delta Streaming │    │     AntiEntropyTask        │ │
│  │  (existing)      │    │  ┌─────────────────────┐   │ │
│  │                  │    │  │ Level 1: Watermarks │   │ │
│  │                  │    │  │ (every 5s)          │   │ │
│  │                  │    │  ├─────────────────────┤   │ │
│  │                  │    │  │ Level 2: Digests    │   │ │
│  │                  │    │  │ (every 30s)         │   │ │
│  │                  │    │  └─────────────────────┘   │ │
│  └─────────────────┘    └─────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## New Components

```
src/replication/
├── anti_entropy.rs    # Main anti-entropy task
├── digest.rs          # Rolling hash digest computation
├── snapshot.rs        # Shard snapshot encode/decode
└── protocol.rs        # New message types
```

## Message Protocol

### New Message Types

```rust
enum Message {
    // ... existing messages ...

    /// Periodic status exchange (Level 1)
    Status {
        shard_seqs: Vec<(u16, u64)>,
    },

    /// Request missing deltas for a shard
    DeltaRequest {
        shard_id: u16,
        from_seq: u64,
        to_seq: u64,
    },

    /// Digest exchange (Level 2)
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
        entries: Vec<(Key, i64)>,
        digest: u64,
    },
}
```

## Data Structures

### Rolling Hash Digest

```rust
pub struct ShardDigest {
    hash: u64,  // running XOR of entry hashes
}

impl ShardDigest {
    fn update(&mut self, key: &Key, old_val: Option<i64>, new_val: i64);
    fn value(&self) -> u64;
}
```

Uses FxHash on (key_bytes, value_bytes) pairs, XORed together. Commutative and order-independent.

### Anti-Entropy State

```rust
pub struct AntiEntropyTask {
    db: Arc<ShardedDb>,
    config: AntiEntropyConfig,
    peer_state: HashMap<SocketAddr, PeerAntiEntropyState>,
}

pub struct PeerAntiEntropyState {
    remote_seqs: Vec<u64>,
    mismatch_count: Vec<u8>,
    snapshot_pending: HashSet<u16>,
}

pub struct AntiEntropyConfig {
    pub status_interval: Duration,        // default: 5s
    pub digest_every_n_cycles: u32,       // default: 6
    pub snapshot_threshold: u8,           // default: 2
}
```

## Task Flow

### Main Loop (every status_interval)

1. Broadcast `STATUS { shard_seqs }` to all peers

2. For each received `STATUS` from peer:
   - Compare their head_seq vs our head_seq per shard
   - If peer ahead: request `DeltaRequest { from, to }`
   - If peer behind within log: they'll request from us
   - If peer behind beyond log: increment mismatch_count

3. Every N cycles, broadcast `DigestExchange` per shard

4. For each received `DigestExchange`:
   - If seqs match but digest differs: increment mismatch_count
   - If seqs match and digest matches: reset mismatch_count to 0

5. If mismatch_count >= threshold: send `SnapshotRequest`

## Snapshot Handling

### Creation

```rust
impl Shard {
    pub fn create_snapshot(&self) -> ShardSnapshot {
        ShardSnapshot {
            shard_id: self.id,
            head_seq: self.replication_log.head_seq(),
            entries: self.data.iter()
                .map(|(k, v)| (k.clone(), v.value()))
                .collect(),
            digest: self.digest.value(),
        }
    }
}
```

### Application

```rust
impl Shard {
    pub fn apply_snapshot(&mut self, snapshot: ShardSnapshot) {
        self.data.clear();
        self.digest = ShardDigest::new();

        for (key, value) in snapshot.entries {
            self.data.insert(key.clone(), PnCounterEntry::from_value(value));
            self.digest.insert(&key, value);
        }

        self.replication_log.reset_to(snapshot.head_seq);
    }
}
```

## Error Handling

| Scenario | Action |
|----------|--------|
| Snapshot request timeout (10s) | Retry once, then log warning |
| Snapshot too large (>10MB) | Stream in chunks (future) |
| Snapshot apply fails | Keep old state, log error, retry |
| Peer disconnects mid-snapshot | Abort, retry on reconnect |

## Testing Strategy

### Unit Tests

- `digest.rs`: empty, insert/update/remove, order independence, XOR reversibility
- `anti_entropy.rs`: gap detection, delta request, mismatch counter, snapshot trigger
- `snapshot.rs`: create, apply, round-trip

### Integration Tests

- Three-node partition recovery
- Silent corruption detection via digest
- Large gap triggers snapshot (>4096 deltas)

### Benchmark

- Baseline INCR throughput vs with anti-entropy
- Target: <2% overhead on hot path

## Implementation Steps

| Step | Component | Description |
|------|-----------|-------------|
| 1 | `digest.rs` | Rolling hash with insert/update/remove |
| 2 | `protocol.rs` | Add 5 new message types + encoding |
| 3 | `shard.rs` | Integrate ShardDigest, update on mutations |
| 4 | `snapshot.rs` | Create/apply snapshot functions |
| 5 | `anti_entropy.rs` | Main task with config and state |
| 6 | `manager.rs` | Spawn anti-entropy task, route new messages |
| 7 | Unit tests | Test each component |
| 8 | Integration tests | Multi-node scenarios |
| 9 | Benchmark | Verify <2% overhead |
