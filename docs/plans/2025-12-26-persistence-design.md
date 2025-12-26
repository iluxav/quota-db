# Phase 5: Persistence Layer Design

## Overview

The persistence layer adds durability to QuotaDB through Write-Ahead Logging (WAL) and periodic snapshots, ensuring data survives restarts and crashes.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Durability approach | WAL + Snapshots | Full durability with fast recovery |
| WAL sync policy | Batched fsync (N ops or M ms) | Balance durability vs performance |
| Snapshot trigger | Hybrid (time OR size) | Regular checkpoints + bounded WAL |
| File structure | Per-shard files | Parallel I/O, independent recovery |
| Serialization | Binary (bincode) | Fast, compact |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        ShardedDb                            │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                   PersistenceManager                    ││
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ ││
│  │  │  WAL Writer │  │  Snapshot   │  │  Recovery       │ ││
│  │  │  (per-shard)│  │  Task       │  │  Loader         │ ││
│  │  └─────────────┘  └─────────────┘  └─────────────────┘ ││
│  └─────────────────────────────────────────────────────────┘│
│                                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
│  │ Shard 0 │  │ Shard 1 │  │ Shard 2 │  │ Shard N │       │
│  │ wal.bin │  │ wal.bin │  │ wal.bin │  │ wal.bin │       │
│  │ snap.bin│  │ snap.bin│  │ snap.bin│  │ snap.bin│       │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘       │
└─────────────────────────────────────────────────────────────┘

data/
├── shard_0/
│   ├── wal.bin           # Append-only write-ahead log
│   └── snapshot.bin      # Latest full state snapshot
├── shard_1/
│   ├── wal.bin
│   └── snapshot.bin
└── ...
```

## New Components

```
src/persistence/
├── mod.rs              # Module exports
├── config.rs           # PersistenceConfig
├── wal.rs              # WalWriter, WalReader, WalEntry
├── snapshot.rs         # SnapshotWriter, SnapshotReader
├── recovery.rs         # RecoveryLoader
└── manager.rs          # PersistenceManager (orchestrates all)
```

## Data Structures

### WAL Entry

```rust
#[derive(Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonic sequence number (matches replication log)
    pub seq: u64,
    /// Timestamp (unix millis) for debugging/TTL
    pub timestamp: u64,
    /// The operation that was applied
    pub op: WalOp,
}

#[derive(Serialize, Deserialize)]
pub enum WalOp {
    /// Increment counter: key, node_id, delta
    Incr { key: Key, node_id: u32, delta: u64 },
    /// Decrement counter: key, node_id, delta
    Decr { key: Key, node_id: u32, delta: u64 },
    /// Set counter to value (from snapshot apply)
    Set { key: Key, value: i64 },
    /// Delete key (TTL expiration)
    Delete { key: Key },
    /// Quota setup: key, limit, window
    QuotaSet { key: Key, limit: u64, window_secs: u64 },
    /// Quota token grant
    QuotaGrant { key: Key, node_id: u32, tokens: u64 },
}
```

### WAL File Format

```
┌──────────────────────────────────────────┐
│ Header (16 bytes)                        │
│  - magic: [u8; 4] = "QWAL"              │
│  - version: u32 = 1                      │
│  - shard_id: u16                         │
│  - reserved: [u8; 6]                     │
├──────────────────────────────────────────┤
│ Entry 1: [len: u32][checksum: u32][data] │
│ Entry 2: [len: u32][checksum: u32][data] │
│ ...                                      │
└──────────────────────────────────────────┘
```

### Snapshot

```rust
#[derive(Serialize, Deserialize)]
pub struct ShardSnapshot {
    /// Magic + version for compatibility
    pub magic: [u8; 4],  // "QSNP"
    pub version: u32,

    /// Shard identifier
    pub shard_id: u16,
    pub node_id: u32,

    /// Sequence number (WAL entries before this are included)
    pub seq: u64,

    /// Timestamp when snapshot was created
    pub timestamp: u64,

    /// Rolling hash digest (for verification)
    pub digest: u64,

    /// All counter entries
    pub counters: Vec<CounterSnapshot>,

    /// All quota entries
    pub quotas: Vec<QuotaSnapshot>,

    /// Allocator states (if this node owns the shard)
    pub allocators: Vec<AllocatorSnapshot>,
}

#[derive(Serialize, Deserialize)]
pub struct CounterSnapshot {
    pub key: Key,
    pub p_values: Vec<(u32, u64)>,  // (node_id, count)
    pub n_values: Vec<(u32, u64)>,  // (node_id, count)
    pub expires_at: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct QuotaSnapshot {
    pub key: Key,
    pub limit: u64,
    pub window_secs: u64,
    pub local_tokens: u64,
    pub used: u64,
    pub window_start: u64,
}
```

## Configuration

```rust
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Enable persistence (default: false for backwards compat)
    pub enabled: bool,

    /// Data directory (default: "./data")
    pub data_dir: PathBuf,

    /// WAL sync threshold: ops count (default: 1000)
    pub wal_sync_ops: usize,

    /// WAL sync threshold: interval (default: 100ms)
    pub wal_sync_interval: Duration,

    /// Snapshot interval (default: 5 min)
    pub snapshot_interval: Duration,

    /// Snapshot WAL size threshold (default: 64MB)
    pub snapshot_wal_threshold: u64,
}
```

## Write Path

```
Client Request (INCR key 1)
         │
         ▼
┌─────────────────────┐
│  1. Create WalEntry │
│     (seq, op, ts)   │
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  2. Append to WAL   │
│     buffer (async)  │
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  3. Apply to Shard  │
│     (in-memory)     │
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  4. Return response │
│     to client       │
└─────────────────────┘
         │
    (background)
         ▼
┌─────────────────────┐
│  5. Batched fsync   │
│     (N ops or M ms) │
└─────────────────────┘
```

## Recovery Process

```
Server Start
     │
     ▼
┌─────────────────────────────┐
│  1. Scan data/ directory    │
│     Find all shard folders  │
└─────────────────────────────┘
     │
     ▼ (parallel per shard)
┌─────────────────────────────┐
│  2. Load snapshot.bin       │
│     - Verify magic/version  │
│     - Verify digest         │
│     - Populate shard data   │
└─────────────────────────────┘
     │
     ▼
┌─────────────────────────────┐
│  3. Open wal.bin            │
│     - Seek to snapshot.seq  │
│     - Replay entries > seq  │
└─────────────────────────────┘
     │
     ▼
┌─────────────────────────────┐
│  4. Verify digest matches   │
│     rebuilt shard state     │
└─────────────────────────────┘
     │
     ▼
┌─────────────────────────────┐
│  5. Truncate WAL at last    │
│     valid entry (if corrupt)│
└─────────────────────────────┘
     │
     ▼
  Ready to serve
```

## Snapshot Creation

```
Trigger (time OR size)
         │
         ▼
┌─────────────────────────────┐
│  1. Acquire shard read lock │
│     (brief, for consistency)│
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  2. Clone shard state       │
│     - entries, allocators   │
│     - current seq & digest  │
└─────────────────────────────┘
         │
    (release lock)
         ▼
┌─────────────────────────────┐
│  3. Serialize to temp file  │
│     snapshot.bin.tmp        │
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  4. Fsync temp file         │
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  5. Atomic rename           │
│     .tmp → snapshot.bin     │
└─────────────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  6. Truncate WAL entries    │
│     before snapshot seq     │
└─────────────────────────────┘
```

## Error Handling

| Scenario | Action |
|----------|--------|
| WAL write fails | Return error to client, don't apply to memory |
| WAL corrupted on recovery | Truncate at last valid entry, log warning |
| Snapshot corrupted | Fall back to older snapshot or full WAL replay |
| Disk full | Reject writes, emit metric, continue serving reads |
| Checksum mismatch | Skip entry, log error, continue recovery |

## Testing Strategy

### Unit Tests

- `test_wal_entry_roundtrip` - serialize/deserialize
- `test_wal_checksum_validation` - detect corruption
- `test_snapshot_roundtrip` - full state save/load
- `test_recovery_empty_state` - fresh start
- `test_recovery_snapshot_only` - no WAL
- `test_recovery_wal_only` - no snapshot
- `test_recovery_snapshot_plus_wal` - normal case
- `test_wal_truncation_on_corrupt` - partial write recovery

### Integration Tests

- `test_persistence_survives_restart`
- `test_snapshot_triggered_by_time`
- `test_snapshot_triggered_by_size`
- `test_concurrent_writes_during_snapshot`

### Benchmark

Target: <5% overhead vs in-memory mode for typical workloads (batched fsync amortizes cost).

## Implementation Steps

| Step | Component | Description |
|------|-----------|-------------|
| 1 | `persistence/config.rs` | PersistenceConfig struct |
| 2 | `persistence/wal.rs` | WalEntry, WalOp, WalWriter, WalReader |
| 3 | `persistence/snapshot.rs` | ShardSnapshot, SnapshotWriter, SnapshotReader |
| 4 | `persistence/recovery.rs` | RecoveryLoader |
| 5 | `persistence/manager.rs` | PersistenceManager orchestration |
| 6 | `engine/shard.rs` | Integrate WAL writes on mutations |
| 7 | `main.rs` | Add persistence CLI flags, recovery on startup |
| 8 | Unit tests | Test each component |
| 9 | Integration tests | Multi-shard recovery scenarios |
| 10 | Benchmark | Verify <5% overhead |
