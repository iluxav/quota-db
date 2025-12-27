# Phase 5: Persistence Layer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add WAL + Snapshot persistence to QuotaDB for durability across restarts.

**Architecture:** Per-shard WAL files with batched fsync, periodic snapshots triggered by time OR WAL size. Binary serialization via bincode. Recovery loads snapshot then replays WAL.

**Tech Stack:** bincode (serialization), serde (derive), crc32fast (checksums), tokio::fs (async I/O)

---

## Task 1: Add Dependencies

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add serde, bincode, and crc32fast dependencies**

Add to `[dependencies]` section in Cargo.toml:

```toml
# Serialization
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"

# CRC32 checksums for WAL entries
crc32fast = "1.4"
```

**Step 2: Verify dependencies compile**

Run: `cargo check`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "feat(persistence): add serde, bincode, crc32fast dependencies"
```

---

## Task 2: Create PersistenceConfig

**Files:**
- Create: `src/persistence/mod.rs`
- Create: `src/persistence/config.rs`
- Modify: `src/lib.rs`

**Step 1: Create persistence module**

Create `src/persistence/mod.rs`:

```rust
mod config;

pub use config::PersistenceConfig;
```

**Step 2: Create PersistenceConfig struct**

Create `src/persistence/config.rs`:

```rust
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the persistence layer.
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

    /// Snapshot WAL size threshold in bytes (default: 64MB)
    pub snapshot_wal_threshold: u64,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            data_dir: PathBuf::from("./data"),
            wal_sync_ops: 1000,
            wal_sync_interval: Duration::from_millis(100),
            snapshot_interval: Duration::from_secs(300),
            snapshot_wal_threshold: 64 * 1024 * 1024,
        }
    }
}

impl PersistenceConfig {
    /// Create a new config with persistence enabled.
    pub fn enabled(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            enabled: true,
            data_dir: data_dir.into(),
            ..Default::default()
        }
    }

    /// Get the path for a shard's data directory.
    pub fn shard_dir(&self, shard_id: u16) -> PathBuf {
        self.data_dir.join(format!("shard_{}", shard_id))
    }

    /// Get the path for a shard's WAL file.
    pub fn wal_path(&self, shard_id: u16) -> PathBuf {
        self.shard_dir(shard_id).join("wal.bin")
    }

    /// Get the path for a shard's snapshot file.
    pub fn snapshot_path(&self, shard_id: u16) -> PathBuf {
        self.shard_dir(shard_id).join("snapshot.bin")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PersistenceConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.wal_sync_ops, 1000);
    }

    #[test]
    fn test_enabled_config() {
        let config = PersistenceConfig::enabled("/tmp/quota-db");
        assert!(config.enabled);
        assert_eq!(config.data_dir, PathBuf::from("/tmp/quota-db"));
    }

    #[test]
    fn test_shard_paths() {
        let config = PersistenceConfig::enabled("/tmp/db");
        assert_eq!(config.shard_dir(0), PathBuf::from("/tmp/db/shard_0"));
        assert_eq!(config.wal_path(0), PathBuf::from("/tmp/db/shard_0/wal.bin"));
        assert_eq!(config.snapshot_path(0), PathBuf::from("/tmp/db/shard_0/snapshot.bin"));
    }
}
```

**Step 3: Add persistence module to lib.rs**

Add to `src/lib.rs`:

```rust
pub mod persistence;
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Run tests**

Run: `cargo test persistence`
Expected: All tests pass

**Step 6: Commit**

```bash
git add src/persistence/ src/lib.rs
git commit -m "feat(persistence): add PersistenceConfig"
```

---

## Task 3: Create WAL Entry Types

**Files:**
- Create: `src/persistence/wal.rs`
- Modify: `src/persistence/mod.rs`
- Modify: `src/types/key.rs`

**Step 1: Add Serialize/Deserialize to Key type**

Modify `src/types/key.rs` to add serde derives. The Key wraps Bytes, so we need custom serialization:

```rust
use serde::{Deserialize, Serialize, Deserializer, Serializer};

// Add after the Key struct definition:

impl Serialize for Key {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.0.as_ref())
    }
}

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(Key::new(bytes::Bytes::from(bytes)))
    }
}
```

**Step 2: Create WalEntry and WalOp types**

Create `src/persistence/wal.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::types::Key;

/// WAL file magic bytes
pub const WAL_MAGIC: [u8; 4] = *b"QWAL";

/// WAL format version
pub const WAL_VERSION: u32 = 1;

/// WAL file header (16 bytes)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalHeader {
    pub magic: [u8; 4],
    pub version: u32,
    pub shard_id: u16,
    pub reserved: [u8; 6],
}

impl WalHeader {
    pub fn new(shard_id: u16) -> Self {
        Self {
            magic: WAL_MAGIC,
            version: WAL_VERSION,
            shard_id,
            reserved: [0; 6],
        }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == WAL_MAGIC && self.version == WAL_VERSION
    }
}

/// Single WAL entry representing one mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonic sequence number (matches replication log)
    pub seq: u64,
    /// Timestamp (unix millis) for debugging
    pub timestamp: u64,
    /// The operation that was applied
    pub op: WalOp,
}

/// Operations that can be logged to the WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOp {
    /// Increment counter: key, node_id, delta
    Incr {
        key: Key,
        node_id: u32,
        delta: u64,
    },
    /// Decrement counter: key, node_id, delta
    Decr {
        key: Key,
        node_id: u32,
        delta: u64,
    },
    /// Set counter to value
    Set {
        key: Key,
        value: i64,
    },
    /// Delete key (TTL expiration)
    Delete {
        key: Key,
    },
    /// Quota setup: key, limit, window
    QuotaSet {
        key: Key,
        limit: u64,
        window_secs: u64,
    },
    /// Quota token grant
    QuotaGrant {
        key: Key,
        node_id: u32,
        tokens: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_header() {
        let header = WalHeader::new(42);
        assert!(header.is_valid());
        assert_eq!(header.shard_id, 42);
    }

    #[test]
    fn test_wal_entry_serialize_roundtrip() {
        let entry = WalEntry {
            seq: 100,
            timestamp: 1234567890,
            op: WalOp::Incr {
                key: Key::from("test"),
                node_id: 1,
                delta: 42,
            },
        };

        let encoded = bincode::serialize(&entry).unwrap();
        let decoded: WalEntry = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded.seq, 100);
        assert_eq!(decoded.timestamp, 1234567890);
        match decoded.op {
            WalOp::Incr { key, node_id, delta } => {
                assert_eq!(key.as_bytes(), b"test");
                assert_eq!(node_id, 1);
                assert_eq!(delta, 42);
            }
            _ => panic!("wrong op type"),
        }
    }

    #[test]
    fn test_all_wal_ops_serialize() {
        let ops = vec![
            WalOp::Incr { key: Key::from("k"), node_id: 1, delta: 10 },
            WalOp::Decr { key: Key::from("k"), node_id: 1, delta: 5 },
            WalOp::Set { key: Key::from("k"), value: -100 },
            WalOp::Delete { key: Key::from("k") },
            WalOp::QuotaSet { key: Key::from("q"), limit: 1000, window_secs: 60 },
            WalOp::QuotaGrant { key: Key::from("q"), node_id: 2, tokens: 100 },
        ];

        for op in ops {
            let entry = WalEntry { seq: 1, timestamp: 0, op };
            let encoded = bincode::serialize(&entry).unwrap();
            let decoded: WalEntry = bincode::deserialize(&encoded).unwrap();
            assert_eq!(decoded.seq, 1);
        }
    }
}
```

**Step 3: Export wal module**

Update `src/persistence/mod.rs`:

```rust
mod config;
mod wal;

pub use config::PersistenceConfig;
pub use wal::{WalEntry, WalHeader, WalOp, WAL_MAGIC, WAL_VERSION};
```

**Step 4: Verify it compiles and tests pass**

Run: `cargo test persistence`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/persistence/ src/types/key.rs
git commit -m "feat(persistence): add WAL entry types with serde"
```

---

## Task 4: Create WalWriter

**Files:**
- Modify: `src/persistence/wal.rs`

**Step 1: Add WalWriter struct**

Add to `src/persistence/wal.rs`:

```rust
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write, Seek, SeekFrom};
use std::path::Path;
use std::time::Instant;

use crc32fast::Hasher;

/// Writer for append-only WAL file.
pub struct WalWriter {
    writer: BufWriter<File>,
    shard_id: u16,
    pending_count: usize,
    pending_bytes: usize,
    last_sync: Instant,
    sync_ops_threshold: usize,
}

impl WalWriter {
    /// Create or open a WAL file for writing.
    pub fn open(path: &Path, shard_id: u16, sync_ops_threshold: usize) -> io::Result<Self> {
        let exists = path.exists();

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        let mut writer = BufWriter::new(file);

        // Write header if new file
        if !exists {
            let header = WalHeader::new(shard_id);
            let header_bytes = bincode::serialize(&header)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            writer.write_all(&header_bytes)?;
            writer.flush()?;
        }

        Ok(Self {
            writer,
            shard_id,
            pending_count: 0,
            pending_bytes: 0,
            last_sync: Instant::now(),
            sync_ops_threshold,
        })
    }

    /// Append an entry to the WAL.
    /// Returns the number of bytes written.
    pub fn append(&mut self, entry: &WalEntry) -> io::Result<usize> {
        // Serialize entry
        let data = bincode::serialize(entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Calculate checksum
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let checksum = hasher.finalize();

        // Write: [len: u32][checksum: u32][data]
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(&data)?;

        let total_bytes = 4 + 4 + data.len();
        self.pending_count += 1;
        self.pending_bytes += total_bytes;

        Ok(total_bytes)
    }

    /// Check if we should sync based on thresholds.
    pub fn should_sync(&self, interval: std::time::Duration) -> bool {
        self.pending_count >= self.sync_ops_threshold
            || self.last_sync.elapsed() >= interval
    }

    /// Flush buffer and sync to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.pending_count = 0;
        self.pending_bytes = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    /// Get pending byte count (for snapshot trigger).
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Get the current file size.
    pub fn file_size(&self) -> io::Result<u64> {
        self.writer.get_ref().metadata().map(|m| m.len())
    }
}
```

**Step 2: Add WalWriter tests**

Add to the tests module in `src/persistence/wal.rs`:

```rust
    #[test]
    fn test_wal_writer_create() {
        let dir = std::env::temp_dir().join("quota-db-test-wal-writer");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("wal.bin");
        let writer = WalWriter::open(&path, 0, 1000).unwrap();

        assert!(path.exists());
        drop(writer);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_wal_writer_append() {
        let dir = std::env::temp_dir().join("quota-db-test-wal-append");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("wal.bin");
        let mut writer = WalWriter::open(&path, 0, 1000).unwrap();

        let entry = WalEntry {
            seq: 1,
            timestamp: 12345,
            op: WalOp::Incr {
                key: Key::from("test"),
                node_id: 1,
                delta: 100,
            },
        };

        let bytes = writer.append(&entry).unwrap();
        assert!(bytes > 0);
        writer.sync().unwrap();

        std::fs::remove_dir_all(&dir).unwrap();
    }
```

**Step 3: Verify tests pass**

Run: `cargo test persistence::wal`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/persistence/wal.rs
git commit -m "feat(persistence): add WalWriter with checksum"
```

---

## Task 5: Create WalReader

**Files:**
- Modify: `src/persistence/wal.rs`
- Modify: `src/persistence/mod.rs`

**Step 1: Add WalReader struct**

Add to `src/persistence/wal.rs`:

```rust
use std::io::{BufReader, Read};

/// Reader for WAL file with checksum validation.
pub struct WalReader {
    reader: BufReader<File>,
    shard_id: u16,
}

impl WalReader {
    /// Open a WAL file for reading.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and validate header
        let header: WalHeader = Self::read_header(&mut reader)?;
        if !header.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid WAL header",
            ));
        }

        Ok(Self {
            reader,
            shard_id: header.shard_id,
        })
    }

    fn read_header(reader: &mut BufReader<File>) -> io::Result<WalHeader> {
        // WalHeader is fixed size when serialized with bincode
        let mut buf = vec![0u8; 16]; // header size
        reader.read_exact(&mut buf)?;
        bincode::deserialize(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Get the shard ID from the header.
    pub fn shard_id(&self) -> u16 {
        self.shard_id
    }

    /// Read the next entry, returns None at EOF.
    /// Validates checksum and returns error on corruption.
    pub fn read_entry(&mut self) -> io::Result<Option<WalEntry>> {
        // Read length
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        // Read checksum
        let mut checksum_buf = [0u8; 4];
        self.reader.read_exact(&mut checksum_buf)?;
        let expected_checksum = u32::from_le_bytes(checksum_buf);

        // Read data
        let mut data = vec![0u8; len];
        self.reader.read_exact(&mut data)?;

        // Validate checksum
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let actual_checksum = hasher.finalize();

        if actual_checksum != expected_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL checksum mismatch: expected {}, got {}",
                    expected_checksum, actual_checksum
                ),
            ));
        }

        // Deserialize entry
        let entry: WalEntry = bincode::deserialize(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(entry))
    }

    /// Read all entries from the WAL.
    pub fn read_all(&mut self) -> io::Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        while let Some(entry) = self.read_entry()? {
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Read entries with sequence > after_seq.
    pub fn read_after(&mut self, after_seq: u64) -> io::Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        while let Some(entry) = self.read_entry()? {
            if entry.seq > after_seq {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}
```

**Step 2: Update exports**

Update `src/persistence/mod.rs`:

```rust
mod config;
mod wal;

pub use config::PersistenceConfig;
pub use wal::{WalEntry, WalHeader, WalOp, WalReader, WalWriter, WAL_MAGIC, WAL_VERSION};
```

**Step 3: Add WalReader tests**

Add to tests in `src/persistence/wal.rs`:

```rust
    #[test]
    fn test_wal_roundtrip() {
        let dir = std::env::temp_dir().join("quota-db-test-wal-roundtrip");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("wal.bin");

        // Write entries
        {
            let mut writer = WalWriter::open(&path, 5, 1000).unwrap();
            for i in 1..=10 {
                let entry = WalEntry {
                    seq: i,
                    timestamp: i * 1000,
                    op: WalOp::Incr {
                        key: Key::from(format!("key{}", i)),
                        node_id: 1,
                        delta: i,
                    },
                };
                writer.append(&entry).unwrap();
            }
            writer.sync().unwrap();
        }

        // Read entries
        {
            let mut reader = WalReader::open(&path).unwrap();
            assert_eq!(reader.shard_id(), 5);

            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 10);

            for (i, entry) in entries.iter().enumerate() {
                assert_eq!(entry.seq, (i + 1) as u64);
            }
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_wal_read_after() {
        let dir = std::env::temp_dir().join("quota-db-test-wal-read-after");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("wal.bin");

        // Write entries
        {
            let mut writer = WalWriter::open(&path, 0, 1000).unwrap();
            for i in 1..=10 {
                let entry = WalEntry {
                    seq: i,
                    timestamp: 0,
                    op: WalOp::Set { key: Key::from("k"), value: i as i64 },
                };
                writer.append(&entry).unwrap();
            }
            writer.sync().unwrap();
        }

        // Read entries after seq 5
        {
            let mut reader = WalReader::open(&path).unwrap();
            let entries = reader.read_after(5).unwrap();
            assert_eq!(entries.len(), 5);
            assert_eq!(entries[0].seq, 6);
            assert_eq!(entries[4].seq, 10);
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }
```

**Step 4: Verify tests pass**

Run: `cargo test persistence::wal`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/persistence/
git commit -m "feat(persistence): add WalReader with checksum validation"
```

---

## Task 6: Create Snapshot Types

**Files:**
- Create: `src/persistence/snapshot.rs`
- Modify: `src/persistence/mod.rs`

**Step 1: Create snapshot types**

Create `src/persistence/snapshot.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::types::Key;

/// Snapshot file magic bytes
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"QSNP";

/// Snapshot format version
pub const SNAPSHOT_VERSION: u32 = 1;

/// Complete shard state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSnapshot {
    /// Magic bytes for file identification
    pub magic: [u8; 4],
    /// Format version
    pub version: u32,
    /// Shard identifier
    pub shard_id: u16,
    /// Node ID that created this snapshot
    pub node_id: u32,
    /// Sequence number (WAL entries <= this are included)
    pub seq: u64,
    /// Timestamp when snapshot was created (unix millis)
    pub timestamp: u64,
    /// Rolling hash digest for verification
    pub digest: u64,
    /// All counter entries
    pub counters: Vec<CounterSnapshot>,
    /// All quota entries
    pub quotas: Vec<QuotaSnapshot>,
    /// Allocator states
    pub allocators: Vec<AllocatorSnapshot>,
}

impl ShardSnapshot {
    /// Create a new empty snapshot.
    pub fn new(shard_id: u16, node_id: u32, seq: u64, digest: u64) -> Self {
        Self {
            magic: SNAPSHOT_MAGIC,
            version: SNAPSHOT_VERSION,
            shard_id,
            node_id,
            seq,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            digest,
            counters: Vec::new(),
            quotas: Vec::new(),
            allocators: Vec::new(),
        }
    }

    /// Validate the snapshot header.
    pub fn is_valid(&self) -> bool {
        self.magic == SNAPSHOT_MAGIC && self.version == SNAPSHOT_VERSION
    }
}

/// Snapshot of a PN-Counter entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterSnapshot {
    pub key: Key,
    /// P values: (node_id, count)
    pub p_values: Vec<(u32, u64)>,
    /// N values: (node_id, count)
    pub n_values: Vec<(u32, u64)>,
    /// Optional TTL timestamp
    pub expires_at: Option<u64>,
}

/// Snapshot of a Quota entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaSnapshot {
    pub key: Key,
    pub limit: u64,
    pub window_secs: u64,
    pub local_tokens: i64,
    pub window_start: u64,
}

/// Snapshot of allocator state for a quota key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocatorSnapshot {
    pub key: Key,
    /// Grants: (node_id, tokens)
    pub grants: Vec<(u32, u64)>,
    pub total_granted: u64,
    pub window_start: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_new() {
        let snap = ShardSnapshot::new(0, 1, 100, 12345);
        assert!(snap.is_valid());
        assert_eq!(snap.shard_id, 0);
        assert_eq!(snap.node_id, 1);
        assert_eq!(snap.seq, 100);
        assert_eq!(snap.digest, 12345);
    }

    #[test]
    fn test_snapshot_serialize_roundtrip() {
        let mut snap = ShardSnapshot::new(5, 2, 500, 99999);
        snap.counters.push(CounterSnapshot {
            key: Key::from("counter1"),
            p_values: vec![(1, 100), (2, 200)],
            n_values: vec![(1, 10)],
            expires_at: Some(1234567890),
        });
        snap.quotas.push(QuotaSnapshot {
            key: Key::from("quota1"),
            limit: 10000,
            window_secs: 60,
            local_tokens: 500,
            window_start: 1000,
        });
        snap.allocators.push(AllocatorSnapshot {
            key: Key::from("quota1"),
            grants: vec![(1, 300), (2, 200)],
            total_granted: 500,
            window_start: 1000,
        });

        let encoded = bincode::serialize(&snap).unwrap();
        let decoded: ShardSnapshot = bincode::deserialize(&encoded).unwrap();

        assert!(decoded.is_valid());
        assert_eq!(decoded.shard_id, 5);
        assert_eq!(decoded.counters.len(), 1);
        assert_eq!(decoded.quotas.len(), 1);
        assert_eq!(decoded.allocators.len(), 1);
        assert_eq!(decoded.counters[0].key.as_bytes(), b"counter1");
    }
}
```

**Step 2: Update exports**

Update `src/persistence/mod.rs`:

```rust
mod config;
mod snapshot;
mod wal;

pub use config::PersistenceConfig;
pub use snapshot::{
    AllocatorSnapshot, CounterSnapshot, QuotaSnapshot, ShardSnapshot,
    SNAPSHOT_MAGIC, SNAPSHOT_VERSION,
};
pub use wal::{WalEntry, WalHeader, WalOp, WalReader, WalWriter, WAL_MAGIC, WAL_VERSION};
```

**Step 3: Verify tests pass**

Run: `cargo test persistence`
Expected: All tests pass

**Step 4: Commit**

```bash
git add src/persistence/
git commit -m "feat(persistence): add snapshot types"
```

---

## Task 7: Create Snapshot Writer/Reader

**Files:**
- Modify: `src/persistence/snapshot.rs`

**Step 1: Add SnapshotWriter and SnapshotReader**

Add to `src/persistence/snapshot.rs`:

```rust
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Write a snapshot atomically (write to .tmp, then rename).
pub fn write_snapshot(path: &Path, snapshot: &ShardSnapshot) -> io::Result<()> {
    // Create parent directory if needed
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("bin.tmp");

    // Write to temp file
    {
        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(file);

        let data = bincode::serialize(snapshot)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        writer.write_all(&data)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
    }

    // Atomic rename
    fs::rename(&tmp_path, path)?;

    Ok(())
}

/// Read a snapshot from file.
pub fn read_snapshot(path: &Path) -> io::Result<ShardSnapshot> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;

    let snapshot: ShardSnapshot = bincode::deserialize(&data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    if !snapshot.is_valid() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid snapshot header",
        ));
    }

    Ok(snapshot)
}
```

**Step 2: Add tests**

Add to tests in `src/persistence/snapshot.rs`:

```rust
    #[test]
    fn test_snapshot_write_read() {
        let dir = std::env::temp_dir().join("quota-db-test-snapshot-rw");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("snapshot.bin");

        let mut snap = ShardSnapshot::new(3, 1, 1000, 555);
        snap.counters.push(CounterSnapshot {
            key: Key::from("test"),
            p_values: vec![(1, 50)],
            n_values: vec![],
            expires_at: None,
        });

        write_snapshot(&path, &snap).unwrap();
        assert!(path.exists());

        let loaded = read_snapshot(&path).unwrap();
        assert_eq!(loaded.shard_id, 3);
        assert_eq!(loaded.seq, 1000);
        assert_eq!(loaded.counters.len(), 1);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_snapshot_atomic_write() {
        let dir = std::env::temp_dir().join("quota-db-test-snapshot-atomic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("snapshot.bin");
        let tmp_path = path.with_extension("bin.tmp");

        let snap = ShardSnapshot::new(0, 1, 100, 0);
        write_snapshot(&path, &snap).unwrap();

        // Temp file should not exist after successful write
        assert!(!tmp_path.exists());
        assert!(path.exists());

        std::fs::remove_dir_all(&dir).unwrap();
    }
```

**Step 3: Update exports**

Update `src/persistence/mod.rs`:

```rust
mod config;
mod snapshot;
mod wal;

pub use config::PersistenceConfig;
pub use snapshot::{
    read_snapshot, write_snapshot,
    AllocatorSnapshot, CounterSnapshot, QuotaSnapshot, ShardSnapshot,
    SNAPSHOT_MAGIC, SNAPSHOT_VERSION,
};
pub use wal::{WalEntry, WalHeader, WalOp, WalReader, WalWriter, WAL_MAGIC, WAL_VERSION};
```

**Step 4: Verify tests pass**

Run: `cargo test persistence`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/persistence/
git commit -m "feat(persistence): add snapshot write/read functions"
```

---

## Task 8: Add Shard Snapshot Creation

**Files:**
- Modify: `src/engine/shard.rs`
- Modify: `src/engine/entry.rs`

**Step 1: Add snapshot creation to PnCounterEntry**

Add to `src/engine/entry.rs`:

```rust
use crate::persistence::CounterSnapshot;

impl PnCounterEntry {
    /// Create a snapshot of this counter.
    pub fn to_snapshot(&self, key: Key) -> CounterSnapshot {
        CounterSnapshot {
            key,
            p_values: self.p.iter().map(|(n, v)| (n.as_u32(), *v)).collect(),
            n_values: self.n.iter().map(|(n, v)| (n.as_u32(), *v)).collect(),
            expires_at: self.expires_at,
        }
    }

    /// Restore from a snapshot.
    pub fn from_snapshot(snap: &CounterSnapshot) -> Self {
        let mut entry = Self::new();
        for &(node_id, count) in &snap.p_values {
            entry.p.insert(NodeId::new(node_id), count);
        }
        for &(node_id, count) in &snap.n_values {
            entry.n.insert(NodeId::new(node_id), count);
        }
        entry.expires_at = snap.expires_at;
        entry.recalculate_value();
        entry
    }
}
```

**Step 2: Add snapshot creation to QuotaEntry and AllocatorState**

Add to `src/engine/quota.rs`:

```rust
use crate::persistence::{AllocatorSnapshot, QuotaSnapshot};
use crate::types::Key;

impl QuotaEntry {
    /// Create a snapshot of this quota entry.
    pub fn to_snapshot(&self, key: Key) -> QuotaSnapshot {
        QuotaSnapshot {
            key,
            limit: self.limit,
            window_secs: self.window_secs,
            local_tokens: self.local_tokens,
            window_start: self.window_start,
        }
    }

    /// Restore from a snapshot.
    pub fn from_snapshot(snap: &QuotaSnapshot) -> Self {
        Self {
            limit: snap.limit,
            window_secs: snap.window_secs,
            local_tokens: snap.local_tokens,
            window_start: snap.window_start,
        }
    }
}

impl AllocatorState {
    /// Create a snapshot of this allocator state.
    pub fn to_snapshot(&self, key: Key) -> AllocatorSnapshot {
        AllocatorSnapshot {
            key,
            grants: self.grants.iter().map(|(n, v)| (n.as_u32(), *v)).collect(),
            total_granted: self.total_granted,
            window_start: self.window_start,
        }
    }

    /// Restore from a snapshot.
    pub fn from_snapshot(snap: &AllocatorSnapshot) -> Self {
        let mut state = Self::with_window_start(snap.window_start);
        for &(node_id, count) in &snap.grants {
            state.grants.insert(NodeId::new(node_id), count);
        }
        state.total_granted = snap.total_granted;
        state
    }
}
```

**Step 3: Add snapshot creation to Shard**

Add to `src/engine/shard.rs`:

```rust
use crate::persistence::ShardSnapshot;

impl Shard {
    /// Create a full snapshot of this shard.
    pub fn create_persistence_snapshot(&self) -> ShardSnapshot {
        let mut snapshot = ShardSnapshot::new(
            self.id as u16,
            self.node_id.as_u32(),
            self.rep_log.head_seq(),
            self.digest.value(),
        );

        // Snapshot all entries
        for (key, entry) in &self.data {
            match entry {
                Entry::Counter(counter) => {
                    snapshot.counters.push(counter.to_snapshot(key.clone()));
                }
                Entry::Quota(quota) => {
                    snapshot.quotas.push(quota.to_snapshot(key.clone()));
                }
            }
        }

        // Snapshot allocator states
        for (key, alloc) in &self.allocators {
            snapshot.allocators.push(alloc.to_snapshot(key.clone()));
        }

        snapshot
    }

    /// Restore shard state from a snapshot.
    pub fn restore_from_snapshot(&mut self, snapshot: ShardSnapshot) {
        use crate::engine::quota::AllocatorState;

        // Clear current state
        self.data.clear();
        self.allocators.clear();
        self.digest = ShardDigest::new();

        // Restore counters
        for counter_snap in snapshot.counters {
            let entry = PnCounterEntry::from_snapshot(&counter_snap);
            let value = entry.value();
            self.data.insert(counter_snap.key.clone(), Entry::Counter(entry));
            self.digest.insert(&counter_snap.key, value);
        }

        // Restore quotas
        for quota_snap in snapshot.quotas {
            let entry = QuotaEntry::from_snapshot(&quota_snap);
            let value = entry.local_tokens();
            self.data.insert(quota_snap.key.clone(), Entry::Quota(entry));
            self.digest.insert(&quota_snap.key, value);
        }

        // Restore allocators
        for alloc_snap in snapshot.allocators {
            let state = AllocatorState::from_snapshot(&alloc_snap);
            self.allocators.insert(alloc_snap.key, state);
        }

        // Reset replication log to snapshot sequence
        self.rep_log.reset_to(snapshot.seq);
    }
}
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Add tests**

Add to tests in `src/engine/shard.rs`:

```rust
    #[test]
    fn test_shard_persistence_snapshot() {
        let node = NodeId::new(1);
        let mut shard = Shard::new(0, node);

        // Add some data
        shard.increment(Key::from("counter1"), node, 100);
        shard.increment(Key::from("counter2"), node, 200);
        shard.set_quota(Key::from("quota1"), 1000, 60);

        // Create snapshot
        let snapshot = shard.create_persistence_snapshot();
        assert_eq!(snapshot.counters.len(), 2);
        assert_eq!(snapshot.quotas.len(), 1);

        // Create new shard and restore
        let mut shard2 = Shard::new(0, node);
        shard2.restore_from_snapshot(snapshot);

        assert_eq!(shard2.get(&Key::from("counter1")), Some(100));
        assert_eq!(shard2.get(&Key::from("counter2")), Some(200));
        assert!(shard2.is_quota(&Key::from("quota1")));
    }
```

**Step 6: Run tests**

Run: `cargo test shard_persistence`
Expected: All tests pass

**Step 7: Commit**

```bash
git add src/engine/ src/persistence/
git commit -m "feat(persistence): add shard snapshot creation and restoration"
```

---

## Task 9: Add CLI Flags for Persistence

**Files:**
- Modify: `src/config.rs`

**Step 1: Add persistence CLI arguments**

Add to `src/config.rs` Config struct:

```rust
    // === Persistence settings ===

    /// Enable persistence (WAL + snapshots)
    #[arg(long, default_value = "false")]
    pub persistence: bool,

    /// Data directory for persistence files
    #[arg(long, default_value = "./data")]
    pub data_dir: String,

    /// WAL sync interval in milliseconds
    #[arg(long, default_value = "100")]
    pub wal_sync_ms: u64,

    /// WAL sync ops threshold
    #[arg(long, default_value = "1000")]
    pub wal_sync_ops: usize,

    /// Snapshot interval in seconds
    #[arg(long, default_value = "300")]
    pub snapshot_interval_secs: u64,

    /// Snapshot WAL size threshold in MB
    #[arg(long, default_value = "64")]
    pub snapshot_wal_mb: u64,
```

**Step 2: Add method to get PersistenceConfig**

Add to `src/config.rs` impl Config:

```rust
    /// Get persistence configuration.
    pub fn persistence_config(&self) -> crate::persistence::PersistenceConfig {
        use std::path::PathBuf;
        use std::time::Duration;

        crate::persistence::PersistenceConfig {
            enabled: self.persistence,
            data_dir: PathBuf::from(&self.data_dir),
            wal_sync_interval: Duration::from_millis(self.wal_sync_ms),
            wal_sync_ops: self.wal_sync_ops,
            snapshot_interval: Duration::from_secs(self.snapshot_interval_secs),
            snapshot_wal_threshold: self.snapshot_wal_mb * 1024 * 1024,
        }
    }
```

**Step 3: Update Default impl**

Add to Default impl:

```rust
            persistence: false,
            data_dir: "./data".to_string(),
            wal_sync_ms: 100,
            wal_sync_ops: 1000,
            snapshot_interval_secs: 300,
            snapshot_wal_mb: 64,
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/config.rs
git commit -m "feat(persistence): add CLI flags for persistence config"
```

---

## Task 10: Create Recovery Module

**Files:**
- Create: `src/persistence/recovery.rs`
- Modify: `src/persistence/mod.rs`

**Step 1: Create recovery module**

Create `src/persistence/recovery.rs`:

```rust
use std::io;
use std::path::Path;

use tracing::{info, warn};

use crate::engine::Shard;
use crate::persistence::{
    read_snapshot, PersistenceConfig, ShardSnapshot, WalEntry, WalOp, WalReader,
};
use crate::types::{Key, NodeId};

/// Recover a shard from persistence files.
pub fn recover_shard(
    config: &PersistenceConfig,
    shard_id: u16,
    node_id: NodeId,
) -> io::Result<(Shard, u64)> {
    let shard_dir = config.shard_dir(shard_id);

    if !shard_dir.exists() {
        info!("Shard {} has no persistence data, starting fresh", shard_id);
        return Ok((Shard::new(shard_id as usize, node_id), 0));
    }

    let snapshot_path = config.snapshot_path(shard_id);
    let wal_path = config.wal_path(shard_id);

    let mut shard = Shard::new(shard_id as usize, node_id);
    let mut last_seq = 0u64;

    // Load snapshot if exists
    if snapshot_path.exists() {
        match read_snapshot(&snapshot_path) {
            Ok(snapshot) => {
                info!(
                    "Shard {} loading snapshot at seq {}",
                    shard_id, snapshot.seq
                );
                last_seq = snapshot.seq;
                shard.restore_from_snapshot(snapshot);
            }
            Err(e) => {
                warn!("Shard {} snapshot corrupted: {}, will replay full WAL", shard_id, e);
            }
        }
    }

    // Replay WAL entries after snapshot
    if wal_path.exists() {
        match replay_wal(&wal_path, &mut shard, last_seq) {
            Ok(replayed) => {
                if replayed > 0 {
                    info!("Shard {} replayed {} WAL entries", shard_id, replayed);
                }
                last_seq += replayed as u64;
            }
            Err(e) => {
                warn!("Shard {} WAL replay error: {}", shard_id, e);
                // Continue with what we have
            }
        }
    }

    Ok((shard, last_seq))
}

/// Replay WAL entries to a shard.
fn replay_wal(path: &Path, shard: &mut Shard, after_seq: u64) -> io::Result<usize> {
    let mut reader = WalReader::open(path)?;
    let entries = reader.read_after(after_seq)?;
    let count = entries.len();

    for entry in entries {
        apply_wal_entry(shard, &entry);
    }

    Ok(count)
}

/// Apply a single WAL entry to a shard.
fn apply_wal_entry(shard: &mut Shard, entry: &WalEntry) {
    match &entry.op {
        WalOp::Incr { key, node_id, delta } => {
            shard.increment(key.clone(), NodeId::new(*node_id), *delta);
        }
        WalOp::Decr { key, node_id, delta } => {
            shard.decrement(key.clone(), NodeId::new(*node_id), *delta);
        }
        WalOp::Set { key, value } => {
            shard.set(key.clone(), *value);
        }
        WalOp::Delete { key } => {
            shard.delete(key);
        }
        WalOp::QuotaSet { key, limit, window_secs } => {
            shard.set_quota(key.clone(), *limit, *window_secs);
        }
        WalOp::QuotaGrant { key, node_id: _, tokens } => {
            // Add tokens to quota
            if let Some(entry) = shard.get_quota_mut(key) {
                entry.add_tokens(*tokens);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::WalWriter;

    #[test]
    fn test_recover_empty_shard() {
        let dir = std::env::temp_dir().join("quota-db-test-recover-empty");
        let _ = std::fs::remove_dir_all(&dir);

        let config = PersistenceConfig::enabled(&dir);
        let (shard, seq) = recover_shard(&config, 0, NodeId::new(1)).unwrap();

        assert_eq!(seq, 0);
        assert_eq!(shard.get(&Key::from("test")), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recover_from_wal() {
        let dir = std::env::temp_dir().join("quota-db-test-recover-wal");
        let _ = std::fs::remove_dir_all(&dir);

        let config = PersistenceConfig::enabled(&dir);
        let wal_path = config.wal_path(0);

        // Write some WAL entries
        {
            let mut writer = WalWriter::open(&wal_path, 0, 1000).unwrap();
            writer.append(&WalEntry {
                seq: 1,
                timestamp: 0,
                op: WalOp::Incr { key: Key::from("counter"), node_id: 1, delta: 100 },
            }).unwrap();
            writer.append(&WalEntry {
                seq: 2,
                timestamp: 0,
                op: WalOp::Incr { key: Key::from("counter"), node_id: 1, delta: 50 },
            }).unwrap();
            writer.sync().unwrap();
        }

        // Recover
        let (shard, _) = recover_shard(&config, 0, NodeId::new(1)).unwrap();
        assert_eq!(shard.get(&Key::from("counter")), Some(150));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
```

**Step 2: Update exports**

Update `src/persistence/mod.rs`:

```rust
mod config;
mod recovery;
mod snapshot;
mod wal;

pub use config::PersistenceConfig;
pub use recovery::recover_shard;
pub use snapshot::{
    read_snapshot, write_snapshot,
    AllocatorSnapshot, CounterSnapshot, QuotaSnapshot, ShardSnapshot,
    SNAPSHOT_MAGIC, SNAPSHOT_VERSION,
};
pub use wal::{WalEntry, WalHeader, WalOp, WalReader, WalWriter, WAL_MAGIC, WAL_VERSION};
```

**Step 3: Add delete method to Shard if not exists**

Add to `src/engine/shard.rs` if needed:

```rust
    /// Delete a key from the shard.
    pub fn delete(&mut self, key: &Key) {
        if let Some(entry) = self.data.remove(key) {
            let value = match &entry {
                Entry::Counter(c) => c.value(),
                Entry::Quota(q) => q.local_tokens(),
            };
            self.digest.remove(key, value);
        }
    }

    /// Get mutable reference to quota entry.
    pub fn get_quota_mut(&mut self, key: &Key) -> Option<&mut QuotaEntry> {
        self.data.get_mut(key).and_then(|e| e.as_quota_mut())
    }
```

**Step 4: Verify tests pass**

Run: `cargo test persistence`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/persistence/ src/engine/
git commit -m "feat(persistence): add recovery module"
```

---

## Task 11: Integrate Persistence into Main

**Files:**
- Modify: `src/main.rs`
- Modify: `src/engine/db.rs`

**Step 1: Add recovery on startup to ShardedDb**

Add to `src/engine/db.rs`:

```rust
use crate::persistence::{recover_shard, PersistenceConfig};

impl ShardedDb {
    /// Create a new ShardedDb with optional recovery from persistence.
    pub fn with_persistence(config: &Config, persistence: &PersistenceConfig) -> Self {
        let node_id = NodeId::new(config.node_id);
        let num_shards = config.shards;

        let shards: Vec<_> = if persistence.enabled {
            (0..num_shards)
                .map(|i| {
                    match recover_shard(persistence, i as u16, node_id) {
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
        }
    }
}
```

**Step 2: Update main.rs to use persistence**

Modify `src/main.rs` to add recovery:

```rust
    // Get persistence config
    let persistence_config = config.persistence_config();

    // Create the sharded database (with recovery if enabled)
    let db = if persistence_config.enabled {
        info!("Persistence enabled, data dir: {:?}", persistence_config.data_dir);
        Arc::new(ShardedDb::with_persistence(&config, &persistence_config))
    } else {
        Arc::new(ShardedDb::new(&config))
    };
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles successfully

**Step 4: Commit**

```bash
git add src/main.rs src/engine/db.rs
git commit -m "feat(persistence): integrate recovery into startup"
```

---

## Task 12: Run All Tests and Verify

**Step 1: Run all tests**

Run: `cargo test`
Expected: All tests pass

**Step 2: Test persistence manually**

```bash
# Build
cargo build --release

# Run with persistence
./target/release/quota-db --persistence --data-dir /tmp/quota-db-test &

# Add some data
redis-cli -p 6380 INCR mycounter
redis-cli -p 6380 INCR mycounter
redis-cli -p 6380 GET mycounter  # Should return 2

# Kill and restart
kill %1
./target/release/quota-db --persistence --data-dir /tmp/quota-db-test &

# Verify data persisted
redis-cli -p 6380 GET mycounter  # Should still return 2

# Cleanup
kill %1
rm -rf /tmp/quota-db-test
```

**Step 3: Commit final state**

```bash
git add -A
git commit -m "feat(persistence): Phase 5 complete - WAL + Snapshots"
```

---

## Summary

This plan implements:
1. **PersistenceConfig** - Configuration for persistence layer
2. **WAL (Write-Ahead Log)** - Append-only log with checksums
3. **Snapshots** - Full state dumps with atomic writes
4. **Recovery** - Load snapshot + replay WAL
5. **CLI integration** - `--persistence` flag and related options

Key design decisions:
- Per-shard files for parallel I/O
- Batched fsync for performance
- CRC32 checksums for corruption detection
- Atomic snapshot writes (tmp + rename)
- Opt-in persistence (disabled by default)
