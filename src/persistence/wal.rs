use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::time::Instant;

use crc32fast::Hasher;
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

/// Writer for append-only WAL file.
pub struct WalWriter {
    writer: BufWriter<File>,
    #[allow(dead_code)] // Will be used for WAL file naming/identification
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
}
