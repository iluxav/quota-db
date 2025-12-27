//! Async WAL writer using tokio for non-blocking I/O.

use std::io;
use std::path::Path;
use std::time::Instant;

use crc32fast::Hasher;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::persistence::{WalEntry, WalHeader, WAL_MAGIC, WAL_VERSION};

/// Maximum retries for transient sync failures
const MAX_SYNC_RETRIES: u32 = 3;

/// Maximum consecutive sync failures before degraded state
const MAX_CONSECUTIVE_SYNC_FAILURES: u32 = 10;

/// Async WAL writer using tokio for non-blocking disk I/O.
pub struct AsyncWalWriter {
    writer: BufWriter<File>,
    #[allow(dead_code)]
    shard_id: u16,
    pending_count: usize,
    pending_bytes: usize,
    last_sync: Instant,
    sync_ops_threshold: usize,
    consecutive_sync_failures: u32,
    total_sync_failures: u64,
    degraded: bool,
}

impl AsyncWalWriter {
    /// Create or open a WAL file for async writing.
    pub async fn open(path: &Path, shard_id: u16, sync_ops_threshold: usize) -> io::Result<Self> {
        let exists = path.exists();

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .await?;

        let mut writer = BufWriter::with_capacity(64 * 1024, file); // 64KB buffer

        // Write header if new file
        if !exists {
            let header = WalHeader {
                magic: WAL_MAGIC,
                version: WAL_VERSION,
                shard_id,
                reserved: [0; 6],
            };
            let header_bytes = bincode::serialize(&header)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            writer.write_all(&header_bytes).await?;
            writer.flush().await?;
        }

        Ok(Self {
            writer,
            shard_id,
            pending_count: 0,
            pending_bytes: 0,
            last_sync: Instant::now(),
            sync_ops_threshold,
            consecutive_sync_failures: 0,
            total_sync_failures: 0,
            degraded: false,
        })
    }

    /// Append an entry to the WAL asynchronously.
    /// Returns the number of bytes written.
    pub async fn append(&mut self, entry: &WalEntry) -> io::Result<usize> {
        // Serialize entry
        let data = bincode::serialize(entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Calculate checksum
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let checksum = hasher.finalize();

        // Write: [len: u32][checksum: u32][data]
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes()).await?;
        self.writer.write_all(&checksum.to_le_bytes()).await?;
        self.writer.write_all(&data).await?;

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

    /// Flush buffer and sync to disk asynchronously with retry logic.
    pub async fn sync(&mut self) -> io::Result<()> {
        // Skip if no pending data
        if self.pending_count == 0 {
            return Ok(());
        }

        let mut last_error = None;

        for attempt in 0..MAX_SYNC_RETRIES {
            // Flush buffer first
            if let Err(e) = self.writer.flush().await {
                if Self::is_transient_error(&e) && attempt < MAX_SYNC_RETRIES - 1 {
                    last_error = Some(e);
                    tokio::time::sleep(std::time::Duration::from_millis(10 << attempt)).await;
                    continue;
                }
                return self.handle_sync_failure(e);
            }

            // Sync to disk
            if let Err(e) = self.writer.get_ref().sync_data().await {
                if Self::is_transient_error(&e) && attempt < MAX_SYNC_RETRIES - 1 {
                    last_error = Some(e);
                    tokio::time::sleep(std::time::Duration::from_millis(10 << attempt)).await;
                    continue;
                }
                return self.handle_sync_failure(e);
            }

            // Success - reset failure counters
            self.pending_count = 0;
            self.pending_bytes = 0;
            self.last_sync = Instant::now();
            self.consecutive_sync_failures = 0;
            if self.degraded {
                self.degraded = false;
            }
            return Ok(());
        }

        // All retries exhausted
        self.handle_sync_failure(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "sync failed after retries")
        }))
    }

    /// Check if an error is transient and worth retrying
    fn is_transient_error(e: &io::Error) -> bool {
        matches!(
            e.kind(),
            io::ErrorKind::Interrupted
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::TimedOut
        )
    }

    /// Handle a sync failure
    fn handle_sync_failure(&mut self, e: io::Error) -> io::Result<()> {
        self.consecutive_sync_failures += 1;
        self.total_sync_failures += 1;

        if self.consecutive_sync_failures >= MAX_CONSECUTIVE_SYNC_FAILURES && !self.degraded {
            self.degraded = true;
        }

        Err(e)
    }

    /// Check if the WAL is in degraded mode
    pub fn is_degraded(&self) -> bool {
        self.degraded
    }

    /// Get the number of consecutive sync failures
    pub fn consecutive_sync_failures(&self) -> u32 {
        self.consecutive_sync_failures
    }

    /// Get pending byte count
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Get the current file size
    pub async fn file_size(&self) -> io::Result<u64> {
        self.writer.get_ref().metadata().await.map(|m| m.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{WalOp, WalReader};
    use crate::types::Key;

    #[tokio::test]
    async fn test_async_wal_writer_create() {
        let dir = std::env::temp_dir().join("quota-db-test-async-wal-create");
        let _ = std::fs::remove_dir_all(&dir);

        let path = dir.join("wal.bin");
        let writer = AsyncWalWriter::open(&path, 0, 1000).await.unwrap();

        assert!(path.exists());
        drop(writer);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn test_async_wal_writer_append_and_sync() {
        let dir = std::env::temp_dir().join("quota-db-test-async-wal-append");
        let _ = std::fs::remove_dir_all(&dir);

        let path = dir.join("wal.bin");
        let mut writer = AsyncWalWriter::open(&path, 0, 1000).await.unwrap();

        let entry = WalEntry {
            seq: 1,
            timestamp: 12345,
            op: WalOp::Incr {
                key: Key::from("test"),
                node_id: 1,
                delta: 100,
            },
        };

        let bytes = writer.append(&entry).await.unwrap();
        assert!(bytes > 0);
        writer.sync().await.unwrap();

        // Verify with sync reader
        let mut reader = WalReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq, 1);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn test_async_wal_roundtrip() {
        let dir = std::env::temp_dir().join("quota-db-test-async-wal-roundtrip");
        let _ = std::fs::remove_dir_all(&dir);

        let path = dir.join("wal.bin");

        // Write entries async
        {
            let mut writer = AsyncWalWriter::open(&path, 5, 1000).await.unwrap();
            for i in 1..=100 {
                let entry = WalEntry {
                    seq: i,
                    timestamp: i * 1000,
                    op: WalOp::Incr {
                        key: Key::from(format!("key{}", i)),
                        node_id: 1,
                        delta: i,
                    },
                };
                writer.append(&entry).await.unwrap();
            }
            writer.sync().await.unwrap();
        }

        // Read entries with sync reader
        {
            let mut reader = WalReader::open(&path).unwrap();
            assert_eq!(reader.shard_id(), 5);

            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 100);

            for (i, entry) in entries.iter().enumerate() {
                assert_eq!(entry.seq, (i + 1) as u64);
            }
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
