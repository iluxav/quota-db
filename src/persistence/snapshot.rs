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
