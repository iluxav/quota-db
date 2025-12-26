use crate::replication::Delta;
use crate::types::{Key, NodeId};

/// Size of the replication ring buffer per shard
pub const REPLICATION_LOG_SIZE: usize = 4096;

/// Ring buffer for storing replication deltas.
///
/// Each shard has its own ReplicationLog that stores recent mutations
/// for streaming to peers. If a peer falls behind more than REPLICATION_LOG_SIZE
/// entries, it will need a full resync (handled in Phase 4).
pub struct ReplicationLog {
    /// Ring buffer storage
    buffer: Box<[Option<Delta>; REPLICATION_LOG_SIZE]>,
    /// Next sequence number to write (also equals total writes)
    head_seq: u64,
}

impl ReplicationLog {
    /// Create a new empty replication log
    pub fn new() -> Self {
        Self {
            buffer: Box::new([const { None }; REPLICATION_LOG_SIZE]),
            head_seq: 0,
        }
    }

    /// Append an increment delta to the log
    #[inline]
    pub fn append_increment(&mut self, key: Key, node_id: NodeId, delta: u64) -> Delta {
        let seq = self.head_seq;
        let d = Delta::increment(seq, key, node_id, delta);
        self.append_delta(d.clone());
        d
    }

    /// Append a decrement delta to the log
    #[inline]
    pub fn append_decrement(&mut self, key: Key, node_id: NodeId, delta: u64) -> Delta {
        let seq = self.head_seq;
        let d = Delta::decrement(seq, key, node_id, delta);
        self.append_delta(d.clone());
        d
    }

    /// Append a delta to the log
    fn append_delta(&mut self, delta: Delta) {
        let index = (self.head_seq as usize) % REPLICATION_LOG_SIZE;
        self.buffer[index] = Some(delta);
        self.head_seq += 1;
    }

    /// Get the current head sequence (next write position)
    #[inline]
    pub fn head_seq(&self) -> u64 {
        self.head_seq
    }

    /// Get the oldest available sequence in the buffer
    #[inline]
    pub fn tail_seq(&self) -> u64 {
        if self.head_seq <= REPLICATION_LOG_SIZE as u64 {
            0
        } else {
            self.head_seq - REPLICATION_LOG_SIZE as u64
        }
    }

    /// Check if a sequence is still available in the buffer
    #[inline]
    pub fn is_available(&self, seq: u64) -> bool {
        seq >= self.tail_seq() && seq < self.head_seq
    }

    /// Get a delta by sequence number
    pub fn get(&self, seq: u64) -> Option<&Delta> {
        if !self.is_available(seq) {
            return None;
        }
        let index = (seq as usize) % REPLICATION_LOG_SIZE;
        self.buffer[index].as_ref()
    }

    /// Iterate over deltas from start_seq (inclusive) to head_seq (exclusive)
    /// Returns None if start_seq is no longer available (peer too far behind)
    pub fn iter_from(&self, start_seq: u64) -> Option<ReplicationLogIter<'_>> {
        if start_seq > self.head_seq {
            // Peer is ahead of us (shouldn't happen normally)
            return Some(ReplicationLogIter {
                log: self,
                current: self.head_seq,
                end: self.head_seq,
            });
        }

        if start_seq < self.tail_seq() {
            // Peer is too far behind, needs full resync
            return None;
        }

        Some(ReplicationLogIter {
            log: self,
            current: start_seq,
            end: self.head_seq,
        })
    }

    /// Get the number of pending deltas for a peer at given acked_seq
    #[inline]
    pub fn pending_count(&self, acked_seq: u64) -> u64 {
        if acked_seq >= self.head_seq {
            0
        } else {
            self.head_seq - acked_seq
        }
    }

    /// Reset the log to a specific sequence number.
    /// Clears all buffer entries and sets head_seq to the given value.
    /// Used during snapshot restore.
    pub fn reset_to(&mut self, seq: u64) {
        self.head_seq = seq;
        for slot in self.buffer.iter_mut() {
            *slot = None;
        }
    }
}

impl Default for ReplicationLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over replication log entries
pub struct ReplicationLogIter<'a> {
    log: &'a ReplicationLog,
    current: u64,
    end: u64,
}

impl<'a> Iterator for ReplicationLogIter<'a> {
    type Item = &'a Delta;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.end {
            return None;
        }

        let delta = self.log.get(self.current)?;
        self.current += 1;
        Some(delta)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = (self.end - self.current) as usize;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for ReplicationLogIter<'a> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_log() {
        let log = ReplicationLog::new();
        assert_eq!(log.head_seq(), 0);
        assert_eq!(log.tail_seq(), 0);
    }

    #[test]
    fn test_append_and_get() {
        let mut log = ReplicationLog::new();
        let node = NodeId::new(1);

        log.append_increment(Key::from("key1"), node, 10);
        log.append_increment(Key::from("key2"), node, 20);
        log.append_decrement(Key::from("key3"), node, 5);

        assert_eq!(log.head_seq(), 3);

        let d0 = log.get(0).unwrap();
        assert_eq!(d0.delta_p, 10);
        assert_eq!(d0.key.as_bytes(), b"key1");

        let d1 = log.get(1).unwrap();
        assert_eq!(d1.delta_p, 20);

        let d2 = log.get(2).unwrap();
        assert_eq!(d2.delta_n, 5);
    }

    #[test]
    fn test_iter_from() {
        let mut log = ReplicationLog::new();
        let node = NodeId::new(1);

        for i in 0..10 {
            log.append_increment(Key::from(format!("key{}", i)), node, i as u64);
        }

        let deltas: Vec<_> = log.iter_from(5).unwrap().collect();
        assert_eq!(deltas.len(), 5);
        assert_eq!(deltas[0].seq, 5);
        assert_eq!(deltas[4].seq, 9);
    }

    #[test]
    fn test_ring_buffer_wrap() {
        let mut log = ReplicationLog::new();
        let node = NodeId::new(1);

        // Fill beyond capacity
        for i in 0..(REPLICATION_LOG_SIZE + 100) {
            log.append_increment(Key::from("key"), node, i as u64);
        }

        assert_eq!(log.head_seq(), (REPLICATION_LOG_SIZE + 100) as u64);
        assert_eq!(log.tail_seq(), 100);

        // Old entries should be unavailable
        assert!(!log.is_available(99));
        assert!(log.is_available(100));

        // iter_from should return None for too-old sequence
        assert!(log.iter_from(50).is_none());
    }

    #[test]
    fn test_pending_count() {
        let mut log = ReplicationLog::new();
        let node = NodeId::new(1);

        for _ in 0..100 {
            log.append_increment(Key::from("key"), node, 1);
        }

        assert_eq!(log.pending_count(0), 100);
        assert_eq!(log.pending_count(50), 50);
        assert_eq!(log.pending_count(100), 0);
        assert_eq!(log.pending_count(150), 0); // Ahead of head
    }

    #[test]
    fn test_reset_to() {
        let mut log = ReplicationLog::new();
        let node = NodeId::new(1);

        // Add some entries
        for i in 0..10 {
            log.append_increment(Key::from(format!("key{}", i)), node, i as u64);
        }

        assert_eq!(log.head_seq(), 10);
        assert!(log.get(5).is_some());

        // Reset to sequence 1000
        log.reset_to(1000);

        // head_seq should be updated
        assert_eq!(log.head_seq(), 1000);

        // Buffer should be cleared (old entries not available)
        assert!(log.get(5).is_none());
        assert!(log.get(999).is_none());

        // tail_seq should reflect new state
        assert_eq!(log.tail_seq(), 0); // Still within buffer size
    }

    #[test]
    fn test_reset_to_zero() {
        let mut log = ReplicationLog::new();
        let node = NodeId::new(1);

        for _ in 0..50 {
            log.append_increment(Key::from("key"), node, 1);
        }

        assert_eq!(log.head_seq(), 50);

        log.reset_to(0);

        assert_eq!(log.head_seq(), 0);
        assert_eq!(log.tail_seq(), 0);
        assert!(log.get(0).is_none());
    }
}
