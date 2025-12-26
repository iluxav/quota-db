use rustc_hash::FxHashMap;

use crate::engine::quota::QuotaEntry;
use crate::types::NodeId;

/// Entry type in the database - either a PN-Counter or a Quota.
#[derive(Debug, Clone)]
pub enum Entry {
    /// PN-Counter CRDT for eventual consistency counting
    Counter(PnCounterEntry),
    /// Quota/rate-limit entry with token bucket semantics
    Quota(QuotaEntry),
}

impl Entry {
    /// Check if this is a quota entry.
    #[inline]
    pub fn is_quota(&self) -> bool {
        matches!(self, Entry::Quota(_))
    }

    /// Check if this is a counter entry.
    #[inline]
    pub fn is_counter(&self) -> bool {
        matches!(self, Entry::Counter(_))
    }

    /// Get as counter reference, if it is one.
    #[inline]
    pub fn as_counter(&self) -> Option<&PnCounterEntry> {
        match self {
            Entry::Counter(c) => Some(c),
            _ => None,
        }
    }

    /// Get as counter mutable reference, if it is one.
    #[inline]
    pub fn as_counter_mut(&mut self) -> Option<&mut PnCounterEntry> {
        match self {
            Entry::Counter(c) => Some(c),
            _ => None,
        }
    }

    /// Get as quota reference, if it is one.
    #[inline]
    pub fn as_quota(&self) -> Option<&QuotaEntry> {
        match self {
            Entry::Quota(q) => Some(q),
            _ => None,
        }
    }

    /// Get as quota mutable reference, if it is one.
    #[inline]
    pub fn as_quota_mut(&mut self) -> Option<&mut QuotaEntry> {
        match self {
            Entry::Quota(q) => Some(q),
            _ => None,
        }
    }
}

impl From<PnCounterEntry> for Entry {
    fn from(counter: PnCounterEntry) -> Self {
        Entry::Counter(counter)
    }
}

impl From<QuotaEntry> for Entry {
    fn from(quota: QuotaEntry) -> Self {
        Entry::Quota(quota)
    }
}

/// PN-Counter CRDT entry with dynamic region support.
///
/// Each counter maintains:
/// - P (positive): monotonically increasing counters per node (increments)
/// - N (negative): monotonically increasing counters per node (decrements)
///
/// Value = sum(P) - sum(N)
///
/// The CRDT merge rule takes the max of each component per node,
/// ensuring convergence across replicas.
///
/// Uses cached value for O(1) reads instead of O(nodes) iteration.
#[derive(Debug, Clone)]
pub struct PnCounterEntry {
    /// Increment counters per node
    p: FxHashMap<NodeId, u64>,

    /// Decrement counters per node
    n: FxHashMap<NodeId, u64>,

    /// Cached computed value (sum(P) - sum(N)) for O(1) reads
    cached_value: i64,

    /// Optional TTL: Unix timestamp in milliseconds when this entry expires
    expires_at: Option<u64>,
}

impl PnCounterEntry {
    /// Create a new empty PN-counter
    #[inline]
    pub fn new() -> Self {
        Self {
            p: FxHashMap::default(),
            n: FxHashMap::default(),
            cached_value: 0,
            expires_at: None,
        }
    }

    /// Increment by delta for the given node.
    /// P counters are monotonically increasing, so we add to existing value.
    #[inline]
    pub fn increment(&mut self, node: NodeId, delta: u64) {
        *self.p.entry(node).or_insert(0) += delta;
        self.cached_value += delta as i64;
    }

    /// Decrement by delta for the given node.
    /// N counters are monotonically increasing, so we add to existing value.
    #[inline]
    pub fn decrement(&mut self, node: NodeId, delta: u64) {
        *self.n.entry(node).or_insert(0) += delta;
        self.cached_value -= delta as i64;
    }

    /// Get the current value: sum(P) - sum(N)
    /// Returns cached value for O(1) performance.
    #[inline]
    pub fn value(&self) -> i64 {
        self.cached_value
    }

    /// Set the counter to a specific value for a given node.
    /// This is done by resetting P and N for that node.
    pub fn set_value(&mut self, node: NodeId, value: i64) {
        // Clear all existing entries
        self.p.clear();
        self.n.clear();

        // Set the value using P or N depending on sign
        if value >= 0 {
            self.p.insert(node, value as u64);
        } else {
            self.n.insert(node, (-value) as u64);
        }
        self.cached_value = value;
    }

    /// Merge with another PN-counter (CRDT merge rule: max per component).
    /// This operation is:
    /// - Commutative: merge(A, B) == merge(B, A)
    /// - Associative: merge(merge(A, B), C) == merge(A, merge(B, C))
    /// - Idempotent: merge(A, A) == A
    pub fn merge(&mut self, other: &PnCounterEntry) {
        for (&node, &count) in &other.p {
            self.p
                .entry(node)
                .and_modify(|v| *v = (*v).max(count))
                .or_insert(count);
        }
        for (&node, &count) in &other.n {
            self.n
                .entry(node)
                .and_modify(|v| *v = (*v).max(count))
                .or_insert(count);
        }
        // Recalculate cached value after merge
        self.recalculate_value();
    }

    /// Recalculate and cache the value from P and N maps.
    /// Called after merge operations.
    #[inline]
    fn recalculate_value(&mut self) {
        let sum_p: u64 = self.p.values().sum();
        let sum_n: u64 = self.n.values().sum();
        self.cached_value = (sum_p as i64) - (sum_n as i64);
    }

    /// Get the P component for a specific node
    #[inline]
    pub fn get_p(&self, node: NodeId) -> u64 {
        self.p.get(&node).copied().unwrap_or(0)
    }

    /// Get the N component for a specific node
    #[inline]
    pub fn get_n(&self, node: NodeId) -> u64 {
        self.n.get(&node).copied().unwrap_or(0)
    }

    /// Set expiration time
    #[inline]
    pub fn set_expires_at(&mut self, ts: u64) {
        self.expires_at = Some(ts);
    }

    /// Get expiration time
    #[inline]
    pub fn expires_at(&self) -> Option<u64> {
        self.expires_at
    }

    /// Check if entry is expired
    #[inline]
    pub fn is_expired(&self, current_ts: u64) -> bool {
        self.expires_at.map_or(false, |exp| current_ts >= exp)
    }

    /// Get the number of nodes that have contributed to this counter
    pub fn node_count(&self) -> usize {
        let mut nodes: std::collections::HashSet<NodeId> = self.p.keys().copied().collect();
        nodes.extend(self.n.keys());
        nodes.len()
    }

    /// Apply a remote increment delta.
    /// Used for replication: adds delta to the specified node's P counter.
    /// Unlike local increment, this is additive to handle out-of-order deltas.
    #[inline]
    pub fn apply_remote_p(&mut self, node: NodeId, delta: u64) {
        *self.p.entry(node).or_insert(0) += delta;
        self.cached_value += delta as i64;
    }

    /// Apply a remote decrement delta.
    /// Used for replication: adds delta to the specified node's N counter.
    /// Unlike local decrement, this is additive to handle out-of-order deltas.
    #[inline]
    pub fn apply_remote_n(&mut self, node: NodeId, delta: u64) {
        *self.n.entry(node).or_insert(0) += delta;
        self.cached_value -= delta as i64;
    }
}

impl Default for PnCounterEntry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_counter_is_zero() {
        let counter = PnCounterEntry::new();
        assert_eq!(counter.value(), 0);
    }

    #[test]
    fn test_increment() {
        let mut counter = PnCounterEntry::new();
        let node = NodeId::new(1);

        counter.increment(node, 5);
        assert_eq!(counter.value(), 5);

        counter.increment(node, 3);
        assert_eq!(counter.value(), 8);
    }

    #[test]
    fn test_decrement() {
        let mut counter = PnCounterEntry::new();
        let node = NodeId::new(1);

        counter.increment(node, 10);
        counter.decrement(node, 3);
        assert_eq!(counter.value(), 7);
    }

    #[test]
    fn test_negative_value() {
        let mut counter = PnCounterEntry::new();
        let node = NodeId::new(1);

        counter.decrement(node, 5);
        assert_eq!(counter.value(), -5);
    }

    #[test]
    fn test_multiple_nodes() {
        let mut counter = PnCounterEntry::new();
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);
        let node3 = NodeId::new(3);

        counter.increment(node1, 10);
        counter.increment(node2, 20);
        counter.decrement(node3, 5);

        assert_eq!(counter.value(), 25); // 10 + 20 - 5
    }

    #[test]
    fn test_merge_commutative() {
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);

        let mut a = PnCounterEntry::new();
        a.increment(node1, 10);

        let mut b = PnCounterEntry::new();
        b.increment(node2, 20);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn test_merge_idempotent() {
        let node = NodeId::new(1);

        let mut counter = PnCounterEntry::new();
        counter.increment(node, 10);

        let before = counter.value();
        counter.merge(&counter.clone());
        let after = counter.value();

        assert_eq!(before, after);
    }

    #[test]
    fn test_merge_takes_max() {
        let node = NodeId::new(1);

        let mut a = PnCounterEntry::new();
        a.increment(node, 10);

        let mut b = PnCounterEntry::new();
        b.increment(node, 5); // Lower value

        a.merge(&b);
        assert_eq!(a.get_p(node), 10); // Should keep the max
    }

    #[test]
    fn test_set_value_positive() {
        let mut counter = PnCounterEntry::new();
        let node = NodeId::new(1);

        counter.set_value(node, 42);
        assert_eq!(counter.value(), 42);
    }

    #[test]
    fn test_set_value_negative() {
        let mut counter = PnCounterEntry::new();
        let node = NodeId::new(1);

        counter.set_value(node, -10);
        assert_eq!(counter.value(), -10);
    }

    #[test]
    fn test_expiration() {
        let mut counter = PnCounterEntry::new();
        counter.set_expires_at(1000);

        assert!(!counter.is_expired(500));
        assert!(!counter.is_expired(999));
        assert!(counter.is_expired(1000));
        assert!(counter.is_expired(1001));
    }

    #[test]
    fn test_no_expiration() {
        let counter = PnCounterEntry::new();
        assert!(!counter.is_expired(u64::MAX));
    }
}
