use std::fmt;
use std::hash::Hash;

/// Compact node identifier for CRDT regions.
///
/// Using u32 allows up to ~4 billion unique nodes while keeping memory compact.
/// Each node in the cluster has a unique NodeId used to track per-node
/// increment/decrement counts in PN-counters.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct NodeId(u32);

impl NodeId {
    /// Create a new NodeId from a u32 value
    #[inline]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the underlying u32 value
    #[inline]
    pub const fn as_u32(&self) -> u32 {
        self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for NodeId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<NodeId> for u32 {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_creation() {
        let id = NodeId::new(42);
        assert_eq!(id.as_u32(), 42);
    }

    #[test]
    fn test_node_id_equality() {
        let a = NodeId::new(1);
        let b = NodeId::new(1);
        let c = NodeId::new(2);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_node_id_hash() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(NodeId::new(1), "one");
        map.insert(NodeId::new(2), "two");
        assert_eq!(map.get(&NodeId::new(1)), Some(&"one"));
    }
}
