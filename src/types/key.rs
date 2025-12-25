use bytes::Bytes;
use rustc_hash::FxHasher;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Zero-copy key using bytes::Bytes for efficient cloning.
///
/// Keys are stored as Bytes which provides:
/// - Reference counting for cheap clones
/// - Zero-copy from network buffers
/// - Efficient comparison and hashing
#[derive(Clone, PartialEq, Eq)]
pub struct Key(Bytes);

impl Key {
    /// Create a new Key from Bytes
    #[inline]
    pub fn new(data: Bytes) -> Self {
        Self(data)
    }

    /// Create a Key from a static byte slice (no allocation)
    #[inline]
    pub fn from_static(s: &'static [u8]) -> Self {
        Self(Bytes::from_static(s))
    }

    /// Create a Key from a Vec<u8>
    #[inline]
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }

    /// Get the underlying bytes
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get the length of the key
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the key is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Compute fast hash for shard routing using FxHash.
    ///
    /// FxHash is a fast, non-cryptographic hash function used by rustc.
    /// It's ideal for hash table lookups and shard routing.
    #[inline]
    pub fn shard_hash(&self) -> u64 {
        let mut hasher = FxHasher::default();
        self.0.hash(&mut hasher);
        hasher.finish()
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "Key({:?})", s),
            Err(_) => write!(f, "Key({:?})", self.0),
        }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{}", s),
            Err(_) => write!(f, "{:?}", self.0),
        }
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Self(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for Key {
    fn from(s: String) -> Self {
        Self(Bytes::from(s))
    }
}

impl From<Bytes> for Key {
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

impl From<Vec<u8>> for Key {
    fn from(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_from_str() {
        let key = Key::from("test_key");
        assert_eq!(key.as_bytes(), b"test_key");
        assert_eq!(key.len(), 8);
    }

    #[test]
    fn test_key_shard_hash_consistency() {
        let key1 = Key::from("same_key");
        let key2 = Key::from("same_key");
        assert_eq!(key1.shard_hash(), key2.shard_hash());
    }

    #[test]
    fn test_key_shard_hash_distribution() {
        let key1 = Key::from("key1");
        let key2 = Key::from("key2");
        // Different keys should (usually) have different hashes
        // This isn't guaranteed but very likely
        assert_ne!(key1.shard_hash(), key2.shard_hash());
    }

    #[test]
    fn test_key_equality() {
        let a = Key::from("test");
        let b = Key::from("test");
        let c = Key::from("other");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_key_hash_for_hashmap() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(Key::from("key1"), 100);
        map.insert(Key::from("key2"), 200);
        assert_eq!(map.get(&Key::from("key1")), Some(&100));
        assert_eq!(map.get(&Key::from("key2")), Some(&200));
    }
}
