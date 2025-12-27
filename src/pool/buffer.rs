//! Lock-free buffer pool for BytesMut reuse.
//!
//! Reduces allocation overhead in hot paths by reusing buffers
//! across connections and operations.

use bytes::BytesMut;
use crossbeam::queue::ArrayQueue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

/// Default buffer capacity for pooled buffers.
pub const DEFAULT_BUFFER_CAPACITY: usize = 8192;

/// Maximum buffers to keep in pool.
const MAX_POOL_SIZE: usize = 256;

/// Global buffer pool for connection and message buffers.
static BUFFER_POOL_INNER: OnceLock<BufferPool> = OnceLock::new();

/// Get the global buffer pool.
pub fn buffer_pool() -> &'static BufferPool {
    BUFFER_POOL_INNER.get_or_init(BufferPool::new)
}

/// Lock-free pool of reusable BytesMut buffers.
pub struct BufferPool {
    /// Pool of available buffers.
    pool: ArrayQueue<BytesMut>,
    /// Statistics: buffers acquired from pool.
    hits: AtomicUsize,
    /// Statistics: buffers created new (pool miss).
    misses: AtomicUsize,
    /// Statistics: buffers returned to pool.
    returns: AtomicUsize,
    /// Statistics: buffers dropped (pool full).
    drops: AtomicUsize,
}

impl BufferPool {
    /// Create a new buffer pool.
    pub fn new() -> Self {
        Self {
            pool: ArrayQueue::new(MAX_POOL_SIZE),
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            returns: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        }
    }

    /// Get a buffer from the pool, or create a new one.
    #[inline]
    pub fn get(&self) -> BytesMut {
        self.get_with_capacity(DEFAULT_BUFFER_CAPACITY)
    }

    /// Get a buffer with at least the specified capacity.
    #[inline]
    pub fn get_with_capacity(&self, min_capacity: usize) -> BytesMut {
        // Try to get from pool
        if let Some(mut buf) = self.pool.pop() {
            self.hits.fetch_add(1, Ordering::Relaxed);
            buf.clear();
            // Ensure minimum capacity
            if buf.capacity() < min_capacity {
                buf.reserve(min_capacity - buf.capacity());
            }
            buf
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            BytesMut::with_capacity(min_capacity)
        }
    }

    /// Return a buffer to the pool.
    ///
    /// The buffer is cleared before being added to the pool.
    /// If the pool is full, the buffer is dropped.
    #[inline]
    pub fn put(&self, mut buf: BytesMut) {
        // Don't pool tiny or huge buffers
        if buf.capacity() < 1024 || buf.capacity() > 64 * 1024 {
            return;
        }

        buf.clear();

        if self.pool.push(buf).is_ok() {
            self.returns.fetch_add(1, Ordering::Relaxed);
        } else {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get pool statistics.
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            size: self.pool.len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            returns: self.returns.load(Ordering::Relaxed),
            drops: self.drops.load(Ordering::Relaxed),
        }
    }

    /// Get the current number of buffers in the pool.
    #[inline]
    pub fn len(&self) -> usize {
        self.pool.len()
    }

    /// Check if the pool is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pool.is_empty()
    }
}

/// Pool statistics for monitoring.
#[derive(Debug, Clone, Copy)]
pub struct PoolStats {
    /// Current buffers in pool.
    pub size: usize,
    /// Buffers acquired from pool (cache hits).
    pub hits: usize,
    /// Buffers created new (cache misses).
    pub misses: usize,
    /// Buffers returned to pool.
    pub returns: usize,
    /// Buffers dropped when pool was full.
    pub drops: usize,
}

impl PoolStats {
    /// Calculate hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// RAII guard that returns buffer to pool on drop.
pub struct PooledBuffer {
    buf: Option<BytesMut>,
}

impl PooledBuffer {
    /// Create a new pooled buffer.
    pub fn new() -> Self {
        Self {
            buf: Some(buffer_pool().get()),
        }
    }

    /// Create a new pooled buffer with minimum capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Some(buffer_pool().get_with_capacity(capacity)),
        }
    }

    /// Take the buffer, preventing return to pool.
    pub fn take(mut self) -> BytesMut {
        self.buf.take().unwrap()
    }
}

impl Default for PooledBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        self.buf.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut().unwrap()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            buffer_pool().put(buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_get_put() {
        let pool = BufferPool::new();

        // Get a buffer (should be a miss)
        let buf1 = pool.get();
        assert!(buf1.capacity() >= DEFAULT_BUFFER_CAPACITY);

        let stats = pool.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);

        // Put it back
        pool.put(buf1);
        assert_eq!(pool.len(), 1);

        // Get again (should be a hit)
        let buf2 = pool.get();
        assert!(buf2.capacity() >= DEFAULT_BUFFER_CAPACITY);

        let stats = pool.stats();
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn test_pool_clears_buffer() {
        let pool = BufferPool::new();

        let mut buf = pool.get();
        buf.extend_from_slice(b"hello world");
        assert!(!buf.is_empty());

        pool.put(buf);

        let buf2 = pool.get();
        assert!(buf2.is_empty());
    }

    #[test]
    fn test_pool_respects_capacity() {
        let pool = BufferPool::new();

        let buf = pool.get_with_capacity(16384);
        assert!(buf.capacity() >= 16384);
    }

    #[test]
    fn test_pooled_buffer_raii() {
        let initial_len = buffer_pool().len();

        // Create scope to test drop
        {
            let mut buf = PooledBuffer::with_capacity(8192);
            buf.extend_from_slice(b"test");
        }
        // Buffer should be back in pool
        assert!(buffer_pool().len() >= initial_len);

        // Get it back
        let buf = buffer_pool().get();
        assert!(buf.is_empty()); // Should be cleared
    }

    #[test]
    fn test_pool_stats() {
        let pool = BufferPool::new();

        let b1 = pool.get();
        let b2 = pool.get();
        pool.put(b1);
        let _b3 = pool.get();

        let stats = pool.stats();
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.returns, 1);
    }

    #[test]
    fn test_hit_rate() {
        let stats = PoolStats {
            size: 0,
            hits: 75,
            misses: 25,
            returns: 70,
            drops: 5,
        };
        assert!((stats.hit_rate() - 0.75).abs() < 0.001);
    }
}
