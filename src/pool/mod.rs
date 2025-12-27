//! Memory pooling utilities for reducing allocation overhead.
//!
//! This module provides lock-free pools for frequently allocated
//! objects in hot paths, improving throughput and reducing GC pressure.

mod buffer;

pub use buffer::{buffer_pool, BufferPool, PooledBuffer, PoolStats, DEFAULT_BUFFER_CAPACITY};
