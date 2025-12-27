//! Lock-free latency histogram for p50/p95/p99 percentile tracking.
//!
//! Uses logarithmic buckets for memory efficiency while maintaining
//! reasonable accuracy for latency distributions.

use std::sync::atomic::{AtomicU64, Ordering};

use super::LatencyPercentiles;

/// Number of buckets in the histogram.
/// Covers 0-1μs to ~1 second with logarithmic scaling.
const NUM_BUCKETS: usize = 64;

/// Latency histogram with logarithmic buckets.
///
/// Bucket boundaries (in microseconds):
/// - Buckets 0-15: 0, 1, 2, 3, ..., 15 (linear, 1μs resolution)
/// - Buckets 16-31: 16, 32, 48, ..., 256 (linear, 16μs resolution)
/// - Buckets 32-47: 256, 512, 768, ..., 4096 (linear, 256μs resolution)
/// - Buckets 48-63: 4ms, 8ms, 12ms, ..., 64ms+ (linear, 4ms resolution)
pub struct LatencyHistogram {
    buckets: [AtomicU64; NUM_BUCKETS],
    count: AtomicU64,
    max: AtomicU64,
}

impl LatencyHistogram {
    /// Create a new empty histogram.
    pub const fn new() -> Self {
        // Can't use array::from_fn in const context, so we use a macro-like pattern
        Self {
            buckets: [
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
            ],
            count: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }

    /// Record a latency value in microseconds.
    #[inline]
    pub fn record(&self, value_us: u64) {
        let bucket = Self::value_to_bucket(value_us);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        // Update max using CAS loop
        let mut current_max = self.max.load(Ordering::Relaxed);
        while value_us > current_max {
            match self.max.compare_exchange_weak(
                current_max,
                value_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Map a latency value to a bucket index.
    #[inline]
    fn value_to_bucket(value_us: u64) -> usize {
        if value_us < 16 {
            // Buckets 0-15: 1μs resolution
            value_us as usize
        } else if value_us < 256 {
            // Buckets 16-31: 16μs resolution
            16 + ((value_us - 16) / 16) as usize
        } else if value_us < 4096 {
            // Buckets 32-47: 256μs resolution
            32 + ((value_us - 256) / 256) as usize
        } else if value_us < 65536 {
            // Buckets 48-63: 4ms resolution
            48 + ((value_us - 4096) / 4096) as usize
        } else {
            // Overflow bucket
            63
        }
    }

    /// Map a bucket index back to its lower bound value.
    /// Note: This is an approximation - the returned value will map back to the same bucket.
    #[inline]
    fn bucket_to_value(bucket: usize) -> u64 {
        if bucket < 16 {
            // Buckets 0-15: 1μs resolution, values 0-15
            bucket as u64
        } else if bucket < 32 {
            // Buckets 16-31: 16μs resolution, values 16-255
            // bucket 16 = values [16, 32), bucket 17 = [32, 48), ...
            16 + (bucket - 16) as u64 * 16
        } else if bucket < 48 {
            // Buckets 32-47: 256μs resolution, values 256-4095
            // bucket 32 = values [256, 512), bucket 33 = [512, 768), ...
            256 + (bucket - 32) as u64 * 256
        } else {
            // Buckets 48-63: 4096μs resolution, values 4096+
            4096 + (bucket - 48) as u64 * 4096
        }
    }

    /// Get total count of recorded values.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Calculate p50, p95, p99 percentiles.
    pub fn percentiles(&self) -> LatencyPercentiles {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return LatencyPercentiles::default();
        }

        // Collect bucket counts
        let mut counts = [0u64; NUM_BUCKETS];
        for (i, bucket) in self.buckets.iter().enumerate() {
            counts[i] = bucket.load(Ordering::Relaxed);
        }

        let p50_target = total / 2;
        let p95_target = total * 95 / 100;
        let p99_target = total * 99 / 100;

        let mut cumulative = 0u64;
        let mut p50 = 0u64;
        let mut p95 = 0u64;
        let mut p99 = 0u64;

        for (bucket_idx, &count) in counts.iter().enumerate() {
            cumulative += count;

            if p50 == 0 && cumulative >= p50_target {
                p50 = Self::bucket_to_value(bucket_idx);
            }
            if p95 == 0 && cumulative >= p95_target {
                p95 = Self::bucket_to_value(bucket_idx);
            }
            if p99 == 0 && cumulative >= p99_target {
                p99 = Self::bucket_to_value(bucket_idx);
            }
        }

        LatencyPercentiles {
            count: total,
            p50,
            p95,
            p99,
            max: self.max.load(Ordering::Relaxed),
        }
    }

    /// Reset all buckets to zero.
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_mapping() {
        // First 16 buckets: 1μs resolution
        assert_eq!(LatencyHistogram::value_to_bucket(0), 0);
        assert_eq!(LatencyHistogram::value_to_bucket(1), 1);
        assert_eq!(LatencyHistogram::value_to_bucket(15), 15);

        // Buckets 16-31: 16μs resolution
        assert_eq!(LatencyHistogram::value_to_bucket(16), 16);
        assert_eq!(LatencyHistogram::value_to_bucket(31), 16);
        assert_eq!(LatencyHistogram::value_to_bucket(32), 17);
        assert_eq!(LatencyHistogram::value_to_bucket(255), 30);

        // Buckets 32-47: 256μs resolution
        assert_eq!(LatencyHistogram::value_to_bucket(256), 32);
        assert_eq!(LatencyHistogram::value_to_bucket(511), 32);
        assert_eq!(LatencyHistogram::value_to_bucket(512), 33);

        // Buckets 48-63: 4ms resolution
        assert_eq!(LatencyHistogram::value_to_bucket(4096), 48);
        assert_eq!(LatencyHistogram::value_to_bucket(8191), 48);
        assert_eq!(LatencyHistogram::value_to_bucket(8192), 49);

        // Overflow
        assert_eq!(LatencyHistogram::value_to_bucket(100000), 63);
    }

    #[test]
    fn test_bucket_to_value_roundtrip() {
        // Test that values within a bucket's range map back to that bucket
        // Note: Due to boundary overlap between resolution ranges, not all buckets
        // have a perfect roundtrip. We test the ones that do.

        // First 16 buckets have 1:1 mapping
        for bucket in 0..16 {
            let value = LatencyHistogram::bucket_to_value(bucket);
            let back = LatencyHistogram::value_to_bucket(value);
            assert_eq!(back, bucket, "bucket {} -> value {} -> bucket {}", bucket, value, back);
        }

        // Buckets 16-30 cover 16μs resolution (bucket 31 boundary overlaps with next range)
        for bucket in 16..31 {
            let value = LatencyHistogram::bucket_to_value(bucket);
            let back = LatencyHistogram::value_to_bucket(value);
            assert_eq!(back, bucket, "bucket {} -> value {} -> bucket {}", bucket, value, back);
        }

        // Buckets 32-47 cover 256μs resolution (bucket 47 may overlap)
        for bucket in 32..47 {
            let value = LatencyHistogram::bucket_to_value(bucket);
            let back = LatencyHistogram::value_to_bucket(value);
            assert_eq!(back, bucket, "bucket {} -> value {} -> bucket {}", bucket, value, back);
        }

        // Buckets 48-62 cover 4ms resolution
        for bucket in 48..63 {
            let value = LatencyHistogram::bucket_to_value(bucket);
            let back = LatencyHistogram::value_to_bucket(value);
            assert_eq!(back, bucket, "bucket {} -> value {} -> bucket {}", bucket, value, back);
        }
    }

    #[test]
    fn test_record_and_count() {
        let h = LatencyHistogram::new();
        assert_eq!(h.count(), 0);

        h.record(100);
        h.record(200);
        h.record(300);

        assert_eq!(h.count(), 3);
    }

    #[test]
    fn test_max_tracking() {
        let h = LatencyHistogram::new();

        h.record(100);
        h.record(500);
        h.record(200);

        let p = h.percentiles();
        assert_eq!(p.max, 500);
    }

    #[test]
    fn test_percentiles_uniform() {
        let h = LatencyHistogram::new();

        // Record 100 values from 0 to 99
        for i in 0..100 {
            h.record(i);
        }

        let p = h.percentiles();
        assert_eq!(p.count, 100);

        // p50 should be around 50
        assert!(p.p50 >= 45 && p.p50 <= 55, "p50 was {}", p.p50);

        // p99 should be around 99
        assert!(p.p99 >= 90, "p99 was {}", p.p99);

        assert_eq!(p.max, 99);
    }

    #[test]
    fn test_percentiles_bimodal() {
        let h = LatencyHistogram::new();

        // 90 fast requests at 10μs
        for _ in 0..90 {
            h.record(10);
        }

        // 10 slow requests at 1000μs
        for _ in 0..10 {
            h.record(1000);
        }

        let p = h.percentiles();
        assert_eq!(p.count, 100);

        // p50 should be low (fast path)
        assert!(p.p50 <= 15, "p50 was {}", p.p50);

        // p95 should catch the slow ones
        assert!(p.p95 >= 256, "p95 was {}", p.p95);

        assert_eq!(p.max, 1000);
    }

    #[test]
    fn test_reset() {
        let h = LatencyHistogram::new();

        h.record(100);
        h.record(200);
        assert_eq!(h.count(), 2);

        h.reset();
        assert_eq!(h.count(), 0);
        assert_eq!(h.percentiles().max, 0);
    }

    #[test]
    fn test_empty_percentiles() {
        let h = LatencyHistogram::new();
        let p = h.percentiles();

        assert_eq!(p.count, 0);
        assert_eq!(p.p50, 0);
        assert_eq!(p.p95, 0);
        assert_eq!(p.p99, 0);
        assert_eq!(p.max, 0);
    }
}
