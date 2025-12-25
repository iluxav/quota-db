use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;

use quota_db::config::Config;
use quota_db::engine::ShardedDb;
use quota_db::types::Key;

/// Benchmark single-threaded INCR operations
fn bench_single_incr(c: &mut Criterion) {
    let config = Config {
        shards: 16,
        node_id: 1,
        ..Default::default()
    };
    let db = ShardedDb::new(&config);

    let mut group = c.benchmark_group("single_incr");
    group.throughput(Throughput::Elements(1));

    group.bench_function("increment_same_key", |b| {
        let key = Key::from("counter");
        b.iter(|| {
            black_box(db.increment(key.clone(), 1));
        })
    });

    group.bench_function("increment_unique_keys", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Key::from(format!("counter:{}", i));
            i = i.wrapping_add(1);
            black_box(db.increment(key, 1));
        })
    });

    group.finish();
}

/// Benchmark GET operations
fn bench_get(c: &mut Criterion) {
    let config = Config {
        shards: 16,
        node_id: 1,
        ..Default::default()
    };
    let db = ShardedDb::new(&config);

    // Pre-populate some keys
    for i in 0..10000 {
        let key = Key::from(format!("key:{}", i));
        db.increment(key, 1);
    }

    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));

    group.bench_function("get_existing_key", |b| {
        let key = Key::from("key:5000");
        b.iter(|| {
            black_box(db.get(&key));
        })
    });

    group.bench_function("get_missing_key", |b| {
        let key = Key::from("nonexistent");
        b.iter(|| {
            black_box(db.get(&key));
        })
    });

    group.finish();
}

/// Benchmark concurrent INCR operations
fn bench_concurrent_incr(c: &mut Criterion) {
    let config = Config {
        shards: 16,
        node_id: 1,
        ..Default::default()
    };
    let db = Arc::new(ShardedDb::new(&config));

    let mut group = c.benchmark_group("concurrent_incr");
    group.throughput(Throughput::Elements(1));

    // Same key contention
    group.bench_function("contended_4_threads", |b| {
        b.iter(|| {
            let db = db.clone();
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let db = db.clone();
                    std::thread::spawn(move || {
                        let key = Key::from("contended");
                        for _ in 0..1000 {
                            black_box(db.increment(key.clone(), 1));
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }
        })
    });

    // Different keys (no contention)
    group.bench_function("uncontended_4_threads", |b| {
        b.iter(|| {
            let db = db.clone();
            let handles: Vec<_> = (0..4)
                .map(|thread_id| {
                    let db = db.clone();
                    std::thread::spawn(move || {
                        for i in 0..1000 {
                            let key = Key::from(format!("key:{}:{}", thread_id, i));
                            black_box(db.increment(key, 1));
                        }
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }
        })
    });

    group.finish();
}

/// Benchmark shard distribution
fn bench_shard_distribution(c: &mut Criterion) {
    let config = Config {
        shards: 16,
        node_id: 1,
        ..Default::default()
    };
    let db = ShardedDb::new(&config);

    let mut group = c.benchmark_group("shard_distribution");
    group.throughput(Throughput::Elements(10000));

    group.bench_function("10k_unique_keys", |b| {
        b.iter(|| {
            for i in 0..10000 {
                let key = Key::from(format!("dist:key:{}", i));
                black_box(db.increment(key, 1));
            }
        })
    });

    group.finish();
}

/// Benchmark CRDT merge operations
fn bench_crdt_merge(c: &mut Criterion) {
    use quota_db::engine::PnCounterEntry;
    use quota_db::types::NodeId;

    let mut group = c.benchmark_group("crdt_merge");
    group.throughput(Throughput::Elements(1));

    group.bench_function("merge_3_node_entries", |b| {
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);
        let node3 = NodeId::new(3);

        b.iter(|| {
            let mut entry1 = PnCounterEntry::default();
            entry1.increment(node1, 100);
            entry1.increment(node2, 50);

            let mut entry2 = PnCounterEntry::default();
            entry2.increment(node2, 75);
            entry2.increment(node3, 25);
            entry2.decrement(node1, 10);

            entry1.merge(&entry2);
            black_box(entry1.value())
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_incr,
    bench_get,
    bench_concurrent_incr,
    bench_shard_distribution,
    bench_crdt_merge,
);
criterion_main!(benches);
