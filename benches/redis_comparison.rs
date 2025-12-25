//! Redis vs QuotaDB comparison benchmarks
//!
//! These benchmarks compare QuotaDB performance against Redis.
//! Run with: cargo bench --bench redis_comparison
//!
//! Prerequisites:
//! - Redis running on localhost:6379
//! - QuotaDB running on localhost:6380

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

const REDIS_ADDR: &str = "127.0.0.1:6379";
const QUOTA_DB_ADDR: &str = "127.0.0.1:6380";

/// Simple RESP client for benchmarking
struct RespClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl RespClient {
    fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        Ok(Self {
            stream,
            buf: vec![0u8; 4096],
        })
    }

    fn send_command(&mut self, cmd: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(cmd)?;
        // Read response (simplified - assumes response fits in buffer)
        let n = self.stream.read(&mut self.buf)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        Ok(())
    }

    fn ping(&mut self) -> std::io::Result<()> {
        self.send_command(b"*1\r\n$4\r\nPING\r\n")
    }

    fn incr(&mut self, key: &str) -> std::io::Result<()> {
        let cmd = format!("*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n", key.len(), key);
        self.send_command(cmd.as_bytes())
    }

    fn incrby(&mut self, key: &str, value: i64) -> std::io::Result<()> {
        let value_str = value.to_string();
        let cmd = format!(
            "*3\r\n$6\r\nINCRBY\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            value_str.len(),
            value_str
        );
        self.send_command(cmd.as_bytes())
    }

    fn get(&mut self, key: &str) -> std::io::Result<()> {
        let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
        self.send_command(cmd.as_bytes())
    }

    fn del(&mut self, key: &str) -> std::io::Result<()> {
        let cmd = format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key);
        self.send_command(cmd.as_bytes())
    }

    fn flushdb(&mut self) -> std::io::Result<()> {
        self.send_command(b"*1\r\n$7\r\nFLUSHDB\r\n")
    }
}

fn check_server(addr: &str, name: &str) -> bool {
    match RespClient::connect(addr) {
        Ok(mut client) => {
            if client.ping().is_ok() {
                println!("{} is available at {}", name, addr);
                true
            } else {
                println!("{} at {} - ping failed", name, addr);
                false
            }
        }
        Err(e) => {
            println!("{} at {} - connection failed: {}", name, addr, e);
            false
        }
    }
}

fn bench_single_incr(c: &mut Criterion) {
    let redis_available = check_server(REDIS_ADDR, "Redis");
    let quotadb_available = check_server(QUOTA_DB_ADDR, "QuotaDB");

    if !redis_available && !quotadb_available {
        println!("Skipping single_incr benchmark: no servers available");
        return;
    }

    let mut group = c.benchmark_group("single_incr_comparison");
    group.throughput(Throughput::Elements(1));

    if redis_available {
        if let Ok(mut client) = RespClient::connect(REDIS_ADDR) {
            let _ = client.del("bench:counter");
            group.bench_function("redis", |b| {
                b.iter(|| {
                    client.incr("bench:counter").unwrap();
                })
            });
        }
    }

    if quotadb_available {
        if let Ok(mut client) = RespClient::connect(QUOTA_DB_ADDR) {
            let _ = client.del("bench:counter");
            group.bench_function("quotadb", |b| {
                b.iter(|| {
                    client.incr("bench:counter").unwrap();
                })
            });
        }
    }

    group.finish();
}

fn bench_bulk_incr(c: &mut Criterion) {
    let redis_available = check_server(REDIS_ADDR, "Redis");
    let quotadb_available = check_server(QUOTA_DB_ADDR, "QuotaDB");

    if !redis_available && !quotadb_available {
        println!("Skipping bulk_incr benchmark: no servers available");
        return;
    }

    let mut group = c.benchmark_group("bulk_incr_comparison");

    for count in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*count as u64));

        if redis_available {
            if let Ok(mut client) = RespClient::connect(REDIS_ADDR) {
                let _ = client.flushdb();
                group.bench_with_input(BenchmarkId::new("redis", count), count, |b, &count| {
                    b.iter(|| {
                        for i in 0..count {
                            let key = format!("bulk:key:{}", i);
                            client.incr(&key).unwrap();
                        }
                    })
                });
            }
        }

        if quotadb_available {
            if let Ok(mut client) = RespClient::connect(QUOTA_DB_ADDR) {
                group.bench_with_input(BenchmarkId::new("quotadb", count), count, |b, &count| {
                    b.iter(|| {
                        for i in 0..count {
                            let key = format!("bulk:key:{}", i);
                            client.incr(&key).unwrap();
                        }
                    })
                });
            }
        }
    }

    group.finish();
}

fn bench_get_existing(c: &mut Criterion) {
    let redis_available = check_server(REDIS_ADDR, "Redis");
    let quotadb_available = check_server(QUOTA_DB_ADDR, "QuotaDB");

    if !redis_available && !quotadb_available {
        println!("Skipping get_existing benchmark: no servers available");
        return;
    }

    let mut group = c.benchmark_group("get_existing_comparison");
    group.throughput(Throughput::Elements(1));

    if redis_available {
        if let Ok(mut client) = RespClient::connect(REDIS_ADDR) {
            // Setup: create key with value
            let _ = client.del("bench:get");
            let _ = client.incrby("bench:get", 12345);

            group.bench_function("redis", |b| {
                b.iter(|| {
                    client.get("bench:get").unwrap();
                })
            });
        }
    }

    if quotadb_available {
        if let Ok(mut client) = RespClient::connect(QUOTA_DB_ADDR) {
            // Setup: create key with value
            let _ = client.incrby("bench:get", 12345);

            group.bench_function("quotadb", |b| {
                b.iter(|| {
                    client.get("bench:get").unwrap();
                })
            });
        }
    }

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let redis_available = check_server(REDIS_ADDR, "Redis");
    let quotadb_available = check_server(QUOTA_DB_ADDR, "QuotaDB");

    if !redis_available && !quotadb_available {
        println!("Skipping mixed_workload benchmark: no servers available");
        return;
    }

    let mut group = c.benchmark_group("mixed_workload_comparison");
    group.throughput(Throughput::Elements(100)); // 100 operations per iteration

    if redis_available {
        if let Ok(mut client) = RespClient::connect(REDIS_ADDR) {
            let _ = client.flushdb();
            // Pre-populate some keys
            for i in 0..50 {
                let _ = client.incrby(&format!("mix:key:{}", i), i as i64);
            }

            group.bench_function("redis", |b| {
                let mut counter = 0u64;
                b.iter(|| {
                    // 70% reads, 30% writes
                    for _ in 0..70 {
                        let key = format!("mix:key:{}", counter % 50);
                        client.get(&key).unwrap();
                        counter = counter.wrapping_add(1);
                    }
                    for _ in 0..30 {
                        let key = format!("mix:key:{}", counter % 100);
                        client.incr(&key).unwrap();
                        counter = counter.wrapping_add(1);
                    }
                })
            });
        }
    }

    if quotadb_available {
        if let Ok(mut client) = RespClient::connect(QUOTA_DB_ADDR) {
            // Pre-populate some keys
            for i in 0..50 {
                let _ = client.incrby(&format!("mix:key:{}", i), i as i64);
            }

            group.bench_function("quotadb", |b| {
                let mut counter = 0u64;
                b.iter(|| {
                    // 70% reads, 30% writes
                    for _ in 0..70 {
                        let key = format!("mix:key:{}", counter % 50);
                        client.get(&key).unwrap();
                        counter = counter.wrapping_add(1);
                    }
                    for _ in 0..30 {
                        let key = format!("mix:key:{}", counter % 100);
                        client.incr(&key).unwrap();
                        counter = counter.wrapping_add(1);
                    }
                })
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_incr,
    bench_bulk_incr,
    bench_get_existing,
    bench_mixed_workload,
);
criterion_main!(benches);
