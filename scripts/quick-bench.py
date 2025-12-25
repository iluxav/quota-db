#!/usr/bin/env python3
"""Quick benchmark script for QuotaDB vs Redis comparison."""

import socket
import time
import sys
from datetime import datetime

REDIS_ADDR = ('localhost', 6379)
QUOTA_ADDR = ('localhost', 6380)

def connect(addr):
    """Create a TCP connection with TCP_NODELAY."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(5)
    try:
        s.connect(addr)
        return s
    except Exception as e:
        return None

def bench_single(name, addr, count=10000):
    """Benchmark single sequential commands."""
    s = connect(addr)
    if not s:
        print(f'{name}: NOT AVAILABLE')
        return None

    # Warmup
    for _ in range(100):
        s.send(b'*2\r\n$4\r\nINCR\r\n$4\r\nwarm\r\n')
        s.recv(64)

    # Benchmark
    start = time.perf_counter()
    for i in range(count):
        key = f'key{i}'.encode()
        cmd = f'*2\r\n$4\r\nINCR\r\n${len(key)}\r\n{key.decode()}\r\n'.encode()
        s.send(cmd)
        s.recv(64)
    elapsed = time.perf_counter() - start
    s.close()

    ops = count / elapsed
    latency = (elapsed / count) * 1_000_000
    print(f'{name}: {ops:>10,.0f} ops/sec  {latency:>6.1f} µs/op')
    return ops

def bench_pipeline(name, addr, count=10000, batch_size=100):
    """Benchmark pipelined commands."""
    s = connect(addr)
    if not s:
        print(f'{name}: NOT AVAILABLE')
        return None

    # Build batch command
    batch = b''
    for i in range(batch_size):
        key = f'p{i}'.encode()
        batch += f'*2\r\n$4\r\nINCR\r\n${len(key)}\r\n{key.decode()}\r\n'.encode()

    # Warmup
    s.send(batch)
    s.recv(8192)

    # Benchmark
    iterations = count // batch_size
    start = time.perf_counter()
    for _ in range(iterations):
        s.send(batch)
        received = 0
        while received < batch_size:
            data = s.recv(8192)
            received += data.count(b':')
    elapsed = time.perf_counter() - start
    s.close()

    total_ops = iterations * batch_size
    ops = total_ops / elapsed
    latency = (elapsed / total_ops) * 1_000_000
    print(f'{name}: {ops:>10,.0f} ops/sec  {latency:>6.2f} µs/op')
    return ops

def main():
    print(f'QuotaDB Benchmark - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)

    # Check availability
    redis_ok = connect(REDIS_ADDR)
    quota_ok = connect(QUOTA_ADDR)
    if redis_ok: redis_ok.close()
    if quota_ok: quota_ok.close()

    if not redis_ok and not quota_ok:
        print('ERROR: Neither Redis nor QuotaDB is running!')
        print('Start with: cd docker && docker compose up -d')
        sys.exit(1)

    print()
    print('Single Command (10k ops):')
    print('-' * 60)
    redis_single = bench_single('Redis   ', REDIS_ADDR)
    quota_single = bench_single('QuotaDB ', QUOTA_ADDR)
    if redis_single and quota_single:
        print(f'{"Ratio":>8}: {quota_single/redis_single:>10.2f}x')

    print()
    print('Pipeline batch=100 (10k ops):')
    print('-' * 60)
    redis_p100 = bench_pipeline('Redis   ', REDIS_ADDR, batch_size=100)
    quota_p100 = bench_pipeline('QuotaDB ', QUOTA_ADDR, batch_size=100)
    if redis_p100 and quota_p100:
        print(f'{"Ratio":>8}: {quota_p100/redis_p100:>10.2f}x')

    print()
    print('Pipeline batch=1000 (10k ops):')
    print('-' * 60)
    redis_p1k = bench_pipeline('Redis   ', REDIS_ADDR, batch_size=1000)
    quota_p1k = bench_pipeline('QuotaDB ', QUOTA_ADDR, batch_size=1000)
    if redis_p1k and quota_p1k:
        print(f'{"Ratio":>8}: {quota_p1k/redis_p1k:>10.2f}x')

    print()
    print('=' * 60)
    print('Done!')

if __name__ == '__main__':
    main()
