//! Criterion microbenchmarks for the custom B-Tree KV engine.
//!
//! These give machine-measured numbers for the performance-sensitive paths of
//! `BTreeKvEngine` — MVCC point reads, range scans, write+commit, and garbage
//! collection — so perf claims about the engine can be reproduced rather than
//! asserted. Run with `cargo bench -p nodus_storage_btree` (or `just bench`).

use std::hint::black_box;

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nodus_storage_api::{KeyRange, KvEngine, TxnId};
use nodus_storage_btree::BTreeKvEngine;

/// Stable, lexicographically ordered key for index `i`.
fn key(i: usize) -> Bytes {
    Bytes::from(format!("key:{i:08}"))
}

/// Build an engine pre-populated with `n` committed single-version keys.
/// Key `i` commits at timestamp `i + 1`, so a read at `n + 1` sees every key.
fn seed(n: usize) -> BTreeKvEngine {
    let engine = BTreeKvEngine::new();
    for i in 0..n {
        let txn = TxnId::new();
        engine
            .write_intent(txn, key(i), Bytes::from(format!("value-{i}")))
            .unwrap();
        engine.commit(txn, (i as u64) + 1).unwrap();
    }
    engine
}

fn bench_point_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/point_read_hit");
    for &n in &[100usize, 1_000, 10_000] {
        let engine = seed(n);
        let read_ts = n as u64 + 1;
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut i = 0usize;
            b.iter(|| {
                let k = key(i % n);
                i = i.wrapping_add(1);
                black_box(engine.get(black_box(k.as_ref()), read_ts).unwrap())
            });
        });
    }
    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let engine = seed(10_000);
    let read_ts = 10_001;
    let mut group = c.benchmark_group("btree/scan");
    for &span in &[10usize, 100, 1_000] {
        group.throughput(Throughput::Elements(span as u64));
        group.bench_with_input(BenchmarkId::from_parameter(span), &span, |b, &span| {
            b.iter(|| {
                let range = KeyRange {
                    start: key(0),
                    end: key(span),
                };
                let iter = engine.scan(range, read_ts).unwrap();
                black_box(iter.count())
            });
        });
    }
    group.finish();
}

fn bench_write_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/write_commit");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_key", |b| {
        let engine = BTreeKvEngine::new();
        let mut i = 0u64;
        b.iter(|| {
            let txn = TxnId::new();
            let k = Bytes::from(format!("w:{i:08}"));
            engine
                .write_intent(txn, k, Bytes::from("payload-value"))
                .unwrap();
            engine.commit(txn, i + 1).unwrap();
            i += 1;
        });
    });
    group.finish();
}

fn bench_gc(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/garbage_collect");
    const N: usize = 1_000;
    group.throughput(Throughput::Elements(N as u64));
    group.bench_function("two_versions_each", |b| {
        b.iter_batched(
            || {
                // Each key gets two committed versions; GC at a high watermark
                // can reclaim the older one.
                let engine = BTreeKvEngine::new();
                for i in 0..N {
                    let t1 = TxnId::new();
                    engine.write_intent(t1, key(i), Bytes::from("v1")).unwrap();
                    engine.commit(t1, 10).unwrap();
                    let t2 = TxnId::new();
                    engine.write_intent(t2, key(i), Bytes::from("v2")).unwrap();
                    engine.commit(t2, 20).unwrap();
                }
                engine
            },
            |engine| black_box(engine.garbage_collect(25).unwrap()),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_point_read,
    bench_scan,
    bench_write_commit,
    bench_gc
);
criterion_main!(benches);
