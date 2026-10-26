//! Criterion microbenchmarks for the custom LSM KV engine.
//!
//! These give machine-measured numbers for the performance-sensitive paths of
//! `LsmKvEngine` — MVCC point reads, range scans, write+commit, and garbage
//! collection — so perf claims about the engine can be reproduced rather than
//! asserted. WAL-less cases isolate engine cost; durable cases use a fresh
//! temporary WAL directory per measured input to include commit sync behavior.
//! Run with `cargo bench -p nodus_storage_lsm` (or `just bench`).

use std::hint::black_box;

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nodus_storage_api::{IntentOperation, KeyRange, KvEngine, TxnId};
use nodus_storage_lsm::LsmKvEngine;

/// Stable, lexicographically ordered key for index `i`.
fn key(i: usize) -> Bytes {
    Bytes::from(format!("key:{i:08}"))
}

/// Build an engine pre-populated with `n` committed single-version keys.
/// Key `i` commits at timestamp `i + 1`, so a read at `n + 1` sees every key.
fn seed(n: usize) -> LsmKvEngine {
    let engine = LsmKvEngine::new();
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
    let mut group = c.benchmark_group("lsm/point_read_hit");
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
    let mut group = c.benchmark_group("lsm/scan");
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
    let mut group = c.benchmark_group("lsm/write_commit");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_key", |b| {
        let engine = LsmKvEngine::new();
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

fn durable_commit_input(n: usize) -> (tempfile::TempDir, LsmKvEngine, Vec<TxnId>) {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    let txns = (0..n)
        .map(|i| {
            let txn = TxnId::new();
            engine
                .write_intent(txn, key(i), Bytes::from("durable-value"))
                .unwrap();
            txn
        })
        .collect();
    (dir, engine, txns)
}

fn bench_durable_commits(c: &mut Criterion) {
    const N: usize = 32;
    let mut group = c.benchmark_group("lsm/durable_commit");
    group.throughput(Throughput::Elements(N as u64));
    group.bench_function("single_commits", |b| {
        b.iter_batched(
            || durable_commit_input(N),
            |(_dir, engine, txns)| {
                for (i, txn) in txns.into_iter().enumerate() {
                    engine.commit(txn, i as u64 + 1).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("atomic_commit_batch", |b| {
        b.iter_batched(
            || durable_commit_input(N),
            |(_dir, engine, txns)| {
                let commits = txns
                    .into_iter()
                    .enumerate()
                    .map(|(i, txn)| (txn, i as u64 + 1))
                    .collect();
                engine.commit_batch(commits).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn durable_intent_input(n: usize) -> (tempfile::TempDir, LsmKvEngine, Vec<IntentOperation>) {
    let dir = tempfile::tempdir().unwrap();
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    let operations = (0..n)
        .map(|i| {
            if i % 4 == 3 {
                IntentOperation::Delete { key: key(i) }
            } else {
                IntentOperation::Put {
                    key: key(i),
                    value: Bytes::from("durable-value"),
                }
            }
        })
        .collect();
    (dir, engine, operations)
}

fn bench_durable_intent_batches(c: &mut Criterion) {
    const N: usize = 32;
    let mut group = c.benchmark_group("lsm/durable_intents");
    group.throughput(Throughput::Elements(N as u64));
    group.bench_function("sequential_intents", |b| {
        b.iter_batched(
            || durable_intent_input(N),
            |(_dir, engine, operations)| {
                let txn = TxnId::new();
                for operation in operations {
                    match operation {
                        IntentOperation::Put { key, value } => {
                            engine.write_intent(txn, key, value).unwrap();
                        }
                        IntentOperation::Delete { key } => {
                            engine.delete_intent(txn, key).unwrap();
                        }
                    }
                }
                engine.commit(txn, 1).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("atomic_intent_batch", |b| {
        b.iter_batched(
            || durable_intent_input(N),
            |(_dir, engine, operations)| {
                let txn = TxnId::new();
                engine.write_intent_batch(txn, operations).unwrap();
                engine.commit(txn, 1).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_gc(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm/garbage_collect");
    const N: usize = 1_000;
    group.throughput(Throughput::Elements(N as u64));
    group.bench_function("two_versions_each", |b| {
        b.iter_batched(
            || {
                // Each key gets two committed versions; GC at a high watermark
                // can reclaim the older one.
                let engine = LsmKvEngine::new();
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
    bench_durable_commits,
    bench_durable_intent_batches,
    bench_gc
);
criterion_main!(benches);
