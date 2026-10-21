//! Server-level benchmarks (build in release mode via `cargo bench`): fixed
//! datasets, real pgwire protocol, a real data directory with WAL. Workload
//! definitions live in `benchmarks/workloads/server_sql.md`; curated reports
//! belong in `benchmarks/reports/`.
//!
//! `NODUS_BENCH_SCALE=smoke` runs 1/100-scale workloads (CI sanity floor);
//! unset runs the full fixed datasets. The three-node mTLS variant is
//! `replicated_oltp` (skipped in smoke mode).

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nodus_testkit::TestServer;
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;

const FULL_ROWS: usize = 1_000;

fn rows() -> usize {
    match std::env::var("NODUS_BENCH_SCALE").as_deref() {
        Ok("smoke") => FULL_ROWS / 10,
        _ => FULL_ROWS,
    }
}

fn smoke() -> bool {
    std::env::var("NODUS_BENCH_SCALE").as_deref() == Ok("smoke")
}

/// Operations per measured iteration: full scale is sized so one iteration
/// lands in the seconds range on fsync-bound writes; smoke is a CI sanity
/// floor at 1/100 of the dataset with proportionally fewer ops.
fn ops(smoke_ops: usize, full_ops: usize) -> usize {
    if smoke() { smoke_ops } else { full_ops }
}

async fn server_with_rows(
    n: usize,
) -> (
    TestServer,
    std::sync::Arc<tokio_postgres::Client>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    config.storage.data_dir = Some(dir.path().to_string_lossy().into_owned());
    let server = TestServer::start_with_config(config).await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} user=nodus password=nodus dbname=default",
            server.pgwire_addr.ip(),
            server.pgwire_addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE bench (id INT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    if std::env::var("BENCH_DEBUG").is_ok() {
        eprintln!("server ready");
    }
    if n > 0 {
        preload(&client, n, 1).await;
        if std::env::var("BENCH_DEBUG").is_ok() {
            eprintln!("preload done");
        }
    }
    (server, std::sync::Arc::new(client), dir)
}

/// Bulk load via `COPY ... FROM STDIN` (one transaction per batch, the
/// intended bulk path; per-row INSERTs fsync per transaction).
async fn preload(client: &tokio_postgres::Client, n: usize, from: usize) {
    let mut i = from;
    while i < from + n {
        let end = (i + 100).min(from + n);
        let mut rows = String::new();
        for j in i..end {
            rows.push_str(&format!("{j}\tnote-{j}\n"));
        }
        use futures_util::SinkExt as _;
        let mut sink = Box::pin(
            client
                .copy_in::<_, bytes::Bytes>("COPY bench (id, note) FROM STDIN")
                .await
                .unwrap(),
        );
        sink.as_mut().send(bytes::Bytes::from(rows)).await.unwrap();
        sink.as_mut().finish().await.unwrap();
        i = end;
    }
}

fn bench_point_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_server, client, _dir) = rt.block_on(server_with_rows(0));
    let n = ops(20, 50);
    let mut group = c.benchmark_group("server/point_insert");
    group.throughput(Throughput::Elements(n as u64));
    let mut base = 1_000_000i32;
    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter_custom(|iters| {
            let client = client.clone();
            let start_at = base;
            base += (iters as usize * n) as i32;
            rt.block_on(async {
                let start = Instant::now();
                for i in 0..iters as usize * n {
                    client
                        .execute(
                            "INSERT INTO bench VALUES ($1, 'x')",
                            &[&(start_at + i as i32)],
                        )
                        .await
                        .unwrap();
                }
                start.elapsed()
            })
        })
    });
    group.finish();
    rt.block_on(_server.shutdown());
}

fn bench_point_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_server, client, _dir) = rt.block_on(server_with_rows(rows()));
    let n = ops(200, 1_000);
    let mut group = c.benchmark_group("server/point_read");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter_custom(|iters| {
            let client = client.clone();
            rt.block_on(async {
                let start = Instant::now();
                for i in 0..iters as usize * n {
                    let id = (i % rows()) as i32 + 1;
                    client
                        .query_one("SELECT note FROM bench WHERE id = $1", &[&id])
                        .await
                        .unwrap();
                }
                start.elapsed()
            })
        })
    });
    group.finish();
    rt.block_on(_server.shutdown());
}

fn bench_mixed_oltp(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_server, client, _dir) = rt.block_on(server_with_rows(rows()));
    let n = ops(100, 500);
    let mut group = c.benchmark_group("server/mixed_oltp");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter_custom(|iters| {
            let client = client.clone();
            rt.block_on(async {
                let start = Instant::now();
                let mut tasks = Vec::new();
                for worker in 0..8 {
                    let client = client.clone();
                    tasks.push(tokio::spawn(async move {
                        for i in 0..(iters as usize * n) / 8 {
                            let id = ((worker + i) % rows()) as i32 + 1;
                            if i % 5 == 0 {
                                // 20% writes
                                client
                                    .execute(
                                        "INSERT INTO bench VALUES ($1, 'w') ON CONFLICT (id) DO NOTHING",
                                        &[&(i as i32 + 2_000_000)],
                                    )
                                    .await
                                    .unwrap();
                            } else {
                                client
                                    .query_one("SELECT note FROM bench WHERE id = $1", &[&id])
                                    .await
                                    .unwrap();
                            }
                        }
                    }));
                }
                for task in tasks {
                    task.await.unwrap();
                }
                start.elapsed()
            })
        })
    });
    group.finish();
    rt.block_on(_server.shutdown());
}

fn bench_bulk_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_server, client, _dir) = rt.block_on(server_with_rows(0));
    let n = ops(200, 10_000);
    let mut group = c.benchmark_group("server/bulk_insert");
    group.throughput(Throughput::Elements(n as u64));
    let mut offset = 4_000_000usize;
    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter_custom(|iters| {
            let client = client.clone();
            let base = offset;
            offset += iters as usize * n;
            if std::env::var("BENCH_DEBUG").is_ok() {
                eprintln!("iter_custom: iters={iters} base={base}");
            }
            rt.block_on(async move {
                let start = Instant::now();
                preload(&client, iters as usize * n, base).await;
                start.elapsed()
            })
        })
    });
    group.finish();
    rt.block_on(_server.shutdown());
}

fn bench_range_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_server, client, _dir) = rt.block_on(server_with_rows(rows()));
    let n = rows();
    let mut group = c.benchmark_group("server/range_scan");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter_custom(|iters| {
            let client = client.clone();
            rt.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let rows = client
                        .query("SELECT id FROM bench ORDER BY id", &[])
                        .await
                        .unwrap();
                    std::hint::black_box(rows);
                }
                start.elapsed()
            })
        })
    });
    group.finish();
    rt.block_on(_server.shutdown());
}

/// Three-node mTLS cluster: mixed OLTP against the seed with reads sampled
/// from every member. Skipped in smoke mode (cluster formation dominates).
fn bench_replicated_oltp(c: &mut Criterion) {
    if smoke() {
        return;
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (cluster, clients, _certs_dir) = rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let certs = nodus_testkit::write_test_certs(dir.path());
        let cluster = nodus_testkit::cluster::ClusterFixture::start_cluster_mtls(3, &certs, |_| {})
            .await
            .unwrap();
        let seed = connect_node(&cluster, 0).await;
        seed.batch_execute("CREATE TABLE bench (id INT PRIMARY KEY, note TEXT)")
            .await
            .unwrap();
        preload(&seed, rows(), 1).await;
        let mut clients = Vec::new();
        for idx in 0..3 {
            clients.push(connect_node(&cluster, idx).await);
        }
        (cluster, clients, dir)
    });
    let _keep = &cluster;
    let n = ops(0, 200);
    let mut group = c.benchmark_group("server/replicated_oltp");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter_custom(|iters| {
            let clients = clients.clone();
            rt.block_on(async {
                let start = Instant::now();
                let mut tasks = Vec::new();
                for worker in 0..8 {
                    let clients = clients.clone();
                    tasks.push(tokio::spawn(async move {
                        for i in 0..(iters as usize * n) / 8 {
                            let id = ((worker + i) % rows()) as i32 + 1;
                            let client = &clients[(worker + i) % 3];
                            if i % 5 == 0 {
                                client
                                    .execute(
                                        "INSERT INTO bench VALUES ($1, 'w') ON CONFLICT (id) DO NOTHING",
                                        &[&(i as i32 + 3_000_000)],
                                    )
                                    .await
                                    .unwrap();
                            } else {
                                client
                                    .query_one("SELECT note FROM bench WHERE id = $1", &[&id])
                                    .await
                                    .unwrap();
                            }
                        }
                    }));
                }
                for task in tasks {
                    task.await.unwrap();
                }
                start.elapsed()
            })
        })
    });
    group.finish();
}

async fn connect_node(
    cluster: &nodus_testkit::cluster::ClusterFixture,
    idx: usize,
) -> std::sync::Arc<tokio_postgres::Client> {
    let addr = cluster.nodes[idx].pgwire_addr();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} user=nodus password=nodus dbname=default",
            addr.ip(),
            addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    std::sync::Arc::new(client)
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(2));
    targets = bench_point_insert, bench_point_read, bench_mixed_oltp, bench_bulk_insert, bench_range_scan, bench_replicated_oltp
}
criterion_main!(benches);
