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

const FULL_ROWS: usize = 10_000;

fn rows() -> usize {
    match std::env::var("NODUS_BENCH_SCALE").as_deref() {
        Ok("smoke") => FULL_ROWS / 100,
        _ => FULL_ROWS,
    }
}

fn smoke() -> bool {
    std::env::var("NODUS_BENCH_SCALE").as_deref() == Ok("smoke")
}

fn write_batch_requested() -> bool {
    std::env::var("NODUS_BENCH_WRITE_BATCH").as_deref() == Ok("1")
}

async fn maybe_enable_write_batching(server: &TestServer) {
    if !write_batch_requested() {
        return;
    }
    let http = reqwest::Client::new();
    let base = format!("http://{}/api/v1/upgrade", server.http_addr);
    for target in ["snapshot-v2", "migration-v3", "write-batching-v4"] {
        for path in [
            format!("start?target={target}"),
            "node-upgraded?node=1".to_string(),
            "finalize".to_string(),
        ] {
            let response = http.post(format!("{base}/{path}")).send().await.unwrap();
            let status = response.status();
            let body = response.text().await.unwrap();
            assert!(
                status.is_success(),
                "benchmark upgrade {path} failed with {status}: {body}"
            );
            let body: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert!(
                body["error"].is_null(),
                "benchmark upgrade {path} failed: {body}"
            );
        }
    }
}

async fn maybe_enable_cluster_write_batching(cluster: &nodus_testkit::cluster::ClusterFixture) {
    if !write_batch_requested() {
        return;
    }
    for target in ["snapshot-v2", "migration-v3", "write-batching-v4"] {
        let body = cluster
            .admin_post(0, &format!("/api/v1/upgrade/start?target={target}"))
            .await
            .unwrap();
        assert!(body["error"].is_null(), "start {target}: {body}");
        for node in 0..cluster.nodes.len() {
            let body = cluster
                .admin_post(
                    0,
                    &format!("/api/v1/upgrade/node-upgraded?node={}", node + 1),
                )
                .await
                .unwrap();
            assert!(body["error"].is_null(), "refresh {target}: {body}");
        }
        let body = cluster
            .admin_post(0, "/api/v1/upgrade/finalize")
            .await
            .unwrap();
        assert!(body["error"].is_null(), "finalize {target}: {body}");
    }
    let state = cluster.admin_get(0, "/api/v1/upgrade").await.unwrap();
    assert_eq!(state["cluster_version"], 4);
    assert_eq!(state["feature_gates"]["write_batching"], true);
}

/// Operations per measured iteration: full scale is sized so one iteration
/// lands in the seconds range on fsync-bound writes; smoke is a CI sanity
/// floor at 1/100 of the dataset with proportionally fewer ops.
fn ops(smoke_ops: usize, full_ops: usize) -> usize {
    if let Ok(value) = std::env::var("NODUS_BENCH_OPS") {
        return value.parse().unwrap();
    }
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
    config.admin.allow_insecure = write_batch_requested();
    config.storage.data_dir = Some(dir.path().to_string_lossy().into_owned());
    let server = TestServer::start_with_config(config).await.unwrap();
    maybe_enable_write_batching(&server).await;
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

fn bench_concurrent_point_inserts(
    c: &mut Criterion,
    name: &str,
    indexed: bool,
    client_counts: &[usize],
) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_server, client, _dir) = rt.block_on(server_with_rows(0));
    if indexed {
        rt.block_on(client.batch_execute("CREATE INDEX bench_note_idx ON bench (note)"))
            .unwrap();
    }
    let max_clients = client_counts.iter().copied().max().unwrap();
    let clients = rt.block_on(async {
        let mut clients = vec![client];
        for _ in 1..max_clients {
            clients.push(connect_addr(_server.pgwire_addr).await);
        }
        clients
    });
    let n = ops(20, 50);
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(n as u64));
    let mut base = 1_000_000i32;
    for &client_count in client_counts {
        group.bench_function(
            BenchmarkId::from_parameter(format!("{client_count}_clients")),
            |b| {
                b.iter_custom(|iters| {
                    let clients = clients.clone();
                    let start_at = base;
                    let total = iters as usize * n;
                    base += total as i32;
                    rt.block_on(async {
                        let start = Instant::now();
                        let mut tasks = Vec::new();
                        for (worker, client) in
                            clients.iter().take(client_count).cloned().enumerate()
                        {
                            let first = total * worker / client_count;
                            let end = total * (worker + 1) / client_count;
                            tasks.push(tokio::spawn(async move {
                                for i in first..end {
                                    client
                                        .execute(
                                            "INSERT INTO bench VALUES ($1, 'x')",
                                            &[&(start_at + i as i32)],
                                        )
                                        .await
                                        .unwrap();
                                }
                            }));
                        }
                        for task in tasks {
                            task.await.unwrap();
                        }
                        start.elapsed()
                    })
                })
            },
        );
    }
    group.finish();
    rt.block_on(_server.shutdown());
}

fn bench_point_insert(c: &mut Criterion) {
    bench_concurrent_point_inserts(c, "server/point_insert", false, &[1, 8, 32]);
}

fn bench_indexed_point_insert(c: &mut Criterion) {
    bench_concurrent_point_inserts(c, "server/indexed_point_insert", true, &[8]);
}

fn bench_point_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_server, client, _dir) = rt.block_on(server_with_rows(rows()));
    if std::env::var("BENCH_DEBUG").is_ok() {
        rt.block_on(async {
            for row in client
                .query("EXPLAIN (ANALYZE) SELECT note FROM bench WHERE id = 1", &[])
                .await
                .unwrap()
            {
                eprintln!("plan: {}", row.get::<usize, String>(0));
            }
            let start = Instant::now();
            client
                .query_one("SELECT note FROM bench WHERE id = 1", &[])
                .await
                .unwrap();
            eprintln!("literal read: {:?}", start.elapsed());
            let start = Instant::now();
            client
                .query_one("SELECT note FROM bench WHERE id = $1", &[&1i32])
                .await
                .unwrap();
            eprintln!("bound read: {:?}", start.elapsed());
        });
    }
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

async fn connect_addr(addr: std::net::SocketAddr) -> std::sync::Arc<tokio_postgres::Client> {
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

fn bench_mixed_oltp(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_server, _client, _dir) = rt.block_on(server_with_rows(rows()));
    let clients = rt.block_on(async {
        let mut clients = Vec::new();
        for _ in 0..8 {
            clients.push(connect_addr(_server.pgwire_addr).await);
        }
        clients
    });
    let next_id = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(2_000_000));
    let n = ops(100, 500);
    let mut group = c.benchmark_group("server/mixed_oltp");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter_custom(|iters| {
            let clients = clients.clone();
            let next_id = next_id.clone();
            rt.block_on(async {
                let start = Instant::now();
                let mut tasks = Vec::new();
                let total = iters as usize * n;
                for (worker, client) in clients.iter().cloned().enumerate() {
                    let next_id = next_id.clone();
                    let first = total * worker / 8;
                    let end = total * (worker + 1) / 8;
                    tasks.push(tokio::spawn(async move {
                        for i in first..end {
                            let debug = std::env::var("BENCH_DEBUG").is_ok();
                            let op_start = Instant::now();
                            if debug {
                                eprintln!("mixed worker={worker} op={i} start");
                            }
                            let id = (i % rows()) as i32 + 1;
                            if i % 5 == 0 {
                                let write_id = next_id
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                                    as i32;
                                client
                                    .execute("INSERT INTO bench VALUES ($1, 'w')", &[&write_id])
                                    .await
                                    .unwrap();
                            } else {
                                client
                                    .query_one("SELECT note FROM bench WHERE id = $1", &[&id])
                                    .await
                                    .unwrap();
                            }
                            if debug {
                                eprintln!(
                                    "mixed worker={worker} op={i} done in {:?}",
                                    op_start.elapsed()
                                );
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

/// Three-node mTLS cluster: eight clients distributed across every member.
/// Skipped in smoke mode (cluster formation dominates).
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
        maybe_enable_cluster_write_batching(&cluster).await;
        let seed = connect_node(&cluster, 0).await;
        seed.batch_execute("CREATE TABLE bench (id INT PRIMARY KEY, note TEXT)")
            .await
            .unwrap();
        preload(&seed, rows(), 1).await;
        let mut clients = Vec::new();
        for idx in 0..8 {
            clients.push(connect_node(&cluster, idx % 3).await);
        }
        (cluster, clients, dir)
    });
    let _keep = &cluster;
    let next_id = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(3_000_000));
    let n = ops(0, 200);
    let mut group = c.benchmark_group("server/replicated_oltp");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter_custom(|iters| {
            let clients = clients.clone();
            let next_id = next_id.clone();
            rt.block_on(async {
                let start = Instant::now();
                let mut tasks = Vec::new();
                let total = iters as usize * n;
                for (worker, client) in clients.iter().cloned().enumerate() {
                    let next_id = next_id.clone();
                    let first = total * worker / 8;
                    let end = total * (worker + 1) / 8;
                    tasks.push(tokio::spawn(async move {
                        for i in first..end {
                            let debug = std::env::var("BENCH_DEBUG").is_ok();
                            let op_start = Instant::now();
                            if debug {
                                eprintln!("mixed worker={worker} op={i} start");
                            }
                            let id = (i % rows()) as i32 + 1;
                            if i % 5 == 0 {
                                let write_id = next_id
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                                    as i32;
                                client
                                    .execute("INSERT INTO bench VALUES ($1, 'w')", &[&write_id])
                                    .await
                                    .unwrap();
                            } else {
                                client
                                    .query_one("SELECT note FROM bench WHERE id = $1", &[&id])
                                    .await
                                    .unwrap();
                            }
                            if debug {
                                eprintln!(
                                    "mixed worker={worker} op={i} done in {:?}",
                                    op_start.elapsed()
                                );
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
    connect_addr(cluster.nodes[idx].pgwire_addr()).await
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(2));
    targets = bench_point_insert, bench_indexed_point_insert, bench_point_read, bench_mixed_oltp, bench_bulk_insert, bench_range_scan, bench_replicated_oltp
}
criterion_main!(benches);
