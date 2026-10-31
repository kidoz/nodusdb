use anyhow::{Context, Result, bail};
use nodus_testkit::benchmark::{
    LatencyRecorder, MetricSamples, ProcessResourceMonitor, scrape_metrics,
};
use nodus_testkit::cluster::ClusterFixture;
use nodus_testkit::soak::{
    ResourceAssessment, SOAK_SCHEMA_VERSION, SoakArtifact, SoakNodeSample, SoakSummary,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const TOTAL_INSERTS: i32 = 6_000;
const VALUE_BYTES: usize = 256;
const SNAPSHOT_RETAINED_LIMIT: f64 = 1_000.0;
const QUIET_WINDOW: Duration = Duration::from_secs(10);

struct Workload {
    next_id: AtomicI32,
    acknowledged: Mutex<BTreeSet<i32>>,
    write_attempts: AtomicU64,
    write_retries: AtomicU64,
    recovered_uncertain_writes: AtomicU64,
    reads: AtomicU64,
    read_retries: AtomicU64,
    stop_reads: AtomicBool,
    pause_writes: AtomicBool,
    write_errors: Mutex<Vec<String>>,
    read_errors: Mutex<Vec<String>>,
    write_latency: LatencyRecorder,
    read_latency: LatencyRecorder,
}

impl Workload {
    fn new() -> Self {
        Self {
            next_id: AtomicI32::new(1),
            acknowledged: Mutex::new(BTreeSet::new()),
            write_attempts: AtomicU64::new(0),
            write_retries: AtomicU64::new(0),
            recovered_uncertain_writes: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            read_retries: AtomicU64::new(0),
            stop_reads: AtomicBool::new(false),
            pause_writes: AtomicBool::new(false),
            write_errors: Mutex::new(Vec::new()),
            read_errors: Mutex::new(Vec::new()),
            write_latency: LatencyRecorder::default(),
            read_latency: LatencyRecorder::default(),
        }
    }

    fn acknowledged_count(&self) -> u64 {
        self.acknowledged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len() as u64
    }

    fn expected(&self) -> BTreeSet<i32> {
        self.acknowledged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn record_error(&self, error: &str, write: bool) {
        let errors = if write {
            &self.write_errors
        } else {
            &self.read_errors
        };
        let mut errors = errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if errors.len() < 20 {
            errors.push(error.to_string());
        }
    }

    fn summary(&self) -> Value {
        json!({
            "target_insert_count": TOTAL_INSERTS,
            "acknowledged_insert_count": self.acknowledged_count(),
            "write_attempts": self.write_attempts.load(Ordering::Relaxed),
            "write_retries": self.write_retries.load(Ordering::Relaxed),
            "recovered_uncertain_writes": self.recovered_uncertain_writes.load(Ordering::Relaxed),
            "successful_reads": self.reads.load(Ordering::Relaxed),
            "read_retries": self.read_retries.load(Ordering::Relaxed),
            "write_latency": self.write_latency.summary(),
            "read_latency": self.read_latency.summary(),
            "captured_write_errors": self.write_errors.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
            "captured_read_errors": self.read_errors.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
        })
    }
}

struct ReportParts {
    workload: Value,
    workload_handle: Option<Arc<Workload>>,
    validation: Value,
}

impl Default for ReportParts {
    fn default() -> Self {
        Self {
            workload: Value::Null,
            workload_handle: None,
            validation: Value::Null,
        }
    }
}

struct Sampler {
    stop: tokio::sync::watch::Sender<()>,
    task: Option<JoinHandle<Result<()>>>,
}

impl Sampler {
    async fn finish(mut self) -> Result<()> {
        let _ = self.stop.send(());
        self.task
            .take()
            .context("resource sampler already finished")?
            .await
            .context("resource sampler panicked")?
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn value_for(id: i32) -> String {
    format!("value-{id}-{}", "x".repeat(VALUE_BYTES))
}

fn pg_error(error: tokio_postgres::Error) -> String {
    error.as_db_error().map_or_else(
        || error.to_string(),
        |db| format!("{} (SQLSTATE {})", db.message(), db.code().code()),
    )
}

async fn wait_for_acknowledged(workload: &Workload, target: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(1_800);
    while workload.acknowledged_count() < target {
        if Instant::now() >= deadline {
            bail!(
                "workload reached {} of {target} required acknowledged writes",
                workload.acknowledged_count()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

async fn writer(client: Arc<tokio_postgres::Client>, workload: Arc<Workload>) -> Result<()> {
    loop {
        while workload.pause_writes.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let id = workload.next_id.fetch_add(1, Ordering::Relaxed);
        if id > TOTAL_INSERTS {
            return Ok(());
        }
        let value = value_for(id);
        let retry_deadline = Instant::now() + Duration::from_secs(120);
        loop {
            workload.write_attempts.fetch_add(1, Ordering::Relaxed);
            let started = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                client.execute(
                    "INSERT INTO mixed_activity_soak (id, value) VALUES ($1, $2)",
                    &[&id, &value],
                ),
            )
            .await
            .map_err(|_| "pgwire insert timed out after 30 seconds".to_string())
            .and_then(|result| result.map_err(pg_error));
            workload.write_latency.record(started.elapsed());
            match result {
                Ok(1) => {
                    workload
                        .acknowledged
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(id);
                    break;
                }
                Ok(count) => bail!("insert for id {id} affected {count} rows"),
                Err(error) => {
                    workload.record_error(&error, true);
                    workload.write_retries.fetch_add(1, Ordering::Relaxed);
                    if error.contains("SQLSTATE 23505") {
                        let confirmation = tokio::time::timeout(
                            Duration::from_secs(30),
                            client.query_one(
                                "SELECT value FROM mixed_activity_soak WHERE id = $1",
                                &[&id],
                            ),
                        )
                        .await;
                        match confirmation {
                            Ok(Ok(row)) => {
                                let existing: String = row.get(0);
                                if existing != value {
                                    bail!(
                                        "duplicate key {id} contains a different value; refusing to adopt it"
                                    );
                                }
                                workload
                                    .acknowledged
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .insert(id);
                                workload
                                    .recovered_uncertain_writes
                                    .fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            Ok(Err(error)) => {
                                workload.record_error(&pg_error(error), true);
                            }
                            Err(_) => {
                                workload.record_error(
                                    "uncertain-write confirmation timed out after 30 seconds",
                                    true,
                                );
                            }
                        }
                    }
                    if Instant::now() >= retry_deadline {
                        return Err(anyhow::anyhow!(error))
                            .with_context(|| format!("inserting id {id}"));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

async fn reader(client: Arc<tokio_postgres::Client>, workload: Arc<Workload>) -> Result<()> {
    let mut retry_started = None;
    while !workload.stop_reads.load(Ordering::Relaxed) {
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            client.query_one("SELECT value FROM mixed_activity_soak WHERE id = 1", &[]),
        )
        .await
        .map_err(|_| "pgwire read timed out after 30 seconds".to_string())
        .and_then(|result| result.map_err(pg_error));
        workload.read_latency.record(started.elapsed());
        match result {
            Ok(row) => {
                let value: String = row.get(0);
                if value != value_for(1) {
                    bail!("point read returned an unexpected value");
                }
                retry_started = None;
                workload.reads.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                workload.record_error(&error, false);
                workload.read_retries.fetch_add(1, Ordering::Relaxed);
                if workload.acknowledged_count() > 0 {
                    let retry_started = retry_started.get_or_insert_with(Instant::now);
                    if retry_started.elapsed() >= Duration::from_secs(120) {
                        return Err(anyhow::anyhow!(error)).context("point read did not recover");
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

async fn scrape_nodes(http: &reqwest::Client, addresses: &[SocketAddr]) -> Vec<SoakNodeSample> {
    let mut samples = Vec::with_capacity(addresses.len());
    for (index, address) in addresses.iter().enumerate() {
        samples.push(match scrape_metrics(http, *address).await {
            Ok(metrics) => SoakNodeSample {
                node_id: index as u64 + 1,
                metrics: Some(metrics),
                error: None,
            },
            Err(error) => SoakNodeSample {
                node_id: index as u64 + 1,
                metrics: None,
                error: Some(error.to_string()),
            },
        });
    }
    samples
}

fn start_sampler(
    artifact: Arc<SoakArtifact>,
    addresses: Vec<SocketAddr>,
    phase: Arc<Mutex<String>>,
) -> Sampler {
    let (stop, mut stopped) = tokio::sync::watch::channel(());
    let task = tokio::spawn(async move {
        let http = reqwest::Client::new();
        loop {
            let current_phase = phase
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            artifact.sample(
                scrape_nodes(&http, &addresses).await,
                json!({"phase": current_phase}),
            )?;
            tokio::select! {
                _ = stopped.changed() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    });
    Sampler {
        stop,
        task: Some(task),
    }
}

fn set_phase(phase: &Mutex<String>, value: &str) {
    *phase
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = value.into();
}

fn metric(samples: &MetricSamples, name: &str) -> Option<f64> {
    samples.get(name).copied()
}

fn snapshot_builds(samples: &MetricSamples, group: &str) -> f64 {
    metric(
        samples,
        &format!("nodus_raft_snapshot_builds_total_total{{group=\"{group}\"}}"),
    )
    .unwrap_or(0.0)
}

fn retained_logs(samples: &MetricSamples, group: &str) -> Option<f64> {
    metric(
        samples,
        &format!("nodus_raft_log_entries{{group=\"{group}\"}}"),
    )
}

async fn metrics(cluster: &ClusterFixture, index: usize) -> Result<MetricSamples> {
    let http = reqwest::Client::new();
    let address = cluster.nodes[index]
        .http_addr()
        .parse::<SocketAddr>()
        .context("invalid fixture HTTP address")?;
    scrape_metrics(&http, address)
        .await
        .map_err(|error| anyhow::anyhow!(error))
}

async fn data_group_applied(cluster: &ClusterFixture, index: usize, group: &str) -> Result<u64> {
    let groups = cluster.admin_get(index, "/api/v1/cluster/groups").await?;
    groups["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|candidate| candidate["id"] == group)
        .and_then(|candidate| candidate["applied"].as_u64())
        .with_context(|| format!("node {} omitted applied index for {group}", index + 1))
}

async fn wait_for_data_group_catchup(cluster: &ClusterFixture, group: &str) -> Result<u64> {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let leader = data_group_applied(cluster, 0, group).await?;
        let mut applied = Vec::with_capacity(cluster.nodes.len());
        for index in 0..cluster.nodes.len() {
            applied.push(data_group_applied(cluster, index, group).await?);
        }
        if applied.iter().all(|value| *value == leader) {
            return Ok(leader);
        }
        if Instant::now() >= deadline {
            bail!("data group {group} did not converge: leader={leader}, applied={applied:?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_data_group_quorum(cluster: &ClusterFixture, group: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut observed = Vec::new();
    loop {
        observed.clear();
        let mut ready = true;
        for index in 0..cluster.nodes.len() {
            let groups = cluster.admin_get(index, "/api/v1/cluster/groups").await?;
            let candidate = groups["groups"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|candidate| candidate["id"] == group);
            ready &= if index == 0 {
                candidate
                    .and_then(|candidate| candidate["voters"].as_array())
                    .is_some_and(|voters| voters.len() == 3)
            } else {
                candidate.is_some()
            };
            observed.push(json!({"node_id": index + 1, "groups": groups}));
        }
        if ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "data group {group} did not form its primary quorum and every replica: {observed:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_compaction(cluster: &ClusterFixture) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let samples = metrics(cluster, 0).await?;
        let compactions: f64 = samples
            .iter()
            .filter(|(name, _)| {
                name.starts_with("nodus_lsm_compactions_total_total")
                    && name.contains("result=\"success\"")
            })
            .map(|(_, value)| *value)
            .sum();
        if compactions >= 1.0 {
            return Ok(json!({"successful_compactions": compactions}));
        }
        if Instant::now() >= deadline {
            bail!("no real LSM compaction observed");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_log_growth(cluster: &ClusterFixture, group: &str) -> Result<f64> {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let samples = metrics(cluster, 0).await?;
        if let Some(retained) = retained_logs(&samples, group)
            && retained > 1_500.0
        {
            return Ok(retained);
        }
        if Instant::now() >= deadline {
            bail!("Raft log did not grow beyond 1500 retained entries");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_snapshot_and_purge(
    cluster: &ClusterFixture,
    group: &str,
    previous_builds: f64,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(600);
    let mut peak_retained = 0.0_f64;
    loop {
        let samples = metrics(cluster, 0).await?;
        let builds = snapshot_builds(&samples, group);
        let retained = retained_logs(&samples, group);
        peak_retained = peak_retained.max(retained.unwrap_or(0.0));
        if builds > previous_builds && retained.is_some_and(|entries| entries < peak_retained) {
            return Ok(json!({
                "snapshot_builds": builds,
                "peak_retained_log_entries": peak_retained,
                "retained_log_entries": retained,
                "reclaimed_log_entries": peak_retained - retained.unwrap_or(peak_retained),
                "production_retained_limit_at_purge": SNAPSHOT_RETAINED_LIMIT,
            }));
        }
        if Instant::now() >= deadline {
            bail!(
                "snapshot/purge did not complete: builds={builds}, previous={previous_builds}, peak={peak_retained}, retained={retained:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn finalize(cluster: &ClusterFixture, target: &str) -> Result<()> {
    for path in [
        format!("/api/v1/upgrade/start?target={target}"),
        "/api/v1/upgrade/finalize".to_string(),
    ] {
        if path.contains("finalize") {
            for node in 1..=3 {
                let body = cluster
                    .admin_post(0, &format!("/api/v1/upgrade/node-upgraded?node={node}"))
                    .await?;
                if !body["error"].is_null() {
                    bail!("node-upgraded {target}: {body}");
                }
            }
        }
        let body = cluster.admin_post(0, &path).await?;
        if !body["error"].is_null() {
            bail!("upgrade {target} {path}: {body}");
        }
    }
    Ok(())
}

async fn record_activity(
    artifact: &SoakArtifact,
    activities: &mut Vec<Value>,
    name: &str,
    started: Instant,
    detail: Value,
) -> Result<()> {
    let elapsed = started.elapsed().as_secs_f64();
    artifact.activity(name, true, detail.clone())?;
    activities.push(json!({
        "name": name,
        "elapsed_seconds": elapsed,
        "success": true,
        "detail": detail,
    }));
    Ok(())
}

async fn validate_on_every_node(
    cluster: &ClusterFixture,
    expected: &BTreeSet<i32>,
    split_map: &Value,
) -> Result<Value> {
    cluster
        .pg_client(0)
        .await?
        .batch_execute("CREATE INDEX mixed_activity_value_idx ON mixed_activity_soak (value)")
        .await?;
    let groups = split_map["shards"]
        .as_array()
        .context("split map omitted shards")?
        .iter()
        .map(|shard| {
            shard["id"]
                .as_str()
                .map(|id| format!("shard-{id}"))
                .context("split map omitted shard id")
        })
        .collect::<Result<Vec<_>>>()?;
    let mut applied = Vec::with_capacity(groups.len());
    for group in groups {
        applied.push(json!({
            "group": group,
            "applied_on_every_node": wait_for_data_group_catchup(cluster, &group).await?,
        }));
    }
    let mut nodes = Vec::new();
    for index in 0..cluster.nodes.len() {
        let client = cluster.pg_client(index).await?;
        let rows = client
            .query("SELECT id, value FROM mixed_activity_soak ORDER BY id", &[])
            .await
            .map_err(|error| anyhow::anyhow!(pg_error(error)))?;
        let mut seen = BTreeSet::new();
        let mut duplicates = 0_u64;
        let mut unexpected = 0_u64;
        let mut mismatched_values = 0_u64;
        for row in &rows {
            let id: i32 = row.get(0);
            let value: String = row.get(1);
            if !seen.insert(id) {
                duplicates += 1;
            }
            if !expected.contains(&id) {
                unexpected += 1;
            }
            if value != value_for(id) {
                mismatched_values += 1;
            }
        }
        let missing = expected.difference(&seen).count() as u64;
        let indexed_id = expected
            .iter()
            .nth(expected.len() / 2)
            .copied()
            .context("no acknowledged rows to validate")?;
        let indexed = client
            .query_one(
                "SELECT id FROM mixed_activity_soak WHERE value = $1",
                &[&value_for(indexed_id)],
            )
            .await?;
        let indexed_ok = indexed.get::<_, i32>(0) == indexed_id;
        let passed = missing == 0
            && unexpected == 0
            && duplicates == 0
            && mismatched_values == 0
            && rows.len() == expected.len()
            && indexed_ok;
        if !passed {
            bail!(
                "node {} failed exact validation: missing={missing}, unexpected={unexpected}, duplicates={duplicates}, mismatched={mismatched_values}, rows={}, expected={}, indexed={indexed_ok}",
                index + 1,
                rows.len(),
                expected.len(),
            );
        }
        nodes.push(json!({
            "node_id": index + 1,
            "rows": rows.len(),
            "missing": missing,
            "unexpected": unexpected,
            "duplicates": duplicates,
            "mismatched_values": mismatched_values,
            "indexed_lookup_verified": indexed_ok,
            "linearizable_reads": false,
        }));
    }
    Ok(json!({
        "exact_acknowledged_multiset_on_every_node": true,
        "expected_rows": expected.len(),
        "nodes": nodes,
        "applied_index_convergence": applied,
        "validation_method": "quiescent MVCC reads after per-group applied-index convergence",
        "shared_cross_shard_snapshot": false,
        "restart_kind": "graceful-in-process",
        "crash_or_power_loss_verified": false,
    }))
}

async fn wait_for_quiescent_queues(cluster: &ClusterFixture) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut depth = 0.0;
        for index in 0..cluster.nodes.len() {
            let samples = metrics(cluster, index).await?;
            depth += metric(&samples, "nodus_raft_router_queue_depth").unwrap_or(0.0);
            depth += metric(&samples, "nodus_raft_commit_queue_depth").unwrap_or(0.0);
        }
        if depth == 0.0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("Raft queues did not drain to zero");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_pending_intents_to_drain(cluster: &ClusterFixture) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut pending = 0.0;
        for index in 0..cluster.nodes.len() {
            let samples = metrics(cluster, index).await?;
            pending += metric(&samples, "nodus_pending_intents").unwrap_or(0.0);
        }
        if pending == 0.0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("pending intents did not drain before migration: {pending}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn execute(
    artifact: Arc<SoakArtifact>,
    activities: &mut Vec<Value>,
    parts: &mut ReportParts,
) -> Result<()> {
    let repository = tempfile::tempdir()?;
    let certificates = tempfile::tempdir()?;
    let certs = nodus_testkit::write_test_certs(certificates.path());
    let repository_uri = format!("file://{}", repository.path().to_string_lossy());
    let mut cluster = ClusterFixture::start_cluster_mtls(3, &certs, {
        let repository_uri = repository_uri.clone();
        move |config| {
            config.backup.repository_uri = repository_uri.clone();
            config.cluster.raft_heartbeat_ms = 200;
            config.cluster.raft_election_timeout_min_ms = 600;
            config.cluster.raft_election_timeout_max_ms = 1200;
        }
    })
    .await?;
    finalize(&cluster, "snapshot-v2").await?;
    finalize(&cluster, "migration-v3").await?;
    let seed = Arc::new(cluster.pg_client(0).await?);
    seed.batch_execute("CREATE TABLE mixed_activity_soak (id INT PRIMARY KEY, value TEXT)")
        .await?;
    let table = cluster
        .admin_get(0, "/api/v1/catalog/table?name=mixed_activity_soak")
        .await?["id"]
        .as_str()
        .context("catalog response omitted table id")?
        .to_owned();
    let body = cluster
        .admin_post(0, &format!("/api/v1/shards/{table}/init"))
        .await?;
    if !body["error"].is_null() {
        bail!("shard init failed: {body}");
    }
    let group_deadline = Instant::now() + Duration::from_secs(60);
    let group = loop {
        let map = cluster
            .admin_get(0, &format!("/api/v1/shards/{table}"))
            .await?;
        if let Some(shard_id) = map["shards"][0]["id"].as_str() {
            break format!("shard-{shard_id}");
        }
        if Instant::now() >= group_deadline {
            bail!("initial shard map was not published: {map}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    wait_for_data_group_quorum(&cluster, &group).await?;

    let phase = Arc::new(Mutex::new("workload".to_string()));
    let addresses = cluster
        .nodes
        .iter()
        .map(|node| node.http_addr().parse())
        .collect::<std::result::Result<Vec<SocketAddr>, _>>()?;
    let sampler = start_sampler(artifact.clone(), addresses, phase.clone());
    let workload = Arc::new(Workload::new());
    parts.workload_handle = Some(workload.clone());
    let mut clients = Vec::new();
    for _ in 0..5 {
        clients.push(Arc::new(cluster.pg_client(0).await?));
    }
    let mut workers = Vec::new();
    for client in clients.iter().take(4).cloned() {
        workers.push(tokio::spawn(writer(client, workload.clone())));
    }
    workers.push(tokio::spawn(reader(clients[4].clone(), workload.clone())));

    wait_for_acknowledged(&workload, 100).await?;
    set_phase(&phase, "backup");
    let started = Instant::now();
    let backup = cluster.admin_post(0, "/api/v1/backups").await?;
    if backup["status"] != "Completed" {
        bail!("backup did not complete: {backup}");
    }
    let backup_id = backup["backup_id"]
        .as_str()
        .context("backup omitted id")?
        .to_owned();
    let verified = cluster
        .admin_post(0, &format!("/api/v1/backups/{backup_id}/verify"))
        .await?;
    if verified["verified"] != true {
        bail!("backup verification failed: {verified}");
    }
    record_activity(
        &artifact,
        activities,
        "full_backup_verified",
        started,
        json!({"backup_id": backup_id, "acknowledged_at_completion": workload.acknowledged_count()}),
    )
    .await?;

    set_phase(&phase, "compaction");
    let started = Instant::now();
    let compaction = wait_for_compaction(&cluster).await?;
    record_activity(
        &artifact,
        activities,
        "automatic_lsm_compaction",
        started,
        compaction,
    )
    .await?;

    set_phase(&phase, "first_snapshot");
    let initial_builds = snapshot_builds(&metrics(&cluster, 0).await?, &group);
    let grown = wait_for_log_growth(&cluster, &group).await?;
    let started = Instant::now();
    let first = wait_for_snapshot_and_purge(&cluster, &group, initial_builds).await?;
    record_activity(
        &artifact,
        activities,
        "first_snapshot_and_log_reclamation",
        started,
        json!({"retained_before": grown, "observation": first}),
    )
    .await?;

    set_phase(&phase, "follower_down");
    let stopped_at = workload.acknowledged_count();
    let follower_applied = data_group_applied(&cluster, 2, &group).await?;
    let started = Instant::now();
    cluster.stop(2).await?;
    record_activity(
        &artifact,
        activities,
        "graceful_follower_stop",
        started,
        json!({"node_id": 3, "acknowledged_writes": stopped_at, "follower_applied": follower_applied}),
    )
    .await?;

    set_phase(&phase, "snapshot_required_catchup");
    let first_builds = first["snapshot_builds"]
        .as_f64()
        .context("first snapshot observation omitted builds")?;
    wait_for_log_growth(&cluster, &group).await?;
    let started = Instant::now();
    let second = wait_for_snapshot_and_purge(&cluster, &group, first_builds).await?;
    let leader_applied = data_group_applied(&cluster, 0, &group).await?;
    let missed_log_entries = leader_applied
        .checked_sub(follower_applied)
        .context("leader applied index regressed below stopped follower")?;
    let missed_writes = workload.acknowledged_count() - stopped_at;
    let second_retained = second["retained_log_entries"]
        .as_f64()
        .context("second snapshot observation omitted retained entries")?;
    if missed_log_entries as f64 <= second_retained {
        bail!(
            "follower lag does not require snapshot catch-up: missed={missed_log_entries}, retained={second_retained}"
        );
    }
    record_activity(
        &artifact,
        activities,
        "second_snapshot_and_log_reclamation_while_follower_down",
        started,
        json!({"missed_acknowledged_writes": missed_writes, "follower_applied": follower_applied, "leader_applied": leader_applied, "missed_log_entries": missed_log_entries, "observation": second}),
    )
    .await?;

    let started = Instant::now();
    cluster.restart(2).await?;
    if !cluster.wait_ready(2, Duration::from_secs(60)).await
        || !cluster.wait_nodes_live(3, Duration::from_secs(60)).await
    {
        bail!("restarted follower did not rejoin");
    }
    record_activity(
        &artifact,
        activities,
        "snapshot_required_follower_restart",
        started,
        json!({"node_id": 3, "missed_log_entries_before_restart": missed_log_entries, "leader_retained_entries": second_retained}),
    )
    .await?;

    set_phase(&phase, "migration_write_pause");
    workload.pause_writes.store(true, Ordering::Relaxed);
    wait_for_quiescent_queues(&cluster).await?;
    wait_for_pending_intents_to_drain(&cluster).await?;
    let catchup_started = Instant::now();
    let caught_up_applied = wait_for_data_group_catchup(&cluster, &group).await?;
    record_activity(
        &artifact,
        activities,
        "snapshot_catchup_verified",
        catchup_started,
        json!({"node_id": 3, "applied_on_every_node": caught_up_applied}),
    )
    .await?;
    let started = Instant::now();
    let map = cluster
        .admin_get(0, &format!("/api/v1/shards/{table}"))
        .await?;
    let source = map["shards"][0]["id"]
        .as_str()
        .context("shard map omitted source id")?;
    let split = cluster
        .admin_post(
            0,
            &format!("/api/v1/shards/{table}/split?shard={source}&key={table}:3000"),
        )
        .await?;
    if !split["error"].is_null()
        && split["note"] != "journaled; the meta leader's discovery worker resumes it"
    {
        bail!("split failed: {split}");
    }
    let deadline = Instant::now() + Duration::from_secs(300);
    let split_map = loop {
        let map = cluster
            .admin_get(0, &format!("/api/v1/shards/{table}"))
            .await?;
        if map["shards"]
            .as_array()
            .is_some_and(|shards| shards.len() == 2)
        {
            break map;
        }
        if Instant::now() >= deadline {
            bail!("split did not publish two shards: initial_response={split}, final_map={map}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let rebalance = cluster
        .admin_post(0, &format!("/api/v1/shards/{table}/rebalance?nodes=1,2,3"))
        .await?;
    if rebalance["rebalanced"] != true {
        bail!("rebalance failed: {rebalance}");
    }
    workload.pause_writes.store(false, Ordering::Relaxed);
    set_phase(&phase, "workload");
    record_activity(
        &artifact,
        activities,
        "split_and_rebalance_with_read_load",
        started,
        json!({"acknowledged_at_completion": workload.acknowledged_count(), "shards": split_map["shards"].as_array().map_or(0, Vec::len), "rebalanced": true, "split_response": split, "write_pause": true, "reads_continued": true, "online_write_availability_verified": false}),
    )
    .await?;

    wait_for_acknowledged(&workload, TOTAL_INSERTS as u64).await?;
    workload.stop_reads.store(true, Ordering::Relaxed);
    for worker in workers {
        worker.await.context("workload worker panicked")??;
    }
    parts.workload = workload.summary();
    set_phase(&phase, "quiescent");
    wait_for_quiescent_queues(&cluster).await?;
    parts.validation = validate_on_every_node(&cluster, &workload.expected(), &split_map).await?;
    record_activity(
        &artifact,
        activities,
        "exact_recovery_validation",
        Instant::now(),
        parts.validation.clone(),
    )
    .await?;
    set_phase(&phase, "post_activity_observation");
    tokio::time::sleep(QUIET_WINDOW).await;
    sampler.finish().await?;
    Ok(())
}

fn git_output(args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|stdout| stdout.trim().to_owned())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "bounded PR2 mixed-activity drill; run explicitly"]
#[serial_test::serial(cluster)]
async fn mixed_activity_smoke_drill() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            "nodus_raftstore=error,nodus_raftstore::migration=info,nodus_server=error,nodus_server::migration_coordinator=warn,openraft=error",
        )
        .try_init();
    let artifact = Arc::new(SoakArtifact::from_environment().unwrap());
    let resources = ProcessResourceMonitor::start();
    let mut activities = Vec::new();
    let mut parts = ReportParts::default();
    let result = execute(artifact.clone(), &mut activities, &mut parts).await;
    if let Some(workload) = &parts.workload_handle {
        parts.workload = workload.summary();
    }
    let (rss, allocator) = resources.finish();
    let status = if result.is_ok() { "passed" } else { "failed" };
    let error = result.as_ref().err().map(ToString::to_string);
    if let Err(error) = artifact.write_summary(SoakSummary {
        schema_version: SOAK_SCHEMA_VERSION,
        run_id: String::new(),
        status: status.into(),
        started_at_unix_micros: 0,
        finished_at_unix_micros: 0,
        error,
        source: json!({
            "commit": git_output(&["rev-parse", "HEAD"]),
            "dirty": git_output(&["status", "--porcelain"]).map(|status| !status.is_empty()),
            "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH,
        }),
        configuration: json!({
            "nodes": 3,
            "storage": "persistent-lsm",
            "raft_transport": "mtls",
            "raft_heartbeat_ms": 200,
            "raft_election_timeout_min_ms": 600,
            "raft_election_timeout_max_ms": 1200,
            "write_path": "legacy",
            "total_insert_target": TOTAL_INSERTS,
            "value_bytes_excluding_id_prefix": VALUE_BYTES,
            "snapshot_policy_logs_since_last": 5000,
            "install_snapshot_timeout_ms": 30000,
            "snapshot_max_chunk_bytes": 256 * 1024,
            "max_in_snapshot_log_to_keep": 1000,
            "flush_threshold_bytes": 4 * 1024 * 1024,
            "compaction_trigger_sstables": 4,
            "migration_copy_batch_rows": 256,
            "migration_copy_concurrency": 16,
            "migration_step_deadline_seconds": 120,
            "repository": "filesystem",
            "required_missed_log_entries_for_snapshot_catchup": SNAPSHOT_RETAINED_LIMIT,
        }),
        workload: parts.workload,
        activities: activities.clone(),
        validation: parts.validation,
        resources: json!({"rss": rss, "allocator": allocator}),
        assessment: ResourceAssessment {
            raft_log_reclamation_observed: Some(activities.iter().any(|activity| {
                activity["name"] == "second_snapshot_and_log_reclamation_while_follower_down"
            })),
            ..ResourceAssessment::unassessed(QUIET_WINDOW)
        },
    }) {
        panic!("failed to write soak summary: {error}");
    }
    println!(
        "mixed-activity soak summary: {}",
        artifact.summary_path().display()
    );
    result.unwrap();
    assert_eq!(SOAK_SCHEMA_VERSION, 1);
}
