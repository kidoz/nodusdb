use axum::{Router, http::StatusCode, routing::get};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use std::time::Duration;

pub type MetricLabels = Vec<(String, String)>;
pub type LabeledCounter = Family<MetricLabels, Counter>;
pub type LabeledHistogram = Family<MetricLabels, Histogram, fn() -> Histogram>;

fn latency_histogram() -> Histogram {
    Histogram::new(exponential_buckets(0.00005, 2.0, 16))
}

fn batch_size_histogram() -> Histogram {
    Histogram::new([1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0])
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterOverview {
    pub cluster_status: String,
    pub nodes_live: u32,
    pub nodes_total: u32,
    pub shards_total: u32,
    pub shards_unavailable: u32,
    pub qps: f64,
    pub active_alerts: u32,
}

/// Server metrics registered into the Prometheus registry. The fields are
/// cheap-to-clone handles (atomic-backed); incrementing a handle is reflected
/// in the registry and surfaced on `/metrics`.
#[derive(Clone)]
pub struct Metrics {
    pub queries_total: Counter,
    pub query_errors_total: Counter,
    pub pgwire_connections_total: Counter,
    pub active_sessions: Gauge,
    pub vacuum_reclaimed_total: Counter,
    pub slow_queries_total: Counter,
    pub query_latency_seconds: Histogram,
    /// Cross-shard transactions committed via two-phase commit.
    pub cross_shard_commits_total: Counter,
    /// Cross-shard transactions aborted because a participant could not prepare.
    pub cross_shard_aborts_total: Counter,
    /// Cross-shard commits re-driven by startup recovery.
    pub txn_recoveries_total: Counter,
    /// Reads that performed a linearizable (Raft ReadIndex) barrier before scanning.
    pub linearizable_reads_total: Counter,
    /// Data-shard Raft groups hosted on this node, and how many lack a leader.
    pub shard_groups: Gauge,
    pub shard_groups_unavailable: Gauge,
    /// Pending intents aborted by the abandoned-intent janitor.
    pub janitor_aborts_total: Counter,
    /// Pending intents observed by the janitor's most recent sweep.
    pub pending_intents: Gauge,
    /// Routed Raft commands by fixed command kind and result.
    pub raft_commands_total: LabeledCounter,
    /// Time from router submission until the applied Raft response, by command and result.
    pub raft_dispatch_seconds: LabeledHistogram,
    /// Number of intent operations in each accepted write batch.
    pub intent_batch_operations: Histogram,
    /// Number of transactions in each dispatched commit batch.
    pub commit_batch_size: Histogram,
    /// Time from the first enqueue until each commit batch is dispatched.
    pub commit_batch_wait_seconds: Histogram,
    /// Commit batches bypassed for a fixed fallback reason.
    pub commit_batch_fallbacks_total: LabeledCounter,
    /// End-to-end transaction commit latency by mode and result.
    pub commit_latency_seconds: LabeledHistogram,
    /// Raft log entries retained per group (`group` label) — grows without
    /// bound while a group's snapshot build keeps deferring.
    pub raft_log_entries: Family<Vec<(String, String)>, Gauge>,
    /// Snapshot builds deferred per group (`group` label).
    pub snapshot_deferrals_total: Family<Vec<(String, String)>, Counter>,
    /// Duration (seconds) of each group's current unbroken snapshot-deferral
    /// streak (`group` label); 0 when builds are succeeding.
    pub snapshot_deferred_seconds: Family<Vec<(String, String)>, Gauge>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            queries_total: Counter::default(),
            query_errors_total: Counter::default(),
            pgwire_connections_total: Counter::default(),
            active_sessions: Gauge::default(),
            vacuum_reclaimed_total: Counter::default(),
            slow_queries_total: Counter::default(),
            // ~0.5ms .. ~1s across 12 exponential buckets.
            query_latency_seconds: Histogram::new(exponential_buckets(0.0005, 2.0, 12)),
            cross_shard_commits_total: Counter::default(),
            cross_shard_aborts_total: Counter::default(),
            txn_recoveries_total: Counter::default(),
            linearizable_reads_total: Counter::default(),
            shard_groups: Gauge::default(),
            shard_groups_unavailable: Gauge::default(),
            janitor_aborts_total: Counter::default(),
            pending_intents: Gauge::default(),
            raft_commands_total: Family::default(),
            raft_dispatch_seconds: Family::new_with_constructor(
                latency_histogram as fn() -> Histogram,
            ),
            intent_batch_operations: batch_size_histogram(),
            commit_batch_size: batch_size_histogram(),
            commit_batch_wait_seconds: latency_histogram(),
            commit_batch_fallbacks_total: Family::default(),
            commit_latency_seconds: Family::new_with_constructor(
                latency_histogram as fn() -> Histogram,
            ),
            raft_log_entries: Family::default(),
            snapshot_deferrals_total: Family::default(),
            snapshot_deferred_seconds: Family::default(),
        }
    }
}

impl Metrics {
    pub fn register(registry: &mut Registry) -> Self {
        let m = Metrics::default();
        registry.register(
            "nodus_queries_total",
            "Total SQL queries processed",
            m.queries_total.clone(),
        );
        registry.register(
            "nodus_query_errors_total",
            "Total SQL queries that returned an error",
            m.query_errors_total.clone(),
        );
        registry.register(
            "nodus_pgwire_connections_total",
            "Total accepted pgwire connections",
            m.pgwire_connections_total.clone(),
        );
        registry.register(
            "nodus_active_sessions",
            "Currently open client sessions",
            m.active_sessions.clone(),
        );
        registry.register(
            "nodus_vacuum_reclaimed_total",
            "Total MVCC versions reclaimed by background GC",
            m.vacuum_reclaimed_total.clone(),
        );
        registry.register(
            "nodus_slow_queries_total",
            "Total queries exceeding the slow-query threshold",
            m.slow_queries_total.clone(),
        );
        registry.register(
            "nodus_query_latency_seconds",
            "Query execution latency",
            m.query_latency_seconds.clone(),
        );
        registry.register(
            "nodus_cross_shard_commits_total",
            "Cross-shard transactions committed via two-phase commit",
            m.cross_shard_commits_total.clone(),
        );
        registry.register(
            "nodus_cross_shard_aborts_total",
            "Cross-shard transactions aborted because a participant could not prepare",
            m.cross_shard_aborts_total.clone(),
        );
        registry.register(
            "nodus_txn_recoveries_total",
            "Cross-shard commits re-driven by startup recovery",
            m.txn_recoveries_total.clone(),
        );
        registry.register(
            "nodus_linearizable_reads_total",
            "Reads that performed a linearizable Raft ReadIndex barrier before scanning",
            m.linearizable_reads_total.clone(),
        );
        registry.register(
            "nodus_shard_groups",
            "Data-shard Raft groups hosted on this node",
            m.shard_groups.clone(),
        );
        registry.register(
            "nodus_shard_groups_unavailable",
            "Hosted data-shard groups currently without a leader",
            m.shard_groups_unavailable.clone(),
        );
        registry.register(
            "nodus_janitor_aborts_total",
            "Pending intents aborted by the abandoned-intent janitor",
            m.janitor_aborts_total.clone(),
        );
        registry.register(
            "nodus_pending_intents",
            "Pending intents observed by the janitor's most recent sweep",
            m.pending_intents.clone(),
        );
        registry.register(
            "nodus_raft_commands_total",
            "Routed Raft commands by command kind and result",
            m.raft_commands_total.clone(),
        );
        registry.register(
            "nodus_raft_dispatch_seconds",
            "Time from router submission until the applied Raft response",
            m.raft_dispatch_seconds.clone(),
        );
        registry.register(
            "nodus_intent_batch_operations",
            "Number of intent operations in each accepted write batch",
            m.intent_batch_operations.clone(),
        );
        registry.register(
            "nodus_commit_batch_size",
            "Number of transactions in each dispatched commit batch",
            m.commit_batch_size.clone(),
        );
        registry.register(
            "nodus_commit_batch_wait_seconds",
            "Time from the first enqueue until each commit batch is dispatched",
            m.commit_batch_wait_seconds.clone(),
        );
        registry.register(
            "nodus_commit_batch_fallbacks_total",
            "Commit batches bypassed for a fixed fallback reason",
            m.commit_batch_fallbacks_total.clone(),
        );
        registry.register(
            "nodus_commit_latency_seconds",
            "End-to-end transaction commit latency by mode and result",
            m.commit_latency_seconds.clone(),
        );
        registry.register(
            "nodus_raft_log_entries",
            "Raft log entries retained per group",
            m.raft_log_entries.clone(),
        );
        registry.register(
            "nodus_snapshot_deferrals_total",
            "Snapshot builds deferred per group",
            m.snapshot_deferrals_total.clone(),
        );
        registry.register(
            "nodus_snapshot_deferred_seconds",
            "Duration of each group's current unbroken snapshot-deferral streak",
            m.snapshot_deferred_seconds.clone(),
        );
        m
    }

    pub fn observe_raft_command(&self, command: &str, result: &str, elapsed: Duration) {
        let labels = || {
            vec![
                ("command".to_string(), command.to_string()),
                ("result".to_string(), result.to_string()),
            ]
        };
        self.raft_commands_total.get_or_create(&labels()).inc();
        self.raft_dispatch_seconds
            .get_or_create(&labels())
            .observe(elapsed.as_secs_f64());
    }

    pub fn observe_intent_batch(&self, operations: usize) {
        self.intent_batch_operations.observe(operations as f64);
    }

    pub fn observe_commit_batch(&self, commits: usize, wait: Duration) {
        self.commit_batch_size.observe(commits as f64);
        self.commit_batch_wait_seconds.observe(wait.as_secs_f64());
    }

    pub fn inc_commit_batch_fallback(&self, reason: &str) {
        self.commit_batch_fallbacks_total
            .get_or_create(&vec![("reason".to_string(), reason.to_string())])
            .inc();
    }

    pub fn observe_commit(&self, mode: &str, result: &str, elapsed: Duration) {
        self.commit_latency_seconds
            .get_or_create(&vec![
                ("mode".to_string(), mode.to_string()),
                ("result".to_string(), result.to_string()),
            ])
            .observe(elapsed.as_secs_f64());
    }
}

/// A bounded, in-memory log of recent slow queries for the admin inspector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQuery {
    pub sql: String,
    pub duration_ms: u64,
    pub session_id: String,
}

pub struct SlowQueryLog {
    inner: Mutex<VecDeque<SlowQuery>>,
    capacity: usize,
    threshold_ms: u64,
}

impl SlowQueryLog {
    pub fn new(capacity: usize, threshold_ms: u64) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            capacity,
            threshold_ms,
        }
    }

    pub fn threshold_ms(&self) -> u64 {
        self.threshold_ms
    }

    /// Records a query if it met the slow threshold, evicting the oldest entry
    /// when the buffer is full.
    pub fn record(&self, sql: String, duration_ms: u64, session_id: String) {
        if duration_ms < self.threshold_ms {
            return;
        }
        let mut guard = self.inner.lock().unwrap();
        if guard.len() >= self.capacity {
            guard.pop_front();
        }
        guard.push_back(SlowQuery {
            sql,
            duration_ms,
            session_id,
        });
    }

    pub fn list(&self) -> Vec<SlowQuery> {
        self.inner.lock().unwrap().iter().cloned().collect()
    }
}

/// Live cluster membership and shard health, updated by the control plane and
/// rendered into a [`ClusterOverview`] for the admin API and Web console.
#[derive(Debug)]
pub struct ClusterState {
    pub nodes_live: AtomicU32,
    pub nodes_total: AtomicU32,
    pub shards_total: AtomicU32,
    pub shards_unavailable: AtomicU32,
    pub qps: std::sync::atomic::AtomicU64,
}

impl Default for ClusterState {
    fn default() -> Self {
        // Single-node default until the control plane reports membership.
        Self {
            nodes_live: AtomicU32::new(1),
            nodes_total: AtomicU32::new(1),
            shards_total: AtomicU32::new(0),
            shards_unavailable: AtomicU32::new(0),
            qps: std::sync::atomic::AtomicU64::new(0f64.to_bits()),
        }
    }
}

impl ClusterState {
    pub fn overview(&self, active_alerts: u32) -> ClusterOverview {
        let nodes_live = self.nodes_live.load(Ordering::Relaxed);
        let nodes_total = self.nodes_total.load(Ordering::Relaxed);
        let shards_unavailable = self.shards_unavailable.load(Ordering::Relaxed);
        let qps = f64::from_bits(self.qps.load(Ordering::Relaxed));
        let cluster_status = if shards_unavailable > 0 {
            "Unhealthy"
        } else if nodes_live < nodes_total {
            "Degraded"
        } else {
            "Healthy"
        };
        ClusterOverview {
            cluster_status: cluster_status.into(),
            nodes_live,
            nodes_total,
            shards_total: self.shards_total.load(Ordering::Relaxed),
            shards_unavailable,
            qps,
            active_alerts,
        }
    }
}

pub struct AppState {
    pub is_ready: AtomicBool,
    /// When set, the node is draining and reports not-ready so load balancers
    /// stop sending new traffic. Shared with the admin drain endpoint.
    pub draining: Arc<AtomicBool>,
    pub registry: std::sync::Mutex<Registry>,
    pub metrics: Metrics,
    pub cluster: Arc<ClusterState>,
}

impl Default for AppState {
    fn default() -> Self {
        let mut registry = Registry::default();
        let metrics = Metrics::register(&mut registry);
        Self {
            is_ready: AtomicBool::new(false),
            draining: Arc::new(AtomicBool::new(false)),
            registry: std::sync::Mutex::new(registry),
            metrics,
            cluster: Arc::new(ClusterState::default()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub name: String,
    pub version: String,
}

pub fn monitoring_routes(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/api/v1/version", get(version))
        .route("/api/v1/cluster/overview", get(cluster_overview))
        .with_state(state)
}

async fn version() -> axum::Json<VersionInfo> {
    axum::Json(VersionInfo {
        name: "nodusd".into(),
        version: env!("CARGO_PKG_VERSION").into(),
    })
}

async fn cluster_overview(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> axum::Json<ClusterOverview> {
    axum::Json(state.cluster.overview(0))
}

async fn healthz() -> &'static str {
    "OK"
}

async fn readyz(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> Result<&'static str, StatusCode> {
    let ready = state.is_ready.load(Ordering::Acquire);
    let draining = state.draining.load(Ordering::Acquire);
    if ready && !draining {
        Ok("OK")
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

async fn metrics(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> Result<String, StatusCode> {
    let mut buffer = String::new();
    let registry = state
        .registry
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    prometheus_client::encoding::text::encode(&mut buffer, &registry)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_when_all_nodes_live() {
        let cs = ClusterState::default();
        cs.nodes_total.store(3, Ordering::Relaxed);
        cs.nodes_live.store(3, Ordering::Relaxed);
        cs.shards_total.store(6, Ordering::Relaxed);
        let o = cs.overview(0);
        assert_eq!(o.cluster_status, "Healthy");
        assert_eq!(o.nodes_live, 3);
        assert_eq!(o.shards_total, 6);
    }

    #[test]
    fn degraded_then_unhealthy() {
        let cs = ClusterState::default();
        cs.nodes_total.store(3, Ordering::Relaxed);
        cs.nodes_live.store(2, Ordering::Relaxed);
        assert_eq!(cs.overview(0).cluster_status, "Degraded");
        cs.shards_unavailable.store(1, Ordering::Relaxed);
        assert_eq!(cs.overview(0).cluster_status, "Unhealthy");
    }

    #[test]
    fn registered_metrics_appear_in_output() {
        let mut registry = Registry::default();
        let metrics = Metrics::register(&mut registry);
        metrics.queries_total.inc();
        metrics.janitor_aborts_total.inc_by(2);
        metrics.observe_raft_command("commit_batch", "success", Duration::from_millis(2));
        metrics.observe_intent_batch(4);
        metrics.observe_commit_batch(4, Duration::from_millis(1));
        metrics.inc_commit_batch_fallback("queue_full");
        metrics.observe_commit("batch", "success", Duration::from_millis(3));
        metrics
            .raft_log_entries
            .get_or_create(&vec![("group".to_string(), "shard-meta".to_string())])
            .set(42);
        metrics
            .snapshot_deferrals_total
            .get_or_create(&vec![("group".to_string(), "shard-meta".to_string())])
            .inc();
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        // prometheus-client appends its own `_total` to counter samples.
        assert!(buf.contains("nodus_queries_total"));
        assert!(buf.contains("nodus_janitor_aborts_total_total 2"));
        assert!(buf.contains(
            "nodus_raft_commands_total_total{command=\"commit_batch\",result=\"success\"} 1"
        ));
        assert!(buf.contains(
            "nodus_raft_dispatch_seconds_count{command=\"commit_batch\",result=\"success\"} 1"
        ));
        assert!(buf.contains("nodus_intent_batch_operations_count 1"));
        assert!(buf.contains("nodus_commit_batch_size_count 1"));
        assert!(buf.contains("nodus_commit_batch_wait_seconds_count 1"));
        assert!(buf.contains("nodus_commit_batch_fallbacks_total_total{reason=\"queue_full\"} 1"));
        assert!(
            buf.contains("nodus_commit_latency_seconds_count{mode=\"batch\",result=\"success\"} 1")
        );
        assert!(buf.contains("nodus_raft_log_entries{group=\"shard-meta\"} 42"));
        assert!(buf.contains("nodus_snapshot_deferrals_total_total{group=\"shard-meta\"} 1"));
    }
}
