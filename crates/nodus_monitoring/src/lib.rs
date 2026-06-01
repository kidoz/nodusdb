use axum::{Router, http::StatusCode, routing::get};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, Ordering},
};

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
        m
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
}

impl Default for ClusterState {
    fn default() -> Self {
        // Single-node default until the control plane reports membership.
        Self {
            nodes_live: AtomicU32::new(1),
            nodes_total: AtomicU32::new(1),
            shards_total: AtomicU32::new(0),
            shards_unavailable: AtomicU32::new(0),
        }
    }
}

impl ClusterState {
    pub fn overview(&self, qps: f64, active_alerts: u32) -> ClusterOverview {
        let nodes_live = self.nodes_live.load(Ordering::Relaxed);
        let nodes_total = self.nodes_total.load(Ordering::Relaxed);
        let shards_unavailable = self.shards_unavailable.load(Ordering::Relaxed);
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
    // Live membership/shard health from ClusterState. QPS rate computation is a
    // follow-up; report 0.0 rather than a fabricated value.
    axum::Json(state.cluster.overview(0.0, 0))
}

async fn healthz() -> &'static str {
    "OK"
}

async fn readyz(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> Result<&'static str, StatusCode> {
    if state.is_ready.load(Ordering::Acquire) {
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
        let o = cs.overview(10.0, 0);
        assert_eq!(o.cluster_status, "Healthy");
        assert_eq!(o.nodes_live, 3);
        assert_eq!(o.shards_total, 6);
    }

    #[test]
    fn degraded_then_unhealthy() {
        let cs = ClusterState::default();
        cs.nodes_total.store(3, Ordering::Relaxed);
        cs.nodes_live.store(2, Ordering::Relaxed);
        assert_eq!(cs.overview(0.0, 0).cluster_status, "Degraded");
        cs.shards_unavailable.store(1, Ordering::Relaxed);
        assert_eq!(cs.overview(0.0, 0).cluster_status, "Unhealthy");
    }

    #[test]
    fn registered_metrics_appear_in_output() {
        let mut registry = Registry::default();
        let metrics = Metrics::register(&mut registry);
        metrics.queries_total.inc();
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        assert!(buf.contains("nodus_queries_total"));
    }
}
