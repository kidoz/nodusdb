use std::sync::Arc;

use nodus_security::SessionRegistry;

mod client_meta;
mod copy;
mod encoding;
mod extended_query;
mod server;
mod simple_query;
mod startup;
mod streaming;
mod type_map;
mod wire_format;
pub use server::start_pgwire_server;

pub(crate) const METADATA_NODUS_SESSION_ID: &str = "nodus_session_id";
pub(crate) const METADATA_NODUS_PRINCIPAL_ID: &str = "nodus_principal_id";
pub(crate) const METADATA_BACKEND_PID: &str = "nodus_backend_pid";
pub(crate) const METADATA_BACKEND_SECRET: &str = "nodus_backend_secret";
pub(crate) const METADATA_TX_STATUS: &str = "nodus_tx_status";
pub(crate) const METADATA_COPY_ROWS: &str = "nodus_copy_rows";
pub(crate) const METADATA_COPY_EXTENDED: &str = "nodus_copy_extended";
/// The `COPY ... FROM STDIN` statement text, stashed when entering copy-in so
/// the copy handler can resolve the target table, columns, and format.
pub(crate) const METADATA_COPY_STMT: &str = "nodus_copy_stmt";
pub(crate) const METADATA_STATEMENT_TIMEOUT_MS: &str = "nodus_statement_timeout_ms";

pub(crate) const POSTGRES_TYPEMOD_NONE: i32 = -1;

/// Runs the synchronous executor on the blocking pool so the calling reactor
/// worker is never parked. This is required for correctness: executor write
/// paths route through the async `RaftRouter`, which waits via `blocking_recv`
/// and would panic (and risk worker-pool starvation) if called on a runtime
/// worker thread.
pub(crate) async fn execute_off_reactor(
    executor: Arc<dyn nodus_executor::Executor>,
    ctx: nodus_executor::ExecutionContext,
    plan: nodus_executor::LogicalPlan,
) -> anyhow::Result<nodus_executor::QueryOutput> {
    match tokio::task::spawn_blocking(move || executor.execute_logical(&ctx, plan)).await {
        Ok(result) => result,
        Err(join_err) => Err(anyhow::anyhow!("execution task failed: {join_err}")),
    }
}

pub struct NodusQueryHandler {
    /// Fallback session id for tests that instantiate the handler directly.
    pub session_id: String,
    pub session_state: nodus_sql::SessionState,
    pub executor: Arc<dyn nodus_executor::Executor>,
    pub metrics: nodus_monitoring::Metrics,
    pub(crate) registry: Arc<SessionRegistry>,
    pub(crate) slow_log: Arc<nodus_monitoring::SlowQueryLog>,
}

/// Observes query latency on drop, so every `do_query` return path is covered:
/// records into the histogram and, if slow, into the slow-query log.
pub(crate) struct QueryTimer<'a> {
    pub(crate) start: std::time::Instant,
    pub(crate) sql: &'a str,
    pub(crate) session_id: &'a str,
    pub(crate) metrics: &'a nodus_monitoring::Metrics,
    pub(crate) slow_log: &'a nodus_monitoring::SlowQueryLog,
}

impl Drop for QueryTimer<'_> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.metrics
            .query_latency_seconds
            .observe(elapsed.as_secs_f64());
        let ms = elapsed.as_millis() as u64;
        if ms >= self.slow_log.threshold_ms() {
            self.slow_log
                .record(self.sql.to_string(), ms, self.session_id.to_string());
            self.metrics.slow_queries_total.inc();
        }
    }
}

pub(crate) struct CurrentQueryGuard<'a> {
    pub(crate) registry: &'a SessionRegistry,
    pub(crate) session_id: &'a str,
}

impl Drop for CurrentQueryGuard<'_> {
    fn drop(&mut self) {
        self.registry.finish_current_query(self.session_id);
    }
}

impl Drop for NodusQueryHandler {
    fn drop(&mut self) {
        if !self.session_id.is_empty() {
            self.registry.deregister(&self.session_id);
        }
    }
}
