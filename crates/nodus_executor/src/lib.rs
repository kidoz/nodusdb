#![allow(warnings)]
#![allow(clippy::collapsible_if, clippy::collapsible_match)]

use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use nodus_audit::{AuditEvent, AuditSink};
use nodus_authz::{Action, AuthzContext, AuthzEngine, AuthzRequest};
use nodus_catalog::{
    AuditEventId, CatalogReader, CatalogWriter, ColumnDescriptor, CreateTableRequest,
    DescriptorState, IndexId, MemoryCatalog, PrincipalId, ResourceRef, RoleId, TableId,
};
use nodus_storage_api::{IntentReplacement, KeyRange, KvEngine, Timestamp, TxnId};
use nodus_txn::TxnManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

mod aggregates;
mod alter_table;
mod constraints;
mod cte_scope;
mod ddl;
mod dml;
mod error_fields;
mod eval_error;
mod execute;
mod explain;
pub(crate) mod filter_eval;
mod functions;
mod information_schema;
mod json_text;
mod merge;
mod parameters;
mod pg_catalog;
mod plan_types;
mod planner;
mod referential;
mod result_types;
mod select;
mod sequences;
mod session_env;
mod session_vars;
mod set_ops;
mod streaming;
mod subqueries;
mod system_views;
mod table_functions;
mod transactions;
mod value;
mod view_helpers;
pub use error_fields::{error_fields, error_message};
pub use explain::ExplainOptions;
pub use json_text::{json_text, jsonb_text};
pub use plan_types::{
    AggregateOp, AlterTableOp, CompareOp, ConflictTarget, DeferredItem, FilterExpr, Join, JoinType,
    LogicalPlan, MergeAction, MergeClause, MergeKind, NewConstraint, OnConflictClause, Operand,
    PatternKind, Predicate, ProjectionItem, ReturningExpr, ScalarBinaryOp, ScalarExpr,
    ScalarUnaryOp, SetOpKind, SortKey, SortTarget, SubPlan, SubqueryKind, TableFnSpec,
};
pub use planner::{
    CopyOutputFormat, expr_to_value, parse_object_name, plan_copy_out, plan_statement,
};
pub(crate) use planner::{eval_scalar_expr, parse_filter_expr, scalar_has_aggregate};
pub use sequences::{SequenceChange, SequenceSpec};
pub use session_vars::canonical_setting_value;
pub use value::{ColumnDef, Value, render};
pub(crate) use value::{
    coerce, column_type, compare, eval_scalar_function, literal_arg, resolve_scalar_arg,
    values_equal,
};

/// Result of executing a statement: a tag for non-row commands, and column
/// names + rows for queries.
#[derive(Debug, Default, Clone)]
pub struct QueryOutput {
    pub columns: Vec<String>,
    pub types: Vec<String>,
    pub rows: Vec<Row>,
    pub tag: String,
}

impl QueryOutput {
    fn tag(tag: &str) -> Self {
        Self {
            columns: vec![],
            types: vec![],
            rows: vec![],
            tag: tag.to_string(),
        }
    }
}

/// Receives a streamed query result: the schema exactly once (before any row),
/// then each row in turn. [`RowSink::row`] returns `Err` to abort streaming early
/// — e.g. when the downstream consumer (the client socket) has gone away — so the
/// producer stops promptly instead of scanning the rest of the table.
pub trait RowSink {
    fn schema(&mut self, columns: Vec<String>, types: Vec<String>);
    fn row(&mut self, row: Row) -> Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalPlan {
    LocalInsert {
        table_id: TableId,
        id: String,
        name_val: String,
    },
    LocalPointGet {
        table_id: TableId,
        id: String,
    },
    LocalIndexScan {
        table_id: TableId,
        index_id: IndexId,
    },
    LocalUpdate {
        table_id: TableId,
    },
    LocalDelete {
        table_id: TableId,
    },
    DistributedRoute {
        plan: Box<PhysicalPlan>,
    },
}

#[derive(Clone)]
pub struct ExecutionContext {
    pub session_id: String,
    /// Authenticated principal making the request; used for authorization.
    pub principal_id: PrincipalId,
    pub active_roles: Vec<RoleId>,
    pub authz_catalog_version: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct SavepointState {
    pub(crate) name: String,
    pub(crate) write_log_len: usize,
    pub(crate) overlay: HashMap<String, Option<String>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveTxn {
    pub(crate) txn_id: TxnId,
    pub(crate) read_ts: Timestamp,
    pub(crate) write_log: Vec<String>,
    pub(crate) overlay: HashMap<String, Option<String>>,
    pub(crate) savepoints: Vec<SavepointState>,
    /// `true` for a user-issued `BEGIN`, `false` for the single-statement
    /// implicit transaction that wraps each autocommit statement. Only explicit
    /// transactions are surfaced in `pg_locks`, so a bare `SELECT FROM pg_locks`
    /// (itself implicitly wrapped) does not report its own throwaway xid.
    pub(crate) explicit: bool,
}

impl ActiveTxn {
    pub(crate) fn new(txn_id: TxnId, read_ts: Timestamp, explicit: bool) -> Self {
        Self {
            txn_id,
            read_ts,
            write_log: Vec::new(),
            overlay: HashMap::new(),
            savepoints: Vec::new(),
            explicit,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

pub trait Executor: Send + Sync {
    /// Infers parameter SQL types without executing the statement. Unknown
    /// contexts remain `None`; caller-specified types take precedence.
    fn infer_parameter_types(
        &self,
        _ctx: &ExecutionContext,
        _sql: &str,
    ) -> Result<Vec<Option<String>>> {
        Ok(vec![])
    }

    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<QueryOutput>;
    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>>;

    /// Releases any per-session state (GUC overlay, dangling transaction) when a
    /// connection closes. Default is a no-op for executors that hold none.
    fn end_session(&self, _session_id: &str) {}

    /// Takes the notices the session's statements raised since the last
    /// call (`table "t" does not exist, skipping`), oldest first, each a
    /// message with fields (see [`error_fields`]).
    fn take_notices(&self, _session_id: &str) -> Vec<String> {
        Vec::new()
    }

    /// Executes `plan`, streaming its result to `sink`: the schema first, then
    /// each row. A plain single-table scan streams row-by-row with bounded
    /// memory; every other shape is fully executed and its rows then pushed.
    /// Returns the command tag. The default materializes via
    /// [`Executor::execute_logical`]; implementations override it to stream.
    fn execute_streaming(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
        sink: &mut dyn RowSink,
    ) -> Result<String> {
        let out = self.execute_logical(ctx, plan)?;
        sink.schema(out.columns, out.types);
        for row in out.rows {
            sink.row(row)?;
        }
        Ok(out.tag)
    }
}

/// Reserved KV key under which the catalog's serialized state is stored. The
/// leading NUL keeps it out of any `{table_id}:{pk}` row key space.
const CATALOG_STATE_KEY: &[u8] = b"\x00catalog\x00state";

/// Session GUC requesting linearizable reads (read-your-writes across nodes).
/// See [`MemExecutor::linearizable_reads`].
const LINEARIZABLE_READS_VAR: &str = "nodus.linearizable_reads";

/// A [`nodus_catalog::CatalogStore`] backed by a [`KvEngine`], so the catalog
/// persists into the same (crash-safe) store as user data — one durable
/// mechanism, one recovery path. Should wrap the node's *local* engine (a direct
/// materialization, like the meta store), not the routing engine.
pub struct KvCatalogStore {
    kv: Arc<dyn KvEngine>,
    last_ts: std::sync::atomic::AtomicU64,
}

impl KvCatalogStore {
    pub fn new(kv: Arc<dyn KvEngine>) -> Self {
        Self {
            kv,
            last_ts: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl nodus_catalog::CatalogStore for KvCatalogStore {
    fn load(&self) -> Option<Vec<u8>> {
        self.kv
            .get(CATALOG_STATE_KEY, u64::MAX)
            .ok()
            .flatten()
            .map(|b| b.to_vec())
    }

    fn save(&self, bytes: &[u8]) -> Result<()> {
        // Strictly-monotonic commit ts so a later save always supersedes earlier
        // ones (reads use `u64::MAX`); wall-clock alone could collide within a µs.
        use std::sync::atomic::Ordering::SeqCst;
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let ts = loop {
            let last = self.last_ts.load(SeqCst);
            let next = wall.max(last + 1);
            if self
                .last_ts
                .compare_exchange(last, next, SeqCst, SeqCst)
                .is_ok()
            {
                break next;
            }
        };
        let txn = TxnId::new();
        self.kv.write_intent(
            txn,
            Bytes::from_static(CATALOG_STATE_KEY),
            Bytes::copy_from_slice(bytes),
        )?;
        self.kv.commit(txn, ts)?;
        Ok(())
    }
}

// MVP implementation mapping to required interfaces
#[allow(dead_code)]
pub struct MemExecutor {
    pub(crate) catalog_reader: Arc<dyn CatalogReader>,
    pub(crate) catalog_writer: Arc<dyn CatalogWriter>,
    pub(crate) authz: Arc<dyn AuthzEngine>,
    pub(crate) audit: Arc<dyn AuditSink>,
    pub(crate) kv: Arc<dyn KvEngine>,
    pub(crate) txn: Arc<dyn TxnManager>,
    /// Active explicit transaction per session id (`BEGIN`..`COMMIT`/`ROLLBACK`).
    /// Keyed by session so one connection's transaction can't affect another's.
    pub(crate) active_txns: parking_lot::RwLock<HashMap<String, ActiveTxn>>,
    /// Per-session GUC overlay (`SET`/`SHOW`). Keyed by session id; each value is
    /// a map of lowercased variable name to its set value. Cleared on session
    /// end so it cannot grow unbounded. See [`crate::session_vars`].
    pub(crate) session_vars: parking_lot::RwLock<HashMap<String, HashMap<String, String>>>,
    /// Set while a restore is replacing the engine's data: new statements are
    /// rejected so no query observes a partially restored state.
    pub(crate) restoring: Arc<std::sync::atomic::AtomicBool>,
    /// Drain/exclusion gate for restores. Each statement holds a read guard for
    /// its duration; a restore takes the write guard, which blocks until every
    /// in-flight statement has drained and then runs with exclusive access.
    pub(crate) restore_gate: Arc<parking_lot::RwLock<()>>,
    /// Sequence state access for `nextval` and friends.
    pub(crate) sequences: Arc<sequences::SequenceStore>,
    /// Notices raised per session and not yet taken by the wire layer.
    pub(crate) notices: parking_lot::Mutex<HashMap<String, Vec<String>>>,
}

impl MemExecutor {
    pub fn new(
        catalog_reader: Arc<dyn CatalogReader>,
        catalog_writer: Arc<dyn CatalogWriter>,
        authz: Arc<dyn AuthzEngine>,
        audit: Arc<dyn AuditSink>,
        kv: Arc<dyn KvEngine>,
        txn: Arc<dyn TxnManager>,
    ) -> Self {
        let sequences = Arc::new(sequences::SequenceStore::new(
            catalog_reader.clone(),
            kv.clone(),
            txn.clone(),
        ));
        Self {
            catalog_reader,
            catalog_writer,
            authz,
            audit,
            kv,
            txn,
            sequences,
            active_txns: parking_lot::RwLock::new(HashMap::new()),
            session_vars: parking_lot::RwLock::new(HashMap::new()),
            restoring: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            restore_gate: Arc::new(parking_lot::RwLock::new(())),
            notices: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Raises a notice to the session running `ctx`'s statement.
    pub(crate) fn notice(&self, ctx: &ExecutionContext, notice: error_fields::DbError) {
        self.notices
            .lock()
            .entry(ctx.session_id.clone())
            .or_default()
            .push(notice.into_text());
    }

    /// Shared handle to the restore-in-progress flag, so the admin restore path
    /// can fence query execution while it replaces engine data.
    pub fn restoring_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.restoring.clone()
    }

    /// Shared handle to the restore drain/exclusion gate (take the write guard to
    /// drain in-flight statements and run a restore with exclusive access).
    pub fn restore_gate(&self) -> Arc<parking_lot::RwLock<()>> {
        self.restore_gate.clone()
    }

    /// Builds an executor over durable components (custom LSM + catalog snapshot)
    pub fn persistent(
        audit: Arc<dyn AuditSink>,
        data_dir: &str,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<(Arc<MemExecutor>, Arc<MemoryCatalog>)> {
        let path = std::path::Path::new(data_dir);
        std::fs::create_dir_all(path)?;
        // Build the KV engine first, then back the catalog with it so both share
        // one durable store and recovery path.
        let kv = Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(
            path,
            encryption_key,
        )?);
        let cat = Arc::new(MemoryCatalog::with_store(Arc::new(KvCatalogStore::new(
            kv.clone(),
        )))?);

        if cat.get_database("default").is_err() {
            let db = cat.create_database(nodus_catalog::CreateDatabaseRequest {
                id: nodus_catalog::DatabaseId::new(),
                name: "default".into(),
                owner_role_id: None,
            })?;
            cat.create_schema(nodus_catalog::CreateSchemaRequest {
                id: nodus_catalog::SchemaId::new(),
                database_id: db.id,
                name: "public".into(),
                owner_role_id: None,
                managed_access: false,
            })?;
        }

        let txn = Arc::new(nodus_txn::MemTxnManager::new());
        let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(cat.clone()));

        let exec = Arc::new(MemExecutor::new(
            cat.clone(),
            cat.clone(),
            authz,
            audit,
            kv,
            txn,
        ));
        Ok((exec, cat))
    }
    /// Builds an executor over fresh in-memory components and returns it
    /// together with the shared catalog, so callers (e.g. the server) can seed
    /// principals/grants and an authenticator against the same catalog. Audit
    /// events are written to `audit`.
    pub fn shared(audit: Arc<dyn AuditSink>) -> (Arc<MemExecutor>, Arc<MemoryCatalog>) {
        let cat = Arc::new(MemoryCatalog::new());

        let db = cat
            .create_database(nodus_catalog::CreateDatabaseRequest {
                id: nodus_catalog::DatabaseId::new(),
                name: "default".into(),
                owner_role_id: None,
            })
            .unwrap();
        cat.create_schema(nodus_catalog::CreateSchemaRequest {
            id: nodus_catalog::SchemaId::new(),
            database_id: db.id,
            name: "public".into(),
            owner_role_id: None,
            managed_access: false,
        })
        .unwrap();

        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let txn = Arc::new(nodus_txn::MemTxnManager::new());
        let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(cat.clone()));
        let exec = Arc::new(MemExecutor::new(
            cat.clone(),
            cat.clone(),
            authz,
            audit,
            kv,
            txn,
        ));
        (exec, cat)
    }

    /// Runs one MVCC garbage-collection pass at the transaction manager's safe
    /// watermark. Returns the number of versions reclaimed.
    pub fn run_gc(&self) -> Result<usize> {
        let watermark = self.txn.gc_watermark();
        self.kv.garbage_collect(watermark)
    }

    /// Runs MVCC garbage collection while honoring an external protected
    /// timestamp, such as the oldest retained backup snapshot. The effective
    /// watermark is the lower of the transaction-safe watermark and the
    /// protected timestamp.
    pub fn run_gc_with_protected_watermark(&self, protected: Option<Timestamp>) -> Result<usize> {
        let txn_watermark = self.txn.gc_watermark();
        let watermark = protected.map_or(txn_watermark, |ts| txn_watermark.min(ts));
        self.kv.garbage_collect(watermark)
    }

    /// Exposes the underlying key-value engine, e.g., for backup payload extraction.
    pub fn kv(&self) -> Arc<dyn KvEngine> {
        self.kv.clone()
    }

    /// Read timestamp for a session: its active transaction's snapshot, or the
    /// latest committed state when the session has no open transaction.
    /// The session context scalar functions see while a statement runs.
    pub(crate) fn session_env(&self, ctx: &ExecutionContext) -> session_env::SessionEnv {
        let statement_micros = session_env::wall_micros();
        let transaction_micros = self
            .active_txns
            .read()
            .get(&ctx.session_id)
            .map_or(statement_micros, |txn| txn.read_ts as i64);
        let backend_pid = ctx
            .session_id
            .bytes()
            .fold(17_i64, |h, b| (h * 31 + i64::from(b)) % 2_000_000_000)
            .abs()
            + 1;
        session_env::SessionEnv {
            user: self
                .catalog_reader
                .get_principal_by_id(ctx.principal_id)
                .map(|p| p.name)
                .unwrap_or_else(|_| "unknown".to_string()),
            settings: self
                .session_vars
                .read()
                .get(&ctx.session_id)
                .cloned()
                .unwrap_or_default(),
            transaction_micros,
            statement_micros,
            backend_pid,
            session_id: ctx.session_id.clone(),
            sequences: Some(self.sequences.clone()),
            catalog: Some(self.catalog_reader.clone()),
            storage: Some((self.kv.clone(), self.read_ts(&ctx.session_id))),
        }
    }

    pub(crate) fn read_ts(&self, session: &str) -> Timestamp {
        match self.active_txns.read().get(session) {
            Some(txn) => txn.read_ts,
            None => u64::MAX,
        }
    }

    /// Whether the session opted into linearizable reads via the
    /// `nodus.linearizable_reads` GUC (`SET nodus.linearizable_reads = on`).
    /// Default off — reads serve from the local replica's snapshot, which is fast
    /// but can lag the leader; turning it on trades a Raft ReadIndex round-trip
    /// for read-your-writes guarantees across nodes.
    fn linearizable_reads(&self, session: &str) -> bool {
        self.session_vars
            .read()
            .get(session)
            .and_then(|vars| vars.get(LINEARIZABLE_READS_VAR))
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "on" | "true" | "1" | "yes"))
            .unwrap_or(false)
    }

    /// When the session requested linearizable reads, force the engine to reflect
    /// every write committed before this read for the group that owns `key`. A
    /// no-op under the default snapshot consistency and for non-replicated
    /// engines. MUST run on a blocking thread (the Raft barrier blocks).
    pub(crate) fn maybe_read_barrier(&self, session: &str, key: &[u8]) -> Result<()> {
        if self.linearizable_reads(session) {
            self.kv.read_barrier(key)?;
        }
        Ok(())
    }

    pub(crate) fn maybe_read_range_barrier(&self, session: &str, range: KeyRange) -> Result<()> {
        if self.linearizable_reads(session) {
            self.kv.read_range_barrier(range)?;
        }
        Ok(())
    }

    /// Returns the session's active txn id. Expects a transaction to be active.
    fn txn_for(&self, session: &str) -> Result<TxnId> {
        match self.active_txns.read().get(session) {
            Some(txn) => Ok(txn.txn_id),
            None => anyhow::bail!("No active transaction for session"),
        }
    }

    /// A table's columns, for restoring decoded rows' types; empty for a
    /// relation the catalog does not know.
    fn table_columns(&self, table_id: TableId) -> Vec<ColumnDescriptor> {
        self.catalog_reader
            .get_table_by_id(table_id)
            .map(|t| t.columns)
            .unwrap_or_default()
    }

    /// Decodes a stored row into typed values (see [`crate::value::restore_row`]).
    fn decode_row(bytes: &[u8], columns: &[ColumnDescriptor]) -> Result<Vec<Value>> {
        let mut row: Vec<Value> = serde_json::from_slice(bytes)?;
        crate::value::restore_row(&mut row, columns);
        Ok(row)
    }

    /// Scans all visible rows of a table, decoding each into typed values.
    pub(crate) fn scan_rows(&self, table_id: TableId, session: &str) -> Result<Vec<Vec<Value>>> {
        Ok(self
            .scan_rows_keyed(table_id, session)?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    /// Like [`Self::scan_rows`] but returns each row paired with its storage key.
    /// Mutations (UPDATE/DELETE) use the actual stored key rather than
    /// re-deriving it from row content, so rows written under any key scheme
    /// (including data from an older binary) stay addressable.
    pub(crate) fn scan_rows_keyed(
        &self,
        table_id: TableId,
        session: &str,
    ) -> Result<Vec<(String, Vec<Value>)>> {
        let read_ts = self.read_ts(session);
        let start = Bytes::from(format!("{}:", table_id));
        let end = Bytes::from(format!("{};", table_id));
        self.maybe_read_range_barrier(
            session,
            KeyRange {
                start: start.clone(),
                end: end.clone(),
            },
        )?;
        let columns = self.table_columns(table_id);
        let mut keyed_rows = std::collections::BTreeMap::new();
        for pair in self.kv.scan(KeyRange { start, end }, read_ts)? {
            let pair = pair?;
            keyed_rows.insert(
                String::from_utf8_lossy(&pair.key).to_string(),
                Self::decode_row(&pair.value, &columns)?,
            );
        }
        if let Some(txn) = self.active_txns.read().get(session) {
            let start = format!("{}:", table_id);
            let end = format!("{};", table_id);
            for (key, value) in &txn.overlay {
                if key >= &start && key < &end {
                    match value {
                        Some(encoded) => {
                            keyed_rows.insert(
                                key.clone(),
                                Self::decode_row(encoded.as_bytes(), &columns)?,
                            );
                        }
                        None => {
                            keyed_rows.remove(key);
                        }
                    }
                }
            }
        }
        Ok(keyed_rows.into_iter().collect())
    }

    /// Like [`Self::scan_rows`] but stops after `cap` rows, for `LIMIT`/`OFFSET`
    /// push-down: the underlying KV scan yields keys in order, so consuming only
    /// the first `cap` of them avoids reading (and deserializing) the whole table
    /// for a query that only needs a bounded prefix.
    ///
    /// Returns `None` — signalling the caller to fall back to a full
    /// [`Self::scan_rows`] — when the session has uncommitted overlay rows in this
    /// table's range, since a capped committed scan could otherwise skip a pending
    /// row that sorts within the cap. A read-only/autocommit statement has no such
    /// overlay, which is the case this optimization targets.
    pub(crate) fn scan_rows_capped(
        &self,
        table_id: TableId,
        session: &str,
        cap: usize,
    ) -> Result<Option<Vec<Vec<Value>>>> {
        if let Some(txn) = self.active_txns.read().get(session) {
            let start = format!("{}:", table_id);
            let end = format!("{};", table_id);
            if txn.overlay.keys().any(|k| k >= &start && k < &end) {
                return Ok(None);
            }
        }
        let read_ts = self.read_ts(session);
        let start = Bytes::from(format!("{}:", table_id));
        let end = Bytes::from(format!("{};", table_id));
        self.maybe_read_range_barrier(
            session,
            KeyRange {
                start: start.clone(),
                end: end.clone(),
            },
        )?;
        let columns = self.table_columns(table_id);
        let mut rows = Vec::with_capacity(cap.min(1024));
        for pair in self.kv.scan(KeyRange { start, end }, read_ts)?.take(cap) {
            let pair = pair?;
            rows.push(Self::decode_row(&pair.value, &columns)?);
        }
        Ok(Some(rows))
    }

    /// Scans a secondary index to quickly look up primary keys, then fetches those rows.
    pub(crate) fn index_scan(
        &self,
        index_id: nodus_catalog::IndexId,
        index_val: &Value,
        table_id: TableId,
        session: &str,
    ) -> Result<Vec<(String, Vec<Value>)>> {
        let read_ts = self.read_ts(session);
        // Barrier the table group whose rows this index lookup will fetch.
        self.maybe_read_range_barrier(
            session,
            KeyRange {
                start: Bytes::from(format!("{}:", table_id)),
                end: Bytes::from(format!("{};", table_id)),
            },
        )?;
        let escaped = Self::escape_index_value(&render(&crate::value::key_form(index_val)));
        let prefix = format!("i:{}:{}:", index_id, escaped);
        let start = Bytes::from(prefix.clone());
        let end_prefix = format!("i:{}:{};", index_id, escaped);
        let end = Bytes::from(end_prefix);

        let columns = self.table_columns(table_id);
        let mut rows = Vec::new();
        for pair in self.kv.scan(KeyRange { start, end }, read_ts)? {
            let pair = pair?;
            let key_str = String::from_utf8_lossy(&pair.key);
            if let Some(pk) = key_str.strip_prefix(&prefix) {
                // Fetch the actual row
                let row_key = Bytes::from(format!("{}:{}", table_id, pk));
                if let Some(row_val) = self.kv.get(&row_key, read_ts)? {
                    rows.push((pk.to_string(), Self::decode_row(&row_val, &columns)?));
                }
            }
        }
        Ok(rows)
    }

    /// Merges the session's uncommitted overlay into committed equality
    /// index-scan results, each keyed by the row's stored key. This lets an
    /// equality lookup use the index *inside* a transaction instead of falling
    /// back to a full table scan, while staying consistent with the txn's own
    /// pending writes, which the overlay keys by the same `{table_id}:{key}`.
    pub(crate) fn merge_overlay_eq(
        &self,
        committed: Vec<(String, Vec<Value>)>,
        table_id: TableId,
        col_pos: Option<usize>,
        val: &Value,
        session: &str,
    ) -> Vec<Vec<Value>> {
        let mut map: std::collections::BTreeMap<String, Vec<Value>> =
            committed.into_iter().collect();
        let columns = self.table_columns(table_id);
        if let Some(txn) = self.active_txns.read().get(session) {
            let start = format!("{}:", table_id);
            let end = format!("{};", table_id);
            for (key, value) in &txn.overlay {
                if key < &start || key >= &end {
                    continue;
                }
                let pk = key.strip_prefix(&start).unwrap_or(key).to_string();
                match value {
                    None => {
                        map.remove(&pk);
                    }
                    Some(encoded) => {
                        if let Ok(row) = Self::decode_row(encoded.as_bytes(), &columns) {
                            let matches = col_pos
                                .and_then(|p| row.get(p))
                                .map(|cv| compare(cv, val) == std::cmp::Ordering::Equal)
                                .unwrap_or(false);
                            if matches {
                                map.insert(pk, row);
                            } else {
                                map.remove(&pk);
                            }
                        }
                    }
                }
            }
        }
        map.into_values().collect()
    }

    /// Writes a row value at `key`, using the session's txn.
    pub(crate) fn write_row(&self, session: &str, key: String, value: String) -> Result<()> {
        let txn_id = self.txn_for(session)?;
        self.txn.track_write(txn_id, key.as_bytes().to_vec())?;
        self.kv
            .write_intent(txn_id, Bytes::from(key.clone()), Bytes::from(value.clone()))?;
        if let Some(txn) = self.active_txns.write().get_mut(session) {
            txn.write_log.push(key.clone());
            txn.overlay.insert(key, Some(value));
        }
        Ok(())
    }

    /// Tombstones `key`, using the session's txn.
    pub(crate) fn delete_row(&self, session: &str, key: String) -> Result<()> {
        let txn_id = self.txn_for(session)?;
        self.txn.track_write(txn_id, key.as_bytes().to_vec())?;
        self.kv.delete_intent(txn_id, Bytes::from(key.clone()))?;
        if let Some(txn) = self.active_txns.write().get_mut(session) {
            txn.write_log.push(key.clone());
            txn.overlay.insert(key, None);
        }
        Ok(())
    }

    /// Escapes an index *value* component so a value containing the `:`
    /// separator (text, UUIDs, timestamps, …) cannot be confused with the
    /// value/PK boundary in the `i:{id}:{value}:{pk}` key layout. The PK is the
    /// open-ended tail and needs no escaping.
    fn escape_index_value(value: &str) -> String {
        value.replace('\\', "\\\\").replace(':', "\\:")
    }

    fn index_key(index_id: nodus_catalog::IndexId, index_val: &Value, pk: &str) -> String {
        format!(
            "i:{}:{}:{}",
            index_id,
            Self::escape_index_value(&render(&crate::value::key_form(index_val))),
            pk
        )
    }

    pub(crate) fn write_index_entry(
        &self,
        session: &str,
        index_id: nodus_catalog::IndexId,
        index_val: &Value,
        pk: &str,
    ) -> Result<()> {
        let key = Self::index_key(index_id, index_val, pk);
        self.write_row(session, key, "".to_string())
    }

    pub(crate) fn delete_index_entry(
        &self,
        session: &str,
        index_id: nodus_catalog::IndexId,
        index_val: &Value,
        pk: &str,
    ) -> Result<()> {
        let key = Self::index_key(index_id, index_val, pk);
        self.delete_row(session, key)
    }

    /// Deny-by-default authorization gate for a single action on a resource.
    pub(crate) fn authorize(
        &self,
        ctx: &ExecutionContext,
        action: Action,
        resource: ResourceRef,
    ) -> Result<()> {
        let decision = self.authz.authorize(AuthzRequest {
            principal_id: ctx.principal_id,
            active_roles: ctx.active_roles.clone(),
            action: action.clone(),
            resource: resource.clone(),
            context: AuthzContext { database_id: None },
        })?;

        // Record every authorization decision to the audit trail.
        let _ = self.audit.record_event(AuditEvent {
            id: AuditEventId::new(),
            time: Utc::now(),
            actor: ctx.principal_id,
            action: action.to_privilege().to_string(),
            resource: Some(resource),
            source_ip: None,
            request_id: None,
            session_id: Some(ctx.session_id.clone()),
            query_id: None,
            reason: Some(format!("{:?}", decision.reason)),
            result: if decision.allowed {
                "Success".to_string()
            } else {
                "Denied".to_string()
            },
            error: if decision.allowed {
                None
            } else {
                Some("permission denied".to_string())
            },
            authz_catalog_version: Some(decision.catalog_version),
        });

        if !decision.allowed {
            anyhow::bail!("permission denied");
        }
        Ok(())
    }
}

// Temporary default constructor so we don't break existing setups
impl Default for MemExecutor {
    fn default() -> Self {
        let cat = Arc::new(nodus_catalog::MemoryCatalog::new());
        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let txn = Arc::new(nodus_txn::MemTxnManager::new());
        let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(cat.clone()));
        let audit = Arc::new(nodus_audit::LogAuditSink);

        Self::new(cat.clone(), cat, authz, audit, kv, txn)
    }
}

impl Executor for MemExecutor {
    fn infer_parameter_types(
        &self,
        ctx: &ExecutionContext,
        sql: &str,
    ) -> Result<Vec<Option<String>>> {
        self.infer_sql_parameters(ctx, sql)
    }

    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<QueryOutput> {
        // Fence query execution during a restore: reject new statements, and
        // hold a drain guard for the rest of this call so a concurrently
        // starting restore waits for us to finish before mutating the engine.
        // Together this guarantees no statement ever observes a partially
        // restored state.
        if self.restoring.load(std::sync::atomic::Ordering::Acquire) {
            anyhow::bail!("restore in progress; retry shortly");
        }
        let _drain_guard = self.restore_gate.read();

        let is_txn_control = matches!(
            plan,
            LogicalPlan::Begin
                | LogicalPlan::Commit
                | LogicalPlan::Rollback
                | LogicalPlan::Savepoint { .. }
                | LogicalPlan::RollbackToSavepoint { .. }
                | LogicalPlan::ReleaseSavepoint { .. }
        );
        let is_read_only = matches!(
            plan,
            LogicalPlan::Select { .. }
                | LogicalPlan::SelectLiteral { .. }
                | LogicalPlan::SetOp { .. }
                | LogicalPlan::Values { .. }
        );
        let mut implicit_txn = None;

        if !is_txn_control && self.active_txns.read().get(&ctx.session_id).is_none() {
            let txn_record = self.txn.begin_txn()?;
            self.active_txns.write().insert(
                ctx.session_id.clone(),
                ActiveTxn::new(txn_record.txn_id, txn_record.read_ts, false),
            );
            implicit_txn = Some(txn_record.txn_id);
        }

        // A runtime evaluation error anywhere in the statement fails it before
        // the implicit transaction can commit.
        let _env = session_env::install(self.session_env(ctx));
        eval_error::reset();
        let result = self
            .execute_logical_inner(ctx, plan)
            .and_then(|out| eval_error::check().map(|()| out))
            .map(|out| self.name_object_identifiers(out));

        if let Some(txn_id) = implicit_txn {
            self.active_txns.write().remove(&ctx.session_id);
            match &result {
                Ok(_) => {
                    let commit_ts = self.commit_or_release(txn_id)?;
                    if !is_read_only {
                        self.kv.commit(txn_id, commit_ts)?;
                    }
                }
                Err(_) => {
                    let _ = self.txn.abort_txn(txn_id);
                    if !is_read_only {
                        let _ = self.kv.abort(txn_id);
                    }
                }
            }
        }
        result
    }

    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>> {
        self.execute_physical_inner(ctx, plan)
    }

    fn execute_streaming(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
        sink: &mut dyn RowSink,
    ) -> Result<String> {
        self.stream_or_fallback(ctx, plan, sink)
    }

    fn take_notices(&self, session_id: &str) -> Vec<String> {
        self.notices.lock().remove(session_id).unwrap_or_default()
    }

    fn end_session(&self, session_id: &str) {
        self.session_vars.write().remove(session_id);
        self.notices.lock().remove(session_id);
        self.sequences.end_session(session_id);
        if let Some(txn) = self.active_txns.write().remove(session_id) {
            // A client that drops mid-transaction must not leave the write intent
            // dangling; abort so the row locks/intents are released.
            let _ = self.txn.abort_txn(txn.txn_id);
            let _ = self.kv.abort(txn.txn_id);
        }
    }
}

#[cfg(test)]
mod constraint_tests;
#[cfg(test)]
mod dml_join_tests;
#[cfg(test)]
mod phase1_tests;
#[cfg(test)]
mod phase2_tests;
#[cfg(test)]
mod phase3_tests;
#[cfg(test)]
mod sql_feature_tests;
#[cfg(test)]
mod streaming_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod utility_tests;
