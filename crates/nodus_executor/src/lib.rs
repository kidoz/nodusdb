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

mod advisory;
mod aggregates;
mod alter_table;
mod bits;
pub mod bytea;
mod constraints;
mod cte_scope;
mod cursors;
mod datetime;
mod datetime_format;
mod ddl;
mod dml;
mod english_stem;
mod error_fields;
mod eval_error;
mod execute;
mod explain;
pub(crate) mod filter_eval;
mod functions;
mod geometric;
mod index_keys;
mod information_schema;
mod json_text;
mod jsonpath;
mod merge;
mod multiranges;
mod net;
pub mod numeric;
mod parameters;
mod pg_catalog;
mod pg_regex;
mod plan_types;
mod planner;
mod random;
mod ranges;
mod referential;
mod result_types;
mod schemas;
mod search_path;
mod select;
mod sequences;
mod session_env;
mod session_functions;
mod session_vars;
mod set_ops;
mod streaming;
mod subqueries;
mod system_views;
mod table_functions;
mod temp_tables;
mod textsearch;
mod timezone;
mod transactions;
mod ts_dict;
mod ts_headline;
mod ts_parse;
mod tsrank;
mod updatable_views;
mod user_types;
mod value;
mod view_helpers;
mod windows;
pub use cursors::FetchDirection;
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
pub use session_vars::set_server_setting;
pub use user_types::{DomainCheck, DomainDefinition, EnumLabel, TypeChange, TypeDefinition};
pub use value::{ColumnDef, Value, float_text, float4_text, render};
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
    pub(crate) overlay: std::collections::BTreeMap<String, Option<String>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveTxn {
    pub(crate) txn_id: TxnId,
    pub(crate) read_ts: Timestamp,
    pub(crate) write_log: Vec<String>,
    /// The transaction's own writes by key (`None`: deleted), ordered so a
    /// table's or an index value's range reads without visiting the rest.
    pub(crate) overlay: std::collections::BTreeMap<String, Option<String>>,
    pub(crate) savepoints: Vec<SavepointState>,
    /// `true` for a user-issued `BEGIN`, `false` for the single-statement
    /// implicit transaction that wraps each autocommit statement. Only explicit
    /// transactions are surfaced in `pg_locks`, so a bare `SELECT FROM pg_locks`
    /// (itself implicitly wrapped) does not report its own throwaway xid.
    pub(crate) explicit: bool,
    /// `READ ONLY`: statements that write are refused.
    pub(crate) read_only: bool,
    /// The isolation level it reports (`read committed`).
    pub(crate) isolation: String,
    /// Whether a query has run in it, after which its isolation level and
    /// read-write mode can no longer change.
    pub(crate) queried: bool,
    /// Settings changed in it, with their values before (`None`: not set),
    /// restored when it rolls back.
    pub(crate) settings_before: HashMap<String, Option<String>>,
    /// Settings a `SET LOCAL` changed, with the values restored when it
    /// commits.
    pub(crate) local_settings: HashMap<String, Option<String>>,
    /// Notifications (`NOTIFY`) sent in it, delivered when it commits:
    /// channel and payload.
    pub(crate) pending_notifications: Vec<(String, String)>,
}

impl ActiveTxn {
    pub(crate) fn new(txn_id: TxnId, read_ts: Timestamp, explicit: bool) -> Self {
        Self {
            txn_id,
            read_ts,
            write_log: Vec::new(),
            overlay: std::collections::BTreeMap::new(),
            savepoints: Vec::new(),
            explicit,
            read_only: false,
            isolation: "read committed".to_string(),
            queried: false,
            settings_before: HashMap::new(),
            local_settings: HashMap::new(),
            pending_notifications: Vec::new(),
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

    /// Starts a session with the settings its client asked for when it
    /// connected (`application_name`, `TimeZone`), which `RESET` returns to.
    fn start_session(&self, _session_id: &str, _settings: &[(String, String)]) {}

    /// Takes the reported settings (`TimeZone`, `application_name`) the
    /// session's statements changed since the last call, with their new
    /// values, for the client's `ParameterStatus` messages.
    fn take_parameter_changes(&self, _session_id: &str) -> Vec<(String, String)> {
        Vec::new()
    }

    /// Takes the notifications (`NOTIFY`) for the session's channels since
    /// the last call: the sender's process id, the channel, and the payload.
    fn take_notifications(&self, _session_id: &str) -> Vec<(i32, String, String)> {
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
    /// The catalog without the relations that hold user types.
    pub(crate) catalog_reader: Arc<dyn CatalogReader>,
    /// The whole catalog, for user types. See [`crate::user_types`].
    pub(crate) types_catalog: Arc<dyn CatalogReader>,
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
    /// Per session, the settings its client asked for when it connected:
    /// what `RESET` returns to.
    pub(crate) session_resets: parking_lot::RwLock<HashMap<String, HashMap<String, String>>>,
    /// Per session, reported settings changed and not yet taken by the wire
    /// layer.
    pub(crate) parameter_changes: parking_lot::Mutex<HashMap<String, Vec<(String, String)>>>,
    /// Per session, its temporary relations. See [`crate::temp_tables`].
    pub(crate) temp_relations: parking_lot::Mutex<HashMap<String, temp_tables::TempRelations>>,
    /// Per session, the statements SQL `PREPARE` named.
    pub(crate) prepared:
        parking_lot::Mutex<HashMap<String, std::collections::BTreeMap<String, PreparedStatement>>>,
    /// Per session, its open cursors. See [`crate::cursors`].
    pub(crate) cursors: parking_lot::Mutex<HashMap<String, cursors::Cursors>>,
    /// The node's advisory locks.
    pub(crate) advisory: Arc<advisory::AdvisoryLocks>,
    /// Per session, the channels it listens on (`LISTEN`).
    pub(crate) listeners: parking_lot::RwLock<HashMap<String, std::collections::BTreeSet<String>>>,
    /// Per session, notifications for it not yet taken by the wire layer:
    /// the sender's process id, the channel, and the payload.
    pub(crate) notifications: parking_lot::Mutex<HashMap<String, Vec<(i32, String, String)>>>,
    /// Per session, what it is doing, for `pg_stat_activity`.
    pub(crate) activity: parking_lot::RwLock<HashMap<String, SessionActivity>>,
}

/// What a session is doing, as `pg_stat_activity` shows it.
pub(crate) struct SessionActivity {
    pub(crate) principal: PrincipalId,
    /// When it connected, its current (or last) statement started, and its
    /// state last changed, microseconds since the epoch.
    pub(crate) backend_start: i64,
    pub(crate) query_start: i64,
    pub(crate) state_change: i64,
    /// Whether a statement is running.
    pub(crate) active: bool,
}

/// A statement SQL `PREPARE` keeps for its session.
pub(crate) struct PreparedStatement {
    pub(crate) statement: String,
    /// The parameters' types, as declared or else as their use shows them,
    /// and as declared (for the statement's text).
    pub(crate) param_types: Vec<String>,
    pub(crate) declared_types: Vec<String>,
    /// When it was prepared, microseconds since the epoch.
    pub(crate) prepared_at: i64,
    pub(crate) executions: i64,
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
        session_functions::server_started();
        let sequences = Arc::new(sequences::SequenceStore::new(
            catalog_reader.clone(),
            kv.clone(),
            txn.clone(),
        ));
        let types_catalog = catalog_reader;
        let catalog_reader: Arc<dyn CatalogReader> =
            Arc::new(user_types::RelationCatalog(types_catalog.clone()));
        Self {
            catalog_reader,
            types_catalog,
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
            session_resets: parking_lot::RwLock::new(HashMap::new()),
            parameter_changes: parking_lot::Mutex::new(HashMap::new()),
            temp_relations: parking_lot::Mutex::new(HashMap::new()),
            prepared: parking_lot::Mutex::new(HashMap::new()),
            cursors: parking_lot::Mutex::new(HashMap::new()),
            advisory: Arc::new(advisory::AdvisoryLocks::default()),
            listeners: parking_lot::RwLock::new(HashMap::new()),
            notifications: parking_lot::Mutex::new(HashMap::new()),
            activity: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Notes that the session's statement starts (`active`) or ends.
    fn note_activity(&self, ctx: &ExecutionContext, active: bool) {
        let now = session_env::wall_micros();
        let mut activity = self.activity.write();
        let entry = activity
            .entry(ctx.session_id.clone())
            .or_insert_with(|| SessionActivity {
                principal: ctx.principal_id,
                backend_start: now,
                query_start: now,
                state_change: now,
                active,
            });
        entry.principal = ctx.principal_id;
        if active {
            entry.query_start = now;
        }
        entry.active = active;
        entry.state_change = now;
    }

    /// Forgets the session state `DISCARD ALL` clears besides settings,
    /// temporary relations, and sequence values: its prepared statements,
    /// channels, and advisory locks.
    pub(crate) fn discard_session_state(&self, session_id: &str) {
        self.prepared.lock().remove(session_id);
        self.cursors.lock().remove(session_id);
        self.listeners.write().remove(session_id);
        self.advisory.unlock_all(session_id);
    }

    /// Sends notifications from the session: when its transaction commits,
    /// or at once outside one.
    pub(crate) fn queue_notifications(&self, ctx: &ExecutionContext, notes: Vec<(String, String)>) {
        if notes.is_empty() {
            return;
        }
        if let Some(txn) = self
            .active_txns
            .write()
            .get_mut(&ctx.session_id)
            .filter(|t| t.explicit)
        {
            txn.pending_notifications.extend(notes);
            return;
        }
        self.deliver_notifications(&ctx.session_id, notes);
    }

    /// Hands notifications to the sessions listening on their channels.
    pub(crate) fn deliver_notifications(&self, sender: &str, notes: Vec<(String, String)>) {
        let pid = temp_tables::backend_pid(sender) as i32;
        let listeners = self.listeners.read();
        let mut inbox = self.notifications.lock();
        for (channel, payload) in notes {
            for (session, channels) in listeners.iter() {
                if channels.contains(&channel) {
                    inbox.entry(session.clone()).or_default().push((
                        pid,
                        channel.clone(),
                        payload.clone(),
                    ));
                }
            }
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
        let backend_pid = temp_tables::backend_pid(&ctx.session_id);
        session_env::SessionEnv {
            user: self
                .catalog_reader
                .get_principal_by_id(ctx.principal_id)
                .map(|p| p.name)
                .unwrap_or_else(|_| "unknown".to_string()),
            settings: {
                let mut settings = self
                    .session_vars
                    .read()
                    .get(&ctx.session_id)
                    .cloned()
                    .unwrap_or_default();
                if let Some(txn) = self.active_txns.read().get(&ctx.session_id) {
                    let read_only = if txn.read_only { "on" } else { "off" };
                    settings.insert("transaction_read_only".into(), read_only.into());
                    settings.insert("transaction_isolation".into(), txn.isolation.clone());
                }
                settings
            },
            staged_settings: Vec::new(),
            staged_notices: Vec::new(),
            staged_notifications: Vec::new(),
            advisory: Some(self.advisory.clone()),
            authz: Some((self.authz.clone(), ctx.principal_id)),
            in_explicit_txn: self
                .active_txns
                .read()
                .get(&ctx.session_id)
                .is_some_and(|t| t.explicit),
            transaction_micros,
            statement_micros,
            backend_pid,
            session_id: ctx.session_id.clone(),
            sequences: Some(self.sequences.clone()),
            catalog: Some(self.catalog_reader.clone()),
            types: Some(self.types_catalog.clone()),
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
            for (key, value) in txn.overlay.range(start..end) {
                match value {
                    Some(encoded) => {
                        keyed_rows
                            .insert(key.clone(), Self::decode_row(encoded.as_bytes(), &columns)?);
                    }
                    None => {
                        keyed_rows.remove(key);
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
            if txn.overlay.range(start..end).next().is_some() {
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
            for (key, value) in txn.overlay.range(start.clone()..end) {
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

    /// The row stored under `pk` in a table, as the session sees it (its own
    /// pending write first).
    pub(crate) fn get_row(
        &self,
        table_id: TableId,
        pk: &str,
        session: &str,
    ) -> Result<Option<Vec<Value>>> {
        let key = format!("{table_id}:{pk}");
        let columns = self.table_columns(table_id);
        if let Some(txn) = self.active_txns.read().get(session)
            && let Some(pending) = txn.overlay.get(&key)
        {
            return pending
                .as_ref()
                .map(|encoded| Self::decode_row(encoded.as_bytes(), &columns))
                .transpose();
        }
        let read_ts = self.read_ts(session);
        self.maybe_read_range_barrier(
            session,
            KeyRange {
                start: Bytes::from(format!("{}:", table_id)),
                end: Bytes::from(format!("{};", table_id)),
            },
        )?;
        self.kv
            .get(&Bytes::from(key), read_ts)?
            .map(|bytes| Self::decode_row(&bytes, &columns))
            .transpose()
    }

    /// The rows an index lists under `index_val`, as the session sees them
    /// (its own pending index entries and rows included), keyed by primary key.
    pub(crate) fn index_rows(
        &self,
        index_id: nodus_catalog::IndexId,
        index_val: &Value,
        table_id: TableId,
        session: &str,
    ) -> Result<Vec<(String, Vec<Value>)>> {
        let escaped = Self::escape_index_value(&render(&crate::value::key_form(index_val)));
        let prefix = format!("i:{}:{}:", index_id, escaped);
        let end = format!("i:{}:{};", index_id, escaped);
        let read_ts = self.read_ts(session);
        let mut pks = std::collections::BTreeSet::new();
        for pair in self.kv.scan(
            KeyRange {
                start: Bytes::from(prefix.clone()),
                end: Bytes::from(end.clone()),
            },
            read_ts,
        )? {
            let pair = pair?;
            if let Some(pk) = String::from_utf8_lossy(&pair.key).strip_prefix(&prefix) {
                pks.insert(pk.to_string());
            }
        }
        if let Some(txn) = self.active_txns.read().get(session) {
            for (key, entry) in txn.overlay.range(prefix.clone()..end) {
                if let Some(pk) = key.strip_prefix(&prefix) {
                    if entry.is_some() {
                        pks.insert(pk.to_string());
                    } else {
                        pks.remove(pk);
                    }
                }
            }
        }
        let mut rows = Vec::new();
        for pk in pks {
            if let Some(row) = self.get_row(table_id, &pk, session)? {
                rows.push((pk, row));
            }
        }
        Ok(rows)
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

impl MemExecutor {
    /// [`Executor::execute_logical`] once a statement may run: its implicit
    /// transaction, session environment, and checks.
    fn execute_logical_tracked(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
    ) -> Result<QueryOutput> {
        let is_txn_control = matches!(
            plan,
            LogicalPlan::Begin { .. }
                | LogicalPlan::Chain { .. }
                | LogicalPlan::Commit
                | LogicalPlan::Rollback
                | LogicalPlan::Savepoint { .. }
                | LogicalPlan::RollbackToSavepoint { .. }
                | LogicalPlan::ReleaseSavepoint { .. }
        );
        // A query whose WITH holds a data-modifying statement writes.
        let is_read_only = matches!(
            plan,
            LogicalPlan::Select { .. }
                | LogicalPlan::SelectLiteral { .. }
                | LogicalPlan::SetOp { .. }
                | LogicalPlan::Values { .. }
        ) && data_modifying_cte(&plan).is_none();
        let mut implicit_txn = None;

        if !is_txn_control && self.active_txns.read().get(&ctx.session_id).is_none() {
            let txn_record = self.txn.begin_txn()?;
            let txn = self.new_active_txn(
                &ctx.session_id,
                txn_record.txn_id,
                txn_record.read_ts,
                false,
            );
            self.active_txns.write().insert(ctx.session_id.clone(), txn);
            implicit_txn = Some(txn_record.txn_id);
        }

        // A runtime evaluation error anywhere in the statement fails it before
        // the implicit transaction can commit.
        let _env = session_env::install(self.session_env(ctx));
        eval_error::reset();

        // A read-only transaction refuses statements that write (but for
        // temporary relations); any other statement fixes the transaction's
        // isolation level and access mode.
        let read_only_error = {
            let write = write_command(&plan).filter(|_| !Self::writes_temp_relation(&plan));
            let mut guard = self.active_txns.write();
            match guard.get_mut(&ctx.session_id) {
                Some(txn) if txn.read_only && write.is_some() => write,
                Some(txn) if !is_txn_control && !is_session_statement(&plan) => {
                    txn.queried = true;
                    None
                }
                _ => None,
            }
        };
        let started = std::time::Instant::now();
        let result = match read_only_error {
            Some(command) => Err(anyhow::anyhow!(
                "cannot execute {command} in a read-only transaction"
            )),
            None => self.execute_logical_inner(ctx, plan),
        };
        let result = result
            .and_then(|out| eval_error::check().map(|()| out))
            .and_then(|out| {
                // A statement that ran past `statement_timeout` is canceled.
                match session_vars::duration_millis(
                    &session_env::setting("statement_timeout").unwrap_or_default(),
                ) {
                    Some(limit)
                        if limit > 0 && started.elapsed().as_millis() > u128::from(limit) =>
                    {
                        Err(anyhow::anyhow!(
                            "canceling statement due to statement timeout"
                        ))
                    }
                    _ => Ok(out),
                }
            })
            .map(|out| {
                // Settings `set_config` changed take effect with the statement,
                // and notifications `pg_notify` sent are sent.
                for (name, value, local) in session_env::take_staged_settings() {
                    self.change_setting(ctx, &name, value, local);
                }
                self.queue_notifications(ctx, session_env::take_staged_notifications());
                out
            })
            .map(|out| self.name_object_identifiers(out))
            .map(|mut out| {
                // A domain's values go to the client as its base type's.
                for ty in &mut out.types {
                    if crate::user_types::may_be_user_type(ty) {
                        *ty = crate::user_types::base_type(ty);
                    }
                }
                out
            })
            .map(|mut out| {
                // Zoned timestamps are shown in the session's zone, and
                // `char(n)` values padded.
                if let Some(forms) = timezone::output_forms(&out.types) {
                    for row in &mut out.rows {
                        timezone::show_row(&mut row.values, &forms);
                    }
                }
                out
            });

        // Warnings functions raised go to the client even when the statement
        // fails.
        for notice in session_env::take_staged_notices() {
            self.notices
                .lock()
                .entry(ctx.session_id.clone())
                .or_default()
                .push(notice);
        }
        if let Some(txn_id) = implicit_txn {
            self.active_txns.write().remove(&ctx.session_id);
            self.advisory.end_transaction(&ctx.session_id);
            match &result {
                Ok(_) => {
                    let commit_ts = self.commit_or_release(txn_id)?;
                    if !is_read_only {
                        self.kv.commit(txn_id, commit_ts)?;
                    }
                    self.after_commit(ctx);
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
        self.note_activity(ctx, true);
        let result = self.execute_logical_tracked(ctx, plan);
        self.note_activity(ctx, false);
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

    fn start_session(&self, session_id: &str, settings: &[(String, String)]) {
        let mut resets = HashMap::new();
        for (name, value) in settings {
            if let Ok(session_vars::SetAction::Set(value)) = session_vars::set_action(name, value) {
                resets.insert(name.to_ascii_lowercase(), value);
            }
        }
        self.session_vars
            .write()
            .entry(session_id.to_string())
            .or_default()
            .extend(resets.clone());
        self.session_resets
            .write()
            .insert(session_id.to_string(), resets);
    }

    fn take_parameter_changes(&self, session_id: &str) -> Vec<(String, String)> {
        self.parameter_changes
            .lock()
            .remove(session_id)
            .unwrap_or_default()
    }

    fn take_notifications(&self, session_id: &str) -> Vec<(i32, String, String)> {
        self.notifications
            .lock()
            .remove(session_id)
            .unwrap_or_default()
    }

    fn end_session(&self, session_id: &str) {
        if let Some(txn) = self.active_txns.write().remove(session_id) {
            // A client that drops mid-transaction must not leave the write intent
            // dangling; abort so the row locks/intents are released.
            let _ = self.txn.abort_txn(txn.txn_id);
            let _ = self.kv.abort(txn.txn_id);
        }
        // Its temporary relations go with it.
        self.drop_temp_relations(session_id);
        self.session_vars.write().remove(session_id);
        self.session_resets.write().remove(session_id);
        self.parameter_changes.lock().remove(session_id);
        self.discard_session_state(session_id);
        crate::random::forget(session_id);
        self.notifications.lock().remove(session_id);
        self.activity.write().remove(session_id);
        self.notices.lock().remove(session_id);
        self.sequences.end_session(session_id);
    }
}

/// The command a statement that writes names in PostgreSQL's
/// `cannot execute ... in a read-only transaction`.
fn write_command(plan: &LogicalPlan) -> Option<&'static str> {
    if let Some(command) = data_modifying_cte(plan) {
        return Some(command);
    }
    Some(match plan {
        LogicalPlan::Insert { .. } => "INSERT",
        LogicalPlan::Update { .. } => "UPDATE",
        LogicalPlan::Delete { .. } => "DELETE",
        LogicalPlan::Merge { .. } => "MERGE",
        LogicalPlan::Truncate { .. } => "TRUNCATE TABLE",
        LogicalPlan::CreateTable { .. } => "CREATE TABLE",
        LogicalPlan::CreateTableAs { .. } => "CREATE TABLE AS",
        LogicalPlan::DropTable { .. } => "DROP TABLE",
        LogicalPlan::AlterTable { .. } => "ALTER TABLE",
        LogicalPlan::CreateIndex { .. } => "CREATE INDEX",
        LogicalPlan::DropIndex { .. } => "DROP INDEX",
        LogicalPlan::RenameIndex { .. } => "ALTER INDEX",
        LogicalPlan::CreateType {
            definition: user_types::TypeDefinition::Domain(_),
            ..
        } => "CREATE DOMAIN",
        LogicalPlan::CreateType { .. } => "CREATE TYPE",
        LogicalPlan::AlterType { domain: true, .. } => "ALTER DOMAIN",
        LogicalPlan::AlterType { .. } => "ALTER TYPE",
        LogicalPlan::DropType { domain: true, .. } => "DROP DOMAIN",
        LogicalPlan::DropType { .. } => "DROP TYPE",
        LogicalPlan::CreateView { .. } => "CREATE VIEW",
        LogicalPlan::DropView { .. } => "DROP VIEW",
        LogicalPlan::CreateSchema { .. } => "CREATE SCHEMA",
        LogicalPlan::DropSchema { .. } => "DROP SCHEMA",
        LogicalPlan::AlterSchema { .. } => "ALTER SCHEMA",
        LogicalPlan::SetSchema { kind, .. } => match kind.as_str() {
            "VIEW" => "ALTER VIEW",
            "MATERIALIZED VIEW" => "ALTER MATERIALIZED VIEW",
            "SEQUENCE" => "ALTER SEQUENCE",
            "TYPE" => "ALTER TYPE",
            "DOMAIN" => "ALTER DOMAIN",
            _ => "ALTER TABLE",
        },
        LogicalPlan::CreateSequence { .. } => "CREATE SEQUENCE",
        LogicalPlan::AlterSequence { .. } => "ALTER SEQUENCE",
        LogicalPlan::DropSequence { .. } => "DROP SEQUENCE",
        LogicalPlan::CreateRole { .. } => "CREATE ROLE",
        LogicalPlan::Grant { .. } => "GRANT",
        LogicalPlan::Revoke { .. } => "REVOKE",
        LogicalPlan::Comment { .. } => "COMMENT",
        LogicalPlan::RefreshMaterializedView { .. } => "REFRESH MATERIALIZED VIEW",
        LogicalPlan::With { body, .. } => return write_command(body),
        _ => return None,
    })
}

/// The command of a data-modifying statement in a query's WITH clause
/// (`WITH d AS (DELETE ... RETURNING *) SELECT ...`), if it has one.
fn data_modifying_cte(plan: &LogicalPlan) -> Option<&'static str> {
    let ctes = match plan {
        LogicalPlan::Select { ctes, .. } | LogicalPlan::With { ctes, .. } => ctes,
        _ => return None,
    };
    ctes.iter().find_map(|(_, cte)| write_command(cte))
}

/// Statements about the session rather than its data, which leave a
/// transaction's characteristics open.
fn is_session_statement(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::SetVariable { .. }
            | LogicalPlan::ShowVariable { .. }
            | LogicalPlan::ResetVariable { .. }
            | LogicalPlan::SetTransaction { .. }
            | LogicalPlan::Noop { .. }
            | LogicalPlan::Prepare { .. }
            | LogicalPlan::Deallocate { .. }
            | LogicalPlan::Discard { .. }
    )
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
mod session_tests;
#[cfg(test)]
mod sql_feature_tests;
#[cfg(test)]
mod streaming_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod user_object_tests;
#[cfg(test)]
mod utility_tests;
