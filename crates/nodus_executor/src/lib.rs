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

mod catalog_views;
mod constraints;
mod filter_eval;
mod plan_types;
mod planner;
mod value;
pub use plan_types::{
    AggregateOp, AlterTableOp, CompareOp, FilterExpr, Join, JoinType, LogicalPlan, Operand,
    Predicate, ProjectionItem,
};
pub(crate) use planner::parse_filter_expr;
pub use planner::{expr_to_value, parse_object_name, plan_statement};
pub use value::{ColumnDef, Value};
pub(crate) use value::{
    coerce, column_type, compare, eval_scalar_function, literal_arg, render, resolve_scalar_arg,
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
struct SavepointState {
    name: String,
    write_log_len: usize,
    overlay: HashMap<String, Option<String>>,
}

#[derive(Debug, Clone)]
struct ActiveTxn {
    txn_id: TxnId,
    read_ts: Timestamp,
    write_log: Vec<String>,
    overlay: HashMap<String, Option<String>>,
    savepoints: Vec<SavepointState>,
}

impl ActiveTxn {
    fn new(txn_id: TxnId, read_ts: Timestamp) -> Self {
        Self {
            txn_id,
            read_ts,
            write_log: Vec::new(),
            overlay: HashMap::new(),
            savepoints: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

pub trait Executor: Send + Sync {
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<QueryOutput>;
    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>>;
}

/// Reserved KV key under which the catalog's serialized state is stored. The
/// leading NUL keeps it out of any `{table_id}:{pk}` row key space.
const CATALOG_STATE_KEY: &[u8] = b"\x00catalog\x00state";

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
    pub(crate) active_txns: std::sync::RwLock<HashMap<String, ActiveTxn>>,
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
        Self {
            catalog_reader,
            catalog_writer,
            authz,
            audit,
            kv,
            txn,
            active_txns: std::sync::RwLock::new(HashMap::new()),
        }
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
        ))));

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

    /// Exposes the underlying key-value engine, e.g., for backup payload extraction.
    pub fn kv(&self) -> Arc<dyn KvEngine> {
        self.kv.clone()
    }

    /// Read timestamp for a session: its active transaction's snapshot, or the
    /// latest committed state when the session has no open transaction.
    fn read_ts(&self, session: &str) -> Timestamp {
        match self.active_txns.read().unwrap().get(session) {
            Some(txn) => txn.read_ts,
            None => u64::MAX,
        }
    }

    /// Returns the session's active txn id. Expects a transaction to be active.
    fn txn_for(&self, session: &str) -> Result<TxnId> {
        match self.active_txns.read().unwrap().get(session) {
            Some(txn) => Ok(txn.txn_id),
            None => anyhow::bail!("No active transaction for session"),
        }
    }

    /// Scans all visible rows of a table, decoding each into typed values.
    pub(crate) fn scan_rows(&self, table_id: TableId, session: &str) -> Result<Vec<Vec<Value>>> {
        let read_ts = self.read_ts(session);
        let start = Bytes::from(format!("{}:", table_id));
        let end = Bytes::from(format!("{};", table_id));
        let mut keyed_rows = std::collections::BTreeMap::new();
        for pair in self.kv.scan(KeyRange { start, end }, read_ts)? {
            let pair = pair?;
            keyed_rows.insert(
                String::from_utf8_lossy(&pair.key).to_string(),
                serde_json::from_slice::<Vec<Value>>(&pair.value)?,
            );
        }
        if let Some(txn) = self.active_txns.read().unwrap().get(session) {
            let start = format!("{}:", table_id);
            let end = format!("{};", table_id);
            for (key, value) in &txn.overlay {
                if key >= &start && key < &end {
                    match value {
                        Some(encoded) => {
                            keyed_rows.insert(key.clone(), serde_json::from_str(encoded)?);
                        }
                        None => {
                            keyed_rows.remove(key);
                        }
                    }
                }
            }
        }
        Ok(keyed_rows.into_values().collect())
    }

    /// Scans a secondary index to quickly look up primary keys, then fetches those rows.
    fn index_scan(
        &self,
        index_id: nodus_catalog::IndexId,
        index_val: &Value,
        table_id: TableId,
        session: &str,
    ) -> Result<Vec<Vec<Value>>> {
        let read_ts = self.read_ts(session);
        let prefix = format!("i:{}:{}:", index_id, render(index_val));
        let start = Bytes::from(prefix.clone());
        let end_prefix = format!("i:{}:{};", index_id, render(index_val));
        let end = Bytes::from(end_prefix);

        let mut rows = Vec::new();
        for pair in self.kv.scan(KeyRange { start, end }, read_ts)? {
            let pair = pair?;
            let key_str = String::from_utf8_lossy(&pair.key);
            if let Some(pk) = key_str.strip_prefix(&prefix) {
                // Fetch the actual row
                let row_key = Bytes::from(format!("{}:{}", table_id, pk));
                if let Ok(Some(row_val)) = self.kv.get(&row_key, read_ts) {
                    rows.push(serde_json::from_slice::<Vec<Value>>(&row_val)?);
                }
            }
        }
        Ok(rows)
    }

    /// Writes a row value at `key`, using the session's txn.
    fn write_row(&self, session: &str, key: String, value: String) -> Result<()> {
        let txn_id = self.txn_for(session)?;
        self.txn.track_write(txn_id, key.as_bytes().to_vec())?;
        self.kv
            .write_intent(txn_id, Bytes::from(key.clone()), Bytes::from(value.clone()))?;
        if let Some(txn) = self.active_txns.write().unwrap().get_mut(session) {
            txn.write_log.push(key.clone());
            txn.overlay.insert(key, Some(value));
        }
        Ok(())
    }

    /// Tombstones `key`, using the session's txn.
    fn delete_row(&self, session: &str, key: String) -> Result<()> {
        let txn_id = self.txn_for(session)?;
        self.txn.track_write(txn_id, key.as_bytes().to_vec())?;
        self.kv.delete_intent(txn_id, Bytes::from(key.clone()))?;
        if let Some(txn) = self.active_txns.write().unwrap().get_mut(session) {
            txn.write_log.push(key.clone());
            txn.overlay.insert(key, None);
        }
        Ok(())
    }

    fn index_key(index_id: nodus_catalog::IndexId, index_val: &Value, pk: &str) -> String {
        format!("i:{}:{}:{}", index_id, render(index_val), pk)
    }

    fn write_index_entry(
        &self,
        session: &str,
        index_id: nodus_catalog::IndexId,
        index_val: &Value,
        pk: &str,
    ) -> Result<()> {
        let key = Self::index_key(index_id, index_val, pk);
        self.write_row(session, key, "".to_string())
    }

    fn delete_index_entry(
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
    fn authorize(
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
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<QueryOutput> {
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
                | LogicalPlan::UnionAll { .. }
        );
        let mut implicit_txn = None;

        if !is_txn_control
            && self
                .active_txns
                .read()
                .unwrap()
                .get(&ctx.session_id)
                .is_none()
        {
            let txn_record = self.txn.begin_txn()?;
            self.active_txns.write().unwrap().insert(
                ctx.session_id.clone(),
                ActiveTxn::new(txn_record.txn_id, txn_record.read_ts),
            );
            implicit_txn = Some(txn_record.txn_id);
        }

        let result = self.execute_logical_inner(ctx, plan);

        if let Some(txn_id) = implicit_txn {
            self.active_txns.write().unwrap().remove(&ctx.session_id);
            match &result {
                Ok(_) => {
                    let commit_ts = self.txn.commit_txn(txn_id)?;
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
}

impl MemExecutor {
    pub(crate) fn execute_logical_inner(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
    ) -> Result<QueryOutput> {
        match plan {
            LogicalPlan::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                let db = self.catalog_reader.get_database("default")?;
                self.authorize(ctx, Action::CreateSchema, ResourceRef::Database(db.id))?;
                match self
                    .catalog_writer
                    .create_schema(nodus_catalog::CreateSchemaRequest {
                        id: nodus_catalog::SchemaId::new(),
                        database_id: db.id,
                        name: schema_name,
                        owner_role_id: None,
                        managed_access: false,
                    }) {
                    Ok(_) => Ok(QueryOutput::tag("CREATE SCHEMA")),
                    Err(e) => {
                        if if_not_exists && e.to_string().contains("already exists") {
                            Ok(QueryOutput::tag("CREATE SCHEMA"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::DropSchema {
                schema_name,
                if_exists,
                cascade: _,
            } => {
                let db_name = "default";
                match self.catalog_reader.get_schema(db_name, &schema_name) {
                    Ok(sch) => {
                        self.authorize(ctx, Action::CreateSchema, ResourceRef::Schema(sch.id))?;
                        self.catalog_writer.drop_schema(sch.id)?;
                        Ok(QueryOutput::tag("DROP SCHEMA"))
                    }
                    Err(e) => {
                        if if_exists {
                            Ok(QueryOutput::tag("DROP SCHEMA"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::CreateTable {
                name,
                columns,
                constraints,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&name)?;
                let db = self.catalog_reader.get_database(db_name)?;
                let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;
                let descriptors: Vec<_> = columns
                    .iter()
                    .map(|c| ColumnDescriptor {
                        id: nodus_catalog::ColumnId::new(),
                        name: c.name.clone(),
                        version: 1,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        state: DescriptorState::Public,
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                    })
                    .collect();

                let mut unique_cols = Vec::new();
                for (c, d) in columns.iter().zip(descriptors.iter()) {
                    if c.unique {
                        unique_cols.push((d.clone(), c.primary));
                    }
                }

                let tbl = self.catalog_writer.create_table(CreateTableRequest {
                    id: nodus_catalog::TableId::new(),
                    database_id: db.id,
                    schema_id: sch.id,
                    name: table_only.to_string(),
                    columns: descriptors,
                    constraints,
                    view_query: None,
                })?;

                for (col, primary) in unique_cols {
                    let index = nodus_catalog::IndexDescriptor {
                        id: nodus_catalog::IndexId::new(),
                        name: if primary {
                            format!("{}_pkey", table_only)
                        } else {
                            format!("{}_{}_idx", name, col.name)
                        },
                        version: 1,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        state: DescriptorState::Public,
                        index_type: if primary {
                            nodus_catalog::IndexType::Primary
                        } else {
                            nodus_catalog::IndexType::Unique
                        },
                        index_state: nodus_catalog::IndexState::Ready,
                        key_columns: vec![nodus_catalog::IndexColumn {
                            column_id: col.id,
                            descending: false,
                        }],
                        include_columns: vec![],
                        unique: true,
                        global: false,
                        predicate: None,
                        expressions: vec![],
                    };
                    self.catalog_writer.update_table_descriptor(
                        nodus_catalog::TableDescriptorChange::AddIndex {
                            table_id: tbl.id,
                            index,
                        },
                    )?;
                }

                Ok(QueryOutput::tag("CREATE TABLE"))
            }
            LogicalPlan::CreateView { name, query } => {
                let (db_name, schema_name, view_only) = parse_object_name(&name)?;
                let db = self.catalog_reader.get_database(db_name)?;
                let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;

                // Resolve the schema of the view by planning/executing a dummy pass or just full execute
                // For MVP, we can just execute the query and take its output columns.
                let mut is_valid = false;
                let mut view_cols = Vec::new();

                // Hack: serialize the logical plan to store it
                let view_query_json = serde_json::to_string(&*query)?;

                // Run query to get shape
                if let Ok(out) = self.execute_logical_inner(ctx, *query) {
                    for (i, cname) in out.columns.iter().enumerate() {
                        let ty = out.types.get(i).unwrap_or(&"VARCHAR".to_string()).clone();
                        view_cols.push(ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name: cname.clone(),
                            version: 1,
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            state: DescriptorState::Public,
                            data_type: ty,
                            nullable: true,
                        });
                    }
                    is_valid = true;
                }

                if !is_valid {
                    anyhow::bail!("Failed to resolve view query schema");
                }

                self.catalog_writer.create_table(CreateTableRequest {
                    id: nodus_catalog::TableId::new(),
                    database_id: db.id,
                    schema_id: sch.id,
                    name: view_only.to_string(),
                    columns: view_cols,
                    constraints: vec![],
                    view_query: Some(view_query_json),
                })?;

                Ok(QueryOutput::tag("CREATE VIEW"))
            }
            LogicalPlan::DropView { name, if_exists } => {
                let (db_name, schema_name, view_only) = parse_object_name(&name)?;
                match self
                    .catalog_reader
                    .get_table(db_name, schema_name, view_only)
                {
                    Ok(tbl) => {
                        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                        if tbl.view_query.is_none() {
                            anyhow::bail!("{} is not a view", name);
                        }
                        self.catalog_writer.drop_table(tbl.id)?;
                        Ok(QueryOutput::tag("DROP VIEW"))
                    }
                    Err(e) => {
                        if if_exists {
                            Ok(QueryOutput::tag("DROP VIEW"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::DropTable { name, if_exists } => {
                let (db_name, schema_name, table_only) = parse_object_name(&name)?;
                match self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)
                {
                    Ok(tbl) => {
                        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                        self.catalog_writer.drop_table(tbl.id)?;
                        Ok(QueryOutput::tag("DROP TABLE"))
                    }
                    Err(e) => {
                        if if_exists {
                            Ok(QueryOutput::tag("DROP TABLE"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::Insert {
                table_name,
                columns,
                values_list,

                returning,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::Insert, ResourceRef::Table(tbl.id))?;

                let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();
                let mut inserted_count = 0;
                let mut returning_rows = Vec::new();

                for values in values_list {
                    // Build the Values row in table-column order...
                    let mut raw = vec![Value::Null; col_names.len()];
                    if columns.is_empty() {
                        for (i, v) in values.iter().enumerate() {
                            if i < raw.len() {
                                raw[i] = v.clone();
                            }
                        }
                    } else {
                        for (cname, val) in columns.iter().zip(values.iter()) {
                            if let Some(idx) = col_names.iter().position(|c| c == cname) {
                                raw[idx] = val.clone();
                            }
                        }
                    }
                    // ...then coerce each cell to its column's type if it's Text, otherwise assume it's correctly bound.
                    let mut row = Vec::new();
                    for (i, c) in tbl.columns.iter().enumerate() {
                        let val = match &raw[i] {
                            Value::Text(s) => coerce(s, column_type(&c.data_type)),
                            other => other.clone(),
                        };
                        if !c.nullable && val == Value::Null {
                            anyhow::bail!("Column {} cannot be NULL", c.name);
                        }
                        row.push(val);
                    }

                    self.check_unique_constraints(&ctx.session_id, &tbl, &row, None)?;
                    self.check_table_constraints(
                        ctx,
                        &tbl,
                        &row,
                        &col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                    )?;

                    // Primary key = first column's rendered value.
                    let pk = row.first().map(render).unwrap_or_default();
                    let key = format!("{}:{}", tbl.id, pk);
                    let encoded = serde_json::to_string(&row)?;
                    self.write_row(&ctx.session_id, key, encoded)?;

                    // Maintain secondary indexes.
                    for idx in &tbl.indexes {
                        for kcol in &idx.key_columns {
                            if let Some(pos) =
                                tbl.columns.iter().position(|c| c.id == kcol.column_id)
                            {
                                let index_val = row.get(pos).unwrap_or(&Value::Null);
                                self.write_index_entry(&ctx.session_id, idx.id, index_val, &pk)?;
                            }
                        }
                    }

                    inserted_count += 1;
                    if !returning.is_empty() {
                        returning_rows.push(row);
                    }
                }

                if returning.is_empty() {
                    Ok(QueryOutput::tag(&format!("INSERT 0 {}", inserted_count)))
                } else {
                    let col_names: Vec<&str> =
                        tbl.columns.iter().map(|c| c.name.as_str()).collect();
                    let indices: Vec<Option<usize>> = returning
                        .iter()
                        .map(|c| {
                            col_names
                                .iter()
                                .position(|&tc| tc == c || tc.ends_with(&format!(".{}", c)))
                        })
                        .collect();
                    let rows = returning_rows
                        .into_iter()
                        .map(|r| Row {
                            values: indices
                                .iter()
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null)
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(QueryOutput {
                        tag: format!("INSERT 0 {}", inserted_count),
                        columns: returning.clone(),
                        types: Self::returning_types(&tbl.columns, &returning),
                        rows,
                    })
                }
            }
            LogicalPlan::Select {
                ctes,
                table_name,
                table_alias,
                joins,
                projection,
                group_by,
                filter,
                order_by,
                limit,
                offset,
                distinct,
            } => {
                if table_name.eq_ignore_ascii_case("pg_stat_ssl") {
                    return Ok(QueryOutput {
                        columns: vec!["ssl".to_string()],
                        types: vec!["BOOL".to_string()],
                        rows: vec![Row {
                            values: vec![Value::Bool(false)],
                        }],
                        tag: "SELECT 1".into(),
                    });
                }

                let mut cte_results = std::collections::HashMap::new();
                for (name, cte_plan) in ctes {
                    let out = self.execute_logical_inner(ctx, *cte_plan)?;
                    cte_results.insert(name, out);
                }

                let (tbl_cols, mut col_names, mut stored_rows) = if let Some(cte_out) =
                    cte_results.get(&table_name)
                {
                    let mut cols = Vec::new();
                    for (i, c) in cte_out.columns.iter().enumerate() {
                        let ty = cte_out
                            .types
                            .get(i)
                            .unwrap_or(&"VARCHAR".to_string())
                            .clone();
                        cols.push(ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name: c.clone(),
                            version: 1,
                            created_at: chrono::Utc::now(),
                            updated_at: chrono::Utc::now(),
                            state: nodus_catalog::DescriptorState::Public,
                            data_type: ty,
                            nullable: true,
                        });
                    }
                    let prefix = table_alias.as_deref().unwrap_or(&table_name);
                    let col_names = cols
                        .iter()
                        .map(|c| format!("{}.{}", prefix, c.name))
                        .collect();
                    (
                        cols,
                        col_names,
                        Some(cte_out.rows.iter().map(|r| r.values.clone()).collect()),
                    )
                } else {
                    let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                    let schema_name = if schema_name.eq_ignore_ascii_case("public")
                        && Self::is_pg_catalog_virtual_table_name(table_only)
                    {
                        "pg_catalog"
                    } else {
                        schema_name
                    };
                    let (tbl_cols, col_names, rows) = if Self::is_virtual_schema(schema_name) {
                        let (cols, rows) =
                            self.get_virtual_table(db_name, schema_name, table_only)?;
                        let prefix = table_alias.as_deref().unwrap_or(&table_name);
                        let col_names: Vec<String> = cols
                            .iter()
                            .map(|c| format!("{}.{}", prefix, c.name))
                            .collect();
                        (cols, col_names, rows)
                    } else {
                        let tbl =
                            self.catalog_reader
                                .get_table(db_name, schema_name, table_only)?;
                        self.authorize(ctx, Action::Select, ResourceRef::Table(tbl.id))?;

                        let prefix = table_alias.as_deref().unwrap_or(&table_name);
                        let col_names: Vec<String> = tbl
                            .columns
                            .iter()
                            .map(|c| format!("{}.{}", prefix, c.name))
                            .collect();

                        let mut rows = None;
                        let has_session_overlay = self
                            .active_txns
                            .read()
                            .unwrap()
                            .get(&ctx.session_id)
                            .map(|txn| !txn.overlay.is_empty())
                            .unwrap_or(false);
                        if !has_session_overlay
                            && let Some(FilterExpr::Predicate(Predicate {
                                left,
                                op: CompareOp::Eq,
                                right,
                            })) = filter.as_ref()
                        {
                            let col_name = left.split('.').last().unwrap_or(left);
                            if let Some(col) = tbl.columns.iter().find(|c| c.name == *col_name) {
                                for idx in &tbl.indexes {
                                    if idx.key_columns.iter().any(|kc| kc.column_id == col.id) {
                                        let val =
                                            self.eval_operand(&[], &[], &[], right, &col.data_type);
                                        if let Ok(indexed_rows) =
                                            self.index_scan(idx.id, &val, tbl.id, &ctx.session_id)
                                        {
                                            rows = Some(indexed_rows);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        let rows = match rows {
                            Some(r) => r,
                            None => {
                                if let Some(vq) = &tbl.view_query {
                                    let plan: LogicalPlan = serde_json::from_str(vq)?;
                                    let out = self.execute_logical_inner(ctx, plan)?;
                                    out.rows.iter().map(|r| r.values.clone()).collect()
                                } else {
                                    self.scan_rows(tbl.id, &ctx.session_id)?
                                }
                            }
                        };
                        (tbl.columns.clone(), col_names, rows)
                    };
                    (tbl_cols, col_names, Some(rows))
                };

                let mut joined_columns = tbl_cols;
                let mut stored_rows = stored_rows.unwrap();

                for join in &joins {
                    let (j_cols, j_rows) = if let Some(cte_out) = cte_results.get(&join.table_name)
                    {
                        let mut cols = Vec::new();
                        for (i, c) in cte_out.columns.iter().enumerate() {
                            let ty = cte_out
                                .types
                                .get(i)
                                .unwrap_or(&"VARCHAR".to_string())
                                .clone();
                            cols.push(ColumnDescriptor {
                                id: nodus_catalog::ColumnId::new(),
                                name: c.clone(),
                                version: 1,
                                created_at: chrono::Utc::now(),
                                updated_at: chrono::Utc::now(),
                                state: nodus_catalog::DescriptorState::Public,
                                data_type: ty,
                                nullable: true,
                            });
                        }
                        (
                            cols,
                            cte_out.rows.iter().map(|r| r.values.clone()).collect(),
                        )
                    } else {
                        let (j_db, j_sch, j_tbl_name) = parse_object_name(&join.table_name)?;
                        let j_sch = if j_sch.eq_ignore_ascii_case("public")
                            && Self::is_pg_catalog_virtual_table_name(j_tbl_name)
                        {
                            "pg_catalog"
                        } else {
                            j_sch
                        };
                        if Self::is_virtual_schema(j_sch) {
                            let (cols, rows) = self.get_virtual_table(j_db, j_sch, j_tbl_name)?;
                            (cols, rows)
                        } else {
                            let j_tbl = self.catalog_reader.get_table(j_db, j_sch, j_tbl_name)?;
                            self.authorize(ctx, Action::Select, ResourceRef::Table(j_tbl.id))?;
                            let j_rows = if let Some(vq) = &j_tbl.view_query {
                                let plan: LogicalPlan = serde_json::from_str(vq)?;
                                let out = self.execute_logical_inner(ctx, plan)?;
                                out.rows.iter().map(|r| r.values.clone()).collect()
                            } else {
                                self.scan_rows(j_tbl.id, &ctx.session_id)?
                            };
                            (j_tbl.columns.clone(), j_rows)
                        }
                    };

                    let j_prefix = join.table_alias.as_deref().unwrap_or(&join.table_name);
                    let j_col_names: Vec<String> = j_cols
                        .iter()
                        .map(|c| format!("{}.{}", j_prefix, c.name))
                        .collect();

                    let mut combined_cols = col_names.clone();
                    combined_cols.extend(j_col_names.clone());

                    let mut combined_desc = joined_columns.clone();
                    combined_desc.extend(j_cols.clone());

                    let mut next_rows = Vec::new();
                    let mut right_matched = vec![false; j_rows.len()];
                    for r1 in &stored_rows {
                        let mut matched = false;
                        for (j_idx, r2) in j_rows.iter().enumerate() {
                            let mut combined_row = r1.clone();
                            combined_row.extend(r2.clone());
                            if self
                                .eval_filter(
                                    ctx,
                                    &combined_row,
                                    &combined_cols,
                                    &combined_desc,
                                    join.condition.as_ref(),
                                )
                                .unwrap_or(false)
                            {
                                next_rows.push(combined_row);
                                matched = true;
                                right_matched[j_idx] = true;
                            }
                        }
                        if !matched
                            && matches!(join.join_type, JoinType::LeftOuter | JoinType::FullOuter)
                        {
                            let mut combined_row = r1.clone();
                            // Left or Full join requires filling the right side with NULLs
                            let num_nulls = j_cols.len();
                            combined_row.extend(vec![Value::Null; num_nulls]);
                            next_rows.push(combined_row);
                        }
                    }
                    if matches!(join.join_type, JoinType::RightOuter | JoinType::FullOuter) {
                        let left_len = col_names.len();
                        for (j_idx, matched) in right_matched.into_iter().enumerate() {
                            if !matched {
                                let mut combined_row = vec![Value::Null; left_len];
                                combined_row.extend(j_rows[j_idx].clone());
                                next_rows.push(combined_row);
                            }
                        }
                    }
                    stored_rows = next_rows;
                    col_names = combined_cols;
                    joined_columns = combined_desc;
                }

                // WHERE: conjunction of typed predicates.
                stored_rows.retain(|r| {
                    self.eval_filter(ctx, r, &col_names, &joined_columns, filter.as_ref())
                        .unwrap_or(false)
                });

                // GROUP BY & Aggregation
                let is_agg = !group_by.is_empty()
                    || projection
                        .iter()
                        .any(|p| matches!(p, ProjectionItem::Aggregate(_, _)));

                if !is_agg {
                    if !order_by.is_empty() {
                        let mut order_indices = Vec::new();
                        for (ocol, asc) in &order_by {
                            let idx = col_names
                                .iter()
                                .position(|c| c == ocol || c.ends_with(&format!(".{}", ocol)));
                            if let Some(i) = idx {
                                order_indices.push((i, *asc));
                            }
                        }
                        stored_rows.sort_by(|a, b| {
                            for (idx, asc) in &order_indices {
                                let ord = compare(
                                    a.get(*idx).unwrap_or(&crate::Value::Null),
                                    b.get(*idx).unwrap_or(&crate::Value::Null),
                                );
                                if ord != std::cmp::Ordering::Equal {
                                    return if *asc { ord } else { ord.reverse() };
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }
                }

                let mut out_rows = Vec::new();
                let mut out_cols = Vec::new();

                if is_agg {
                    let mut groups: std::collections::BTreeMap<Vec<Vec<u8>>, Vec<Vec<Value>>> =
                        std::collections::BTreeMap::new();

                    let group_by_indices: Vec<Option<usize>> = group_by
                        .iter()
                        .map(|c| {
                            col_names
                                .iter()
                                .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)))
                        })
                        .collect();

                    if stored_rows.is_empty() && group_by.is_empty() {
                        // Empty set but scalar agg like COUNT(*), yields one row
                        groups.insert(vec![], vec![]);
                    } else {
                        for r in stored_rows {
                            let key = group_by_indices
                                .iter()
                                .map(|i| {
                                    let val = i.and_then(|idx| r.get(idx)).unwrap_or(&Value::Null);
                                    // serialize for BTreeMap key
                                    serde_json::to_vec(val).unwrap_or_default()
                                })
                                .collect::<Vec<_>>();
                            groups.entry(key).or_default().push(r);
                        }
                    }

                    for (_k, group_rows) in groups {
                        let mut out_row = Vec::new();
                        for proj_item in &projection {
                            match proj_item {
                                ProjectionItem::Literal(v)
                                | ProjectionItem::AliasedLiteral(v, _) => {
                                    out_row.push(v.clone());
                                }
                                ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
                                    let idx = col_names
                                        .iter()
                                        .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)));
                                    // Take from first row of group
                                    out_row.push(
                                        group_rows
                                            .first()
                                            .and_then(|r| idx.and_then(|i| r.get(i)))
                                            .map(|v| v.clone())
                                            .unwrap_or(crate::Value::Null),
                                    );
                                }
                                ProjectionItem::WindowFunction { .. }
                                | ProjectionItem::ScalarFunction { .. }
                                | ProjectionItem::JsonAccess { .. }
                                | ProjectionItem::CaseWhenEq { .. } => {
                                    out_row.push(Value::Null); // MVP fallback
                                }
                                ProjectionItem::Aggregate(op, inner) => {
                                    let mut idx = col_names.iter().position(|tc| {
                                        tc == inner || tc.ends_with(&format!(".{}", inner))
                                    });
                                    if inner == "*" {
                                        idx = Some(0); // For COUNT(*)
                                    }

                                    match op {
                                        AggregateOp::Count => {
                                            let count = if inner == "*" {
                                                group_rows.len() as i64
                                            } else {
                                                group_rows
                                                    .iter()
                                                    .filter(|r| {
                                                        idx.and_then(|i| r.get(i))
                                                            .map_or(false, |v| {
                                                                !matches!(v, Value::Null)
                                                            })
                                                    })
                                                    .count()
                                                    as i64
                                            };
                                            out_row.push(Value::Int(count));
                                        }
                                        AggregateOp::Sum => {
                                            let mut sum_int = 0i64;
                                            let mut sum_float = 0f64;
                                            let mut is_float = false;
                                            for r in &group_rows {
                                                if let Some(v) = idx.and_then(|i| r.get(i)) {
                                                    match v {
                                                        Value::Int(n) => {
                                                            if is_float {
                                                                sum_float += (*n) as f64
                                                            } else {
                                                                sum_int += n
                                                            }
                                                        }
                                                        Value::Float(f) => {
                                                            if !is_float {
                                                                sum_float = sum_int as f64;
                                                                is_float = true;
                                                            }
                                                            sum_float += f;
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                            if group_rows.is_empty() {
                                                out_row.push(Value::Null);
                                            } else if is_float {
                                                out_row.push(Value::Float(sum_float));
                                            } else {
                                                out_row.push(Value::Int(sum_int));
                                            }
                                        }
                                        AggregateOp::Min => {
                                            let mut min_val: Option<Value> = None;
                                            for r in &group_rows {
                                                if let Some(v) = idx.and_then(|i| r.get(i)) {
                                                    if !matches!(v, Value::Null) {
                                                        if let Some(cur) = &min_val {
                                                            if compare(&v, cur)
                                                                == std::cmp::Ordering::Less
                                                            {
                                                                min_val = Some(v.clone());
                                                            }
                                                        } else {
                                                            min_val = Some(v.clone());
                                                        }
                                                    }
                                                }
                                            }
                                            out_row.push(min_val.unwrap_or(crate::Value::Null));
                                        }
                                        AggregateOp::Max => {
                                            let mut max_val: Option<Value> = None;
                                            for r in &group_rows {
                                                if let Some(v) = idx.and_then(|i| r.get(i)) {
                                                    if !matches!(v, Value::Null) {
                                                        if let Some(cur) = &max_val {
                                                            if compare(&v, cur)
                                                                == std::cmp::Ordering::Greater
                                                            {
                                                                max_val = Some(v.clone());
                                                            }
                                                        } else {
                                                            max_val = Some(v.clone());
                                                        }
                                                    }
                                                }
                                            }
                                            out_row.push(max_val.unwrap_or(crate::Value::Null));
                                        }
                                    }
                                }
                            }
                        }
                        out_rows.push(out_row);
                    }

                    out_cols = if projection.is_empty() {
                        col_names.clone()
                    } else {
                        projection
                            .iter()
                            .map(|p| match p {
                                ProjectionItem::Column(c) => {
                                    c.split('.').last().unwrap_or(c).to_string()
                                }
                                ProjectionItem::AliasedColumn(_, a) => a.clone(),
                                ProjectionItem::Literal(_) => "?column?".to_string(),
                                ProjectionItem::AliasedLiteral(_, a) => a.clone(),
                                ProjectionItem::WindowFunction {
                                    func_name, alias, ..
                                } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                ProjectionItem::ScalarFunction {
                                    func_name, alias, ..
                                } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                ProjectionItem::JsonAccess {
                                    left,
                                    operator,
                                    right,
                                    alias,
                                } => alias
                                    .clone()
                                    .unwrap_or_else(|| format!("{}{}{}", left, operator, right)),
                                ProjectionItem::CaseWhenEq {
                                    else_column, alias, ..
                                } => alias.clone().unwrap_or_else(|| {
                                    else_column
                                        .split('.')
                                        .last()
                                        .unwrap_or(else_column)
                                        .to_string()
                                }),
                                ProjectionItem::Aggregate(op, inner) => {
                                    format!("{:?}({})", op, inner)
                                }
                            })
                            .collect()
                    };
                } else {
                    out_cols =
                        if projection.is_empty() {
                            col_names
                                .iter()
                                .map(|c| c.split('.').last().unwrap_or(c).to_string())
                                .collect()
                        } else {
                            projection
                                .iter()
                                .filter_map(|p| match p {
                                    ProjectionItem::Column(c) => {
                                        Some(c.split('.').last().unwrap_or(c).to_string())
                                    }
                                    ProjectionItem::AliasedColumn(_, a) => Some(a.clone()),
                                    ProjectionItem::Literal(_) => Some("?column?".to_string()),
                                    ProjectionItem::AliasedLiteral(_, a) => Some(a.clone()),
                                    ProjectionItem::WindowFunction {
                                        alias, func_name, ..
                                    } => Some(alias.clone().unwrap_or_else(|| func_name.clone())),
                                    ProjectionItem::ScalarFunction {
                                        alias, func_name, ..
                                    } => Some(alias.clone().unwrap_or_else(|| func_name.clone())),
                                    ProjectionItem::JsonAccess {
                                        left,
                                        operator,
                                        right,
                                        alias,
                                    } => Some(alias.clone().unwrap_or_else(|| {
                                        format!("{}{}{}", left, operator, right)
                                    })),
                                    ProjectionItem::CaseWhenEq {
                                        else_column, alias, ..
                                    } => Some(alias.clone().unwrap_or_else(|| {
                                        else_column
                                            .split('.')
                                            .last()
                                            .unwrap_or(else_column)
                                            .to_string()
                                    })),
                                    _ => None,
                                })
                                .collect()
                        };

                    // Evaluate Window Functions and Scalar Expressions before projecting
                    for proj_item in projection.iter() {
                        match proj_item {
                            ProjectionItem::WindowFunction {
                                func_name,
                                partition_by,
                                order_by: w_order_by,
                                alias,
                            } => {
                                let p_indices: Vec<usize> = partition_by
                                    .iter()
                                    .filter_map(|c| {
                                        col_names.iter().position(|tc| {
                                            tc == c || tc.ends_with(&format!(".{}", c))
                                        })
                                    })
                                    .collect();

                                let o_indices: Vec<(usize, bool)> = w_order_by
                                    .iter()
                                    .filter_map(|(c, asc)| {
                                        col_names
                                            .iter()
                                            .position(|tc| {
                                                tc == c || tc.ends_with(&format!(".{}", c))
                                            })
                                            .map(|idx| (idx, *asc))
                                    })
                                    .collect();

                                let mut row_indices: Vec<usize> = (0..stored_rows.len()).collect();
                                row_indices.sort_by(|&a_idx, &b_idx| {
                                    let a = &stored_rows[a_idx];
                                    let b = &stored_rows[b_idx];
                                    for &p_idx in &p_indices {
                                        let cmp = compare(
                                            a.get(p_idx).unwrap_or(&Value::Null),
                                            b.get(p_idx).unwrap_or(&Value::Null),
                                        );
                                        if cmp != std::cmp::Ordering::Equal {
                                            return cmp;
                                        }
                                    }
                                    for &(o_idx, asc) in &o_indices {
                                        let mut cmp = compare(
                                            a.get(o_idx).unwrap_or(&Value::Null),
                                            b.get(o_idx).unwrap_or(&Value::Null),
                                        );
                                        if !asc {
                                            cmp = cmp.reverse();
                                        }
                                        if cmp != std::cmp::Ordering::Equal {
                                            return cmp;
                                        }
                                    }
                                    std::cmp::Ordering::Equal
                                });

                                let mut results = vec![Value::Null; stored_rows.len()];
                                if func_name == "ROW_NUMBER" {
                                    let mut current_partition: Vec<Value> = Vec::new();
                                    let mut row_num = 1i64;
                                    for &row_idx in &row_indices {
                                        let row = &stored_rows[row_idx];
                                        let partition_key: Vec<Value> = p_indices
                                            .iter()
                                            .map(|&idx| {
                                                row.get(idx).unwrap_or(&Value::Null).clone()
                                            })
                                            .collect();
                                        if partition_key != current_partition {
                                            current_partition = partition_key;
                                            row_num = 1;
                                        }
                                        results[row_idx] = Value::Int(row_num);
                                        row_num += 1;
                                    }
                                } else {
                                    anyhow::bail!("Unsupported window function: {}", func_name);
                                }

                                // Append the result to `stored_rows`
                                for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                                    row.push(results[row_idx].clone());
                                }
                                let w_col_name = alias.clone().unwrap_or_else(|| func_name.clone());
                                col_names.push(w_col_name);
                            }
                            ProjectionItem::ScalarFunction {
                                func_name,
                                args,
                                alias,
                            } => {
                                let mut results = vec![Value::Null; stored_rows.len()];
                                for (row_idx, row) in stored_rows.iter().enumerate() {
                                    let resolved: Vec<Value> = args
                                        .iter()
                                        .map(|a| resolve_scalar_arg(a, row, &col_names))
                                        .collect();
                                    results[row_idx] = eval_scalar_function(func_name, &resolved);
                                }
                                for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                                    row.push(results[row_idx].clone());
                                }
                                let w_col_name = alias.clone().unwrap_or_else(|| func_name.clone());
                                col_names.push(w_col_name);
                            }
                            ProjectionItem::JsonAccess {
                                left,
                                operator,
                                right,
                                alias,
                            } => {
                                let mut results = vec![Value::Null; stored_rows.len()];
                                for (row_idx, row) in stored_rows.iter().enumerate() {
                                    let c_idx = col_names.iter().position(|tc| {
                                        tc == left || tc.ends_with(&format!(".{}", left))
                                    });
                                    if let Some(i) = c_idx {
                                        if let Some(v) = row.get(i) {
                                            if operator == "->>" {
                                                let json_str = match v {
                                                    Value::Jsonb(j) => j.to_string(),
                                                    Value::Text(s) => s.clone(),
                                                    _ => "".to_string(),
                                                };
                                                if let Ok(json) =
                                                    serde_json::from_str::<serde_json::Value>(
                                                        &json_str,
                                                    )
                                                {
                                                    if let Some(obj) = json.as_object() {
                                                        if let Some(val) = obj.get(right) {
                                                            results[row_idx] = match val {
                                                                serde_json::Value::String(s) => {
                                                                    Value::Text(s.clone())
                                                                }
                                                                _ => Value::Text(val.to_string()),
                                                            };
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                                    row.push(results[row_idx].clone());
                                }
                                let w_col_name = alias
                                    .clone()
                                    .unwrap_or_else(|| format!("{}{}{}", left, operator, right));
                                col_names.push(w_col_name);
                            }
                            _ => {}
                        }
                    }

                    let indices: Vec<Option<usize>> = out_cols
                        .iter()
                        .enumerate()
                        .map(|(pi, c)| {
                            if projection.is_empty() {
                                // `out_cols` mirrors `col_names` positionally (just unqualified);
                                // resolving by name would collapse duplicate column names across joined tables.
                                Some(pi)
                            } else {
                                let actual_col = match &projection[pi] {
                                    ProjectionItem::Column(c) => c.clone(),
                                    ProjectionItem::AliasedColumn(c, _) => c.clone(),
                                    ProjectionItem::Literal(_)
                                    | ProjectionItem::AliasedLiteral(_, _) => "".to_string(),
                                    ProjectionItem::WindowFunction {
                                        func_name, alias, ..
                                    } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                    ProjectionItem::ScalarFunction {
                                        func_name, alias, ..
                                    } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                    ProjectionItem::JsonAccess {
                                        left,
                                        operator,
                                        right,
                                        alias,
                                    } => alias.clone().unwrap_or_else(|| {
                                        format!("{}{}{}", left, operator, right)
                                    }),
                                    ProjectionItem::CaseWhenEq {
                                        else_column, alias, ..
                                    } => alias.clone().unwrap_or_else(|| else_column.clone()),
                                    _ => c.clone(),
                                };
                                col_names.iter().position(|tc| {
                                    tc == &actual_col || tc.ends_with(&format!(".{}", actual_col))
                                })
                            }
                        })
                        .collect();

                    out_rows = stored_rows
                        .into_iter()
                        .map(|r| {
                            if projection.is_empty() {
                                indices
                                    .iter()
                                    .map(|i| {
                                        i.and_then(|idx| r.get(idx))
                                            .cloned()
                                            .unwrap_or(crate::Value::Null)
                                    })
                                    .collect()
                            } else {
                                projection
                                    .iter()
                                    .enumerate()
                                    .map(|(pi, proj)| match proj {
                                        ProjectionItem::Literal(v)
                                        | ProjectionItem::AliasedLiteral(v, _) => v.clone(),
                                        ProjectionItem::CaseWhenEq {
                                            left,
                                            equals,
                                            then_value,
                                            then_column,
                                            else_column,
                                            ..
                                        } => {
                                            let left_idx = col_names.iter().position(|tc| {
                                                tc == left || tc.ends_with(&format!(".{}", left))
                                            });
                                            let else_idx = col_names.iter().position(|tc| {
                                                tc == else_column
                                                    || tc.ends_with(&format!(".{}", else_column))
                                            });
                                            let left_value = left_idx
                                                .and_then(|idx| r.get(idx))
                                                .unwrap_or(&Value::Null);
                                            if compare(left_value, equals)
                                                == std::cmp::Ordering::Equal
                                            {
                                                if let Some(then_column) = then_column {
                                                    col_names
                                                        .iter()
                                                        .position(|tc| {
                                                            tc == then_column
                                                                || tc.ends_with(&format!(
                                                                    ".{}",
                                                                    then_column
                                                                ))
                                                        })
                                                        .and_then(|idx| r.get(idx))
                                                        .cloned()
                                                        .unwrap_or(Value::Null)
                                                } else {
                                                    then_value.clone()
                                                }
                                            } else {
                                                else_idx
                                                    .and_then(|idx| r.get(idx))
                                                    .cloned()
                                                    .unwrap_or(Value::Null)
                                            }
                                        }
                                        _ => indices[pi]
                                            .and_then(|idx| r.get(idx))
                                            .cloned()
                                            .unwrap_or(crate::Value::Null),
                                    })
                                    .collect()
                            }
                        })
                        .collect::<Vec<_>>();
                }

                // ORDER BY for aggregates (uses out_cols). For non-aggregates, it was already sorted.
                if is_agg {
                    if !order_by.is_empty() {
                        let mut order_indices = Vec::new();
                        for (ocol, asc) in &order_by {
                            let idx = out_cols
                                .iter()
                                .position(|c| c == ocol || c.ends_with(&format!(".{}", ocol)));
                            if let Some(i) = idx {
                                order_indices.push((i, *asc));
                            }
                        }
                        out_rows.sort_by(|a, b| {
                            for (idx, asc) in &order_indices {
                                let ord = compare(
                                    a.get(*idx).unwrap_or(&crate::Value::Null),
                                    b.get(*idx).unwrap_or(&crate::Value::Null),
                                );
                                if ord != std::cmp::Ordering::Equal {
                                    return if *asc { ord } else { ord.reverse() };
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }
                }

                // DISTINCT
                if distinct {
                    let mut seen = Vec::new();
                    out_rows.retain(|r| {
                        let is_seen = seen.iter().any(|s: &Vec<Value>| {
                            s.iter()
                                .zip(r.iter())
                                .all(|(va, vb)| compare(va, vb) == std::cmp::Ordering::Equal)
                        });
                        if is_seen {
                            false
                        } else {
                            seen.push(r.clone());
                            true
                        }
                    });
                }

                // OFFSET
                if let Some(o) = offset {
                    if o < out_rows.len() {
                        out_rows.drain(0..o);
                    } else {
                        out_rows.clear();
                    }
                }

                // LIMIT
                if let Some(n) = limit {
                    out_rows.truncate(n);
                }

                let rows = out_rows
                    .into_iter()
                    .map(|r| Row { values: r })
                    .collect::<Vec<_>>();

                let tag = format!("SELECT {}", rows.len());
                let mut types = Vec::new();
                for (i, c) in out_cols.iter().enumerate() {
                    // Quick lookup for type. Default to VARCHAR.
                    let mut ty = "VARCHAR".to_string();

                    if projection.is_empty() {
                        if let Some(col_desc) = joined_columns.get(i) {
                            ty = col_desc.data_type.clone();
                        }
                    } else if let Some(source_col) = projection.get(i).and_then(|item| match item {
                        ProjectionItem::Column(col) | ProjectionItem::AliasedColumn(col, _) => {
                            Some(col)
                        }
                        _ => None,
                    }) {
                        if let Some(source_idx) = col_names.iter().position(|candidate| {
                            candidate == source_col
                                || candidate.ends_with(&format!(".{}", source_col))
                        }) {
                            if let Some(col_desc) = joined_columns.get(source_idx) {
                                ty = col_desc.data_type.clone();
                            }
                        }
                    }

                    if ty == "VARCHAR" && !rows.is_empty() {
                        if let Some(val) = rows[0].values.get(i) {
                            match val {
                                Value::Int(_) => ty = "INTEGER".to_string(),
                                Value::Float(_) => ty = "DOUBLE".to_string(),
                                Value::Bool(_) => ty = "BOOLEAN".to_string(),
                                Value::Text(_) => ty = "VARCHAR".to_string(),
                                Value::Null => ty = "VARCHAR".to_string(),
                                Value::Array(_) => ty = "VARCHAR".to_string(),
                                Value::Jsonb(_) => ty = "VARCHAR".to_string(),
                            }
                        }
                    } else if ty == "VARCHAR" {
                        // Also try to deduce from projection items if available
                        if i < projection.len() {
                            match &projection[i] {
                                ProjectionItem::Literal(Value::Int(_))
                                | ProjectionItem::AliasedLiteral(Value::Int(_), _) => {
                                    ty = "INTEGER".to_string()
                                }
                                ProjectionItem::Literal(Value::Float(_))
                                | ProjectionItem::AliasedLiteral(Value::Float(_), _) => {
                                    ty = "DOUBLE".to_string()
                                }
                                ProjectionItem::Literal(Value::Bool(_))
                                | ProjectionItem::AliasedLiteral(Value::Bool(_), _) => {
                                    ty = "BOOLEAN".to_string()
                                }
                                _ => {}
                            }
                        }
                    }
                    types.push(ty);
                }
                Ok(QueryOutput {
                    columns: out_cols,
                    types,
                    rows,
                    tag,
                })
            }
            LogicalPlan::Update {
                table_name,
                assignments,
                filter,
                returning,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::Update, ResourceRef::Table(tbl.id))?;
                let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();

                let mut updated = 0;
                let mut returning_rows = Vec::new();
                for mut row in self.scan_rows(tbl.id, &ctx.session_id)? {
                    if !self.row_matches(ctx, &row, &tbl.columns, filter.as_ref()) {
                        continue;
                    }
                    let old_row = row.clone();
                    let old_pk_str = old_row.first().map(render).unwrap_or_default();
                    let old_key = format!("{}:{}", tbl.id, old_pk_str);
                    for (col, val) in &assignments {
                        if let Some(idx) = col_names.iter().position(|c| c == col) {
                            let coerced = match val {
                                Value::Text(s) => {
                                    coerce(s, column_type(&tbl.columns[idx].data_type))
                                }
                                other => other.clone(),
                            };
                            if !tbl.columns[idx].nullable && coerced == Value::Null {
                                anyhow::bail!("Column {} cannot be NULL", col);
                            }
                            row[idx] = coerced;
                        }
                    }

                    let pk_str = row.first().map(render).unwrap_or_default();
                    self.check_unique_constraints(&ctx.session_id, &tbl, &row, Some(&pk_str))?;
                    self.check_table_constraints(
                        ctx,
                        &tbl,
                        &row,
                        &col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                    )?;

                    let new_key = format!("{}:{}", tbl.id, pk_str);
                    self.write_row(
                        &ctx.session_id,
                        new_key.clone(),
                        serde_json::to_string(&row)?,
                    )?;
                    if new_key != old_key {
                        self.delete_row(&ctx.session_id, old_key)?;
                    }

                    // Maintain secondary indexes.
                    for idx in &tbl.indexes {
                        for kcol in &idx.key_columns {
                            if let Some(pos) =
                                tbl.columns.iter().position(|c| c.id == kcol.column_id)
                            {
                                let old_index_val = old_row.get(pos).unwrap_or(&Value::Null);
                                let new_index_val = row.get(pos).unwrap_or(&Value::Null);
                                if old_index_val != new_index_val || old_pk_str != pk_str {
                                    self.delete_index_entry(
                                        &ctx.session_id,
                                        idx.id,
                                        old_index_val,
                                        &old_pk_str,
                                    )?;
                                    self.write_index_entry(
                                        &ctx.session_id,
                                        idx.id,
                                        new_index_val,
                                        &pk_str,
                                    )?;
                                }
                            }
                        }
                    }

                    updated += 1;
                    if !returning.is_empty() {
                        returning_rows.push(row);
                    }
                }
                if returning.is_empty() {
                    Ok(QueryOutput::tag(&format!("UPDATE {updated}")))
                } else {
                    let indices: Vec<Option<usize>> = returning
                        .iter()
                        .map(|c| {
                            col_names
                                .iter()
                                .position(|&tc| tc == c || tc.ends_with(&format!(".{}", c)))
                        })
                        .collect();
                    let rows = returning_rows
                        .into_iter()
                        .map(|r| Row {
                            values: indices
                                .iter()
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null)
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(QueryOutput {
                        tag: format!("UPDATE {updated}"),
                        columns: returning.clone(),
                        types: Self::returning_types(&tbl.columns, &returning),
                        rows,
                    })
                }
            }
            LogicalPlan::Delete {
                table_name,
                filter,
                returning,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::Delete, ResourceRef::Table(tbl.id))?;

                let mut deleted = 0;
                let mut returning_rows = Vec::new();
                for row in self.scan_rows(tbl.id, &ctx.session_id)? {
                    if !self.row_matches(ctx, &row, &tbl.columns, filter.as_ref()) {
                        continue;
                    }
                    let pk_str = row.first().map(render).unwrap_or_default();
                    let key = format!("{}:{}", tbl.id, pk_str);
                    self.delete_row(&ctx.session_id, key)?;

                    // Maintain secondary indexes.
                    for idx in &tbl.indexes {
                        for kcol in &idx.key_columns {
                            if let Some(pos) =
                                tbl.columns.iter().position(|c| c.id == kcol.column_id)
                            {
                                let index_val = row.get(pos).unwrap_or(&Value::Null);
                                self.delete_index_entry(
                                    &ctx.session_id,
                                    idx.id,
                                    index_val,
                                    &pk_str,
                                )?;
                            }
                        }
                    }

                    deleted += 1;
                    if !returning.is_empty() {
                        returning_rows.push(row);
                    }
                }
                if returning.is_empty() {
                    Ok(QueryOutput::tag(&format!("DELETE {deleted}")))
                } else {
                    let col_names: Vec<&str> =
                        tbl.columns.iter().map(|c| c.name.as_str()).collect();
                    let indices: Vec<Option<usize>> = returning
                        .iter()
                        .map(|c| {
                            col_names
                                .iter()
                                .position(|&tc| tc == c || tc.ends_with(&format!(".{}", c)))
                        })
                        .collect();
                    let rows = returning_rows
                        .into_iter()
                        .map(|r| Row {
                            values: indices
                                .iter()
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null)
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(QueryOutput {
                        tag: format!("DELETE {deleted}"),
                        columns: returning.clone(),
                        types: Self::returning_types(&tbl.columns, &returning),
                        rows,
                    })
                }
            }
            LogicalPlan::AlterTable {
                table_name,
                operation,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;

                let change = match operation {
                    AlterTableOp::AddColumn {
                        name,
                        data_type,
                        nullable,
                    } => {
                        let column = ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name,
                            version: 1,
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            state: DescriptorState::Public,
                            data_type,
                            nullable,
                        };

                        // Migrate existing data to include the new column (as NULL)
                        for mut row in self.scan_rows(tbl.id, &ctx.session_id)? {
                            let pk_str = row.first().map(render).unwrap_or_default();
                            let key = format!("{}:{}", tbl.id, pk_str);
                            row.push(Value::Null); // Append null for the new column
                            self.write_row(&ctx.session_id, key, serde_json::to_string(&row)?)?;
                        }

                        nodus_catalog::TableDescriptorChange::AddColumn {
                            table_id: tbl.id,
                            column,
                        }
                    }
                    AlterTableOp::DropColumn { name } => {
                        if let Some(col_idx) = tbl.columns.iter().position(|c| c.name == name) {
                            // Cannot drop primary key (assuming first column is PK for now)
                            if col_idx == 0 {
                                anyhow::bail!("Cannot drop primary key column");
                            }

                            // Migrate existing data to remove the column
                            for mut row in self.scan_rows(tbl.id, &ctx.session_id)? {
                                let pk_str = row.first().map(render).unwrap_or_default();
                                let key = format!("{}:{}", tbl.id, pk_str);
                                if col_idx < row.len() {
                                    row.remove(col_idx);
                                }
                                self.write_row(&ctx.session_id, key, serde_json::to_string(&row)?)?;
                            }
                        } else {
                            anyhow::bail!("Column {} not found", name);
                        }

                        nodus_catalog::TableDescriptorChange::DropColumn {
                            table_id: tbl.id,
                            column_name: name,
                        }
                    }
                    AlterTableOp::RenameColumn { old_name, new_name } => {
                        nodus_catalog::TableDescriptorChange::RenameColumn {
                            table_id: tbl.id,
                            old_name,
                            new_name,
                        }
                    }
                    AlterTableOp::RenameTable { new_name } => {
                        nodus_catalog::TableDescriptorChange::RenameTable {
                            table_id: tbl.id,
                            new_name,
                        }
                    }
                };
                self.catalog_writer.update_table_descriptor(change)?;
                Ok(QueryOutput::tag("ALTER TABLE"))
            }
            LogicalPlan::CreateIndex {
                name,
                table_name,
                columns,
                unique,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;

                let mut index_cols = Vec::new();
                for c in &columns {
                    if let Some(col) = tbl.columns.iter().find(|tc| tc.name == *c) {
                        index_cols.push(nodus_catalog::IndexColumn {
                            column_id: col.id,
                            descending: false,
                        });
                    } else {
                        anyhow::bail!("Column not found for index: {}", c);
                    }
                }

                let idx_type = if unique {
                    nodus_catalog::IndexType::Unique
                } else {
                    nodus_catalog::IndexType::LocalSecondary
                };

                let index = nodus_catalog::IndexDescriptor {
                    id: nodus_catalog::IndexId::new(),
                    name,
                    version: 1,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    state: DescriptorState::Public,
                    index_type: idx_type,
                    index_state: nodus_catalog::IndexState::Creating,
                    key_columns: index_cols,
                    include_columns: vec![],
                    unique,
                    global: false,
                    predicate: None,
                    expressions: vec![],
                };

                let change = nodus_catalog::TableDescriptorChange::AddIndex {
                    table_id: tbl.id,
                    index: index.clone(),
                };
                self.catalog_writer.update_table_descriptor(change)?;

                // Backfill existing rows into the new index
                let mut seen_values = std::collections::HashSet::new();
                for row in self.scan_rows(tbl.id, &ctx.session_id)? {
                    let pk_str = row.first().map(render).unwrap_or_default();
                    for kcol in &index.key_columns {
                        if let Some(pos) = tbl.columns.iter().position(|c| c.id == kcol.column_id) {
                            let index_val = row.get(pos).unwrap_or(&Value::Null);

                            if unique {
                                let val_str = render(index_val);
                                if val_str != "NULL" && !seen_values.insert(val_str) {
                                    // Set state to Failed/Dropping or just error out. We'd need to drop it, but we can just error for now.
                                    let _ = self.catalog_writer.update_index_state(
                                        tbl.id,
                                        index.id,
                                        nodus_catalog::IndexState::Dropping,
                                    );
                                    anyhow::bail!(
                                        "Unique constraint violation during index backfill for value: {:?}",
                                        index_val
                                    );
                                }
                            }

                            self.write_index_entry(&ctx.session_id, index.id, index_val, &pk_str)?;
                        }
                    }
                }

                self.catalog_writer.update_index_state(
                    tbl.id,
                    index.id,
                    nodus_catalog::IndexState::Ready,
                )?;
                Ok(QueryOutput::tag("CREATE INDEX"))
            }
            LogicalPlan::CreateRole { name } => {
                // Must be superuser or have create role privilege in a real system
                self.catalog_writer
                    .create_role(nodus_catalog::CreateRoleRequest {
                        id: nodus_catalog::PrincipalId::new(),
                        name: name.clone(),
                        principal_type: nodus_catalog::PrincipalType::Role,
                        database_id: None,
                    })?;
                Ok(QueryOutput::tag("CREATE ROLE"))
            }
            LogicalPlan::Grant {
                privilege,
                object_name,
                grantee,
            } => {
                let role = self.catalog_reader.get_principal_by_name(&grantee)?;
                let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                // Typically you need to own the table or have grant option
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                self.catalog_writer
                    .grant_privileges(nodus_catalog::GrantPrivilegesRequest {
                        id: nodus_catalog::GrantId::new(),
                        principal_id: role.id,
                        resource: ResourceRef::Table(tbl.id),
                        privilege: privilege.clone(),
                    })?;
                Ok(QueryOutput::tag("GRANT"))
            }
            LogicalPlan::Revoke {
                privilege,
                object_name,
                revokee,
            } => {
                let role = self.catalog_reader.get_principal_by_name(&revokee)?;
                let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                self.catalog_writer
                    .revoke_privileges(nodus_catalog::RevokePrivilegesRequest {
                        principal_id: role.id,
                        resource: ResourceRef::Table(tbl.id),
                        privilege: privilege.clone(),
                    })?;
                Ok(QueryOutput::tag("REVOKE"))
            }
            LogicalPlan::Begin => {
                let txn_record = self.txn.begin_txn()?;
                self.active_txns.write().unwrap().insert(
                    ctx.session_id.clone(),
                    ActiveTxn::new(txn_record.txn_id, txn_record.read_ts),
                );
                Ok(QueryOutput::tag("BEGIN"))
            }
            LogicalPlan::Commit => {
                if let Some(txn) = self.active_txns.write().unwrap().remove(&ctx.session_id) {
                    let commit_ts = self.txn.commit_txn(txn.txn_id)?;
                    self.kv.commit(txn.txn_id, commit_ts)?;
                }
                Ok(QueryOutput::tag("COMMIT"))
            }
            LogicalPlan::Rollback => {
                if let Some(txn) = self.active_txns.write().unwrap().remove(&ctx.session_id) {
                    self.txn.abort_txn(txn.txn_id)?;
                    self.kv.abort(txn.txn_id)?;
                }
                Ok(QueryOutput::tag("SAVEPOINT"))
            }
            LogicalPlan::Savepoint { name } => {
                let mut guard = self.active_txns.write().unwrap();
                let txn = guard.get_mut(&ctx.session_id).ok_or_else(|| {
                    anyhow::anyhow!("SAVEPOINT can only be used in transaction blocks")
                })?;
                txn.savepoints.push(SavepointState {
                    name,
                    write_log_len: txn.write_log.len(),
                    overlay: txn.overlay.clone(),
                });
                Ok(QueryOutput::tag("SAVEPOINT"))
            }
            LogicalPlan::RollbackToSavepoint { name } => {
                let (txn_id, affected, snapshot, keep_len, keep_savepoints) = {
                    let guard = self.active_txns.read().unwrap();
                    let txn = guard.get(&ctx.session_id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "ROLLBACK TO SAVEPOINT can only be used in transaction blocks"
                        )
                    })?;
                    let savepoint_idx = txn
                        .savepoints
                        .iter()
                        .rposition(|savepoint| savepoint.name.eq_ignore_ascii_case(&name))
                        .ok_or_else(|| anyhow::anyhow!("savepoint \"{}\" does not exist", name))?;
                    let savepoint = txn.savepoints[savepoint_idx].clone();
                    let affected = txn.write_log[savepoint.write_log_len..].to_vec();
                    (
                        txn.txn_id,
                        affected,
                        savepoint.overlay,
                        savepoint.write_log_len,
                        savepoint_idx + 1,
                    )
                };

                let mut unique_keys = affected;
                unique_keys.sort();
                unique_keys.dedup();
                for key in unique_keys {
                    let replacement = match snapshot.get(&key) {
                        Some(Some(value)) => IntentReplacement::Put(Bytes::from(value.clone())),
                        Some(None) => IntentReplacement::Delete,
                        None => IntentReplacement::Clear,
                    };
                    self.kv
                        .replace_intent(txn_id, Bytes::from(key), replacement)?;
                }

                let mut guard = self.active_txns.write().unwrap();
                if let Some(txn) = guard.get_mut(&ctx.session_id) {
                    txn.overlay = snapshot;
                    txn.write_log.truncate(keep_len);
                    txn.savepoints.truncate(keep_savepoints);
                }
                Ok(QueryOutput::tag("ROLLBACK"))
            }
            LogicalPlan::ReleaseSavepoint { name } => {
                let mut guard = self.active_txns.write().unwrap();
                let txn = guard.get_mut(&ctx.session_id).ok_or_else(|| {
                    anyhow::anyhow!("RELEASE SAVEPOINT can only be used in transaction blocks")
                })?;
                let savepoint_idx = txn
                    .savepoints
                    .iter()
                    .rposition(|savepoint| savepoint.name.eq_ignore_ascii_case(&name))
                    .ok_or_else(|| anyhow::anyhow!("savepoint \"{}\" does not exist", name))?;
                txn.savepoints.truncate(savepoint_idx);
                Ok(QueryOutput::tag("RELEASE"))
            }
            LogicalPlan::ShowVariable { variable } => {
                let value = if variable.eq_ignore_ascii_case("search_path") {
                    "public".to_string()
                } else {
                    String::new()
                };
                Ok(QueryOutput {
                    columns: vec![variable],
                    types: vec!["VARCHAR".to_string()],
                    rows: vec![Row {
                        values: vec![Value::Text(value)],
                    }],
                    tag: "SHOW".into(),
                })
            }
            LogicalPlan::SetVariable { variable, value: _ } => {
                // Acknowledging SET requests to support clients like JDBC
                Ok(QueryOutput::tag("SET"))
            }
            LogicalPlan::Noop { tag } => Ok(QueryOutput::tag(&tag)),
            LogicalPlan::SelectLiteral { values } => {
                let mut columns = Vec::new();
                let mut types = Vec::new();
                let mut row_values = Vec::new();

                for (alias, value) in values {
                    columns.push(alias);
                    types.push(match &value {
                        Value::Int(_) => "INTEGER".to_string(),
                        Value::Float(_) => "DOUBLE".to_string(),
                        Value::Bool(_) => "BOOLEAN".to_string(),
                        _ => "VARCHAR".to_string(),
                    });
                    row_values.push(value);
                }

                Ok(QueryOutput {
                    columns,
                    types,
                    rows: vec![Row { values: row_values }],
                    tag: "SELECT 1".into(),
                })
            }
            LogicalPlan::UnionAll { left, right } => {
                let mut left_out = self.execute_logical_inner(ctx, *left)?;
                let right_out = self.execute_logical_inner(ctx, *right)?;
                // Assume columns match in count.
                left_out.rows.extend(right_out.rows);
                Ok(left_out)
            }
        }
    }

    fn execute_physical_inner(
        &self,
        ctx: &ExecutionContext,
        plan: PhysicalPlan,
    ) -> Result<Vec<Row>> {
        // Retained for the point-get path used by lower layers/tests.
        match plan {
            PhysicalPlan::LocalPointGet { table_id, id } => {
                let read_ts = self.read_ts(&ctx.session_id);
                let key = format!("{}:{}", table_id, id);
                match self.kv.get(key.as_bytes(), read_ts)? {
                    Some(val) => {
                        let row: Vec<Value> = serde_json::from_slice(&val).unwrap_or_default();
                        Ok(vec![Row {
                            values: row.into_iter().collect(),
                        }])
                    }
                    None => Ok(vec![]),
                }
            }
            _ => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod phase1_tests;
#[cfg(test)]
mod phase2_tests;
#[cfg(test)]
mod phase3_tests;
#[cfg(test)]
mod tests;
