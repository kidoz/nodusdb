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
use nodus_storage_api::{KeyRange, KvEngine, Timestamp, TxnId};
use nodus_txn::TxnManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A column definition parsed from `CREATE TABLE`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

/// A typed cell value. Rows are stored as `Vec<Value>` in table-column order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
}

/// Logical column type derived from a SQL type name.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ColumnType {
    Int,
    Float,
    Bool,
    Text,
}

fn column_type(data_type: &str) -> ColumnType {
    let t = data_type.to_uppercase();
    if t.contains("INT") || t.contains("SERIAL") {
        ColumnType::Int
    } else if t.contains("FLOAT")
        || t.contains("DOUBLE")
        || t.contains("REAL")
        || t.contains("NUMERIC")
        || t.contains("DECIMAL")
    {
        ColumnType::Float
    } else if t.contains("BOOL") {
        ColumnType::Bool
    } else {
        ColumnType::Text
    }
}

/// Coerces a literal string into a typed value for the given column type.
/// Empty strings and unparseable numerics become `Null`.
fn coerce(raw: &str, ty: ColumnType) -> Value {
    if raw.is_empty() {
        return Value::Null;
    }
    match ty {
        ColumnType::Int => raw.parse::<i64>().map(Value::Int).unwrap_or(Value::Null),
        ColumnType::Float => raw.parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
        ColumnType::Bool => raw.parse::<bool>().map(Value::Bool).unwrap_or(Value::Null),
        ColumnType::Text => Value::Text(raw.to_string()),
    }
}

fn render(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    Insert {
        table_name: String,
        /// Target column names; empty means positional (table order).
        columns: Vec<String>,
        values: Vec<String>,
    },
    Select {
        table_name: String,
        /// Projected column names; empty means all columns (`SELECT *`).
        projection: Vec<String>,
        /// Optional equality filter `(column, value)`.
        filter: Option<(String, String)>,
    },
    Begin,
    Commit,
    Rollback,
    ShowVariable {
        variable: String,
    },
    SelectLiteral {
        value: String,
    },
}

/// Result of executing a statement: a tag for non-row commands, and column
/// names + rows for queries.
#[derive(Debug, Default)]
pub struct QueryOutput {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub tag: String,
}

impl QueryOutput {
    fn tag(tag: &str) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            tag: tag.to_string(),
        }
    }
}

fn expr_to_string(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::{Expr, Value};
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()),
        Expr::Value(Value::Number(n, _)) => Some(n.clone()),
        Expr::Value(Value::Boolean(b)) => Some(b.to_string()),
        Expr::Value(Value::Null) => Some(String::new()),
        Expr::Identifier(id) => Some(id.value.clone()),
        _ => None,
    }
}

pub fn plan_statement(stmt: &sqlparser::ast::Statement) -> Result<LogicalPlan> {
    use sqlparser::ast::*;
    match stmt {
        Statement::CreateTable { name, columns, .. } => {
            let cols = columns
                .iter()
                .map(|c| crate::ColumnDef {
                    name: c.name.value.clone(),
                    data_type: c.data_type.to_string(),
                })
                .collect();
            Ok(LogicalPlan::CreateTable {
                name: name.to_string(),
                columns: cols,
            })
        }
        Statement::Insert {
            table_name,
            columns,
            source,
            ..
        } => {
            let cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
            let mut values = Vec::new();
            if let Some(query) = source {
                if let SetExpr::Values(vs) = &*query.body {
                    if let Some(row) = vs.rows.first() {
                        for e in row {
                            values.push(expr_to_string(e).unwrap_or_default());
                        }
                    }
                }
            }
            Ok(LogicalPlan::Insert {
                table_name: table_name.to_string(),
                columns: cols,
                values,
            })
        }
        Statement::Query(query) => plan_query(query),
        Statement::StartTransaction { .. } => Ok(LogicalPlan::Begin),
        Statement::Commit { .. } => Ok(LogicalPlan::Commit),
        Statement::Rollback { .. } => Ok(LogicalPlan::Rollback),
        Statement::ShowVariable { variable } => {
            let var_name = variable
                .iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            Ok(LogicalPlan::ShowVariable { variable: var_name })
        }
        _ => anyhow::bail!("Unsupported SQL statement: {:?}", stmt),
    }
}

fn plan_query(query: &sqlparser::ast::Query) -> Result<LogicalPlan> {
    use sqlparser::ast::*;
    let SetExpr::Select(select) = &*query.body else {
        anyhow::bail!("Unsupported query body");
    };

    // FROM-less single-item projections are scalar/literal selects.
    if select.from.is_empty() && select.projection.len() == 1 {
        return match &select.projection[0] {
            SelectItem::UnnamedExpr(Expr::Value(Value::Number(n, _))) => {
                Ok(LogicalPlan::SelectLiteral { value: n.clone() })
            }
            SelectItem::UnnamedExpr(Expr::Value(Value::SingleQuotedString(s))) => {
                Ok(LogicalPlan::SelectLiteral { value: s.clone() })
            }
            SelectItem::UnnamedExpr(Expr::Function(func)) => Ok(LogicalPlan::SelectLiteral {
                value: func.name.to_string(),
            }),
            _ => anyhow::bail!("Unsupported scalar select"),
        };
    }

    if select.from.is_empty() {
        anyhow::bail!("SELECT without FROM");
    }
    let table_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.to_string(),
        other => anyhow::bail!("Unsupported FROM relation: {:?}", other),
    };

    // Projection: `*` -> empty (all); otherwise plain column identifiers.
    let mut projection = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                projection.clear();
                break;
            }
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => projection.push(id.value.clone()),
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(id),
                ..
            } => projection.push(id.value.clone()),
            other => anyhow::bail!("Unsupported projection item: {:?}", other),
        }
    }

    // Optional `WHERE <col> = <literal>` filter.
    let mut filter = None;
    if let Some(Expr::BinaryOp { left, op, right }) = &select.selection {
        if *op == BinaryOperator::Eq {
            if let (Expr::Identifier(col), Some(val)) = (&**left, expr_to_string(right)) {
                filter = Some((col.value.clone(), val));
            }
        }
    }

    Ok(LogicalPlan::Select {
        table_name,
        projection,
        filter,
    })
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

pub struct ExecutionContext {
    pub session_id: String,
    /// Authenticated principal making the request; used for authorization.
    pub principal_id: PrincipalId,
    pub active_roles: Vec<RoleId>,
    pub authz_catalog_version: u64,
}

#[derive(Debug)]
pub struct Row {
    pub columns: Vec<String>,
}

pub trait Executor: Send + Sync {
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<QueryOutput>;
    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>>;
}

// MVP implementation mapping to required interfaces
#[allow(dead_code)]
pub struct MemExecutor {
    catalog_reader: Arc<dyn CatalogReader>,
    catalog_writer: Arc<dyn CatalogWriter>,
    authz: Arc<dyn AuthzEngine>,
    audit: Arc<dyn AuditSink>,
    kv: Arc<dyn KvEngine>,
    txn: Arc<dyn TxnManager>,
    // Hack for MVP: track active transaction in memory for the single session
    active_txn: std::sync::RwLock<Option<(TxnId, Timestamp)>>,
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
        // Bootstrapping the default database and schema
        let _db = catalog_writer.create_database(nodus_catalog::CreateDatabaseRequest {
            name: "default".into(),
            owner_role_id: None,
        });
        if let Ok(db) = catalog_reader.get_database("default") {
            let _sch = catalog_writer.create_schema(nodus_catalog::CreateSchemaRequest {
                database_id: db.id,
                name: "public".into(),
                owner_role_id: None,
                managed_access: false,
            });
        }

        Self {
            catalog_reader,
            catalog_writer,
            authz,
            audit,
            kv,
            txn,
            active_txn: std::sync::RwLock::new(None),
        }
    }

    /// Builds an executor over fresh in-memory components and returns it
    /// together with the shared catalog, so callers (e.g. the server) can seed
    /// principals/grants and an authenticator against the same catalog. Audit
    /// events are written to `audit`.
    pub fn shared(audit: Arc<dyn AuditSink>) -> (Arc<MemExecutor>, Arc<MemoryCatalog>) {
        let cat = Arc::new(MemoryCatalog::new());
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

    fn read_ts(&self) -> Timestamp {
        match *self.active_txn.read().unwrap() {
            Some((_, ts)) => ts,
            None => u64::MAX, // read latest when no active txn
        }
    }

    /// Scans all visible rows of a table, decoding each into typed values.
    fn scan_rows(&self, table_id: TableId) -> Result<Vec<Vec<Value>>> {
        let read_ts = self.read_ts();
        let start = Bytes::from(format!("{}:", table_id));
        let end = Bytes::from(format!("{};", table_id));
        let mut rows = Vec::new();
        for pair in self.kv.scan(KeyRange { start, end }, read_ts)? {
            let pair = pair?;
            rows.push(serde_json::from_slice::<Vec<Value>>(&pair.value)?);
        }
        Ok(rows)
    }

    /// Writes a row value at `key`, using the active txn or an auto-commit txn.
    fn write_row(&self, key: String, value: String) -> Result<()> {
        let active = *self.active_txn.read().unwrap();
        let (txn_id, auto) = match active {
            Some((tid, _)) => (tid, false),
            None => (self.txn.begin_txn()?.txn_id, true),
        };
        self.kv
            .write_intent(txn_id, Bytes::from(key), Bytes::from(value))?;
        if auto {
            let commit_ts = self.txn.commit_txn(txn_id)?;
            self.kv.commit(txn_id, commit_ts)?;
        }
        Ok(())
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
            action: action.as_privilege().to_string(),
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
        match plan {
            LogicalPlan::CreateTable { name, columns } => {
                let db = self.catalog_reader.get_database("default")?;
                let sch = self.catalog_reader.get_schema("default", "public")?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;
                let descriptors = columns
                    .iter()
                    .map(|c| ColumnDescriptor {
                        id: nodus_catalog::ColumnId::new(),
                        name: c.name.clone(),
                        version: 1,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        state: DescriptorState::Public,
                        data_type: c.data_type.clone(),
                        nullable: true,
                    })
                    .collect();
                self.catalog_writer.create_table(CreateTableRequest {
                    database_id: db.id,
                    schema_id: sch.id,
                    name: name.clone(),
                    columns: descriptors,
                })?;
                Ok(QueryOutput::tag("CREATE TABLE"))
            }
            LogicalPlan::Insert {
                table_name,
                columns,
                values,
            } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
                self.authorize(ctx, Action::Insert, ResourceRef::Table(tbl.id))?;

                let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();
                // Build the raw string row in table-column order...
                let mut raw = vec![String::new(); col_names.len()];
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
                // ...then coerce each cell to its column's type.
                let row: Vec<Value> = tbl
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| coerce(&raw[i], column_type(&c.data_type)))
                    .collect();
                // Primary key = first column's rendered value.
                let pk = row.first().map(render).unwrap_or_default();
                let key = format!("{}:{}", tbl.id, pk);
                let encoded = serde_json::to_string(&row)?;
                self.write_row(key, encoded)?;
                Ok(QueryOutput::tag("INSERT 0 1"))
            }
            LogicalPlan::Select {
                table_name,
                projection,
                filter,
            } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
                self.authorize(ctx, Action::Select, ResourceRef::Table(tbl.id))?;

                let col_names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
                let mut stored_rows = self.scan_rows(tbl.id)?;

                // Apply the optional equality filter (typed comparison).
                if let Some((fcol, fval)) = &filter {
                    if let Some(idx) = col_names.iter().position(|c| c == fcol) {
                        let target = coerce(fval, column_type(&tbl.columns[idx].data_type));
                        stored_rows.retain(|r| r.get(idx).map(|v| *v == target).unwrap_or(false));
                    } else {
                        stored_rows.clear();
                    }
                }

                // Resolve projection (empty = all columns).
                let out_cols: Vec<String> = if projection.is_empty() {
                    col_names.clone()
                } else {
                    projection.clone()
                };
                let indices: Vec<Option<usize>> = out_cols
                    .iter()
                    .map(|c| col_names.iter().position(|tc| tc == c))
                    .collect();

                let rows = stored_rows
                    .into_iter()
                    .map(|r| Row {
                        columns: indices
                            .iter()
                            .map(|i| i.and_then(|i| r.get(i)).map(render).unwrap_or_default())
                            .collect(),
                    })
                    .collect::<Vec<_>>();

                let tag = format!("SELECT {}", rows.len());
                Ok(QueryOutput {
                    columns: out_cols,
                    rows,
                    tag,
                })
            }
            LogicalPlan::Begin => {
                let txn_record = self.txn.begin_txn()?;
                *self.active_txn.write().unwrap() = Some((txn_record.txn_id, txn_record.read_ts));
                Ok(QueryOutput::tag("BEGIN"))
            }
            LogicalPlan::Commit => {
                if let Some((txn_id, _)) = *self.active_txn.read().unwrap() {
                    let commit_ts = self.txn.commit_txn(txn_id)?;
                    self.kv.commit(txn_id, commit_ts)?;
                }
                *self.active_txn.write().unwrap() = None;
                Ok(QueryOutput::tag("COMMIT"))
            }
            LogicalPlan::Rollback => {
                if let Some((txn_id, _)) = *self.active_txn.read().unwrap() {
                    self.txn.abort_txn(txn_id)?;
                    self.kv.abort(txn_id)?;
                }
                *self.active_txn.write().unwrap() = None;
                Ok(QueryOutput::tag("ROLLBACK"))
            }
            LogicalPlan::ShowVariable { variable } => {
                let value = if variable.eq_ignore_ascii_case("search_path") {
                    "public".to_string()
                } else {
                    String::new()
                };
                Ok(QueryOutput {
                    columns: vec![variable],
                    rows: vec![Row {
                        columns: vec![value],
                    }],
                    tag: "SHOW".into(),
                })
            }
            LogicalPlan::SelectLiteral { value } => {
                let rendered = if value.eq_ignore_ascii_case("version") {
                    "PostgreSQL 16.0 (NodusDB)".to_string()
                } else {
                    value
                };
                Ok(QueryOutput {
                    columns: vec!["?column?".into()],
                    rows: vec![Row {
                        columns: vec![rendered],
                    }],
                    tag: "SELECT 1".into(),
                })
            }
        }
    }

    fn execute_physical(&self, _ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>> {
        // Retained for the point-get path used by lower layers/tests.
        match plan {
            PhysicalPlan::LocalPointGet { table_id, id } => {
                let read_ts = self.read_ts();
                let key = format!("{}:{}", table_id, id);
                match self.kv.get(key.as_bytes(), read_ts)? {
                    Some(val) => {
                        let row: Vec<Value> = serde_json::from_slice(&val).unwrap_or_default();
                        Ok(vec![Row {
                            columns: row.iter().map(render).collect(),
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
mod tests {
    use super::*;
    use nodus_audit::{AuditQuery, AuditQueryable, MemoryAuditSink};
    use nodus_catalog::{CreateRoleRequest, GrantPrivilegeRequest, PrincipalType};

    fn ctx_for(principal: PrincipalId) -> ExecutionContext {
        ExecutionContext {
            session_id: "test".to_string(),
            principal_id: principal,
            active_roles: vec![],
            authz_catalog_version: 1,
        }
    }

    fn cols(names: &[(&str, &str)]) -> Vec<ColumnDef> {
        names
            .iter()
            .map(|(n, t)| ColumnDef {
                name: n.to_string(),
                data_type: t.to_string(),
            })
            .collect()
    }

    #[test]
    fn create_table_denied_then_allowed_by_grant() {
        let audit = Arc::new(MemoryAuditSink::new());
        let (exec, cat) = MemExecutor::shared(audit.clone());
        let user = cat
            .create_role(CreateRoleRequest {
                name: "bob".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        let ctx = ctx_for(user.id);
        let plan = || LogicalPlan::CreateTable {
            name: "t1".into(),
            columns: cols(&[("id", "INT"), ("name", "TEXT")]),
        };

        assert!(exec.execute_logical(&ctx, plan()).is_err());

        let sch = cat.get_schema("default", "public").unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            principal_id: user.id,
            resource: ResourceRef::Schema(sch.id),
            privilege: "CREATE_TABLE".into(),
        })
        .unwrap();
        assert!(exec.execute_logical(&ctx, plan()).is_ok());

        assert_eq!(
            audit
                .query(&AuditQuery {
                    result: Some("Denied".into()),
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn create_insert_select_round_trip() {
        // Superuser so authz passes for all actions.
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(CreateRoleRequest {
                name: "admin".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = ctx_for(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "books".into(),
                columns: cols(&[("id", "INT"), ("title", "TEXT"), ("author", "TEXT")]),
            },
        )
        .unwrap();

        for (id, title, author) in [("1", "Dune", "Herbert"), ("2", "Foundation", "Asimov")] {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "books".into(),
                    columns: vec!["id".into(), "title".into(), "author".into()],
                    values: vec![id.into(), title.into(), author.into()],
                },
            )
            .unwrap();
        }

        // SELECT * returns all rows with all columns.
        let all = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "books".into(),
                    projection: vec![],
                    filter: None,
                },
            )
            .unwrap();
        assert_eq!(all.columns, vec!["id", "title", "author"]);
        assert_eq!(all.rows.len(), 2);

        // Projection + filter.
        let one = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "books".into(),
                    projection: vec!["title".into(), "author".into()],
                    filter: Some(("id".into(), "2".into())),
                },
            )
            .unwrap();
        assert_eq!(one.columns, vec!["title", "author"]);
        assert_eq!(one.rows.len(), 1);
        assert_eq!(one.rows[0].columns, vec!["Foundation", "Asimov"]);
    }

    #[test]
    fn typed_values_round_trip_and_filter_by_int() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(CreateRoleRequest {
                name: "admin".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = ctx_for(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "items".into(),
                columns: cols(&[("id", "INT"), ("name", "TEXT"), ("active", "BOOL")]),
            },
        )
        .unwrap();
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "items".into(),
                columns: vec!["id".into(), "name".into(), "active".into()],
                values: vec!["7".into(), "widget".into(), "true".into()],
            },
        )
        .unwrap();

        // Filter on an INT column coerces the literal numerically.
        let out = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "items".into(),
                    projection: vec![],
                    filter: Some(("id".into(), "7".into())),
                },
            )
            .unwrap();
        assert_eq!(out.rows.len(), 1);
        // Int renders without quotes, bool as true/false.
        assert_eq!(out.rows[0].columns, vec!["7", "widget", "true"]);
    }

    #[test]
    fn run_gc_is_safe_with_no_active_txns() {
        let exec = MemExecutor::default();
        assert_eq!(exec.run_gc().unwrap(), 0);
    }
}
