//! Tests for LIMIT/OFFSET push-down into the table scan: a bounded query must
//! consume only a bounded prefix of the scan, and still return exactly the rows
//! the full pipeline would.

use super::*;
use bytes::Bytes;
use nodus_audit::MemoryAuditSink;
use nodus_catalog::{
    CreateDatabaseRequest, CreateRoleRequest, CreateSchemaRequest, GrantPrivilegeRequest,
    MemoryCatalog, PrincipalType, SchemaId,
};
use nodus_storage_api::{
    IntentReplacement, KeyRange, KvEngine, KvPair, KvResult, Timestamp, TxnId,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Wraps a KV engine and counts how many pairs are *pulled* from each scan
/// iterator, so a test can assert the executor consumed only a bounded prefix.
struct CountingKv {
    inner: Arc<dyn KvEngine>,
    scanned: Arc<AtomicUsize>,
}

struct CountingIter {
    inner: Box<dyn Iterator<Item = Result<KvPair>> + Send>,
    scanned: Arc<AtomicUsize>,
}

impl Iterator for CountingIter {
    type Item = Result<KvPair>;
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next();
        if next.is_some() {
            self.scanned.fetch_add(1, Ordering::SeqCst);
        }
        next
    }
}

impl KvEngine for CountingKv {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        self.inner.get(key, read_ts)
    }
    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        let inner = self.inner.scan(range, read_ts)?;
        Ok(Box::new(CountingIter {
            inner,
            scanned: self.scanned.clone(),
        }))
    }
    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        self.inner.write_intent(txn_id, key, value)
    }
    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> KvResult<()> {
        self.inner.delete_intent(txn_id, key)
    }
    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()> {
        self.inner.replace_intent(txn_id, key, replacement)
    }
    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> KvResult<()> {
        self.inner.commit(txn_id, commit_ts)
    }
    fn abort(&self, txn_id: TxnId) -> KvResult<()> {
        self.inner.abort(txn_id)
    }
    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        self.inner.garbage_collect(watermark)
    }
    fn flush(&self) -> Result<()> {
        self.inner.flush()
    }
}

fn ctx_for(principal: PrincipalId) -> ExecutionContext {
    ExecutionContext {
        session_id: "stream-test".to_string(),
        principal_id: principal,
        active_roles: vec![],
        authz_catalog_version: 1,
    }
}

/// Builds an executor over a counting KV engine, with a superuser principal and a
/// `t(id INT, name TEXT)` table holding `n` rows.
fn exec_with_rows(n: i64) -> (Arc<MemExecutor>, Arc<AtomicUsize>, ExecutionContext) {
    let cat = Arc::new(MemoryCatalog::new());
    let db = cat
        .create_database(CreateDatabaseRequest {
            id: nodus_catalog::DatabaseId::new(),
            name: "default".into(),
            owner_role_id: None,
        })
        .unwrap();
    cat.create_schema(CreateSchemaRequest {
        id: SchemaId::new(),
        database_id: db.id,
        name: "public".into(),
        owner_role_id: None,
        managed_access: false,
    })
    .unwrap();

    let scanned = Arc::new(AtomicUsize::new(0));
    let kv = Arc::new(CountingKv {
        inner: Arc::new(nodus_storage_mem::MemKvEngine::new()),
        scanned: scanned.clone(),
    });
    let txn = Arc::new(nodus_txn::MemTxnManager::new());
    let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(cat.clone()));
    let exec = Arc::new(MemExecutor::new(
        cat.clone(),
        cat.clone(),
        authz,
        Arc::new(MemoryAuditSink::new()),
        kv,
        txn,
    ));

    let admin = cat
        .create_role(CreateRoleRequest {
            id: nodus_catalog::PrincipalId::new(),
            name: "admin".into(),
            principal_type: PrincipalType::User,
            database_id: None,
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),
    })
    .unwrap();
    let ctx = ctx_for(admin.id);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "t".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: "INT".into(),
                    nullable: false,
                    unique: false,
                    primary: true,
                },
                ColumnDef {
                    name: "name".into(),
                    data_type: "TEXT".into(),
                    nullable: true,
                    unique: false,
                    primary: false,
                },
            ],
        },
    )
    .unwrap();

    for i in 0..n {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "t".into(),
                columns: vec!["id".into(), "name".into()],
                values_list: vec![vec![Value::Int(i), Value::Text(format!("n{i}"))]],
                returning: vec![],
            },
        )
        .unwrap();
    }

    (exec, scanned, ctx)
}

fn select_plan(
    limit: Option<usize>,
    offset: Option<usize>,
    filter: Option<FilterExpr>,
) -> LogicalPlan {
    LogicalPlan::Select {
        ctes: vec![],
        table_alias: None,
        group_by: vec![],
        table_name: "t".into(),
        joins: vec![],
        projection: vec![],
        filter,
        having: None,
        order_by: vec![],
        limit,
        offset,
        distinct: false,
    }
}

fn rendered(out: &QueryOutput) -> Vec<Vec<String>> {
    out.rows
        .iter()
        .map(|r| r.values.iter().map(render).collect())
        .collect()
}

/// A [`RowSink`] that records the schema and every row (rendered to strings).
#[derive(Default)]
struct CollectSink {
    columns: Vec<String>,
    types: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl RowSink for CollectSink {
    fn schema(&mut self, columns: Vec<String>, types: Vec<String>) {
        self.columns = columns;
        self.types = types;
    }
    fn row(&mut self, row: Row) -> anyhow::Result<()> {
        self.rows.push(row.values.iter().map(render).collect());
        Ok(())
    }
}

/// A [`RowSink`] that aborts (returns `Err`) once it has accepted `limit` rows,
/// modelling a client that disconnects mid-stream.
struct AbortAfter {
    limit: usize,
    count: usize,
}

impl RowSink for AbortAfter {
    fn schema(&mut self, _columns: Vec<String>, _types: Vec<String>) {}
    fn row(&mut self, _row: Row) -> anyhow::Result<()> {
        self.count += 1;
        if self.count > self.limit {
            anyhow::bail!("consumer gone");
        }
        Ok(())
    }
}

#[test]
fn streaming_select_star_matches_the_full_path() {
    let (exec, _scanned, ctx) = exec_with_rows(20);

    let full = exec
        .execute_logical(&ctx, select_plan(None, None, None))
        .unwrap();
    let mut sink = CollectSink::default();
    let tag = exec
        .execute_streaming(&ctx, select_plan(None, None, None), &mut sink)
        .unwrap();

    assert_eq!(sink.columns, full.columns);
    assert_eq!(sink.types, full.types);
    assert_eq!(sink.rows, rendered(&full));
    assert_eq!(tag, "SELECT 20");
}

#[test]
fn streaming_projects_columns_with_where_and_limit() {
    let (exec, _scanned, ctx) = exec_with_rows(30);

    // SELECT name FROM t WHERE name = 'n7' LIMIT 5 — column projection + filter.
    let filter = Some(FilterExpr::Predicate(Predicate {
        left: "name".into(),
        op: CompareOp::Eq,
        right: Operand::Literal(Value::Text("n7".into())),
    }));
    let plan = LogicalPlan::Select {
        ctes: vec![],
        table_alias: None,
        group_by: vec![],
        table_name: "t".into(),
        joins: vec![],
        projection: vec![ProjectionItem::Column("name".into())],
        filter,
        having: None,
        order_by: vec![],
        limit: Some(5),
        offset: None,
        distinct: false,
    };

    let full = exec.execute_logical(&ctx, plan.clone()).unwrap();
    let mut sink = CollectSink::default();
    exec.execute_streaming(&ctx, plan, &mut sink).unwrap();

    assert_eq!(sink.columns, vec!["name".to_string()]);
    assert_eq!(sink.rows, rendered(&full));
    assert_eq!(sink.rows, vec![vec!["n7".to_string()]]);
}

#[test]
fn streaming_stops_scanning_when_the_sink_aborts() {
    let (exec, scanned, ctx) = exec_with_rows(50);

    // The sink accepts 3 rows then errors; the scan must stop right after,
    // never reading the remaining ~47 rows.
    scanned.store(0, Ordering::SeqCst);
    let mut sink = AbortAfter { limit: 3, count: 0 };
    let result = exec.execute_streaming(&ctx, select_plan(None, None, None), &mut sink);
    assert!(result.is_err(), "the aborted sink should surface an error");
    assert!(
        scanned.load(Ordering::SeqCst) <= 4,
        "scan should stop within one row of the abort, read {}",
        scanned.load(Ordering::SeqCst)
    );
}

#[test]
fn streaming_falls_back_for_non_streamable_shapes() {
    let (exec, _scanned, ctx) = exec_with_rows(10);

    // ORDER BY needs the full input, so this takes the fallback path; the result
    // must still be correct (and sorted).
    let plan = LogicalPlan::Select {
        ctes: vec![],
        table_alias: None,
        group_by: vec![],
        table_name: "t".into(),
        joins: vec![],
        projection: vec![],
        filter: None,
        having: None,
        order_by: vec![("id".into(), false)], // DESC
        limit: Some(3),
        offset: None,
        distinct: false,
    };

    let full = exec.execute_logical(&ctx, plan.clone()).unwrap();
    let mut sink = CollectSink::default();
    exec.execute_streaming(&ctx, plan, &mut sink).unwrap();

    assert_eq!(sink.rows, rendered(&full));
    assert_eq!(sink.rows.len(), 3);
    // DESC: first id is the largest (9).
    assert_eq!(sink.rows[0][0], "9");
}

#[test]
fn limit_pushdown_scans_only_the_capped_prefix() {
    let (exec, scanned, ctx) = exec_with_rows(50);

    // LIMIT 5: the scan must yield only 5 rows, not all 50.
    scanned.store(0, Ordering::SeqCst);
    let out = exec
        .execute_logical(&ctx, select_plan(Some(5), None, None))
        .unwrap();
    assert_eq!(out.rows.len(), 5);
    assert_eq!(
        scanned.load(Ordering::SeqCst),
        5,
        "LIMIT 5 should consume exactly 5 scanned rows"
    );

    // LIMIT 5 OFFSET 10: cap is offset + limit = 15.
    scanned.store(0, Ordering::SeqCst);
    let out = exec
        .execute_logical(&ctx, select_plan(Some(5), Some(10), None))
        .unwrap();
    assert_eq!(out.rows.len(), 5);
    assert_eq!(scanned.load(Ordering::SeqCst), 15);

    // No LIMIT: the whole table is scanned (baseline contrast).
    scanned.store(0, Ordering::SeqCst);
    let out = exec
        .execute_logical(&ctx, select_plan(None, None, None))
        .unwrap();
    assert_eq!(out.rows.len(), 50);
    assert_eq!(scanned.load(Ordering::SeqCst), 50);
}

#[test]
fn pushdown_returns_the_same_rows_as_the_full_pipeline() {
    let (exec, _scanned, ctx) = exec_with_rows(40);

    let full = exec
        .execute_logical(&ctx, select_plan(None, None, None))
        .unwrap();
    let full = rendered(&full);

    for (limit, offset) in [(5usize, 0usize), (5, 10), (1, 39), (10, 35)] {
        let out = exec
            .execute_logical(&ctx, select_plan(Some(limit), Some(offset), None))
            .unwrap();
        let expected: Vec<Vec<String>> = full.iter().skip(offset).take(limit).cloned().collect();
        assert_eq!(
            rendered(&out),
            expected,
            "LIMIT {limit} OFFSET {offset} must match the full pipeline's slice"
        );
    }
}

#[test]
fn a_where_filter_disables_pushdown() {
    let (exec, scanned, ctx) = exec_with_rows(50);

    // A WHERE clause could remove rows, so the cap can't be applied pre-filter:
    // the scan must read the whole table even with a LIMIT.
    let filter = Some(FilterExpr::Predicate(Predicate {
        left: "name".into(),
        op: CompareOp::Eq,
        right: Operand::Literal(Value::Text("n7".into())),
    }));
    scanned.store(0, Ordering::SeqCst);
    let out = exec
        .execute_logical(&ctx, select_plan(Some(5), None, filter))
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(
        scanned.load(Ordering::SeqCst),
        50,
        "a filtered LIMIT must not cap the scan"
    );
}
