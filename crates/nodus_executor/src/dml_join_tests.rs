//! `UPDATE ... FROM`, `DELETE ... USING`, and `MERGE`, driven through SQL.

use super::*;
use nodus_audit::MemoryAuditSink;

/// An executor with an all-privileged session, and a function running one
/// SQL statement in it.
fn session() -> impl Fn(&str) -> Result<QueryOutput> {
    let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
    let admin = cat
        .create_role(nodus_catalog::CreateRoleRequest {
            id: nodus_catalog::PrincipalId::new(),
            name: "admin".into(),
            principal_type: nodus_catalog::PrincipalType::User,
            database_id: None,
        })
        .unwrap();
    cat.grant_privilege(nodus_catalog::GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: nodus_catalog::ResourceRef::System,
        privilege: "ALL".into(),
    })
    .unwrap();
    let ctx = ExecutionContext {
        session_id: "test".into(),
        principal_id: admin.id,
        active_roles: vec![],
        authz_catalog_version: 1,
    };
    move |sql: &str| {
        let mut statements = nodus_sql::parse_sql(sql)?;
        let plan = plan_statement(&statements.remove(0), &[])?;
        exec.execute_logical(&ctx, plan)
    }
}

/// The rows of `out`, rendered and sorted.
fn rows(out: &QueryOutput) -> Vec<String> {
    let mut rows: Vec<String> = out
        .rows
        .iter()
        .map(|r| r.values.iter().map(render).collect::<Vec<_>>().join("|"))
        .collect();
    rows.sort();
    rows
}

fn setup(sql: &impl Fn(&str) -> Result<QueryOutput>) {
    for statement in [
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT, n INT)",
        "CREATE TABLE s (id INT, w TEXT)",
        "INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
        "INSERT INTO s VALUES (1, 'x'), (3, 'z'), (4, 'q')",
    ] {
        sql(statement).unwrap();
    }
}

#[test]
fn update_from_writes_joined_values_and_returns_both_sides() {
    let sql = session();
    setup(&sql);
    let out = sql("UPDATE t SET v = s.w FROM s WHERE s.id = t.id RETURNING t.id, v, s.w").unwrap();
    assert_eq!(out.tag, "UPDATE 2");
    assert_eq!(out.columns, ["id", "v", "w"]);
    assert_eq!(rows(&out), ["1|x|x", "3|z|z"]);
    let out = sql("SELECT id, v FROM t ORDER BY id").unwrap();
    assert_eq!(rows(&out), ["1|x", "2|b", "3|z"]);
}

#[test]
fn update_from_rejects_ambiguous_and_hidden_names() {
    let sql = session();
    setup(&sql);
    let err = sql("UPDATE t SET n = 0 FROM s WHERE id = 1").unwrap_err();
    assert_eq!(err.to_string(), "column reference \"id\" is ambiguous");
    let err = sql("UPDATE t AS a SET n = 0 WHERE t.id = 1").unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid reference to FROM-clause entry for table \"t\""
    );
    let err = sql("UPDATE t SET n = 0 FROM t WHERE t.id = 1").unwrap_err();
    assert_eq!(err.to_string(), "table name \"t\" specified more than once");
}

#[test]
fn delete_using_removes_rows_that_join() {
    let sql = session();
    setup(&sql);
    let out = sql("DELETE FROM t USING s WHERE s.id = t.id AND s.w <> 'z' RETURNING *").unwrap();
    assert_eq!(out.tag, "DELETE 1");
    assert_eq!(rows(&out), ["1|a|10|1|x"]);
    let out = sql("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(rows(&out), ["2", "3"]);
}

#[test]
fn merge_applies_the_first_clause_that_holds_for_each_row() {
    let sql = session();
    setup(&sql);
    let out = sql("MERGE INTO t USING s ON t.id = s.id \
         WHEN MATCHED AND s.w = 'z' THEN DELETE \
         WHEN MATCHED THEN UPDATE SET v = s.w \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.w) \
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET n = t.n + 1")
    .unwrap();
    assert_eq!(out.tag, "MERGE 4");
    let out = sql("SELECT id, v, n FROM t").unwrap();
    assert_eq!(rows(&out), ["1|x|10", "2|b|21", "4|q|"]);
}

#[test]
fn merge_changes_a_row_at_most_once() {
    let sql = session();
    setup(&sql);
    sql("INSERT INTO s VALUES (1, 'y')").unwrap();
    let err = sql("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.w")
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "MERGE command cannot affect row a second time"
    );
    // Doing nothing to it twice is fine.
    let out = sql("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DO NOTHING").unwrap();
    assert_eq!(out.tag, "MERGE 0");
}

#[test]
fn merge_clauses_see_only_their_side_of_an_unmatched_row() {
    let sql = session();
    setup(&sql);
    let err =
        sql("MERGE INTO t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (t.id)")
            .unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid reference to FROM-clause entry for table \"t\""
    );
    // An unqualified name resolves to the side the clause sees.
    let out = sql("MERGE INTO t USING (SELECT 7 AS id) src ON t.id = src.id \
         WHEN NOT MATCHED THEN INSERT (id, n) VALUES (id, id * 2) RETURNING t.id, t.n")
    .unwrap();
    assert_eq!(rows(&out), ["7|14"]);
}

#[test]
fn subquery_without_from_reads_the_enclosing_row() {
    let sql = session();
    setup(&sql);
    let out = sql("SELECT id, (SELECT n + 1) FROM t").unwrap();
    assert_eq!(rows(&out), ["1|11", "2|21", "3|31"]);
    let out = sql("SELECT t.id, l.d FROM t, LATERAL (SELECT t.n * 2 AS d) l").unwrap();
    assert_eq!(rows(&out), ["1|20", "2|40", "3|60"]);
    let err = sql("SELECT (SELECT nosuch) FROM t").unwrap_err();
    assert_eq!(err.to_string(), "column \"nosuch\" does not exist");
}
