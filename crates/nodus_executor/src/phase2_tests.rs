use super::{Row, render};

fn render_row(row: &Row) -> Vec<String> {
    row.values.iter().map(render).collect()
}

use super::*;
use crate::tests::cols;
use nodus_audit::MemoryAuditSink;

fn test_ctx(admin_id: PrincipalId) -> ExecutionContext {
    ExecutionContext {
        session_id: "test".into(),
        principal_id: admin_id,
        active_roles: vec![],
        authz_catalog_version: 1,
    }
}

#[test]
fn test_group_by_aggregates() {
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
    let ctx = test_ctx(admin.id);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "sales".into(),
            columns: cols(&[("id", "int"), ("category", "text"), ("amount", "int")]),
        },
    )
    .unwrap();

    let insert = |id: &str, cat: &str, amt: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "sales".into(),
                columns: vec![],
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(cat.into()),
                    Value::Text(amt.into()),
                ]],
                returning: vec![],
            },
        )
        .unwrap();
    };

    insert("1", "A", "10");
    insert("2", "A", "20");
    insert("3", "B", "15");
    insert("4", "C", "5");
    insert("5", "C", "5");
    insert("6", "C", "5");

    let read = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        let out = exec.execute_logical(&ctx, plan).unwrap();

        // To ignore unpredictable hashmap/btree iteration order of groups, we'll sort the output strings.
        let mut res: Vec<String> = out
            .rows
            .into_iter()
            .map(|r| render_row(&r).join(","))
            .collect();
        res.sort();
        res
    };

    // 1. Group By with COUNT and SUM
    let p1 = read("SELECT category, COUNT(id), SUM(amount) FROM sales GROUP BY category");
    assert_eq!(p1, vec!["A,2,30", "B,1,15", "C,3,15",]);

    // 2. MIN / MAX
    let p2 = read("SELECT category, MIN(amount), MAX(amount) FROM sales GROUP BY category");
    assert_eq!(p2, vec!["A,10,20", "B,15,15", "C,5,5",]);

    // 3. Scalar Aggregation without GROUP BY
    let p3 = read("SELECT COUNT(*), SUM(amount), MAX(amount) FROM sales");
    assert_eq!(p3, vec!["6,60,20"]);

    // 4. Scalar empty aggregation
    // Delete all rows
    exec.execute_logical(
        &ctx,
        LogicalPlan::Delete {
            table_name: "sales".into(),
            filter: None,
            returning: vec![],
        },
    )
    .unwrap();

    let p4 = read("SELECT COUNT(*) FROM sales");
    assert_eq!(p4, vec!["0"]);
}

#[test]
fn test_scalar_functions() {
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
    let ctx = test_ctx(admin.id);

    let run = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        exec.execute_logical(&ctx, plan).unwrap()
    };

    run("CREATE TABLE t (id INT, name TEXT)");
    run("INSERT INTO t (id, name) VALUES (1, 'Alice')");

    // Column args resolve per row; string/numeric literal args (e.g. SUBSTR
    // start/len, ROUND digits) are now captured by the planner.
    let out = run(
        "SELECT UPPER(name), LOWER(name), LENGTH(name), SUBSTR(name, 1, 3), \
             COALESCE(name, 'x'), CONCAT(name, '!'), REPLACE(name, 'lic', 'LIC'), \
             ROUND(12.345, 1) FROM t",
    );
    assert_eq!(out.rows.len(), 1);
    let row = render_row(&out.rows[0]);
    assert_eq!(row[0], "ALICE"); // UPPER
    assert_eq!(row[1], "alice"); // LOWER
    assert_eq!(row[2], "5"); // LENGTH
    assert_eq!(row[3], "Ali"); // SUBSTR(name, 1, 3)
    assert_eq!(row[4], "Alice"); // COALESCE(name, 'x')
    assert_eq!(row[5], "Alice!"); // CONCAT(name, '!')
    assert_eq!(row[6], "ALICe"); // REPLACE(name, 'lic', 'LIC')
    assert_eq!(row[7], "12.3"); // ROUND(12.345, 1)
}

#[test]
fn test_table_functions() {
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
    let ctx = test_ctx(admin.id);
    let run = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        exec.execute_logical(&ctx, plan).unwrap()
    };

    // generate_series as the sole driving relation.
    let out = run("SELECT * FROM generate_series(1, 5)");
    assert_eq!(out.rows.len(), 5);
    assert_eq!(render_row(&out.rows[0]), vec!["1"]);
    assert_eq!(render_row(&out.rows[4]), vec!["5"]);

    // unnest of a literal array WITH ORDINALITY -> value + 1-based index columns.
    let out = run("SELECT * FROM unnest(ARRAY[10, 20, 30]) WITH ORDINALITY");
    assert_eq!(out.rows.len(), 3);
    assert_eq!(out.columns.len(), 2);
    assert_eq!(render_row(&out.rows[0]), vec!["10", "1"]);
    assert_eq!(render_row(&out.rows[2]), vec!["30", "3"]);

    // Comma-join (cross) table function: each driving row x the function's rows.
    run("CREATE TABLE d (id INT)");
    run("INSERT INTO d (id) VALUES (7)");
    let out = run("SELECT * FROM d, generate_series(1, 3) AS g(n)");
    assert_eq!(out.rows.len(), 3, "1 driving row x 3 series rows");
}

#[test]
fn test_set_operations() {
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
    let ctx = test_ctx(admin.id);

    for t in ["a", "b"] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                constraints: vec![],
                name: t.into(),
                columns: cols(&[("id", "int"), ("n", "int")]),
            },
        )
        .unwrap();
    }
    let insert = |t: &str, id: usize, n: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: t.into(),
                columns: vec![],
                values_list: vec![vec![Value::Text(id.to_string()), Value::Text(n.into())]],
                returning: vec![],
            },
        )
        .unwrap();
    };
    // a.n = {1,2,2,3}, b.n = {2,3,3,4}
    for (i, n) in ["1", "2", "2", "3"].iter().enumerate() {
        insert("a", i, n);
    }
    for (i, n) in ["2", "3", "3", "4"].iter().enumerate() {
        insert("b", i, n);
    }

    let read = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        let out = exec.execute_logical(&ctx, plan).unwrap();
        let mut res: Vec<String> = out.rows.iter().map(|r| render_row(r).join(",")).collect();
        res.sort();
        res
    };

    assert_eq!(
        read("SELECT n FROM a UNION SELECT n FROM b"),
        vec!["1", "2", "3", "4"]
    );
    assert_eq!(
        read("SELECT n FROM a UNION ALL SELECT n FROM b").len(),
        8,
        "UNION ALL keeps all rows"
    );
    assert_eq!(
        read("SELECT n FROM a INTERSECT SELECT n FROM b"),
        vec!["2", "3"]
    );
    assert_eq!(
        read("SELECT n FROM a INTERSECT ALL SELECT n FROM b"),
        vec!["2", "3"],
        "a has one 2 and one matching, two 3s vs two 3s -> 2,3"
    );
    assert_eq!(read("SELECT n FROM a EXCEPT SELECT n FROM b"), vec!["1"]);
    assert_eq!(
        read("SELECT n FROM a EXCEPT ALL SELECT n FROM b"),
        vec!["1", "2"],
        "multiset diff: a has two 2s, b has one -> one 2 remains; 1 remains"
    );
}

#[test]
fn test_cross_join() {
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
    let ctx = test_ctx(admin.id);

    for (t, col) in [("a", "n"), ("b", "m")] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                constraints: vec![],
                name: t.into(),
                columns: cols(&[("id", "int"), (col, "text")]),
            },
        )
        .unwrap();
    }
    let insert = |t: &str, id: &str, v: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: t.into(),
                columns: vec![],
                values_list: vec![vec![Value::Text(id.into()), Value::Text(v.into())]],
                returning: vec![],
            },
        )
        .unwrap();
    };
    insert("a", "1", "x");
    insert("a", "2", "y");
    insert("b", "1", "p");
    insert("b", "2", "q");

    let mut stmts = nodus_sql::parse_sql("SELECT a.n, b.m FROM a CROSS JOIN b").unwrap();
    let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
    let out = exec.execute_logical(&ctx, plan).unwrap();
    let mut res: Vec<String> = out.rows.iter().map(|r| render_row(r).join("-")).collect();
    res.sort();
    assert_eq!(res, vec!["x-p", "x-q", "y-p", "y-q"]);
}

#[test]
fn test_having() {
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
    let ctx = test_ctx(admin.id);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "sales".into(),
            columns: cols(&[("id", "int"), ("category", "text"), ("amount", "int")]),
        },
    )
    .unwrap();
    let insert = |id: &str, c: &str, amt: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "sales".into(),
                columns: vec![],
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(c.into()),
                    Value::Text(amt.into()),
                ]],
                returning: vec![],
            },
        )
        .unwrap();
    };
    // A: 2 rows (sum 30), B: 1 row (sum 15), C: 3 rows (sum 15)
    insert("1", "A", "10");
    insert("2", "A", "20");
    insert("3", "B", "15");
    insert("4", "C", "5");
    insert("5", "C", "5");
    insert("6", "C", "5");

    let read = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        let out = exec.execute_logical(&ctx, plan).unwrap();
        let mut res: Vec<String> = out.rows.iter().map(|r| render_row(r).join(",")).collect();
        res.sort();
        res
    };

    // COUNT(*) > 1 -> A (2) and C (3)
    assert_eq!(
        read("SELECT category, COUNT(id) FROM sales GROUP BY category HAVING COUNT(*) > 1"),
        vec!["A,2", "C,3"]
    );
    // SUM(amount) > 20 -> only A (30)
    assert_eq!(
        read("SELECT category FROM sales GROUP BY category HAVING SUM(amount) > 20"),
        vec!["A"]
    );
    // group column predicate + aggregate predicate combined
    assert_eq!(
        read(
            "SELECT category FROM sales GROUP BY category HAVING COUNT(*) >= 2 AND MIN(amount) = 5"
        ),
        vec!["C"]
    );
}

#[test]
fn test_window_functions() {
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
    let ctx = test_ctx(admin.id);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "emp".into(),
            columns: cols(&[("id", "int"), ("amount", "int")]),
        },
    )
    .unwrap();
    let insert = |id: &str, amt: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "emp".into(),
                columns: vec![],
                values_list: vec![vec![Value::Text(id.into()), Value::Text(amt.into())]],
                returning: vec![],
            },
        )
        .unwrap();
    };
    // amounts: 10, 10, 20, 30
    insert("1", "10");
    insert("2", "10");
    insert("3", "20");
    insert("4", "30");

    let run = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        let out = exec.execute_logical(&ctx, plan).unwrap();
        out.rows
            .iter()
            .map(|r| render_row(r).join(","))
            .collect::<Vec<_>>()
    };

    // RANK over amount asc -> 1,1,3,4 ; DENSE_RANK -> 1,1,2,3
    let rank = run("SELECT id, RANK() OVER (ORDER BY amount) FROM emp ORDER BY id");
    assert_eq!(rank, vec!["1,1", "2,1", "3,3", "4,4"]);
    let dense = run("SELECT id, DENSE_RANK() OVER (ORDER BY amount) FROM emp ORDER BY id");
    assert_eq!(dense, vec!["1,1", "2,1", "3,2", "4,3"]);

    // LAG(amount) ordered by id -> NULL,10,10,20
    let lag = run("SELECT id, LAG(amount) OVER (ORDER BY id) FROM emp ORDER BY id");
    assert_eq!(lag, vec!["1,", "2,10", "3,10", "4,20"]);

    // SUM(amount) OVER () -> 70 for all rows
    let sum = run("SELECT id, SUM(amount) OVER () FROM emp ORDER BY id");
    assert_eq!(sum, vec!["1,70", "2,70", "3,70", "4,70"]);
}

#[test]
fn test_index_ddl() {
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
    let ctx = test_ctx(admin.id);
    let run = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        exec.execute_logical(&ctx, plan)
    };
    run("CREATE TABLE t (id INT, name TEXT)").unwrap();
    run("CREATE INDEX idx_name ON t (name)").unwrap();
    // CREATE INDEX IF NOT EXISTS on an existing index -> ok (no error)
    run("CREATE INDEX IF NOT EXISTS idx_name ON t (name)").unwrap();
    // DROP INDEX -> ok
    run("DROP INDEX idx_name").unwrap();
    // DROP INDEX again without IF EXISTS -> error
    assert!(run("DROP INDEX idx_name").is_err());
    // DROP INDEX IF EXISTS on missing -> ok
    run("DROP INDEX IF EXISTS idx_name").unwrap();
}

#[test]
fn test_general_case() {
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
    let ctx = test_ctx(admin.id);
    let run = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        let out = exec.execute_logical(&ctx, plan).unwrap();
        out.rows
            .iter()
            .map(|r| render_row(r).join(","))
            .collect::<Vec<_>>()
    };
    run("CREATE TABLE t (id INT, n INT)");
    run("INSERT INTO t (id, n) VALUES (1, 5)");
    run("INSERT INTO t (id, n) VALUES (2, 15)");
    run("INSERT INTO t (id, n) VALUES (3, 25)");

    // Searched multi-branch CASE with ELSE.
    let res = run(
        "SELECT id, CASE WHEN n < 10 THEN 'low' WHEN n < 20 THEN 'mid' ELSE 'high' END AS bucket FROM t ORDER BY id",
    );
    assert_eq!(res, vec!["1,low", "2,mid", "3,high"]);
}

#[test]
fn test_index_overlay_merge() {
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
    let ctx = test_ctx(admin.id);
    let exec_sql = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        exec.execute_logical(&ctx, plan).unwrap()
    };
    let query = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        let out = exec.execute_logical(&ctx, plan).unwrap();
        let mut r: Vec<String> = out
            .rows
            .iter()
            .map(|row| render_row(row).join(","))
            .collect();
        r.sort();
        r
    };

    exec_sql("CREATE TABLE t (id INT, n INT)");
    exec_sql("CREATE INDEX idx_n ON t (n)");
    exec_sql("INSERT INTO t (id, n) VALUES (1, 10)");
    exec_sql("INSERT INTO t (id, n) VALUES (2, 20)");

    // Outside a transaction: equality lookup hits the index.
    assert_eq!(query("SELECT id FROM t WHERE n = 10"), vec!["1"]);

    // Inside a transaction, the overlay must merge with the index result.
    exec_sql("BEGIN");
    exec_sql("INSERT INTO t (id, n) VALUES (3, 10)"); // overlay row matching n=10
    assert_eq!(
        query("SELECT id FROM t WHERE n = 10"),
        vec!["1", "3"],
        "index lookup inside a txn sees the uncommitted overlay row"
    );
    exec_sql("COMMIT");

    // After commit, the row is durably visible via the index.
    assert_eq!(query("SELECT id FROM t WHERE n = 10"), vec!["1", "3"]);
}
