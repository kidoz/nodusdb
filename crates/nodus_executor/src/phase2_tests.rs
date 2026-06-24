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
