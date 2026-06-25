use super::render;

fn render_row(row: &Row) -> Vec<String> {
    row.values.iter().map(render).collect()
}

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

pub fn cols(names: &[(&str, &str)]) -> Vec<ColumnDef> {
    names
        .iter()
        .map(|(n, t)| ColumnDef {
            name: n.to_string(),
            data_type: t.to_string(),
            nullable: true,
            unique: false,
            primary: false,
        })
        .collect()
}

fn eq(col: &str, val: &str) -> Option<FilterExpr> {
    Some(FilterExpr::Predicate(Predicate {
        left: col.to_string(),
        op: CompareOp::Eq,
        right: Operand::Literal(crate::Value::Text(val.to_string())),
    }))
}

#[test]
fn create_table_denied_then_allowed_by_grant() {
    let audit = Arc::new(MemoryAuditSink::new());
    let (exec, cat) = MemExecutor::shared(audit.clone());
    let user = cat
        .create_role(CreateRoleRequest {
            id: nodus_catalog::PrincipalId::new(),
            name: "bob".into(),
            principal_type: PrincipalType::User,
            database_id: None,
        })
        .unwrap();
    let ctx = ctx_for(user.id);
    let plan = || LogicalPlan::CreateTable {
        constraints: vec![],
        name: "t1".into(),
        columns: cols(&[("id", "INT"), ("name", "TEXT")]),
    };

    assert!(exec.execute_logical(&ctx, plan()).is_err());

    let sch = cat.get_schema("default", "public").unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: user.id,
        resource: ResourceRef::Schema(sch.id),
        privilege: "CREATE".into(),
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
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(title.into()),
                    Value::Text(author.into()),
                ]],

                returning: vec![],
            },
        )
        .unwrap();
    }

    // SELECT * returns all rows with all columns.
    let all = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "books".into(),
                joins: vec![],
                projection: vec![],
                filter: None,
                having: None,
                order_by: vec![],
                limit: None,

                offset: None,
                distinct: false,
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
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "books".into(),
                joins: vec![],
                projection: vec![
                    ProjectionItem::Column("title".into()),
                    ProjectionItem::Column("author".into()),
                ],
                filter: eq("id", "2"),
                having: None,
                order_by: vec![],
                limit: None,

                offset: None,
                distinct: false,
            },
        )
        .unwrap();
    assert_eq!(one.columns, vec!["title", "author"]);
    assert_eq!(one.rows.len(), 1);
    assert_eq!(render_row(&one.rows[0]), vec!["Foundation", "Asimov"]);
}

#[test]
fn rows_keyed_by_declared_pk_not_first_column() {
    // Regression: rows must be keyed by the declared PRIMARY KEY, not the first
    // column. Here the PK is the *second* column; the first column is not unique.
    let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
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

    let columns = vec![
        ColumnDef {
            name: "label".into(),
            data_type: "TEXT".into(),
            nullable: false,
            unique: false,
            primary: false,
        },
        ColumnDef {
            name: "id".into(),
            data_type: "INT".into(),
            nullable: false,
            // PRIMARY KEY implies UNIQUE, as the SQL planner produces it.
            unique: true,
            primary: true,
        },
    ];
    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "t".into(),
            columns,
        },
    )
    .unwrap();

    // Two rows share the first column ("dup") but have distinct primary keys.
    // Pre-fix this collided on key `t:dup` (rejected as a PK violation / overwrite).
    for id in ["1", "2"] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "t".into(),
                columns: vec!["label".into(), "id".into()],
                values_list: vec![vec![Value::Text("dup".into()), Value::Text(id.into())]],
                returning: vec![],
            },
        )
        .unwrap_or_else(|e| panic!("insert id={id} must succeed: {e}"));
    }

    let select_all = || LogicalPlan::Select {
        ctes: vec![],
        table_alias: None,
        group_by: vec![],
        table_name: "t".into(),
        joins: vec![],
        projection: vec![],
        filter: None,
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
    };

    // Both distinct-PK rows persist independently.
    let all = exec.execute_logical(&ctx, select_all()).unwrap();
    assert_eq!(all.rows.len(), 2, "both rows must persist");

    // A genuine duplicate primary key is still rejected.
    let dup = exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            table_name: "t".into(),
            columns: vec!["label".into(), "id".into()],
            values_list: vec![vec![Value::Text("other".into()), Value::Text("1".into())]],
            returning: vec![],
        },
    );
    assert!(dup.is_err(), "duplicate primary key must be rejected");

    // DELETE keyed by the declared PK removes exactly the targeted row.
    exec.execute_logical(
        &ctx,
        LogicalPlan::Delete {
            table_name: "t".into(),
            filter: eq("id", "1"),
            returning: vec![],
        },
    )
    .unwrap();
    let remaining = exec.execute_logical(&ctx, select_all()).unwrap();
    assert_eq!(remaining.rows.len(), 1, "exactly one row should remain");
    // The surviving row is the one whose PK is 2.
    assert!(
        remaining.rows[0].values.iter().any(|v| render(v) == "2"),
        "row with id=2 should survive"
    );
}

#[test]
fn typed_values_round_trip_and_filter_by_int() {
    let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
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
            values_list: vec![vec![
                Value::Text("7".into()),
                Value::Text("widget".into()),
                Value::Text("true".into()),
            ]],

            returning: vec![],
        },
    )
    .unwrap();

    // Filter on an INT column coerces the literal numerically.
    let out = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "items".into(),
                joins: vec![],
                projection: vec![],
                filter: eq("id", "7"),
                having: None,
                order_by: vec![],
                limit: None,

                offset: None,
                distinct: false,
            },
        )
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    // Int renders without quotes, bool as true/false.
    assert_eq!(render_row(&out.rows[0]), vec!["7", "widget", "t"]);
}

#[test]
fn update_and_delete_rows() {
    let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
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
            columns: cols(&[("id", "INT"), ("name", "TEXT")]),
        },
    )
    .unwrap();
    for (id, name) in [("1", "a"), ("2", "b"), ("3", "c")] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "t".into(),
                columns: vec!["id".into(), "name".into()],
                values_list: vec![vec![Value::Text(id.into()), Value::Text(name.into())]],

                returning: vec![],
            },
        )
        .unwrap();
    }

    // UPDATE one row.
    let out = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Update {
                table_name: "t".into(),
                assignments: vec![("name".into(), Value::Text("B".into()))],
                filter: eq("id", "2"),

                returning: vec![],
            },
        )
        .unwrap();
    assert_eq!(out.tag, "UPDATE 1");

    let read = |filter: Option<FilterExpr>| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "t".into(),
                joins: vec![],
                projection: vec![ProjectionItem::Column("name".into())],
                filter,
                having: None,
                order_by: vec![],
                limit: None,

                offset: None,
                distinct: false,
            },
        )
        .unwrap()
    };
    assert_eq!(render_row(&read(eq("id", "2")).rows[0]), vec!["B"]);

    // DELETE one row, then confirm it's gone and the rest remain.
    let out = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Delete {
                table_name: "t".into(),
                filter: eq("id", "1"),

                returning: vec![],
            },
        )
        .unwrap();
    assert_eq!(out.tag, "DELETE 1");
    assert_eq!(read(eq("id", "1")).rows.len(), 0);
    assert_eq!(read(None).rows.len(), 2);
}

#[test]
fn test_join_execution() {
    let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
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

    // Create authors
    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "authors".into(),
            columns: cols(&[("id", "INT"), ("name", "TEXT")]),
        },
    )
    .unwrap();

    // Create books
    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "books".into(),
            columns: cols(&[("id", "INT"), ("title", "TEXT"), ("author_id", "INT")]),
        },
    )
    .unwrap();

    for (id, name) in [("1", "Herbert"), ("2", "Asimov")] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "authors".into(),
                columns: vec!["id".into(), "name".into()],
                values_list: vec![vec![Value::Text(id.into()), Value::Text(name.into())]],

                returning: vec![],
            },
        )
        .unwrap();
    }

    for (id, title, author_id) in [
        ("10", "Dune", "1"),
        ("11", "Foundation", "2"),
        ("12", "Dune Messiah", "1"),
    ] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "books".into(),
                columns: vec!["id".into(), "title".into(), "author_id".into()],
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(title.into()),
                    Value::Text(author_id.into()),
                ]],

                returning: vec![],
            },
        )
        .unwrap();
    }

    let join_plan = LogicalPlan::Select {
        ctes: vec![],
        table_alias: None,
        group_by: vec![],
        table_name: "books".into(),
        joins: vec![Join {
            table_alias: None,
            table_name: "authors".into(),
            condition: Some(FilterExpr::Predicate(Predicate {
                left: "books.author_id".into(),
                op: CompareOp::Eq,
                right: Operand::Ident("authors.id".into()),
            })),
            join_type: JoinType::Inner,
        }],
        projection: vec![
            ProjectionItem::Column("books.title".into()),
            ProjectionItem::Column("authors.name".into()),
        ],
        filter: Some(FilterExpr::Predicate(Predicate {
            left: "authors.name".into(),
            op: CompareOp::Eq,
            right: Operand::Literal(Value::Text("Herbert".into())),
        })),
        having: None,
        order_by: vec![("books.id".into(), true)],
        limit: None,

        offset: None,
        distinct: false,
    };

    let out = exec.execute_logical(&ctx, join_plan).unwrap();
    assert_eq!(out.columns, vec!["title", "name"]);
    assert_eq!(out.rows.len(), 2);
    assert_eq!(render_row(&out.rows[0]), vec!["Dune", "Herbert"]);
    assert_eq!(render_row(&out.rows[1]), vec!["Dune Messiah", "Herbert"]);
}

#[test]
fn transactions_are_isolated_per_session() {
    let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
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

    let ctx_a = ExecutionContext {
        session_id: "sess-a".into(),
        principal_id: admin.id,
        active_roles: vec![],
        authz_catalog_version: 1,
    };
    let ctx_b = ExecutionContext {
        session_id: "sess-b".into(),
        principal_id: admin.id,
        active_roles: vec![],
        authz_catalog_version: 1,
    };

    exec.execute_logical(
        &ctx_a,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "t".into(),
            columns: cols(&[("id", "INT"), ("name", "TEXT")]),
        },
    )
    .unwrap();

    // Session A opens a transaction; session B does NOT.
    exec.execute_logical(&ctx_a, LogicalPlan::Begin).unwrap();

    // Session B auto-commits an insert while A's txn is open.
    exec.execute_logical(
        &ctx_b,
        LogicalPlan::Insert {
            table_name: "t".into(),
            columns: vec!["id".into(), "name".into()],
            values_list: vec![vec![Value::Text("1".into()), Value::Text("b".into())]],

            returning: vec![],
        },
    )
    .unwrap();

    // B sees its own committed row immediately (B has no open snapshot).
    let read = |ctx: &ExecutionContext| {
        exec.execute_logical(
            ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "t".into(),
                joins: vec![],
                projection: vec![],
                filter: None,
                having: None,
                order_by: vec![],
                limit: None,

                offset: None,
                distinct: false,
            },
        )
        .unwrap()
        .rows
        .len()
    };
    assert_eq!(
        read(&ctx_b),
        1,
        "session B sees its own auto-committed write"
    );

    // A COMMIT from B must not touch A's still-open transaction.
    exec.execute_logical(&ctx_b, LogicalPlan::Commit).unwrap();
    // A's transaction is still open and independently committable.
    exec.execute_logical(&ctx_a, LogicalPlan::Commit).unwrap();
    assert_eq!(read(&ctx_a), 1);
}

#[test]
fn run_gc_is_safe_with_no_active_txns() {
    let exec = MemExecutor::default();
    assert_eq!(exec.run_gc().unwrap(), 0);
}

#[test]
fn run_gc_honors_protected_watermark() {
    let exec = MemExecutor::default();
    let kv = exec.kv();
    let key = bytes::Bytes::from("protected-key");

    let txn1 = nodus_storage_api::TxnId::new();
    kv.write_intent(txn1, key.clone(), bytes::Bytes::from("v1"))
        .unwrap();
    kv.commit(txn1, 10).unwrap();

    let txn2 = nodus_storage_api::TxnId::new();
    kv.write_intent(txn2, key.clone(), bytes::Bytes::from("v2"))
        .unwrap();
    kv.commit(txn2, 20).unwrap();

    assert_eq!(kv.get(&key, 15).unwrap().unwrap(), bytes::Bytes::from("v1"));
    assert_eq!(exec.run_gc_with_protected_watermark(Some(10)).unwrap(), 0);
    assert_eq!(kv.get(&key, 15).unwrap().unwrap(), bytes::Bytes::from("v1"));

    assert_eq!(exec.run_gc_with_protected_watermark(None).unwrap(), 1);
    assert!(kv.get(&key, 15).unwrap().is_none());
}

#[test]
fn test_complex_filters() {
    let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
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
            columns: cols(&[("id", "int"), ("name", "text"), ("status", "text")]),
        },
    )
    .unwrap();

    let insert = |id: &str, name: &str, status: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "t".into(),
                columns: vec![],
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(name.into()),
                    Value::Text(status.into()),
                ]],

                returning: vec![],
            },
        )
        .unwrap();
    };

    insert("1", "alice", "active");
    insert("2", "bob", "inactive");
    insert("3", "charlie", "active");
    insert("4", "dave", "banned");

    let read = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        exec.execute_logical(&ctx, plan).unwrap().rows.len()
    };

    assert_eq!(
        read("SELECT * FROM t WHERE status = 'active' OR status = 'banned'"),
        3
    );
    assert_eq!(
        read("SELECT * FROM t WHERE status IN ('active', 'banned')"),
        3
    );
    assert_eq!(read("SELECT * FROM t WHERE name LIKE 'a%'"), 1);
    assert_eq!(read("SELECT * FROM t WHERE name LIKE '%e'"), 3); // alice, charlie, dave
    assert_eq!(read("SELECT * FROM t WHERE NOT status = 'active'"), 2);
}

#[test]
fn test_left_outer_join() {
    let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
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
            name: "users".into(),
            columns: cols(&[("id", "int"), ("name", "text")]),
        },
    )
    .unwrap();

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "orders".into(),
            columns: cols(&[("id", "int"), ("user_id", "int"), ("amount", "int")]),
        },
    )
    .unwrap();

    let insert_user = |id: &str, name: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "users".into(),
                columns: vec![],
                values_list: vec![vec![Value::Text(id.into()), Value::Text(name.into())]],

                returning: vec![],
            },
        )
        .unwrap();
    };

    let insert_order = |id: &str, uid: &str, amt: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "orders".into(),
                columns: vec![],
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(uid.into()),
                    Value::Text(amt.into()),
                ]],

                returning: vec![],
            },
        )
        .unwrap();
    };

    insert_user("1", "Alice");
    insert_user("2", "Bob");
    insert_user("3", "Charlie");

    insert_order("101", "1", "500");
    insert_order("102", "1", "300");
    insert_order("103", "3", "700");

    let read = |sql: &str| {
        let mut stmts = nodus_sql::parse_sql(sql).unwrap();
        let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
        exec.execute_logical(&ctx, plan).unwrap()
    };

    // Inner Join
    let inner = read("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    assert_eq!(inner.rows.len(), 3); // 2 for Alice, 0 for Bob, 1 for Charlie

    // Left Join
    let left = read("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
    assert_eq!(left.rows.len(), 4); // 2 for Alice, 1 for Bob (NULLs), 1 for Charlie

    // Let's verify Bob's row has NULLs
    let bob_row = left
        .rows
        .iter()
        .find(|r| r.values[1] == Value::Text("Bob".to_string()))
        .unwrap();
    assert_eq!(bob_row.values.len(), 5); // users(id, name) + orders(id, user_id, amount)
    assert_eq!(bob_row.values[2], Value::Null); // order.id
    assert_eq!(bob_row.values[3], Value::Null); // order.user_id
    assert_eq!(bob_row.values[4], Value::Null); // order.amount
}
