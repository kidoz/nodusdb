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
fn test_ddl_and_subqueries() {
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
            name: "employees".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: "INT".into(),
                    nullable: false,
                    unique: true,
                    primary: true,
                },
                ColumnDef {
                    name: "name".into(),
                    data_type: "TEXT".into(),
                    nullable: false,
                    unique: false,
                    primary: false,
                },
                ColumnDef {
                    name: "dept_id".into(),
                    data_type: "INT".into(),
                    nullable: true,
                    unique: false,
                    primary: false,
                },
            ],
        },
    )
    .unwrap();

    exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            table_name: "employees".into(),
            columns: vec![],
            values_list: vec![
                vec![
                    Value::Text("1".into()),
                    Value::Text("Alice".into()),
                    Value::Text("100".into()),
                ],
                vec![
                    Value::Text("2".into()),
                    Value::Text("Bob".into()),
                    Value::Text("200".into()),
                ],
            ],
            returning: vec![],
        },
    )
    .unwrap();

    exec.execute_logical(
        &ctx,
        LogicalPlan::AlterTable {
            table_name: "employees".into(),
            operation: AlterTableOp::AddColumn {
                name: "salary".into(),
                data_type: "INT".into(),
                nullable: true,
            },
        },
    )
    .unwrap();

    let tbl = cat.get_table("default", "public", "employees").unwrap();
    assert_eq!(tbl.columns.len(), 4);
    assert_eq!(tbl.columns[3].name, "salary");
    assert_eq!(tbl.indexes.len(), 1);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateIndex {
            name: "idx_emp_dept".into(),
            table_name: "employees".into(),
            columns: vec!["dept_id".into()],
            unique: false,
        },
    )
    .unwrap();

    let tbl = cat.get_table("default", "public", "employees").unwrap();
    assert_eq!(tbl.indexes.len(), 2);
    assert_eq!(tbl.indexes[1].name, "idx_emp_dept");

    exec.execute_logical(
        &ctx,
        LogicalPlan::AlterTable {
            table_name: "employees".into(),
            operation: AlterTableOp::RenameTable {
                new_name: "staff".into(),
            },
        },
    )
    .unwrap();

    assert!(cat.get_table("default", "public", "employees").is_err());
    assert!(cat.get_table("default", "public", "staff").is_ok());

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            constraints: vec![],
            name: "departments".into(),
            columns: cols(&[("id", "int"), ("name", "text")]),
        },
    )
    .unwrap();

    exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            table_name: "departments".into(),
            columns: vec![],
            values_list: vec![vec![
                Value::Text("200".into()),
                Value::Text("Engineering".into()),
            ]],
            returning: vec![],
        },
    )
    .unwrap();

    let subquery = LogicalPlan::Select {
        ctes: vec![],
        table_alias: None,
        group_by: vec![],
        table_name: "departments".into(),
        joins: vec![],
        projection: vec![ProjectionItem::Column("id".into())],
        filter: None,
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        distinct: false,
    };

    let check_out = exec.execute_logical(&ctx, subquery.clone()).unwrap();
    // Debugging output removed

    let filter = FilterExpr::InSubquery {
        left: "dept_id".into(),
        subquery: Box::new(subquery),
        negated: false,
    };

    let out = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "staff".into(),
                joins: vec![],
                projection: vec![ProjectionItem::Column("name".into())],
                filter: Some(filter),
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
                distinct: false,
            },
        )
        .unwrap();

    assert_eq!(out.rows.len(), 1);
    assert_eq!(render_row(&out.rows[0])[0], "Bob");
}

#[test]
fn test_unique_constraints() {
    use super::*;
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
            name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: "INT".into(),
                    nullable: false,
                    unique: true,
                    primary: true,
                },
                ColumnDef {
                    name: "email".into(),
                    data_type: "TEXT".into(),
                    nullable: false,
                    unique: true,
                    primary: false,
                },
            ],
        },
    )
    .unwrap();

    exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            table_name: "users".into(),
            columns: vec![],
            values_list: vec![
                vec![Value::Int(1), Value::Text("a@a.com".into())],
                vec![Value::Int(2), Value::Text("b@b.com".into())],
            ],
            returning: vec![],
        },
    )
    .unwrap();

    let res = exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            table_name: "users".into(),
            columns: vec![],
            values_list: vec![vec![Value::Int(1), Value::Text("c@c.com".into())]],
            returning: vec![],
        },
    );
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Unique constraint violation")
    );

    let res2 = exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            table_name: "users".into(),
            columns: vec![],
            values_list: vec![vec![Value::Int(3), Value::Text("b@b.com".into())]],
            returning: vec![],
        },
    );
    assert!(res2.is_err());

    let res3 = exec.execute_logical(
        &ctx,
        LogicalPlan::Update {
            table_name: "users".into(),
            assignments: vec![("email".into(), Value::Text("a@a.com".into()))],
            filter: Some(FilterExpr::Predicate(Predicate {
                left: "id".into(),
                op: CompareOp::Eq,
                right: Operand::Literal(Value::Int(2)),
            })),
            returning: vec![],
        },
    );
    assert!(res3.is_err());

    let res4 = exec.execute_logical(
        &ctx,
        LogicalPlan::Update {
            table_name: "users".into(),
            assignments: vec![("email".into(), Value::Text("b@b.com".into()))],
            filter: Some(FilterExpr::Predicate(Predicate {
                left: "id".into(),
                op: CompareOp::Eq,
                right: Operand::Literal(Value::Int(2)),
            })),
            returning: vec![],
        },
    );
    assert!(res4.is_ok());
}

#[test]
fn test_secondary_indexing() {
    use super::*;
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
            name: "products".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: "INT".into(),
                    nullable: false,
                    unique: true,
                    primary: true,
                },
                ColumnDef {
                    name: "category".into(),
                    data_type: "TEXT".into(),
                    nullable: false,
                    unique: false,
                    primary: false,
                },
            ],
        },
    )
    .unwrap();

    // 1. Insert rows before indexing
    exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            table_name: "products".into(),
            columns: vec![],
            values_list: vec![
                vec![Value::Int(1), Value::Text("A".into())],
                vec![Value::Int(2), Value::Text("B".into())],
                vec![Value::Int(3), Value::Text("A".into())],
            ],
            returning: vec![],
        },
    )
    .unwrap();

    // 2. Create index on category (should backfill)
    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateIndex {
            name: "idx_cat".into(),
            table_name: "products".into(),
            columns: vec!["category".into()],
            unique: false,
        },
    )
    .unwrap();

    // 3. Insert rows after indexing (should synchronously maintain index)
    exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            table_name: "products".into(),
            columns: vec![],
            values_list: vec![
                vec![Value::Int(4), Value::Text("C".into())],
                vec![Value::Int(5), Value::Text("A".into())],
            ],
            returning: vec![],
        },
    )
    .unwrap();

    // 4. Query using index
    let out = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                table_name: "products".into(),
                joins: vec![],
                projection: vec![ProjectionItem::Column("id".into())],
                group_by: vec![],
                filter: Some(FilterExpr::Predicate(Predicate {
                    left: "category".into(),
                    op: CompareOp::Eq,
                    right: Operand::Literal(Value::Text("A".into())),
                })),
                having: None,
                order_by: vec![("id".into(), true)],
                limit: None,
                offset: None,
                distinct: false,
            },
        )
        .unwrap();

    assert_eq!(out.rows.len(), 3);
    assert_eq!(render_row(&out.rows[0])[0], "1");
    assert_eq!(render_row(&out.rows[1])[0], "3");
    assert_eq!(render_row(&out.rows[2])[0], "5");

    // 5. Update row (change category from A to B)
    exec.execute_logical(
        &ctx,
        LogicalPlan::Update {
            table_name: "products".into(),
            assignments: vec![("category".into(), Value::Text("B".into()))],
            filter: Some(FilterExpr::Predicate(Predicate {
                left: "id".into(),
                op: CompareOp::Eq,
                right: Operand::Literal(Value::Int(1)),
            })),
            returning: vec![],
        },
    )
    .unwrap();

    // Query category A again
    let out_a = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                table_name: "products".into(),
                joins: vec![],
                projection: vec![ProjectionItem::Column("id".into())],
                group_by: vec![],
                filter: Some(FilterExpr::Predicate(Predicate {
                    left: "category".into(),
                    op: CompareOp::Eq,
                    right: Operand::Literal(Value::Text("A".into())),
                })),
                having: None,
                order_by: vec![("id".into(), true)],
                limit: None,
                offset: None,
                distinct: false,
            },
        )
        .unwrap();
    assert_eq!(out_a.rows.len(), 2); // 3 and 5

    // Query category B
    let out_b = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                table_name: "products".into(),
                joins: vec![],
                projection: vec![ProjectionItem::Column("id".into())],
                group_by: vec![],
                filter: Some(FilterExpr::Predicate(Predicate {
                    left: "category".into(),
                    op: CompareOp::Eq,
                    right: Operand::Literal(Value::Text("B".into())),
                })),
                having: None,
                order_by: vec![("id".into(), true)],
                limit: None,
                offset: None,
                distinct: false,
            },
        )
        .unwrap();
    assert_eq!(out_b.rows.len(), 2); // 1 and 2

    // 6. Delete row
    exec.execute_logical(
        &ctx,
        LogicalPlan::Delete {
            table_name: "products".into(),
            filter: Some(FilterExpr::Predicate(Predicate {
                left: "id".into(),
                op: CompareOp::Eq,
                right: Operand::Literal(Value::Int(2)),
            })),
            returning: vec![],
        },
    )
    .unwrap();

    // Query category B again
    let out_b2 = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                table_name: "products".into(),
                joins: vec![],
                projection: vec![ProjectionItem::Column("id".into())],
                group_by: vec![],
                filter: Some(FilterExpr::Predicate(Predicate {
                    left: "category".into(),
                    op: CompareOp::Eq,
                    right: Operand::Literal(Value::Text("B".into())),
                })),
                having: None,
                order_by: vec![("id".into(), true)],
                limit: None,
                offset: None,
                distinct: false,
            },
        )
        .unwrap();
    assert_eq!(out_b2.rows.len(), 1); // Only 1 should be left
    assert_eq!(render_row(&out_b2.rows[0])[0], "1");
}

#[test]
fn test_alter_table_migrations() {
    use super::*;
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
            name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: "INT".into(),
                    nullable: false,
                    unique: true,
                    primary: true,
                },
                ColumnDef {
                    name: "name".into(),
                    data_type: "TEXT".into(),
                    nullable: false,
                    unique: false,
                    primary: false,
                },
            ],
        },
    )
    .unwrap();

    exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            table_name: "users".into(),
            columns: vec![],
            values_list: vec![vec![Value::Int(1), Value::Text("Alice".into())]],
            returning: vec![],
        },
    )
    .unwrap();

    // Add column
    exec.execute_logical(
        &ctx,
        LogicalPlan::AlterTable {
            table_name: "users".into(),
            operation: AlterTableOp::AddColumn {
                name: "age".into(),
                data_type: "INT".into(),
                nullable: true,
            },
        },
    )
    .unwrap();

    // Read to ensure column exists and is NULL
    let out1 = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                table_name: "users".into(),
                joins: vec![],
                projection: vec![],
                group_by: vec![],
                filter: None,
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
                distinct: false,
            },
        )
        .unwrap();
    assert_eq!(out1.rows[0].values.len(), 3);
    assert_eq!(out1.rows[0].values[2], Value::Null);

    // Update the new column
    exec.execute_logical(
        &ctx,
        LogicalPlan::Update {
            table_name: "users".into(),
            assignments: vec![("age".into(), Value::Int(30))],
            filter: None,
            returning: vec![],
        },
    )
    .unwrap();

    let out2 = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                table_name: "users".into(),
                joins: vec![],
                projection: vec![],
                group_by: vec![],
                filter: None,
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
                distinct: false,
            },
        )
        .unwrap();
    assert_eq!(render_row(&out2.rows[0])[2], "30");

    // Drop the column
    exec.execute_logical(
        &ctx,
        LogicalPlan::AlterTable {
            table_name: "users".into(),
            operation: AlterTableOp::DropColumn { name: "age".into() },
        },
    )
    .unwrap();

    let out3 = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                ctes: vec![],
                table_alias: None,
                table_name: "users".into(),
                joins: vec![],
                projection: vec![],
                group_by: vec![],
                filter: None,
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
                distinct: false,
            },
        )
        .unwrap();
    assert_eq!(out3.rows[0].values.len(), 2);
}
