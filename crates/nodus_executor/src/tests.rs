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
        ..Default::default()
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
            default: None,
            sequence: None,
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
fn create_role_and_grant_require_grant_management() {
    let audit = Arc::new(MemoryAuditSink::new());
    let (exec, cat) = MemExecutor::shared(audit.clone());

    // A superuser (ALL on System) sets up a table to grant on.
    let admin = cat
        .create_role(CreateRoleRequest {
            id: nodus_catalog::PrincipalId::new(),
            name: "admin".into(),
            principal_type: PrincipalType::User,
            database_id: None,

            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),

        grantable: false,
        grantor: None,
    })
    .unwrap();
    let admin_ctx = ctx_for(admin.id);
    exec.execute_logical(
        &admin_ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "t".into(),
            columns: cols(&[("id", "INT")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();

    // An ordinary user with no grants can neither create roles nor grant.
    let bob = cat
        .create_role(CreateRoleRequest {
            id: nodus_catalog::PrincipalId::new(),
            name: "bob".into(),
            principal_type: PrincipalType::User,
            database_id: None,

            attributes: Default::default(),
        })
        .unwrap();
    let bob_ctx = ctx_for(bob.id);
    assert!(
        exec.execute_logical(
            &bob_ctx,
            LogicalPlan::CreateRole {
                name: "evil".into(),
                attributes: Default::default(),
            },
        )
        .is_err(),
        "an unprivileged user must not be able to CREATE ROLE"
    );
    assert!(
        exec.execute_logical(
            &bob_ctx,
            LogicalPlan::Grant {
                privileges: vec!["SELECT".into()],
                objects: crate::plan_types::GrantObjectsPlan::ByName {
                    kind: "TABLE".to_string(),
                    names: vec!["t".to_string()],
                },
                grantees: vec!["bob".into()],
                with_grant_option: false,
            },
        )
        .is_err(),
        "an unprivileged user must not be able to GRANT"
    );

    // The superuser can do both.
    assert!(
        exec.execute_logical(
            &admin_ctx,
            LogicalPlan::CreateRole {
                name: "auditor".into(),
                attributes: Default::default(),
            },
        )
        .is_ok()
    );
    assert!(
        exec.execute_logical(
            &admin_ctx,
            LogicalPlan::Grant {
                privileges: vec!["SELECT".into()],
                objects: crate::plan_types::GrantObjectsPlan::ByName {
                    kind: "TABLE".to_string(),
                    names: vec!["t".to_string()],
                },
                grantees: vec!["bob".into()],
                with_grant_option: false,
            },
        )
        .is_ok()
    );
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

            attributes: Default::default(),
        })
        .unwrap();
    let ctx = ctx_for(user.id);
    let plan = || LogicalPlan::CreateTable {
        inherits: Vec::new(),
        on_commit: None,
        like: Vec::new(),
        unique_constraints: vec![],
        key_names: vec![],
        if_not_exists: false,
        constraints: vec![],
        name: "t1".into(),
        columns: cols(&[("id", "INT"), ("name", "TEXT")]),
        partition_by: None,
        partition_of: None,
        for_values: None,
        key_flags: vec![],
        key_includes: Vec::new(),
        key_temporal: Vec::new(),
    };

    assert!(exec.execute_logical(&ctx, plan()).is_err());

    let sch = cat.get_schema("default", "public").unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: user.id,
        resource: ResourceRef::Schema(sch.id),
        privilege: "CREATE".into(),

        grantable: false,
        grantor: None,
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

            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),

        grantable: false,
        grantor: None,
    })
    .unwrap();
    let ctx = ctx_for(admin.id);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "books".into(),
            columns: cols(&[("id", "INT"), ("title", "TEXT"), ("author", "TEXT")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();

    for (id, title, author) in [("1", "Dune", "Herbert"), ("2", "Foundation", "Asimov")] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                source: None,
                overriding: None,
                alias: None,
                table_name: "books".into(),
                columns: vec!["id".into(), "title".into(), "author".into()],
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(title.into()),
                    Value::Text(author.into()),
                ]],

                returning: vec![],
                on_conflict: None,
                default_cells: vec![],
                returning_exprs: vec![],
            },
        )
        .unwrap();
    }

    // SELECT * returns all rows with all columns.
    let all = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                cte_hints: Vec::new(),
                only: false,
                sample: None,
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "books".into(),
                joins: vec![],
                projection: vec![],
                filter: None,
                having: None,
                grouping_sets: None,
                sort: vec![],
                group_exprs: vec![],
                distinct_on: vec![],
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
                cte_hints: Vec::new(),
                only: false,
                sample: None,
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
                grouping_sets: None,
                sort: vec![],
                group_exprs: vec![],
                distinct_on: vec![],
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
fn execution_is_fenced_while_restoring() {
    use std::sync::atomic::Ordering;
    let (exec, _cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
    let ctx = ctx_for(nodus_catalog::PrincipalId::new());

    // While a restore is in progress, every statement is rejected up front so it
    // cannot observe partially restored state.
    exec.restoring_flag().store(true, Ordering::SeqCst);
    let err = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Begin {
                read_only: None,
                isolation: None,
                start: false,
            },
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("restore in progress"),
        "expected fence error, got: {err}"
    );

    // Clearing the flag resumes normal execution.
    exec.restoring_flag().store(false, Ordering::SeqCst);
    assert!(
        exec.execute_logical(
            &ctx,
            LogicalPlan::Begin {
                read_only: None,
                isolation: None,
                start: false
            }
        )
        .is_ok()
    );
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

            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),

        grantable: false,
        grantor: None,
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
            default: None,
            sequence: None,
        },
        ColumnDef {
            name: "id".into(),
            data_type: "INT".into(),
            nullable: false,
            // PRIMARY KEY implies UNIQUE, as the SQL planner produces it.
            unique: true,
            primary: true,
            default: None,
            sequence: None,
        },
    ];
    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "t".into(),
            columns,
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();

    // Two rows share the first column ("dup") but have distinct primary keys.
    // Pre-fix this collided on key `t:dup` (rejected as a PK violation / overwrite).
    for id in ["1", "2"] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                source: None,
                overriding: None,
                alias: None,
                table_name: "t".into(),
                columns: vec!["label".into(), "id".into()],
                values_list: vec![vec![Value::Text("dup".into()), Value::Text(id.into())]],
                returning: vec![],
                on_conflict: None,
                default_cells: vec![],
                returning_exprs: vec![],
            },
        )
        .unwrap_or_else(|e| panic!("insert id={id} must succeed: {e}"));
    }

    let select_all = || LogicalPlan::Select {
        cte_hints: Vec::new(),
        only: false,
        sample: None,
        ctes: vec![],
        table_alias: None,
        group_by: vec![],
        table_name: "t".into(),
        joins: vec![],
        projection: vec![],
        filter: None,
        having: None,
        grouping_sets: None,
        sort: vec![],
        group_exprs: vec![],
        distinct_on: vec![],
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
            source: None,
            overriding: None,
            alias: None,
            table_name: "t".into(),
            columns: vec!["label".into(), "id".into()],
            values_list: vec![vec![Value::Text("other".into()), Value::Text("1".into())]],
            returning: vec![],
            on_conflict: None,
            default_cells: vec![],
            returning_exprs: vec![],
        },
    );
    assert!(dup.is_err(), "duplicate primary key must be rejected");

    // DELETE keyed by the declared PK removes exactly the targeted row.
    exec.execute_logical(
        &ctx,
        LogicalPlan::Delete {
            only: false,
            table_name: "t".into(),
            filter: eq("id", "1"),
            returning: vec![],
            table_alias: None,
            using: None,
            joins: vec![],
            returning_exprs: vec![],
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

            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),

        grantable: false,
        grantor: None,
    })
    .unwrap();
    let ctx = ctx_for(admin.id);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "items".into(),
            columns: cols(&[("id", "INT"), ("name", "TEXT"), ("active", "BOOL")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();
    exec.execute_logical(
        &ctx,
        LogicalPlan::Insert {
            source: None,
            overriding: None,
            alias: None,
            table_name: "items".into(),
            columns: vec!["id".into(), "name".into(), "active".into()],
            values_list: vec![vec![
                Value::Text("7".into()),
                Value::Text("widget".into()),
                Value::Text("true".into()),
            ]],

            returning: vec![],
            on_conflict: None,
            default_cells: vec![],
            returning_exprs: vec![],
        },
    )
    .unwrap();

    // Filter on an INT column coerces the literal numerically.
    let out = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Select {
                cte_hints: Vec::new(),
                only: false,
                sample: None,
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "items".into(),
                joins: vec![],
                projection: vec![],
                filter: eq("id", "7"),
                having: None,
                grouping_sets: None,
                sort: vec![],
                group_exprs: vec![],
                distinct_on: vec![],
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

            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),

        grantable: false,
        grantor: None,
    })
    .unwrap();
    let ctx = ctx_for(admin.id);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "t".into(),
            columns: cols(&[("id", "INT"), ("name", "TEXT")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();
    for (id, name) in [("1", "a"), ("2", "b"), ("3", "c")] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                source: None,
                overriding: None,
                alias: None,
                table_name: "t".into(),
                columns: vec!["id".into(), "name".into()],
                values_list: vec![vec![Value::Text(id.into()), Value::Text(name.into())]],

                returning: vec![],
                on_conflict: None,
                default_cells: vec![],
                returning_exprs: vec![],
            },
        )
        .unwrap();
    }

    // UPDATE one row.
    let out = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Update {
                only: false,
                table_name: "t".into(),
                assignments: vec![("name".into(), ScalarExpr::Literal(Value::Text("B".into())))],
                filter: eq("id", "2"),

                returning: vec![],
                table_alias: None,
                from: None,
                joins: vec![],
                returning_exprs: vec![],
            },
        )
        .unwrap();
    assert_eq!(out.tag, "UPDATE 1");

    let read = |filter: Option<FilterExpr>| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Select {
                cte_hints: Vec::new(),
                only: false,
                sample: None,
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "t".into(),
                joins: vec![],
                projection: vec![ProjectionItem::Column("name".into())],
                filter,
                having: None,
                grouping_sets: None,
                sort: vec![],
                group_exprs: vec![],
                distinct_on: vec![],
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
                only: false,
                table_name: "t".into(),
                filter: eq("id", "1"),

                returning: vec![],
                table_alias: None,
                using: None,
                joins: vec![],
                returning_exprs: vec![],
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

            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),

        grantable: false,
        grantor: None,
    })
    .unwrap();
    let ctx = ctx_for(admin.id);

    // Create authors
    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "authors".into(),
            columns: cols(&[("id", "INT"), ("name", "TEXT")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();

    // Create books
    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "books".into(),
            columns: cols(&[("id", "INT"), ("title", "TEXT"), ("author_id", "INT")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();

    for (id, name) in [("1", "Herbert"), ("2", "Asimov")] {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                source: None,
                overriding: None,
                alias: None,
                table_name: "authors".into(),
                columns: vec!["id".into(), "name".into()],
                values_list: vec![vec![Value::Text(id.into()), Value::Text(name.into())]],

                returning: vec![],
                on_conflict: None,
                default_cells: vec![],
                returning_exprs: vec![],
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
                source: None,
                overriding: None,
                alias: None,
                table_name: "books".into(),
                columns: vec!["id".into(), "title".into(), "author_id".into()],
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(title.into()),
                    Value::Text(author_id.into()),
                ]],

                returning: vec![],
                on_conflict: None,
                default_cells: vec![],
                returning_exprs: vec![],
            },
        )
        .unwrap();
    }

    let join_plan = LogicalPlan::Select {
        cte_hints: Vec::new(),
        only: false,
        sample: None,
        ctes: vec![],
        table_alias: None,
        group_by: vec![],
        table_name: "books".into(),
        joins: vec![Join {
            only: false,
            sample: None,
            table_alias: None,
            table_name: "authors".into(),
            condition: Some(FilterExpr::Predicate(Predicate {
                left: "books.author_id".into(),
                op: CompareOp::Eq,
                right: Operand::Ident("authors.id".into()),
            })),
            join_type: JoinType::Inner,
            lateral: None,
            using_columns: vec![],
            natural: false,
            table_fn: None,
            semi_subquery: None,
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
        grouping_sets: None,
        sort: vec![],
        group_exprs: vec![],
        distinct_on: vec![],
        order_by: vec![("books.id".into(), true, None)],
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

            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),

        grantable: false,
        grantor: None,
    })
    .unwrap();

    let ctx_a = ExecutionContext {
        session_id: "sess-a".into(),
        principal_id: admin.id,
        active_roles: vec![],
        authz_catalog_version: 1,
        ..Default::default()
    };
    let ctx_b = ExecutionContext {
        session_id: "sess-b".into(),
        principal_id: admin.id,
        active_roles: vec![],
        authz_catalog_version: 1,
        ..Default::default()
    };

    exec.execute_logical(
        &ctx_a,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "t".into(),
            columns: cols(&[("id", "INT"), ("name", "TEXT")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();

    // Session A opens a transaction; session B does NOT.
    exec.execute_logical(
        &ctx_a,
        LogicalPlan::Begin {
            read_only: None,
            isolation: None,
            start: false,
        },
    )
    .unwrap();

    // Session B auto-commits an insert while A's txn is open.
    exec.execute_logical(
        &ctx_b,
        LogicalPlan::Insert {
            source: None,
            overriding: None,
            alias: None,
            table_name: "t".into(),
            columns: vec!["id".into(), "name".into()],
            values_list: vec![vec![Value::Text("1".into()), Value::Text("b".into())]],

            returning: vec![],
            on_conflict: None,
            default_cells: vec![],
            returning_exprs: vec![],
        },
    )
    .unwrap();

    // B sees its own committed row immediately (B has no open snapshot).
    let read = |ctx: &ExecutionContext| {
        exec.execute_logical(
            ctx,
            LogicalPlan::Select {
                cte_hints: Vec::new(),
                only: false,
                sample: None,
                ctes: vec![],
                table_alias: None,
                group_by: vec![],
                table_name: "t".into(),
                joins: vec![],
                projection: vec![],
                filter: None,
                having: None,
                grouping_sets: None,
                sort: vec![],
                group_exprs: vec![],
                distinct_on: vec![],
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

            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),

        grantable: false,
        grantor: None,
    })
    .unwrap();
    let ctx = ctx_for(admin.id);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "t".into(),
            columns: cols(&[("id", "int"), ("name", "text"), ("status", "text")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();

    let insert = |id: &str, name: &str, status: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                source: None,
                overriding: None,
                alias: None,
                table_name: "t".into(),
                columns: vec![],
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(name.into()),
                    Value::Text(status.into()),
                ]],

                returning: vec![],
                on_conflict: None,
                default_cells: vec![],
                returning_exprs: vec![],
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

            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),

        grantable: false,
        grantor: None,
    })
    .unwrap();
    let ctx = ctx_for(admin.id);

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "users".into(),
            columns: cols(&[("id", "int"), ("name", "text")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();

    exec.execute_logical(
        &ctx,
        LogicalPlan::CreateTable {
            inherits: Vec::new(),
            on_commit: None,
            like: Vec::new(),
            unique_constraints: vec![],
            key_names: vec![],
            if_not_exists: false,
            constraints: vec![],
            name: "orders".into(),
            columns: cols(&[("id", "int"), ("user_id", "int"), ("amount", "int")]),
            partition_by: None,
            partition_of: None,
            for_values: None,
            key_flags: vec![],
            key_includes: Vec::new(),
            key_temporal: Vec::new(),
        },
    )
    .unwrap();

    let insert_user = |id: &str, name: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                source: None,
                overriding: None,
                alias: None,
                table_name: "users".into(),
                columns: vec![],
                values_list: vec![vec![Value::Text(id.into()), Value::Text(name.into())]],

                returning: vec![],
                on_conflict: None,
                default_cells: vec![],
                returning_exprs: vec![],
            },
        )
        .unwrap();
    };

    let insert_order = |id: &str, uid: &str, amt: &str| {
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                source: None,
                overriding: None,
                alias: None,
                table_name: "orders".into(),
                columns: vec![],
                values_list: vec![vec![
                    Value::Text(id.into()),
                    Value::Text(uid.into()),
                    Value::Text(amt.into()),
                ]],

                returning: vec![],
                on_conflict: None,
                default_cells: vec![],
                returning_exprs: vec![],
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

// ---------------------------------------------------------------------------
// Regression: a failed `kv.commit` after `commit_or_release` must release the
// transaction's intents instead of leaking them (they would block every later
// writer to those keys and stall snapshot builds).
// ---------------------------------------------------------------------------

/// A `MemKvEngine` wrapper whose `commit` fails while `fail_commit` is set,
/// modelling a Raft submit that never landed.
struct FailCommitKv {
    inner: nodus_storage_mem::MemKvEngine,
    fail_commit: std::sync::atomic::AtomicBool,
}

impl FailCommitKv {
    fn new() -> Self {
        Self {
            inner: nodus_storage_mem::MemKvEngine::new(),
            fail_commit: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl KvEngine for FailCommitKv {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        self.inner.get(key, read_ts)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<nodus_storage_api::KvPair>> + Send>> {
        self.inner.scan(range, read_ts)
    }

    fn write_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        value: Bytes,
    ) -> nodus_storage_api::KvResult<()> {
        self.inner.write_intent(txn_id, key, value)
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> nodus_storage_api::KvResult<()> {
        self.inner.delete_intent(txn_id, key)
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> nodus_storage_api::KvResult<()> {
        self.inner.replace_intent(txn_id, key, replacement)
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> nodus_storage_api::KvResult<()> {
        if self.fail_commit.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(nodus_storage_api::KvError::Other(anyhow::anyhow!(
                "injected commit failure"
            )));
        }
        self.inner.commit(txn_id, commit_ts)
    }

    fn abort(&self, txn_id: TxnId) -> nodus_storage_api::KvResult<()> {
        self.inner.abort(txn_id)
    }

    fn pending_intent_keys(&self, txn_id: TxnId) -> Vec<Bytes> {
        self.inner.pending_intent_keys(txn_id)
    }

    fn has_pending_intents(&self, prefix: &[u8]) -> Result<bool> {
        self.inner.has_pending_intents(prefix)
    }
}

fn sql(exec: &MemExecutor, ctx: &ExecutionContext, sql: &str) -> Result<QueryOutput> {
    let mut stmts = nodus_sql::parse_sql(sql).unwrap();
    let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
    exec.execute_logical(ctx, plan)
}

fn executor_with_kv(kv: Arc<dyn KvEngine>) -> (Arc<MemExecutor>, ExecutionContext) {
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
    let admin = cat
        .create_role(CreateRoleRequest {
            id: nodus_catalog::PrincipalId::new(),
            name: "admin".into(),
            principal_type: PrincipalType::User,
            database_id: None,
            attributes: Default::default(),
        })
        .unwrap();
    cat.grant_privilege(GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: ResourceRef::System,
        privilege: "ALL".into(),
        grantable: false,
        grantor: None,
    })
    .unwrap();
    let txn = Arc::new(nodus_txn::MemTxnManager::new());
    let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(cat.clone()));
    let exec = Arc::new(MemExecutor::new(
        cat.clone(),
        cat,
        authz,
        Arc::new(MemoryAuditSink::new()),
        kv,
        txn,
    ));
    (exec, ctx_for(admin.id))
}

#[test]
fn buffered_multirow_insert_preserves_errors_indexes_and_savepoints() {
    let (exec, ctx) = executor_with_kv(Arc::new(nodus_storage_mem::MemKvEngine::new()));
    sql(
        &exec,
        &ctx,
        "CREATE TABLE buffered (id INT PRIMARY KEY, grp INT)",
    )
    .unwrap();
    sql(&exec, &ctx, "CREATE INDEX buffered_grp ON buffered (grp)").unwrap();

    let duplicate_values = (0..130)
        .map(|id| if id == 129 { 0 } else { id })
        .map(|id| format!("({id}, {})", id % 10))
        .collect::<Vec<_>>()
        .join(",");
    let error = sql(
        &exec,
        &ctx,
        &format!("INSERT INTO buffered VALUES {duplicate_values}"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("duplicate key"));
    let out = sql(&exec, &ctx, "SELECT count(*) FROM buffered").unwrap();
    assert_eq!(out.rows[0].values[0], Value::Int(0));

    sql(&exec, &ctx, "BEGIN").unwrap();
    let values = (0..130)
        .map(|id| format!("({id}, {})", id % 10))
        .collect::<Vec<_>>()
        .join(",");
    sql(
        &exec,
        &ctx,
        &format!("INSERT INTO buffered VALUES {values}"),
    )
    .unwrap();
    let out = sql(&exec, &ctx, "SELECT count(*) FROM buffered").unwrap();
    assert_eq!(out.rows[0].values[0], Value::Int(130));
    let out = sql(&exec, &ctx, "SELECT id FROM buffered WHERE grp = 7").unwrap();
    assert_eq!(out.rows.len(), 13);

    sql(&exec, &ctx, "SAVEPOINT after_bulk").unwrap();
    sql(
        &exec,
        &ctx,
        "INSERT INTO buffered VALUES (200, 7), (201, 7)",
    )
    .unwrap();
    sql(&exec, &ctx, "ROLLBACK TO SAVEPOINT after_bulk").unwrap();
    sql(&exec, &ctx, "RELEASE SAVEPOINT after_bulk").unwrap();
    sql(&exec, &ctx, "COMMIT").unwrap();

    let out = sql(&exec, &ctx, "SELECT count(*) FROM buffered").unwrap();
    assert_eq!(out.rows[0].values[0], Value::Int(130));
    let out = sql(&exec, &ctx, "SELECT count(*) FROM buffered WHERE id >= 200").unwrap();
    assert_eq!(out.rows[0].values[0], Value::Int(0));
}

#[test]
fn failed_kv_commit_releases_intents_explicit_txn() {
    let kv = Arc::new(FailCommitKv::new());
    let (exec, ctx) = executor_with_kv(kv.clone());
    sql(&exec, &ctx, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    sql(&exec, &ctx, "BEGIN").unwrap();
    sql(&exec, &ctx, "INSERT INTO t VALUES (1)").unwrap();
    kv.fail_commit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let error = sql(&exec, &ctx, "COMMIT").unwrap_err();
    assert!(
        error.to_string().contains("injected commit failure"),
        "client-facing error is the storage failure, got: {error}"
    );
    kv.fail_commit
        .store(false, std::sync::atomic::Ordering::SeqCst);

    // The failed commit released its intents: nothing is pending, and a later
    // transaction writing the same key neither conflicts nor resurrects the
    // never-committed row.
    assert!(!kv.has_pending_intents(b"").unwrap());
    sql(&exec, &ctx, "INSERT INTO t VALUES (1)").unwrap();
    let out = sql(&exec, &ctx, "SELECT * FROM t").unwrap();
    assert_eq!(out.rows.len(), 1);
}

#[test]
fn failed_kv_commit_releases_intents_implicit_txn() {
    let kv = Arc::new(FailCommitKv::new());
    let (exec, ctx) = executor_with_kv(kv.clone());
    sql(&exec, &ctx, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    kv.fail_commit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let error = sql(&exec, &ctx, "INSERT INTO t VALUES (1)").unwrap_err();
    assert!(
        error.to_string().contains("injected commit failure"),
        "client-facing error is the storage failure, got: {error}"
    );
    kv.fail_commit
        .store(false, std::sync::atomic::Ordering::SeqCst);

    assert!(!kv.has_pending_intents(b"").unwrap());
    sql(&exec, &ctx, "INSERT INTO t VALUES (1)").unwrap();
    sql(&exec, &ctx, "INSERT INTO t VALUES (2)").unwrap();
    let out = sql(&exec, &ctx, "SELECT * FROM t").unwrap();
    assert_eq!(out.rows.len(), 2);
}

// ---------------------------------------------------------------------------
// Regression: on an LSM engine, a point read must not resurrect a deleted row
// from an older SSTable. These probes drive the unique/foreign-key paths that
// call `kv.get` (crates/nodus_executor/src/constraints.rs) against a real
// `LsmKvEngine` with a flush between the delete and the probe.
// ---------------------------------------------------------------------------

#[test]
fn lsm_deleted_rows_stay_deleted_for_constraint_probes() {
    let dir = std::env::temp_dir().join(format!(
        "nodus-executor-lsm-test-{}",
        nodus_storage_api::TxnId::new().0
    ));
    let kv = Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(&dir, None).unwrap());
    let (exec, ctx) = executor_with_kv(kv.clone());

    sql(&exec, &ctx, "CREATE TABLE parent (id INT PRIMARY KEY)").unwrap();
    sql(
        &exec,
        &ctx,
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))",
    )
    .unwrap();
    sql(&exec, &ctx, "INSERT INTO parent VALUES (1)").unwrap();
    // Flush so the value sits in an SSTable; the delete's tombstone then lands
    // in a *newer* source, which is the state the resurrection bug needs.
    kv.flush().unwrap();

    // Delete the row and flush so the tombstone moves to an SSTable while the
    // old value remains in an older one.
    sql(&exec, &ctx, "DELETE FROM parent WHERE id = 1").unwrap();
    kv.flush().unwrap();

    // Unique probe: re-inserting the deleted primary key must succeed — the
    // duplicate-key check (`kv.get`) must not see the deleted row.
    sql(&exec, &ctx, "INSERT INTO parent VALUES (1)")
        .expect("re-inserting a deleted primary key must not raise duplicate key");

    // Foreign-key probe: referencing a deleted parent must fail — the FK
    // existence check must not see the deleted row either. (Parent id 2 was
    // never inserted; the resurrected id 1 row must not satisfy it either.)
    sql(&exec, &ctx, "DELETE FROM parent WHERE id = 1").unwrap();
    kv.flush().unwrap();
    let error = sql(&exec, &ctx, "INSERT INTO child VALUES (10, 1)").unwrap_err();
    assert!(
        error.to_string().contains("foreign key"),
        "expected a foreign-key violation, got: {error}"
    );

    drop(exec);
    let _ = std::fs::remove_dir_all(&dir);
}
