use super::*;
use crate::tests::cols;
use nodus_audit::MemoryAuditSink;

fn render_row(row: &Row) -> Vec<String> {
    row.values.iter().map(render).collect()
}

fn test_ctx(admin_id: PrincipalId) -> ExecutionContext {
    ExecutionContext {
        session_id: "test".into(),
        principal_id: admin_id,
        active_roles: vec![],
        authz_catalog_version: 1,
    }
}

#[test]
fn test_offset_distinct_returning() {
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
            if_not_exists: false,
            constraints: vec![],
            name: "t".into(),
            columns: cols(&[("id", "int"), ("val", "text")]),
        },
    )
    .unwrap();

    // 1. Multi-row INSERT with RETURNING
    let insert_out = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "t".into(),
                columns: vec!["id".into(), "val".into()],
                values_list: vec![
                    vec![Value::Text("1".into()), Value::Text("A".into())],
                    vec![Value::Text("2".into()), Value::Text("B".into())],
                    vec![Value::Text("3".into()), Value::Text("A".into())],
                    vec![Value::Text("4".into()), Value::Text("C".into())],
                ],
                returning: vec!["id".into(), "val".into()],
            },
        )
        .unwrap();

    assert_eq!(insert_out.tag, "INSERT 0 4");
    assert_eq!(insert_out.rows.len(), 4);
    assert_eq!(render_row(&insert_out.rows[0]), vec!["1", "A"]);
    assert_eq!(render_row(&insert_out.rows[3]), vec!["4", "C"]);

    let read = |offset: Option<usize>, limit: Option<usize>, distinct: bool, proj: Vec<&str>| {
        let out = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    ctes: vec![],
                    table_alias: None,
                    group_by: vec![],
                    table_name: "t".into(),
                    joins: vec![],
                    projection: proj
                        .into_iter()
                        .map(|s| ProjectionItem::Column(s.to_string()))
                        .collect(),
                    filter: None,
                    having: None,
                    order_by: vec![],
                    limit,
                    offset,
                    distinct,
                },
            )
            .unwrap();
        out.rows
            .into_iter()
            .map(|r| render_row(&r).join(","))
            .collect::<Vec<_>>()
    };

    // 2. OFFSET and LIMIT
    let p1 = read(None, Some(2), false, vec![]);
    assert_eq!(p1, vec!["1,A", "2,B"]);

    let p2 = read(Some(2), Some(2), false, vec![]);
    assert_eq!(p2, vec!["3,A", "4,C"]);

    let p3 = read(Some(3), None, false, vec![]);
    assert_eq!(p3, vec!["4,C"]);

    // 3. DISTINCT
    let dist = read(None, None, true, vec!["val"]);
    // Should only be A, B, C (3 items)
    assert_eq!(dist.len(), 3);
    assert!(dist.contains(&"A".to_string()));
    assert!(dist.contains(&"B".to_string()));
    assert!(dist.contains(&"C".to_string()));

    // 4. RETURNING on UPDATE
    let update_out = exec
        .execute_logical(
            &ctx,
            LogicalPlan::Update {
                table_name: "t".into(),
                assignments: vec![("val".into(), ScalarExpr::Literal(Value::Text("Z".into())))],
                filter: Some(FilterExpr::Predicate(Predicate {
                    left: "id".into(),
                    op: CompareOp::Eq,
                    right: Operand::Literal(Value::Text("2".into())),
                })),
                returning: vec!["id".into(), "val".into()],
            },
        )
        .unwrap();
    assert_eq!(update_out.tag, "UPDATE 1");
    assert_eq!(update_out.rows.len(), 1);
    assert_eq!(render_row(&update_out.rows[0]), vec!["2", "Z"]);
}
