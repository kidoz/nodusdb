//! Session state across statements and between sessions: settings,
//! driven through SQL.

use super::*;
use crate::dml_join_tests::rows;
use nodus_audit::MemoryAuditSink;

/// An executor, and a function running one SQL statement in a named session.
fn sessions() -> (Arc<MemExecutor>, impl Fn(&str, &str) -> Result<QueryOutput>) {
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
    let runner = exec.clone();
    let run = move |session: &str, sql: &str| {
        let ctx = ExecutionContext {
            session_id: session.into(),
            principal_id: admin.id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };
        let mut statements = nodus_sql::parse_sql(sql)?;
        let plan = plan_statement(&statements.remove(0), &[])?;
        runner.execute_logical(&ctx, plan)
    };
    (exec, run)
}

#[test]
fn settings_follow_their_transaction() {
    let (exec, sql) = sessions();
    exec.start_session("a", &[("application_name".into(), "psql".into())]);
    let show = |name: &str| rows(&sql("a", &format!("SHOW {name}")).unwrap());
    sql("a", "BEGIN").unwrap();
    sql("a", "SET work_mem = '2MB'").unwrap();
    sql("a", "ROLLBACK").unwrap();
    assert_eq!(show("work_mem"), vec!["4MB"]);
    sql("a", "BEGIN").unwrap();
    sql("a", "SET LOCAL work_mem = '3MB'").unwrap();
    assert_eq!(show("work_mem"), vec!["3MB"]);
    sql("a", "COMMIT").unwrap();
    assert_eq!(show("work_mem"), vec!["4MB"]);
    // RESET returns to the value the client connected with.
    sql("a", "SET application_name = 'other'").unwrap();
    sql("a", "RESET application_name").unwrap();
    assert_eq!(show("application_name"), vec!["psql"]);
    assert!(sql("a", "SET no_such_setting = 1").is_err());
    let changes = exec.take_parameter_changes("a");
    assert_eq!(
        changes.last(),
        Some(&("application_name".to_string(), "psql".to_string()))
    );
}
