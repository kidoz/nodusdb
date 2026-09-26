//! Session state across statements and between sessions: transaction
//! modes, settings, and the time zone, driven through SQL.

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
fn read_only_transactions_refuse_writes() {
    let (_, sql) = sessions();
    sql("a", "CREATE TABLE t (id INT)").unwrap();
    sql("a", "BEGIN READ ONLY").unwrap();
    let err = sql("a", "INSERT INTO t VALUES (1)")
        .unwrap_err()
        .to_string();
    assert_eq!(err, "cannot execute INSERT in a read-only transaction");
    sql("a", "ROLLBACK").unwrap();
    sql("a", "SET default_transaction_read_only = on").unwrap();
    assert!(sql("a", "DELETE FROM t").is_err());
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

#[test]
fn zoned_timestamps_show_in_the_session_time_zone() {
    let (_, sql) = sessions();
    if crate::timezone::Zone::resolve("America/New_York").is_err() {
        return; // No tz database here.
    }
    sql("a", "CREATE TABLE t (at TIMESTAMPTZ)").unwrap();
    sql("a", "INSERT INTO t VALUES ('2024-07-01 12:00:00+00')").unwrap();
    sql("a", "SET TIME ZONE 'America/New_York'").unwrap();
    assert_eq!(
        rows(&sql("a", "SELECT at FROM t").unwrap()),
        vec!["2024-07-01 08:00:00-04"]
    );
    assert_eq!(
        rows(&sql("a", "SELECT at::date FROM t").unwrap()),
        vec!["2024-07-01"]
    );
    // A value without an offset is read in the session's zone.
    sql("a", "INSERT INTO t VALUES ('2024-01-01 00:00')").unwrap();
    sql("b", "SET TIME ZONE 'UTC'").unwrap();
    assert_eq!(
        rows(&sql("b", "SELECT at FROM t").unwrap()),
        vec!["2024-01-01 05:00:00+00", "2024-07-01 12:00:00+00"]
    );
}
