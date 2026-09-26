//! Constraint violations as PostgreSQL reports them, foreign key actions,
//! and notices, driven through SQL.

use super::*;
use crate::dml_join_tests::rows;
use nodus_audit::MemoryAuditSink;

/// A superuser session: a function running one statement, and one taking
/// the notices raised since it was last called.
fn session() -> (
    impl Fn(&str) -> Result<QueryOutput>,
    impl Fn() -> Vec<String>,
) {
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
    let notices = exec.clone();
    (
        move |sql: &str| {
            let mut statements = nodus_sql::parse_sql(sql)?;
            let plan = plan_statement(&statements.remove(0), &[])?;
            exec.execute_logical(&ctx, plan)
        },
        move || {
            notices
                .take_notices("test")
                .iter()
                .map(|n| error_message(n).to_string())
                .collect()
        },
    )
}

/// An error's message and its fields, as the wire layer splits them.
fn fields(error: anyhow::Error) -> (String, Vec<(String, String)>) {
    let text = error.to_string();
    (
        error_message(&text).to_string(),
        error_fields(&text)
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

fn field(pairs: &[(String, String)], name: &str) -> Option<String> {
    pairs
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.clone())
}

#[test]
fn violations_carry_postgresql_messages_and_fields() {
    let (sql, _) = session();
    sql("CREATE TABLE p (id int PRIMARY KEY, name text NOT NULL, qty int CHECK (qty >= 0), a int, b int, UNIQUE (a, b))").unwrap();
    sql("CREATE TABLE c (id int PRIMARY KEY, p_id int REFERENCES p)").unwrap();
    sql("INSERT INTO p VALUES (1, 'x', 1, 1, 1)").unwrap();
    sql("INSERT INTO c VALUES (1, 1)").unwrap();

    let (message, f) = fields(sql("INSERT INTO p VALUES (1, 'y', 0, 2, 2)").unwrap_err());
    assert_eq!(
        message,
        "duplicate key value violates unique constraint \"p_pkey\""
    );
    assert_eq!(field(&f, "detail").unwrap(), "Key (id)=(1) already exists.");
    assert_eq!(field(&f, "constraint").unwrap(), "p_pkey");
    assert_eq!(field(&f, "table").unwrap(), "p");
    assert_eq!(field(&f, "schema").unwrap(), "public");

    let (message, f) = fields(sql("INSERT INTO p VALUES (2, 'y', 0, 1, 1)").unwrap_err());
    assert_eq!(
        message,
        "duplicate key value violates unique constraint \"p_a_b_key\""
    );
    assert_eq!(
        field(&f, "detail").unwrap(),
        "Key (a, b)=(1, 1) already exists."
    );

    let (message, f) = fields(sql("INSERT INTO p VALUES (3, NULL, 0, 3, 3)").unwrap_err());
    assert_eq!(
        message,
        "null value in column \"name\" of relation \"p\" violates not-null constraint"
    );
    assert_eq!(
        field(&f, "detail").unwrap(),
        "Failing row contains (3, null, 0, 3, 3)."
    );
    assert_eq!(field(&f, "column").unwrap(), "name");

    let (message, f) = fields(sql("INSERT INTO p VALUES (4, 'z', -1, 4, 4)").unwrap_err());
    assert_eq!(
        message,
        "new row for relation \"p\" violates check constraint \"p_qty_check\""
    );
    assert_eq!(field(&f, "constraint").unwrap(), "p_qty_check");

    let (message, f) = fields(sql("INSERT INTO c VALUES (2, 99)").unwrap_err());
    assert_eq!(
        message,
        "insert or update on table \"c\" violates foreign key constraint \"c_p_id_fkey\""
    );
    assert_eq!(
        field(&f, "detail").unwrap(),
        "Key (p_id)=(99) is not present in table \"p\"."
    );

    let (message, f) = fields(sql("DELETE FROM p WHERE id = 1").unwrap_err());
    assert_eq!(
        message,
        "update or delete on table \"p\" violates foreign key constraint \"c_p_id_fkey\" on table \"c\""
    );
    assert_eq!(
        field(&f, "detail").unwrap(),
        "Key (id)=(1) is still referenced from table \"c\"."
    );
    assert_eq!(field(&f, "table").unwrap(), "c");
    assert_eq!(rows(&sql("SELECT id FROM p").unwrap()), ["1"]);
}

#[test]
fn foreign_key_actions_follow_removed_and_changed_keys() {
    let (sql, _) = session();
    sql("CREATE TABLE p (id int PRIMARY KEY, code text UNIQUE)").unwrap();
    sql("CREATE TABLE c (id int PRIMARY KEY, p_id int REFERENCES p ON DELETE CASCADE ON UPDATE CASCADE)").unwrap();
    sql("CREATE TABLE g (id int PRIMARY KEY, c_id int REFERENCES c ON DELETE CASCADE)").unwrap();
    sql("CREATE TABLE n (id int PRIMARY KEY, code text REFERENCES p (code) ON DELETE SET NULL)")
        .unwrap();
    sql("CREATE TABLE r (id int PRIMARY KEY, p_id int REFERENCES p ON DELETE RESTRICT)").unwrap();
    sql("INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    sql("INSERT INTO c VALUES (10, 1), (11, 2)").unwrap();
    sql("INSERT INTO g VALUES (100, 10), (101, 11)").unwrap();
    sql("INSERT INTO n VALUES (20, 'a')").unwrap();
    sql("INSERT INTO r VALUES (30, 3)").unwrap();

    sql("DELETE FROM p WHERE id = 1").unwrap();
    assert_eq!(rows(&sql("SELECT id FROM c").unwrap()), ["11"]);
    assert_eq!(rows(&sql("SELECT id FROM g").unwrap()), ["101"]);
    assert_eq!(rows(&sql("SELECT id, code FROM n").unwrap()), ["20|"]);

    sql("UPDATE p SET id = 22 WHERE id = 2").unwrap();
    assert_eq!(rows(&sql("SELECT id, p_id FROM c").unwrap()), ["11|22"]);

    let (message, _) = fields(sql("DELETE FROM p WHERE id = 3").unwrap_err());
    assert!(
        message.contains("violates RESTRICT setting of foreign key constraint \"r_p_id_fkey\"")
    );

    // A table referencing itself deletes a whole subtree.
    sql("CREATE TABLE t (id int PRIMARY KEY, parent int REFERENCES t ON DELETE CASCADE)").unwrap();
    sql("INSERT INTO t VALUES (1, NULL), (2, 1), (3, 2), (4, NULL)").unwrap();
    sql("DELETE FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&sql("SELECT id FROM t").unwrap()), ["4"]);
}

#[test]
fn truncate_empties_referenced_tables_only_with_their_references() {
    let (sql, notices) = session();
    sql("CREATE TABLE p (id int PRIMARY KEY)").unwrap();
    sql("CREATE TABLE c (p_id int REFERENCES p)").unwrap();
    sql("INSERT INTO p VALUES (1)").unwrap();
    sql("INSERT INTO c VALUES (1)").unwrap();
    let (message, f) = fields(sql("TRUNCATE p").unwrap_err());
    assert_eq!(
        message,
        "cannot truncate a table referenced in a foreign key constraint"
    );
    assert_eq!(
        field(&f, "detail").unwrap(),
        "Table \"c\" references \"p\"."
    );
    sql("TRUNCATE p CASCADE").unwrap();
    assert_eq!(notices(), ["truncate cascades to table \"c\""]);
    assert_eq!(rows(&sql("SELECT count(*) FROM c").unwrap()), ["0"]);
}
