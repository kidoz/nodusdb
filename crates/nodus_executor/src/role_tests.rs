//! Roles, privileges, memberships, and the role catalogs, driven through
//! SQL. Expectations from PostgreSQL 18.4.

use super::*;
use crate::dml_join_tests::rows;
use crate::error_fields::error_fields;
use nodus_audit::MemoryAuditSink;

/// A session helper: a superuser running one statement, one running as
/// another principal, and the notices raised.
struct Harness {
    exec: Arc<MemExecutor>,
    admin: ExecutionContext,
    catalog: Arc<nodus_catalog::MemoryCatalog>,
    notices: Arc<MemExecutor>,
}

impl Harness {
    fn new() -> Harness {
        let (exec, catalog) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = catalog
            .create_role(nodus_catalog::CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "nodus".into(),
                principal_type: nodus_catalog::PrincipalType::User,
                database_id: None,
                attributes: nodus_catalog::RoleAttributes {
                    superuser: true,
                    can_login: true,
                    ..Default::default()
                },
            })
            .unwrap();
        catalog
            .grant_privilege(nodus_catalog::GrantPrivilegeRequest {
                id: nodus_catalog::GrantId::new(),
                principal_id: admin.id,
                resource: nodus_catalog::ResourceRef::System,
                privilege: "ALL".into(),
                grantable: false,
                grantor: None,
            })
            .unwrap();
        Harness {
            notices: exec.clone(),
            exec,
            admin: ctx_for(admin.id),
            catalog,
        }
    }

    fn sql(&self, sql: &str) -> Result<QueryOutput> {
        let ctx = self.admin.clone();
        self.run(&ctx, sql)
    }

    fn run(&self, ctx: &ExecutionContext, sql: &str) -> Result<QueryOutput> {
        let mut statements = nodus_sql::parse_sql(sql)?;
        let plan = plan_statement(&statements.remove(0), &[])?;
        self.exec.execute_logical(ctx, plan)
    }

    /// The notices raised so far, newest last.
    fn notices(&self) -> Vec<String> {
        self.notices
            .take_notices("test")
            .iter()
            .map(|notice| crate::error_message(&notice.to_string()).to_string())
            .collect()
    }

    /// A login-capable principal without privileges, and its context.
    fn principal(&self, name: &str) -> (nodus_catalog::PrincipalDescriptor, ExecutionContext) {
        let principal = self
            .catalog
            .create_role(nodus_catalog::CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: name.into(),
                principal_type: nodus_catalog::PrincipalType::User,
                database_id: None,
                attributes: nodus_catalog::RoleAttributes {
                    can_login: true,
                    ..Default::default()
                },
            })
            .unwrap();
        let ctx = ctx_for(principal.id);
        (principal, ctx)
    }
}

fn ctx_for(principal: nodus_catalog::PrincipalId) -> ExecutionContext {
    ExecutionContext {
        session_id: "test".to_string(),
        principal_id: principal,
        active_roles: vec![],
        authz_catalog_version: 1,
    }
}

#[test]
fn role_attributes_round_trip() {
    let h = Harness::new();
    h.sql("CREATE ROLE ra CREATEDB CREATEROLE CONNECTION LIMIT 3")
        .unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT rolname, rolcanlogin, rolcreatedb, rolcreaterole, rolconnlimit FROM pg_roles WHERE rolname = 'ra'")
                .unwrap()
        ),
        ["ra|f|t|t|3"]
    );
    h.sql("ALTER ROLE ra NOLOGIN CONNECTION LIMIT -1").unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT rolconnlimit FROM pg_roles WHERE rolname = 'ra'")
                .unwrap()
        ),
        ["-1"]
    );
    h.sql("ALTER ROLE ra RENAME TO rax").unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT rolname FROM pg_roles WHERE rolname LIKE 'ra%'")
                .unwrap()
        ),
        ["rax"]
    );
    // The rename keeps the role, so a second role cannot take the name.
    let error = h.sql("CREATE ROLE rax").unwrap_err().to_string();
    assert_eq!(crate::error_message(&error), "role \"rax\" already exists");
    // Its settings land in `pg_db_role_setting`, keyed by the new name.
    h.sql("ALTER ROLE rax SET search_path TO public").unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT setconfig FROM pg_db_role_setting WHERE setrole = 'rax'::regrole::oid")
                .unwrap()
        ),
        ["{search_path=public}"]
    );
    h.sql("ALTER ROLE rax RESET search_path").unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT count(*) FROM pg_db_role_setting WHERE setrole = 'rax'::regrole::oid")
                .unwrap()
        ),
        ["0"]
    );
}

#[test]
fn grants_carry_the_grant_option() {
    let h = Harness::new();
    h.sql("CREATE ROLE ga").unwrap();
    h.sql("CREATE TABLE gt (a int)").unwrap();
    h.sql("GRANT SELECT, INSERT ON gt TO ga").unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT has_table_privilege('ga', 'gt', 'SELECT'), has_table_privilege('ga', 'gt', 'DELETE')")
                .unwrap()
        ),
        ["t|f"]
    );
    h.sql("GRANT UPDATE ON gt TO ga WITH GRANT OPTION").unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT has_table_privilege('ga', 'gt', 'UPDATE WITH GRANT OPTION'), has_table_privilege('ga', 'gt', 'SELECT WITH GRANT OPTION')")
                .unwrap()
        ),
        ["t|f"]
    );
    h.sql("REVOKE SELECT ON gt FROM ga").unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT has_table_privilege('ga', 'gt', 'SELECT')")
                .unwrap()
        ),
        ["f"]
    );
    // All of them can go at once.
    h.sql("REVOKE ALL ON gt FROM ga").unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT count(*) FROM information_schema.table_privileges WHERE table_name = 'gt' AND grantee = 'ga'")
                .unwrap()
        ),
        ["0"]
    );
}

#[test]
fn memberships_and_their_admin_option() {
    let h = Harness::new();
    h.sql("CREATE ROLE ma").unwrap();
    h.sql("CREATE ROLE mb").unwrap();
    h.sql("GRANT ma TO mb").unwrap();
    h.sql("GRANT ma TO mb").unwrap();
    assert_eq!(
        h.notices(),
        ["role \"mb\" has already been granted membership in role \"ma\" by role \"nodus\""]
    );
    assert_eq!(
        rows(
            &h.sql("SELECT roleid::regrole, member::regrole, admin_option FROM pg_auth_members WHERE roleid = 'ma'::regrole")
                .unwrap()
        ),
        ["ma|mb|f"]
    );
    h.sql("GRANT ma TO mb WITH ADMIN OPTION").unwrap();
    assert_eq!(
        rows(
            &h.sql("SELECT admin_option FROM pg_auth_members WHERE roleid = 'ma'::regrole")
                .unwrap()
        ),
        ["t"]
    );
    assert_eq!(
        rows(
            &h.sql("SELECT pg_has_role('mb', 'ma', 'MEMBER'), pg_has_role('ma', 'mb', 'MEMBER')")
                .unwrap()
        ),
        ["t|f"]
    );
    // A membership that would close a cycle, as written.
    let error = h.sql("GRANT mb TO ma").unwrap_err().to_string();
    assert_eq!(
        crate::error_message(&error),
        "role \"mb\" is a member of role \"ma\""
    );
    let error = h.sql("GRANT ma TO ma").unwrap_err().to_string();
    assert_eq!(
        crate::error_message(&error),
        "role \"ma\" is a member of role \"ma\""
    );
    // Revoking what was never granted is a warning, not an error.
    h.sql("REVOKE ma FROM mb").unwrap();
    h.sql("REVOKE ma FROM mb").unwrap();
    assert_eq!(
        h.notices(),
        ["role \"mb\" has not been granted membership in role \"ma\" by role \"nodus\""]
    );
    assert_eq!(
        rows(&h.sql("SELECT pg_has_role('mb', 'ma', 'MEMBER')").unwrap()),
        ["f"]
    );
}

#[test]
fn ownership_and_drop_dependencies() {
    let h = Harness::new();
    h.sql("CREATE ROLE oa").unwrap();
    h.sql("CREATE SCHEMA osch").unwrap();
    h.sql("CREATE TABLE ot (a int)").unwrap();
    h.sql("ALTER SCHEMA osch OWNER TO oa").unwrap();
    h.sql("ALTER TABLE ot OWNER TO oa").unwrap();
    // The owner holds every privilege, grantable.
    assert_eq!(
        rows(
            &h.sql("SELECT has_table_privilege('oa', 'ot', 'SELECT'), has_table_privilege('oa', 'ot', 'TRUNCATE'), has_table_privilege('oa', 'ot', 'SELECT WITH GRANT OPTION')")
                .unwrap()
        ),
        ["t|t|t"]
    );
    assert_eq!(
        rows(
            &h.sql(
                "SELECT schema_owner FROM information_schema.schemata WHERE schema_name = 'osch'"
            )
            .unwrap()
        ),
        ["oa"]
    );
    // What it owns blocks the drop, and the found objects are named.
    let error = h.sql("DROP ROLE oa").unwrap_err().to_string();
    assert_eq!(
        crate::error_message(&error),
        "role \"oa\" cannot be dropped because some objects depend on it"
    );
    assert_eq!(
        error_fields(&error)
            .into_iter()
            .filter(|(name, _)| *name == "detail")
            .map(|(_, value)| value)
            .collect::<Vec<_>>(),
        ["owner of schema osch\nowner of table ot"]
    );
    h.sql("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM oa")
        .unwrap();
    // Privileges it holds block it too.
    h.sql("GRANT SELECT ON ot TO oa").unwrap();
    let error = h.sql("DROP ROLE oa").unwrap_err().to_string();
    assert_eq!(
        error_fields(&error)
            .into_iter()
            .filter(|(name, _)| *name == "detail")
            .map(|(_, value)| value)
            .collect::<Vec<_>>(),
        ["owner of schema osch\nowner of table ot\nprivileges for table ot"]
    );
}

#[test]
fn table_privileges_show_the_owner_last() {
    let h = Harness::new();
    h.sql("CREATE ROLE pa").unwrap();
    h.sql("CREATE TABLE pt (a int)").unwrap();
    h.sql("GRANT SELECT, INSERT ON pt TO pa WITH GRANT OPTION")
        .unwrap();
    h.sql("GRANT ALL ON pt TO PUBLIC").unwrap();
    // PostgreSQL walks the ACL backwards: PUBLIC, the grantees, the owner.
    assert_eq!(
        rows(
            &h.sql("SELECT grantee, privilege_type FROM information_schema.table_privileges WHERE table_name = 'pt'")
                .unwrap()
        ),
        [
            "PUBLIC|DELETE",
            "PUBLIC|INSERT",
            "PUBLIC|REFERENCES",
            "PUBLIC|SELECT",
            "PUBLIC|TRIGGER",
            "PUBLIC|TRUNCATE",
            "PUBLIC|UPDATE",
            "nodus|DELETE",
            "nodus|INSERT",
            "nodus|REFERENCES",
            "nodus|SELECT",
            "nodus|TRIGGER",
            "nodus|TRUNCATE",
            "nodus|UPDATE",
            "pa|INSERT",
            "pa|SELECT",
        ]
    );
    assert_eq!(
        rows(
            &h.sql("SELECT is_grantable, with_hierarchy FROM information_schema.table_privileges WHERE table_name = 'pt' AND grantee = 'pa' AND privilege_type = 'SELECT'")
                .unwrap()
        ),
        ["YES|YES"]
    );
}

#[test]
fn role_ddl_authority() {
    let h = Harness::new();
    let (user, ctx) = h.principal("ua");
    assert!(user.attributes.can_login);
    // Without CREATEROLE, PostgreSQL's messages and details.
    let error = h.run(&ctx, "CREATE ROLE bad").unwrap_err().to_string();
    assert_eq!(
        crate::error_message(&error),
        "permission denied to create role"
    );
    assert_eq!(
        error_fields(&error)
            .into_iter()
            .filter(|(name, _)| *name == "detail")
            .map(|(_, value)| value)
            .collect::<Vec<_>>(),
        ["Only roles with the CREATEROLE attribute may create roles."]
    );
    h.sql("ALTER ROLE ua CREATEROLE").unwrap();
    h.run(&ctx, "CREATE ROLE child").unwrap();
    // The creator holds the admin option on what it created.
    assert_eq!(
        rows(
            &h.sql("SELECT member::regrole, admin_option FROM pg_auth_members WHERE roleid = 'child'::regrole")
                .unwrap()
        ),
        ["ua|t"]
    );
    h.run(&ctx, "ALTER ROLE child NOLOGIN").unwrap();
    h.run(&ctx, "DROP ROLE child").unwrap();
    // A role it does not administer stays out of reach.
    h.sql("CREATE ROLE other").unwrap();
    let error = h
        .run(&ctx, "ALTER ROLE other NOLOGIN")
        .unwrap_err()
        .to_string();
    assert_eq!(
        error_fields(&error)
            .into_iter()
            .filter(|(name, _)| *name == "detail")
            .map(|(_, value)| value)
            .collect::<Vec<_>>(),
        [
            "Only roles with the CREATEROLE attribute and the ADMIN option on role \"other\" may alter this role."
        ]
    );
    let error = h.run(&ctx, "DROP ROLE other").unwrap_err().to_string();
    assert_eq!(
        error_fields(&error)
            .into_iter()
            .filter(|(name, _)| *name == "detail")
            .map(|(_, value)| value)
            .collect::<Vec<_>>(),
        [
            "Only roles with the CREATEROLE attribute and the ADMIN option on role \"other\" may drop this role."
        ]
    );
    // Only a superuser changes the superuser attribute.
    let error = h
        .run(&ctx, "ALTER ROLE ua SUPERUSER")
        .unwrap_err()
        .to_string();
    assert_eq!(
        error_fields(&error)
            .into_iter()
            .filter(|(name, _)| *name == "detail")
            .map(|(_, value)| value)
            .collect::<Vec<_>>(),
        ["Only roles with the SUPERUSER attribute may change the SUPERUSER attribute."]
    );
}

#[test]
fn set_role_switches_the_session() {
    let h = Harness::new();
    let (_role, role_ctx) = h.principal("sa");
    h.sql("SET ROLE sa").unwrap();
    assert_eq!(
        rows(&h.sql("SELECT current_user, session_user").unwrap()),
        ["sa|nodus"]
    );
    h.sql("RESET ROLE").unwrap();
    assert_eq!(
        rows(&h.sql("SELECT current_user, session_user").unwrap()),
        ["nodus|nodus"]
    );
    // A role may not set a role it is not a member of.
    let error = h.run(&role_ctx, "SET ROLE nodus").unwrap_err().to_string();
    assert_eq!(
        crate::error_message(&error),
        "permission denied to set role \"nodus\""
    );
}
