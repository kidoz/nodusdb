//! SQL GRANT preflights every privilege and target before publishing grants.

use nodus_testkit::TestServer;
use tokio_postgres::{Client, NoTls};

async fn connect(server: &TestServer, user: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} user={user} password=nodus dbname=default",
            server.pgwire_addr.ip(),
            server.pgwire_addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

async fn fixture() -> (TestServer, Client, Client) {
    let server = TestServer::start().await.unwrap();
    let admin = connect(&server, "nodus").await;
    admin
        .batch_execute(
            "CREATE ROLE delegator LOGIN PASSWORD 'nodus';
         CREATE ROLE recipient LOGIN PASSWORD 'nodus';
         CREATE ROLE recipient2 LOGIN PASSWORD 'nodus';
         CREATE ROLE owner_role LOGIN PASSWORD 'nodus';
         CREATE TABLE grant_a (id INT PRIMARY KEY, v INT);
         CREATE TABLE grant_b (id INT PRIMARY KEY, v INT);
         INSERT INTO grant_a VALUES (1, 10);
         GRANT SELECT ON grant_a TO delegator WITH GRANT OPTION;",
        )
        .await
        .unwrap();
    let delegator = connect(&server, "delegator").await;
    (server, admin, delegator)
}

async fn no_recipient_grants(admin: &Client) {
    let count: i64 = admin
        .query_one(
            "SELECT COUNT(*) FROM information_schema.table_privileges
         WHERE grantee IN ('recipient', 'recipient2') AND table_name IN ('grant_a', 'grant_b')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0, "a rejected GRANT must not publish any privileges");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_privileges_cannot_escalate_delegation() {
    let (server, admin, delegator) = fixture().await;
    for privileges in ["SELECT, UPDATE", "UPDATE, SELECT", "ALL"] {
        let error = delegator
            .batch_execute(&format!(
                "GRANT {privileges} ON grant_a TO recipient, recipient2"
            ))
            .await
            .expect_err("SELECT grant option cannot delegate UPDATE");
        assert_eq!(error.code().unwrap().code(), "42501");
        no_recipient_grants(&admin).await;
    }
    let recipient = connect(&server, "recipient").await;
    assert!(
        recipient
            .batch_execute("UPDATE grant_a SET v = 99")
            .await
            .is_err()
    );
    let value: i32 = admin
        .query_one("SELECT v FROM grant_a", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, 10);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unauthorized_later_target_leaves_no_partial_grants() {
    let (server, admin, delegator) = fixture().await;
    for statement in [
        "GRANT SELECT ON grant_a, grant_b TO recipient, recipient2",
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO recipient, recipient2",
    ] {
        let error = delegator
            .batch_execute(statement)
            .await
            .expect_err("delegator has no grant option on grant_b");
        assert_eq!(error.code().unwrap().code(), "42501");
        no_recipient_grants(&admin).await;
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_later_privilege_leaves_no_partial_grants() {
    let (server, admin, delegator) = fixture().await;
    assert!(
        delegator
            .batch_execute("GRANT SELECT, USAGE ON TABLE grant_a TO recipient, recipient2")
            .await
            .is_err()
    );
    no_recipient_grants(&admin).await;
    let public_before: i64 = admin
        .query_one(
            "SELECT COUNT(*) FROM pg_roles WHERE rolname = 'public'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        delegator
            .batch_execute("GRANT SELECT, UPDATE ON grant_a TO PUBLIC")
            .await
            .is_err()
    );
    assert!(
        delegator
            .batch_execute("GRANT SELECT ON grant_a TO PUBLIC, no_such_recipient")
            .await
            .is_err()
    );
    let public_after: i64 = admin
        .query_one(
            "SELECT COUNT(*) FROM pg_roles WHERE rolname = 'public'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(public_before, public_after, "denial must not create PUBLIC");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_all_without_grant_option_cannot_delegate() {
    let (server, admin, delegator) = fixture().await;
    admin
        .batch_execute("REVOKE ALL ON grant_a FROM delegator; GRANT ALL ON grant_a TO delegator")
        .await
        .unwrap();
    let error = delegator
        .batch_execute("GRANT SELECT ON grant_a TO recipient")
        .await
        .expect_err("object ALL does not include grant management");
    assert_eq!(error.code().unwrap().code(), "42501");
    no_recipient_grants(&admin).await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inherited_grant_options_allow_all_and_are_rechecked_after_revoke() {
    let (server, admin, delegator) = fixture().await;
    admin
        .batch_execute(
            "REVOKE ALL ON grant_a FROM delegator;
         CREATE ROLE grant_holder;
         GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
             ON grant_a, grant_b TO grant_holder WITH GRANT OPTION;
         GRANT grant_holder TO delegator;",
        )
        .await
        .unwrap();
    delegator
        .batch_execute("GRANT ALL ON grant_a, grant_b TO recipient WITH GRANT OPTION")
        .await
        .unwrap();
    let recipient = connect(&server, "recipient").await;
    let error = recipient
        .batch_execute("DROP TABLE grant_a")
        .await
        .expect_err("delegated ALL data privileges do not convey ownership");
    assert_eq!(error.code().unwrap().code(), "42501");
    recipient
        .batch_execute("UPDATE grant_a SET v = 20; GRANT SELECT ON grant_a TO recipient2")
        .await
        .unwrap();
    let reader = connect(&server, "recipient2").await;
    assert_eq!(
        reader
            .query_one("SELECT v FROM grant_a", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        20
    );
    admin
        .batch_execute("REVOKE ALL ON grant_a FROM recipient2")
        .await
        .unwrap();
    let prepared = delegator
        .prepare("GRANT SELECT ON grant_a TO recipient2")
        .await
        .unwrap();
    admin
        .batch_execute("REVOKE GRANT OPTION FOR SELECT ON grant_a FROM grant_holder")
        .await
        .unwrap();
    let error = delegator
        .execute(&prepared, &[])
        .await
        .expect_err("revocation must affect prepared execution");
    assert_eq!(error.code().unwrap().code(), "42501");
    assert!(reader.query("SELECT v FROM grant_a", &[]).await.is_err());
    delegator
        .batch_execute("GRANT SELECT ON grant_b TO recipient2")
        .await
        .unwrap();
    assert!(
        reader
            .query("SELECT * FROM grant_b", &[])
            .await
            .unwrap()
            .is_empty()
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_and_superuser_can_grant_without_explicit_grant_options() {
    let (server, admin, _delegator) = fixture().await;
    admin
        .batch_execute(
            "ALTER TABLE grant_a OWNER TO owner_role;
         CREATE ROLE sql_admin LOGIN SUPERUSER PASSWORD 'nodus';",
        )
        .await
        .unwrap();
    let owner = connect(&server, "owner_role").await;
    owner
        .batch_execute("GRANT SELECT, UPDATE ON grant_a TO recipient")
        .await
        .unwrap();
    let superuser = connect(&server, "sql_admin").await;
    superuser
        .batch_execute("GRANT SELECT ON grant_b TO recipient2")
        .await
        .unwrap();
    let recipient = connect(&server, "recipient").await;
    recipient
        .batch_execute("UPDATE grant_a SET v = 30")
        .await
        .unwrap();
    assert_eq!(
        recipient
            .query_one("SELECT v FROM grant_a", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        30
    );
    let recipient2 = connect(&server, "recipient2").await;
    assert!(
        recipient2
            .query("SELECT * FROM grant_b", &[])
            .await
            .unwrap()
            .is_empty()
    );
    server.shutdown().await;
}
