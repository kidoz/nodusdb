use nodus_testkit::TestServer;
use std::time::Duration;
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=nodus password=nodus dbname=default",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port()
    );
    for _ in 0..30 {
        if let Ok((client, connection)) = tokio_postgres::connect(&conn_str, NoTls).await {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("PGWire server did not start in time");
}

fn rows_of(msgs: &[SimpleQueryMessage]) -> Vec<&tokio_postgres::SimpleQueryRow> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_transactions() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client.simple_query("CREATE SCHEMA pg18_txn;").await.unwrap();
    client.simple_query("CREATE TABLE pg18_txn.accounts (id INT PRIMARY KEY, balance NUMERIC);").await.unwrap();
    client.simple_query("INSERT INTO pg18_txn.accounts VALUES (1, 100), (2, 50);").await.unwrap();

    // Begin transaction
    client.simple_query("BEGIN;").await.unwrap();
    client.simple_query("UPDATE pg18_txn.accounts SET balance = 90 WHERE id = 1;").await.unwrap();
    client.simple_query("UPDATE pg18_txn.accounts SET balance = 60 WHERE id = 2;").await.unwrap();
    client.simple_query("COMMIT;").await.unwrap();

    let msgs = client.simple_query("SELECT balance FROM pg18_txn.accounts WHERE id = 1;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows[0].get("balance"), Some("90"));

    // Rollback test
    client.simple_query("BEGIN;").await.unwrap();
    client.simple_query("UPDATE pg18_txn.accounts SET balance = 80 WHERE id = 1;").await.unwrap();
    client.simple_query("ROLLBACK;").await.unwrap();

    let msgs = client.simple_query("SELECT balance FROM pg18_txn.accounts WHERE id = 1;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows[0].get("balance"), Some("90")); // Still 90

    // Isolation level syntax
    client.simple_query("BEGIN ISOLATION LEVEL SERIALIZABLE;").await.unwrap();
    client.simple_query("COMMIT;").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_session_management() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    // SET and SHOW
    client.simple_query("SET application_name = 'nodus_admin_test';").await.unwrap();
    // SHOW not strictly required by standard but common
    // Just ensuring SET doesn't panic
    client.simple_query("SET statement_timeout = 60000;").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_rbac() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    // Create role
    client.simple_query("CREATE ROLE test_user;").await.unwrap();
    
    // Create schema and table
    client.simple_query("CREATE SCHEMA pg18_rbac;").await.unwrap();
    client.simple_query("CREATE TABLE pg18_rbac.secure_data (id INT PRIMARY KEY, secret TEXT);").await.unwrap();

    // Grant and Revoke
    client.simple_query("GRANT SELECT ON TABLE pg18_rbac.secure_data TO test_user;").await.unwrap();
    client.simple_query("REVOKE SELECT ON TABLE pg18_rbac.secure_data FROM test_user;").await.unwrap();
}