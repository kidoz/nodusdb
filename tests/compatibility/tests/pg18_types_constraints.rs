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
async fn test_pg18_not_null_unique() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA pg18_types_schema;")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE pg18_types_schema.users (id INT PRIMARY KEY, username TEXT NOT NULL UNIQUE, email TEXT UNIQUE);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO pg18_types_schema.users (id, username, email) VALUES (1, 'alice', 'alice@example.com');")
        .await
        .unwrap();

    // Should fail: NOT NULL constraint
    let err = client
        .simple_query("INSERT INTO pg18_types_schema.users (id, username, email) VALUES (2, NULL, 'bob@example.com');")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("Expected DbError");
    assert!(
        db_err.message().contains("cannot be NULL") || db_err.message().contains("NOT NULL"),
        "Expected NOT NULL error, got: {}",
        db_err.message()
    );

    // Should fail: UNIQUE constraint
    let err = client
        .simple_query("INSERT INTO pg18_types_schema.users (id, username, email) VALUES (3, 'alice', 'alice2@example.com');")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("Expected DbError");
    assert!(
        db_err.message().contains("Unique constraint violation")
            || db_err.message().contains("UNIQUE")
            || db_err.message().contains("Duplicate")
            || db_err.message().contains("conflict"),
        "Expected UNIQUE error, got: {}",
        db_err.message()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_check_constraint() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA pg18_types_schema;")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE pg18_types_schema.products (id INT PRIMARY KEY, name TEXT NOT NULL, price NUMERIC CHECK (price > 0));")
        .await
        .unwrap();

    client
        .simple_query(
            "INSERT INTO pg18_types_schema.products (id, name, price) VALUES (1, 'Widget', 10.50);",
        )
        .await
        .unwrap();

    let err = client
        .simple_query(
            "INSERT INTO pg18_types_schema.products (id, name, price) VALUES (2, 'Freebie', 0);",
        )
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("Expected DbError");
    assert!(
        db_err.message().contains("violates check constraint")
            || db_err.message().contains("CHECK")
            || db_err.message().contains("check constraint"),
        "Expected CHECK constraint error, got: {}",
        db_err.message()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_foreign_key() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA pg18_types_schema;")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE pg18_types_schema.users (id INT PRIMARY KEY);")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE pg18_types_schema.orders (order_id INT PRIMARY KEY, user_id INT REFERENCES pg18_types_schema.users(id), amount NUMERIC);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO pg18_types_schema.users (id) VALUES (1);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO pg18_types_schema.orders (order_id, user_id, amount) VALUES (101, 1, 50.0);")
        .await
        .unwrap();

    let err = client
        .simple_query("INSERT INTO pg18_types_schema.orders (order_id, user_id, amount) VALUES (102, 999, 10.0);")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("Expected DbError");
    assert!(
        db_err.message().contains("violates foreign key constraint")
            || db_err.message().contains("FOREIGN KEY")
            || db_err.message().contains("foreign key constraint"),
        "Expected FK error, got: {}",
        db_err.message()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_data_types() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA pg18_types_schema;")
        .await
        .unwrap();

    client
        .simple_query(
            "CREATE TABLE pg18_types_schema.all_types (
            id INT PRIMARY KEY,
            is_active BOOLEAN,
            score REAL,
            rating DOUBLE PRECISION,
            created_at TIMESTAMP,
            start_date DATE
        );",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO pg18_types_schema.all_types (id, is_active, score, rating, created_at, start_date) VALUES (1, true, 4.5, 4.555, '2026-06-08 12:00:00', '2026-06-08');")
        .await
        .unwrap();

    let msgs = client
        .simple_query("SELECT id, is_active, score, rating, created_at, start_date FROM pg18_types_schema.all_types WHERE id = 1;")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);

    assert_eq!(rows[0].get("id"), Some("1"));
    assert_eq!(rows[0].get("is_active"), Some("t"));
    assert_eq!(rows[0].get("score"), Some("4.5"));
    assert_eq!(rows[0].get("rating"), Some("4.555"));
    assert_eq!(rows[0].get("created_at"), Some("2026-06-08 12:00:00"));
    assert_eq!(rows[0].get("start_date"), Some("2026-06-08"));
}
