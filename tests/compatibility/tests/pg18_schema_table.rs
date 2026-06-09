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
async fn test_pg18_schema_operations() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    // 1. Create Schema
    client
        .simple_query("CREATE SCHEMA test_schema;")
        .await
        .expect("Failed to create schema");

    // 2. Create Table in Schema
    client
        .simple_query("CREATE TABLE test_schema.users (id INT PRIMARY KEY, name TEXT, created_at TIMESTAMP);")
        .await
        .expect("Failed to create table in schema");

    // 3. Insert data
    client
        .simple_query("INSERT INTO test_schema.users (id, name, created_at) VALUES (1, 'Alice', '2026-01-01 10:00:00');")
        .await
        .expect("Failed to insert into schema table");

    // 4. Select data
    let msgs = client
        .simple_query("SELECT id, name FROM test_schema.users WHERE id = 1;")
        .await
        .expect("Failed to select from schema table");
    
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some("1"));
    assert_eq!(rows[0].get("name"), Some("Alice"));

    // 5. Update data
    client
        .simple_query("UPDATE test_schema.users SET name = 'Bob' WHERE id = 1;")
        .await
        .expect("Failed to update schema table");

    let msgs = client
        .simple_query("SELECT name FROM test_schema.users WHERE id = 1;")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows[0].get("name"), Some("Bob"));

    // 6. Delete data
    client
        .simple_query("DELETE FROM test_schema.users WHERE id = 1;")
        .await
        .expect("Failed to delete from schema table");

    let msgs = client
        .simple_query("SELECT * FROM test_schema.users;")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 0);

    // 7. Drop Table
    client
        .simple_query("DROP TABLE test_schema.users;")
        .await
        .expect("Failed to drop table");

    // 8. Drop Schema
    client
        .simple_query("DROP SCHEMA test_schema;")
        .await
        .expect("Failed to drop schema");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_table_alterations() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE TABLE alter_test (id INT PRIMARY KEY);")
        .await
        .expect("Failed to create table");

    // Add column
    client
        .simple_query("ALTER TABLE alter_test ADD COLUMN description TEXT;")
        .await
        .expect("Failed to add column");

    client
        .simple_query("INSERT INTO alter_test (id, description) VALUES (1, 'Test desc');")
        .await
        .unwrap();

    let msgs = client.simple_query("SELECT description FROM alter_test WHERE id = 1;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows[0].get("description"), Some("Test desc"));

    // Rename column
    client
        .simple_query("ALTER TABLE alter_test RENAME COLUMN description TO details;")
        .await
        .expect("Failed to rename column");

    let msgs = client.simple_query("SELECT details FROM alter_test WHERE id = 1;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows[0].get("details"), Some("Test desc"));

    // Drop column
    client
        .simple_query("ALTER TABLE alter_test DROP COLUMN details;")
        .await
        .expect("Failed to drop column");

    client
        .simple_query("DROP TABLE alter_test;")
        .await
        .expect("Failed to drop table");
}
