use nodus_testkit::TestServer;
use std::time::Duration;
use tokio_postgres::NoTls;

async fn connect(addr: &std::net::SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=nodus password=nodus dbname=default",
        addr.ip(),
        addr.port()
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
    panic!("Failed to connect to pgwire");
}

#[tokio::test]
async fn test_crud_operations() {
    let server = TestServer::start().await.expect("Failed to start server");
    let client = connect(&server.pgwire_addr).await;

    // 1. Create table
    client
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT)",
            &[],
        )
        .await
        .expect("Failed to create table");

    // 2. Create (Insert) - Using Extended Query with parameters
    let insert_stmt = client
        .prepare_typed(
            "INSERT INTO users (id, name, age) VALUES ($1, $2, $3)",
            &[tokio_postgres::types::Type::INT4, tokio_postgres::types::Type::VARCHAR, tokio_postgres::types::Type::INT4],
        )
        .await
        .expect("Failed to prepare insert");

    let rows_inserted = client
        .execute(&insert_stmt, &[&1i32, &"Alice", &30i32])
        .await
        .expect("Failed to insert Alice");
    assert_eq!(rows_inserted, 1);

    let rows_inserted = client
        .execute(&insert_stmt, &[&2i32, &"Bob", &25i32])
        .await
        .expect("Failed to insert Bob");
    assert_eq!(rows_inserted, 1);

    // 3. Read (Select)
    let rows = client
        .query("SELECT id, name, age FROM users ORDER BY id", &[])
        .await
        .expect("Failed to select");
    assert_eq!(rows.len(), 2);

    let id_str: &str = rows[0].get(0);
    let name: &str = rows[0].get(1);
    let age_str: &str = rows[0].get(2);
    assert_eq!(id_str, "1");
    assert_eq!(name, "Alice");
    assert_eq!(age_str, "30");

    // 4. Update
    let update_stmt = client
        .prepare_typed(
            "UPDATE users SET age = $1 WHERE name = $2",
            &[tokio_postgres::types::Type::INT4, tokio_postgres::types::Type::VARCHAR],
        )
        .await
        .expect("Failed to prepare update");

    let _rows_updated = client
        .execute(&update_stmt, &[&31i32, &"Alice"])
        .await
        .expect("Failed to update");

    // Verify update
    let select_stmt = client
        .prepare_typed(
            "SELECT age FROM users WHERE name = $1",
            &[tokio_postgres::types::Type::VARCHAR],
        )
        .await
        .expect("Failed to prepare select");

    let row = client
        .query_one(&select_stmt, &[&"Alice"])
        .await
        .expect("Failed to select after update");
    let updated_age_str: &str = row.get(0);
    assert_eq!(updated_age_str, "31");

    // 5. Delete
    let delete_stmt = client
        .prepare_typed(
            "DELETE FROM users WHERE name = $1",
            &[tokio_postgres::types::Type::VARCHAR],
        )
        .await
        .expect("Failed to prepare delete");

    let _rows_deleted = client
        .execute(&delete_stmt, &[&"Bob"])
        .await
        .expect("Failed to delete");

    // Verify delete
    let rows = client
        .query("SELECT * FROM users", &[])
        .await
        .expect("Failed to select after delete");
    assert_eq!(rows.len(), 1);

    // 6. Drop Table
    client
        .execute("DROP TABLE users", &[])
        .await
        .expect("Failed to drop table");
}
