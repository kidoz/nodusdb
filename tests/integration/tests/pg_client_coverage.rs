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
async fn test_full_pg_client_coverage() {
    let server = TestServer::start().await.expect("Failed to start server");
    let client = connect(&server.pgwire_addr).await;

    // ---------------------------------------------------------
    // 1. Data Types & Table Creation
    // ---------------------------------------------------------
    client
        .execute(
            "CREATE TABLE employees (id INT, name TEXT, is_active BOOL)",
            &[],
        )
        .await
        .expect("Failed to create employees table");

    client
        .execute("CREATE TABLE departments (id INT, dept_name TEXT)", &[])
        .await
        .expect("Failed to create departments table");

    // ---------------------------------------------------------
    // 2. Insert with Parameters
    // ---------------------------------------------------------
    let insert_emp = client
        .prepare_typed(
            "INSERT INTO employees (id, name, is_active) VALUES ($1, $2, $3)",
            &[
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::BOOL,
            ],
        )
        .await
        .expect("Failed to prepare insert employee");

    client
        .execute(&insert_emp, &[&1i32, &"Alice", &true])
        .await
        .unwrap();
    client
        .execute(&insert_emp, &[&2i32, &"Bob", &true])
        .await
        .unwrap();
    client
        .execute(&insert_emp, &[&3i32, &"Charlie", &false])
        .await
        .unwrap();

    let insert_dept = client
        .prepare_typed(
            "INSERT INTO departments (id, dept_name) VALUES ($1, $2)",
            &[
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::TEXT,
            ],
        )
        .await
        .unwrap();

    client
        .execute(&insert_dept, &[&1i32, &"Engineering"])
        .await
        .unwrap();
    client.execute(&insert_dept, &[&2i32, &"HR"]).await.unwrap();

    // ---------------------------------------------------------
    // 2.5 Multi-Row INSERT
    // ---------------------------------------------------------
    let multi_insert_emp = client
        .prepare_typed(
            "INSERT INTO employees (id, name, is_active) VALUES ($1, $2, $3), ($4, $5, $6)",
            &[
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::BOOL,
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::BOOL,
            ],
        )
        .await
        .unwrap();

    let rows_inserted = client
        .execute(
            &multi_insert_emp,
            &[&10i32, &"Zara", &true, &11i32, &"Xander", &false],
        )
        .await
        .unwrap();
    assert_eq!(rows_inserted, 2);

    // ---------------------------------------------------------
    // 3. SELECT with WHERE, ORDER BY, LIMIT
    // ---------------------------------------------------------
    let select_active = client
        .prepare_typed(
            "SELECT id, name FROM employees WHERE is_active = $1 ORDER BY id DESC LIMIT 1",
            &[tokio_postgres::types::Type::BOOL],
        )
        .await
        .unwrap();

    let rows = client.query(&select_active, &[&true]).await.unwrap();
    assert_eq!(rows.len(), 1);

    // NodusDB returns everything as string/varchar for now in Extended Query Handler Describe.
    let id_str: &str = rows[0].get(0);
    let name_str: &str = rows[0].get(1);
    assert_eq!(id_str, "10");
    assert_eq!(name_str, "Zara");

    // ---------------------------------------------------------
    // 4. UPDATE and DELETE
    // ---------------------------------------------------------
    let update_emp = client
        .prepare_typed(
            "UPDATE employees SET name = $1 WHERE id = $2",
            &[
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::INT4,
            ],
        )
        .await
        .unwrap();
    client
        .execute(&update_emp, &[&"Alice Updated", &1i32])
        .await
        .unwrap();

    let delete_emp = client
        .prepare_typed(
            "DELETE FROM employees WHERE id = $1",
            &[tokio_postgres::types::Type::INT4],
        )
        .await
        .unwrap();
    client.execute(&delete_emp, &[&3i32]).await.unwrap();

    // Verify
    let rows = client
        .query("SELECT name FROM employees WHERE id = 1", &[])
        .await
        .unwrap();
    let name_str: &str = rows[0].get(0);
    assert_eq!(name_str, "Alice Updated");

    let rows = client.query("SELECT id FROM employees", &[]).await.unwrap();
    assert_eq!(rows.len(), 4, "Alice, Bob, Zara, Xander should remain");

    // ---------------------------------------------------------
    // 5. Transactions (BEGIN, COMMIT, ROLLBACK)
    // ---------------------------------------------------------
    client.execute("BEGIN", &[]).await.unwrap();
    client
        .execute(&insert_emp, &[&4i32, &"Dave", &true])
        .await
        .unwrap();
    client.execute("ROLLBACK", &[]).await.unwrap();

    let rows = client
        .query("SELECT id FROM employees WHERE id = 4", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 0, "Dave should have been rolled back");

    client.execute("BEGIN", &[]).await.unwrap();
    client
        .execute(&insert_emp, &[&5i32, &"Eve", &true])
        .await
        .unwrap();
    client.execute("COMMIT", &[]).await.unwrap();

    let rows = client
        .query("SELECT id FROM employees WHERE id = 5", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "Eve should have been committed");

    // ---------------------------------------------------------
    // 7. SHOW and Literal SELECTs
    // ---------------------------------------------------------
    let rows = client.query("SELECT version()", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let version: &str = rows[0].get(0);
    assert!(version.contains("NodusDB"));

    let rows = client.query("SHOW search_path", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let path: &str = rows[0].get(0);
    assert_eq!(path, "public");

    // Clean up (may fail if DROP TABLE is unsupported, which is fine)
    let _ = client.execute("DROP TABLE employees", &[]).await;
    let _ = client.execute("DROP TABLE departments", &[]).await;
}
