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
async fn test_pg18_indexes() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA pg18_indexes;")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE TABLE pg18_indexes.base_table (id INT PRIMARY KEY, val TEXT, category TEXT);",
        )
        .await
        .unwrap();
    client.simple_query("INSERT INTO pg18_indexes.base_table VALUES (1, 'A', 'CAT1'), (2, 'B', 'CAT1'), (3, 'C', 'CAT2');").await.unwrap();

    // Create a secondary index
    client
        .simple_query("CREATE INDEX idx_category ON pg18_indexes.base_table (category);")
        .await
        .unwrap();

    // Query utilizing the index implicitly
    let msgs = client
        .simple_query("SELECT id FROM pg18_indexes.base_table WHERE category = 'CAT1' ORDER BY id;")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some("1"));
    assert_eq!(rows[1].get("id"), Some("2"));

    // Create a unique index
    client
        .simple_query("CREATE UNIQUE INDEX idx_val_unique ON pg18_indexes.base_table (val);")
        .await
        .unwrap();

    // Attempting to insert a duplicate value should fail if unique index is enforced
    let err = client
        .simple_query("INSERT INTO pg18_indexes.base_table VALUES (4, 'A', 'CAT3');")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("Expected DbError");
    assert!(
        db_err.message().contains("unique")
            || db_err.message().contains("UNIQUE")
            || db_err.message().contains("Duplicate")
            || db_err.message().contains("conflict"),
        "Expected UNIQUE error, got: {}",
        db_err.message()
    );

    client
        .simple_query("DROP TABLE pg18_indexes.base_table;")
        .await
        .unwrap();
}
