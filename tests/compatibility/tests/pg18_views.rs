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
async fn test_pg18_views() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client.simple_query("CREATE SCHEMA pg18_views;").await.unwrap();
    client.simple_query("CREATE TABLE pg18_views.base_table (id INT, val TEXT);").await.unwrap();
    client.simple_query("INSERT INTO pg18_views.base_table VALUES (1, 'A'), (2, 'B');").await.unwrap();

    // Create a view
    client.simple_query("CREATE VIEW pg18_views.my_view AS SELECT id, val FROM pg18_views.base_table WHERE id = 1;").await.unwrap();

    // Query the view
    let msgs = client.simple_query("SELECT id, val FROM pg18_views.my_view;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some("1"));
    assert_eq!(rows[0].get("val"), Some("A"));

    // Drop the view
    client.simple_query("DROP VIEW pg18_views.my_view;").await.unwrap();
}