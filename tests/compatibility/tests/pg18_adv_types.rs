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
async fn test_pg18_jsonb() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client.simple_query("CREATE SCHEMA pg18_adv_types;").await.unwrap();

    client
        .simple_query("CREATE TABLE pg18_adv_types.events (id INT PRIMARY KEY, payload JSONB);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO pg18_adv_types.events (id, payload) VALUES (1, '{\"user\": \"alice\", \"active\": true, \"clicks\": 5}'), (2, '{\"user\": \"bob\", \"active\": false, \"clicks\": 0}');")
        .await
        .unwrap();

    // JSON Operator ->>
    let msgs = client.simple_query("SELECT payload->>'user' as username FROM pg18_adv_types.events WHERE id = 1;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("username"), Some("alice"));

    // JSON Operator @>
    let msgs = client.simple_query("SELECT id FROM pg18_adv_types.events WHERE payload @> '{\"active\": true}';").await.unwrap();
    let rows = rows_of(&msgs);
    println!("JSONB Rows: {:?}", rows.iter().map(|r| r.get("id")).collect::<Vec<_>>());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some("1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_arrays() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client.simple_query("CREATE SCHEMA pg18_adv_types;").await.unwrap();

    client
        .simple_query("CREATE TABLE pg18_adv_types.events (id INT PRIMARY KEY, tags TEXT[]);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO pg18_adv_types.events (id, tags) VALUES (1, ARRAY['login', 'signup']), (2, ARRAY['logout']);")
        .await
        .unwrap();

    // Array contains @>
    let msgs = client.simple_query("SELECT id FROM pg18_adv_types.events WHERE tags @> ARRAY['signup'];").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some("1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_functions() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client.simple_query("CREATE SCHEMA pg18_adv_types;").await.unwrap();

    client
        .simple_query("CREATE TABLE pg18_adv_types.users (id INT PRIMARY KEY, first_name TEXT, last_name TEXT);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO pg18_adv_types.users (id, first_name, last_name) VALUES (1, 'Alice', 'Smith');")
        .await
        .unwrap();

    let msgs = client.simple_query("SELECT CONCAT(first_name, ' ', last_name) as full_name FROM pg18_adv_types.users WHERE id = 1;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("full_name"), Some("Alice Smith"));
}