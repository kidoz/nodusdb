use nodus_testkit::TestServer;
use std::time::Duration;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pgwire_queries() {
    let server = TestServer::start().await.expect("Failed to start server");

    // Connect to the pgwire server
    let mut is_up = false;
    let mut client_opt = None;
    let conn_str = format!(
        "host={} port={} user=nodus password=nodus dbname=default",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port()
    );
    for _ in 0..30 {
        if let Ok((client, connection)) = tokio_postgres::connect(&conn_str, NoTls).await {
            is_up = true;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            client_opt = Some(client);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(is_up, "PGWire server did not start in time");
    let client = client_opt.unwrap();

    // SELECT 1
    let rows = client.simple_query("SELECT 1;").await.unwrap();
    println!("Received rows: {:?}", rows);
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert_eq!(row.get(0).unwrap(), "1");
    } else {
        panic!("Expected a row");
    }

    // SELECT 'hello'
    let rows = client.simple_query("SELECT 'hello';").await.unwrap();
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert_eq!(row.get(0).unwrap(), "hello");
    } else {
        panic!("Expected a row");
    }

    // SELECT version()
    let rows = client.simple_query("SELECT version();").await.unwrap();
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert!(row.get(0).unwrap().contains("PostgreSQL"));
    } else {
        panic!("Expected a row");
    }

    // SHOW search_path
    let rows = client.simple_query("SHOW search_path;").await.unwrap();
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert_eq!(row.get(0).unwrap(), "public");
    } else {
        panic!("Expected a row");
    }

    // Execute the vertical slice pipeline
    client
        .simple_query("CREATE TABLE users (id UUID PRIMARY KEY, name TEXT NOT NULL);")
        .await
        .unwrap();
    client.simple_query("BEGIN;").await.unwrap();
    client
        .simple_query("INSERT INTO users (id, name) VALUES ('u1', 'alice');")
        .await
        .unwrap();
    client.simple_query("COMMIT;").await.unwrap();

    let rows = client
        .simple_query("SELECT id, name FROM users WHERE id = 'u1';")
        .await
        .unwrap();
    if let tokio_postgres::SimpleQueryMessage::Row(row) = &rows[1] {
        assert_eq!(row.get(0).unwrap(), "u1");
        assert_eq!(row.get(1).unwrap(), "alice");
    } else {
        panic!("Expected a row");
    }
}
