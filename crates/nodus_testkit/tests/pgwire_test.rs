use std::process::Command;
use std::time::Duration;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pgwire_queries() {
    let mut server = Command::new("../../target/debug/nodus_server")
        .spawn()
        .expect("Failed to start server. Ensure it's compiled first.");

    // Wait for the server to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect to the pgwire server
    let mut is_up = false;
    let mut client_opt = None;
    for _ in 0..10 {
        if let Ok((client, connection)) =
            tokio_postgres::connect("host=127.0.0.1 port=5432 user=nodus dbname=default", NoTls)
                .await
        {
            is_up = true;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            client_opt = Some(client);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
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

    server.kill().unwrap();
    server.wait().unwrap();
}
