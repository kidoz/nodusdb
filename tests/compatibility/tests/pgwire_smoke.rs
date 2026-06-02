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

#[tokio::test]
async fn test_arbitrary_table_sql() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO products (sku, name, price) VALUES ('s1', 'Widget', '9.99');")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO products (sku, name, price) VALUES ('s2', 'Gadget', '19.99');")
        .await
        .unwrap();

    // SELECT * returns every row with all columns in table order.
    let msgs = client
        .simple_query("SELECT * FROM products;")
        .await
        .unwrap();
    assert_eq!(rows_of(&msgs).len(), 2);

    // Projection + filter on the primary key.
    let msgs = client
        .simple_query("SELECT name, price FROM products WHERE sku = 's2';")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), "Gadget");
    assert_eq!(rows[0].get(1).unwrap(), "19.99");
}

#[tokio::test]
async fn test_where_order_limit() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE TABLE scores (id INT PRIMARY KEY, points INT);")
        .await
        .unwrap();
    for (id, pts) in [("1", "30"), ("2", "10"), ("3", "50"), ("4", "20")] {
        client
            .simple_query(&format!(
                "INSERT INTO scores (id, points) VALUES ({id}, {pts});"
            ))
            .await
            .unwrap();
    }

    // Comparison + ORDER BY DESC + LIMIT, numeric (not lexical) ordering.
    let msgs = client
        .simple_query("SELECT id FROM scores WHERE points > 15 ORDER BY points DESC LIMIT 2;")
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0).unwrap(), "3"); // 50
    assert_eq!(rows[1].get(0).unwrap(), "1"); // 30

    // Conjunction (AND) of predicates.
    let msgs = client
        .simple_query("SELECT id FROM scores WHERE points >= 10 AND id < 3;")
        .await
        .unwrap();
    let mut ids: Vec<String> = rows_of(&msgs)
        .iter()
        .map(|r| r.get(0).unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["1", "2"]);

    // UPDATE then DELETE through the wire.
    let msgs = client
        .simple_query("UPDATE scores SET points = 0 WHERE id = 2;")
        .await
        .unwrap();
    assert!(matches!(&msgs[0], SimpleQueryMessage::CommandComplete(_)));
    client
        .simple_query("DELETE FROM scores WHERE id = 4;")
        .await
        .unwrap();
    let msgs = client.simple_query("SELECT id FROM scores;").await.unwrap();
    assert_eq!(rows_of(&msgs).len(), 3);
}

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
