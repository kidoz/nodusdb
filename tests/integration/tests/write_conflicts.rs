//! Concurrent writers to one row: a lost write-write race must surface as a
//! retryable `40001` error, never as an acknowledged-but-dropped write, and the
//! loser must not leave intents behind that strand the row for later writers.

use nodus_testkit::TestServer;
use std::time::Duration;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};

async fn connect(addr: &std::net::SocketAddr) -> Client {
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
    panic!("could not connect to pgwire");
}

async fn value(client: &Client) -> Option<i32> {
    client
        .query_opt("SELECT v FROM cc WHERE id = 1", &[])
        .await
        .unwrap_or_else(|e| panic!("read failed: {e:?}"))
        .map(|row| row.get(0))
}

async fn assert_serialization_failure(client: &Client, sql: &str) {
    let err = client
        .simple_query(sql)
        .await
        .expect_err(&format!("`{sql}` must lose the write-write race"));
    assert_eq!(
        err.as_db_error().map(|e| e.code()),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE),
        "`{sql}`: {err:?}"
    );
}

async fn setup() -> (TestServer, Client, Client) {
    let server = TestServer::start().await.expect("server starts");
    let a = connect(&server.pgwire_addr).await;
    let b = connect(&server.pgwire_addr).await;
    a.batch_execute("CREATE TABLE cc (id INT PRIMARY KEY, v INT); INSERT INTO cc VALUES (1, 10)")
        .await
        .unwrap();
    (server, a, b)
}

#[tokio::test(flavor = "multi_thread")]
async fn write_to_a_row_with_a_pending_intent_is_rejected_not_lost() {
    let (_server, a, b) = setup().await;
    a.batch_execute("BEGIN; UPDATE cc SET v = 100 WHERE id = 1")
        .await
        .unwrap();

    assert_serialization_failure(&b, "UPDATE cc SET v = v + 1 WHERE id = 1").await;
    assert_serialization_failure(&b, "DELETE FROM cc WHERE id = 1").await;
    b.batch_execute("BEGIN").await.unwrap();
    assert_serialization_failure(&b, "UPDATE cc SET v = 7 WHERE id = 1").await;
    b.batch_execute("ROLLBACK").await.unwrap();

    a.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        value(&b).await,
        Some(100),
        "the intent holder's commit wins"
    );

    b.batch_execute("UPDATE cc SET v = 42 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(value(&a).await, Some(42), "the row stays writable");
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_conflict_releases_the_losers_intents() {
    let (_server, a, b) = setup().await;
    a.batch_execute("BEGIN; SELECT v FROM cc").await.unwrap();
    b.batch_execute("UPDATE cc SET v = 5 WHERE id = 1")
        .await
        .unwrap();
    a.batch_execute("UPDATE cc SET v = 6 WHERE id = 1")
        .await
        .unwrap();
    assert_serialization_failure(&a, "COMMIT").await;
    assert_eq!(value(&b).await, Some(5), "the first committer wins");

    // Before the fix the loser's orphaned intent silently swallowed these.
    b.batch_execute("UPDATE cc SET v = 7 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(value(&a).await, Some(7));
    b.batch_execute("DELETE FROM cc WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(value(&a).await, None);
}
