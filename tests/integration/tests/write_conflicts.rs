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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_reads_and_unique_writes_do_not_deadlock() {
    let (_server, a, b) = setup().await;
    let mut clients = vec![a, b];
    for _ in 2..8 {
        clients.push(connect(&_server.pgwire_addr).await);
    }
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(9));
    let mut tasks = Vec::new();
    for (worker, client) in clients.into_iter().enumerate() {
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            for i in (worker..40).step_by(8) {
                if i % 5 == 0 {
                    let id = (worker * 100 + i) as i32 + 10;
                    client
                        .execute("INSERT INTO cc VALUES ($1, $1)", &[&id])
                        .await
                        .unwrap();
                } else {
                    client
                        .query_one("SELECT v FROM cc WHERE id = $1", &[&1i32])
                        .await
                        .unwrap();
                }
            }
        }));
    }
    barrier.wait().await;
    tokio::time::timeout(
        Duration::from_secs(20),
        futures_util::future::join_all(tasks),
    )
    .await
    .expect("mixed reads and writes deadlocked")
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_deferred_constraint_checks_do_not_deadlock() {
    let (_server, a, b) = setup().await;
    a.batch_execute(
        "CREATE TABLE dc (id INT PRIMARY KEY, u INT UNIQUE DEFERRABLE INITIALLY DEFERRED)",
    )
    .await
    .unwrap();
    let mut clients = vec![std::sync::Arc::new(a), std::sync::Arc::new(b)];
    for _ in 2..8 {
        clients.push(std::sync::Arc::new(connect(&_server.pgwire_addr).await));
    }
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(9));

    for round in 0..2 {
        let mut tasks = Vec::new();
        for (worker, client) in clients.iter().cloned().enumerate() {
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                for i in (worker..16).step_by(8) {
                    let id = (round * 1_000 + worker * 100 + i) as i32 + 10;
                    if round == 1 {
                        client.batch_execute("BEGIN").await.unwrap();
                    }
                    client
                        .execute("INSERT INTO dc VALUES ($1, $1)", &[&id])
                        .await
                        .unwrap();
                    if round == 1 {
                        client.batch_execute("COMMIT").await.unwrap();
                    }
                }
            }));
        }
        barrier.wait().await;
        tokio::time::timeout(
            Duration::from_secs(20),
            futures_util::future::join_all(tasks),
        )
        .await
        .expect("deferred constraint checks deadlocked")
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    }
}
