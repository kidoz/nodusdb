//! COPY FROM STDIN at scale: bulk-loading thousands of rows must complete
//! (regression coverage for copy-in flow control against a persistent server).

use futures_util::SinkExt as _;
use nodus_testkit::TestServer;
use tokio_postgres::NoTls;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_in_loads_ten_thousand_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    config.storage.data_dir = Some(dir.path().to_string_lossy().into_owned());
    let server = TestServer::start_with_config(config).await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} user=nodus password=nodus dbname=default",
            server.pgwire_addr.ip(),
            server.pgwire_addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE bulk (id INT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();

    for round in 0..5 {
        let mut rows = String::new();
        for i in 0..2000 {
            let id = round * 2000 + i;
            rows.push_str(&format!("{id}\tnote-{id}\n"));
        }
        let mut sink = Box::pin(
            client
                .copy_in::<_, bytes::Bytes>("COPY bulk (id, note) FROM STDIN")
                .await
                .unwrap(),
        );
        sink.as_mut().send(bytes::Bytes::from(rows)).await.unwrap();
        let copied =
            tokio::time::timeout(std::time::Duration::from_secs(60), sink.as_mut().finish())
                .await
                .expect("copy_in finish hangs")
                .unwrap();
        assert_eq!(copied, 2000);
    }

    let count: i64 = client
        .query_one("SELECT count(*) FROM bulk", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 10_000);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_in_late_constraint_failure_rolls_back_autocommit() {
    let server = TestServer::start().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} user=nodus password=nodus dbname=default",
            server.pgwire_addr.ip(),
            server.pgwire_addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE copy_atomic (id INT PRIMARY KEY)")
        .await
        .unwrap();

    let mut rows = (0..500).map(|id| format!("{id}\n")).collect::<String>();
    rows.push_str("0\n");
    let mut sink = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY copy_atomic (id) FROM STDIN")
            .await
            .unwrap(),
    );
    sink.as_mut().send(bytes::Bytes::from(rows)).await.unwrap();
    sink.as_mut()
        .finish()
        .await
        .expect_err("the duplicate key in the second chunk must fail COPY");

    let count: i64 = client
        .query_one("SELECT count(*) FROM copy_atomic", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copy_in_preserves_explicit_transaction_ownership() {
    let server = TestServer::start().await.unwrap();
    let (mut client, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} user=nodus password=nodus dbname=default",
            server.pgwire_addr.ip(),
            server.pgwire_addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("CREATE TABLE copy_explicit (id INT PRIMARY KEY)")
        .await
        .unwrap();

    let rows = (0..501).map(|id| format!("{id}\n")).collect::<String>();
    let txn = client.transaction().await.unwrap();
    let mut sink = Box::pin(
        txn.copy_in::<_, bytes::Bytes>("COPY copy_explicit (id) FROM STDIN")
            .await
            .unwrap(),
    );
    sink.as_mut().send(bytes::Bytes::from(rows)).await.unwrap();
    assert_eq!(sink.as_mut().finish().await.unwrap(), 501);
    let count: i64 = txn
        .query_one("SELECT count(*) FROM copy_explicit", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 501);
    txn.commit().await.unwrap();

    let txn = client.transaction().await.unwrap();
    txn.execute("INSERT INTO copy_explicit (id) VALUES (1000)", &[])
        .await
        .unwrap();
    let mut rows = (0..500).map(|id| format!("{id}\n")).collect::<String>();
    rows.push_str("0\n");
    let mut sink = Box::pin(
        txn.copy_in::<_, bytes::Bytes>("COPY copy_explicit (id) FROM STDIN")
            .await
            .unwrap(),
    );
    sink.as_mut().send(bytes::Bytes::from(rows)).await.unwrap();
    sink.as_mut()
        .finish()
        .await
        .expect_err("COPY must fail inside the explicit transaction");
    txn.rollback().await.unwrap();

    let count: i64 = client
        .query_one("SELECT count(*) FROM copy_explicit", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 501);
    let uncommitted: i64 = client
        .query_one("SELECT count(*) FROM copy_explicit WHERE id = 1000", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(uncommitted, 0);
}
