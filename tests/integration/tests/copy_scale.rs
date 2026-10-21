//! COPY FROM STDIN at scale: bulk-loading thousands of rows must complete
//! (regression coverage for copy-in flow control against a persistent server).

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

    use futures_util::SinkExt as _;
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
