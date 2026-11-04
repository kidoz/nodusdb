#![cfg(all(feature = "iceberg", unix))]
//! Invoked only by the owned Lakekeeper/PyIceberg runner. Missing inputs fail.
use nodus_testkit::TestServer;
use std::time::{Duration, Instant};
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=nodus password=nodus dbname=default",
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
}
fn text_rows(messages: Vec<SimpleQueryMessage>) -> Vec<Vec<Option<String>>> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires owned Lakekeeper stack; run just test-iceberg-sql-live"]
async fn iceberg_sql_live() {
    let profiles = std::env::var("NODUS_ICEBERG_SQL_PROFILES").expect("owned runner profiles");
    let expected_path =
        std::env::var("NODUS_ICEBERG_SQL_EXPECTED").expect("independent PyIceberg rows");
    let expected: Vec<Vec<Option<String>>> =
        serde_json::from_slice(&std::fs::read(expected_path).unwrap()).unwrap();
    let data = tempfile::tempdir().unwrap();
    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    config.admin.allow_insecure = true;
    config.storage.data_dir = Some(data.path().to_str().unwrap().into());
    nodus_testkit::apply_fast_raft_timers(&mut config);
    config.iceberg = nodus_common::iceberg::Settings {
        enabled: true,
        profiles_file: profiles,
        worker_executable: env!("CARGO_BIN_EXE_nodus_cli").into(),
        ..Default::default()
    };
    let server = TestServer::start_with_config(config.clone()).await.unwrap();
    let client = connect(&server).await;
    client
        .batch_execute(
            "CREATE VIEW lake_events AS SELECT * FROM nodus_iceberg('lake', 'interop', 'events')",
        )
        .await
        .unwrap();
    let actual = text_rows(
        client
            .simple_query("SELECT * FROM lake_events")
            .await
            .unwrap(),
    );
    assert_eq!(
        actual, expected,
        "SQL values differ from independent PyIceberg scan"
    );
    let statement = client
        .prepare("SELECT name AS label, id, payload, active FROM lake_events LIMIT 2")
        .await
        .unwrap();
    assert_eq!(
        statement
            .columns()
            .iter()
            .map(|c| c.type_().name())
            .collect::<Vec<_>>(),
        ["text", "int8", "bytea", "bool"]
    );
    let rows = client.query(&statement, &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i64>(1), 1);
    assert_eq!(rows[0].get::<_, Vec<u8>>(2), vec![0, 255]);
    assert!(rows[0].get::<_, bool>(3));
    assert_eq!(
        client
            .query("SELECT name FROM lake_events LIMIT 0", &[])
            .await
            .unwrap()
            .len(),
        0
    );
    for sql in [
        "DELETE FROM lake_events",
        "UPDATE lake_events SET name = 'changed'",
        "INSERT INTO lake_events (id) VALUES (8)",
        "TRUNCATE lake_events",
        "SELECT * FROM lake_events WHERE id = 1",
        "SELECT count(*) FROM lake_events",
    ] {
        assert!(
            client.simple_query(sql).await.is_err(),
            "unexpected success: {sql}"
        );
    }
    // The real PostgreSQL CancelRequest reaches the worker before schema/rows.
    let token = client.cancel_token();
    let pending = client.simple_query(
        "CREATE VIEW stalled AS SELECT * FROM nodus_iceberg('stall', 'interop', 'events')",
    );
    let cancel = async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        token.cancel_query(NoTls).await.unwrap();
    };
    let start = Instant::now();
    let (result, ()) = tokio::join!(pending, cancel);
    let cancel_error = result.unwrap_err();
    assert_eq!(
        cancel_error.as_db_error().unwrap().code().code(),
        "57014",
        "{cancel_error:?}"
    );
    assert!(start.elapsed() < Duration::from_secs(3));
    client
        .batch_execute("SET statement_timeout = '100ms'")
        .await
        .unwrap();
    for _ in 0..5 {
        let error = client
            .simple_query(
                "CREATE VIEW stalled AS SELECT * FROM nodus_iceberg('stall', 'interop', 'events')",
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.as_db_error().unwrap().code().code(),
            "57014",
            "{error:?}"
        );
    }
    client
        .batch_execute("SET statement_timeout = 0")
        .await
        .unwrap();
    assert!(
        client.simple_query("SELECT * FROM stalled").await.is_err(),
        "failed registration was published"
    );
    assert_eq!(
        text_rows(client.simple_query("SELECT 1").await.unwrap()).len(),
        1
    );
    client.batch_execute("CREATE VIEW lake_wide AS SELECT * FROM nodus_iceberg('lake', 'interop', 'wide_events')").await.unwrap();
    let wide_rows = text_rows(
        client
            .simple_query("SELECT payload FROM lake_wide LIMIT 1")
            .await
            .unwrap(),
    );
    assert_eq!(wide_rows[0][0].as_ref().unwrap().len(), 8192);
    drop(client);
    server.shutdown().await;
    // The existing durable view representation survives a real server restart.
    config.iceberg.worker_memory_mib = 64;
    let server = TestServer::start_with_config(config).await.unwrap();
    let client = connect(&server).await;
    assert_eq!(
        text_rows(
            client
                .simple_query("SELECT * FROM lake_events")
                .await
                .unwrap()
        ),
        expected
    );
    let memory_error = client
        .simple_query("SELECT payload FROM lake_wide LIMIT 1")
        .await
        .unwrap_err();
    let memory_error = memory_error.as_db_error().unwrap();
    assert_eq!(memory_error.code().code(), "53200", "{memory_error:?}");
    assert!(
        memory_error
            .message()
            .contains("decoder process exceeded its memory limit")
    );
    assert_eq!(
        text_rows(client.simple_query("SELECT 1").await.unwrap()).len(),
        1
    );
    client
        .batch_execute("DROP VIEW lake_events, lake_wide")
        .await
        .unwrap();
    drop(client);
    server.shutdown().await;
}
