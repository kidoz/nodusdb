//! End-to-end fault/recovery scenarios driven through a real NodusDB server
//! over the PostgreSQL wire protocol. These validate durability and transaction
//! atomicity across the full stack (pgwire → executor → MVCC → persistent store),
//! complementing the in-crate unit tests of the distributed paths.
//!
//! Multi-node chaos (network partitions, per-shard leader failover) needs a
//! multi-process cluster harness and is out of scope here.

use nodus_testkit::TestServer;
use std::time::Duration;
use tokio_postgres::{Client, NoTls};

fn persistent_config(data_dir: &std::path::Path) -> nodus_config::NodusConfig {
    let mut config = nodus_config::NodusConfig::default();
    config.storage.data_dir = Some(data_dir.to_string_lossy().into_owned());
    config.admin.password = Some("nodus".into());
    config
}

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
    panic!("failed to connect to pgwire");
}

/// A committed row must survive a full process restart against the same data
/// directory (catalog persisted to disk, row durable in the LSM/WAL).
#[tokio::test(flavor = "multi_thread")]
async fn committed_data_survives_a_restart() {
    let data_dir = std::env::temp_dir().join(format!("nodus-chaos-{}", uuid::Uuid::new_v4()));

    // First lifetime: create a table and insert a row.
    {
        let server = TestServer::start_with_config(persistent_config(&data_dir))
            .await
            .expect("server starts");
        let client = connect(&server.pgwire_addr).await;
        client
            .batch_execute("CREATE TABLE survivor (id INT, note TEXT)")
            .await
            .unwrap();
        client
            .execute("INSERT INTO survivor (id, note) VALUES (1, 'durable')", &[])
            .await
            .unwrap();
        let rows = client.query("SELECT note FROM survivor", &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        // Drop the server (sends shutdown); give tasks a moment to wind down.
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Second lifetime: same data dir — the table and row must still be there.
    {
        let server = TestServer::start_with_config(persistent_config(&data_dir))
            .await
            .expect("server restarts");
        let client = connect(&server.pgwire_addr).await;
        let rows = client.query("SELECT note FROM survivor", &[]).await.unwrap();
        assert_eq!(rows.len(), 1, "committed row must survive restart");
        let note: &str = rows[0].get(0);
        assert_eq!(note, "durable");
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// A rolled-back transaction must leave no trace: its writes are never visible,
/// before or after a restart.
#[tokio::test(flavor = "multi_thread")]
async fn rolled_back_writes_are_never_visible() {
    let data_dir = std::env::temp_dir().join(format!("nodus-chaos-{}", uuid::Uuid::new_v4()));

    {
        let server = TestServer::start_with_config(persistent_config(&data_dir))
            .await
            .expect("server starts");
        let client = connect(&server.pgwire_addr).await;
        client
            .batch_execute("CREATE TABLE ghosts (id INT, note TEXT)")
            .await
            .unwrap();

        // Write inside a transaction, then roll it back.
        client.batch_execute("BEGIN").await.unwrap();
        client
            .execute("INSERT INTO ghosts (id, note) VALUES (1, 'phantom')", &[])
            .await
            .unwrap();
        client.batch_execute("ROLLBACK").await.unwrap();

        let rows = client.query("SELECT id FROM ghosts", &[]).await.unwrap();
        assert!(rows.is_empty(), "rolled-back write must not be visible");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // And it must not reappear after a restart.
    {
        let server = TestServer::start_with_config(persistent_config(&data_dir))
            .await
            .expect("server restarts");
        let client = connect(&server.pgwire_addr).await;
        let rows = client.query("SELECT id FROM ghosts", &[]).await.unwrap();
        assert!(rows.is_empty(), "rolled-back write must stay absent across restart");
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}
