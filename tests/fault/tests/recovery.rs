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
        let rows = client
            .query("SELECT note FROM survivor", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        // Cleanly shut down so the data dir is flushed and released before the
        // restart reopens it (a bare drop only signals and cannot await).
        drop(client);
        server.shutdown().await;
    }

    // Second lifetime: same data dir — the table and row must still be there.
    {
        let server = TestServer::start_with_config(persistent_config(&data_dir))
            .await
            .expect("server restarts");
        let client = connect(&server.pgwire_addr).await;
        let rows = client
            .query("SELECT note FROM survivor", &[])
            .await
            .unwrap();
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
        drop(client);
        server.shutdown().await;
    }

    // And it must not reappear after a restart.
    {
        let server = TestServer::start_with_config(persistent_config(&data_dir))
            .await
            .expect("server restarts");
        let client = connect(&server.pgwire_addr).await;
        let rows = client.query("SELECT id FROM ghosts", &[]).await.unwrap();
        assert!(
            rows.is_empty(),
            "rolled-back write must stay absent across restart"
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Process-level crash durability: a real `nodus_server` binary is SIGKILLed
// mid-write-load — no graceful shutdown — and restarted on the same data dir.
// Every write acknowledged before the kill must survive, and nothing
// uncommitted may appear. The in-memory default would mask all of this, so the
// server runs with an explicit `storage.data_dir`.
// ---------------------------------------------------------------------------

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex, Once};

/// Absolute path to the `nodus_server` binary, building it once if the
/// workspace hasn't already produced it (same resolution as the distributed
/// cluster smoke tests, which cannot rely on `cargo run` from this crate).
fn nodus_server_bin() -> PathBuf {
    if let Ok(explicit) = std::env::var("NODUS_SERVER_BIN") {
        return PathBuf::from(explicit);
    }
    // <workspace>/tests/fault -> <workspace>
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root sits two levels above the test crate")
        .to_path_buf();
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root.join("target"));
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let bin = target_dir.join(profile).join("nodus_server");
    if !bin.exists() {
        static BUILD: Once = Once::new();
        BUILD.call_once(|| {
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
            let status = Command::new(cargo)
                .args(["build", "-p", "nodus_server", "--bin", "nodus_server"])
                .current_dir(&workspace_root)
                .status()
                .expect("run cargo build for nodus_server");
            assert!(status.success(), "failed to build the nodus_server binary");
        });
    }
    bin
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A spawned server process; killed on drop so a panicking test never leaks a
/// node holding the data dir.
struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_server(data_dir: &std::path::Path, http_port: u16, pg_port: u16) -> ServerGuard {
    let child = Command::new(nodus_server_bin())
        .env("NODUS_CLUSTER__NODE_ID", "1")
        .env("NODUS_SERVER__HTTP_ADDR", format!("127.0.0.1:{http_port}"))
        .env("NODUS_SERVER__PGWIRE_ADDR", format!("127.0.0.1:{pg_port}"))
        .env(
            "NODUS_CLUSTER__RAFT_ADVERTISE_ADDR",
            format!("127.0.0.1:{http_port}"),
        )
        .env("NODUS_CLUSTER__JOIN_PEERS", "[]")
        .env("NODUS_STORAGE__DATA_DIR", data_dir)
        .env("NODUS_ADMIN__PASSWORD", "nodus")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn nodus_server");
    ServerGuard(child)
}

/// Connects over pgwire, retrying while the process boots (binary startup is
/// slower than the in-process `TestServer`, so the budget is generous).
async fn connect_port(port: u16) -> Client {
    let conn_str = format!("host=127.0.0.1 port={port} user=nodus password=nodus dbname=default");
    for _ in 0..150 {
        if let Ok((client, connection)) = tokio_postgres::connect(&conn_str, NoTls).await {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("failed to connect to pgwire on port {port}");
}

/// SIGKILL (not graceful shutdown) in the middle of write load: the restarted
/// server must show exactly the writes that were acknowledged before the kill —
/// no lost commits, no torn multi-row transaction, no uncommitted rows.
#[tokio::test(flavor = "multi_thread")]
async fn sigkill_mid_write_preserves_exactly_the_committed_writes() {
    let data_dir = std::env::temp_dir().join(format!("nodus-sigkill-{}", uuid::Uuid::new_v4()));
    let http_port = free_port();
    let pg_port = free_port();
    let acked: Arc<Mutex<BTreeSet<i32>>> = Arc::new(Mutex::new(BTreeSet::new()));

    {
        let mut server = spawn_server(&data_dir, http_port, pg_port);
        let client = connect_port(pg_port).await;
        client
            .batch_execute("CREATE TABLE crashd (id INT PRIMARY KEY, note TEXT)")
            .await
            .unwrap();

        // A known committed prefix of single-row writes.
        for id in 0..20 {
            client
                .execute("INSERT INTO crashd (id, note) VALUES ($1, 'p')", &[&id])
                .await
                .unwrap();
            acked.lock().unwrap().insert(id);
        }

        // A committed multi-row transaction: atomicity demands the restart show
        // both rows or neither (a torn commit would show exactly one).
        client.batch_execute("BEGIN").await.unwrap();
        client
            .execute("INSERT INTO crashd (id, note) VALUES (500, 't')", &[])
            .await
            .unwrap();
        client
            .execute("INSERT INTO crashd (id, note) VALUES (501, 't')", &[])
            .await
            .unwrap();
        client.batch_execute("COMMIT").await.unwrap();
        acked.lock().unwrap().extend([500, 501]);

        // Continuous write load so the kill interrupts genuinely in-flight
        // commits; every insert the server acknowledges is recorded.
        let load_acked = acked.clone();
        let load = tokio::spawn(async move {
            let load_client = connect_port(pg_port).await;
            let mut id = 1000_i32;
            loop {
                match load_client
                    .execute("INSERT INTO crashd (id, note) VALUES ($1, 'l')", &[&id])
                    .await
                {
                    Ok(_) => {
                        load_acked.lock().unwrap().insert(id);
                    }
                    // The server is gone; anything not acknowledged here was
                    // never promised to the client.
                    Err(_) => break,
                }
                id += 1;
            }
        });

        // A transaction that stays uncommitted through the kill: none of it may
        // ever become visible.
        let victim = connect_port(pg_port).await;
        victim.batch_execute("BEGIN").await.unwrap();
        victim
            .execute("INSERT INTO crashd (id, note) VALUES (999, 'victim')", &[])
            .await
            .unwrap();

        // Let the load get genuinely in flight, then SIGKILL the process.
        tokio::time::sleep(Duration::from_millis(500)).await;
        server.0.kill().expect("SIGKILL the server");
        server.0.wait().expect("reap the killed server");
        let _ = load.await;
    }

    // Second lifetime: same data dir. Recovery must replay exactly the
    // acknowledged prefix — no more, no less.
    {
        let _server = spawn_server(&data_dir, http_port, pg_port);
        let client = connect_port(pg_port).await;
        let rows = client
            .query("SELECT id FROM crashd ORDER BY id", &[])
            .await
            .expect("the table itself must survive the crash");
        let ids: BTreeSet<i32> = rows.iter().map(|r| r.get(0)).collect();
        let expected = acked.lock().unwrap().clone();
        assert!(
            !expected.is_empty(),
            "the load must have acknowledged writes before the kill"
        );
        // Durability: every write acknowledged before the kill survives. The
        // table may additionally contain writes the server committed but never
        // got to acknowledge (the crash raced the response) — those were never
        // promised to the client, so their presence is legal; their *absence*
        // is what would be a lost commit.
        let missing: BTreeSet<_> = expected.difference(&ids).collect();
        assert!(
            missing.is_empty(),
            "acknowledged writes were lost across the crash: {missing:?}"
        );
        // Atomicity: a committed multi-row transaction appears in full — never
        // torn to exactly one row — and an uncommitted transaction leaves no
        // trace at all.
        assert!(
            ids.contains(&500) && ids.contains(&501),
            "the committed multi-row transaction must appear in full"
        );
        assert!(
            !ids.contains(&999),
            "the uncommitted transaction must stay invisible"
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}
