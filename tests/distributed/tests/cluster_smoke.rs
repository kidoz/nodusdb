#![allow(clippy::collapsible_if)]
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Once;
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::NoTls;

/// Absolute path to the `nodus_server` binary, building it once if the
/// workspace hasn't already produced it.
///
/// These tests run from the `nodus_distributed_tests` package, where the old
/// `cargo run --bin nodus_server` fallback failed with *"no bin target named
/// nodus_server in default-run packages"* and the relative
/// `../../target/debug` path was brittle to the test process's working
/// directory. Resolving the path from this crate's manifest dir and spawning
/// the prebuilt binary fixes both and keeps node startup within the readiness
/// window (no per-node `cargo` build-lock contention).
fn nodus_server_bin() -> PathBuf {
    if let Ok(explicit) = std::env::var("NODUS_SERVER_BIN") {
        return PathBuf::from(explicit);
    }

    // <workspace>/tests/distributed -> <workspace>
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
        // Both cluster tests run concurrently — build the binary exactly once.
        static BUILD: Once = Once::new();
        BUILD.call_once(|| {
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
            let mut build = Command::new(cargo);
            build
                .args(["build", "-p", "nodus_server", "--bin", "nodus_server"])
                .current_dir(&workspace_root);
            if !cfg!(debug_assertions) {
                build.arg("--release");
            }
            let status = build.status().expect("run cargo build for nodus_server");
            assert!(status.success(), "failed to build the nodus_server binary");
        });
    }
    bin
}

async fn wait_for_server(addr: &str) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    for _ in 0..20 {
        if let Ok((client, connection)) = tokio_postgres::connect(
            &format!(
                "host={} port={} user=nodus password=nodus",
                addr.split(':').next().unwrap(),
                addr.split(':').nth(1).unwrap()
            ),
            NoTls,
        )
        .await
        {
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });
            return Ok(client);
        }
        sleep(Duration::from_millis(500)).await;
    }
    tokio_postgres::connect(
        &format!(
            "host={} port={} user=nodus password=nodus",
            addr.split(':').next().unwrap(),
            addr.split(':').nth(1).unwrap()
        ),
        NoTls,
    )
    .await
    .map(|(c, conn)| {
        tokio::spawn(async move {
            let _ = conn.await;
        });
        c
    })
}

struct NodeGuard {
    child: Child,
    #[allow(dead_code)]
    dir: tempfile::TempDir,
}

impl Drop for NodeGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_node(id: u64, http: &str, pg: &str, peers: Option<&str>) -> NodeGuard {
    let dir = tempfile::tempdir().unwrap();

    let mut cmd = Command::new(nodus_server_bin());
    cmd.env("NODUS_CLUSTER__NODE_ID", id.to_string())
        .env("NODUS_SERVER__HTTP_ADDR", http)
        .env("NODUS_SERVER__PGWIRE_ADDR", pg)
        .env("NODUS_CLUSTER__RAFT_ADVERTISE_ADDR", http)
        .env("NODUS_STORAGE__DATA_DIR", dir.path())
        .env("NODUS_ADMIN__PASSWORD", "nodus")
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());

    if let Some(p) = peers {
        cmd.env("NODUS_CLUSTER__JOIN_PEERS", format!("[\"{}\"]", p));
    } else {
        cmd.env("NODUS_CLUSTER__JOIN_PEERS", "[]");
    }

    NodeGuard {
        child: cmd.spawn().unwrap(),
        dir,
    }
}

async fn wait_for_readyz(addr: &str) {
    let client = reqwest::Client::new();
    let url = format!("http://{}/readyz", addr);
    for _ in 0..100 {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                return;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("Node {} did not become ready", addr);
}

#[tokio::test]
async fn test_cluster_replication() {
    let _n1 = spawn_node(1, "127.0.0.1:8181", "127.0.0.1:5531", None);
    wait_for_readyz("127.0.0.1:8181").await;

    let _n2 = spawn_node(
        2,
        "127.0.0.1:8182",
        "127.0.0.1:5532",
        Some("127.0.0.1:8181"),
    );
    let _n3 = spawn_node(
        3,
        "127.0.0.1:8183",
        "127.0.0.1:5533",
        Some("127.0.0.1:8181"),
    );

    wait_for_readyz("127.0.0.1:8182").await;
    wait_for_readyz("127.0.0.1:8183").await;

    let leader = wait_for_server("127.0.0.1:5531")
        .await
        .expect("connect leader");

    leader
        .execute("CREATE TABLE dist_test (id INT, value TEXT);", &[])
        .await
        .unwrap();
    leader
        .execute(
            "INSERT INTO dist_test (id, value) VALUES (1, 'hello raft');",
            &[],
        )
        .await
        .unwrap();

    // Follower 3
    let follower = wait_for_server("127.0.0.1:5533")
        .await
        .expect("connect follower");

    let mut found = false;
    for _ in 0..50 {
        // Since CREATE TABLE replication might take a moment, query might fail initially.
        if let Ok(rows) = follower
            .query("SELECT value FROM dist_test WHERE id = 1;", &[])
            .await
        {
            if rows.len() == 1 {
                let val: String = rows[0].get(0);
                if val == "hello raft" {
                    found = true;
                    break;
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "Follower did not replicate the row in time");
}

#[tokio::test]
async fn test_cluster_draining() {
    let _n1 = spawn_node(1, "127.0.0.1:8191", "127.0.0.1:5541", None);
    wait_for_readyz("127.0.0.1:8191").await;

    let _n2 = spawn_node(
        2,
        "127.0.0.1:8192",
        "127.0.0.1:5542",
        Some("127.0.0.1:8191"),
    );
    let _n3 = spawn_node(
        3,
        "127.0.0.1:8193",
        "127.0.0.1:5543",
        Some("127.0.0.1:8191"),
    );

    wait_for_readyz("127.0.0.1:8192").await;
    wait_for_readyz("127.0.0.1:8193").await;

    // Node 1 is the leader initially because it boots first.
    let http = reqwest::Client::new();

    // Call drain on node 1
    let resp: serde_json::Value = http
        .post("http://127.0.0.1:8191/api/v1/node/drain")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["draining"], true);
    assert!(
        resp["leadership_transfers"].as_u64().unwrap_or(0) > 0,
        "Expected leadership transfer to occur during drain"
    );

    // readyz should now return 503 for node 1
    let r = http
        .get("http://127.0.0.1:8191/readyz")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 503);

    // Node 2 or 3 should now be the leader.
}
