use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::NoTls;
use std::process::{Command, Child};
use std::path::PathBuf;

async fn wait_for_server(addr: &str) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    for _ in 0..20 {
        if let Ok((client, connection)) =
            tokio_postgres::connect(&format!("host={} port={} user=nodus password=nodus", addr.split(':').next().unwrap(), addr.split(':').nth(1).unwrap()), NoTls).await
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
    tokio_postgres::connect(&format!("host={} port={} user=nodus password=nodus", addr.split(':').next().unwrap(), addr.split(':').nth(1).unwrap()), NoTls).await.map(|(c, conn)| {
        tokio::spawn(async move { let _ = conn.await; });
        c
    })
}

struct NodeGuard {
    child: Child,
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
    
    // We expect `nodusd` or `nodus_server` to be in PATH or built locally
    let bin_path = if PathBuf::from("../../target/debug/nodus_server").exists() {
        "../../target/debug/nodus_server"
    } else {
        "cargo"
    };

    let mut cmd = Command::new(bin_path);
    if bin_path == "cargo" {
        cmd.arg("run").arg("--bin").arg("nodus_server").arg("--");
    }

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
    
    let _n2 = spawn_node(2, "127.0.0.1:8182", "127.0.0.1:5532", Some("127.0.0.1:8181"));
    let _n3 = spawn_node(3, "127.0.0.1:8183", "127.0.0.1:5533", Some("127.0.0.1:8181"));
    
    wait_for_readyz("127.0.0.1:8182").await;
    wait_for_readyz("127.0.0.1:8183").await;

    let leader = wait_for_server("127.0.0.1:5531").await.expect("connect leader");
    
    leader.execute("CREATE TABLE dist_test (id INT, value TEXT);", &[]).await.unwrap();
    leader.execute("INSERT INTO dist_test (id, value) VALUES (1, 'hello raft');", &[]).await.unwrap();

    // Follower 3
    let follower = wait_for_server("127.0.0.1:5533").await.expect("connect follower");
    
    let mut found = false;
    for _ in 0..50 {
        // Since CREATE TABLE replication might take a moment, query might fail initially.
        if let Ok(rows) = follower.query("SELECT value FROM dist_test WHERE id = 1;", &[]).await {
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
