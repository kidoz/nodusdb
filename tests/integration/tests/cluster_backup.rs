//! A journaled backup taken on a real three-node mTLS cluster covers every
//! shard exactly once (per-shard objects + manifest coverage), and restores
//! into a fresh cluster.

use nodus_testkit::cluster::{ADMIN_TOKEN, ClusterFixture};
use serde_json::Value;
use tokio_postgres::NoTls;

async fn connect(addr: std::net::SocketAddr) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host={} port={} user=nodus password=nodus dbname=default",
            addr.ip(),
            addr.port()
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

async fn post(cluster: &ClusterFixture, idx: usize, path: &str) -> Value {
    cluster.admin_post(idx, path).await.unwrap()
}

async fn finalize(cluster: &ClusterFixture, target: &str) {
    let body = post(
        cluster,
        0,
        &format!("/api/v1/upgrade/start?target={target}"),
    )
    .await;
    assert!(body["error"].is_null(), "start {target}: {body}");
    for node in 1..=3 {
        let body = post(
            cluster,
            0,
            &format!("/api/v1/upgrade/node-upgraded?node={node}"),
        )
        .await;
        assert!(body["error"].is_null(), "node-upgraded {node}: {body}");
    }
    let body = post(cluster, 0, "/api/v1/upgrade/finalize").await;
    assert!(body["error"].is_null(), "finalize {target}: {body}");
}

async fn rows_on(client: &tokio_postgres::Client, expect: usize) -> Vec<i32> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let rows: Option<Vec<i32>> = match client.query("SELECT id FROM t ORDER BY id", &[]).await {
            Ok(rows) => Some(rows.iter().map(|r| r.get(0)).collect()),
            Err(_) => None,
        };
        if let Some(rows) = rows
            && rows.len() == expect
        {
            return rows;
        }
        assert!(std::time::Instant::now() < deadline, "rows never visible");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_backup_covers_every_shard_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let certs = nodus_testkit::write_test_certs(dir.path());
    let repo = tempfile::tempdir().unwrap();
    let repo_uri = format!("file://{}", repo.path().to_string_lossy());
    let cluster = ClusterFixture::start_cluster_mtls(3, &certs, {
        let repo_uri = repo_uri.clone();
        move |config| config.backup.repository_uri = repo_uri.clone()
    })
    .await
    .unwrap();

    finalize(&cluster, "snapshot-v2").await;
    finalize(&cluster, "migration-v3").await;

    let seed = connect(cluster.nodes[0].pgwire_addr()).await;
    seed.batch_execute("CREATE TABLE t (id INT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    let table: Value = reqwest::Client::new()
        .get(format!(
            "http://{}/api/v1/catalog/table?name=t",
            cluster.nodes[0].http_addr()
        ))
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let table = table["id"].as_str().unwrap().to_string();
    let body = post(&cluster, 0, &format!("/api/v1/shards/{table}/init")).await;
    assert!(body["error"].is_null(), "init: {body}");
    seed.batch_execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .await
        .unwrap();

    // Split so the table has two shards, then write on both sides.
    let map: Value = reqwest::Client::new()
        .get(format!(
            "http://{}/api/v1/shards/{table}",
            cluster.nodes[0].http_addr()
        ))
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let source = map["shards"][0]["id"].as_str().unwrap().to_string();
    let body = post(
        &cluster,
        0,
        &format!("/api/v1/shards/{table}/split?shard={source}&key={table}:2"),
    )
    .await;
    assert!(body["error"].is_null(), "split: {body}");
    seed.batch_execute("INSERT INTO t VALUES (10,'x'),(20,'y')")
        .await
        .unwrap();
    let ids = rows_on(&seed, 5).await;
    assert_eq!(ids, [1, 2, 3, 10, 20]);

    // The journaled backup on the meta leader covers every shard exactly once.
    let body = post(&cluster, 0, "/api/v1/backups").await;
    assert_eq!(body["status"], "Completed", "backup: {body}");
    let backup_id = body["backup_id"].as_str().unwrap().to_string();
    let manifest: Value = serde_json::from_slice(
        &std::fs::read(repo.path().join(&backup_id).join("manifest.json")).unwrap(),
    )
    .unwrap();
    let coverage = manifest["shard_coverage"].as_array().unwrap();
    assert_eq!(
        coverage.len(),
        2,
        "every shard covered exactly once: {manifest}"
    );
    let total: u64 = coverage.iter().map(|c| c["rows"].as_u64().unwrap()).sum();
    assert_eq!(total, 5, "row counts add up to the full table");
    for entry in coverage {
        let object = entry["object"].as_str().unwrap();
        assert!(
            repo.path()
                .join(&backup_id)
                .join("data")
                .join(object)
                .exists(),
            "covered object {object} exists"
        );
    }

    // Restore into a fresh cluster replicates to every member.
    let restored = ClusterFixture::start_cluster_mtls(3, &certs, {
        let repo_uri = repo_uri.clone();
        move |config| config.backup.repository_uri = repo_uri.clone()
    })
    .await
    .unwrap();
    let response = restored
        .admin_post(1, &format!("/api/v1/backups/{backup_id}/restore"))
        .await
        .unwrap();
    assert!(response["error"].is_null(), "restore: {response}");
    for idx in 0..3 {
        let client = connect(restored.nodes[idx].pgwire_addr()).await;
        assert_eq!(rows_on(&client, 5).await, [1, 2, 3, 10, 20]);
    }
}
