//! Restoring a backup into a fresh multi-node cluster: the replay replicates
//! to every member, and a cluster that already has data refuses the restore.

use nodus_testkit::TestServer;
use nodus_testkit::cluster::{ADMIN_TOKEN, ClusterFixture};
use reqwest::StatusCode;
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

/// Reads all ids of `rows_t` from the given node, waiting (bounded) for the
/// replay to replicate there.
async fn ids_on(client: &tokio_postgres::Client, expect: usize) -> Vec<i32> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        // A shard group still forming on this node answers with a retryable
        // routing error ("retry after replica reconciliation"); keep waiting.
        let rows: Option<Vec<i32>> =
            match client.query("SELECT id FROM rows_t ORDER BY id", &[]).await {
                Ok(rows) => Some(rows.iter().map(|r| r.get(0)).collect()),
                Err(_) => None,
            };
        if let Some(rows) = rows
            && rows.len() == expect
        {
            return rows;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rows never replicated"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// A single-node source server with `repo` as its backup repository and the
/// fixture's admin token, holding one indexed table with five rows; returns
/// after the backup completes.
async fn backed_up_source(repo: &std::path::Path, journaled: bool) -> (String, String) {
    let data_dir = tempfile::tempdir().unwrap();
    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    config.admin.token = Some(ADMIN_TOKEN.into());
    config.storage.data_dir = Some(data_dir.path().to_string_lossy().into_owned());
    config.backup.repository_uri = format!("file://{}", repo.to_string_lossy());
    let server = TestServer::start_with_config(config).await.unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    let post = |url: String| {
        let http = http.clone();
        async move {
            http.post(url)
                .bearer_auth(ADMIN_TOKEN)
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        }
    };
    if journaled {
        for target in ["snapshot-v2", "migration-v3"] {
            for url in [
                format!("{base}/api/v1/upgrade/start?target={target}"),
                format!("{base}/api/v1/upgrade/node-upgraded?node=1"),
                format!("{base}/api/v1/upgrade/finalize"),
            ] {
                let body = post(url).await;
                assert!(body["error"].is_null(), "{target}: {body}");
            }
        }
    }
    let client = connect(server.pgwire_addr).await;
    client
        .batch_execute("CREATE TABLE rows_t (id INT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    let table: Value = http
        .get(format!("{base}/api/v1/catalog/table?name=rows_t"))
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let table = table["id"].as_str().unwrap().to_string();
    if journaled {
        let body = post(format!("{base}/api/v1/shards/{table}/init")).await;
        assert!(body["error"].is_null(), "init: {body}");
        client
            .batch_execute("CREATE INDEX rows_note ON rows_t (note)")
            .await
            .unwrap();
    }
    client
        .batch_execute("INSERT INTO rows_t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
        .await
        .unwrap();
    let body = post(format!("{base}/api/v1/backups")).await;
    assert_eq!(body["status"], "Completed", "backup: {body}");
    let backup_id = body["backup_id"].as_str().unwrap().to_string();
    drop(client);
    server.shutdown().await;
    (backup_id, table)
}

async fn restore_and_read_everywhere(journaled: bool) {
    let repo = tempfile::tempdir().unwrap();
    let (backup_id, _table) = backed_up_source(repo.path(), journaled).await;
    let repo_uri = format!("file://{}", repo.path().to_string_lossy());
    let cluster = ClusterFixture::start_with(3, move |config| {
        config.backup.repository_uri = repo_uri.clone();
    })
    .await
    .unwrap();

    // Restore through a non-seed node: replay writes forward to group leaders.
    let response: Value = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/backups/{backup_id}/restore",
            cluster.nodes[1].http_addr()
        ))
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        response["error"].is_null(),
        "fresh-cluster restore: {response}"
    );

    // Every member serves the restored rows (and the index, when journaled).
    for idx in 0..3 {
        let client = connect(cluster.nodes[idx].pgwire_addr()).await;
        let ids = ids_on(&client, 5).await;
        assert_eq!(ids, [1, 2, 3, 4, 5], "node {idx} serves all rows");
        if journaled {
            let row = client
                .query_one("SELECT id FROM rows_t WHERE note = 'd'", &[])
                .await
                .unwrap();
            assert_eq!(row.get::<_, i32>(0), 4, "node {idx} index serves");
        }
    }

    // A cluster that now holds data refuses a second restore.
    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/api/v1/backups/{backup_id}/restore",
            cluster.nodes[1].http_addr()
        ))
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body: Value = response.json().await.unwrap();
    assert_eq!(
        body["code"].as_str().unwrap_or_default(),
        "restore_clustered_not_empty",
        "non-empty cluster refuses: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn restore_into_fresh_cluster_replicates_to_all_members() {
    restore_and_read_everywhere(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn restore_journaled_backup_into_fresh_cluster_replicates() {
    restore_and_read_everywhere(true).await;
}
