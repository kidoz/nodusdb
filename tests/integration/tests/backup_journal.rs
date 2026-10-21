//! Cluster backup journaling at the finalized migration level: create is
//! journaled through the meta group (full and incremental), migration
//! operations proceed after the job completes, and restores read the same
//! manifest format as the legacy path.

use nodus_testkit::TestServer;
use reqwest::StatusCode;
use serde_json::Value;
use tokio_postgres::NoTls;

async fn connect(server: &TestServer) -> tokio_postgres::Client {
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
}

async fn json_post(http: &reqwest::Client, url: &str) -> (StatusCode, Value) {
    let response = http.post(url).send().await.unwrap();
    let status = response.status();
    let body = response.json().await.unwrap_or(Value::Null);
    (status, body)
}

async fn json_get(http: &reqwest::Client, url: &str) -> Value {
    http.get(url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn finalize(base: &str, target: &str) {
    let http = reqwest::Client::new();
    for url in [
        format!("{base}/api/v1/upgrade/start?target={target}"),
        format!("{base}/api/v1/upgrade/node-upgraded?node=1"),
        format!("{base}/api/v1/upgrade/finalize"),
    ] {
        let (status, body) = json_post(&http, &url).await;
        assert!(
            status.is_success() && body["error"].is_null(),
            "{target} step failed: {status} {body}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn journaled_backup_lifecycle_at_migration_level() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    finalize(&base, "snapshot-v2").await;
    finalize(&base, "migration-v3").await;

    client
        .batch_execute("CREATE TABLE journaled (id INT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    client
        .batch_execute("INSERT INTO journaled VALUES (1, 'base')")
        .await
        .unwrap();

    // Full backup through the journal.
    let (status, body) = json_post(&http, &format!("{base}/api/v1/backups")).await;
    assert_eq!(status, StatusCode::OK, "full backup: {body}");
    assert_eq!(body["status"], "Completed");
    let full_id = body["backup_id"].as_str().unwrap().to_string();

    // Incremental on top (journal pins the parent's window during export).
    client
        .batch_execute("INSERT INTO journaled VALUES (2, 'delta')")
        .await
        .unwrap();
    let (status, body) = json_post(
        &http,
        &format!("{base}/api/v1/backups?parent_backup_id={full_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "incremental backup: {body}");
    assert_eq!(body["status"], "Completed");

    // Both manifests verify through the repository.
    let (status, _) = json_post(&http, &format!("{base}/api/v1/backups/{full_id}/verify")).await;
    assert_eq!(status, StatusCode::OK);

    // Migration planning proceeds once no job is exporting.
    client
        .batch_execute("CREATE TABLE after_backup (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let table = json_get(
        &http,
        &format!("{base}/api/v1/catalog/table?name=after_backup"),
    )
    .await["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = json_post(&http, &format!("{base}/api/v1/shards/{table}/init")).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "init after completed backup: {status} {body}"
    );

    drop(client);
    server.shutdown().await;
}

/// A journaled backup at the migration level writes the per-shard layout with
/// manifest coverage, and a fresh node restores rows, routing, and the
/// secondary index from it — across a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn journaled_backup_restores_into_a_fresh_node_with_shard_layout() {
    let unique = std::process::id();
    let data_a = std::env::temp_dir().join(format!("nodus_bj_a_{unique}"));
    let data_b = std::env::temp_dir().join(format!("nodus_bj_b_{unique}"));
    let repo_dir = std::env::temp_dir().join(format!("nodus_bj_repo_{unique}"));
    let _ = std::fs::remove_dir_all(&data_a);
    let _ = std::fs::remove_dir_all(&data_b);
    let _ = std::fs::remove_dir_all(&repo_dir);

    let config = |dir: &std::path::Path| {
        let mut config = nodus_config::NodusConfig::default();
        config.admin.password = Some("nodus".into());
        config.admin.allow_insecure = true;
        config.storage.data_dir = Some(dir.to_string_lossy().to_string());
        config.backup.repository_uri = format!("file://{}", repo_dir.to_string_lossy());
        config
    };

    // Source: finalized level 3, one initialized shard, five rows, an index.
    let server = TestServer::start_with_config(config(&data_a))
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    finalize(&base, "snapshot-v2").await;
    finalize(&base, "migration-v3").await;
    let client = connect(&server).await;
    client
        .batch_execute("CREATE TABLE restored_t (id INT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    let table = json_get(
        &http,
        &format!("{base}/api/v1/catalog/table?name=restored_t"),
    )
    .await["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = json_post(&http, &format!("{base}/api/v1/shards/{table}/init")).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "init: {status} {body}"
    );
    client
        .batch_execute("CREATE INDEX restored_note ON restored_t (note)")
        .await
        .unwrap();
    client
        .batch_execute("INSERT INTO restored_t VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
        .await
        .unwrap();

    let (status, body) = json_post(&http, &format!("{base}/api/v1/backups")).await;
    assert_eq!(status, StatusCode::OK, "backup: {body}");
    assert_eq!(body["status"], "Completed");
    let backup_id = body["backup_id"].as_str().unwrap().to_string();

    // The manifest records per-shard coverage and no legacy dump exists.
    let manifest: Value = serde_json::from_slice(
        &std::fs::read(repo_dir.join(&backup_id).join("manifest.json")).unwrap(),
    )
    .unwrap();
    let coverage = manifest["shard_coverage"].as_array().unwrap();
    assert_eq!(coverage.len(), 1, "one shard covered: {manifest}");
    assert_eq!(coverage[0]["rows"], 5);
    let shard_object = coverage[0]["object"].as_str().unwrap().to_string();
    assert!(shard_object.starts_with("shard-"));
    assert!(
        repo_dir
            .join(&backup_id)
            .join("data")
            .join(&shard_object)
            .exists()
    );
    assert!(
        repo_dir
            .join(&backup_id)
            .join("data")
            .join("meta.json")
            .exists()
    );
    assert!(
        !repo_dir
            .join(&backup_id)
            .join("data")
            .join("kv_data.json")
            .exists()
    );
    // A fresh engine has no storage checkpoint yet, so no generation object
    // travels; the additive field is simply absent from the manifest.
    assert!(manifest["recovery_generation"].is_null());
    drop(client);
    server.shutdown().await;

    // Fresh node, same repository: restore, restart, verify.
    let server = TestServer::start_with_config(config(&data_b))
        .await
        .unwrap();
    let base = format!("http://{}", server.http_addr);
    let http = reqwest::Client::new();
    let (status, body) =
        json_post(&http, &format!("{base}/api/v1/backups/{backup_id}/restore")).await;
    assert_eq!(status, StatusCode::OK, "restore: {body}");
    server.shutdown().await;

    let server = TestServer::start_with_config(config(&data_b))
        .await
        .unwrap();
    let client = connect(&server).await;
    let ids: Vec<i32> = client
        .query("SELECT id FROM restored_t ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        ids,
        [1, 2, 3, 4, 5],
        "all rows restored through the shard layout"
    );
    let row = client
        .query_one("SELECT id FROM restored_t WHERE note = 'c'", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 3, "restored secondary index serves");
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    let map = json_get(&http, &format!("{base}/api/v1/shards/{table}")).await;
    assert_eq!(
        map["shards"].as_array().unwrap().len(),
        1,
        "routing restored"
    );

    drop(client);
    server.shutdown().await;
}
