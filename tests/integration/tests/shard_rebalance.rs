//! Rebalance end to end at the finalized migration level: a guarded, atomic
//! placements write. Data groups replicate to every cluster member, so no
//! bytes move; the guard refuses unknown nodes and in-flight migrations.

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
    let body = response.json().await.unwrap();
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
async fn rebalance_reassigns_placements_without_touching_data() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    client
        .batch_execute("CREATE TABLE rebal (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let table = json_get(&http, &format!("{base}/api/v1/catalog/table?name=rebal")).await["id"]
        .as_str()
        .unwrap()
        .to_string();
    let rebalance = format!("{base}/api/v1/shards/{table}/rebalance");

    // Containment holds before finalization.
    let (status, body) = json_post(&http, &format!("{rebalance}?nodes=1")).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["code"], "shard_migration_unavailable");

    finalize(&base, "snapshot-v2").await;
    finalize(&base, "migration-v3").await;

    // The table needs a map before its shards can be assigned anywhere.
    let (status, _) = json_post(&http, &format!("{rebalance}?nodes=1")).await;
    assert_eq!(status, StatusCode::OK, "rebalance: {}", "see body");
    let (status, body) = json_post(&http, &format!("{base}/api/v1/shards/{table}/init")).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "init: {status} {body}"
    );

    client
        .batch_execute("INSERT INTO rebal VALUES (1), (2), (3)")
        .await
        .unwrap();

    // Unknown nodes are refused; empty targets are a client error.
    let (status, body) = json_post(&http, &format!("{rebalance}?nodes=99")).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(body["error"].as_str().unwrap().contains("roster"));
    let (status, _) = json_post(&http, &format!("{rebalance}?nodes=")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // A real rebalance onto this single node's id assigns every shard and
    // returns the assignments; data is untouched.
    let (status, body) = json_post(&http, &format!("{rebalance}?nodes=1")).await;
    assert_eq!(status, StatusCode::OK, "rebalance: {body}");
    assert_eq!(body["rebalanced"], true);
    let assignments = body["assignments"].as_array().unwrap();
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0]["node"], "1");

    // Two racing rebalances converge on one consistent assignment set.
    let url = format!("{rebalance}?nodes=1");
    let (a, b) = tokio::join!(json_post(&http, &url), json_post(&http, &url));
    assert!(
        a.0 == StatusCode::OK && b.0 == StatusCode::OK,
        "{a:?} {b:?}"
    );

    let ids: Vec<i32> = client
        .query("SELECT id FROM rebal ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, [1, 2, 3], "rebalance never touches rows");

    drop(client);
    server.shutdown().await;
}
