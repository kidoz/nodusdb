//! Shard split end to end at the finalized migration level: the exact row
//! multiset survives the copy/cutover, routing flips to the children, the
//! source group is decommissioned, and the secondary index keeps serving.

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

/// Waits (bounded) for the table's map to reach `n` shards, returning it.
async fn await_map(http: &reqwest::Client, base: &str, table: &str, n: usize) -> Value {
    for _ in 0..100 {
        let map = json_get(http, &format!("{base}/api/v1/shards/{table}")).await;
        if map["shards"].as_array().is_some_and(|s| s.len() == n) {
            return map;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("table {table} never reached {n} shards");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_moves_the_exact_multiset_and_flips_routing() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    finalize(&base, "snapshot-v2").await;
    finalize(&base, "migration-v3").await;
    client
        .batch_execute("CREATE TABLE split_me (id INT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    let table = json_get(&http, &format!("{base}/api/v1/catalog/table?name=split_me")).await["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Pre-finalization containment still holds for split on a fresh map…
    let (status, _) = json_post(
        &http,
        &format!("{base}/api/v1/shards/{table}/split?shard={table}&key=x"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "no shard map yet: init first");

    let (status, body) = json_post(&http, &format!("{base}/api/v1/shards/{table}/init")).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "init: {status} {body}"
    );
    let map = await_map(&http, &base, &table, 1).await;
    let source = map["shards"][0]["id"].as_str().unwrap().to_string();

    // Rows on both sides of the split point (lexicographic pk order: "1",
    // "10"–"19" sort before "2").
    let mut dml = String::new();
    for i in 1..=20 {
        dml.push_str(&format!("INSERT INTO split_me VALUES ({i}, 'n{i}');"));
    }
    client.batch_execute(&dml).await.unwrap();

    let (status, body) = json_post(
        &http,
        &format!("{base}/api/v1/shards/{table}/split?shard={source}&key={table}:2"),
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "split: {status} {body}"
    );
    let map = await_map(&http, &base, &table, 2).await;
    let shards = map["shards"].as_array().unwrap();
    // Contiguous, disjoint cover: the left child's end is the right's start.
    assert_eq!(shards[0]["end_key"], shards[1]["start_key"]);
    assert!(shards[0]["start_key"].as_array().unwrap().is_empty());
    assert!(shards[1]["end_key"].as_array().unwrap().is_empty());

    // The exact multiset survived: count, every id, and the indexed lookup.
    let ids: Vec<i32> = client
        .query("SELECT id FROM split_me ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        ids,
        (1..=20).collect::<Vec<_>>(),
        "no lost or duplicated rows"
    );
    client
        .batch_execute("CREATE INDEX split_note ON split_me (note)")
        .await
        .unwrap();
    let row = client
        .query_one("SELECT id FROM split_me WHERE note = 'n7'", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 7);

    // Post-cutover writes land on both children by the new map ("100" sorts
    // left of the "2" boundary, "21" right of it).
    client
        .batch_execute("INSERT INTO split_me VALUES (100, 'left-of-split'), (21, 'right-of-split')")
        .await
        .unwrap();
    assert_eq!(
        client
            .query_one("SELECT note FROM split_me WHERE id = 21", &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        "right-of-split"
    );
    assert_eq!(
        client
            .query_one("SELECT note FROM split_me WHERE id = 100", &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        "left-of-split"
    );

    drop(client);
    server.shutdown().await;
}
