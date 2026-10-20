//! Shard merge end to end at the finalized migration level: two shards' exact
//! row multisets land in the merged group, routing flips to the single merged
//! shard, and post-merge writes keep working.

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
async fn merge_moves_the_exact_multiset_and_flips_routing() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    finalize(&base, "snapshot-v2").await;
    finalize(&base, "migration-v3").await;
    client
        .batch_execute("CREATE TABLE merge_me (id INT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    let table = json_get(&http, &format!("{base}/api/v1/catalog/table?name=merge_me")).await["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, body) = json_post(&http, &format!("{base}/api/v1/shards/{table}/init")).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "init: {status} {body}"
    );
    let map = await_map(&http, &base, &table, 1).await;
    let source = map["shards"][0]["id"].as_str().unwrap().to_string();

    // Split at "{table}:2", then write rows on both sides of the boundary.
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
    let left = map["shards"][0]["id"].as_str().unwrap().to_string();
    let right = map["shards"][1]["id"].as_str().unwrap().to_string();
    client
        .batch_execute(
            "INSERT INTO merge_me VALUES (10, 'left'), (11, 'also-left'), (21, 'right'), (22, 'also-right')",
        )
        .await
        .unwrap();

    let (status, body) = json_post(
        &http,
        &format!("{base}/api/v1/shards/{table}/merge?left={left}&right={right}"),
    )
    .await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "merge: {status} {body}"
    );
    let map = await_map(&http, &base, &table, 1).await;
    let shards = map["shards"].as_array().unwrap();
    assert!(
        shards[0]["start_key"].as_array().unwrap().is_empty()
            && shards[0]["end_key"].as_array().unwrap().is_empty(),
        "merged shard covers the whole key space"
    );

    let rows: Vec<(i32, String)> = client
        .query("SELECT id, note FROM merge_me ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        rows,
        [
            (10, "left".to_string()),
            (11, "also-left".to_string()),
            (21, "right".to_string()),
            (22, "also-right".to_string())
        ],
        "both sources' exact rows survived the merge"
    );

    client
        .batch_execute("INSERT INTO merge_me VALUES (30, 'after')")
        .await
        .unwrap();
    assert_eq!(
        client
            .query_one("SELECT note FROM merge_me WHERE id = 30", &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        "after"
    );

    drop(client);
    server.shutdown().await;
}
