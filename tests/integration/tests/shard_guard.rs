//! Public shard mutations must fail without changing committed data or routing
//! until the server implements a durable, cluster-wide migration fence.

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_shard_mutations_preserve_rows_indexes_and_routing() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    client
        .batch_execute(
            "CREATE TABLE protected_rows (id INT PRIMARY KEY, note TEXT); \
         CREATE INDEX protected_note ON protected_rows (note); \
         INSERT INTO protected_rows VALUES (1, 'before')",
        )
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    let table = json_get(
        &http,
        &format!("{base}/api/v1/catalog/table?name=protected_rows"),
    )
    .await;
    let table = table["id"].as_str().unwrap();
    let map_url = format!("{base}/api/v1/shards/{table}");
    let groups_url = format!("{base}/api/v1/cluster/groups");
    let before_map = json_get(&http, &map_url).await;
    let before_groups = json_get(&http, &groups_url).await;

    // Two simultaneous initialization requests race an acknowledged SQL write.
    // Repeating init must not overwrite a map, and checking emptiness before
    // updating placement must never become a racy substitute for a fence.
    let init = format!("{map_url}/init");
    let (a, b, insert) = tokio::join!(
        http.post(&init).send(),
        http.post(&init).send(),
        client.batch_execute("INSERT INTO protected_rows VALUES (2, 'during')"),
    );
    insert.unwrap();
    for response in [a.unwrap(), b.unwrap()] {
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["code"], "shard_migration_unavailable");
    }
    for operation in [
        format!("split?shard={table}&key=50"),
        format!("merge?left={table}&right={table}"),
        "rebalance?nodes=1,2".into(),
    ] {
        let response = http
            .post(format!("{map_url}/{operation}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    assert_eq!(json_get(&http, &map_url).await, before_map);
    assert_eq!(json_get(&http, &groups_url).await, before_groups);
    let rows = client
        .query("SELECT id FROM protected_rows ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(),
        [1, 2]
    );
    for (note, id) in [("before", 1), ("during", 2)] {
        let rows = client
            .query("SELECT id FROM protected_rows WHERE note = $1", &[&note])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), id);
    }
    let audit = json_get(&http, &format!("{base}/api/v1/audit")).await;
    let mutations: Vec<_> = audit
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["action"] == "ManageShards")
        .collect();
    assert_eq!(mutations.len(), 5);
    assert!(mutations.iter().all(|event| event["result"] == "Failure"));
    drop(client);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_table_initialization_is_also_rejected() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    client
        .batch_execute("CREATE TABLE empty_rows (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    let table = json_get(
        &http,
        &format!("{base}/api/v1/catalog/table?name=empty_rows"),
    )
    .await;
    let table = table["id"].as_str().unwrap();
    let response = http
        .post(format!("{base}/api/v1/shards/{table}/init"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    client
        .batch_execute("INSERT INTO empty_rows VALUES (1)")
        .await
        .unwrap();
    let row = client
        .query_one("SELECT id FROM empty_rows", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
    drop(client);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_containment_runs_after_authentication() {
    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    config.admin.token = Some("shard-guard-test-token".into());
    let server = TestServer::start_with_config(config).await.unwrap();
    let http = reqwest::Client::new();
    let url = format!(
        "http://{}/api/v1/shards/11111111-1111-1111-1111-111111111111/init",
        server.http_addr
    );
    assert_eq!(
        http.post(&url).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        http.post(&url)
            .bearer_auth("wrong")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        http.post(&url)
            .bearer_auth("shard-guard-test-token")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(
        http.post(&url)
            .basic_auth("nodus", Some("nodus"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    server.shutdown().await;
}
