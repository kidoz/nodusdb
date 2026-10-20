//! Shard initialization end to end: fenced with 501 until the migration
//! cluster level finalizes, then planned through the V3 journal and driven to
//! Done — racing INSERTs are rejected (never lost), and a repeated init is a
//! conflict, never an overwrite.

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

/// Drives one upgrade target to finalization through the admin API.
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

async fn table_id(http: &reqwest::Client, base: &str, name: &str) -> String {
    let table = json_get(http, &format!("{base}/api/v1/catalog/table?name={name}")).await;
    table["id"].as_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_init_after_migration_level_finalization() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    client
        .batch_execute("CREATE TABLE init_target (id INT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    let table = table_id(&http, &base, "init_target").await;
    let init = format!("{base}/api/v1/shards/{table}/init");

    // Containment holds until the migration level finalizes.
    let (status, body) = json_post(&http, &init).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["code"], "shard_migration_unavailable");

    // Finalize snapshot-v2, then migration-v3.
    finalize(&base, "snapshot-v2").await;
    let (status, _) = json_post(&http, &init).await;
    assert_eq!(
        status,
        StatusCode::NOT_IMPLEMENTED,
        "level 2 does not unlock shard init"
    );
    finalize(&base, "migration-v3").await;

    // Init plans through the V3 journal and drives to Done synchronously on
    // this single node.
    let (status, body) = json_post(&http, &init).await;
    assert_eq!(status, StatusCode::OK, "init must complete: {body}");
    assert_eq!(body["phase"], "done");
    let shard = body["shard_id"].as_str().unwrap().to_string();

    // The published map is one full-range shard, and the table serves SQL
    // through the new group.
    let map = json_get(&http, &format!("{base}/api/v1/shards/{table}")).await;
    let shards = map["shards"].as_array().unwrap();
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0]["id"], shard);
    client
        .batch_execute("INSERT INTO init_target VALUES (1, 'after')")
        .await
        .unwrap();
    let row = client
        .query_one("SELECT note FROM init_target WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "after");

    // Repeating init conflicts; it never overwrites the map.
    let (status, _) = json_post(&http, &init).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        json_get(&http, &format!("{base}/api/v1/shards/{table}")).await,
        map
    );

    drop(client);
    server.shutdown().await;
}

/// Two racing inits against concurrent INSERTs: at most one init commits, the
/// loser conflicts, and no acknowledged write is lost or duplicated. INSERTs
/// before publication fail cleanly (the table has no routing), never partially.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_init_and_inserts_never_lose_acknowledged_writes() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    client
        .batch_execute("CREATE TABLE race_target (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    finalize(&base, "snapshot-v2").await;
    finalize(&base, "migration-v3").await;
    let table = table_id(&http, &base, "race_target").await;
    let init = format!("{base}/api/v1/shards/{table}/init");

    let (a, b, insert) = tokio::join!(
        json_post(&http, &init),
        json_post(&http, &init),
        client.batch_execute("INSERT INTO race_target VALUES (1)"),
    );
    // The winner drives to Done synchronously (200) or, if the racing insert
    // briefly fences the meta group, journals the operation and lets the
    // meta-leader discovery worker finish it (202). The loser conflicts.
    let winners = [&a, &b]
        .iter()
        .filter(|(s, _)| *s == StatusCode::OK || *s == StatusCode::ACCEPTED)
        .count();
    assert_eq!(winners, 1, "exactly one init is journaled: {a:?}, {b:?}");
    assert!(
        [&a, &b].iter().any(|(s, _)| *s == StatusCode::CONFLICT),
        "the loser conflicts: {a:?}, {b:?}"
    );
    // The racing insert either failed before publication or committed to the
    // meta group and must have moved with the copy; if it was acknowledged,
    // the row survives.
    let acknowledged = insert.is_ok();

    // Wait (bounded) for the worker to finish a 202'd operation.
    let mut published = false;
    for _ in 0..100 {
        let map = json_get(&http, &format!("{base}/api/v1/shards/{table}")).await;
        if map["shards"].as_array().is_some_and(|s| !s.is_empty()) {
            published = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(published, "the journaled init completed via the worker");

    let rows = client
        .query("SELECT id FROM race_target", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        usize::from(acknowledged),
        "acknowledged writes survive init exactly once"
    );

    // Post-publication writes succeed and read back.
    client
        .batch_execute("INSERT INTO race_target VALUES (2)")
        .await
        .unwrap();
    let ids: Vec<i32> = client
        .query("SELECT id FROM race_target ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids.last(), Some(&2));

    drop(client);
    server.shutdown().await;
}

/// Rows written while the table had no shard map live on the meta group; init
/// must move them with the copy, and the secondary index (which stays on meta
/// permanently) must keep serving after the flip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_moves_rows_written_before_the_map_existed() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    finalize(&base, "snapshot-v2").await;
    finalize(&base, "migration-v3").await;
    client
        .batch_execute(
            "CREATE TABLE early_rows (id INT PRIMARY KEY, note TEXT); \
             CREATE INDEX early_note ON early_rows (note)",
        )
        .await
        .unwrap();
    // No shard map yet: these rows are routed to the meta group.
    client
        .batch_execute("INSERT INTO early_rows VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .await
        .unwrap();

    let table = table_id(&http, &base, "early_rows").await;
    let (status, body) = json_post(&http, &format!("{base}/api/v1/shards/{table}/init")).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "init with pre-existing rows: {status} {body}"
    );
    // Bounded wait in case the discovery worker finishes a 202'd drive.
    for _ in 0..100 {
        let map = json_get(&http, &format!("{base}/api/v1/shards/{table}")).await;
        if map["shards"].as_array().is_some_and(|s| !s.is_empty()) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Every pre-init row survived the cutover, by primary key and by index.
    let ids: Vec<i32> = client
        .query("SELECT id FROM early_rows ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, [1, 2, 3], "pre-init rows moved with the copy");
    let row = client
        .query_one("SELECT id FROM early_rows WHERE note = 'beta'", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 2, "secondary index still serves");

    drop(client);
    server.shutdown().await;
}
