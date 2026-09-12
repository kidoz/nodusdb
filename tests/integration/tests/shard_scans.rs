//! Existing multi-shard tables must produce complete SQL results. Internal
//! commands install a fixture before writes; public online mutations stay off.

use nodus_testkit::TestServer;
use serde_json::{Value, json};
use std::time::Duration;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

async fn connect(server: &TestServer) -> Client {
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

async fn command(http: &reqwest::Client, base: &str, command: Value) {
    let result: Value = http
        .post(format!("{base}/raft/shard-meta/write"))
        .json(&command)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(result["success"], true, "{result}");
}

async fn ids(client: &Client, sql: &str) -> Vec<String> {
    client
        .query(sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_scans_cover_three_shards_and_fail_when_one_is_unavailable() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server).await;
    client
        .batch_execute(
            "CREATE TABLE scan_rows (id TEXT PRIMARY KEY, n INT, bucket TEXT); \
        CREATE INDEX scan_bucket ON scan_rows (bucket)",
        )
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
    let table: Value = http
        .get(format!("{base}/api/v1/catalog/table?name=scan_rows"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let table = table["id"].as_str().unwrap();
    let shard_ids = [
        "11111111-1111-1111-1111-111111111111",
        "22222222-2222-2222-2222-222222222222",
        "33333333-3333-3333-3333-333333333333",
    ];
    let bounds = [
        Vec::new(),
        format!("{table}:m").into_bytes(),
        format!("{table}:t").into_bytes(),
        Vec::new(),
    ];
    let shards: Vec<_> = shard_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            json!({
                "id": id, "name": format!("scan-fixture-{i}"), "version": 1,
                "created_at": "2026-09-12T00:00:00Z", "updated_at": "2026-09-12T00:00:00Z",
                "state": "Public", "table_id": table,
                "start_key": bounds[i], "end_key": bounds[i + 1]
            })
        })
        .collect();
    let map = json!({"table_id": table, "shards": shards});
    command(&http, &base, json!({"UpdateShardMap": map})).await;
    command(
        &http,
        &base,
        json!({"UpdateShardPlacements": {
            shard_ids[0]: "1", shard_ids[1]: "1", shard_ids[2]: "1"
        }}),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let groups: Value = http
                .get(format!("{base}/api/v1/cluster/groups"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if groups["groups"]
                .as_array()
                .is_some_and(|groups| groups.len() == 3 && groups.iter().all(|g| g["leader"] == 1))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("all three data replicas elect a leader");

    client
        .batch_execute(
            "INSERT INTO scan_rows VALUES \
        ('a', 1, 'odd'), ('b', 2, 'even'), ('m', 3, 'odd'), \
        ('n', 4, 'even'), ('t', 5, 'odd'), ('z', 6, 'even')",
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&client, "SELECT id FROM scan_rows ORDER BY id").await,
        ["a", "b", "m", "n", "t", "z"]
    );
    let streamed: Vec<_> = client
        .simple_query("SELECT id FROM scan_rows")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(streamed, ["a", "b", "m", "n", "t", "z"]);
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM scan_rows ORDER BY id LIMIT 3 OFFSET 2"
        )
        .await,
        ["m", "n", "t"]
    );
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM scan_rows WHERE id >= 'm' AND id < 'z' ORDER BY id"
        )
        .await,
        ["m", "n", "t"]
    );
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM scan_rows WHERE bucket = 'even' ORDER BY id"
        )
        .await,
        ["b", "n", "z"]
    );
    let count = client
        .simple_query("SELECT count(*) FROM scan_rows")
        .await
        .unwrap();
    let count = count
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("count row");
    assert_eq!(count.get(0), Some("6"));

    // One read timestamp survives an intervening committed cross-shard update.
    let writer = connect(&server).await;
    client.batch_execute("BEGIN").await.unwrap();
    assert_eq!(
        ids(&client, "SELECT id FROM scan_rows WHERE n < 10 ORDER BY id")
            .await
            .len(),
        6
    );
    writer
        .batch_execute("UPDATE scan_rows SET n = n + 10")
        .await
        .unwrap();
    assert_eq!(
        ids(&client, "SELECT id FROM scan_rows WHERE n < 10 ORDER BY id")
            .await
            .len(),
        6
    );
    client.batch_execute("COMMIT").await.unwrap();
    assert!(
        ids(&client, "SELECT id FROM scan_rows WHERE n < 10")
            .await
            .is_empty()
    );
    client
        .batch_execute("DELETE FROM scan_rows WHERE id = 'b'")
        .await
        .unwrap();
    assert_eq!(
        ids(
            &client,
            "SELECT id FROM scan_rows WHERE bucket = 'even' ORDER BY id"
        )
        .await,
        ["n", "z"]
    );

    client
        .batch_execute("SET nodus.linearizable_reads = on")
        .await
        .unwrap();
    let error = client
        .query("SELECT id FROM scan_rows", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "0A000");
    let error = client
        .simple_query("SELECT id FROM scan_rows")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "0A000");
    client
        .batch_execute("SET nodus.linearizable_reads = off")
        .await
        .unwrap();

    // The last range has no local replica. Even LIMIT 1 must fail preflight,
    // and an index probe must propagate its missing base-row owner's error.
    let mut unavailable = map.clone();
    unavailable["shards"][2]["id"] = json!("44444444-4444-4444-4444-444444444444");
    command(&http, &base, json!({"UpdateShardMap": unavailable})).await;
    for sql in [
        "SELECT id FROM scan_rows",
        "SELECT id FROM scan_rows LIMIT 1",
        "SELECT id FROM scan_rows WHERE bucket = 'even'",
        "INSERT INTO scan_rows VALUES ('zz', 7, 'even')",
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "40001", "{sql}: {error}");
    }
    command(&http, &base, json!({"UpdateShardMap": map})).await;
    assert_eq!(
        ids(&client, "SELECT id FROM scan_rows ORDER BY id").await,
        ["a", "m", "n", "t", "z"]
    );
    drop(writer);
    drop(client);
    server.shutdown().await;
}
