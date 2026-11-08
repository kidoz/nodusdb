use futures_util::SinkExt as _;
use nodus_testkit::benchmark::{read_storage_usage, scrape_metrics};
use nodus_testkit::cluster::ClusterFixture;
use serde_json::Value;
use std::time::{Duration, Instant};

const ROWS: i32 = 6_000;

async fn finalize(cluster: &ClusterFixture, target: &str) {
    let body = cluster
        .admin_post(0, &format!("/api/v1/upgrade/start?target={target}"))
        .await
        .unwrap();
    assert!(body["error"].is_null(), "start {target}: {body}");
    for node in 1..=3 {
        let body = cluster
            .admin_post(0, &format!("/api/v1/upgrade/node-upgraded?node={node}"))
            .await
            .unwrap();
        assert!(body["error"].is_null(), "node-upgraded {node}: {body}");
    }
    let body = cluster
        .admin_post(0, "/api/v1/upgrade/finalize")
        .await
        .unwrap();
    assert!(body["error"].is_null(), "finalize {target}: {body}");
}

async fn initialize_table(cluster: &ClusterFixture) -> (String, String) {
    let client = cluster.pg_client(0).await.unwrap();
    client
        .batch_execute("CREATE TABLE snapshot_index_regression (id INT PRIMARY KEY, value TEXT)")
        .await
        .unwrap();
    let table = cluster
        .admin_get(0, "/api/v1/catalog/table?name=snapshot_index_regression")
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let body = cluster
        .admin_post(0, &format!("/api/v1/shards/{table}/init"))
        .await
        .unwrap();
    assert!(body["error"].is_null(), "shard init: {body}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let map = cluster
            .admin_get(0, &format!("/api/v1/shards/{table}"))
            .await
            .unwrap();
        if let Some(shard) = map["shards"][0]["id"].as_str() {
            return (table, format!("shard-{shard}"));
        }
        assert!(Instant::now() < deadline, "shard map not published: {map}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_catchup(cluster: &ClusterFixture, group: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let mut applied = Vec::new();
        for index in 0..3 {
            let groups = cluster
                .admin_get(index, "/api/v1/cluster/groups")
                .await
                .unwrap();
            applied.push(
                groups["groups"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find_map(|candidate| {
                        (candidate["id"] == group).then(|| candidate["applied"].as_u64())
                    }),
            );
        }
        if applied.first().is_some_and(Option::is_some)
            && applied.iter().all(|value| *value == applied[0])
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{group} did not converge: {applied:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_removal(cluster: &ClusterFixture, group: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut hosted = false;
        for node in &cluster.nodes {
            hosted |= node.raft_applied_index(group).await.is_ok();
        }
        if !hosted {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "retired group {group} remains hosted"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_purge(cluster: &ClusterFixture, group: &str) {
    let address = cluster.nodes[0].http_addr().parse().unwrap();
    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let metrics = scrape_metrics(&http, address).await.unwrap();
        let retained = metrics.get(&format!("nodus_raft_log_entries{{group=\"{group}\"}}"));
        if retained.is_some_and(|entries| *entries <= 1_000.0) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{group} did not purge: {retained:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn diagnostics(cluster: &ClusterFixture) -> Value {
    let mut nodes = Vec::new();
    for index in 0..3 {
        let groups = cluster
            .admin_get(index, "/api/v1/cluster/groups")
            .await
            .unwrap_or_else(|error| Value::String(error.to_string()));
        let catalog = cluster
            .admin_get(
                index,
                "/api/v1/catalog/table?name=snapshot_index_regression",
            )
            .await
            .unwrap_or_else(|error| Value::String(error.to_string()));
        let index_entries = match cluster.nodes[index]
            .local_index_entry_count("snapshot_index_regression", "snapshot_index_value_idx")
        {
            Ok(count) => Value::from(count as u64),
            Err(error) => Value::String(error.to_string()),
        };
        nodes.push(serde_json::json!({
            "node_id": index + 1,
            "groups": groups,
            "catalog": catalog,
            "index_entries": index_entries,
        }));
    }
    Value::Array(nodes)
}

async fn allocated_storage(cluster: &ClusterFixture) -> Vec<(u64, u64)> {
    let mut samples = Vec::with_capacity(cluster.nodes.len());
    for node in &cluster.nodes {
        let usage = read_storage_usage(node.data_path()).await.unwrap();
        samples.push((node.node_id, usage.allocated_bytes.unwrap()));
    }
    samples
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(cluster)]
async fn meta_applied_index_converges_after_catalog_write() {
    let cluster = ClusterFixture::start(3).await.unwrap();
    cluster
        .pg_client(0)
        .await
        .unwrap()
        .batch_execute("CREATE TABLE meta_applied_regression (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut applied = Vec::new();
        for node in &cluster.nodes {
            applied.push(node.raft_applied_index("shard-meta").await.unwrap());
        }
        if applied.first().is_some_and(Option::is_some)
            && applied.iter().all(|value| *value == applied[0])
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "meta applied positions: {applied:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "expensive snapshot and secondary-index replication regression"]
#[serial_test::serial(cluster)]
async fn create_index_after_snapshot_catchup_replicates_every_entry() {
    let certificates = tempfile::tempdir().unwrap();
    let certs = nodus_testkit::write_test_certs(certificates.path());
    let mut cluster = ClusterFixture::start_cluster_mtls(3, &certs, |config| {
        config.cluster.raft_heartbeat_ms = 200;
        config.cluster.raft_election_timeout_min_ms = 600;
        config.cluster.raft_election_timeout_max_ms = 1200;
    })
    .await
    .unwrap();
    for target in ["snapshot-v2", "migration-v3", "write-batching-v4"] {
        finalize(&cluster, target).await;
    }
    let (table, group) = initialize_table(&cluster).await;
    wait_for_catchup(&cluster, &group).await;

    let client = cluster.pg_client(0).await.unwrap();
    let rows = (1..=ROWS)
        .map(|id| format!("{id}\tvalue-{id}\n"))
        .collect::<String>();
    let mut sink = Box::pin(
        client
            .copy_in::<_, bytes::Bytes>("COPY snapshot_index_regression (id, value) FROM STDIN")
            .await
            .unwrap(),
    );
    sink.as_mut().send(bytes::Bytes::from(rows)).await.unwrap();
    assert_eq!(sink.as_mut().finish().await.unwrap(), ROWS as u64);
    wait_for_catchup(&cluster, &group).await;

    cluster.stop(2).await.unwrap();
    cluster.nodes[0]
        .trigger_raft_snapshot(&group)
        .await
        .unwrap();
    wait_for_purge(&cluster, &group).await;
    cluster.restart(2).await.unwrap();
    assert!(cluster.wait_ready(2, Duration::from_secs(120)).await);
    assert!(cluster.wait_nodes_live(3, Duration::from_secs(120)).await);
    wait_for_catchup(&cluster, &group).await;

    let source = group.strip_prefix("shard-").unwrap();
    let split = cluster
        .admin_post(
            0,
            &format!("/api/v1/shards/{table}/split?shard={source}&key={table}:3000"),
        )
        .await
        .unwrap();
    assert!(split["error"].is_null(), "split: {split}");
    let deadline = Instant::now() + Duration::from_secs(300);
    let split_map = loop {
        let map = cluster
            .admin_get(0, &format!("/api/v1/shards/{table}"))
            .await
            .unwrap();
        if map["shards"]
            .as_array()
            .is_some_and(|shards| shards.len() == 2)
        {
            break map;
        }
        assert!(
            Instant::now() < deadline,
            "split did not finish: {split}; {map}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let rebalance = cluster
        .admin_post(0, &format!("/api/v1/shards/{table}/rebalance?nodes=1,2,3"))
        .await
        .unwrap();
    assert_eq!(rebalance["rebalanced"], true, "rebalance: {rebalance}");
    for shard in split_map["shards"].as_array().unwrap() {
        wait_for_catchup(
            &cluster,
            &format!("shard-{}", shard["id"].as_str().unwrap()),
        )
        .await;
    }
    let storage_before = allocated_storage(&cluster).await;
    wait_for_removal(&cluster, &group).await;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let namespace_counts = cluster
            .nodes
            .iter()
            .map(|node| node.local_namespace_row_count(&group))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let storage_after = allocated_storage(&cluster).await;
        let empty = namespace_counts.iter().all(|count| *count == 0);
        let no_growth = storage_before.iter().zip(&storage_after).all(
            |((before_id, before), (after_id, after))| before_id == after_id && after <= before,
        );
        let reduced = storage_before.iter().zip(&storage_after).any(
            |((before_id, before), (after_id, after))| before_id == after_id && after < before,
        );
        if empty && no_growth && reduced {
            println!("physical reclamation: before={storage_before:?}, after={storage_after:?}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "physical reclamation did not converge: before={storage_before:?}, after={storage_after:?}, namespace_counts={namespace_counts:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    client
        .batch_execute("CREATE INDEX snapshot_index_value_idx ON snapshot_index_regression (value)")
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let counts = cluster
            .nodes
            .iter()
            .map(|node| {
                node.local_index_entry_count(
                    "snapshot_index_regression",
                    "snapshot_index_value_idx",
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        if counts.iter().all(|count| *count == ROWS as usize) {
            for index in 0..3 {
                let rows = cluster
                    .pg_client(index)
                    .await
                    .unwrap()
                    .query(
                        "SELECT id FROM snapshot_index_regression WHERE value = $1",
                        &[&"value-3001".to_owned()],
                    )
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1, "node {} indexed lookup", index + 1);
                assert_eq!(rows[0].get::<_, i32>(0), 3001);
            }
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "index entries did not converge: {counts:?}; {}",
                diagnostics(&cluster).await
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
