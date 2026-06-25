//! Multi-node cluster tests over the in-process [`ClusterFixture`]: real servers,
//! real Raft over loopback, real join protocol. These validate the harness and
//! the genuinely-distributed properties — formation, replication, and quorum
//! fault-tolerance.

use nodus_testkit::cluster::ClusterFixture;
use serial_test::serial;
use std::time::Duration;

/// Three nodes form a cluster and a write on the leader replicates to a follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(cluster)]
async fn three_node_cluster_forms_and_replicates() {
    let cluster = ClusterFixture::start(3)
        .await
        .expect("3-node cluster forms");
    assert!(
        cluster.wait_nodes_live(3, Duration::from_secs(45)).await,
        "all three nodes should be live"
    );

    // Write on the seed/leader.
    let leader = cluster.pg_client(0).await.unwrap();
    leader
        .simple_query("CREATE TABLE repl (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    leader
        .simple_query("INSERT INTO repl (id, v) VALUES (1, 'hello')")
        .await
        .unwrap();

    // Read it back on a follower — the write must have replicated there (allow a
    // moment for the apply to land).
    let follower = cluster.pg_client(1).await.unwrap();
    let mut seen: Option<String> = None;
    for _ in 0..50 {
        if let Ok(rows) = follower.query("SELECT v FROM repl WHERE id = 1", &[]).await
            && let Some(row) = rows.first()
        {
            seen = Some(row.get::<_, String>(0));
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        seen.as_deref(),
        Some("hello"),
        "follower should observe the replicated write"
    );
}

/// With one follower down the leader keeps quorum (2/3) and still serves writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(cluster)]
async fn cluster_tolerates_a_follower_failure() {
    let mut cluster = ClusterFixture::start(3)
        .await
        .expect("3-node cluster forms");
    assert!(cluster.wait_nodes_live(3, Duration::from_secs(45)).await);

    let leader = cluster.pg_client(0).await.unwrap();
    leader
        .simple_query("CREATE TABLE q (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    leader
        .simple_query("INSERT INTO q (id, v) VALUES (1, 'a')")
        .await
        .unwrap();

    // Stop a follower; the remaining two nodes are a majority, so the leader must
    // still commit writes.
    cluster.stop(2).await.unwrap();
    leader
        .simple_query("INSERT INTO q (id, v) VALUES (2, 'b')")
        .await
        .expect("leader retains quorum with one follower down");

    let rows = leader
        .query("SELECT v FROM q ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    assert_eq!(rows[1].get::<_, String>(0), "b");
}

/// A stopped node restarts from its data dir — durable Raft log/vote + KV +
/// catalog — rejoins the cluster, and serves the data written while it was a
/// member. This only works because the Raft log/vote/applied-state is durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(cluster)]
async fn a_restarted_node_rejoins_and_recovers_its_data() {
    let mut cluster = ClusterFixture::start(3)
        .await
        .expect("3-node cluster forms");
    assert!(cluster.wait_nodes_live(3, Duration::from_secs(45)).await);

    let leader = cluster.pg_client(0).await.unwrap();
    leader
        .simple_query("CREATE TABLE d (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    leader
        .simple_query("INSERT INTO d (id, v) VALUES (1, 'durable')")
        .await
        .unwrap();

    // Restart a follower on its same ports + data directory.
    cluster.stop(2).await.unwrap();
    cluster.restart(2).await.unwrap();

    // It rejoins (membership recovered from its durable Raft state)...
    assert!(
        cluster.wait_nodes_live(3, Duration::from_secs(60)).await,
        "the restarted node should rejoin the cluster"
    );

    // ...and serves the replicated row (recovered or caught up after rejoin).
    let rejoined = cluster.pg_client(2).await.unwrap();
    let mut seen: Option<String> = None;
    for _ in 0..75 {
        if let Ok(rows) = rejoined.query("SELECT v FROM d WHERE id = 1", &[]).await
            && let Some(row) = rows.first()
        {
            seen = Some(row.get::<_, String>(0));
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        seen.as_deref(),
        Some("durable"),
        "the restarted node should recover the replicated write"
    );
}

/// Killing the leader does not lose committed data: the surviving majority
/// re-elects a new leader that keeps serving, and the pre-failure write is still
/// readable. This is the consensus payoff of the harness + durable log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(cluster)]
async fn cluster_survives_leader_failure_and_reelects() {
    let mut cluster = ClusterFixture::start(3)
        .await
        .expect("3-node cluster forms");
    assert!(cluster.wait_nodes_live(3, Duration::from_secs(45)).await);

    // Commit a row through the original leader (the seed, node 0).
    let leader = cluster.pg_client(0).await.unwrap();
    leader
        .simple_query("CREATE TABLE f (id INT PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    leader
        .simple_query("INSERT INTO f (id, v) VALUES (1, 'before')")
        .await
        .unwrap();

    // Kill the leader. The surviving two nodes (1, 2) are a majority and must
    // re-elect; one of them starts accepting writes.
    cluster.stop(0).await.unwrap();
    let new_leader = cluster
        .write_on_any(
            &[1, 2],
            "INSERT INTO f (id, v) VALUES (2, 'after')",
            Duration::from_secs(60),
        )
        .await
        .expect("a surviving node should win the election and accept writes");

    // The new leader serves both the pre-failure and post-failover rows — no
    // committed data was lost across the leadership change.
    let client = cluster.pg_client(new_leader).await.unwrap();
    let mut rows = Vec::new();
    for _ in 0..50 {
        if let Ok(r) = client.query("SELECT v FROM f ORDER BY id", &[]).await
            && r.len() == 2
        {
            rows = r;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(rows.len(), 2, "both rows should be present after failover");
    assert_eq!(rows[0].get::<_, String>(0), "before");
    assert_eq!(rows[1].get::<_, String>(0), "after");
}

/// A shard map created on one node replicates through the meta Raft group so
/// every node routes identically. Before Phase 1 this write was node-local.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(cluster)]
async fn shard_map_replicates_across_the_cluster() {
    let cluster = ClusterFixture::start(3)
        .await
        .expect("3-node cluster forms");
    assert!(cluster.wait_nodes_live(3, Duration::from_secs(45)).await);

    // A fixed table id (no catalog row needed — the shard map is keyed by id).
    let table = "11111111-1111-1111-1111-111111111111";

    // Initialize a shard on the meta leader (node 0). This write replicates
    // through the meta group to every node's local store.
    let init = cluster
        .admin_post(0, &format!("/api/v1/shards/{table}/init"))
        .await
        .expect("shard init succeeds on the leader");
    assert!(init.get("shard_id").is_some(), "shard created: {init:?}");

    // A different node now sees the same shard map (allow time for the apply).
    let mut shards_seen = 0;
    for _ in 0..60 {
        if let Ok(map) = cluster
            .admin_get(2, &format!("/api/v1/shards/{table}"))
            .await
        {
            shards_seen = map
                .get("shards")
                .and_then(|s| s.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            if shards_seen > 0 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        shards_seen, 1,
        "the shard map should replicate to another node"
    );
}

/// A data shard placed on one node forms a Raft group replicated across the
/// whole cluster: the primary drives every other node to host a replica and
/// folds them in as voters. Before Phase 2 a data group was single-node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial(cluster)]
async fn data_shard_forms_a_multi_node_group() {
    let cluster = ClusterFixture::start(3)
        .await
        .expect("3-node cluster forms");
    assert!(cluster.wait_nodes_live(3, Duration::from_secs(45)).await);

    let table = "22222222-2222-2222-2222-222222222222";

    // Create the shard map on the meta leader (node 0 = node_id 1), then place
    // the single shard on that node so it becomes the group's primary. The
    // rebalance triggers an immediate reconcile that forms the group across the
    // cluster.
    cluster
        .admin_post(0, &format!("/api/v1/shards/{table}/init"))
        .await
        .expect("shard init succeeds");
    cluster
        .admin_post(0, &format!("/api/v1/shards/{table}/rebalance?nodes=1"))
        .await
        .expect("placing the shard on node 1 succeeds");

    // The primary (node 0) should report the data group with all three nodes as
    // voters once formation completes.
    let mut primary_voters = 0;
    for _ in 0..75 {
        if let Ok(groups) = cluster.admin_get(0, "/api/v1/cluster/groups").await {
            primary_voters = groups
                .get("groups")
                .and_then(|g| g.as_array())
                .and_then(|a| a.first())
                .and_then(|g| g.get("voters"))
                .and_then(|v| v.as_array())
                .map(|v| v.len())
                .unwrap_or(0);
            if primary_voters == 3 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        primary_voters, 3,
        "the data group should span all three nodes as voters"
    );

    // A non-primary node (node 2) must now host its own replica of the group —
    // proof the primary instantiated it cross-node, not just locally.
    let mut replica_hosted = false;
    for _ in 0..75 {
        if let Ok(groups) = cluster.admin_get(2, "/api/v1/cluster/groups").await {
            replica_hosted = groups
                .get("groups")
                .and_then(|g| g.as_array())
                .map(|a| !a.is_empty())
                .unwrap_or(false);
            if replica_hosted {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        replica_hosted,
        "a follower should host a replica of the data group"
    );
}
