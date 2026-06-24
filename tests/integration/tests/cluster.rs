//! Multi-node cluster tests over the in-process [`ClusterFixture`]: real servers,
//! real Raft over loopback, real join protocol. These validate the harness and
//! the genuinely-distributed properties — formation, replication, and quorum
//! fault-tolerance.

use nodus_testkit::cluster::ClusterFixture;
use std::time::Duration;

/// Three nodes form a cluster and a write on the leader replicates to a follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
