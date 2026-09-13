//! Run via tools/testing/mixed_binary.py; ordinary tests never build old revisions.
#[path = "mixed_binary/process.rs"]
mod process;
use process::Matrix;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires two pinned production binaries; run just test-mixed-binary"]
async fn pinned_binary_upgrade_matrix() {
    let mut m = Matrix::new();
    m.start(0, false, false);
    m.alive(0).await;
    m.ready(0).await;
    for i in 1..3 {
        m.start(i, false, true);
        m.alive(i).await;
        m.ready(i).await;
    }
    let leader = m.leader().await;
    m.voters(leader, 3).await;
    for i in 0..3 {
        assert_eq!(m.capability(i).await.get("admission_version"), None);
    }
    m.sql(
        leader,
        "CREATE TABLE mixed_probe (id INT PRIMARY KEY, value TEXT)",
    )
    .await;
    m.sql(leader, "INSERT INTO mixed_probe VALUES (1, 'old')")
        .await;
    m.sql(
        leader,
        "BEGIN; INSERT INTO mixed_probe VALUES (99, 'aborted'); ROLLBACK",
    )
    .await;
    m.record(
        "old_cluster_writes_and_abort",
        json!({"voters":3,"admission_version":0}),
    );

    // Keep this voter's durable history intact. Taking it offline before log
    // compaction forces snapshot catch-up without reverting acknowledged logs.
    m.elect(0).await;
    m.nodes[2].kill();
    m.snapshot(0, 1).await;
    m.start(2, true, true);
    m.alive(2).await;
    m.ready(2).await;
    m.received_snapshot(2, 1).await;
    m.snapshot_login(2, true).await;
    assert_eq!(m.capability(2).await["admission_version"], 1);
    for i in 0..3 {
        assert_eq!(
            m.sql(i, "SELECT id, value FROM mixed_probe ORDER BY id")
                .await,
            vec!["1|old"]
        );
    }
    m.record(
        "old_to_new_v1_snapshot_into_lagging_replica",
        json!({"sender": "old", "receiver":"new","wire":1}),
    );

    m.elect(2).await;
    m.phase(2, "start?target=snapshot-v2", "RollingNodes").await;
    m.phase(2, "node-upgraded?node=3", "ReadyToFinalize").await;
    m.phase(2, "rollback", "Idle").await;
    m.nodes[2].kill();
    m.start(2, false, true);
    m.alive(2).await;
    m.ready(2).await;
    m.elect(2).await;
    assert_eq!(m.state(2).await["phase"], "Idle");
    assert_eq!(
        m.sql(2, "SELECT value FROM mixed_probe WHERE id=1").await,
        vec!["old"]
    );
    m.sql(2, "INSERT INTO mixed_probe VALUES (2, 'rollback')")
        .await;
    m.record(
        "feature_and_executable_rollback_before_finalization",
        json!({"old_binary_reopened_new_authority_log":true}),
    );

    m.nodes[2].kill();
    m.start(2, true, true);
    m.alive(2).await;
    m.ready(2).await;
    m.elect(2).await;
    m.phase(2, "start?target=snapshot-v2", "RollingNodes").await;
    m.phase(2, "node-upgraded?node=3", "ReadyToFinalize").await;
    m.phase(2, "finalize", "Finalized").await;
    let authority = m.state(2).await;
    assert_eq!(authority["cluster_version"], 2);
    assert!(
        !authority["reports"]
            .to_string()
            .contains("admission_version")
    );
    let (_, denied) = m.api(2, "upgrade/rollback", Some(json!({}))).await;
    assert!(denied["error"].as_str().unwrap().contains("finalized"));
    m.record(
        "mixed_binary_finalization_and_closed_feature_rollback",
        json!({"old_voters":2,"new_voters":1,"cluster_version":2}),
    );

    m.sql(2, "UPDATE mixed_probe SET value='v2' WHERE id=1")
        .await;
    m.nodes[1].kill();
    m.snapshot(2, 2).await;
    m.start(1, false, true);
    m.alive(1).await;
    m.ready(1).await;
    m.received_snapshot(1, 2).await;
    m.snapshot_login(1, false).await;
    assert_eq!(
        m.sql(1, "SELECT id, value FROM mixed_probe ORDER BY id")
            .await,
        vec!["1|v2", "2|rollback"]
    );
    m.elect(1).await;
    assert_eq!(m.state(1).await["revision"], authority["revision"]);
    m.sql(1, "INSERT INTO mixed_probe VALUES (3, 'old-v2-reader')")
        .await;
    m.record("new_to_old_v2_snapshot_and_old_leadership",json!({"sender":"new","receiver":"old","wire":2,"authority_revision":authority["revision"]}));

    // Execute an actual new->old replacement after finalized v2, before any
    // admission ledger exists. This pair both understands snapshot v2.
    m.nodes[2].kill();
    m.start(2, false, true);
    m.alive(2).await;
    m.ready(2).await;
    m.elect(2).await;
    assert_eq!(m.state(2).await["phase"], "Finalized");
    assert_eq!(
        m.sql(2, "SELECT value FROM mixed_probe WHERE id=3").await,
        vec!["old-v2-reader"]
    );
    m.record(
        "executable_rollback_after_v2_before_admission",
        json!({"cluster_version":2,"admission_ledger":false}),
    );
    m.nodes[2].kill();
    m.start(2, true, true);
    m.alive(2).await;
    m.ready(2).await;
    m.elect(2).await;
    m.start(3, true, true);
    m.alive(3).await;
    m.reject_join(2, 3).await;
    m.record(
        "admission_refused_with_two_old_voters",
        json!({"voters_unchanged":3}),
    );
    m.nodes[0].kill();
    m.start(0, true, true);
    m.alive(0).await;
    m.ready(0).await;
    m.elect(2).await;
    m.reject_join(2, 3).await;
    m.record(
        "admission_refused_with_one_old_voter",
        json!({"voters_unchanged":3}),
    );

    // Keep the candidate old while upgrading the final existing voter.
    m.nodes[3].kill();
    m.start(3, false, true);
    m.alive(3).await;
    m.nodes[1].kill();
    m.start(1, true, true);
    m.alive(1).await;
    m.ready(1).await;
    m.elect(2).await;
    m.reject_join(2, 3).await;
    m.record(
        "old_candidate_refused_by_all_new_cluster",
        json!({"candidate_admission_version":0}),
    );
    let command = nodus_raftstore::ShardCommand::UpgradeAdmissionV1(
        nodus_raftstore::upgrade::admission::CommandV1 {
            expected_revision: 0,
            operation_id: uuid::Uuid::new_v4(),
            membership: Default::default(),
            challenge: uuid::Uuid::new_v4(),
            reports: vec![],
            action: nodus_raftstore::upgrade::admission::OperationV1::Complete,
        },
    );
    for (index, expected) in [(3, 422), (2, 403)] {
        let response = m
            .peer
            .post(format!(
                "https://{}/raft/shard-meta/write",
                m.nodes[index].raft
            ))
            .json(&command)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected);
    }
    m.record(
        "historical_admission_command_reader_boundary",
        json!({"old_unknown_variant":422,"new_reserved_ingress":403}),
    );

    m.nodes[3].kill();
    m.start(3, true, true);
    m.alive(3).await;
    let (code, joined) = m
        .api(
            2,
            "cluster/join",
            Some(json!({"node_id":4,"raft_advertise_addr":m.nodes[3].raft})),
        )
        .await;
    assert_eq!(code, 200, "{joined}");
    m.ready(3).await;
    m.voters(2, 4).await;
    m.received_snapshot(3, 2).await;
    m.snapshot_login(3, true).await;
    let admitted = m.state(2).await;
    assert!(admitted["member_admission"]["pending"].is_null());
    assert!(admitted["member_admission"]["admitted"]["4"].is_object());
    assert_eq!(admitted["revision"], authority["revision"]);
    m.record(
        "all_new_admission_and_v2_catchup",
        json!({"voters":4,"admission_revision":admitted["member_admission"]["revision"]}),
    );

    m.sql(2, "INSERT INTO mixed_probe VALUES (4, 'after-admission')")
        .await;
    m.nodes[2].kill();
    let leader = m.leader().await;
    m.sql(leader, "INSERT INTO mixed_probe VALUES (5, 'after-crash')")
        .await;
    m.start(2, true, true);
    m.alive(2).await;
    m.ready(2).await;
    let expected = vec![
        "1|v2",
        "2|rollback",
        "3|old-v2-reader",
        "4|after-admission",
        "5|after-crash",
    ];
    for i in 0..4 {
        assert_eq!(
            m.sql(i, "SELECT id, value FROM mixed_probe ORDER BY id")
                .await,
            expected
        );
    }
    m.record(
        "acknowledged_rows_survive_sigkill_and_leader_change",
        json!({"rows":5,"aborted_row_absent":true}),
    );
    let leader = m.leader().await;
    let before = m.state(leader).await;
    let (_, denied) = m.api(leader, "upgrade/rollback", Some(json!({}))).await;
    assert!(denied["error"].as_str().unwrap().contains("finalized"));
    assert_eq!(m.state(leader).await, before);
    m.record("rollback_after_admission_refused",json!({"feature_rollback":"rejected","executable_downgrade":"unsupported: historical binary lacks admission reader; no startup fence claimed"}));
    m.finish();
}
