//! Candidate-only qualification beyond the pinned level-4 historical matrix.
use super::{
    json,
    process::{Matrix, Node},
};
use std::{path::PathBuf, time::Duration};

pub async fn qualify(m: &mut Matrix) {
    let previous = PathBuf::from(std::env::var_os("NODUS_MIXED_V4").unwrap());
    let leader = m.leader().await;
    let old = (leader + 1) % 4;
    m.nodes[old].kill();
    let seeds: Vec<_> = m
        .nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != old)
        .map(|(_, n)| n.http.clone())
        .collect();
    m.nodes[old].start(&previous, &m.root, &seeds);
    m.alive(old).await;
    m.ready(old).await;
    assert_eq!(m.capability(old).await.get("txn_authority_version"), None);
    // The previous executable coordinates legacy SQL through the current
    // leader. This checks old writers without requiring executable rollback
    // to preserve leadership eligibility across unrelated historical fixes.
    m.sql(
        old,
        "CREATE TABLE tracked_upgrade (id INT PRIMARY KEY, value TEXT)",
    )
    .await;
    m.sql(
        old,
        "CREATE UNIQUE INDEX tracked_upgrade_value ON tracked_upgrade(value)",
    )
    .await;
    m.sql(
        old,
        "INSERT INTO tracked_upgrade VALUES (1, 'before-finalization')",
    )
    .await;
    m.elect(leader).await;
    let before = m.state(leader).await;
    assert_eq!(before["cluster_version"], 4);
    assert_eq!(before["feature_gates"]["transaction_authority"], false);
    let (_, denied) = m
        .api(
            leader,
            "upgrade/start?target=transaction-authority-v5",
            Some(json!({})),
        )
        .await;
    assert!(
        denied["error"]
            .as_str()
            .unwrap()
            .contains("authenticated transaction protocol"),
        "{denied}"
    );
    assert_eq!(m.state(leader).await, before);
    m.record(
        "transaction_authority_old_reader_writes_and_refused_preflight",
        json!({"cluster_version":4,"previous_reader_coordinator":old+1,"authority_unchanged":true}),
    );

    m.nodes[old].kill();
    m.start(old, true, true);
    m.alive(old).await;
    m.ready(old).await;
    for i in 0..4 {
        assert_eq!(m.capability(i).await["txn_authority_version"], 1);
    }
    m.phase(
        leader,
        "start?target=transaction-authority-v5",
        "RollingNodes",
    )
    .await;
    assert_eq!(
        m.state(leader).await["feature_gates"]["transaction_authority"],
        false
    );
    let (_, premature) = m.api(leader, "upgrade/finalize", Some(json!({}))).await;
    assert!(premature.get("error").is_some(), "{premature}");
    m.phase(leader, "node-upgraded?node=1", "ReadyToFinalize")
        .await;
    m.phase(leader, "finalize", "Finalized").await;
    let final_state = m.state(leader).await;
    assert_eq!(final_state["cluster_version"], 5);
    assert_eq!(final_state["feature_gates"]["transaction_authority"], true);
    let (_, rollback) = m.api(leader, "upgrade/rollback", Some(json!({}))).await;
    let reason = rollback["error"].as_str().unwrap();
    assert!(
        reason.contains("finalized") || reason.contains("admission ledger exists"),
        "{rollback}"
    );
    assert_eq!(
        m.state(leader).await,
        final_state,
        "rollback changed finalized authority"
    );
    m.record(
        "transaction_authority_v5_finalizes_only_after_fresh_reports",
        json!({"voters":4,"premature_finalize":"rejected","rollback":"closed","cluster_version":5}),
    );

    // Submit through a follower, exercising verified production forwarding.
    let follower = (leader + 1) % 4;
    m.sql(follower, "BEGIN; INSERT INTO tracked_upgrade VALUES (2, 'tracked'); SAVEPOINT kept; INSERT INTO tracked_upgrade VALUES (90, 'cleared'); ROLLBACK TO kept; COMMIT").await;
    m.sql(
        follower,
        "BEGIN; INSERT INTO tracked_upgrade VALUES (91, 'aborted'); ROLLBACK",
    )
    .await;
    let owner = json!({"node_id":1,"incarnation":uuid::Uuid::new_v4()});
    let txn = uuid::Uuid::new_v4();
    let forged = json!({"TxnParticipantV1":{"txn_id":txn,"owner":owner,"generation":1,"epoch":0,
        "operation":{"Resolve":{"decision":{"txn_id":txn,"token":{"owner":owner,"generation":1,"revision":1},
            "participants":{"shard-meta":0},"phase":{"Committed":{"commit_ts":1_000_000}}}}}}});
    let response: serde_json::Value = m
        .peer
        .post(format!(
            "https://{}/raft/shard-meta/write",
            m.nodes[leader].raft
        ))
        .json(&forged)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["success"], false, "{response}");
    for i in 0..4 {
        assert_eq!(
            m.sql(i, "SELECT id, value FROM tracked_upgrade ORDER BY id")
                .await,
            vec!["1|before-finalization", "2|tracked"]
        );
        assert_eq!(
            m.sql(i, "SELECT id FROM tracked_upgrade WHERE value='tracked'")
                .await,
            vec!["2"]
        );
    }
    m.record(
        "tracked_forwarding_savepoints_abort_indexes_and_forged_decision_refusal",
        json!({"members":4,"committed_rows":2,"invisible_rows":2,"forged_decision":"rejected"}),
    );

    m.nodes[leader].kill();
    let replacement = m.leader().await;
    m.sql(
        replacement,
        "INSERT INTO tracked_upgrade VALUES (3, 'after-decision-crash')",
    )
    .await;
    m.start(leader, true, true);
    m.alive(leader).await;
    m.ready(leader).await;
    for i in 0..4 {
        assert_eq!(
            m.sql(i, "SELECT id FROM tracked_upgrade ORDER BY id").await,
            vec!["1", "2", "3"]
        );
    }
    m.record(
        "tracked_acknowledged_writes_survive_sigkill_and_meta_failover",
        json!({"members":4,"rows":3,"new_leader":replacement+1}),
    );

    let lagging = (replacement + 1) % 4;
    m.nodes[lagging].kill();
    m.snapshot(replacement, 2).await;
    let bytes = std::fs::read(m.nodes[replacement].snapshot()).unwrap();
    for prefix in [
        b"\x01txn-authority/".as_slice(),
        b"\x01txn-participant/",
        b"\x01txn-resolution/",
    ] {
        assert!(
            bytes.windows(prefix.len()).any(|w| w == prefix),
            "snapshot missing transaction control records"
        );
    }
    m.start(lagging, true, true);
    m.alive(lagging).await;
    m.ready(lagging).await;
    m.received_snapshot(lagging, 2).await;
    m.snapshot_login(lagging, true).await;
    assert_eq!(
        m.sql(
            lagging,
            "SELECT id FROM tracked_upgrade WHERE value='after-decision-crash'"
        )
        .await,
        vec!["3"]
    );
    m.record(
        "level_five_snapshot_carries_authority_participants_and_resolution_proofs",
        json!({"wire":2,"lagging_member":lagging+1,"index_read":3}),
    );

    let candidate = Node::new(5, &m.root);
    m.nodes.push(candidate);
    m.nodes[4].start(&previous, &m.root, &[]);
    m.alive(4).await;
    let before = m.state(replacement).await;
    let (code, denied) = m
        .api(
            replacement,
            "cluster/join",
            Some(json!({"node_id":5,"raft_advertise_addr":m.nodes[4].raft})),
        )
        .await;
    assert_eq!(code, 409, "{denied}");
    assert!(
        denied["error"]
            .as_str()
            .unwrap()
            .contains("authenticated transaction protocol"),
        "{denied}"
    );
    assert_eq!(m.state(replacement).await, before);
    m.nodes[4].kill();
    m.nodes[lagging].kill();
    m.nodes[lagging].start(&previous, &m.root, &seeds);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = m.nodes[lagging].child.as_mut().unwrap().try_wait().unwrap() {
                assert!(!status.success(), "old binary accepted finalized level 5");
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("old binary did not reject incompatible durable authority");
    m.nodes[lagging].child.take();
    m.start(lagging, true, true);
    m.alive(lagging).await;
    m.ready(lagging).await;
    assert_eq!(
        m.sql(lagging, "SELECT id FROM tracked_upgrade ORDER BY id")
            .await,
        vec!["1", "2", "3"]
    );
    m.record("readerless_admission_and_executable_downgrade_refused_at_level_five",
        json!({"admission":"rejected","authority_unchanged":true,"old_process":"exited with failure","new_restart_rows":3}));
}
