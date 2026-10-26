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

    let before_write_batch_refusal = m.state(2).await;
    for i in 0..2 {
        assert_eq!(m.capability(i).await.get("write_batch_version"), None);
    }
    let (status, denied) = m
        .api(2, "upgrade/start?target=migration-v3", Some(json!({})))
        .await;
    assert_eq!(status, 200);
    assert!(
        denied["error"]
            .as_str()
            .unwrap()
            .contains("migration-v3 requires a migration version 4 report"),
        "{denied}"
    );
    let (status, denied) = m
        .api(2, "upgrade/start?target=write-batching-v4", Some(json!({})))
        .await;
    assert_eq!(status, 200);
    assert!(
        denied["error"]
            .as_str()
            .unwrap()
            .contains("supported target is migration-v3"),
        "{denied}"
    );
    let (status, denied) = m
        .api(2, "upgrade/node-upgraded?node=3", Some(json!({})))
        .await;
    assert_eq!(status, 200);
    assert!(denied["error"].is_string(), "{denied}");
    assert_eq!(m.state(2).await, before_write_batch_refusal);
    m.sql(2, "UPDATE mixed_probe SET value='v2' WHERE id=1")
        .await;
    for i in 0..3 {
        assert_eq!(
            m.sql(i, "SELECT value FROM mixed_probe WHERE id=1").await,
            vec!["v2"]
        );
    }
    m.record(
        "write_batching_start_and_preflight_refused_with_old_readers",
        json!({"old_readers":2,"cluster_version":2,"migration_capability_refused":true,"legacy_sql_writes":true}),
    );
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
    assert!(denied["error"].is_string(), "{denied}");
    assert_eq!(m.state(leader).await, before);
    m.record("rollback_after_admission_refused",json!({"feature_rollback":"rejected","executable_downgrade":"unsupported: historical binary lacks admission reader; no startup fence claimed"}));

    m.phase(leader, "start?target=migration-v3", "RollingNodes")
        .await;
    m.phase(
        leader,
        &format!("node-upgraded?node={}", leader + 1),
        "ReadyToFinalize",
    )
    .await;
    m.phase(leader, "finalize", "Finalized").await;
    let migration = m.state(leader).await;
    assert_eq!(migration["cluster_version"], 3);
    assert_eq!(migration["feature_gates"]["migration_v3"], true);
    assert_eq!(migration["feature_gates"]["write_batching"], false);
    m.record(
        "migration_v3_finalized_before_write_batching",
        json!({"voters":4,"cluster_version":3,"write_batching":false}),
    );

    for i in 0..4 {
        let capability = m.capability(i).await;
        assert!(capability["migration_version"].as_u64().unwrap_or(0) >= 4);
        assert!(capability["write_batch_version"].as_u64().unwrap_or(0) >= 4);
    }
    m.phase(leader, "start?target=write-batching-v4", "RollingNodes")
        .await;
    m.phase(
        leader,
        &format!("node-upgraded?node={}", leader + 1),
        "ReadyToFinalize",
    )
    .await;
    m.phase(leader, "finalize", "Finalized").await;
    let write_batching = m.state(leader).await;
    assert_eq!(write_batching["cluster_version"], 4);
    assert_eq!(write_batching["feature_gates"]["write_batching"], true);
    m.record(
        "write_batching_v4_finalized_with_all_candidate_voters",
        json!({"voters":4,"cluster_version":4,"write_batch_version":4}),
    );

    m.sql(
        leader,
        "CREATE TABLE mixed_batched (id INT PRIMARY KEY, value TEXT UNIQUE)",
    )
    .await;
    let copy = "COPY mixed_batched (id, value) FROM STDIN;\n\
                10\tcopy-10\n\
                11\tcopy-11\n\
                12\tcopy-12\n\
                13\tcopy-13\n\
                \\.\n";
    let response = m
        .http
        .post(format!(
            "http://{}/api/v1/import?on_error=stop&batch_rows=2",
            m.nodes[leader].http
        ))
        .bearer_auth(process::TOKEN)
        .header(reqwest::header::CONTENT_TYPE, "text/plain")
        .body(copy)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let report = response.json::<serde_json::Value>().await.unwrap();
    assert_eq!(report["report"]["rows_inserted"], 4, "{report}");
    assert_eq!(report["report"]["statements_failed"], 0, "{report}");
    assert_eq!(report["report"]["stopped_early"], false, "{report}");
    m.sql(
        leader,
        "INSERT INTO mixed_batched VALUES \
         (20, 'insert-20'), (21, 'insert-21'), \
         (22, 'insert-22'), (23, 'insert-23')",
    )
    .await;
    let mut writers = tokio::task::JoinSet::new();
    for worker in 0..8 {
        let pg = m.nodes[leader].pg;
        writers.spawn(async move {
            let (client, connection) = tokio_postgres::connect(
                &format!("host=127.0.0.1 port={pg} user=nodus password=nodus dbname=default"),
                tokio_postgres::NoTls,
            )
            .await
            .unwrap();
            let task = tokio::spawn(connection);
            for offset in 0..4 {
                let id = 1000 + worker * 4 + offset;
                let value = format!("concurrent-{id}");
                client
                    .execute("INSERT INTO mixed_batched VALUES ($1, $2)", &[&id, &value])
                    .await
                    .unwrap();
            }
            drop(client);
            task.abort();
        });
    }
    while let Some(writer) = writers.join_next().await {
        writer.unwrap();
    }
    m.record(
        "write_batching_indexed_copy_multirow_insert_and_concurrent_commits",
        json!({"copy_rows":4,"multirow_insert_rows":4,"concurrent_insert_rows":32,"unique_index":true}),
    );

    m.nodes[leader].kill();
    let replacement = m.leader().await;
    m.sql(
        replacement,
        "INSERT INTO mixed_batched VALUES (2000, 'after-v4-crash')",
    )
    .await;
    m.start(leader, true, true);
    m.alive(leader).await;
    m.ready(leader).await;
    let mut expected = Vec::new();
    for id in 10..14 {
        expected.push(format!("{id}|copy-{id}"));
    }
    for id in 20..24 {
        expected.push(format!("{id}|insert-{id}"));
    }
    for id in 1000..1032 {
        expected.push(format!("{id}|concurrent-{id}"));
    }
    expected.push("2000|after-v4-crash".into());
    for i in 0..4 {
        assert_eq!(
            m.sql(i, "SELECT id, value FROM mixed_batched ORDER BY id")
                .await,
            expected
        );
        assert_eq!(
            m.sql(i, "SELECT id FROM mixed_batched WHERE value='copy-11'")
                .await,
            vec!["11"]
        );
        assert_eq!(
            m.sql(
                i,
                "SELECT id FROM mixed_batched WHERE value='concurrent-1031'"
            )
            .await,
            vec!["1031"]
        );
        assert_eq!(
            m.sql(
                i,
                "SELECT id FROM mixed_batched WHERE value='after-v4-crash'"
            )
            .await,
            vec!["2000"]
        );
    }
    m.record(
        "batched_rows_and_indexes_survive_sigkill_and_leader_change",
        json!({"rows":41,"members":4,"post_failover_rows":1,"index_reads_per_member":3}),
    );
    m.finish();
}
