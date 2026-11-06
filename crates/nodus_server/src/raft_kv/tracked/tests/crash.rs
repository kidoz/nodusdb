use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tracked_coordinator_crash_child() {
    let Some(path) = std::env::var_os("NODUS_TRACKED_CRASH_DIR") else {
        return;
    };
    let c = Cluster::open(std::path::Path::new(&path)).await;
    let txn = TxnId(Uuid::from_u128(999));
    c.write_pair(txn).await;
    let phase = std::env::var("NODUS_TRACKED_CRASH_PHASE").unwrap();
    if phase == "decided" {
        c.engines[0]
            .router
            .set_authority_hook_for_test(Some(Arc::new(|cmd| {
                if matches!(
                    cmd,
                    ShardCommand::TxnParticipantV1(participant::CommandV1 {
                        operation: participant::OperationV1::Resolve { .. },
                        ..
                    })
                ) {
                    std::process::exit(87);
                }
                Ok(())
            })));
    }
    let e = c.engines[0].clone();
    tokio::task::spawn_blocking(move || e.commit(txn, 1_000_000))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(phase, "acknowledged");
    // Acknowledged user commit; skip every Raft and LSM destructor.
    std::process::exit(88);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_exit_after_decision_or_ack_recovers_every_participant() {
    for phase in ["decided", "marker", "acknowledged"] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "raft_kv::tracked::tests::crash::tracked_coordinator_crash_child",
                "--nocapture",
            ])
            .env("NODUS_TRACKED_CRASH_DIR", dir.path())
            .env("NODUS_TRACKED_CRASH_PHASE", phase)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(40);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("tracked crash child timed out");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(
            status.code(),
            Some(match phase {
                "decided" => 87,
                "marker" => 90,
                _ => 88,
            })
        );
        let c = Cluster::open(dir.path()).await;
        let txn = TxnId(Uuid::from_u128(999));
        if phase == "acknowledged" {
            c.assert_rows(true);
        }
        let e = c.engines[1].clone();
        let recovered = tokio::task::spawn_blocking(move || e.recover_pending_txns())
            .await
            .unwrap();
        if let Err(error) = recovered {
            for engine in &c.engines {
                for group in [META_SHARD, c.groups[0].as_str(), c.groups[1].as_str()] {
                    let raft = engine.manager.get(group).await.unwrap();
                    eprintln!(
                        "recovery {phase} node {} group {group}: {:?}",
                        engine.manager.node_id(),
                        raft.metrics().borrow()
                    );
                }
            }
            panic!("recovery after {phase} failed: {error:#}");
        }
        c.assert_rows(true);
        for i in 0..2 {
            assert_eq!(
                participant::read(c.engines[i].engine_for(&c.groups[i]).as_ref(), txn.0)
                    .unwrap()
                    .unwrap()
                    .phase,
                participant::PhaseV1::Committed {
                    commit_ts: 1_000_000
                }
            );
        }
        c.shutdown().await;
    }
}
