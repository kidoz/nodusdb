use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_ingress_rejects_forged_stale_and_unverifiable_decisions() {
    let dir = tempfile::tempdir().unwrap();
    let c = Cluster::open(dir.path()).await;
    let txn = TxnId::new();
    c.write_pair(txn).await;
    let canonical = c.engines[0].authority_read_for_test(txn).await;
    let group = &c.groups[1];
    let raft = c.engines[1].manager.get(group).await.unwrap();
    let address = raft
        .metrics()
        .borrow()
        .membership_config
        .membership()
        .get_node(&2)
        .unwrap()
        .addr
        .clone();
    let transport = c.engines[0].manager.transport();
    let command = |decision: authority::RecordV1| {
        ShardCommand::TxnParticipantV1(participant::CommandV1 {
            txn_id: txn.0,
            owner: canonical.token.owner.clone(),
            generation: canonical.token.generation,
            epoch: canonical.participants[group],
            operation: participant::OperationV1::Resolve {
                decision: Box::new(decision),
            },
        })
    };
    // A CA-authenticated peer can fabricate a structurally valid terminal
    // record. It still cannot turn an active transaction into a commit.
    let mut forged = canonical.clone();
    forged.phase = authority::PhaseV1::Committed {
        commit_ts: 1_000_000,
    };
    let request = command(forged.clone());
    assert!(
        !transport
            .forward_command(&address, group, &request, "forged")
            .await
            .unwrap()
            .success
    );
    let e = c.engines[0].clone();
    tokio::task::spawn_blocking(move || e.abort(txn))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !transport
            .forward_command(&address, group, &request, "stale")
            .await
            .unwrap()
            .success
    );
    let aborted = c.engines[0].authority_read_for_test(txn).await;
    assert!(
        transport
            .forward_command(&address, group, &command(aborted.clone()), "canonical")
            .await
            .unwrap()
            .success
    );
    // Even an exact terminal record is not trusted without current meta quorum.
    let meta = c.engines[0].manager.get(META_SHARD).await.unwrap();
    meta.shutdown().await.unwrap();
    assert!(
        transport
            .forward_command(&address, group, &command(aborted), "no-quorum")
            .await
            .is_err()
    );
    c.assert_rows(false);
    drop((raft, meta, transport));
    c.shutdown().await;
}
