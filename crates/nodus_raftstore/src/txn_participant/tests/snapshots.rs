use super::*;
use openraft::storage::RaftSnapshotBuilder;
use tokio::io::AsyncWriteExt;

#[test]
fn migration_waits_for_empty_prepared_participant_until_authoritative_abort() {
    let kv = MemKvEngine::new();
    let txn = Uuid::new_v4();
    assert!(apply(&kv, "shard-a", &writes(txn), 1).unwrap().success);
    for k in [b"a", b"b"] {
        assert!(
            apply(
                &kv,
                "shard-a",
                &command(
                    txn,
                    OperationV1::Repair {
                        key: k.to_vec(),
                        replacement: migration::writes::ReplacementV2::Clear
                    }
                ),
                2
            )
            .unwrap()
            .success
        );
    }
    assert!(
        apply(
            &kv,
            "shard-a",
            &command(
                txn,
                OperationV1::Prepare {
                    keys_hash: digest_keys(&[])
                }
            ),
            3
        )
        .unwrap()
        .success
    );
    assert!(!kv.has_pending_intents(b"").unwrap());
    let acquire = ShardCommand::MigrationV1(migration::MigrationCommandV1::Acquire {
        operation_id: Uuid::new_v4(),
        expected_epoch: 0,
    });
    assert!(
        !migration::apply(&kv, &acquire, 4, None)
            .unwrap()
            .unwrap()
            .success
    );
    assert!(
        apply(&kv, "shard-a", &resolve(decision(txn, false)), 5)
            .unwrap()
            .success
    );
    assert!(
        migration::apply(&kv, &acquire, 6, None)
            .unwrap()
            .unwrap()
            .success
    );
}

#[tokio::test]
async fn participant_fences_survive_snapshot_transfer_and_cannot_be_omitted() {
    let kv = Arc::new(MemKvEngine::new());
    let txn = Uuid::new_v4();
    prepare(kv.as_ref(), txn);
    let prepared = read(kv.as_ref(), txn).unwrap().unwrap();
    assert!(
        apply(kv.as_ref(), "shard-a", &resolve(decision(txn, false)), 3)
            .unwrap()
            .success
    );
    assert!(validate_snapshot(kv.as_ref(), &key(txn), &encode(&prepared).unwrap()).is_err());
    let mut source = NodusRaftStore::with_kv(kv.clone())
        .unwrap()
        .with_snapshot_group("shard-a");
    source.state_machine.write().await.last_applied_log = Some(openraft::LogId::new(
        openraft::CommittedLeaderId::new(1, 1),
        3,
    ));
    let snapshot = source.build_snapshot().await.unwrap();
    assert_ne!(snapshot.meta.snapshot_id, "deferred");
    let target = Arc::new(MemKvEngine::new());
    let mut receiver = NodusRaftStore::with_kv(target.clone())
        .unwrap()
        .with_snapshot_group("shard-a");
    receiver
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    let legacy = openraft::Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 4),
        payload: openraft::EntryPayload::Normal(ShardCommand::AbortTxn {
            txn_id: txn.to_string(),
            shard_id: None,
            recorded: false,
        }),
    };
    assert!(
        !receiver.apply_to_state_machine(&[legacy]).await.unwrap()[0].success,
        "install must enable the tracked-ID guard before serving legacy commands"
    );
    assert_eq!(
        read(target.as_ref(), txn).unwrap(),
        read(kv.as_ref(), txn).unwrap()
    );
    assert!(
        !apply(target.as_ref(), "shard-a", &writes(txn), 4)
            .unwrap()
            .success
    );
    let mut bytes = Vec::new();
    crate::write_snapshot_header(&mut bytes, None)
        .await
        .unwrap();
    let mut file = receiver.begin_receiving_snapshot().await.unwrap();
    file.write_all(&bytes).await.unwrap();
    file.flush().await.unwrap();
    let mut meta = snapshot.meta;
    meta.last_log_id = Some(openraft::LogId::new(
        openraft::CommittedLeaderId::new(1, 1),
        5,
    ));
    meta.snapshot_id = "missing-participant".into();
    assert!(receiver.install_snapshot(&meta, file).await.is_err());
    assert_eq!(
        read(target.as_ref(), txn).unwrap(),
        read(kv.as_ref(), txn).unwrap()
    );
}
