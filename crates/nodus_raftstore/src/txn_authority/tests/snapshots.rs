use super::*;

struct SnapshotFixture;
impl crate::SnapshotCompatibility for SnapshotFixture {
    fn finalized_cluster_version(&self) -> anyhow::Result<u64> {
        Ok(2)
    }
    fn member_snapshot_versions(&self) -> anyhow::Result<BTreeMap<u64, u16>> {
        Ok(BTreeMap::from([(1, 2)]))
    }
}

#[tokio::test]
async fn snapshots_transfer_decisions_and_reject_omitted_or_older_authority() {
    use openraft::storage::RaftSnapshotBuilder;
    use tokio::io::AsyncWriteExt;
    let kv = Arc::new(MemKvEngine::new());
    let txn = Uuid::new_v4();
    let pending = preparing(kv.as_ref(), txn);
    let mut source = NodusRaftStore::with_kv(kv.clone())
        .unwrap()
        .with_snapshot_group("shard-meta")
        .with_snapshot_compatibility(Arc::new(SnapshotFixture));
    {
        let mut sm = source.state_machine.write().await;
        sm.last_applied_log = Some(openraft::LogId::new(
            openraft::CommittedLeaderId::new(1, 1),
            4,
        ));
        sm.last_membership = openraft::StoredMembership::new(
            None,
            openraft::Membership::new(
                vec![std::collections::BTreeSet::from([1])],
                BTreeMap::from([(1, openraft::BasicNode::new("one"))]),
            ),
        );
    }
    let stale = source.build_snapshot().await.unwrap();
    assert!(
        apply(
            kv.as_ref(),
            &command(
                &pending,
                OperationV1::FenceAbort {
                    recovery: owner(20)
                }
            ),
            5
        )
        .unwrap()
        .success
    );
    source.state_machine.write().await.last_applied_log = Some(openraft::LogId::new(
        openraft::CommittedLeaderId::new(1, 1),
        5,
    ));
    let snapshot = source.build_snapshot().await.unwrap();
    let target = Arc::new(MemKvEngine::new());
    let mut receiver = NodusRaftStore::with_kv(target.clone())
        .unwrap()
        .with_snapshot_group("shard-meta");
    receiver
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(
        read(target.as_ref(), txn).unwrap(),
        read(kv.as_ref(), txn).unwrap()
    );
    assert!(
        receiver
            .install_snapshot(&stale.meta, stale.snapshot)
            .await
            .is_err()
    );

    // Give a newer valid snapshot header no authority row: absence cannot erase
    // the terminal fence. Test the actual install path, not just record checks.
    let mut bytes = Vec::new();
    crate::write_snapshot_header(&mut bytes, None)
        .await
        .unwrap();
    let mut file = receiver.begin_receiving_snapshot().await.unwrap();
    file.write_all(&bytes).await.unwrap();
    file.flush().await.unwrap();
    let mut missing_meta = snapshot.meta;
    missing_meta.last_log_id = Some(openraft::LogId::new(
        openraft::CommittedLeaderId::new(1, 1),
        6,
    ));
    missing_meta.snapshot_id = "missing-authority".into();
    assert!(
        receiver
            .install_snapshot(&missing_meta, file)
            .await
            .is_err()
    );
    assert_eq!(
        read(target.as_ref(), txn).unwrap(),
        read(kv.as_ref(), txn).unwrap()
    );
}
