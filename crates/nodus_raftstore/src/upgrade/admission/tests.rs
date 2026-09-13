use super::*;
use openraft::Membership;
fn membership(voters: Vec<BTreeSet<u64>>, learner: bool) -> StoredMembership<u64, BasicNode> {
    let mut nodes = BTreeMap::from([(1, BasicNode::new("localhost:1001"))]);
    if learner {
        nodes.insert(2, BasicNode::new("localhost:1002"));
    }
    StoredMembership::new(None, Membership::new(voters, nodes))
}
fn authority() -> super::super::RecordV1 {
    let session = Uuid::new_v4();
    let mut report = CapabilityV1::local(1, session);
    report.ready_for_finalize = true;
    super::super::RecordV1 {
        version: 1,
        revision: 10,
        phase: Phase::Finalized,
        cluster_version: 2,
        session,
        membership: membership(vec![[1].into()], false),
        reports: BTreeMap::from([(1, report)]),
    }
}
fn command(
    old: Option<&RecordV1>,
    action: OperationV1,
    membership: StoredMembership<u64, BasicNode>,
) -> CommandV1 {
    let challenge = Uuid::new_v4();
    CommandV1 {
        expected_revision: old.map_or(0, |r| r.revision),
        operation_id: old
            .and_then(|r| r.pending.as_ref())
            .map_or_else(Uuid::new_v4, |p| p.operation),
        membership,
        challenge,
        reports: if matches!(action, OperationV1::Complete) {
            vec![]
        } else {
            (1..=2)
                .map(|id| CapabilityV1::local(id, challenge))
                .collect()
        },
        action,
    }
}
fn approval(a: &super::super::RecordV1) -> CommandV1 {
    command(
        None,
        OperationV1::Approve {
            node_id: 2,
            address: "localhost:1002".into(),
        },
        a.membership.clone(),
    )
}
#[test]
fn learner_promotion_and_joint_consensus_are_ordered_and_retryable() {
    let a = authority();
    let approve = approval(&a);
    let approved = transition(&a, None, &approve, &a.membership, 11).unwrap();
    assert!(decode(&serde_json::to_vec(&approved).unwrap()).is_ok());
    assert_eq!(super::approved(&a, Some(&approved)).unwrap().len(), 2);
    assert!(transition(&a, Some(approved.clone()), &approve, &a.membership, 12).is_err());
    let early = command(Some(&approved), OperationV1::Promote, a.membership.clone());
    assert!(transition(&a, Some(approved.clone()), &early, &early.membership, 12).is_err());
    let learner = membership(vec![[1].into()], true);
    let mut promote = command(Some(&approved), OperationV1::Promote, learner.clone());
    promote.challenge = approve.challenge;
    promote.reports = approve.reports.clone();
    assert!(transition(&a, Some(approved.clone()), &promote, &learner, 12).is_err());
    promote = command(Some(&approved), OperationV1::Promote, learner.clone());
    let promoting = transition(&a, Some(approved), &promote, &learner, 12).unwrap();
    let joint = membership(vec![[1].into(), [1, 2].into()], true);
    validate_progress(&promoting, &joint).unwrap();
    let early = command(Some(&promoting), OperationV1::Complete, joint.clone());
    assert!(transition(&a, Some(promoting.clone()), &early, &joint, 13).is_err());
    let stable = membership(vec![[1, 2].into()], true);
    let done = command(Some(&promoting), OperationV1::Complete, stable.clone());
    let complete = transition(&a, Some(promoting), &done, &stable, 14).unwrap();
    assert!(complete.pending.is_none());
    assert!(decode(&serde_json::to_vec(&complete).unwrap()).is_ok());
}
#[test]
fn admission_requires_all_readers_fresh_identity_and_exclusive_reservation() {
    let a = authority();
    for defect in 0..6 {
        let mut cmd = approval(&a);
        match defect {
            0 => cmd.reports[0].admission_version = 0,
            1 => cmd.reports[1].snapshot_version = 1,
            2 => cmd.reports[1].node_id = 99,
            3 => cmd.reports[1].challenge = Uuid::new_v4(),
            4 => {
                cmd.reports.pop();
            }
            _ => cmd.reports[1] = cmd.reports[0].clone(),
        }
        assert!(transition(&a, None, &cmd, &a.membership, 11).is_err());
    }
    let old = transition(&a, None, &approval(&a), &a.membership, 11).unwrap();
    let mut competing = approval(&a);
    competing.expected_revision = 11;
    assert!(transition(&a, Some(old.clone()), &competing, &a.membership, 12).is_err());
    assert!(validate_progress(&old, &membership(vec![[1, 2].into()], true)).is_err());
    for addr in [
        "localhost",
        "https://localhost:1",
        "user@localhost:1",
        "localhost:0",
        "localhost:3/path",
    ] {
        assert!(validate_address(addr).is_err());
    }
}
#[test]
fn old_capability_fixture_remains_readable_but_cannot_admit() {
    let bytes=br#"{"version":1,"node_id":1,"challenge":"00000000-0000-0000-0000-000000000001","binary_version":"0.1.0","authority_version":1,"snapshot_version":2,"ready_for_finalize":true}"#;
    let old: CapabilityV1 = serde_json::from_slice(bytes).unwrap();
    old.validate(1, old.challenge).unwrap();
    assert_eq!(old.admission_version, 0);
    assert!(require_reader(&old).is_err());
    assert_eq!(serde_json::to_vec(&old).unwrap(), bytes);
}
#[test]
fn snapshots_cannot_erase_or_replace_admitted_identity() {
    let a = authority();
    let record = transition(&a, None, &approval(&a), &a.membership, 11).unwrap();
    let kv = nodus_storage_mem::MemKvEngine::new();
    let txn = TxnId::new();
    kv.write_intent(
        txn,
        Bytes::from_static(KEY),
        Bytes::from(serde_json::to_vec(&record).unwrap()),
    )
    .unwrap();
    kv.commit(txn, 11).unwrap();
    validate_snapshot(&kv, &serde_json::to_vec(&record).unwrap()).unwrap();
    for defect in 0..3 {
        let mut next = record.clone();
        next.revision += 1;
        match defect {
            0 => next.revision -= 2,
            1 => next.authority_revision -= 1,
            _ => next.admitted.get_mut(&2).unwrap().address = "localhost:9999".into(),
        }
        assert!(validate_snapshot(&kv, &serde_json::to_vec(&next).unwrap()).is_err());
    }
}

#[tokio::test]
async fn snapshot_admission_requires_anchor_applied_index_and_no_omission() {
    use openraft::storage::{RaftSnapshotBuilder, RaftStorage};
    use tokio::io::AsyncWriteExt;
    let a = authority();
    let ledger = transition(&a, None, &approval(&a), &a.membership, 11).unwrap();
    for defect in 0..4 {
        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let txn = TxnId::new();
        kv.write_intent(txn, Bytes::from_static(b"row"), Bytes::from_static(b"keep"))
            .unwrap();
        kv.commit(txn, 5).unwrap();
        if defect == 0 {
            let txn = TxnId::new();
            kv.write_intent(
                txn,
                Bytes::from_static(KEY),
                Bytes::from(serde_json::to_vec(&ledger).unwrap()),
            )
            .unwrap();
            kv.commit(txn, 11).unwrap();
        }
        let mut receiver = crate::NodusRaftStore::with_kv(kv.clone());
        receiver.state_machine.write().await.meta_store =
            Some(Arc::new(nodus_meta::MemMetaStore::new()));
        let original = receiver.build_snapshot().await.unwrap();
        let mut next = ledger.clone();
        if defect == 1 {
            next.authority_revision = 9;
        }
        let mut bytes = Vec::new();
        crate::write_snapshot_header(&mut bytes, None)
            .await
            .unwrap();
        if defect != 0 {
            crate::write_kv_record(&mut bytes, KEY, &serde_json::to_vec(&next).unwrap(), 11)
                .await
                .unwrap();
        }
        if defect != 2 {
            crate::write_kv_record(
                &mut bytes,
                super::super::KEY,
                &serde_json::to_vec(&a).unwrap(),
                10,
            )
            .await
            .unwrap();
        }
        let meta = crate::NodusSnapshotMeta {
            last_log_id: Some(openraft::LogId::new(
                openraft::CommittedLeaderId::new(1, 1),
                if defect == 3 { 10 } else { 12 },
            )),
            last_membership: a.membership.clone(),
            snapshot_id: "invalid-admission".into(),
        };
        let mut file = receiver.begin_receiving_snapshot().await.unwrap();
        file.write_all(&bytes).await.unwrap();
        file.flush().await.unwrap();
        assert!(
            receiver.install_snapshot(&meta, file).await.is_err(),
            "defect {defect}"
        );
        assert_eq!(kv.get(b"row", u64::MAX).unwrap().unwrap().as_ref(), b"keep");
        assert_eq!(
            receiver
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .meta
                .snapshot_id,
            original.meta.snapshot_id
        );
    }
}

#[test]
fn stable_roster_checks_voters_as_well_as_addresses() {
    let a = authority();
    validate_stable(&a, None, &a.membership).unwrap();
    let changed = membership(vec![BTreeSet::new()], false);
    assert!(validate_stable(&a, None, &changed).is_err());
    let mut cmd = approval(&a);
    cmd.membership = changed.clone();
    assert!(transition(&a, None, &cmd, &changed, 11).is_err());
    let mut ledger = transition(&a, None, &approval(&a), &a.membership, 11).unwrap();
    ledger.pending.as_mut().unwrap().base = changed;
    assert!(approved(&a, Some(&ledger)).is_err());
}
