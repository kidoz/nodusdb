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
fn completed_admission(a: &super::super::RecordV1) -> (RecordV1, StoredMembership<u64, BasicNode>) {
    let approved = transition(a, None, &approval(a), &a.membership, 11).unwrap();
    let learner = membership(vec![[1].into()], true);
    let promote = command(Some(&approved), OperationV1::Promote, learner.clone());
    let promoting = transition(a, Some(approved), &promote, &learner, 12).unwrap();
    let stable = membership(vec![[1, 2].into()], true);
    let complete = command(Some(&promoting), OperationV1::Complete, stable.clone());
    let completed = transition(a, Some(promoting), &complete, &stable, 13).unwrap();
    (completed, stable)
}
fn authority_command(
    old: &super::super::RecordV1,
    membership: &StoredMembership<u64, BasicNode>,
    operation: super::super::OperationV1,
) -> super::super::CommandV1 {
    let session = Uuid::new_v4();
    super::super::CommandV1 {
        expected_revision: old.revision,
        membership: membership.clone(),
        session,
        operation,
        reports: membership
            .membership()
            .nodes()
            .map(|(id, _)| {
                let mut report = CapabilityV1::local(*id, session);
                report.ready_for_finalize = true;
                report
            })
            .collect(),
    }
}
fn authority_state(membership: StoredMembership<u64, BasicNode>) -> crate::StateMachine {
    crate::StateMachine {
        last_applied_log: None,
        last_membership: membership,
        last_purged: None,
        kv: None,
        catalog_writer: None,
        catalog_reader: None,
        meta_store: Some(Arc::new(nodus_meta::MemMetaStore::new())),
    }
}
fn persist_records(kv: &dyn KvEngine, a: &super::super::RecordV1, ledger: &RecordV1) {
    for (key, value, revision) in [
        (
            super::super::KEY,
            serde_json::to_vec(a).unwrap(),
            a.revision,
        ),
        (KEY, serde_json::to_vec(ledger).unwrap(), ledger.revision),
    ] {
        let txn = TxnId::new();
        kv.write_intent(txn, Bytes::from_static(key), Bytes::from(value))
            .unwrap();
        kv.commit(txn, revision).unwrap();
    }
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
    let (completed, _) = completed_admission(&a);
    let mut later_admission = completed;
    later_admission.authority_revision = 14;
    later_admission.revision = 15;
    validate_snapshot(&kv, &serde_json::to_vec(&later_admission).unwrap()).unwrap();
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
        let mut receiver = crate::NodusRaftStore::with_kv(kv.clone()).unwrap();
        receiver.state_machine.write().await.meta_store =
            Some(Arc::new(nodus_meta::MemMetaStore::new()));
        let original = receiver.build_snapshot().await.unwrap();
        let mut next = ledger.clone();
        if defect == 1 {
            next.authority_revision = 11;
            next.revision = 12;
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

/// A cluster finalized at the migration level admits and promotes only members
/// whose fresh reports carry `migration_version >= MIGRATION_VERSION`; a stale
/// binary would fail closed on V3 records and diverge.
#[test]
fn level3_admission_requires_migration_capable_reports() {
    let mut a = authority();
    a.cluster_version = MIGRATION_LEVEL;

    // Candidate without a migration capability report: refused at Approve.
    let mut stale_candidate = approval(&a);
    stale_candidate.reports[1].migration_version = None;
    assert!(transition(&a, None, &stale_candidate, &a.membership, 11).is_err());

    // An existing member whose fresh report went stale is refused too.
    let mut stale_member = approval(&a);
    stale_member.reports[0].migration_version = Some(MIGRATION_VERSION - 1);
    assert!(transition(&a, None, &stale_member, &a.membership, 11).is_err());

    // Capable reports admit, and promotion re-checks: a candidate that
    // downgrades between Approve and Promote is refused.
    let approved = transition(&a, None, &approval(&a), &a.membership, 11).unwrap();
    assert!(approved.pending.is_some());
    let learner = membership(vec![[1].into()], true);
    let mut promote = command(Some(&approved), OperationV1::Promote, learner.clone());
    promote.reports[1].migration_version = None;
    assert!(transition(&a, Some(approved.clone()), &promote, &learner, 12).is_err());
    let promote = command(Some(&approved), OperationV1::Promote, learner.clone());
    assert!(transition(&a, Some(approved), &promote, &learner, 12).is_ok());

    // Below the migration level the rule is inert (legacy admission unchanged).
    let a2 = authority();
    let mut legacy = approval(&a2);
    legacy.reports[1].migration_version = None;
    assert!(transition(&a2, None, &legacy, &a2.membership, 11).is_ok());
}

#[test]
fn level4_admission_requires_write_batch_capable_reports() {
    let mut a = authority();
    a.cluster_version = WRITE_BATCH_LEVEL;

    let mut missing_candidate = approval(&a);
    missing_candidate.reports[1].write_batch_version = None;
    assert!(transition(&a, None, &missing_candidate, &a.membership, 11).is_err());

    let mut old_member = approval(&a);
    old_member.reports[0].write_batch_version = Some(WRITE_BATCH_VERSION - 1);
    assert!(transition(&a, None, &old_member, &a.membership, 11).is_err());

    let approved = transition(&a, None, &approval(&a), &a.membership, 11).unwrap();
    let learner = membership(vec![[1].into()], true);
    let mut promote = command(Some(&approved), OperationV1::Promote, learner.clone());
    promote.reports[1].write_batch_version = None;
    assert!(transition(&a, Some(approved.clone()), &promote, &learner, 12).is_err());
    let promote = command(Some(&approved), OperationV1::Promote, learner.clone());
    assert!(transition(&a, Some(approved), &promote, &learner, 12).is_ok());

    let a3 = authority();
    let mut legacy = approval(&a3);
    legacy.reports[1].write_batch_version = None;
    assert!(transition(&a3, None, &legacy, &a3.membership, 11).is_ok());
}

#[test]
fn pending_admission_blocks_start_without_modifying_authority() {
    let kv = nodus_storage_mem::MemKvEngine::new();
    let a = authority();
    let ledger = transition(&a, None, &approval(&a), &a.membership, 11).unwrap();
    assert!(ledger.pending.is_some());
    persist_records(&kv, &a, &ledger);
    let sm = authority_state(a.membership.clone());
    let start = authority_command(&a, &a.membership, super::super::OperationV1::Start);

    let result = super::super::apply(&kv, &sm, &start, 15).unwrap();
    assert!(!result.success);
    assert!(result.error.unwrap().contains("admission is pending"));
    assert_eq!(super::super::read(&kv).unwrap(), a);
    assert_eq!(read(&kv).unwrap(), Some(ledger));
}

#[test]
fn admission_ledger_blocks_rollback_without_modifying_authority() {
    let kv = nodus_storage_mem::MemKvEngine::new();
    let a = authority();
    let (ledger, current) = completed_admission(&a);
    persist_records(&kv, &a, &ledger);
    let sm = authority_state(current.clone());
    let start = authority_command(&a, &current, super::super::OperationV1::Start);
    assert!(super::super::apply(&kv, &sm, &start, 15).unwrap().success);
    let rolling = super::super::read(&kv).unwrap();
    let rollback = authority_command(&rolling, &current, super::super::OperationV1::Rollback);

    let result = super::super::apply(&kv, &sm, &rollback, 16).unwrap();
    assert!(!result.success);
    assert!(result.error.unwrap().contains("admission ledger exists"));
    assert_eq!(super::super::read(&kv).unwrap(), rolling);
    assert_eq!(read(&kv).unwrap(), Some(ledger));
}

#[test]
fn active_upgrades_accept_ancestor_admission_for_startup_validation() {
    let kv = nodus_storage_mem::MemKvEngine::new();
    let a = authority();
    let (ledger, current) = completed_admission(&a);
    persist_records(&kv, &a, &ledger);
    let sm = authority_state(current.clone());
    let start = authority_command(&a, &current, super::super::OperationV1::Start);
    assert!(super::super::apply(&kv, &sm, &start, 15).unwrap().success);

    let rolling = super::super::read(&kv).unwrap();
    assert_eq!(rolling.phase, Phase::RollingNodes);
    assert_eq!(super::approved(&rolling, Some(&ledger)).unwrap().len(), 2);
    validate_stable(&rolling, Some(&ledger), &current).unwrap();

    let refresh = authority_command(&rolling, &current, super::super::OperationV1::Refresh);
    assert!(super::super::apply(&kv, &sm, &refresh, 16).unwrap().success);
    let ready = super::super::read(&kv).unwrap();
    assert_eq!(ready.phase, Phase::ReadyToFinalize);
    assert_eq!(super::approved(&ready, Some(&ledger)).unwrap().len(), 2);
    validate_stable(&ready, Some(&ledger), &current).unwrap();

    let mut idle = ready.clone();
    idle.phase = Phase::Idle;
    assert!(super::approved(&idle, Some(&ledger)).is_err());
    let mut future = ledger.clone();
    future.authority_revision = ready.revision + 1;
    future.revision = ready.revision + 2;
    assert!(super::approved(&ready, Some(&future)).is_err());
    assert!(validate_stable(&ready, Some(&future), &current).is_err());
}

#[test]
fn sequential_finalization_preserves_admission_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let kv =
        Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap());
    let a = authority();
    let (ledger, current) = completed_admission(&a);
    let ledger_bytes = serde_json::to_vec(&ledger).unwrap();
    persist_records(kv.as_ref(), &a, &ledger);
    let sm = authority_state(current.clone());

    for (level, first_index) in [(3, 15), (4, 18)] {
        for (index, operation) in [
            (first_index, super::super::OperationV1::Start),
            (first_index + 1, super::super::OperationV1::Refresh),
        ] {
            let old = super::super::read(kv.as_ref()).unwrap();
            let cmd = authority_command(&old, &current, operation);
            assert!(
                super::super::apply(kv.as_ref(), &sm, &cmd, index)
                    .unwrap()
                    .success
            );
        }
        let ready = super::super::read(kv.as_ref()).unwrap();
        let finalize = authority_command(&ready, &current, super::super::OperationV1::Finalize);
        let index = first_index + 2;
        assert_eq!(
            kv.get(KEY, index - 1).unwrap().unwrap().as_ref(),
            ledger_bytes
        );
        let result = super::super::apply(kv.as_ref(), &sm, &finalize, index).unwrap();
        assert!(result.success, "{:?}", result.error);

        let authority = super::super::read(kv.as_ref()).unwrap();
        let unchanged = read(kv.as_ref()).unwrap().unwrap();
        assert_eq!(authority.cluster_version, level);
        assert_eq!(authority.revision, index);
        assert!(authority.revision > unchanged.authority_revision);
        assert_eq!(unchanged, ledger);
        assert_eq!(unchanged.authority_revision, a.revision);
        assert_eq!(
            super::approved(&authority, Some(&unchanged)).unwrap().len(),
            2
        );
        assert_eq!(kv.get(KEY, index).unwrap().unwrap().as_ref(), ledger_bytes);
        validate_snapshot(kv.as_ref(), &ledger_bytes).unwrap();

        assert!(
            super::super::apply(kv.as_ref(), &sm, &finalize, index)
                .unwrap()
                .success
        );
        assert_eq!(super::super::read(kv.as_ref()).unwrap(), authority);
        assert_eq!(read(kv.as_ref()).unwrap(), Some(unchanged));
    }
    drop(kv);

    let reopened = nodus_storage_lsm::LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap();
    let authority = super::super::read(&reopened).unwrap();
    let reopened_ledger = read(&reopened).unwrap().unwrap();
    assert_eq!(authority.cluster_version, WRITE_BATCH_LEVEL);
    assert!(authority.revision > reopened_ledger.authority_revision);
    assert_eq!(reopened_ledger, ledger);
    assert_eq!(reopened_ledger.authority_revision, a.revision);
    assert_eq!(
        super::approved(&authority, Some(&reopened_ledger))
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        reopened.get(KEY, u64::MAX).unwrap().unwrap().as_ref(),
        ledger_bytes
    );
}
