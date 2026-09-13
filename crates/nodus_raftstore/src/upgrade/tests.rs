use super::*;
use openraft::{CommittedLeaderId, LogId, Membership};
fn roster() -> StoredMembership<u64, BasicNode> {
    StoredMembership::new(
        Some(LogId::new(CommittedLeaderId::new(1, 1), 1)),
        Membership::new(
            vec![[1, 2].into()],
            BTreeMap::from([
                (1, BasicNode::new("one")),
                (2, BasicNode::new("two")),
                (3, BasicNode::new("learner")),
            ]),
        ),
    )
}
fn command(old: &RecordV1, operation: OperationV1) -> CommandV1 {
    let session = Uuid::new_v4();
    CommandV1 {
        expected_revision: old.revision,
        membership: roster(),
        session,
        operation,
        reports: (1..=3)
            .map(|id| {
                let mut report = CapabilityV1::local(id, session);
                report.ready_for_finalize = true;
                report
            })
            .collect(),
    }
}
#[test]
fn every_voter_and_learner_is_required_and_reports_are_bound_to_challenge() {
    let old = RecordV1::default();
    for defect in 0..6 {
        let mut cmd = command(&old, OperationV1::Start);
        match defect {
            0 => {
                cmd.reports.pop();
            }
            1 => {
                cmd.reports[2].node_id = 99;
            }
            2 => {
                cmd.reports[2] = cmd.reports[0].clone();
            }
            3 => {
                cmd.reports[1].challenge = Uuid::new_v4();
            }
            4 => {
                cmd.reports[1].snapshot_version = 1;
            }
            _ => {
                cmd.reports[1].ready_for_finalize = false;
            }
        }
        assert!(transition(&old, &cmd, &roster(), 2).is_err());
    }
}
#[test]
fn finalization_requires_fresh_reports_and_exact_stable_membership() {
    let start = command(&RecordV1::default(), OperationV1::Start);
    let rolling = transition(&RecordV1::default(), &start, &roster(), 2).unwrap();
    assert_eq!(rolling.cluster_version, 1);
    assert!(
        transition(
            &rolling,
            &command(&rolling, OperationV1::Finalize),
            &roster(),
            3
        )
        .is_err()
    );
    let ready = transition(
        &rolling,
        &command(&rolling, OperationV1::Refresh),
        &roster(),
        3,
    )
    .unwrap();
    let mut stale = command(&ready, OperationV1::Finalize);
    stale.session = ready.session;
    stale.reports = ready.reports.values().cloned().collect();
    assert!(transition(&ready, &stale, &roster(), 4).is_err());
    let mut moved = roster();
    moved = StoredMembership::new(
        moved.log_id().to_owned(),
        Membership::new(
            vec![[1, 2, 3].into()],
            BTreeMap::from([
                (1, BasicNode::new("one")),
                (2, BasicNode::new("two")),
                (3, BasicNode::new("learner")),
            ]),
        ),
    );
    assert!(transition(&ready, &command(&ready, OperationV1::Finalize), &moved, 4).is_err());
    let final_state = transition(
        &ready,
        &command(&ready, OperationV1::Finalize),
        &roster(),
        4,
    )
    .unwrap();
    assert_eq!(final_state.cluster_version, 2);
    assert!(
        transition(
            &final_state,
            &command(&final_state, OperationV1::Rollback),
            &roster(),
            5
        )
        .is_err()
    );
    assert!(decode(&serde_json::to_vec(&final_state).unwrap()).is_ok());
    let kv = nodus_storage_mem::MemKvEngine::new();
    let txn = TxnId::new();
    kv.write_intent(
        txn,
        Bytes::from_static(KEY),
        Bytes::from(serde_json::to_vec(&final_state).unwrap()),
    )
    .unwrap();
    kv.commit(txn, 4).unwrap();
    let mut changed = final_state.clone();
    changed.revision = 5;
    assert!(validate_snapshot(&kv, &serde_json::to_vec(&changed).unwrap(), None).is_err());
    assert!(validate_snapshot(&kv, &serde_json::to_vec(&final_state).unwrap(), None).is_ok());
}
#[test]
fn rollback_is_durable_revision_and_old_sessions_cannot_resume() {
    let old = RecordV1::default();
    let cmd = command(&old, OperationV1::Start);
    let rolling = transition(&old, &cmd, &roster(), 2).unwrap();
    let idle = transition(
        &rolling,
        &command(&rolling, OperationV1::Rollback),
        &roster(),
        3,
    )
    .unwrap();
    assert_eq!(idle.phase, Phase::Idle);
    assert!(transition(&idle, &cmd, &roster(), 4).is_err());
    assert_eq!(idle.cluster_version, 1);
}
#[test]
fn corrupt_unknown_or_incomplete_authority_fails_closed() {
    for bytes in [b"garbage".as_slice(), b"{\"version\":99}", b"{}"] {
        assert!(decode(bytes).is_err());
    }
    let old = RecordV1 {
        cluster_version: 2,
        ..Default::default()
    };
    assert!(decode(&serde_json::to_vec(&old).unwrap()).is_err());
}

#[test]
fn preflight_ignores_local_raft_intents_but_reports_user_and_decision_state() {
    let kv = nodus_storage_mem::MemKvEngine::new();
    let housekeeping = TxnId::new();
    for key in [b"\0raft\0log".as_slice(), b"shard-a\0\0raft\0applied"] {
        kv.write_intent(
            housekeeping,
            Bytes::copy_from_slice(key),
            Bytes::from_static(b"busy"),
        )
        .unwrap();
    }
    assert!(preflight(&kv).unwrap());
    let user = TxnId::new();
    kv.write_intent(
        user,
        Bytes::from_static(b"shard-a\0row"),
        Bytes::from_static(b"pending"),
    )
    .unwrap();
    assert!(!preflight(&kv).unwrap());
    kv.abort(user).unwrap();
    assert!(preflight(&kv).unwrap());
    let decision = TxnId::new();
    kv.write_intent(
        decision,
        Bytes::from_static(b"\0txn2pc\0decision"),
        Bytes::from_static(b"unresolved"),
    )
    .unwrap();
    kv.commit(decision, 10).unwrap();
    assert!(!preflight(&kv).unwrap());
}

#[test]
fn base_authority_bytes_match_reader_that_discards_admission_capability() {
    let mut old = RecordV1::default();
    for (index, action) in [
        (2, OperationV1::Start),
        (3, OperationV1::Refresh),
        (4, OperationV1::Finalize),
    ] {
        let cmd = command(&old, action);
        assert!(cmd.reports.iter().all(|r| r.admission_version == 1));
        let mut legacy_json = serde_json::to_value(&cmd).unwrap();
        for report in legacy_json["reports"].as_array_mut().unwrap() {
            report.as_object_mut().unwrap().remove("admission_version");
        }
        let legacy: CommandV1 = serde_json::from_value(legacy_json).unwrap();
        let new = transition(&old, &cmd, &roster(), index).unwrap();
        let old_reader = transition(&old, &legacy, &roster(), index).unwrap();
        let bytes = serde_json::to_vec(&new).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&old_reader).unwrap());
        assert!(
            !String::from_utf8(bytes)
                .unwrap()
                .contains("admission_version")
        );
        old = new;
    }
}
