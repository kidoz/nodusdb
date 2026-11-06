use super::*;
use crate::NodusRaftStore;
use authority::{CommandV1 as ACommand, OperationV1 as Op, OwnerV1};
use nodus_storage_mem::MemKvEngine;
use openraft::storage::{RaftSnapshotBuilder, RaftStorage};
use std::sync::Arc;

fn decided(kv: &dyn KvEngine, start: u64) -> RecordV1 {
    let txn = Uuid::new_v4();
    let owner = OwnerV1 {
        node_id: 1,
        incarnation: Uuid::new_v4(),
    };
    let apply = |expected, operation, index| {
        assert!(
            authority::apply(
                kv,
                &ACommand {
                    txn_id: txn,
                    expected,
                    operation
                },
                index
            )
            .unwrap()
            .success
        );
    };
    apply(None, Op::Begin { owner }, start);
    for (offset, op) in [
        Op::Enroll {
            group: "a".into(),
            epoch: 0,
        },
        Op::Enroll {
            group: "b".into(),
            epoch: 1,
        },
        Op::Seal,
        Op::Commit {
            commit_ts: 1_000_000,
        },
    ]
    .into_iter()
    .enumerate()
    {
        let old = authority::read(kv, txn).unwrap().unwrap();
        apply(Some(old.token), op, start + 1 + offset as u64);
    }
    authority::read(kv, txn).unwrap().unwrap()
}
fn ack(record: &RecordV1, group: &str) -> CommandV1 {
    CommandV1::Acknowledge {
        txn_id: record.txn_id,
        decision: record.token.clone(),
        group: group.into(),
    }
}

#[test]
fn terminal_queue_and_durable_acknowledgements_are_idempotent() {
    let kv = MemKvEngine::new();
    let r = decided(&kv, 1);
    assert_eq!(
        pending_page(&kv, 0, high_water(&kv).unwrap(), 32).unwrap(),
        vec![(5, r.txn_id)]
    );
    assert!(apply(&kv, &ack(&r, "a"), 6).unwrap().success);
    assert!(
        progress(&kv, r.txn_id)
            .unwrap()
            .unwrap()
            .acknowledged
            .contains("a")
    );
    assert!(apply(&kv, &ack(&r, "a"), 7).unwrap().success);
    assert!(!apply(&kv, &ack(&r, "unknown"), 8).unwrap().success);
    let mut stale = r.clone();
    stale.token.generation += 1;
    assert!(!apply(&kv, &ack(&stale, "b"), 8).unwrap().success);
    assert!(apply(&kv, &ack(&r, "b"), 9).unwrap().success);
    assert!(progress(&kv, r.txn_id).unwrap().unwrap().complete);
    assert!(
        pending_page(&kv, 0, high_water(&kv).unwrap(), 32)
            .unwrap()
            .is_empty()
    );
    assert!(apply(&kv, &ack(&r, "a"), 10).unwrap().success);
    assert_eq!(authority::read(&kv, r.txn_id).unwrap(), Some(r));
}

#[test]
fn bounded_pages_use_a_fixed_cycle_end_and_conditional_durable_cursor() {
    let kv = MemKvEngine::new();
    for i in 0..5 {
        decided(&kv, 1 + i * 10);
    }
    let end = high_water(&kv).unwrap();
    let first = pending_page(&kv, 0, end, 2).unwrap();
    assert_eq!(first.len(), 2);
    decided(&kv, 100);
    let rest = pending_page(&kv, first[1].0, end, 32).unwrap();
    assert_eq!(rest.len(), 3);
    assert!(rest.iter().all(|(i, _)| *i <= end));
    assert!(
        apply(
            &kv,
            &CommandV1::Advance {
                expected_revision: 0,
                after: first[1].0,
                through: end
            },
            200
        )
        .unwrap()
        .success
    );
    assert!(
        !apply(
            &kv,
            &CommandV1::Advance {
                expected_revision: 0,
                after: 0,
                through: 0
            },
            201
        )
        .unwrap()
        .success
    );
    assert_eq!(cursor(&kv).unwrap().after, first[1].0);
    assert!(
        !apply(
            &kv,
            &CommandV1::Advance {
                expected_revision: 200,
                after: 105,
                through: end
            },
            201
        )
        .unwrap()
        .success
    );
}

#[tokio::test]
async fn snapshots_require_pending_work_or_completion_proof_and_preserve_it() {
    let kv = Arc::new(MemKvEngine::new());
    let r = decided(kv.as_ref(), 1);
    let mut rows: std::collections::BTreeMap<_, _> = kv
        .snapshot_rows(&Default::default())
        .unwrap()
        .into_iter()
        .map(|r| (r.key.clone(), r))
        .collect();
    rows.remove(pending_key(5, r.txn_id).as_slice());
    assert!(validate_snapshot(kv.as_ref(), &rows, 5).is_err());
    let mut source = NodusRaftStore::with_kv(kv.clone())
        .unwrap()
        .with_snapshot_group("shard-meta");
    source.state_machine.write().await.last_applied_log = Some(openraft::LogId::new(
        openraft::CommittedLeaderId::new(1, 1),
        5,
    ));
    let pending = source.build_snapshot().await.unwrap();
    let target = Arc::new(MemKvEngine::new());
    let mut receiver = NodusRaftStore::with_kv(target.clone())
        .unwrap()
        .with_snapshot_group("shard-meta");
    receiver
        .install_snapshot(&pending.meta, pending.snapshot)
        .await
        .unwrap();
    assert_eq!(pending_page(target.as_ref(), 0, 5, 10).unwrap().len(), 1);
    assert!(apply(kv.as_ref(), &ack(&r, "a"), 6).unwrap().success);
    assert!(apply(kv.as_ref(), &ack(&r, "b"), 7).unwrap().success);
    source.state_machine.write().await.last_applied_log = Some(openraft::LogId::new(
        openraft::CommittedLeaderId::new(1, 1),
        7,
    ));
    let completed = source.build_snapshot().await.unwrap();
    receiver
        .install_snapshot(&completed.meta, completed.snapshot)
        .await
        .unwrap();
    assert!(
        progress(target.as_ref(), r.txn_id)
            .unwrap()
            .unwrap()
            .complete
    );
    assert!(pending_page(target.as_ref(), 0, 5, 10).unwrap().is_empty());
}

#[test]
fn undecided_transactions_cannot_acquire_resolution_progress() {
    let kv = MemKvEngine::new();
    let txn = Uuid::new_v4();
    assert!(
        authority::apply(
            &kv,
            &ACommand {
                txn_id: txn,
                expected: None,
                operation: Op::Begin {
                    owner: OwnerV1 {
                        node_id: 1,
                        incarnation: Uuid::new_v4()
                    }
                }
            },
            1
        )
        .unwrap()
        .success
    );
    let record = authority::read(&kv, txn).unwrap().unwrap();
    assert!(!apply(&kv, &ack(&record, "a"), 2).unwrap().success);
    assert!(progress(&kv, txn).unwrap().is_none());
    assert!(pending_page(&kv, 0, 10, 32).unwrap().is_empty());
    assert_eq!(
        authority::read(&kv, txn).unwrap().unwrap().phase,
        PhaseV1::Active
    );
}
