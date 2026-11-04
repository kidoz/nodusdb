use super::*;
use crate::{NodusRaftStore, ShardCommand};
use nodus_storage_mem::MemKvEngine;
use openraft::storage::RaftStorage;
use std::sync::Arc;

mod recovery;
mod snapshots;

fn owner(id: u128) -> OwnerV1 {
    OwnerV1 {
        node_id: 1,
        incarnation: Uuid::from_u128(id),
    }
}

fn begin(txn: Uuid) -> CommandV1 {
    CommandV1 {
        txn_id: txn,
        expected: None,
        operation: OperationV1::Begin { owner: owner(10) },
    }
}

fn command(record: &RecordV1, operation: OperationV1) -> CommandV1 {
    CommandV1 {
        txn_id: record.txn_id,
        expected: Some(record.token.clone()),
        operation,
    }
}

fn preparing(kv: &dyn KvEngine, txn: Uuid) -> RecordV1 {
    assert!(apply(kv, &begin(txn), 1).unwrap().success);
    for (index, operation) in [
        OperationV1::Enroll {
            group: "shard-a".into(),
            epoch: 7,
        },
        OperationV1::Enroll {
            group: "shard-b".into(),
            epoch: 9,
        },
        OperationV1::Seal,
    ]
    .into_iter()
    .enumerate()
    {
        let record = read(kv, txn).unwrap().unwrap();
        assert!(
            apply(kv, &command(&record, operation), index as u64 + 2)
                .unwrap()
                .success
        );
    }
    read(kv, txn).unwrap().unwrap()
}

#[test]
fn first_decision_wins_in_both_orders_and_terminal_records_never_reopen() {
    for commit_first in [false, true] {
        let kv = MemKvEngine::new();
        let txn = Uuid::new_v4();
        let pending = preparing(&kv, txn);
        let commit = command(&pending, OperationV1::Commit { commit_ts: 42 });
        let abort = command(
            &pending,
            OperationV1::FenceAbort {
                recovery: owner(20),
            },
        );
        let (first, second) = if commit_first {
            (&commit, &abort)
        } else {
            (&abort, &commit)
        };
        assert!(apply(&kv, first, 5).unwrap().success);
        assert!(!apply(&kv, second, 6).unwrap().success);
        let terminal = read(&kv, txn).unwrap().unwrap();
        assert_eq!(terminal.participants, pending.participants);
        assert_eq!(terminal.token.revision, pending.token.revision + 1);
        if commit_first {
            assert_eq!(terminal.phase, PhaseV1::Committed { commit_ts: 42 });
            assert!(apply(&kv, &commit, 7).unwrap().success);
        } else {
            assert_eq!(terminal.phase, PhaseV1::Aborted);
            assert_eq!(terminal.token.generation, 2);
            assert_eq!(terminal.token.owner, owner(20));
            assert!(!apply(&kv, &commit, 7).unwrap().success);
        }
        for operation in [
            OperationV1::Enroll {
                group: "late".into(),
                epoch: 0,
            },
            OperationV1::Seal,
            OperationV1::Commit { commit_ts: 43 },
            OperationV1::FenceAbort {
                recovery: owner(30),
            },
        ] {
            assert!(
                !apply(&kv, &command(&terminal, operation), 8)
                    .unwrap()
                    .success
            );
        }
        assert!(!apply(&kv, &begin(txn), 9).unwrap().success);
        assert_eq!(read(&kv, txn).unwrap().unwrap(), terminal);
    }
}

#[test]
fn enrollment_and_sealing_require_exact_tokens_and_stable_epochs() {
    let kv = MemKvEngine::new();
    let txn = Uuid::new_v4();
    assert!(apply(&kv, &begin(txn), 1).unwrap().success);
    let initial = read(&kv, txn).unwrap().unwrap();
    for operation in [OperationV1::Seal, OperationV1::Commit { commit_ts: 1 }] {
        assert!(
            !apply(&kv, &command(&initial, operation), 2)
                .unwrap()
                .success
        );
    }
    let enroll = command(
        &initial,
        OperationV1::Enroll {
            group: "a".into(),
            epoch: 1,
        },
    );
    assert!(apply(&kv, &enroll, 2).unwrap().success);
    assert!(!apply(&kv, &enroll, 3).unwrap().success);
    let enrolled = read(&kv, txn).unwrap().unwrap();
    assert!(
        !apply(
            &kv,
            &command(
                &enrolled,
                OperationV1::Enroll {
                    group: "a".into(),
                    epoch: 2
                }
            ),
            3
        )
        .unwrap()
        .success
    );
    for token in [
        TokenV1 {
            owner: owner(20),
            ..enrolled.token.clone()
        },
        TokenV1 {
            generation: 2,
            ..enrolled.token.clone()
        },
        TokenV1 {
            revision: 99,
            ..enrolled.token.clone()
        },
    ] {
        let mut stale = command(&enrolled, OperationV1::Abort);
        stale.expected = Some(token);
        assert!(!apply(&kv, &stale, 3).unwrap().success);
    }
    assert!(
        apply(&kv, &command(&enrolled, OperationV1::Seal), 3)
            .unwrap()
            .success
    );
    let sealed = read(&kv, txn).unwrap().unwrap();
    for operation in [
        OperationV1::Enroll {
            group: "b".into(),
            epoch: 0,
        },
        OperationV1::Commit { commit_ts: 0 },
        OperationV1::FenceAbort {
            recovery: sealed.token.owner.clone(),
        },
    ] {
        assert!(!apply(&kv, &command(&sealed, operation), 4).unwrap().success);
    }
    assert_eq!(read(&kv, txn).unwrap().unwrap(), sealed);
    assert!(
        apply(&kv, &command(&sealed, OperationV1::Abort), 4)
            .unwrap()
            .success
    );
    assert!(
        apply(&kv, &command(&sealed, OperationV1::Abort), 5)
            .unwrap()
            .success
    );
}

#[test]
fn missing_invalid_and_exhausted_records_fail_closed() {
    let kv = MemKvEngine::new();
    let txn = Uuid::new_v4();
    let mut record = preparing(&kv, txn);
    let absent = command(
        &RecordV1 {
            txn_id: Uuid::new_v4(),
            ..record.clone()
        },
        OperationV1::Abort,
    );
    assert!(!apply(&kv, &absent, 6).unwrap().success);
    for bytes in [
        b"{}".to_vec(),
        nodus_common::versioned::encode(2, b"{}"),
        nodus_common::versioned::encode(1, b"{}"),
        vec![0; MAX_RECORD_BYTES + 1],
    ] {
        assert!(decode(&bytes).is_err());
    }
    for defect in 0..7 {
        let mut malformed = record.clone();
        match defect {
            0 => malformed.txn_id = Uuid::nil(),
            1 => malformed.token.owner.incarnation = Uuid::nil(),
            2 => malformed.token.generation = 0,
            3 => malformed.token.revision = 0,
            4 => malformed.participants.clear(),
            5 => malformed.phase = PhaseV1::Committed { commit_ts: 0 },
            _ => {
                malformed.participants.insert("\0bad".into(), 0);
            }
        }
        assert!(
            decode(&nodus_common::versioned::encode(
                1,
                &serde_json::to_vec(&malformed).unwrap()
            ))
            .is_err()
        );
    }
    record.token.revision = u64::MAX;
    assert!(transition(Some(&record), &command(&record, OperationV1::Abort)).is_err());
    record.token.generation = u64::MAX;
    assert!(
        transition(
            Some(&record),
            &command(
                &record,
                OperationV1::FenceAbort {
                    recovery: owner(20)
                }
            )
        )
        .is_err()
    );
    // A corrupt existing value is an apply error, never an absent authority.
    let write = TxnId::new();
    kv.write_intent(write, Bytes::from(key(txn)), Bytes::from_static(b"broken"))
        .unwrap();
    kv.commit(write, 10).unwrap();
    assert!(apply(&kv, &begin(txn), 11).is_err());
}

#[test]
fn snapshot_cannot_roll_back_or_change_a_terminal_decision() {
    let kv = MemKvEngine::new();
    let txn = Uuid::new_v4();
    let pending = preparing(&kv, txn);
    let bytes = |r: &RecordV1| nodus_common::versioned::encode(1, &serde_json::to_vec(r).unwrap());
    assert!(validate_snapshot_record(&kv, &key(txn), &bytes(&pending)).is_ok());
    assert!(validate_snapshot_record(&kv, &key(Uuid::new_v4()), &bytes(&pending)).is_err());
    assert!(
        apply(
            &kv,
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
    assert!(validate_snapshot_record(&kv, &key(txn), &bytes(&pending)).is_err());
    let terminal = read(&kv, txn).unwrap().unwrap();
    assert!(validate_snapshot_record(&kv, &key(txn), &bytes(&terminal)).is_ok());
    let mut rewritten = terminal.clone();
    rewritten.token.revision += 1;
    rewritten.phase = PhaseV1::Committed { commit_ts: 42 };
    assert!(validate_snapshot_record(&kv, &key(txn), &bytes(&rewritten)).is_err());
}

#[tokio::test]
async fn data_groups_cannot_own_authority() {
    let dir = tempfile::tempdir().unwrap();
    let kv = Arc::new(MemKvEngine::new());
    let mut store = NodusRaftStore::with_kv_at(kv.clone(), dir.path().join("snap"))
        .unwrap()
        .with_snapshot_group("shard-a");
    let txn = Uuid::new_v4();
    let entry = openraft::Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
        payload: openraft::EntryPayload::Normal(ShardCommand::TxnAuthorityV1(begin(txn))),
    };
    assert!(!store.apply_to_state_machine(&[entry]).await.unwrap()[0].success);
    assert!(read(kv.as_ref(), txn).unwrap().is_none());
}

#[tokio::test]
async fn every_intent_mutation_path_rejects_authority_keys() {
    use crate::migration::{EpochIntentOperationV1, EpochMutationV1, writes::ReplacementV2};
    let kv = Arc::new(MemKvEngine::new());
    let txn = Uuid::new_v4();
    let record = preparing(kv.as_ref(), txn);
    let write = Uuid::new_v4();
    let key = key(txn);
    let mut store = NodusRaftStore::with_kv(kv.clone())
        .unwrap()
        .with_snapshot_group("shard-meta");
    let commands = vec![
        ShardCommand::PutIntent {
            txn_id: write.to_string(),
            key: key.clone(),
            value: vec![1],
            shard_id: None,
        },
        ShardCommand::DeleteIntent {
            txn_id: write.to_string(),
            key: key.clone(),
            shard_id: None,
        },
        ShardCommand::IndexPutIntent {
            txn_id: write.to_string(),
            index_id: "x".into(),
            key: key.clone(),
            value: vec![1],
        },
        ShardCommand::IndexDeleteIntent {
            txn_id: write.to_string(),
            index_id: "x".into(),
            key: key.clone(),
        },
        ShardCommand::EpochWriteV1 {
            epoch: 0,
            mutation: EpochMutationV1::Put {
                txn_id: write,
                key: key.clone(),
                value: vec![1],
            },
        },
        ShardCommand::EpochWriteV1 {
            epoch: 0,
            mutation: EpochMutationV1::Delete {
                txn_id: write,
                key: key.clone(),
            },
        },
        ShardCommand::EpochRepairV2 {
            epoch: 0,
            txn_id: write,
            key: key.clone(),
            replacement: ReplacementV2::Put(vec![1]),
        },
        ShardCommand::EpochRepairV2 {
            epoch: 0,
            txn_id: write,
            key: key.clone(),
            replacement: ReplacementV2::Clear,
        },
        ShardCommand::EpochWriteBatchV1 {
            epoch: 0,
            txn_id: write,
            operations: vec![
                EpochIntentOperationV1::Put {
                    key: b"safe".to_vec(),
                    value: vec![1],
                },
                EpochIntentOperationV1::Delete { key },
            ],
        },
    ];
    for (offset, cmd) in commands.into_iter().enumerate() {
        let entry = openraft::Entry {
            log_id: openraft::LogId::new(
                openraft::CommittedLeaderId::new(1, 1),
                10 + offset as u64,
            ),
            payload: openraft::EntryPayload::Normal(cmd),
        };
        assert!(!store.apply_to_state_machine(&[entry]).await.unwrap()[0].success);
        assert!(kv.pending_intent_keys(TxnId(write)).is_empty());
        assert_eq!(read(kv.as_ref(), txn).unwrap().unwrap(), record);
        assert!(kv.get(b"safe", u64::MAX).unwrap().is_none());
    }
}
