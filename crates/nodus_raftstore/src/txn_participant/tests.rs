use super::*;
use crate::{
    NodusRaftStore, ShardCommand,
    txn_authority::{PhaseV1 as DecisionPhase, TokenV1},
};
use nodus_storage_mem::MemKvEngine;
use openraft::storage::RaftStorage;
use std::{collections::BTreeMap, sync::Arc};

mod snapshots;

fn owner(n: u128) -> OwnerV1 {
    OwnerV1 {
        node_id: 1,
        incarnation: Uuid::from_u128(n),
    }
}
fn command(txn: Uuid, operation: OperationV1) -> CommandV1 {
    CommandV1 {
        txn_id: txn,
        owner: owner(10),
        generation: 1,
        epoch: 0,
        operation,
    }
}
fn writes(txn: Uuid) -> CommandV1 {
    command(
        txn,
        OperationV1::Write {
            operations: vec![
                migration::EpochIntentOperationV1::Put {
                    key: b"a".to_vec(),
                    value: b"one".to_vec(),
                },
                migration::EpochIntentOperationV1::Put {
                    key: b"b".to_vec(),
                    value: b"two".to_vec(),
                },
            ],
        },
    )
}
fn decision(txn: Uuid, commit: bool) -> DecisionV1 {
    DecisionV1 {
        txn_id: txn,
        token: TokenV1 {
            owner: owner(if commit { 10 } else { 20 }),
            generation: if commit { 1 } else { 2 },
            revision: 5,
        },
        participants: BTreeMap::from([("shard-a".into(), 0)]),
        phase: if commit {
            DecisionPhase::Committed {
                commit_ts: 1_000_000,
            }
        } else {
            DecisionPhase::Aborted
        },
    }
}
fn resolve(d: DecisionV1) -> CommandV1 {
    CommandV1 {
        txn_id: d.txn_id,
        owner: d.token.owner.clone(),
        generation: d.token.generation,
        epoch: 0,
        operation: OperationV1::Resolve {
            decision: Box::new(d),
        },
    }
}
fn prepare(kv: &dyn KvEngine, txn: Uuid) {
    assert!(apply(kv, "shard-a", &writes(txn), 1).unwrap().success);
    assert!(
        apply(
            kv,
            "shard-a",
            &command(
                txn,
                OperationV1::Prepare {
                    keys_hash: digest_keys(&[b"a".to_vec(), b"b".to_vec()])
                }
            ),
            2
        )
        .unwrap()
        .success
    );
}

#[test]
fn prepared_write_set_and_terminal_decisions_are_fenced() {
    for commit in [false, true] {
        let kv = MemKvEngine::new();
        let txn = Uuid::new_v4();
        prepare(&kv, txn);
        assert!(!apply(&kv, "shard-a", &writes(txn), 3).unwrap().success);
        assert!(
            !apply(
                &kv,
                "shard-a",
                &command(
                    txn,
                    OperationV1::Repair {
                        key: b"a".to_vec(),
                        replacement: migration::writes::ReplacementV2::Clear
                    }
                ),
                3
            )
            .unwrap()
            .success
        );
        let resolution = resolve(decision(txn, commit));
        assert!(apply(&kv, "shard-a", &resolution, 3).unwrap().success);
        assert!(apply(&kv, "shard-a", &resolution, 4).unwrap().success);
        for (key, value) in [(b"a", b"one".as_slice()), (b"b", b"two".as_slice())] {
            assert_eq!(
                kv.get(key, u64::MAX).unwrap(),
                commit.then(|| Bytes::copy_from_slice(value))
            );
        }
        assert!(kv.pending_intent_keys(TxnId(txn)).is_empty());
        assert!(!apply(&kv, "shard-a", &writes(txn), 5).unwrap().success);
        assert!(
            !apply(&kv, "shard-a", &resolve(decision(txn, !commit)), 6)
                .unwrap()
                .success
        );
    }
}

#[test]
fn prepare_checks_every_key_and_abort_can_fence_a_delayed_first_write() {
    let kv = MemKvEngine::new();
    let txn = Uuid::new_v4();
    assert!(apply(&kv, "shard-a", &writes(txn), 1).unwrap().success);
    assert!(
        !apply(
            &kv,
            "shard-a",
            &command(
                txn,
                OperationV1::Prepare {
                    keys_hash: digest_keys(&[b"a".to_vec()])
                }
            ),
            2
        )
        .unwrap()
        .success
    );
    assert!(has_unresolved(&kv).unwrap());
    let delayed = Uuid::new_v4();
    assert!(
        apply(&kv, "shard-a", &resolve(decision(delayed, false)), 3)
            .unwrap()
            .success
    );
    assert!(!apply(&kv, "shard-a", &writes(delayed), 4).unwrap().success);
    // Do not adopt or delete pre-existing, untracked legacy intents.
    let legacy = Uuid::new_v4();
    kv.write_intent(
        TxnId(legacy),
        Bytes::from_static(b"legacy"),
        Bytes::from_static(b"keep"),
    )
    .unwrap();
    assert!(!apply(&kv, "shard-a", &writes(legacy), 5).unwrap().success);
    assert!(
        !apply(&kv, "shard-a", &resolve(decision(legacy, false)), 5)
            .unwrap()
            .success
    );
    assert_eq!(
        kv.pending_intent_keys(TxnId(legacy)),
        vec![Bytes::from_static(b"legacy")]
    );
}

#[test]
fn commit_never_acknowledges_missing_prepared_user_intents() {
    let kv = MemKvEngine::new();
    let txn = Uuid::new_v4();
    prepare(&kv, txn);
    kv.abort(TxnId(txn)).unwrap();
    assert!(
        !apply(&kv, "shard-a", &resolve(decision(txn, true)), 3)
            .unwrap()
            .success
    );
    assert!(matches!(
        read(&kv, txn).unwrap().unwrap().phase,
        PhaseV1::Prepared { .. }
    ));
    assert!(kv.get(b"a", u64::MAX).unwrap().is_none());
}

#[test]
fn empty_prepared_transactions_and_crash_gap_markers_resolve_correctly() {
    let kv = MemKvEngine::new();
    let txn = Uuid::new_v4();
    prepare(&kv, txn);
    // Model the durable marker-intent boundary before the atomic KV commit.
    let terminal = RecordV1 {
        txn_id: txn,
        owner: owner(10),
        generation: 1,
        epoch: 0,
        phase: PhaseV1::Committed {
            commit_ts: 1_000_000,
        },
    };
    kv.write_intent(
        TxnId(txn),
        Bytes::from(key(txn)),
        encode(&terminal).unwrap(),
    )
    .unwrap();
    assert!(
        apply(&kv, "shard-a", &resolve(decision(txn, true)), 3)
            .unwrap()
            .success
    );
    assert_eq!(read(&kv, txn).unwrap(), Some(terminal));
    assert_eq!(
        kv.get(b"a", u64::MAX).unwrap(),
        Some(Bytes::from_static(b"one"))
    );
    assert_eq!(
        kv.get(b"b", u64::MAX).unwrap(),
        Some(Bytes::from_static(b"two"))
    );

    let empty = Uuid::new_v4();
    assert!(apply(&kv, "shard-a", &writes(empty), 4).unwrap().success);
    for k in [b"a", b"b"] {
        assert!(
            apply(
                &kv,
                "shard-a",
                &command(
                    empty,
                    OperationV1::Repair {
                        key: k.to_vec(),
                        replacement: migration::writes::ReplacementV2::Clear
                    }
                ),
                5
            )
            .unwrap()
            .success
        );
    }
    assert!(has_unresolved(&kv).unwrap());
    assert!(
        apply(
            &kv,
            "shard-a",
            &command(
                empty,
                OperationV1::Prepare {
                    keys_hash: digest_keys(&[])
                }
            ),
            6
        )
        .unwrap()
        .success
    );
    assert!(
        apply(&kv, "shard-a", &resolve(decision(empty, true)), 7)
            .unwrap()
            .success
    );
    assert!(!has_unresolved(&kv).unwrap());
}

#[tokio::test]
async fn legacy_commands_and_whole_commit_batches_cannot_bypass_tracking() {
    let kv = Arc::new(MemKvEngine::new());
    let txn = Uuid::new_v4();
    prepare(kv.as_ref(), txn);
    let legacy = TxnId::new();
    kv.write_intent(
        legacy,
        Bytes::from_static(b"legacy"),
        Bytes::from_static(b"pending"),
    )
    .unwrap();
    let mut store = NodusRaftStore::with_kv(kv.clone())
        .unwrap()
        .with_snapshot_group("shard-a");
    let commands = vec![
        ShardCommand::PutIntent {
            txn_id: txn.to_string(),
            key: b"late".to_vec(),
            value: vec![1],
            shard_id: None,
        },
        ShardCommand::DeleteIntent {
            txn_id: txn.to_string(),
            key: b"a".to_vec(),
            shard_id: None,
        },
        ShardCommand::CommitTxn {
            txn_id: txn.to_string(),
            commit_ts: 100,
            shard_id: None,
        },
        ShardCommand::AbortTxn {
            txn_id: txn.to_string(),
            shard_id: None,
            recorded: false,
        },
        ShardCommand::AbortTxn {
            txn_id: txn.to_string(),
            shard_id: None,
            recorded: true,
        },
        ShardCommand::PrepareTxn {
            txn_id: txn.to_string(),
            shard_id: None,
        },
        ShardCommand::EpochRepairV2 {
            txn_id: txn,
            epoch: 0,
            key: b"a".to_vec(),
            replacement: migration::writes::ReplacementV2::Clear,
        },
        ShardCommand::EpochCommitBatchV1 {
            epoch: 0,
            commits: vec![
                migration::EpochCommitV1 {
                    txn_id: legacy.0,
                    commit_ts: 100,
                },
                migration::EpochCommitV1 {
                    txn_id: txn,
                    commit_ts: 100,
                },
            ],
        },
        ShardCommand::EpochWriteBatchV1 {
            txn_id: txn,
            epoch: 0,
            operations: vec![migration::EpochIntentOperationV1::Put {
                key: b"late".to_vec(),
                value: vec![1],
            }],
        },
    ];
    for (i, cmd) in commands.into_iter().enumerate() {
        let entry = openraft::Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), i as u64 + 10),
            payload: openraft::EntryPayload::Normal(cmd),
        };
        assert!(!store.apply_to_state_machine(&[entry]).await.unwrap()[0].success);
        assert_eq!(
            kv.pending_intent_keys(legacy),
            vec![Bytes::from_static(b"legacy")]
        );
        assert_eq!(kv.pending_intent_keys(TxnId(txn)).len(), 2);
        assert!(kv.get(b"a", u64::MAX).unwrap().is_none());
    }
    assert!(validate_legacy_repair(kv.as_ref(), TxnId(txn), b"a").is_err());
}

#[tokio::test]
async fn recorded_id_cleanup_on_raw_meta_cannot_abort_another_groups_tracked_intents() {
    let raw = Arc::new(MemKvEngine::new());
    let data = nodus_storage_api::NamespacedKvEngine::new(raw.clone(), "shard-a");
    let txn = Uuid::new_v4();
    prepare(&data, txn);
    let mut meta = NodusRaftStore::with_kv(raw)
        .unwrap()
        .with_snapshot_group("shard-meta");
    let cmd = ShardCommand::AbortTxn {
        txn_id: data.map_txn_id(TxnId(txn)).0.to_string(),
        shard_id: None,
        recorded: true,
    };
    let entry = openraft::Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 10),
        payload: openraft::EntryPayload::Normal(cmd),
    };
    assert!(!meta.apply_to_state_machine(&[entry]).await.unwrap()[0].success);
    assert_eq!(data.pending_intent_keys(TxnId(txn)).len(), 2);
}

#[test]
fn invalid_and_future_records_and_wrong_decision_bindings_fail_closed() {
    let kv = MemKvEngine::new();
    let txn = Uuid::new_v4();
    prepare(&kv, txn);
    for bytes in [
        b"{}".to_vec(),
        nodus_common::versioned::encode(99, b"{}"),
        nodus_common::versioned::encode(1, b"{}"),
        vec![0; MAX_RECORD_BYTES + 1],
    ] {
        assert!(decode(&bytes).is_err());
    }
    for defect in 0..5 {
        let mut d = decision(txn, true);
        match defect {
            0 => d.participants.clear(),
            1 => {
                d.participants.insert("shard-a".into(), 1);
            }
            2 => d.txn_id = Uuid::new_v4(),
            3 => d.phase = DecisionPhase::Preparing,
            _ => d.token.owner = owner(30),
        }
        let mut cmd = resolve(d);
        cmd.txn_id = txn;
        assert!(!apply(&kv, "shard-a", &cmd, 3).unwrap().success);
    }
    let write = TxnId::new();
    kv.write_intent(write, Bytes::from(key(txn)), Bytes::from_static(b"broken"))
        .unwrap();
    kv.commit(write, 10).unwrap();
    assert!(apply(&kv, "shard-a", &writes(txn), 11).is_err());
}
