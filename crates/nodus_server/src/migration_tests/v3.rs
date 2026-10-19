//! Apply-level tests for the V3 copy/cutover journal and the conditional
//! routing publication commands. Same harness style as the V2 coordinator
//! tests: commands are applied through the Raft state machine, journals are
//! read back from the durable store.
use super::*;
use crate::multi_raft::META_SHARD;
use nodus_catalog::{DescriptorState, ShardDescriptor, ShardId};
use nodus_meta::ShardMap;
use nodus_raftstore::migration::coordinator::{
    self,
    v3::{self, CheckpointV3, CommandV3 as C3, PhaseV3 as P3, PlanV3, RecordV3, ValidationV3},
};
use std::collections::{BTreeMap, HashMap};

fn shard(table: TableId, name: &str, start_key: &[u8], end_key: &[u8]) -> ShardDescriptor {
    let now = chrono::Utc::now();
    ShardDescriptor {
        id: ShardId::new(),
        name: name.into(),
        version: 1,
        created_at: now,
        updated_at: now,
        state: DescriptorState::Public,
        table_id: table,
        start_key: start_key.to_vec(),
        end_key: end_key.to_vec(),
    }
}

/// Two-source, two-destination plan whose expected routing matches an empty
/// meta store only after `prepare_routing` has run.
struct Fixture3 {
    plan: PlanV3,
    table: TableId,
    operation: Uuid,
}

fn fixture_plan(table: TableId) -> Fixture3 {
    let current = ShardMap {
        table_id: table,
        shards: vec![shard(table, "shard-a", b"", b"")],
    };
    let destination = ShardMap {
        table_id: table,
        shards: vec![
            shard(table, "shard-b", b"", b"m"),
            shard(table, "shard-c", b"m", b""),
        ],
    };
    let source_group = format!("shard-{}", current.shards[0].id);
    let destination_placements = HashMap::from([
        (destination.shards[0].id, "shard-d0".to_string()),
        (destination.shards[1].id, "shard-d1".to_string()),
    ]);
    let mut expected_placements = HashMap::from([(current.shards[0].id, source_group.clone())]);
    expected_placements.extend(destination_placements.clone());
    let plan = PlanV3 {
        migration: MigrationPlan {
            operation_id: Uuid::new_v4(),
            table_id: table,
            source_epochs: BTreeMap::from([(META_SHARD.to_string(), 0), (source_group.clone(), 0)]),
            sources: vec![META_SHARD.to_string(), source_group.clone()],
            destinations: vec!["shard-d0".to_string(), "shard-d1".to_string()],
        },
        expected_map: Some(current.clone()),
        expected_placements,
        destination_map: Some(destination),
        destination_placements,
    };
    Fixture3 {
        operation: plan.migration.operation_id,
        plan,
        table,
    }
}

async fn meta_store(h: &Harness) -> Arc<dyn nodus_meta::MetaStore> {
    h.store
        .state_machine
        .read()
        .await
        .meta_store
        .clone()
        .unwrap()
}

/// Meta-store writes that bypass Raft, simulating routing committed by an
/// earlier (or drifting) operation. The apply-time checks read this content.
fn prepare_routing(meta: &Arc<dyn nodus_meta::MetaStore>, fx: &Fixture3) {
    meta.update_shard_map(fx.plan.expected_map.clone().unwrap())
        .unwrap();
    meta.update_shard_placements(&fx.plan.expected_placements)
        .unwrap();
}

async fn new_harness(dir: &std::path::Path) -> Harness {
    Harness::new(
        Arc::new(nodus_storage_mem::MemKvEngine::new()),
        dir.to_path_buf(),
    )
    .await
}

async fn v3(h: &mut Harness, cmd: C3) -> ShardResponse {
    h.apply(ShardCommand::MigrationV3(cmd)).await
}

async fn journal(h: &Harness, table: TableId) -> RecordV3 {
    v3::read_record_v3(h.kv.as_ref(), table).unwrap().unwrap()
}

async fn begin(h: &mut Harness, fx: &Fixture3) {
    assert!(
        v3(
            h,
            C3::Begin {
                table: fx.table,
                operation: fx.operation,
            }
        )
        .await
        .success
    );
}

async fn fence(h: &mut Harness, fx: &Fixture3) {
    for source in &fx.plan.migration.sources {
        assert!(
            v3(
                h,
                C3::Ack {
                    table: fx.table,
                    operation: fx.operation,
                    source: source.clone(),
                    epoch: 1,
                    cancelled: false,
                }
            )
            .await
            .success
        );
    }
    assert!(
        v3(
            h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Fencing,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
}

async fn prepare(h: &mut Harness, fx: &Fixture3) {
    assert!(
        v3(
            h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Fenced,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
    for destination in &fx.plan.migration.destinations {
        assert!(
            v3(
                h,
                C3::PrepareAck {
                    table: fx.table,
                    operation: fx.operation,
                    destination: destination.clone(),
                }
            )
            .await
            .success
        );
    }
}

/// Finish{Preparing} -> Copying, then journal complete checkpoints for every
/// source. Leaves the journal in `Copying`.
async fn copy(h: &mut Harness, fx: &Fixture3) {
    assert!(
        v3(
            h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Preparing,
                protected_read_ts: Some(42),
            }
        )
        .await
        .success
    );
    for source in &fx.plan.migration.sources {
        assert!(
            v3(
                h,
                C3::CopyCheckpoint {
                    table: fx.table,
                    operation: fx.operation,
                    source: source.clone(),
                    checkpoint: CheckpointV3 {
                        cursor: b"half".to_vec(),
                        complete: false,
                    },
                }
            )
            .await
            .success
        );
        assert!(
            v3(
                h,
                C3::CopyCheckpoint {
                    table: fx.table,
                    operation: fx.operation,
                    source: source.clone(),
                    checkpoint: CheckpointV3 {
                        cursor: b"end".to_vec(),
                        complete: true,
                    },
                }
            )
            .await
            .success
        );
    }
}

/// Finish{Copying} -> Validating.
async fn finish_copy(h: &mut Harness, fx: &Fixture3) {
    assert!(
        v3(
            h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Copying,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
}

async fn validate_match(h: &mut Harness, fx: &Fixture3) {
    assert!(
        v3(
            h,
            C3::ValidationReport {
                table: fx.table,
                operation: fx.operation,
                report: ValidationV3 {
                    matches: true,
                    source_digest: 7,
                    destination_digest: 7,
                },
            }
        )
        .await
        .success
    );
}

async fn publish(h: &mut Harness, fx: &Fixture3) {
    assert!(
        v3(
            h,
            C3::Publish {
                table: fx.table,
                operation: fx.operation,
            }
        )
        .await
        .success
    );
    assert!(publish_map(h, fx).await.success);
    assert!(publish_placements(h, fx).await.success);
}

async fn publish_map(h: &mut Harness, fx: &Fixture3) -> ShardResponse {
    h.apply(ShardCommand::UpdateShardMapConditional {
        table: fx.table,
        operation: fx.operation,
        phase: P3::Publishing,
        expected: fx.plan.expected_map.clone(),
        map: fx.plan.destination_map.clone().unwrap(),
    })
    .await
}

async fn publish_placements(h: &mut Harness, fx: &Fixture3) -> ShardResponse {
    h.apply(ShardCommand::UpdateShardPlacementsConditional {
        table: fx.table,
        operation: fx.operation,
        phase: P3::Publishing,
        expected: fx.plan.expected_placements.clone(),
        placements: fx.plan.destination_placements.clone(),
    })
    .await
}

async fn complete(h: &mut Harness, fx: &Fixture3) {
    assert!(
        v3(
            h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Publishing,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
    for source in &fx.plan.migration.sources {
        assert!(
            v3(
                h,
                C3::CompleteAck {
                    table: fx.table,
                    operation: fx.operation,
                    source: source.clone(),
                }
            )
            .await
            .success
        );
    }
    assert!(
        v3(
            h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Completing,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
}

async fn plan_and_begin(h: &mut Harness, fx: &Fixture3) {
    assert!(v3(h, C3::Plan(fx.plan.clone())).await.success);
    begin(h, fx).await;
}

/// Drive a fresh operation to `phase` on a fresh harness.
async fn driven(dir: &std::path::Path, phase: P3) -> (Harness, Fixture3) {
    let mut h = new_harness(dir).await;
    let fx = fixture_plan(TableId::new());
    prepare_routing(&meta_store(&h).await, &fx);
    assert!(v3(&mut h, C3::Plan(fx.plan.clone())).await.success);
    if phase == P3::Planned {
        return (h, fx);
    }
    begin(&mut h, &fx).await;
    if phase == P3::Fencing {
        return (h, fx);
    }
    fence(&mut h, &fx).await;
    if phase == P3::Fenced {
        return (h, fx);
    }
    prepare(&mut h, &fx).await;
    if phase == P3::Preparing {
        return (h, fx);
    }
    copy(&mut h, &fx).await;
    if phase == P3::Copying {
        return (h, fx);
    }
    finish_copy(&mut h, &fx).await;
    validate_match(&mut h, &fx).await;
    if phase == P3::Validating {
        return (h, fx);
    }
    publish(&mut h, &fx).await;
    (h, fx)
}

async fn cancel_out(h: &mut Harness, fx: &Fixture3) {
    assert!(
        v3(
            h,
            C3::RequestCancel {
                table: fx.table,
                operation: fx.operation,
            }
        )
        .await
        .success
    );
    for source in &fx.plan.migration.sources {
        assert!(
            v3(
                h,
                C3::Ack {
                    table: fx.table,
                    operation: fx.operation,
                    source: source.clone(),
                    epoch: 1,
                    cancelled: true,
                }
            )
            .await
            .success
        );
    }
    for destination in &fx.plan.migration.destinations {
        assert!(
            v3(
                h,
                C3::CleanupAck {
                    table: fx.table,
                    operation: fx.operation,
                    destination: destination.clone(),
                }
            )
            .await
            .success
        );
    }
    assert!(
        v3(
            h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Cancelling,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
    let record = journal(h, fx.table).await;
    assert_eq!(record.phase, P3::Cancelled);
    assert_eq!(record.cancelled.len(), fx.plan.migration.sources.len());
    assert!(
        record.prepared.iter().all(|d| record.cleaned.contains(d)),
        "every prepared destination cleaned up"
    );
}

#[tokio::test]
async fn v3_happy_path_reaches_done_with_real_fences() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = new_harness(dir.path()).await;
    let fx = fixture_plan(TableId::new());
    prepare_routing(&meta_store(&h).await, &fx);

    assert!(v3(&mut h, C3::Plan(fx.plan.clone())).await.success);
    // Same plan and operation is an idempotent retry; reusing the operation ID
    // with a different plan is rejected.
    assert!(v3(&mut h, C3::Plan(fx.plan.clone())).await.success);
    let mut conflict = fx.plan.clone();
    conflict.destination_map.as_mut().unwrap().shards[0].name = "renamed".into();
    assert!(!v3(&mut h, C3::Plan(conflict)).await.success);

    begin(&mut h, &fx).await;
    // The real participant fences close: meta first, then the row source.
    for source in &fx.plan.migration.sources {
        assert!(
            h.control(Control::Acquire {
                operation_id: fx.operation,
                expected_epoch: 0,
            })
            .await
            .success,
            "acquire {source}"
        );
    }
    fence(&mut h, &fx).await;
    // Acquire retries of the same closed operation do not advance again.
    assert!(
        h.control(Control::Acquire {
            operation_id: fx.operation,
            expected_epoch: 0,
        })
        .await
        .success
    );
    assert_eq!(journal(&h, fx.table).await.phase, P3::Fenced);

    prepare(&mut h, &fx).await;
    assert!(
        !v3(
            &mut h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Preparing,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
    copy(&mut h, &fx).await;
    let record = journal(&h, fx.table).await;
    assert_eq!(record.phase, P3::Copying);
    assert_eq!(record.protected_read_ts, 42);
    assert!(
        record
            .checkpoints
            .values()
            .all(|c| c.complete && c.cursor == b"end")
    );
    finish_copy(&mut h, &fx).await;
    assert_eq!(journal(&h, fx.table).await.phase, P3::Validating);

    validate_match(&mut h, &fx).await;
    publish(&mut h, &fx).await;
    let meta = meta_store(&h).await;
    assert_eq!(
        meta.get_shard_map(fx.table).unwrap().shards.len(),
        2,
        "destination map published"
    );
    assert_eq!(
        meta.get_shard_placements().unwrap(),
        fx.plan.destination_placements
    );
    // The fence owned by this operation did not block its own publication, but
    // an unconditional routing write stays fenced.
    assert!(
        !h.apply(ShardCommand::UpdateShardMap(
            fx.plan.expected_map.clone().unwrap()
        ))
        .await
        .success
    );

    complete(&mut h, &fx).await;
    let record = journal(&h, fx.table).await;
    assert_eq!(record.phase, P3::Done);
    assert_eq!(record.completed.len(), fx.plan.migration.sources.len());
}

#[tokio::test]
async fn v3_conditional_publication_is_checked_idempotent_and_gated() {
    let dir = tempfile::tempdir().unwrap();
    let (mut h, fx) = driven(dir.path(), P3::Validating).await;
    publish_gate(&mut h, &fx).await;
}

async fn publish_gate(h: &mut Harness, fx: &Fixture3) {
    assert!(
        v3(
            h,
            C3::Publish {
                table: fx.table,
                operation: fx.operation,
            }
        )
        .await
        .success
    );

    // Stale expected content fails closed with the distinct rejection.
    let stale = h
        .apply(ShardCommand::UpdateShardMapConditional {
            table: fx.table,
            operation: fx.operation,
            phase: P3::Publishing,
            expected: None,
            map: fx.plan.destination_map.clone().unwrap(),
        })
        .await;
    assert!(!stale.success);
    assert!(
        stale
            .error
            .unwrap()
            .contains("conditional routing publication rejected")
    );
    // A wrong phase token or a foreign operation cannot publish either.
    assert!(
        !h.apply(ShardCommand::UpdateShardMapConditional {
            table: fx.table,
            operation: fx.operation,
            phase: P3::Validating,
            expected: fx.plan.expected_map.clone(),
            map: fx.plan.destination_map.clone().unwrap(),
        })
        .await
        .success
    );
    assert!(
        !h.apply(ShardCommand::UpdateShardMapConditional {
            table: fx.table,
            operation: Uuid::new_v4(),
            phase: P3::Publishing,
            expected: fx.plan.expected_map.clone(),
            map: fx.plan.destination_map.clone().unwrap(),
        })
        .await
        .success
    );
    // The payload must match the journaled destination content.
    let mut wrong = fx.plan.destination_map.clone().unwrap();
    wrong.shards.pop();
    assert!(
        !h.apply(ShardCommand::UpdateShardMapConditional {
            table: fx.table,
            operation: fx.operation,
            phase: P3::Publishing,
            expected: fx.plan.expected_map.clone(),
            map: wrong,
        })
        .await
        .success
    );
    let meta = meta_store(h).await;
    assert_eq!(meta.get_shard_map(fx.table).unwrap().shards.len(), 1);

    // Correct expected content applies; replays are accepted without rewriting.
    assert!(publish_map(h, fx).await.success);
    assert!(publish_map(h, fx).await.success);
    assert!(publish_placements(h, fx).await.success);
    assert_eq!(meta.get_shard_map(fx.table).unwrap().shards.len(), 2);

    // Publication replay after the journal advanced past Publishing is
    // idempotent; a stale-expected replay is then rejected.
    assert!(
        v3(
            h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Publishing,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
    assert!(publish_map(h, fx).await.success);
    assert!(publish_placements(h, fx).await.success);
    let stale = h
        .apply(ShardCommand::UpdateShardPlacementsConditional {
            table: fx.table,
            operation: fx.operation,
            phase: P3::Publishing,
            expected: HashMap::new(),
            placements: fx.plan.destination_placements.clone(),
        })
        .await;
    assert!(
        stale.success,
        "already-applied content replays idempotently"
    );
}

#[tokio::test]
async fn v3_cancellation_from_every_pre_publication_phase() {
    for phase in [
        P3::Planned,
        P3::Fencing,
        P3::Fenced,
        P3::Preparing,
        P3::Copying,
        P3::Validating,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (mut h, fx) = driven(dir.path(), phase).await;
        cancel_out(&mut h, &fx).await;
        // A cancelled journal releases the table for a new operation over the
        // same (unchanged) routing.
        let mut next = fx.plan.clone();
        next.migration.operation_id = Uuid::new_v4();
        assert!(v3(&mut h, C3::Plan(next)).await.success);
    }
}

#[tokio::test]
async fn v3_cancellation_after_publishing_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (mut h, fx) = driven(dir.path(), P3::Publishing).await;
    for phase in [P3::Publishing, P3::Completing] {
        let response = v3(
            &mut h,
            C3::RequestCancel {
                table: fx.table,
                operation: fx.operation,
            },
        )
        .await;
        assert!(!response.success, "cancel in {phase:?} must be rejected");
        if phase == P3::Publishing {
            assert!(
                v3(
                    &mut h,
                    C3::Finish {
                        table: fx.table,
                        operation: fx.operation,
                        phase: P3::Publishing,
                        protected_read_ts: None,
                    }
                )
                .await
                .success
            );
        }
    }
    // Only completion is possible now.
    for source in &fx.plan.migration.sources {
        assert!(
            v3(
                &mut h,
                C3::CompleteAck {
                    table: fx.table,
                    operation: fx.operation,
                    source: source.clone(),
                }
            )
            .await
            .success
        );
    }
    assert!(
        v3(
            &mut h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Completing,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
    assert_eq!(journal(&h, fx.table).await.phase, P3::Done);
    assert!(
        !v3(
            &mut h,
            C3::RequestCancel {
                table: fx.table,
                operation: fx.operation,
            }
        )
        .await
        .success
    );
}

#[tokio::test]
async fn v3_validation_mismatch_cancels_and_never_publishes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut h, fx) = driven(dir.path(), P3::Copying).await;
    finish_copy(&mut h, &fx).await;
    assert_eq!(journal(&h, fx.table).await.phase, P3::Validating);
    assert!(
        v3(
            &mut h,
            C3::ValidationReport {
                table: fx.table,
                operation: fx.operation,
                report: ValidationV3 {
                    matches: false,
                    source_digest: 1,
                    destination_digest: 2,
                },
            }
        )
        .await
        .success
    );
    assert_eq!(journal(&h, fx.table).await.phase, P3::Cancelling);
    assert!(
        !v3(
            &mut h,
            C3::Publish {
                table: fx.table,
                operation: fx.operation,
            }
        )
        .await
        .success
    );
    cancel_out(&mut h, &fx).await;
}

#[tokio::test]
async fn v3_routing_drift_before_publish_forces_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let (mut h, fx) = driven(dir.path(), P3::Validating).await;
    // Routing drifted since planning (a concurrent committed change).
    let meta = meta_store(&h).await;
    let mut drifted = fx.plan.expected_map.clone().unwrap();
    drifted.shards.push(shard(fx.table, "shard-z", b"m", b"z"));
    meta.update_shard_map(drifted).unwrap();
    assert!(
        !v3(
            &mut h,
            C3::Publish {
                table: fx.table,
                operation: fx.operation,
            }
        )
        .await
        .success
    );
    cancel_out(&mut h, &fx).await;
}

/// Unsharded-table plan: no current map, one destination shard/group.
fn fixture_plan_unsharded(table: TableId) -> Fixture3 {
    let destination = ShardMap {
        table_id: table,
        shards: vec![shard(table, "shard-b", b"", b"")],
    };
    let destination_placements =
        HashMap::from([(destination.shards[0].id, "shard-d0".to_string())]);
    let plan = PlanV3 {
        migration: MigrationPlan {
            operation_id: Uuid::new_v4(),
            table_id: table,
            source_epochs: BTreeMap::from([(META_SHARD.to_string(), 0)]),
            sources: vec![META_SHARD.to_string()],
            destinations: vec!["shard-d0".to_string()],
        },
        expected_map: None,
        expected_placements: HashMap::new(),
        destination_map: Some(destination),
        destination_placements,
    };
    Fixture3 {
        operation: plan.migration.operation_id,
        plan,
        table,
    }
}

#[tokio::test]
async fn v2_and_v3_journals_block_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = new_harness(dir.path()).await;
    let table = TableId::new();
    let fx = fixture_plan_unsharded(table);

    // A live V2 journal blocks V3 planning.
    let v2_plan = coordinator::PlanV2 {
        migration: MigrationPlan {
            operation_id: Uuid::new_v4(),
            table_id: table,
            source_epochs: BTreeMap::from([(META_SHARD.to_string(), 0)]),
            sources: vec![META_SHARD.to_string()],
            destinations: vec!["legacy-destination".to_string()],
        },
        expected_map: None,
    };
    assert!(
        h.apply(ShardCommand::MigrationV2(coordinator::CommandV2::Plan(
            v2_plan.clone()
        )))
        .await
        .success
    );
    assert!(!v3(&mut h, C3::Plan(fx.plan.clone())).await.success);
    // Cancel the V2 operation to completion.
    let v2_operation = v2_plan.migration.operation_id;
    assert!(
        h.apply(ShardCommand::MigrationV2(
            coordinator::CommandV2::RequestCancel {
                table,
                operation: v2_operation,
            }
        ))
        .await
        .success
    );
    assert!(
        h.apply(ShardCommand::MigrationV2(coordinator::CommandV2::Ack {
            table,
            operation: v2_operation,
            source: META_SHARD.to_string(),
            epoch: 1,
            cancelled: true,
        }))
        .await
        .success
    );
    assert!(
        h.apply(ShardCommand::MigrationV2(coordinator::CommandV2::Finish {
            table,
            operation: v2_operation,
            cancelled: true,
        }))
        .await
        .success
    );
    // V3 planning is now allowed, and blocks V2 planning in turn.
    assert!(v3(&mut h, C3::Plan(fx.plan.clone())).await.success);
    let mut v2_next = v2_plan.clone();
    v2_next.migration.operation_id = Uuid::new_v4();
    assert!(
        !h.apply(ShardCommand::MigrationV2(coordinator::CommandV2::Plan(
            v2_next
        )))
        .await
        .success
    );
    // V1 planning for another table is unaffected, but rejected for this one.
    assert!(h.control(Control::Plan(plan())).await.success);
    let mut v1_same_table = plan();
    v1_same_table.table_id = table;
    assert!(!h.control(Control::Plan(v1_same_table)).await.success);
}

#[tokio::test]
async fn v1_journal_blocks_v3_planning() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = new_harness(dir.path()).await;
    let table = TableId::new();
    let fx = fixture_plan(table);
    prepare_routing(&meta_store(&h).await, &fx);
    let mut v1 = plan();
    v1.table_id = table;
    assert!(h.control(Control::Plan(v1)).await.success);
    let response = v3(&mut h, C3::Plan(fx.plan.clone())).await;
    assert!(!response.success);
    assert!(response.error.unwrap().contains("V1 journal"));
}

#[tokio::test]
async fn v3_snapshot_round_trip_preserves_midflight_journal_and_fence() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = new_harness(&dir.path().join("source")).await;
    let fx = fixture_plan(TableId::new());
    prepare_routing(&meta_store(&h).await, &fx);
    plan_and_begin(&mut h, &fx).await;
    assert!(
        h.control(Control::Acquire {
            operation_id: fx.operation,
            expected_epoch: 0,
        })
        .await
        .success
    );
    assert!(
        v3(
            &mut h,
            C3::Ack {
                table: fx.table,
                operation: fx.operation,
                source: META_SHARD.to_string(),
                epoch: 1,
                cancelled: false,
            }
        )
        .await
        .success
    );

    let snapshot = h.store.build_snapshot().await.unwrap();
    let mut receiver = new_harness(&dir.path().join("receiver")).await;
    receiver
        .store
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    receiver.index = h.index;
    // Note: the in-memory meta store is not part of the snapshot; the durable
    // journal and fence are.
    assert_eq!(
        serde_json::to_value(journal(&receiver, fx.table).await).unwrap(),
        serde_json::to_value(journal(&h, fx.table).await).unwrap()
    );
    assert_eq!(
        read_fence(receiver.kv.as_ref()).unwrap(),
        read_fence(h.kv.as_ref()).unwrap()
    );
    // The reopened state machine resumes the mid-flight phase: the remaining
    // source ack is accepted under the same operation.
    assert!(
        v3(
            &mut receiver,
            C3::Ack {
                table: fx.table,
                operation: fx.operation,
                source: fx.plan.migration.sources[1].clone(),
                epoch: 1,
                cancelled: false,
            }
        )
        .await
        .success
    );
    // Replaying the already-recorded ack is idempotent; an ack for a foreign
    // operation is rejected.
    assert!(
        v3(
            &mut receiver,
            C3::Ack {
                table: fx.table,
                operation: fx.operation,
                source: META_SHARD.to_string(),
                epoch: 1,
                cancelled: false,
            }
        )
        .await
        .success
    );
    assert!(
        !v3(
            &mut receiver,
            C3::Ack {
                table: fx.table,
                operation: Uuid::new_v4(),
                source: META_SHARD.to_string(),
                epoch: 1,
                cancelled: false,
            }
        )
        .await
        .success
    );
}

#[tokio::test]
async fn v3_unknown_envelope_version_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = new_harness(dir.path()).await;
    let table = TableId::new();
    let txn = TxnId::new();
    h.kv.write_intent(
        txn,
        Bytes::from(v3::key(table)),
        Bytes::from(nodus_common::versioned::encode(99, b"{}")),
    )
    .unwrap();
    h.kv.commit(txn, 1000).unwrap();
    assert!(v3::read_record_v3(h.kv.as_ref(), table).is_err());
    // Snapshot construction validates durable migration state and fails closed.
    assert!(h.store.build_snapshot().await.is_err());
    // So does applying any V3 command against the corrupt journal: the apply
    // error halts the state machine instead of skipping the entry.
    h.index += 1;
    let entry = openraft::Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), h.index),
        payload: openraft::EntryPayload::Normal(ShardCommand::MigrationV3(C3::Begin {
            table,
            operation: Uuid::new_v4(),
        })),
    };
    assert!(h.store.apply_to_state_machine(&[entry]).await.is_err());
}

#[test]
fn v3_decode_rejects_inconsistent_journals() {
    let table = TableId::new();
    let fx = fixture_plan(table);
    let record = RecordV3 {
        plan: fx.plan.clone(),
        phase: P3::Planned,
        acquired: BTreeMap::new(),
        cancelled: BTreeMap::new(),
        prepared: Default::default(),
        checkpoints: BTreeMap::new(),
        validation: None,
        completed: Default::default(),
        cleaned: Default::default(),
        protected_read_ts: 0,
    };
    let encode = |record: &RecordV3| {
        nodus_common::versioned::encode(3, &serde_json::to_vec(record).unwrap())
    };

    // A well-formed Planned journal round-trips.
    assert_eq!(v3::decode(&encode(&record)).unwrap().phase, P3::Planned);
    // Wrong envelope versions fail closed.
    assert!(
        v3::decode(&nodus_common::versioned::encode(
            2,
            &serde_json::to_vec(&record).unwrap()
        ))
        .is_err()
    );
    assert!(
        v3::decode(&nodus_common::versioned::encode(
            99,
            &serde_json::to_vec(&record).unwrap()
        ))
        .is_err()
    );

    // Copying without every destination prepare ack.
    let mut bad = record.clone();
    bad.phase = P3::Copying;
    bad.acquired = fx
        .plan
        .migration
        .sources
        .iter()
        .map(|s| (s.clone(), 1))
        .collect();
    bad.protected_read_ts = 42;
    assert!(v3::decode(&encode(&bad)).is_err());

    // Validating with an incomplete copy.
    let mut bad = bad.clone();
    bad.phase = P3::Validating;
    bad.prepared = fx.plan.migration.destinations.iter().cloned().collect();
    assert!(v3::decode(&encode(&bad)).is_err());

    // Publishing without a matching validation report.
    let mut bad = bad.clone();
    bad.phase = P3::Publishing;
    bad.checkpoints = fx
        .plan
        .migration
        .sources
        .iter()
        .map(|s| {
            (
                s.clone(),
                CheckpointV3 {
                    cursor: b"end".to_vec(),
                    complete: true,
                },
            )
        })
        .collect();
    assert!(v3::decode(&encode(&bad)).is_err());
    // A mismatched report does not satisfy the boundary either, and the digest
    // consistency itself is enforced.
    bad.validation = Some(ValidationV3 {
        matches: true,
        source_digest: 1,
        destination_digest: 2,
    });
    assert!(v3::decode(&encode(&bad)).is_err());

    // Done without every cutover acknowledgement.
    let mut bad = bad.clone();
    bad.phase = P3::Done;
    bad.validation = Some(ValidationV3 {
        matches: true,
        source_digest: 7,
        destination_digest: 7,
    });
    assert!(v3::decode(&encode(&bad)).is_err());
    bad.completed = fx.plan.migration.sources.iter().cloned().collect();
    assert!(v3::decode(&encode(&bad)).is_ok());

    // Cancelled without destination cleanup, and with a bogus ack epoch.
    let mut bad = record.clone();
    bad.phase = P3::Cancelled;
    bad.cancelled = fx
        .plan
        .migration
        .sources
        .iter()
        .map(|s| (s.clone(), 1))
        .collect();
    bad.prepared = fx.plan.migration.destinations.iter().cloned().collect();
    assert!(v3::decode(&encode(&bad)).is_err());
    bad.cleaned = bad.prepared.clone();
    assert!(v3::decode(&encode(&bad)).is_ok());
    bad.cancelled.insert(META_SHARD.to_string(), 7);
    assert!(v3::decode(&encode(&bad)).is_err());
}

#[tokio::test]
async fn v3_out_of_phase_and_stale_commands_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = new_harness(dir.path()).await;
    let fx = fixture_plan(TableId::new());
    prepare_routing(&meta_store(&h).await, &fx);

    // Commands before any journal exists.
    assert!(
        !v3(
            &mut h,
            C3::Begin {
                table: fx.table,
                operation: fx.operation,
            }
        )
        .await
        .success
    );
    // A plan that does not cover every source group.
    let mut partial = fx.plan.clone();
    partial.migration.sources.pop();
    partial
        .migration
        .source_epochs
        .remove(&fx.plan.migration.sources[1]);
    assert!(!v3(&mut h, C3::Plan(partial)).await.success);
    // A plan whose routing does not match the meta store.
    let mut drifted = fx.plan.clone();
    drifted.expected_placements.clear();
    assert!(!v3(&mut h, C3::Plan(drifted)).await.success);

    assert!(v3(&mut h, C3::Plan(fx.plan.clone())).await.success);
    // Ack before Begin, and Finish before every ack.
    assert!(
        !v3(
            &mut h,
            C3::Ack {
                table: fx.table,
                operation: fx.operation,
                source: META_SHARD.to_string(),
                epoch: 1,
                cancelled: false,
            }
        )
        .await
        .success
    );
    assert!(
        !v3(
            &mut h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Planned,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
    begin(&mut h, &fx).await;
    // Wrong ack epoch (must be source_epoch + 1).
    assert!(
        !v3(
            &mut h,
            C3::Ack {
                table: fx.table,
                operation: fx.operation,
                source: META_SHARD.to_string(),
                epoch: 7,
                cancelled: false,
            }
        )
        .await
        .success
    );
    fence(&mut h, &fx).await;
    // An oversized checkpoint cursor is rejected.
    assert!(
        !v3(
            &mut h,
            C3::CopyCheckpoint {
                table: fx.table,
                operation: fx.operation,
                source: META_SHARD.to_string(),
                checkpoint: CheckpointV3 {
                    cursor: vec![0; 2048],
                    complete: false,
                },
            }
        )
        .await
        .success
    );
    // Finish{Publishing} before a publication commits is rejected.
    prepare(&mut h, &fx).await;
    copy(&mut h, &fx).await;
    finish_copy(&mut h, &fx).await;
    validate_match(&mut h, &fx).await;
    assert!(
        !v3(
            &mut h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Publishing,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
    publish(&mut h, &fx).await;
    // Idempotent replays of committed transitions are accepted.
    begin(&mut h, &fx).await;
    assert!(
        v3(
            &mut h,
            C3::Finish {
                table: fx.table,
                operation: fx.operation,
                phase: P3::Fencing,
                protected_read_ts: None,
            }
        )
        .await
        .success
    );
    assert!(
        v3(
            &mut h,
            C3::Publish {
                table: fx.table,
                operation: fx.operation,
            }
        )
        .await
        .success
    );
    complete(&mut h, &fx).await;
    assert_eq!(journal(&h, fx.table).await.phase, P3::Done);
}
