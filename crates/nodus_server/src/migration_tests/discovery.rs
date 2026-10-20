//! Meta-leader journal discovery: the production backends drive interrupted
//! V2/V3 journals to their terminal phases, and terminal journals are left
//! alone. Single-node fixtures exercise the real `ProductionBackendV3` and
//! `MigrationCoordinator` against real in-process Raft groups.
use super::driver::{assert_completed, new_mem_fixture, split_plan};
use super::*;
use crate::migration_coordinator::{drive_open_journals_once, v3_backend::ProductionBackendV3};
use crate::multi_raft::META_SHARD;
use nodus_raftstore::migration::{
    MigrationPlan,
    coordinator::{
        CommandV2 as C2, PhaseV2 as P2, PlanV2, read_record_v2,
        v3::{self, CommandV3 as C3, PhaseV3 as P3},
    },
};
use nodus_sharding::ShardOrchestrator;
use std::{collections::BTreeMap, time::Duration};

/// The production backend drives a split to `Done` with the exact multiset
/// moved and routing flipped — the same assertions the fixture-backend driver
/// tests make, but through the real manager/router plumbing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_backend_split_moves_the_exact_multiset() {
    let (_dir, fx) = new_mem_fixture().await;
    let table = TableId::new();
    let orchestrator = ShardOrchestrator::new(fx.meta.clone());
    let source = orchestrator.init_single_shard(table).unwrap();
    orchestrator.move_shard(source, "1").unwrap();
    let source_group = v3::destination_group(source);
    let rows: Vec<(String, String)> = (0..10)
        .map(|i| (format!("k{i:02}"), format!("v{i}")))
        .collect();
    fx.seed(&source_group, table, &rows, 100).await;
    let before = fx.multiset(&source_group, table, u64::MAX);

    let plan = split_plan(&fx, table, source);
    assert!(fx.command(C3::Plan(plan.clone())).await.success);

    let backend = ProductionBackendV3::new(fx.manager.clone(), fx.router.clone());
    assert_eq!(
        v3::driver::run(&backend, table, Duration::from_secs(5), 400)
            .await
            .unwrap(),
        P3::Done
    );
    assert_completed(&fx, table, &plan, &before).await;
    fx.manager.shutdown_all().await;
}

/// An operation interrupted right after `Begin` is found by the discovery
/// pass and driven to `Done` without any further coordinator calls.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_worker_resumes_an_interrupted_v3_operation() {
    let (_dir, fx) = new_mem_fixture().await;
    let table = TableId::new();
    let orchestrator = ShardOrchestrator::new(fx.meta.clone());
    let source = orchestrator.init_single_shard(table).unwrap();
    orchestrator.move_shard(source, "1").unwrap();
    let source_group = v3::destination_group(source);
    let rows: Vec<(String, String)> = (0..10)
        .map(|i| (format!("k{i:02}"), format!("v{i}")))
        .collect();
    fx.seed(&source_group, table, &rows, 100).await;
    let before = fx.multiset(&source_group, table, u64::MAX);

    let plan = split_plan(&fx, table, source);
    assert!(fx.command(C3::Plan(plan.clone())).await.success);

    // The "crash": one journal transition lands, then the coordinator stops.
    let backend = ProductionBackendV3::new(fx.manager.clone(), fx.router.clone());
    assert_eq!(
        v3::driver::resume(&backend, table, Duration::from_secs(5))
            .await
            .unwrap(),
        P3::Fencing
    );

    // The discovery pass drives the rest; one pass is enough for this small
    // operation, but loop to mimic the worker's ticks.
    for _ in 0..5 {
        let open = drive_open_journals_once(&fx.manager, &fx.router)
            .await
            .unwrap();
        let journal = fx.journal(table).await;
        if journal.phase == P3::Done && !fx.manager.hosts(&source_group) {
            break;
        }
        assert_eq!(open, 1, "the interrupted journal stays open until Done");
    }
    assert_eq!(fx.journal(table).await.phase, P3::Done);
    assert!(!fx.manager.hosts(&source_group), "source decommissioned");
    assert_completed(&fx, table, &plan, &before).await;
    // Fully finished: nothing left open.
    assert_eq!(
        drive_open_journals_once(&fx.manager, &fx.router)
            .await
            .unwrap(),
        0
    );
    fx.manager.shutdown_all().await;
}

/// A V2 fencing journal is resumed to its terminal `Fenced` phase, then left
/// alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_worker_resumes_a_v2_journal() {
    let (_dir, fx) = new_mem_fixture().await;
    let table = TableId::new();
    let plan = PlanV2 {
        migration: MigrationPlan {
            table_id: table,
            operation_id: Uuid::new_v4(),
            sources: vec![META_SHARD.into()],
            source_epochs: BTreeMap::from([(META_SHARD.into(), 0)]),
            destinations: vec!["destination".into()],
        },
        expected_map: None,
    };
    // The fixture's `command` helper wraps V3; V2 goes in raw.
    let response = fx
        .manager
        .get(META_SHARD)
        .await
        .unwrap()
        .client_write(ShardCommand::MigrationV2(C2::Plan(plan)))
        .await
        .unwrap()
        .data;
    assert!(response.success);

    for _ in 0..10 {
        drive_open_journals_once(&fx.manager, &fx.router)
            .await
            .unwrap();
        let record = read_record_v2(fx.base.as_ref(), table).unwrap().unwrap();
        if record.phase == P2::Fenced {
            break;
        }
    }
    let record = read_record_v2(fx.base.as_ref(), table).unwrap().unwrap();
    assert_eq!(record.phase, P2::Fenced);
    // The fence is durable on the meta group itself.
    let fence = read_fence(fx.base.as_ref()).unwrap().unwrap();
    assert!(fence.closed);
    // Terminal journals are no longer driven.
    assert_eq!(
        drive_open_journals_once(&fx.manager, &fx.router)
            .await
            .unwrap(),
        0
    );
    fx.manager.shutdown_all().await;
}

/// The rebalance guard: an operation blocks only while its journal can still
/// drive routing changes — V2 until `Fenced`, V3 until `Done`/`Cancelled`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_migration_tracks_journal_terminality() {
    use crate::migration_coordinator::active_migration;

    let (_dir, fx) = new_mem_fixture().await;
    assert_eq!(active_migration(&fx.manager).await.unwrap(), None);

    // V2: Planned blocks, terminal Fenced releases.
    let table = TableId::new();
    let plan = PlanV2 {
        migration: MigrationPlan {
            table_id: table,
            operation_id: Uuid::new_v4(),
            sources: vec![META_SHARD.into()],
            source_epochs: BTreeMap::from([(META_SHARD.into(), 0)]),
            destinations: vec!["destination".into()],
        },
        expected_map: None,
    };
    let response = fx
        .manager
        .get(META_SHARD)
        .await
        .unwrap()
        .client_write(ShardCommand::MigrationV2(C2::Plan(plan)))
        .await
        .unwrap()
        .data;
    assert!(response.success);
    assert_eq!(active_migration(&fx.manager).await.unwrap(), Some(table));
    for _ in 0..10 {
        drive_open_journals_once(&fx.manager, &fx.router)
            .await
            .unwrap();
        if read_record_v2(fx.base.as_ref(), table)
            .unwrap()
            .unwrap()
            .phase
            == P2::Fenced
        {
            break;
        }
    }
    assert_eq!(active_migration(&fx.manager).await.unwrap(), None);
    // V2 has no completion phase: a Fenced journal keeps its fences closed
    // until explicitly cancelled. Cancel it so the meta fence reopens for the
    // V3 operation below (also driving V2 cancellation through the worker).
    let response = fx
        .manager
        .get(META_SHARD)
        .await
        .unwrap()
        .client_write(ShardCommand::MigrationV2(C2::RequestCancel {
            table,
            operation: read_record_v2(fx.base.as_ref(), table)
                .unwrap()
                .unwrap()
                .plan
                .migration
                .operation_id,
        }))
        .await
        .unwrap()
        .data;
    assert!(response.success);
    for _ in 0..10 {
        drive_open_journals_once(&fx.manager, &fx.router)
            .await
            .unwrap();
        if read_record_v2(fx.base.as_ref(), table)
            .unwrap()
            .unwrap()
            .phase
            == P2::Cancelled
        {
            break;
        }
    }
    assert!(!read_fence(fx.base.as_ref()).unwrap().unwrap().closed);

    // V3: Planned blocks, Done releases. The V2 operation above advanced the
    // meta fence epoch, so the plan must name the current one.
    let table = TableId::new();
    let orchestrator = ShardOrchestrator::new(fx.meta.clone());
    let source = orchestrator.init_single_shard(table).unwrap();
    orchestrator.move_shard(source, "1").unwrap();
    fx.host_group(&v3::destination_group(source)).await;
    let mut plan = split_plan(&fx, table, source);
    let meta_epoch = read_fence(fx.base.as_ref()).unwrap().map_or(0, |f| f.epoch);
    plan.migration
        .source_epochs
        .insert(META_SHARD.to_string(), meta_epoch);
    assert!(fx.command(C3::Plan(plan)).await.success);
    assert_eq!(active_migration(&fx.manager).await.unwrap(), Some(table));
    for _ in 0..5 {
        drive_open_journals_once(&fx.manager, &fx.router)
            .await
            .unwrap();
        if fx.journal(table).await.phase == P3::Done {
            break;
        }
    }
    assert_eq!(fx.journal(table).await.phase, P3::Done);
    assert_eq!(active_migration(&fx.manager).await.unwrap(), None);
    fx.manager.shutdown_all().await;
}
