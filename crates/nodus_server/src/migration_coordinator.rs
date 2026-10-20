//! Production coordinator for the recoverable pre-copy (V2) protocol and the
//! home of the meta-leader journal-discovery worker. The worker resumes
//! interrupted V2 and V3 operations after restart or election; the scheduler
//! must call `resume` again after a timeout or leadership change.
use crate::{
    multi_raft::{META_SHARD, MultiRaftManager},
    raft_router::RaftRouter,
};
use anyhow::{Result, anyhow, ensure};
use nodus_catalog::TableId;
use nodus_raftstore::{
    ShardCommand,
    migration::coordinator::{self, PhaseV2, PlanV2, RecordV2, driver::Backend, v3},
};
use std::{sync::Arc, time::Duration};

pub(crate) mod v3_backend;

pub struct MigrationCoordinator {
    manager: Arc<MultiRaftManager>,
    router: RaftRouter,
}

impl MigrationCoordinator {
    pub fn new(manager: Arc<MultiRaftManager>, router: RaftRouter) -> Self {
        Self { manager, router }
    }

    /// Resume a known table from its durable journal. No public start endpoint
    /// exists; the discovery worker drives journaled operations once the
    /// durable migration level is finalized.
    pub async fn resume(&self, table: TableId) -> Result<PhaseV2> {
        ensure!(
            self.router.migration_enabled(),
            "migration protocol activation requires verified cluster-wide compatibility"
        );
        coordinator::driver::resume(self, table, Duration::from_secs(5)).await
    }
}

impl Backend for MigrationCoordinator {
    async fn load(&self, table: TableId) -> Result<RecordV2> {
        let raft = self
            .manager
            .get(META_SHARD)
            .await
            .ok_or_else(|| anyhow!("meta group unavailable"))?;
        // Only the current meta leader coordinates. A lost election after this
        // check remains safe: every transition checks operation/phase at apply.
        raft.ensure_linearizable().await?;
        let machine = self.manager.machine(META_SHARD).await?;
        let sm = machine.read().await;
        let kv = sm
            .kv
            .as_ref()
            .ok_or_else(|| anyhow!("meta storage unavailable"))?;
        coordinator::read_record_v2(kv.as_ref(), table)?
            .ok_or_else(|| anyhow!("migration journal absent"))
    }

    async fn submit(&self, group: &str, command: ShardCommand) -> Result<()> {
        ensure!(
            self.router.migration_enabled(),
            "migration protocol activation requires verified cluster-wide compatibility"
        );
        // Submit through the router: a remote leader is reached by
        // capability-checked mTLS forwarding; a group not hosted on this node
        // is an explicit, retryable failure.
        let router = self.router.clone();
        let group = group.to_string();
        tokio::task::spawn_blocking(move || router.submit(&group, command))
            .await
            .map_err(|e| anyhow!("migration submit task failed: {e}"))?
    }

    async fn routing_matches(&self, plan: &PlanV2) -> Result<bool> {
        let machine = self.manager.machine(META_SHARD).await?;
        let sm = machine.read().await;
        let meta = sm
            .meta_store
            .as_ref()
            .ok_or_else(|| anyhow!("metadata unavailable"))?;
        coordinator::routing_matches(plan, meta.as_ref())
    }
}

// ---- Meta-leader journal discovery ----

/// Max driver steps per journal per discovery pass; a longer operation
/// (e.g. a multi-checkpoint copy) continues on the next tick.
const STEPS_PER_PASS: usize = 64;

/// Scans the meta group's journal prefixes and drives every non-terminal
/// operation by a bounded number of steps. Overlapping runs (a leadership-gain
/// trigger racing the tick, or two nodes both believing they lead meta while a
/// partition heals) are safe: every journal transition revalidates operation,
/// phase, and routing content at apply, and every participant effect is
/// idempotent. Returns the number of open journals found.
pub(crate) async fn drive_open_journals_once(
    manager: &Arc<MultiRaftManager>,
    router: &RaftRouter,
) -> Result<usize> {
    let raft = manager
        .get(META_SHARD)
        .await
        .ok_or_else(|| anyhow!("meta group unavailable"))?;
    // Only the current meta leader drives; the scan is behind a ReadIndex
    // barrier so it never acts on stale journal state.
    raft.ensure_linearizable().await?;
    let machine = manager.machine(META_SHARD).await?;
    let kv = machine.read().await.kv.clone();
    let kv = kv.ok_or_else(|| anyhow!("meta storage unavailable"))?;

    let mut open = 0;
    // V2 journals: pre-copy fencing; terminal at Fenced or Cancelled.
    for row in kv.scan(
        nodus_storage_api::KeyRange {
            start: coordinator::JOURNAL_PREFIX.into(),
            // '/' + 1 == '0': the smallest key past the prefix.
            end: b"\x01migration/v2/table0".to_vec().into(),
        },
        u64::MAX,
    )? {
        let row = row?;
        let record = coordinator::decode(&row.value)?;
        if matches!(record.phase, PhaseV2::Fenced | PhaseV2::Cancelled) {
            continue;
        }
        open += 1;
        let coordinator = MigrationCoordinator::new(manager.clone(), router.clone());
        if let Err(e) = coordinator.resume(record.plan.migration.table_id).await {
            tracing::warn!(
                table = %record.plan.migration.table_id,
                "V2 migration resume failed; retried on the next tick: {e}"
            );
        }
    }
    // V3 journals: full copy/cutover; terminal at Cancelled, or at Done once
    // the source groups are physically gone (the driver decommissions them).
    for row in kv.scan(
        nodus_storage_api::KeyRange {
            start: v3::JOURNAL_PREFIX.into(),
            end: b"\x01migration/v3/table0".to_vec().into(),
        },
        u64::MAX,
    )? {
        let row = row?;
        let record = v3::decode(&row.value)?;
        if record.phase == v3::PhaseV3::Cancelled {
            continue;
        }
        if record.phase == v3::PhaseV3::Done {
            // Post-Done work is exactly the source decommission; a journal
            // whose sources are all gone is complete history.
            let remaining = record
                .plan
                .migration
                .sources
                .iter()
                .any(|s| s != META_SHARD && manager.hosts(s));
            if !remaining {
                continue;
            }
        }
        open += 1;
        let backend = v3_backend::ProductionBackendV3::new(manager.clone(), router.clone());
        if let Err(e) = v3::driver::drive_some(
            &backend,
            record.plan.migration.table_id,
            v3_backend::STEP_DEADLINE,
            STEPS_PER_PASS,
        )
        .await
        {
            tracing::warn!(
                table = %record.plan.migration.table_id,
                "V3 migration drive failed; resumed from the journal on the next tick: {e}"
            );
        }
    }
    Ok(open)
}

/// Bounded journal-discovery worker on the meta leader: finds and resumes
/// interrupted migration operations after a restart or election. There is no
/// start API beyond the admin endpoints; this worker is the only scheduler,
/// and it acts only while this node leads the meta group and the durable
/// migration level is finalized (`router.migration_enabled()`).
pub async fn journal_discovery_loop(
    meta_raft: nodus_raftstore::server::NodusRaft,
    manager: Arc<MultiRaftManager>,
    router: RaftRouter,
    node_id: u64,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<()>,
) {
    let mut metrics = meta_raft.metrics();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut was_leader = false;
    loop {
        tokio::select! {
            res = metrics.changed() => {
                if res.is_err() {
                    break;
                }
                let leader = metrics.borrow().current_leader == Some(node_id);
                let gained = leader && !was_leader;
                was_leader = leader;
                if !gained {
                    continue;
                }
            }
            _ = ticker.tick() => {
                was_leader = metrics.borrow().current_leader == Some(node_id);
                if !was_leader {
                    continue;
                }
            }
            _ = shutdown.changed() => break,
        }
        if !router.migration_enabled() {
            continue;
        }
        match drive_open_journals_once(&manager, &router).await {
            Ok(0) => {}
            Ok(n) => tracing::info!("drove {n} open migration journal(s) as meta leader"),
            Err(e) => tracing::warn!("migration journal discovery failed: {e}"),
        }
    }
}
