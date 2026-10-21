//! Server-side driving of the cluster backup job journal
//! (`nodus_raftstore::backup`): the admin handler plans and completes jobs
//! through the meta group, and the meta leader sweeps abandoned jobs to
//! `Failed` once their heartbeat proves the creator died. A job that never
//! completes is never restorable — the manifest commit point stays the final
//! arbiter; the journal adds cluster-wide visibility (migration exclusion, GC
//! pin, crash-safe abandonment).
use crate::{multi_raft::META_SHARD, multi_raft::MultiRaftManager, raft_router::RaftRouter};
use anyhow::{Result, anyhow, ensure};
use nodus_raftstore::{
    ShardCommand,
    backup::{self, CommandV1, JobV1, PhaseV1},
};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// A job whose heartbeat is older than this is abandoned: its creator died
/// mid-export. Coarse on purpose — heartbeats run on a 10 s tick and the bound
/// must dwarf any clock skew between the exporter and the meta leader.
pub(crate) const JOB_STALE_AFTER: Duration = Duration::from_secs(600);

/// Submits a journal command through the meta group on a blocking thread.
async fn submit(router: &RaftRouter, command: CommandV1) -> Result<()> {
    let router = router.clone();
    tokio::task::spawn_blocking(move || {
        router.submit(META_SHARD, ShardCommand::BackupJobV1(command))
    })
    .await
    .map_err(|e| anyhow!("backup job submit task failed: {e}"))?
}

/// Whether this node may write backup journal commands (durable level ≥ 3).
fn journal_enabled(router: &RaftRouter) -> bool {
    router.migration_enabled()
}

/// Opens a journaled backup job. Fails (nothing journaled) when another job
/// or a migration is in flight. No-op below the migration cluster level — the
/// caller takes the legacy node-local path there.
pub(crate) async fn plan_job(router: &RaftRouter, job: JobV1) -> Result<()> {
    ensure!(
        journal_enabled(router),
        "backup journal requires the finalized migration level"
    );
    submit(router, CommandV1::Plan(Box::new(job))).await
}

pub(crate) async fn heartbeat_job(router: &RaftRouter, job: Uuid, at_ms: u64) -> Result<()> {
    submit(router, CommandV1::Heartbeat { job, at_ms }).await
}

/// Marks the job `Completed`. Call only after the manifest's own commit point
/// (`manifest.json` written and verified).
pub(crate) async fn complete_job(router: &RaftRouter, job: Uuid) -> Result<()> {
    submit(router, CommandV1::Complete { job }).await
}

pub(crate) async fn fail_job(router: &RaftRouter, job: Uuid, reason: String) -> Result<()> {
    submit(router, CommandV1::Fail { job, reason }).await
}

/// Whether any backup job is currently exporting (for migration planning
/// refusal at the handler level; the apply path enforces it regardless).
pub(crate) async fn active_job(manager: &Arc<MultiRaftManager>) -> Result<Option<Uuid>> {
    let raft = manager
        .get(META_SHARD)
        .await
        .ok_or_else(|| anyhow!("meta group unavailable"))?;
    raft.ensure_linearizable().await?;
    let machine = manager.machine(META_SHARD).await?;
    let kv = machine
        .read()
        .await
        .kv
        .clone()
        .ok_or_else(|| anyhow!("meta storage unavailable"))?;
    Ok(backup::list_jobs(kv.as_ref())?
        .into_iter()
        .find(|j| j.phase == PhaseV1::Exporting)
        .map(|j| j.job_id))
}

/// One sweep: fail every exporting job whose heartbeat is stale. Returns the
/// number failed. Errors are the caller's to log; the next tick retries.
pub(crate) async fn sweep_stale_jobs_once(
    manager: &Arc<MultiRaftManager>,
    router: &RaftRouter,
    now_ms: u64,
    stale_after: Duration,
) -> Result<usize> {
    let raft = manager
        .get(META_SHARD)
        .await
        .ok_or_else(|| anyhow!("meta group unavailable"))?;
    // Only the current meta leader sweeps (ReadIndex barrier).
    raft.ensure_linearizable().await?;
    let machine = manager.machine(META_SHARD).await?;
    let kv = machine
        .read()
        .await
        .kv
        .clone()
        .ok_or_else(|| anyhow!("meta storage unavailable"))?;
    let mut failed = 0;
    for job in backup::list_jobs(kv.as_ref())? {
        if job.phase != PhaseV1::Exporting {
            continue;
        }
        let age = now_ms.saturating_sub(job.updated_at_ms);
        if age > stale_after.as_millis() as u64 {
            fail_job(
                router,
                job.job_id,
                format!(
                    "exporter heartbeat stale for {} ms; creator presumed dead",
                    age
                ),
            )
            .await?;
            failed += 1;
        }
    }
    Ok(failed)
}

/// The meta-leader sweep loop, wired beside the migration journal discovery
/// worker. Inert until the durable migration level finalizes (the router gate
/// rejects the Fail commands anyway).
pub async fn backup_job_sweep_loop(
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
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        match sweep_stale_jobs_once(&manager, &router, now_ms, JOB_STALE_AFTER).await {
            Ok(0) => {}
            Ok(n) => tracing::warn!("marked {n} abandoned backup job(s) failed"),
            Err(e) => tracing::warn!("backup job sweep failed: {e}"),
        }
    }
}
