//! Bounded, restartable pre-copy coordinator. Each call reloads the committed
//! journal and advances at most one participant. Transport timeouts are uncertain
//! outcomes: the next call retries the same idempotent operation.
use super::*;
use std::{future::Future, time::Duration};

/// Implementations must load after a meta-leader ReadIndex barrier. Submission
/// must return only after quorum commit and application, checking success votes.
pub trait Backend: Sync {
    fn load(&self, table: TableId) -> impl Future<Output = Result<RecordV2>> + Send;
    fn submit(&self, group: &str, command: ShardCommand)
    -> impl Future<Output = Result<()>> + Send;
    fn routing_matches(&self, plan: &PlanV2) -> impl Future<Output = Result<bool>> + Send;
}

/// No discovery loop or unbounded retries: the owner schedules another tick on
/// transient failure. A deadline includes load, participant RPC, and journal ack.
pub async fn resume(backend: &impl Backend, table: TableId, deadline: Duration) -> Result<PhaseV2> {
    tokio::time::timeout(deadline, step(backend, table))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "migration coordinator step timed out; outcome uncertain, resume from journal"
            )
        })?
}

async fn step(backend: &impl Backend, table: TableId) -> Result<PhaseV2> {
    let record = backend.load(table).await?;
    let operation = record.plan.migration.operation_id;
    let phase = record.phase.clone();
    let command = match phase {
        PhaseV2::Cancelled => return Ok(phase),
        PhaseV2::Planned | PhaseV2::Fencing | PhaseV2::Fenced
            if !backend.routing_matches(&record.plan).await? =>
        {
            CommandV2::RequestCancel { table, operation }
        }
        PhaseV2::Planned => CommandV2::Begin { table, operation },
        PhaseV2::Fenced => return Ok(phase),
        PhaseV2::Fencing | PhaseV2::Cancelling => {
            let cancelled = phase == PhaseV2::Cancelling;
            let acks = if cancelled {
                &record.cancelled
            } else {
                &record.acquired
            };
            if let Some(source) = record
                .plan
                .migration
                .sources
                .iter()
                .find(|s| !acks.contains_key(*s))
            {
                let expected_epoch = record.plan.migration.source_epochs[source];
                let cmd = if cancelled {
                    ShardCommand::MigrationV2(CommandV2::CancelParticipant {
                        operation,
                        expected_epoch,
                    })
                } else {
                    ShardCommand::MigrationV1(MigrationCommandV1::Acquire {
                        operation_id: operation,
                        expected_epoch,
                    })
                };
                backend.submit(source, cmd).await?;
                CommandV2::Ack {
                    table,
                    operation,
                    source: source.clone(),
                    epoch: expected_epoch
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("migration epoch exhausted"))?,
                    cancelled,
                }
            } else {
                CommandV2::Finish {
                    table,
                    operation,
                    cancelled,
                }
            }
        }
    };
    backend
        .submit("shard-meta", ShardCommand::MigrationV2(command))
        .await?;
    Ok(backend.load(table).await?.phase)
}
