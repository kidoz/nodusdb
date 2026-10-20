//! Bounded, restartable V3 copy/cutover driver. Each `resume` call reloads the
//! committed journal (the backend loads behind a meta-leader ReadIndex barrier)
//! and advances at most one bounded step: one participant fence, one
//! destination preparation, one copy batch, one publication, or one journal
//! transition. Timeouts are uncertain outcomes retried from persisted state;
//! every effect is idempotent so a re-driven step never double-applies.
use super::*;
use std::{future::Future, time::Duration};

/// Physical effects the journal driver needs from the hosting service.
/// Implementations must make every method idempotent: a timeout after the
/// effect but before its journal acknowledgement re-drives the same call.
pub trait BackendV3: Sync {
    /// Loads the committed journal after a meta-leader ReadIndex barrier.
    fn load(&self, table: TableId) -> impl Future<Output = Result<RecordV3>> + Send;
    /// Submit a journal command to the meta group; resolves after quorum
    /// commit and application, erroring when the apply vote rejects.
    fn submit_journal(&self, command: CommandV3) -> impl Future<Output = Result<()>> + Send;
    /// Submit a conditional routing publication to the meta group.
    fn submit_conditional(&self, command: ShardCommand) -> impl Future<Output = Result<()>> + Send;
    /// Current routing content as (map matches expected, placements matches
    /// expected, placements match the prepare-step intermediate).
    fn routing_state(&self, plan: &PlanV3) -> impl Future<Output = Result<RoutingState>> + Send;
    /// Acquire the migration fence on one source at `expected_epoch`.
    fn fence_source(
        &self,
        source: &str,
        operation: Uuid,
        expected_epoch: u64,
    ) -> impl Future<Output = Result<()>> + Send;
    /// Seal one source for cancellation (shared `CancelParticipant` semantics).
    fn seal_source(
        &self,
        source: &str,
        operation: Uuid,
        expected_epoch: u64,
    ) -> impl Future<Output = Result<()>> + Send;
    /// Reopen one fenced source at its advanced epoch so residual writers
    /// resume against the new map.
    fn reopen_source(
        &self,
        source: &str,
        operation: Uuid,
        epoch: u64,
    ) -> impl Future<Output = Result<()>> + Send;
    /// Form one destination group replicated with a durable, serving replica
    /// set. Placements are already durable when this runs.
    fn prepare_destination(
        &self,
        plan: &PlanV3,
        destination: &str,
    ) -> impl Future<Output = Result<()>> + Send;
    /// Remove one destination group during cancellation cleanup.
    fn cleanup_destination(&self, destination: &str) -> impl Future<Output = Result<()>> + Send;
    /// Choose the protected read timestamp for the copy: above every source's
    /// visible committed timestamp, with the destination clock reserved above
    /// it. Called once, at the Preparing boundary.
    fn choose_protected_ts(&self, record: &RecordV3) -> impl Future<Output = Result<u64>> + Send;
    /// Copy one bounded batch for `source` starting after the journaled
    /// cursor at `protected_read_ts`, writing through the destination group's
    /// Raft with the operation's epoch discipline. Returns the next durable
    /// checkpoint (`complete` when the source is exhausted).
    fn copy_batch(
        &self,
        record: &RecordV3,
        source: &str,
    ) -> impl Future<Output = Result<CheckpointV3>> + Send;
    /// Recompute both sides' exact key/version multisets at the protected
    /// timestamp and report the digest comparison.
    fn validate(&self, record: &RecordV3) -> impl Future<Output = Result<ValidationV3>> + Send;
    /// Whether a row source group is still physically hosted (post-`Done`
    /// decommission loop). The meta group is never decommissioned.
    fn source_hosted(&self, source: &str) -> impl Future<Output = Result<bool>> + Send;
    /// Physically remove one source group. Only runs after `Done` is
    /// journaled, i.e. after the destination data and the replicated routing
    /// decision are both durable.
    fn decommission_source(&self, source: &str) -> impl Future<Output = Result<()>> + Send;
}

/// Current routing relative to a plan, read from the meta store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoutingState {
    /// Current shard map equals `expected_map`.
    pub map_matches_expected: bool,
    /// Current placements equal `expected_placements` (pre-preparation).
    pub placements_match_expected: bool,
    /// Current placements equal the derived prepare-step intermediate.
    pub placements_prepared: bool,
    /// Current shard map equals `destination_map`.
    pub map_matches_destination: bool,
    /// Current map equals `destination_map` and placements equal
    /// `destination_placements` (the cutover flip has committed).
    pub published: bool,
}

/// One bounded step per call; the owner schedules another tick. A deadline
/// covers the load, the participant interaction, and the journal write.
pub async fn resume(
    backend: &impl BackendV3,
    table: TableId,
    deadline: Duration,
) -> Result<PhaseV3> {
    tokio::time::timeout(deadline, step(backend, table))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "V3 migration driver step timed out; outcome uncertain, resume from journal"
            )
        })?
}

/// Drives a journaled operation to its terminal phase. Loops `resume` until
/// `Done`/`Cancelled` (with post-`Done` decommission passes) or an error; each
/// iteration is one bounded step.
pub async fn run(
    backend: &impl BackendV3,
    table: TableId,
    deadline: Duration,
    max_steps: usize,
) -> Result<PhaseV3> {
    for _ in 0..max_steps {
        let phase = resume(backend, table, deadline).await?;
        if phase == PhaseV3::Cancelled {
            return Ok(phase);
        }
        if phase == PhaseV3::Done {
            let record = backend.load(table).await?;
            let mut remaining = false;
            for source in &record.plan.migration.sources {
                if source != "shard-meta" && backend.source_hosted(source).await? {
                    remaining = true;
                    break;
                }
            }
            if !remaining {
                return Ok(phase);
            }
        }
    }
    anyhow::bail!("migration did not reach a terminal phase within {max_steps} steps")
}

/// Like [`run`], but exhausting `max_steps` is not an error: returns the last
/// observed phase so a scheduler (the meta-leader discovery worker) continues
/// the operation on its next tick instead of treating a long copy as failed.
pub async fn drive_some(
    backend: &impl BackendV3,
    table: TableId,
    deadline: Duration,
    max_steps: usize,
) -> Result<PhaseV3> {
    let mut phase = backend.load(table).await?.phase;
    for _ in 0..max_steps {
        phase = resume(backend, table, deadline).await?;
        if phase == PhaseV3::Cancelled {
            return Ok(phase);
        }
        if phase == PhaseV3::Done {
            let record = backend.load(table).await?;
            let mut remaining = false;
            for source in &record.plan.migration.sources {
                if source != "shard-meta" && backend.source_hosted(source).await? {
                    remaining = true;
                    break;
                }
            }
            if !remaining {
                return Ok(phase);
            }
        }
    }
    Ok(phase)
}

async fn step(backend: &impl BackendV3, table: TableId) -> Result<PhaseV3> {
    let record = backend.load(table).await?;
    let plan = &record.plan;
    let operation = plan.migration.operation_id;
    let phase = record.phase;
    let finish = |phase: PhaseV3| CommandV3::Finish {
        table,
        operation,
        phase,
        protected_read_ts: None,
    };
    match phase {
        PhaseV3::Cancelled => return Ok(phase),
        PhaseV3::Done => {
            // The routing decision is durable; physical source removal follows,
            // one still-hosted group per call.
            for source in plan.migration.sources.iter().filter(|s| *s != "shard-meta") {
                if backend.source_hosted(source).await? {
                    backend.decommission_source(source).await?;
                    break;
                }
            }
            return Ok(phase);
        }
        PhaseV3::Publishing | PhaseV3::Completing | PhaseV3::Cancelling => {} // no drift check
        _ => {
            // Routing drift at any cancellable check requests cancellation.
            let routing = backend.routing_state(plan).await?;
            let drifted = match phase {
                PhaseV3::Planned | PhaseV3::Fencing | PhaseV3::Fenced => {
                    !routing.map_matches_expected || !routing.placements_match_expected
                }
                // The placements flip is itself the first Preparing step, so
                // both pre- and post-flip content are legitimate mid-phase.
                PhaseV3::Preparing => {
                    !routing.map_matches_expected
                        || !(routing.placements_match_expected || routing.placements_prepared)
                }
                PhaseV3::Copying | PhaseV3::Validating => {
                    !routing.map_matches_expected || !routing.placements_prepared
                }
                _ => unreachable!(),
            };
            if drifted {
                backend
                    .submit_journal(CommandV3::RequestCancel { table, operation })
                    .await?;
                return Ok(backend.load(table).await?.phase);
            }
        }
    }
    match phase {
        PhaseV3::Planned => {
            backend
                .submit_journal(CommandV3::Begin { table, operation })
                .await?;
        }
        PhaseV3::Fencing => {
            if let Some(source) = plan
                .migration
                .sources
                .iter()
                .find(|s| !record.acquired.contains_key(*s))
            {
                let expected_epoch = plan.migration.source_epochs[source];
                backend
                    .fence_source(source, operation, expected_epoch)
                    .await?;
                backend
                    .submit_journal(CommandV3::Ack {
                        table,
                        operation,
                        source: source.clone(),
                        epoch: expected_epoch
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("migration epoch exhausted"))?,
                        cancelled: false,
                    })
                    .await?;
            } else {
                backend.submit_journal(finish(PhaseV3::Fencing)).await?;
            }
        }
        PhaseV3::Fenced => {
            backend.submit_journal(finish(PhaseV3::Fenced)).await?;
        }
        PhaseV3::Preparing => {
            let routing = backend.routing_state(plan).await?;
            if !routing.placements_prepared {
                // Placements durable in the meta group before group formation.
                backend
                    .submit_conditional(ShardCommand::UpdateShardPlacementsConditional {
                        table,
                        operation,
                        phase: PhaseV3::Preparing,
                        expected: plan.expected_placements.clone(),
                        placements: prepare_placements(plan),
                    })
                    .await?;
            } else if let Some(destination) = plan
                .migration
                .destinations
                .iter()
                .find(|d| !record.prepared.contains(*d))
            {
                backend.prepare_destination(plan, destination).await?;
                backend
                    .submit_journal(CommandV3::PrepareAck {
                        table,
                        operation,
                        destination: destination.clone(),
                    })
                    .await?;
            } else {
                let ts = backend.choose_protected_ts(&record).await?;
                backend
                    .submit_journal(CommandV3::Finish {
                        table,
                        operation,
                        phase: PhaseV3::Preparing,
                        protected_read_ts: Some(ts),
                    })
                    .await?;
            }
        }
        PhaseV3::Copying => {
            if let Some(source) = plan
                .migration
                .sources
                .iter()
                .find(|s| !record.checkpoints.get(*s).is_some_and(|c| c.complete))
            {
                let checkpoint = backend.copy_batch(&record, source).await?;
                backend
                    .submit_journal(CommandV3::CopyCheckpoint {
                        table,
                        operation,
                        source: source.clone(),
                        checkpoint,
                    })
                    .await?;
            } else {
                backend.submit_journal(finish(PhaseV3::Copying)).await?;
            }
        }
        PhaseV3::Validating => match &record.validation {
            None => {
                let report = backend.validate(&record).await?;
                backend
                    .submit_journal(CommandV3::ValidationReport {
                        table,
                        operation,
                        report,
                    })
                    .await?;
            }
            Some(report) if report.matches => {
                backend
                    .submit_journal(CommandV3::Publish { table, operation })
                    .await?;
            }
            // A mismatched report already moved the journal to Cancelling at
            // apply; unreachable, kept defensive.
            Some(_) => {
                backend
                    .submit_journal(CommandV3::RequestCancel { table, operation })
                    .await?;
            }
        },
        PhaseV3::Publishing => {
            // A restarted driver may physically lack a destination group that
            // received no copied rows; never publish routing to an unserved
            // group. Preparation is idempotent.
            for destination in &plan.migration.destinations {
                backend.prepare_destination(plan, destination).await?;
            }
            let routing = backend.routing_state(plan).await?;
            if routing.published {
                backend.submit_journal(finish(PhaseV3::Publishing)).await?;
            } else if !routing.map_matches_destination {
                // Map first, then placements; both conditional writes replay
                // idempotently, so a crash between them converges on resume.
                backend
                    .submit_conditional(ShardCommand::UpdateShardMapConditional {
                        table,
                        operation,
                        phase: PhaseV3::Publishing,
                        expected: plan.expected_map.clone(),
                        map: plan
                            .destination_map
                            .clone()
                            .ok_or_else(|| anyhow::anyhow!("publish requires a destination map"))?,
                    })
                    .await?;
            } else {
                backend
                    .submit_conditional(ShardCommand::UpdateShardPlacementsConditional {
                        table,
                        operation,
                        phase: PhaseV3::Publishing,
                        expected: prepare_placements(plan),
                        placements: plan.destination_placements.clone(),
                    })
                    .await?;
            }
        }
        PhaseV3::Completing => {
            if let Some(source) = plan
                .migration
                .sources
                .iter()
                .find(|s| !record.completed.contains(*s))
            {
                let epoch = plan.migration.source_epochs[source]
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("migration epoch exhausted"))?;
                backend.reopen_source(source, operation, epoch).await?;
                backend
                    .submit_journal(CommandV3::CompleteAck {
                        table,
                        operation,
                        source: source.clone(),
                    })
                    .await?;
            } else {
                backend.submit_journal(finish(PhaseV3::Completing)).await?;
            }
        }
        PhaseV3::Cancelling => {
            if let Some(source) = plan
                .migration
                .sources
                .iter()
                .find(|s| !record.cancelled.contains_key(*s))
            {
                let expected_epoch = plan.migration.source_epochs[source];
                backend
                    .seal_source(source, operation, expected_epoch)
                    .await?;
                backend
                    .submit_journal(CommandV3::Ack {
                        table,
                        operation,
                        source: source.clone(),
                        epoch: expected_epoch
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("migration epoch exhausted"))?,
                        cancelled: true,
                    })
                    .await?;
            } else {
                // Sources are sealed; restore the pre-operation placements
                // before cleaning the destination groups they referenced.
                let routing = backend.routing_state(plan).await?;
                if !routing.placements_match_expected {
                    backend
                        .submit_conditional(ShardCommand::UpdateShardPlacementsConditional {
                            table,
                            operation,
                            phase: PhaseV3::Cancelling,
                            expected: prepare_placements(plan),
                            placements: plan.expected_placements.clone(),
                        })
                        .await?;
                    return Ok(backend.load(table).await?.phase);
                }
                if let Some(destination) = plan
                    .migration
                    .destinations
                    .iter()
                    .find(|d| !record.cleaned.contains(*d))
                {
                    // Every planned destination is cleaned, not only the ones whose
                    // PrepareAck landed: a crash between formation and the ack must
                    // not leak a group.
                    backend.cleanup_destination(destination).await?;
                    backend
                        .submit_journal(CommandV3::CleanupAck {
                            table,
                            operation,
                            destination: destination.clone(),
                        })
                        .await?;
                } else {
                    backend.submit_journal(finish(PhaseV3::Cancelling)).await?;
                }
            }
        }
        PhaseV3::Done | PhaseV3::Cancelled => unreachable!(),
    }
    Ok(backend.load(table).await?.phase)
}
