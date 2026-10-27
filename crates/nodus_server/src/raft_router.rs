//! Async write-submission actor bridging the synchronous storage/catalog write
//! traits to async OpenRaft `client_write`.
//!
//! The `KvEngine` and `CatalogWriter` traits are synchronous, but OpenRaft's
//! `client_write` is async. Previously each write wrapped the call in
//! `tokio::task::block_in_place(|| Handle::current().block_on(..))`, which is a
//! brittle anti-pattern (it nests a `block_on` inside a runtime worker and
//! depletes the worker pool). Instead, the synchronous write paths now call
//! [`RaftRouter::submit`], which hands the command to a dispatcher task running
//! on the Tokio runtime and waits for the applied result on a oneshot channel.
//!
//! IMPORTANT: [`RaftRouter::submit`] waits via
//! [`tokio::sync::oneshot::Receiver::blocking_recv`], which panics if called on a
//! runtime worker thread and risks worker-pool starvation if it blocked one.
//! Every caller MUST therefore run on a blocking context (e.g. inside
//! `tokio::task::spawn_blocking`), never directly on a reactor worker thread.

use anyhow::{Result, anyhow};
use nodus_raftstore::{NodusTypeConfig, ShardCommand, ShardResponse};
use openraft::Raft;
use openraft::error::{CheckIsLeaderError, ClientWriteError, ForwardToLeader, RaftError};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

use crate::multi_raft::MultiRaftManager;

/// The applied command's response (carries the 2PC prepare vote in
/// [`ShardResponse::success`]).
type WriteResult = Result<ShardResponse>;

/// How long a follower waits for its state machine to catch up to the leader's
/// read index before giving the linearizable read up (and retrying).
const BARRIER_WAIT: Duration = Duration::from_secs(5);
const COMMIT_BATCH_MAX: usize = 32;
const COMMIT_BATCH_WAIT: Duration = Duration::from_millis(1);
const COMMIT_QUEUE_MAX: usize = 1024;

struct CommitRequest {
    shard_id: String,
    epoch: u64,
    txn_id: nodus_storage_api::TxnId,
    commit_ts: u64,
    resp: oneshot::Sender<Result<()>>,
}

struct PendingCommitBatch {
    requests: Vec<CommitRequest>,
    created_at: tokio::time::Instant,
    deadline: tokio::time::Instant,
}

#[derive(Debug)]
struct SingleCommitFallbackError;

impl std::fmt::Display for SingleCommitFallbackError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("single-commit fallback failed")
    }
}

impl std::error::Error for SingleCommitFallbackError {}

pub(crate) fn is_single_commit_fallback(error: &anyhow::Error) -> bool {
    error.downcast_ref::<SingleCommitFallbackError>().is_some()
}

/// A request handed to the dispatcher: replicate a command, or run a
/// linearizable-read barrier on a group. Both are bridged sync→async here so the
/// synchronous storage traits can drive them via `blocking_recv`.
enum RouterRequest {
    Write {
        shard_id: String,
        cmd: ShardCommand,
        resp: oneshot::Sender<WriteResult>,
    },
    RepairLegacy {
        shard_id: String,
        txn: nodus_storage_api::TxnId,
        key: bytes::Bytes,
        replacement: nodus_storage_api::IntentReplacement,
        resp: oneshot::Sender<Result<()>>,
    },
    Barrier {
        shard_id: String,
        resp: oneshot::Sender<Result<()>>,
    },
}

/// Cheap, clonable handle used by synchronous write paths to replicate commands
/// through the appropriate Raft group.
#[derive(Clone)]
pub struct RaftRouter {
    tx: mpsc::UnboundedSender<RouterRequest>,
    commit_tx: mpsc::Sender<CommitRequest>,
    /// Isolated-fixture override; combined (OR) with the durable gate.
    migration_override: Arc<std::sync::atomic::AtomicBool>,
    /// Production activation: reads the durable upgrade authority and opens
    /// only once the migration cluster level is finalized. Evaluated per
    /// migration-protocol submission (ordinary writes never call it) and must
    /// fail closed. Defaults to closed.
    migration_gate: Arc<dyn Fn() -> bool + Send + Sync>,
    write_batch_gate: Arc<dyn Fn() -> bool + Send + Sync>,
    metrics: Arc<OnceLock<nodus_monitoring::Metrics>>,
}

impl RaftRouter {
    /// Spawn the dispatcher task. Must be called from within the Tokio runtime.
    /// Groups are resolved per request through the [`MultiRaftManager`], so a
    /// shard whose group is created later is reachable without re-spawning.
    pub fn spawn(manager: Arc<MultiRaftManager>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<RouterRequest>();
        // All peer traffic — ordinary forwarded writes, read-index requests,
        // and migration-protocol commands — rides the manager's transport: plain
        // HTTP + the admin bearer token, or the mTLS peer client when
        // `cluster.tls` is configured. Migration-protocol commands additionally
        // require the mTLS scheme and a capability re-probe (see
        // `forward_migration`).
        let transport = manager.transport();
        let metrics = Arc::new(OnceLock::new());
        let (commit_tx, commit_rx) = mpsc::channel::<CommitRequest>(COMMIT_QUEUE_MAX);
        tokio::spawn(commit_dispatch(
            manager.clone(),
            transport.clone(),
            commit_rx,
            metrics.clone(),
        ));
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let shard_id = match &req {
                    RouterRequest::Write { shard_id, .. } => shard_id.clone(),
                    RouterRequest::Barrier { shard_id, .. }
                    | RouterRequest::RepairLegacy { shard_id, .. } => shard_id.clone(),
                };
                let raft = manager.get(&shard_id).await;
                // Drive each request concurrently so independent submissions do
                // not serialize behind one another.
                match (raft, req) {
                    (
                        _,
                        RouterRequest::RepairLegacy {
                            txn,
                            key,
                            replacement,
                            resp,
                            ..
                        },
                    ) => {
                        let manager = manager.clone();
                        tokio::spawn(async move {
                            let _ = resp.send(
                                manager
                                    .repair_legacy(&shard_id, txn, key, replacement)
                                    .await,
                            );
                        });
                    }
                    (Some(raft), RouterRequest::Write { cmd, resp, .. }) => {
                        let transport = transport.clone();
                        tokio::spawn(async move {
                            let res = replicate(raft, &shard_id, cmd, &transport).await;
                            let _ = resp.send(res);
                        });
                    }
                    (Some(raft), RouterRequest::Barrier { resp, .. }) => {
                        let transport = transport.clone();
                        tokio::spawn(async move {
                            let res = linearizable_barrier(raft, &shard_id, &transport).await;
                            let _ = resp.send(res);
                        });
                    }
                    (None, RouterRequest::Write { resp, .. }) => {
                        let _ = resp.send(Err(anyhow!("raft group '{shard_id}' not found")));
                    }
                    (None, RouterRequest::Barrier { resp, .. }) => {
                        let _ = resp.send(Err(anyhow!("raft group '{shard_id}' not found")));
                    }
                }
            }
        });
        Self {
            tx,
            commit_tx,
            migration_override: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            migration_gate: Arc::new(|| false),
            write_batch_gate: Arc::new(|| false),
            metrics,
        }
    }

    pub(crate) fn attach_metrics(&self, metrics: &nodus_monitoring::Metrics) {
        self.metrics.get_or_init(|| metrics.clone());
    }

    /// Wire the production activation gate: a durable-authority read that
    /// opens only at the finalized migration cluster level. Fails closed on
    /// any read error by construction of the closure.
    pub fn with_migration_gate(mut self, gate: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        self.migration_gate = Arc::new(gate);
        self
    }

    pub fn with_write_batch_gate(
        mut self,
        gate: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        self.write_batch_gate = Arc::new(gate);
        self
    }

    pub(crate) fn migration_enabled(&self) -> bool {
        self.migration_override
            .load(std::sync::atomic::Ordering::Acquire)
            || (self.migration_gate)()
    }

    pub(crate) fn write_batch_enabled(&self) -> bool {
        (self.write_batch_gate)()
    }

    /// Isolated fixtures only: no config flag or production activation bypass.
    #[cfg(test)]
    pub(crate) fn enable_migration_for_test(&mut self) {
        self.migration_override
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn repair_legacy(
        &self,
        shard_id: &str,
        txn: nodus_storage_api::TxnId,
        key: bytes::Bytes,
        replacement: nodus_storage_api::IntentReplacement,
    ) -> Result<()> {
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(RouterRequest::RepairLegacy {
                shard_id: shard_id.to_string(),
                txn,
                key,
                replacement,
                resp,
            })
            .map_err(|_| anyhow!("raft router dispatcher stopped"))?;
        rx.blocking_recv()
            .map_err(|_| anyhow!("raft router dropped repair response"))?
    }

    /// Replicate `cmd` to `shard_id` and block until it is applied on the leader.
    ///
    /// MUST be called from a blocking context (not a runtime worker thread): the
    /// wait uses `blocking_recv`, which panics on a runtime worker. `client_write`
    /// resolves only after the entry is applied to the leader's state machine, so
    /// a subsequent local read observes the write (read-your-writes); a non-leader
    /// fails here with `ForwardToLeader`.
    pub fn submit(&self, shard_id: &str, cmd: ShardCommand) -> Result<()> {
        let response = self.submit_inner(shard_id, cmd)?;
        anyhow::ensure!(
            response.success,
            "{}",
            response
                .error
                .unwrap_or_else(|| "Raft command rejected by participant".into())
        );
        Ok(())
    }

    /// Like [`Self::submit`] but returns the applied [`ShardResponse`], so the
    /// caller can read its `success` flag — used for the 2PC prepare vote.
    pub fn submit_voting(&self, shard_id: &str, cmd: ShardCommand) -> Result<ShardResponse> {
        self.submit_inner(shard_id, cmd)
    }

    pub fn submit_commit(
        &self,
        shard_id: &str,
        epoch: u64,
        txn_id: nodus_storage_api::TxnId,
        commit_ts: u64,
    ) -> Result<bool> {
        anyhow::ensure!(
            self.write_batch_enabled(),
            "write batching requires verified cluster-wide compatibility"
        );
        let (resp_tx, resp_rx) = oneshot::channel();
        let request = CommitRequest {
            shard_id: shard_id.to_string(),
            epoch,
            txn_id,
            commit_ts,
            resp: resp_tx,
        };
        match self.commit_tx.try_send(request) {
            Ok(()) => {
                resp_rx
                    .blocking_recv()
                    .map_err(|_| anyhow!("raft router dropped commit response"))??;
                Ok(true)
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                if let Some(metrics) = self.metrics.get() {
                    metrics.inc_commit_batch_fallback("queue_full");
                }
                tracing::debug!("Raft commit queue full; using single-commit fallback");
                self.submit(
                    shard_id,
                    ShardCommand::EpochWriteV1 {
                        epoch,
                        mutation: nodus_raftstore::migration::EpochMutationV1::Commit {
                            txn_id: txn_id.0,
                            commit_ts,
                        },
                    },
                )
                .map_err(|error| error.context(SingleCommitFallbackError))?;
                Ok(false)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow!("raft commit dispatcher stopped"))
            }
        }
    }

    fn submit_inner(&self, shard_id: &str, cmd: ShardCommand) -> Result<ShardResponse> {
        let started = Instant::now();
        let command = cmd.metrics_kind();
        let intent_operations = match &cmd {
            ShardCommand::EpochWriteBatchV1 { operations, .. } => Some(operations.len()),
            _ => None,
        };
        let result = (|| {
            anyhow::ensure!(
                !cmd.requires_migration_protocol() || self.migration_enabled(),
                "migration protocol activation requires verified cluster-wide compatibility"
            );
            anyhow::ensure!(
                !cmd.requires_write_batch_protocol() || self.write_batch_enabled(),
                "write batching requires verified cluster-wide compatibility"
            );
            let (resp_tx, resp_rx) = oneshot::channel();
            self.tx
                .send(RouterRequest::Write {
                    shard_id: shard_id.to_string(),
                    cmd,
                    resp: resp_tx,
                })
                .map_err(|_| anyhow!("raft router dispatcher stopped"))?;
            resp_rx
                .blocking_recv()
                .map_err(|_| anyhow!("raft router dropped response"))?
        })();
        observe_command(&self.metrics, command, started, &result);
        if let (Some(operations), Ok(response)) = (intent_operations, &result)
            && response.success
            && let Some(metrics) = self.metrics.get()
        {
            metrics.observe_intent_batch(operations);
        }
        result
    }

    /// Blocks until this node's replica of `shard_id` reflects every write
    /// committed before the call — a linearizable-read barrier (Raft ReadIndex).
    /// After it returns, a local read at the latest snapshot observes all such
    /// writes, so a read served from any replica is linearizable.
    ///
    /// MUST be called from a blocking context (it waits via `blocking_recv`).
    pub fn read_barrier(&self, shard_id: &str) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(RouterRequest::Barrier {
                shard_id: shard_id.to_string(),
                resp: resp_tx,
            })
            .map_err(|_| anyhow!("raft router dispatcher stopped"))?;
        resp_rx
            .blocking_recv()
            .map_err(|_| anyhow!("raft router dropped response"))?
    }
}

fn observe_command(
    metrics: &OnceLock<nodus_monitoring::Metrics>,
    command: &str,
    started: Instant,
    result: &WriteResult,
) {
    if let Some(metrics) = metrics.get() {
        let result = match result {
            Ok(response) if response.success => "success",
            Ok(_) => "rejected",
            Err(_) => "error",
        };
        metrics.observe_raft_command(command, result, started.elapsed());
    }
}

async fn commit_dispatch(
    manager: Arc<MultiRaftManager>,
    transport: nodus_raftstore::network::RaftTransport,
    mut rx: mpsc::Receiver<CommitRequest>,
    metrics: Arc<OnceLock<nodus_monitoring::Metrics>>,
) {
    let mut batches: HashMap<(String, u64), PendingCommitBatch> = HashMap::new();
    loop {
        let next_deadline = batches.values().map(|batch| batch.deadline).min();
        let wait = async {
            if let Some(deadline) = next_deadline {
                tokio::time::sleep_until(deadline).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            request = rx.recv() => {
                let Some(request) = request else {
                    for ((shard_id, epoch), batch) in batches.drain() {
                        spawn_commit_batch(
                            manager.clone(),
                            transport.clone(),
                            shard_id,
                            epoch,
                            batch,
                            metrics.clone(),
                        );
                    }
                    break;
                };
                let key = (request.shard_id.clone(), request.epoch);
                let full = {
                    let batch = batches.entry(key.clone()).or_insert_with(|| {
                        let now = tokio::time::Instant::now();
                        PendingCommitBatch {
                            requests: Vec::with_capacity(COMMIT_BATCH_MAX),
                            created_at: now,
                            deadline: now + COMMIT_BATCH_WAIT,
                        }
                    });
                    batch.requests.push(request);
                    batch.requests.len() >= COMMIT_BATCH_MAX
                };
                if full && let Some(batch) = batches.remove(&key) {
                    spawn_commit_batch(
                        manager.clone(),
                        transport.clone(),
                        key.0,
                        key.1,
                        batch,
                        metrics.clone(),
                    );
                }
            }
            _ = wait => {
                let now = tokio::time::Instant::now();
                let expired = batches
                    .iter()
                    .filter(|(_, batch)| batch.deadline <= now)
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                for key in expired {
                    if let Some(batch) = batches.remove(&key) {
                        spawn_commit_batch(
                            manager.clone(),
                            transport.clone(),
                            key.0,
                            key.1,
                            batch,
                            metrics.clone(),
                        );
                    }
                }
            }
        }
    }
}

fn spawn_commit_batch(
    manager: Arc<MultiRaftManager>,
    transport: nodus_raftstore::network::RaftTransport,
    shard_id: String,
    epoch: u64,
    batch: PendingCommitBatch,
    metrics: Arc<OnceLock<nodus_monitoring::Metrics>>,
) {
    let wait = batch.created_at.elapsed();
    if let Some(metrics) = metrics.get() {
        metrics.observe_commit_batch(batch.requests.len(), wait);
    }
    tracing::debug!(
        group = %shard_id,
        commits = batch.requests.len(),
        wait = ?wait,
        "Dispatching Raft commit batch"
    );
    tokio::spawn(async move {
        let started = Instant::now();
        let commits = batch
            .requests
            .iter()
            .map(|request| nodus_raftstore::migration::EpochCommitV1 {
                txn_id: request.txn_id.0,
                commit_ts: request.commit_ts,
            })
            .collect();
        let result = match manager.get(&shard_id).await {
            Some(raft) => {
                replicate(
                    raft,
                    &shard_id,
                    ShardCommand::EpochCommitBatchV1 { epoch, commits },
                    &transport,
                )
                .await
            }
            None => Err(anyhow!("raft group '{shard_id}' not found")),
        };
        observe_command(&metrics, "commit_batch", started, &result);
        let result = match result {
            Ok(response) if response.success => Ok(()),
            Ok(response) => Err(response
                .error
                .unwrap_or_else(|| "Raft commit batch rejected by participant".into())),
            Err(error) => Err(error.to_string()),
        };
        for request in batch.requests {
            let _ = request
                .resp
                .send(result.clone().map_err(|error| anyhow!(error)));
        }
    });
}

/// Replicates `cmd` to `group_id`, **forwarding to the leader** when this node
/// isn't it. OpenRaft's `client_write` only succeeds on the leader (it returns
/// `ForwardToLeader` otherwise and never auto-forwards), so a non-leader posts
/// the command to the leader's `/raft/{group}/write`. Bounded retries let a
/// transient election settle and re-resolve a leader that moves mid-forward.
///
/// This decouples *where a write is issued* from *which node leads the group*:
/// any node can accept a write for any group it can route to — essential after a
/// failover, when the data-group and meta-group leaders may sit on different
/// nodes than the one serving the query.
async fn replicate(
    raft: Raft<NodusTypeConfig>,
    group_id: &str,
    cmd: ShardCommand,
    transport: &nodus_raftstore::network::RaftTransport,
) -> WriteResult {
    // A stable id for this logical write, shared across every retry/forward
    // below, so the leader can dedup a re-forwarded write whose prior response
    // was lost (see `RequestDedup`) instead of applying it twice.
    let request_id = uuid::Uuid::new_v4().to_string();
    for _ in 0..25 {
        match raft.client_write(cmd.clone()).await {
            Ok(resp) => return Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_id,
                leader_node: Some(node),
                ..
            }))) => {
                let forwarded = if cmd.requires_migration_protocol() {
                    if transport.scheme() != "https" {
                        // No retry changes this: migration-protocol commands
                        // never travel the shared bearer-token listener.
                        return Err(anyhow!(
                            "migration protocol command for '{group_id}' needs leader forwarding, which requires the mTLS peer transport (cluster.tls)"
                        ));
                    }
                    forward_migration(
                        transport,
                        leader_id,
                        &node.addr,
                        group_id,
                        &cmd,
                        &request_id,
                    )
                    .await
                } else {
                    transport
                        .forward_command(&node.addr, group_id, &cmd, &request_id)
                        .await
                };
                match forwarded {
                    Ok(resp) => return Ok(resp),
                    // The leader may have just changed; re-evaluate after a beat.
                    Err(e) => {
                        tracing::debug!("forward of {group_id} write to {} failed: {e}", node.addr)
                    }
                }
            }
            // No known leader yet — wait for an election, then retry.
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_node: None,
                ..
            }))) => {}
            Err(e) => return Err(anyhow!("raft write error: {}", e)),
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    Err(anyhow!("raft write to '{group_id}' did not reach a leader"))
}

/// Forwards a migration-protocol command to the group's leader over the peer
/// mTLS transport. The recipient is re-probed immediately before the send and
/// must report a migration capability at or above `MIGRATION_VERSION`; the
/// probe is challenge-bound to the TLS-authenticated peer, so a recipient that
/// cannot enforce the protocol is never sent the command. Rejected forwards
/// never enter the leader's dedup cache (leader-side behavior, unchanged).
async fn forward_migration(
    transport: &nodus_raftstore::network::RaftTransport,
    leader_id: Option<u64>,
    addr: &str,
    group_id: &str,
    cmd: &ShardCommand,
    request_id: &str,
) -> WriteResult {
    let leader_id = leader_id
        .ok_or_else(|| anyhow!("cannot forward migration command to {addr}: leader id unknown"))?;
    let report = transport
        .probe(leader_id, addr, uuid::Uuid::new_v4(), false)
        .await
        .map_err(|e| anyhow!("migration forward to {addr}: capability probe failed: {e}"))?;
    anyhow::ensure!(
        report.migration_version >= Some(nodus_raftstore::upgrade::MIGRATION_VERSION),
        "migration forward to {addr}: peer reports migration_version {:?}, below {}",
        report.migration_version,
        nodus_raftstore::upgrade::MIGRATION_VERSION
    );
    if cmd.requires_write_batch_protocol() {
        anyhow::ensure!(
            report.write_batch_version >= Some(nodus_raftstore::upgrade::WRITE_BATCH_VERSION),
            "write batch forward to {addr}: peer reports write_batch_version {:?}, below {}",
            report.write_batch_version,
            nodus_raftstore::upgrade::WRITE_BATCH_VERSION
        );
    }
    transport
        .forward_write(addr, group_id, cmd, request_id)
        .await
}

/// Runs a linearizable-read barrier on this node's replica of `group_id`.
///
/// On the leader, [`Raft::ensure_linearizable`] heartbeats a quorum to confirm
/// leadership and blocks until the state machine has applied up to the read log
/// id — a complete barrier. On a follower it returns `ForwardToLeader`; we then
/// ask the leader for its read index (where the leader confirms *its* leadership)
/// and wait for this node's own state machine to apply at least that far. Either
/// way, once this returns a subsequent local read reflects all writes committed
/// before the barrier. Bounded retries ride out an in-flight election.
async fn linearizable_barrier(
    raft: Raft<NodusTypeConfig>,
    group_id: &str,
    transport: &nodus_raftstore::network::RaftTransport,
) -> Result<()> {
    for _ in 0..25 {
        match raft.ensure_linearizable().await {
            // Leader: leadership confirmed and the state machine is caught up.
            Ok(_) => return Ok(()),
            // Follower: fetch the leader's read index, then wait for our own
            // state machine to catch up to it before reading locally.
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(node),
                ..
            }))) => {
                match transport.read_index(&node.addr, group_id).await {
                    Ok(Some(index)) => {
                        raft.wait(Some(BARRIER_WAIT))
                            .applied_index_at_least(Some(index), "linearizable read barrier")
                            .await
                            .map_err(|e| {
                                anyhow!("await applied index {index} for {group_id}: {e}")
                            })?;
                        return Ok(());
                    }
                    // Empty leader log — nothing committed yet, so nothing to wait for.
                    Ok(None) => return Ok(()),
                    Err(e) => {
                        tracing::debug!("read-index for {group_id} from {} failed: {e}", node.addr)
                    }
                }
            }
            // No known leader, or leadership unconfirmable for now — retry.
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: None,
                ..
            })))
            | Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))) => {}
            Err(e) => return Err(anyhow!("linearizable barrier on {group_id}: {e}")),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow!(
        "linearizable barrier on '{group_id}' did not converge"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_single_commit_fallback_errors_through_context() {
        let error = anyhow!("raft group unavailable").context(SingleCommitFallbackError);
        assert!(is_single_commit_fallback(&error));
        assert_eq!(
            format!("{error:#}"),
            "single-commit fallback failed: raft group unavailable"
        );
        assert!(!is_single_commit_fallback(&anyhow!(
            "batched commit failed"
        )));
    }
}
