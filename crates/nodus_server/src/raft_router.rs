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
use nodus_raftstore::{NodusTypeConfig, ReadIndexResponse, ShardCommand, ShardResponse};
use openraft::Raft;
use openraft::error::{CheckIsLeaderError, ClientWriteError, ForwardToLeader, RaftError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::multi_raft::MultiRaftManager;

/// The applied command's response (carries the 2PC prepare vote in
/// [`ShardResponse::success`]).
type WriteResult = Result<ShardResponse>;

/// How long a follower waits for its state machine to catch up to the leader's
/// read index before giving the linearizable read up (and retrying).
const BARRIER_WAIT: Duration = Duration::from_secs(5);

/// A request handed to the dispatcher: replicate a command, or run a
/// linearizable-read barrier on a group. Both are bridged sync→async here so the
/// synchronous storage traits can drive them via `blocking_recv`.
enum RouterRequest {
    Write {
        shard_id: String,
        cmd: ShardCommand,
        resp: oneshot::Sender<WriteResult>,
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
}

impl RaftRouter {
    /// Spawn the dispatcher task. Must be called from within the Tokio runtime.
    /// Groups are resolved per request through the [`MultiRaftManager`], so a
    /// shard whose group is created later is reachable without re-spawning.
    pub fn spawn(manager: Arc<MultiRaftManager>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<RouterRequest>();
        // Built on first actual use, inside `forward`/`fetch_read_index`. The
        // client is only needed to forward a request to a remote leader (or run a
        // cross-node read barrier); a single-node leader does neither, so it is
        // never constructed there. That matters because the first
        // `reqwest::Client::new()` in a process initializes the rustls + aws-lc-rs
        // TLS stack, whose native-library load on macOS goes through a Gatekeeper
        // code-signing assessment that can block startup for seconds.
        let http: Arc<std::sync::OnceLock<reqwest::Client>> = Arc::new(std::sync::OnceLock::new());
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let shard_id = match &req {
                    RouterRequest::Write { shard_id, .. } => shard_id.clone(),
                    RouterRequest::Barrier { shard_id, .. } => shard_id.clone(),
                };
                let raft = manager.get(&shard_id).await;
                let http = http.clone();
                // Drive each request concurrently so independent submissions do
                // not serialize behind one another.
                match (raft, req) {
                    (Some(raft), RouterRequest::Write { cmd, resp, .. }) => {
                        tokio::spawn(async move {
                            let res = replicate(raft, &shard_id, cmd, &http).await;
                            let _ = resp.send(res);
                        });
                    }
                    (Some(raft), RouterRequest::Barrier { resp, .. }) => {
                        tokio::spawn(async move {
                            let res = linearizable_barrier(raft, &shard_id, &http).await;
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
        Self { tx }
    }

    /// Replicate `cmd` to `shard_id` and block until it is applied on the leader.
    ///
    /// MUST be called from a blocking context (not a runtime worker thread): the
    /// wait uses `blocking_recv`, which panics on a runtime worker. `client_write`
    /// resolves only after the entry is applied to the leader's state machine, so
    /// a subsequent local read observes the write (read-your-writes); a non-leader
    /// fails here with `ForwardToLeader`.
    pub fn submit(&self, shard_id: &str, cmd: ShardCommand) -> Result<()> {
        self.submit_inner(shard_id, cmd).map(|_| ())
    }

    /// Like [`Self::submit`] but returns the applied [`ShardResponse`], so the
    /// caller can read its `success` flag — used for the 2PC prepare vote.
    pub fn submit_voting(&self, shard_id: &str, cmd: ShardCommand) -> Result<ShardResponse> {
        self.submit_inner(shard_id, cmd)
    }

    fn submit_inner(&self, shard_id: &str, cmd: ShardCommand) -> Result<ShardResponse> {
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
    http: &std::sync::OnceLock<reqwest::Client>,
) -> WriteResult {
    // A stable id for this logical write, shared across every retry/forward
    // below, so the leader can dedup a re-forwarded write whose prior response
    // was lost (see `RequestDedup`) instead of applying it twice.
    let request_id = uuid::Uuid::new_v4().to_string();
    for _ in 0..25 {
        match raft.client_write(cmd.clone()).await {
            Ok(resp) => return Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(node),
                ..
            }))) => match forward(
                http.get_or_init(reqwest::Client::new),
                &node.addr,
                group_id,
                &cmd,
                &request_id,
            )
            .await
            {
                Ok(resp) => return Ok(resp),
                // The leader may have just changed; re-evaluate after a beat.
                Err(e) => {
                    tracing::debug!("forward of {group_id} write to {} failed: {e}", node.addr)
                }
            },
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

/// Posts a command to `addr`'s leader-side write endpoint for `group_id`,
/// tagging it with `request_id` so the leader can dedup a retried forward.
async fn forward(
    http: &reqwest::Client,
    addr: &str,
    group_id: &str,
    cmd: &ShardCommand,
    request_id: &str,
) -> WriteResult {
    let url = format!("http://{addr}/raft/{group_id}/write");
    let resp = http
        .post(&url)
        .header(nodus_raftstore::server::REQUEST_ID_HEADER, request_id)
        .json(cmd)
        .send()
        .await
        .map_err(|e| anyhow!("forward to {addr}: {e}"))?;
    if resp.status().is_success() {
        // The leader returns the applied ShardResponse so a forwarded prepare's
        // vote reaches the coordinator.
        resp.json::<ShardResponse>()
            .await
            .map_err(|e| anyhow!("parse forwarded write response from {addr}: {e}"))
    } else {
        Err(anyhow!(
            "leader {addr} rejected forwarded write: {}",
            resp.status()
        ))
    }
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
    http: &std::sync::OnceLock<reqwest::Client>,
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
                match fetch_read_index(http.get_or_init(reqwest::Client::new), &node.addr, group_id)
                    .await
                {
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

/// Asks the leader at `addr` for its read log index for `group_id`. The leader
/// confirms its own leadership (quorum heartbeat) before answering, so the
/// returned index is a safe linearization point for a follower to wait on.
async fn fetch_read_index(
    http: &reqwest::Client,
    addr: &str,
    group_id: &str,
) -> Result<Option<u64>> {
    let url = format!("http://{addr}/raft/{group_id}/read_index");
    let resp = http
        .post(&url)
        .send()
        .await
        .map_err(|e| anyhow!("read-index to {addr}: {e}"))?;
    if resp.status().is_success() {
        resp.json::<ReadIndexResponse>()
            .await
            .map(|r| r.index)
            .map_err(|e| anyhow!("parse read-index response from {addr}: {e}"))
    } else {
        Err(anyhow!(
            "leader {addr} could not serve read-index: {}",
            resp.status()
        ))
    }
}
