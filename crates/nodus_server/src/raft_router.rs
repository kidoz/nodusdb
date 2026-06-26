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
use nodus_raftstore::{NodusTypeConfig, ShardCommand};
use openraft::Raft;
use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use crate::multi_raft::MultiRaftManager;

type WriteResult = Result<()>;

struct WriteRequest {
    shard_id: String,
    cmd: ShardCommand,
    resp: oneshot::Sender<WriteResult>,
}

/// Cheap, clonable handle used by synchronous write paths to replicate commands
/// through the appropriate Raft group.
#[derive(Clone)]
pub struct RaftRouter {
    tx: mpsc::UnboundedSender<WriteRequest>,
}

impl RaftRouter {
    /// Spawn the dispatcher task. Must be called from within the Tokio runtime.
    /// Groups are resolved per request through the [`MultiRaftManager`], so a
    /// shard whose group is created later is reachable without re-spawning.
    pub fn spawn(manager: Arc<MultiRaftManager>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<WriteRequest>();
        let http = reqwest::Client::new();
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let raft = manager.get(&req.shard_id).await;
                match raft {
                    Some(raft) => {
                        // Drive each replication concurrently so independent
                        // submissions do not serialize behind one another.
                        let http = http.clone();
                        let WriteRequest {
                            shard_id,
                            cmd,
                            resp,
                        } = req;
                        tokio::spawn(async move {
                            let res = replicate(raft, &shard_id, cmd, &http).await;
                            let _ = resp.send(res);
                        });
                    }
                    None => {
                        let _ = req
                            .resp
                            .send(Err(anyhow!("raft group '{}' not found", req.shard_id)));
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
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(WriteRequest {
                shard_id: shard_id.to_string(),
                cmd,
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
    http: &reqwest::Client,
) -> WriteResult {
    // A stable id for this logical write, shared across every retry/forward
    // below, so the leader can dedup a re-forwarded write whose prior response
    // was lost (see `RequestDedup`) instead of applying it twice.
    let request_id = uuid::Uuid::new_v4().to_string();
    for _ in 0..25 {
        match raft.client_write(cmd.clone()).await {
            Ok(_) => return Ok(()),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(node),
                ..
            }))) => match forward(http, &node.addr, group_id, &cmd, &request_id).await {
                Ok(()) => return Ok(()),
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
        Ok(())
    } else {
        Err(anyhow!(
            "leader {addr} rejected forwarded write: {}",
            resp.status()
        ))
    }
}
