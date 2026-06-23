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
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let raft = manager.get(&req.shard_id).await;
                match raft {
                    Some(raft) => {
                        // Drive each replication concurrently so independent
                        // submissions do not serialize behind one another.
                        tokio::spawn(async move {
                            let res = replicate(raft, req.cmd).await;
                            let _ = req.resp.send(res);
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

async fn replicate(raft: Raft<NodusTypeConfig>, cmd: ShardCommand) -> WriteResult {
    raft.client_write(cmd)
        .await
        .map(|_| ())
        .map_err(|e| anyhow!("raft write error: {}", e))
}
