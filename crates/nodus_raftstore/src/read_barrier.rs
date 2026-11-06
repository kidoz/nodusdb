//! Shared leader/follower ReadIndex barrier for routers and verified ingress.
use crate::{network::RaftTransport, server::NodusRaft};
use anyhow::{Result, anyhow};
use openraft::error::{CheckIsLeaderError, ForwardToLeader, RaftError};
use std::time::Duration;

/// Confirm a leader or bring a local follower up to the leader's read index.
/// Failed quorum/transport checks never authorize a local metadata decision.
pub async fn ensure_local(raft: NodusRaft, group: &str, transport: &RaftTransport) -> Result<()> {
    for _ in 0..25 {
        match raft.ensure_linearizable().await {
            Ok(_) => return Ok(()),
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: Some(node),
                ..
            }))) => match transport.read_index(&node.addr, group).await {
                Ok(Some(index)) => {
                    raft.wait(Some(Duration::from_secs(5)))
                        .applied_index_at_least(Some(index), "linearizable read barrier")
                        .await
                        .map_err(|e| anyhow!("await applied index {index} for {group}: {e}"))?;
                    return Ok(());
                }
                Ok(None) => return Ok(()),
                Err(error) => tracing::debug!(%error, group, "read-index deferred"),
            },
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_node: None,
                ..
            })))
            | Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))) => {}
            Err(error) => return Err(anyhow!("linearizable barrier on {group}: {error}")),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("linearizable barrier on '{group}' did not converge")
}
