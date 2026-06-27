use crate::NodusTypeConfig;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::sync::Arc;

/// The HTTP client and URL scheme used for inter-node Raft RPCs. Plain HTTP by
/// default; a TLS-configured client with `https` is supplied when inter-node
/// mTLS is enabled, so the same RPC code path serves both.
#[derive(Clone)]
pub struct RaftTransport {
    client: reqwest::Client,
    scheme: Arc<str>,
}

impl RaftTransport {
    /// Plain-HTTP transport (no peer TLS).
    pub fn plain() -> Self {
        Self {
            client: reqwest::Client::new(),
            scheme: Arc::from("http"),
        }
    }

    /// Transport over a caller-built client (e.g. one carrying a client identity
    /// and peer-CA trust) with the given scheme (`http` or `https`).
    pub fn new(client: reqwest::Client, scheme: impl Into<Arc<str>>) -> Self {
        Self {
            client,
            scheme: scheme.into(),
        }
    }

    /// The URL scheme RPCs are issued over (`http` or `https`).
    pub fn scheme(&self) -> &str {
        &self.scheme
    }
}

impl Default for RaftTransport {
    fn default() -> Self {
        Self::plain()
    }
}

pub struct NodusNetwork {
    shard_id: String,
    target: u64,
    target_node: BasicNode,
    client: reqwest::Client,
    scheme: Arc<str>,
}

impl RaftNetwork<NodusTypeConfig> for NodusNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<NodusTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let url = format!(
            "{}://{}/raft/{}/append",
            self.scheme, self.target_node.addr, self.shard_id
        );
        let resp = self
            .client
            .post(url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<AppendEntriesResponse<u64>, RaftError<u64>> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<NodusTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let url = format!(
            "{}://{}/raft/{}/snapshot",
            self.scheme, self.target_node.addr, self.shard_id
        );
        let resp = self
            .client
            .post(url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let url = format!(
            "{}://{}/raft/{}/vote",
            self.scheme, self.target_node.addr, self.shard_id
        );
        let resp = self
            .client
            .post(url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let res: Result<VoteResponse<u64>, RaftError<u64>> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

pub struct NodusNetworkFactory {
    shard_id: String,
    transport: RaftTransport,
}

impl NodusNetworkFactory {
    /// Builds a factory for `shard_id` over `transport` (plain HTTP or peer-mTLS).
    pub fn new(shard_id: String, transport: RaftTransport) -> Self {
        Self {
            shard_id,
            transport,
        }
    }
}

impl RaftNetworkFactory<NodusTypeConfig> for NodusNetworkFactory {
    type Network = NodusNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        NodusNetwork {
            shard_id: self.shard_id.clone(),
            target,
            target_node: node.clone(),
            client: self.transport.client.clone(),
            scheme: self.transport.scheme.clone(),
        }
    }
}
