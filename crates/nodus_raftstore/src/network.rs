use crate::NodusTypeConfig;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

pub struct NodusNetwork {
    target: u64,
    target_node: BasicNode,
    client: reqwest::Client,
}

impl RaftNetwork<NodusTypeConfig> for NodusNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<NodusTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let url = format!("http://{}/raft/append", self.target_node.addr);
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
        let url = format!("http://{}/raft/snapshot", self.target_node.addr);
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
        let url = format!("http://{}/raft/vote", self.target_node.addr);
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
    client: reqwest::Client,
}

impl NodusNetworkFactory {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for NodusNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<NodusTypeConfig> for NodusNetworkFactory {
    type Network = NodusNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        NodusNetwork {
            target,
            target_node: node.clone(),
            client: self.client.clone(),
        }
    }
}
