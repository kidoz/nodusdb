use crate::NodusTypeConfig;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::sync::{Arc, OnceLock};

/// The HTTP client and URL scheme used for inter-node Raft RPCs. Plain HTTP by
/// default; a TLS-configured client with `https` is supplied when inter-node
/// mTLS is enabled, so the same RPC code path serves both.
#[derive(Clone)]
pub struct RaftTransport {
    client: RaftClient,
    scheme: Arc<str>,
    check_snapshots: bool,
}

/// The outbound HTTP client behind a [`RaftTransport`].
#[derive(Clone)]
enum RaftClient {
    /// A plain-HTTP client, built lazily on the first outbound RPC. A single-node
    /// server (and every test) issues no peer RPCs at all, so the client is never
    /// constructed — which matters because `reqwest::Client::new()` performs a
    /// one-time TLS-stack initialization that, on some platforms (notably macOS,
    /// where it reaches the keychain/Gatekeeper through the system trust store),
    /// can block for many seconds. Deferring it keeps that cost off the startup
    /// path until a peer is actually contacted.
    Lazy(Arc<OnceLock<reqwest::Client>>),
    /// A caller-supplied client (e.g. one carrying a peer-mTLS identity and CA
    /// trust), built eagerly by the caller.
    Ready(reqwest::Client),
}

impl RaftTransport {
    /// Production transports re-probe the recipient before every v2 chunk,
    /// including cached snapshots and resumed transfers.
    pub fn with_snapshot_checks(mut self) -> Self {
        self.check_snapshots = true;
        self
    }

    pub async fn probe(
        &self,
        node: u64,
        addr: &str,
        challenge: uuid::Uuid,
        preflight: bool,
    ) -> anyhow::Result<crate::upgrade::CapabilityV1> {
        anyhow::ensure!(
            self.scheme() == "https",
            "remote upgrade verification requires peer mTLS"
        );
        let mut response = self
            .client()
            .post(format!("https://{addr}/raft/capabilities/v1"))
            .timeout(std::time::Duration::from_secs(3))
            .json(&crate::upgrade::ProbeV1 {
                challenge,
                preflight,
            })
            .send()
            .await?
            .error_for_status()?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            anyhow::ensure!(
                bytes.len() + chunk.len() <= 8192,
                "capability response too large"
            );
            bytes.extend_from_slice(&chunk);
        }
        let report: crate::upgrade::CapabilityV1 = serde_json::from_slice(&bytes)?;
        report.validate(node, challenge)?;
        Ok(report)
    }

    /// Plain-HTTP transport (no peer TLS). The HTTP client is built on first use.
    pub fn plain() -> Self {
        Self {
            client: RaftClient::Lazy(Arc::new(OnceLock::new())),
            scheme: Arc::from("http"),
            check_snapshots: false,
        }
    }

    /// Transport over a caller-built client (e.g. one carrying a client identity
    /// and peer-CA trust) with the given scheme (`http` or `https`).
    pub fn new(client: reqwest::Client, scheme: impl Into<Arc<str>>) -> Self {
        Self {
            client: RaftClient::Ready(client),
            scheme: scheme.into(),
            check_snapshots: false,
        }
    }

    /// The URL scheme RPCs are issued over (`http` or `https`).
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// The HTTP client for outbound RPCs, building the lazy plain-HTTP client on
    /// first access and reusing it thereafter.
    fn client(&self) -> reqwest::Client {
        match &self.client {
            RaftClient::Ready(client) => client.clone(),
            RaftClient::Lazy(cell) => cell.get_or_init(reqwest::Client::new).clone(),
        }
    }
}

impl Default for RaftTransport {
    fn default() -> Self {
        Self::plain()
    }
}

pub struct NodusNetwork {
    transport: RaftTransport,
    snapshot_format: Option<(String, bool)>,
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
        if self.transport.check_snapshots {
            if rpc.offset == 0 {
                self.snapshot_format = Some((
                    rpc.meta.snapshot_id.clone(),
                    !rpc.data.starts_with(b"NSNP\0\x01"),
                ));
            }
            // An unknown resumed stream is conservatively treated as v2.
            let needs_probe = self
                .snapshot_format
                .as_ref()
                .filter(|(id, _)| id == &rpc.meta.snapshot_id)
                .is_none_or(|(_, v2)| *v2);
            if needs_probe {
                self.transport
                    .probe(
                        self.target,
                        &self.target_node.addr,
                        uuid::Uuid::new_v4(),
                        false,
                    )
                    .await
                    .map_err(|e| {
                        RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
                    })?;
            }
        }
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
            transport: self.transport.clone(),
            snapshot_format: None,
            shard_id: self.shard_id.clone(),
            target,
            target_node: node.clone(),
            client: self.transport.client(),
            scheme: self.transport.scheme.clone(),
        }
    }
}
