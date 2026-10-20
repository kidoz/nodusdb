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
    admission_source: Option<Arc<dyn nodus_storage_api::KvEngine>>,
    /// Bearer token attached to every outbound Raft HTTP RPC. Peers serving the
    /// routes on their public listener require it (the admin token doubles as
    /// the cluster's shared secret); mTLS-only peer listeners ignore it.
    auth_token: Option<Arc<str>>,
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

    /// Physical storage carries the cluster admission floor for every group.
    pub fn with_admission_source(mut self, kv: Arc<dyn nodus_storage_api::KvEngine>) -> Self {
        self.admission_source = Some(kv);
        self
    }

    /// Attach a bearer token to every outbound Raft RPC (see the field docs).
    pub fn with_auth_token(mut self, token: Option<String>) -> Self {
        self.auth_token = token.map(Into::into);
        self
    }

    /// Applies the configured bearer credential to an outbound request.
    fn authorize(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth_token {
            Some(token) => req.bearer_auth(token),
            None => req,
        }
    }
    fn requires_admission_reader(&self) -> anyhow::Result<bool> {
        self.admission_source.as_ref().map_or(Ok(false), |kv| {
            Ok(crate::upgrade::admission::read(kv.as_ref())?.is_some())
        })
    }
    async fn probe_admission(&self, node: u64, address: &str) -> anyhow::Result<()> {
        let report = self
            .probe(node, address, uuid::Uuid::new_v4(), false)
            .await?;
        crate::upgrade::admission::require_reader(&report)
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
            .authorize(
                self.client()
                    .post(format!("https://{addr}/raft/capabilities/v1")),
            )
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
            admission_source: None,
            auth_token: None,
        }
    }

    /// Transport over a caller-built client (e.g. one carrying a client identity
    /// and peer-CA trust) with the given scheme (`http` or `https`).
    pub fn new(client: reqwest::Client, scheme: impl Into<Arc<str>>) -> Self {
        Self {
            client: RaftClient::Ready(client),
            scheme: scheme.into(),
            check_snapshots: false,
            admission_source: None,
            auth_token: None,
        }
    }

    /// Asks the node at `addr` for `group_id`'s read log index over the
    /// transport's own scheme and credentials (plain HTTP + bearer, or mTLS).
    /// `Ok(None)` means the leader's log is empty; transport errors and
    /// non-success statuses (unknown group, not the leader) are `Err`.
    pub async fn read_index(&self, addr: &str, group_id: &str) -> anyhow::Result<Option<u64>> {
        let response = self
            .authorize(self.client().post(format!(
                "{}://{addr}/raft/{group_id}/read_index",
                self.scheme()
            )))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("read-index to {addr}: {e}"))?;
        anyhow::ensure!(
            response.status().is_success(),
            "leader {addr} could not serve read-index: {}",
            response.status()
        );
        let body: crate::ReadIndexResponse = response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("parse read-index response from {addr}: {e}"))?;
        Ok(body.index)
    }

    /// Asks the node at `addr` for `group_id`'s leader read index. Succeeds
    /// only when the group is hosted there and currently has a leader — used
    /// to verify a remotely placed destination group is serving before a
    /// migration publishes routing to it.
    pub async fn group_read_index(&self, addr: &str, group_id: &str) -> anyhow::Result<u64> {
        self.read_index(addr, group_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("group {group_id} on {addr} has no committed log yet"))
    }

    /// The URL scheme RPCs are issued over (`http` or `https`).
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Forwards a client write to the leader-side `/raft/{group}/write`
    /// endpoint at `addr` over the transport's own scheme and credentials,
    /// tagged with `request_id` so the leader can dedup a retried forward.
    /// Ordinary commands use this; migration-protocol commands must go through
    /// [`Self::forward_write`], which additionally enforces mTLS.
    pub async fn forward_command(
        &self,
        addr: &str,
        group_id: &str,
        cmd: &crate::ShardCommand,
        request_id: &str,
    ) -> anyhow::Result<crate::ShardResponse> {
        let response = self
            .authorize(
                self.client()
                    .post(format!("{}://{addr}/raft/{group_id}/write", self.scheme()))
                    .header(crate::server::REQUEST_ID_HEADER, request_id)
                    .json(cmd),
            )
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("forward to {addr}: {e}"))?;
        if response.status().is_success() {
            // The leader returns the applied ShardResponse so a forwarded
            // prepare's vote reaches the coordinator.
            response
                .json::<crate::ShardResponse>()
                .await
                .map_err(|e| anyhow::anyhow!("parse forwarded write response from {addr}: {e}"))
        } else {
            Err(anyhow::anyhow!(
                "leader {addr} rejected forwarded write: {}",
                response.status()
            ))
        }
    }

    /// Forwards a migration-protocol command to the leader over the peer
    /// transport. Hard-requires the mTLS `https` scheme: migration-protocol
    /// commands never travel the shared bearer-token listener. Pair with
    /// [`Self::probe`] so the recipient's reported capabilities are re-verified
    /// before sending.
    pub async fn forward_write(
        &self,
        addr: &str,
        group_id: &str,
        cmd: &crate::ShardCommand,
        request_id: &str,
    ) -> anyhow::Result<crate::ShardResponse> {
        anyhow::ensure!(
            self.scheme() == "https",
            "migration-protocol forwarding requires peer mTLS"
        );
        self.forward_command(addr, group_id, cmd, request_id).await
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
        let admission_command = rpc.entries.iter().any(|entry| {
            matches!(
                &entry.payload,
                openraft::EntryPayload::Normal(crate::ShardCommand::UpgradeAdmissionV1(_))
            )
        });
        let needs_admission = self.transport.requires_admission_reader().map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
        })?;
        if admission_command || needs_admission {
            self.transport
                .probe_admission(self.target, &self.target_node.addr)
                .await
                .map_err(|e| {
                    RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
                })?;
        }
        let url = format!(
            "{}://{}/raft/{}/append",
            self.scheme, self.target_node.addr, self.shard_id
        );
        let resp = self
            .transport
            .authorize(self.client.post(url))
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
                let report = self
                    .transport
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
                let required = self.transport.requires_admission_reader().map_err(|e| {
                    RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
                })?;
                if required {
                    crate::upgrade::admission::require_reader(&report).map_err(|e| {
                        RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
                    })?;
                }
            }
        }
        let url = format!(
            "{}://{}/raft/{}/snapshot",
            self.scheme, self.target_node.addr, self.shard_id
        );
        let resp = self
            .transport
            .authorize(self.client.post(url))
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
            .transport
            .authorize(self.client.post(url))
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
