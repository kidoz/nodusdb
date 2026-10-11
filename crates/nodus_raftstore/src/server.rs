use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use openraft::Raft;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

use crate::{NodusTypeConfig, ReadIndexResponse, ShardCommand, ShardResponse};

pub type NodusRaft = Raft<NodusTypeConfig>;

/// Header carrying a forwarded write's stable request id, so a retried forward
/// (whose previous response was lost in flight) is recognized on the leader.
pub const REQUEST_ID_HEADER: &str = "x-nodus-request-id";

/// How many recent forwarded-write request ids the leader remembers.
const REQUEST_DEDUP_CAPACITY: usize = 4096;

/// Bounded, in-memory set of recently-applied forwarded-write request ids. It
/// lets a leader recognize a retried forward and answer it *without* submitting
/// a second Raft log entry for the same logical write. It is best-effort: a
/// leader change or restart drops the set, after which the state machine's
/// command idempotency (a re-applied intent/commit is a no-op) is the
/// correctness backstop.
#[derive(Default)]
pub struct RequestDedup {
    order: VecDeque<String>,
    seen: HashSet<String>,
}

impl RequestDedup {
    fn contains(&self, id: &str) -> bool {
        self.seen.contains(id)
    }

    fn record(&mut self, id: String) {
        if self.seen.insert(id.clone()) {
            self.order.push_back(id);
            if self.order.len() > REQUEST_DEDUP_CAPACITY
                && let Some(evicted) = self.order.pop_front()
            {
                self.seen.remove(&evicted);
            }
        }
    }
}

// Define a struct to hold our Raft instances so they can be passed via Axum State.
#[derive(Clone)]
pub struct RaftState {
    pub rafts: Arc<RwLock<HashMap<String, NodusRaft>>>,
    /// Recently-applied forwarded-write request ids (see [`RequestDedup`]).
    dedup: Arc<Mutex<RequestDedup>>,
    node_id: Arc<std::sync::atomic::AtomicU64>,
    capability_kv: Arc<std::sync::OnceLock<Arc<dyn nodus_storage_api::KvEngine>>>,
}

impl RaftState {
    pub fn set_capability_storage(&self, kv: Arc<dyn nodus_storage_api::KvEngine>) {
        let _ = self.capability_kv.set(kv);
    }
    pub fn set_node_id(&self, id: u64) {
        self.node_id.store(id, std::sync::atomic::Ordering::Release);
    }
    pub fn new() -> Self {
        Self {
            rafts: Arc::new(RwLock::new(HashMap::new())),
            dedup: Arc::new(Mutex::new(RequestDedup::default())),
            node_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            capability_kv: Arc::new(std::sync::OnceLock::new()),
        }
    }
}

impl Default for RaftState {
    fn default() -> Self {
        Self::new()
    }
}

/// How the Raft HTTP endpoints (`/raft/...`) authenticate callers. These
/// endpoints carry inter-node consensus RPCs and forwarded client writes —
/// including shard-map and transaction commands — so they must never be
/// reachable anonymously on a listener that untrusted clients can reach.
#[derive(Clone)]
pub enum RaftRouteAuth {
    /// The routes share a listener with admin/web traffic. Callers must present
    /// `Authorization: Bearer <token>` matching the configured admin token —
    /// the same credential the admin API accepts — so internode and forwarded
    /// writes are authenticated with the cluster's shared secret. When no token
    /// is configured, anonymous access is permitted only with the same explicit
    /// `admin.allow_insecure` opt-in that opens the admin API; otherwise every
    /// request is rejected (mirroring the admin API, which is also unusable in
    /// that configuration, so no working deployment regressions).
    AdminToken {
        token: Option<Arc<str>>,
        allow_insecure: bool,
    },
    /// The routes are served on a dedicated peer listener whose transport
    /// already authenticates callers: `cluster.tls` mandates a client
    /// certificate signed by the cluster CA, so the TLS handshake itself is the
    /// peer credential. Bearer tokens are not required here — mixed-version
    /// clusters during a rolling upgrade include older binaries that present
    /// their certificate but no token, and rejecting them would break Raft
    /// mid-upgrade.
    TransportAuthenticated,
}

/// Constant-time bearer-token comparison (avoids timing oracles on the token).
fn bearer_token_matches(provided: &str, expected: &str) -> bool {
    let (a, b) = (provided.as_bytes(), expected.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn require_raft_auth(
    State(auth): State<RaftRouteAuth>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    match &auth {
        RaftRouteAuth::TransportAuthenticated => Ok(next.run(req).await),
        RaftRouteAuth::AdminToken {
            token,
            allow_insecure,
        } => {
            if let Some(expected) = token {
                let ok = req
                    .headers()
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|h| h.to_str().ok())
                    .and_then(|h| h.strip_prefix("Bearer "))
                    .is_some_and(|bearer| bearer_token_matches(bearer, expected));
                if ok {
                    Ok(next.run(req).await)
                } else {
                    Err(StatusCode::UNAUTHORIZED)
                }
            } else if *allow_insecure {
                Ok(next.run(req).await)
            } else {
                Err(StatusCode::UNAUTHORIZED)
            }
        }
    }
}

pub fn raft_routes(auth: RaftRouteAuth) -> Router<RaftState> {
    Router::new()
        .route("/raft/capabilities/v1", post(capabilities))
        .route("/raft/{shard_id}/vote", post(vote))
        .route("/raft/{shard_id}/append", post(append))
        .route("/raft/{shard_id}/snapshot", post(snapshot))
        .route("/raft/{shard_id}/write", post(write))
        .route("/raft/{shard_id}/read_index", post(read_index))
        .route_layer(axum::middleware::from_fn_with_state(
            auth,
            require_raft_auth,
        ))
}

async fn capabilities(
    State(state): State<RaftState>,
    Json(request): Json<crate::upgrade::ProbeV1>,
) -> impl IntoResponse {
    let mut report = crate::upgrade::CapabilityV1::local(
        state.node_id.load(std::sync::atomic::Ordering::Acquire),
        request.challenge,
    );
    if !request.preflight {
        return Json(report).into_response();
    }
    let Some(kv) = state.capability_kv.get().cloned() else {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "capability storage unavailable",
        )
            .into_response();
    };
    match tokio::task::spawn_blocking(move || crate::upgrade::preflight(kv.as_ref())).await {
        Ok(Ok(ready)) => {
            report.ready_for_finalize = ready;
            Json(report).into_response()
        }
        error => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!("capability preflight failed: {error:?}"),
        )
            .into_response(),
    }
}

async fn get_raft(state: &RaftState, shard_id: &str) -> Option<NodusRaft> {
    state.rafts.read().await.get(shard_id).cloned()
}

async fn vote(
    Path(shard_id): Path<String>,
    State(state): State<RaftState>,
    Json(req): Json<VoteRequest<u64>>,
) -> impl axum::response::IntoResponse {
    if let Some(raft) = get_raft(&state, &shard_id).await {
        let res = raft.vote(req).await;
        Json(res).into_response()
    } else {
        (axum::http::StatusCode::NOT_FOUND, "Shard not found").into_response()
    }
}

async fn append(
    Path(shard_id): Path<String>,
    State(state): State<RaftState>,
    Json(req): Json<AppendEntriesRequest<NodusTypeConfig>>,
) -> impl axum::response::IntoResponse {
    if let Some(raft) = get_raft(&state, &shard_id).await {
        let res = raft.append_entries(req).await;
        Json(res).into_response()
    } else {
        (axum::http::StatusCode::NOT_FOUND, "Shard not found").into_response()
    }
}

async fn snapshot(
    Path(shard_id): Path<String>,
    State(state): State<RaftState>,
    Json(req): Json<InstallSnapshotRequest<NodusTypeConfig>>,
) -> impl axum::response::IntoResponse {
    if let Some(raft) = get_raft(&state, &shard_id).await {
        let res = raft.install_snapshot(req).await;
        Json(res).into_response()
    } else {
        (axum::http::StatusCode::NOT_FOUND, "Shard not found").into_response()
    }
}

/// Applies a client command on this node's replica of `shard_id` — used to
/// *forward* a write to the group's leader. The write path posts here when its
/// local `client_write` returns `ForwardToLeader`. Returns `503` (retryable)
/// when this node isn't the leader either (leadership moved again), so the
/// forwarder re-evaluates and retries.
async fn write(
    Path(shard_id): Path<String>,
    State(state): State<RaftState>,
    headers: HeaderMap,
    Json(cmd): Json<ShardCommand>,
) -> impl axum::response::IntoResponse {
    if cmd.is_upgrade_command() {
        return (
            axum::http::StatusCode::FORBIDDEN,
            "upgrade commands require direct authenticated leader verification",
        )
            .into_response();
    }
    if cmd.requires_migration_protocol() {
        return (
            axum::http::StatusCode::NOT_IMPLEMENTED,
            "migration protocol activation requires verified cluster-wide compatibility",
        )
            .into_response();
    }
    let Some(raft) = get_raft(&state, &shard_id).await else {
        return (axum::http::StatusCode::NOT_FOUND, "Shard not found").into_response();
    };
    let request_id = headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|id| id.to_string());

    // `PrepareTxn` changes no state and its vote reflects the *current* intents,
    // so it is never deduped — a retried prepare must re-vote, not get a stale
    // ack. All other commands keep dedup to avoid a duplicate Raft log entry.
    let is_prepare = matches!(cmd, ShardCommand::PrepareTxn { .. });

    // A retried forward (the original's response was lost) carries the same
    // request id; if we already applied it, ack without writing a duplicate
    // entry to the Raft log.
    if !is_prepare
        && let Some(id) = &request_id
        && state.dedup.lock().unwrap().contains(id)
    {
        return axum::Json(ShardResponse {
            success: true,
            error: None,
        })
        .into_response();
    }

    match raft.client_write(cmd).await {
        Ok(resp) => {
            if resp.data.success
                && !is_prepare
                && let Some(id) = request_id
            {
                state.dedup.lock().unwrap().record(id);
            }
            // Return the applied response so a forwarded prepare's vote (and any
            // other applied result) reaches the forwarding node.
            axum::Json(resp.data).into_response()
        }
        Err(e) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

/// Returns the leader's read log index for `shard_id`, used by a follower to
/// satisfy a linearizable read. `get_read_log_id` confirms this node is still
/// the leader by heartbeating a quorum before answering, so the index it returns
/// is a safe linearization point. A non-leader (or a node that can't confirm
/// leadership) answers `503` so the caller re-resolves the leader and retries.
async fn read_index(
    Path(shard_id): Path<String>,
    State(state): State<RaftState>,
) -> impl axum::response::IntoResponse {
    let Some(raft) = get_raft(&state, &shard_id).await else {
        return (axum::http::StatusCode::NOT_FOUND, "Shard not found").into_response();
    };
    match raft.get_read_log_id().await {
        Ok((read_log_id, _applied)) => axum::Json(ReadIndexResponse {
            index: read_log_id.map(|l| l.index),
        })
        .into_response(),
        Err(e) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_dedup_remembers_and_bounds() {
        let mut dedup = RequestDedup::default();
        dedup.record("a".to_string());
        assert!(dedup.contains("a"));
        assert!(!dedup.contains("b"));
        // Recording the same id twice is idempotent.
        dedup.record("a".to_string());
        assert_eq!(dedup.order.len(), 1);

        // The set is bounded: the oldest entries are evicted past capacity.
        for i in 0..REQUEST_DEDUP_CAPACITY {
            dedup.record(format!("k{i}"));
        }
        assert!(dedup.order.len() <= REQUEST_DEDUP_CAPACITY);
        assert!(!dedup.contains("a"), "oldest id evicted once over capacity");
    }
}
