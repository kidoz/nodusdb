use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use openraft::Raft;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

use crate::{NodusTypeConfig, ShardCommand};

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
}

impl RaftState {
    pub fn new() -> Self {
        Self {
            rafts: Arc::new(RwLock::new(HashMap::new())),
            dedup: Arc::new(Mutex::new(RequestDedup::default())),
        }
    }
}

impl Default for RaftState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn raft_routes() -> Router<RaftState> {
    Router::new()
        .route("/raft/:shard_id/vote", post(vote))
        .route("/raft/:shard_id/append", post(append))
        .route("/raft/:shard_id/snapshot", post(snapshot))
        .route("/raft/:shard_id/write", post(write))
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
    let Some(raft) = get_raft(&state, &shard_id).await else {
        return (axum::http::StatusCode::NOT_FOUND, "Shard not found").into_response();
    };
    let request_id = headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|id| id.to_string());

    // A retried forward (the original's response was lost) carries the same
    // request id; if we already applied it, ack without writing a duplicate
    // entry to the Raft log.
    if let Some(id) = &request_id
        && state.dedup.lock().unwrap().contains(id)
    {
        return axum::http::StatusCode::OK.into_response();
    }

    match raft.client_write(cmd).await {
        Ok(_) => {
            if let Some(id) = request_id {
                state.dedup.lock().unwrap().record(id);
            }
            axum::http::StatusCode::OK.into_response()
        }
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
