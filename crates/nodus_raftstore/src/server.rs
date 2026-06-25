use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use openraft::Raft;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{NodusTypeConfig, ShardCommand};

pub type NodusRaft = Raft<NodusTypeConfig>;

// Define a struct to hold our Raft instances so they can be passed via Axum State.
#[derive(Clone)]
pub struct RaftState {
    pub rafts: Arc<RwLock<HashMap<String, NodusRaft>>>,
}

impl RaftState {
    pub fn new() -> Self {
        Self {
            rafts: Arc::new(RwLock::new(HashMap::new())),
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
    Json(cmd): Json<ShardCommand>,
) -> impl axum::response::IntoResponse {
    let Some(raft) = get_raft(&state, &shard_id).await else {
        return (axum::http::StatusCode::NOT_FOUND, "Shard not found").into_response();
    };
    match raft.client_write(cmd).await {
        Ok(_) => axum::http::StatusCode::OK.into_response(),
        Err(e) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    }
}
