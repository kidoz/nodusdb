use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use openraft::Raft;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

use crate::NodusTypeConfig;

pub type NodusRaft = Raft<NodusTypeConfig>;

// Define a struct to hold our Raft instance so it can be passed via Axum State.
#[derive(Clone)]
pub struct RaftState {
    pub raft: NodusRaft,
}

pub fn raft_routes() -> Router<RaftState> {
    Router::new()
        .route("/raft/vote", post(vote))
        .route("/raft/append", post(append))
        .route("/raft/snapshot", post(snapshot))
}

async fn vote(
    State(state): State<RaftState>,
    Json(req): Json<VoteRequest<u64>>,
) -> impl axum::response::IntoResponse {
    let res = state.raft.vote(req).await;
    Json(res)
}

async fn append(
    State(state): State<RaftState>,
    Json(req): Json<AppendEntriesRequest<NodusTypeConfig>>,
) -> impl axum::response::IntoResponse {
    let res = state.raft.append_entries(req).await;
    Json(res)
}

async fn snapshot(
    State(state): State<RaftState>,
    Json(req): Json<InstallSnapshotRequest<NodusTypeConfig>>,
) -> impl axum::response::IntoResponse {
    let res = state.raft.install_snapshot(req).await;
    Json(res)
}
