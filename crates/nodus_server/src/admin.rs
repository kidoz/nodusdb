//! Admin HTTP API (`/api/v1/...`) backed by shared runtime handles.

use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use nodus_security::{SessionInfo, SessionRegistry};
use std::sync::Arc;

/// Shared handles the admin endpoints operate on. Grows as more control-plane
/// surfaces are wired in.
#[derive(Clone)]
pub struct AdminState {
    pub registry: Arc<SessionRegistry>,
}

pub fn admin_routes(state: AdminState) -> Router {
    Router::new()
        .route("/api/v1/sessions", get(list_sessions))
        .route("/api/v1/sessions/:id/kill", post(kill_session))
        .with_state(state)
}

async fn list_sessions(State(state): State<AdminState>) -> Json<Vec<SessionInfo>> {
    Json(state.registry.list())
}

async fn kill_session(State(state): State<AdminState>, Path(id): Path<String>) -> Json<bool> {
    Json(state.registry.kill(&id))
}
