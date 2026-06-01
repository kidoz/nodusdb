//! Admin HTTP API (`/api/v1/...`) backed by shared runtime handles.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post},
};
use nodus_audit::{AuditEvent, AuditQuery, AuditQueryable, MemoryAuditSink};
use nodus_catalog::PrincipalId;
use nodus_security::{SessionInfo, SessionRegistry};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Shared handles the admin endpoints operate on. Grows as more control-plane
/// surfaces are wired in.
#[derive(Clone)]
pub struct AdminState {
    pub registry: Arc<SessionRegistry>,
    pub audit: Arc<MemoryAuditSink>,
}

pub fn admin_routes(state: AdminState) -> Router {
    Router::new()
        .route("/api/v1/sessions", get(list_sessions))
        .route("/api/v1/sessions/:id/kill", post(kill_session))
        .route("/api/v1/audit", get(query_audit))
        .with_state(state)
}

async fn list_sessions(State(state): State<AdminState>) -> Json<Vec<SessionInfo>> {
    Json(state.registry.list())
}

async fn kill_session(State(state): State<AdminState>, Path(id): Path<String>) -> Json<bool> {
    Json(state.registry.kill(&id))
}

async fn query_audit(
    State(state): State<AdminState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Vec<AuditEvent>> {
    let query = AuditQuery {
        actor: params
            .get("actor")
            .and_then(|s| Uuid::parse_str(s).ok())
            .map(PrincipalId),
        action: params.get("action").cloned(),
        result: params.get("result").cloned(),
        since: None,
        until: None,
        limit: params.get("limit").and_then(|s| s.parse().ok()),
    };
    Json(state.audit.query(&query).unwrap_or_default())
}
