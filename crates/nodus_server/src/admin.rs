//! Admin HTTP API (`/api/v1/...`) backed by shared runtime handles.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post},
};
use nodus_audit::{AuditEvent, AuditQuery, AuditQueryable, MemoryAuditSink};
use nodus_authz::{Action, AuthzContext, AuthzEngine, AuthzExplanation, AuthzRequest};
use nodus_catalog::{CatalogReader, PrincipalId, ResourceRef};
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
    pub authz: Arc<dyn AuthzEngine>,
    pub catalog: Arc<dyn CatalogReader>,
}

pub fn admin_routes(state: AdminState) -> Router {
    Router::new()
        .route("/api/v1/sessions", get(list_sessions))
        .route("/api/v1/sessions/:id/kill", post(kill_session))
        .route("/api/v1/audit", get(query_audit))
        .route("/api/v1/authz/explain", get(explain_authz))
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

/// `GET /api/v1/authz/explain?principal=<uuid>&action=SELECT&table=<name>`
/// Returns the step-by-step authorization decision. Omit `table` to check a
/// `System`-level action.
async fn explain_authz(
    State(state): State<AdminState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<AuthzExplanation> {
    let principal_id = params
        .get("principal")
        .and_then(|s| Uuid::parse_str(s).ok())
        .map(PrincipalId)
        .unwrap_or_default();

    let action = params
        .get("action")
        .and_then(|s| Action::from_privilege(s))
        .unwrap_or(Action::Select);

    // Resolve the resource: a named table (in default/public) or System.
    let resource = match params.get("table") {
        Some(name) => match state.catalog.get_table("default", "public", name) {
            Ok(tbl) => ResourceRef::Table(tbl.id),
            Err(_) => {
                return Json(AuthzExplanation {
                    is_allowed: false,
                    steps: vec![format!("Table '{name}' not found in default.public.")],
                });
            }
        },
        None => ResourceRef::System,
    };

    let explanation = state
        .authz
        .explain(AuthzRequest {
            principal_id,
            active_roles: vec![],
            action,
            resource,
            context: AuthzContext { database_id: None },
        })
        .unwrap_or(AuthzExplanation {
            is_allowed: false,
            steps: vec!["authorization engine error".into()],
        });
    Json(explanation)
}
