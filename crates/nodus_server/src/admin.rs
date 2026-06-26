#![allow(clippy::collapsible_if)]
//! Admin HTTP API (`/api/v1/...`) backed by shared runtime handles.

use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{Next, from_fn_with_state},
    response::Response,
    routing::{get, post},
};
use base64::Engine;
use bytes::Bytes;
use nodus_audit::{AuditEvent, AuditQuery, AuditQueryable};
use nodus_authz::{Action, AuthzContext, AuthzEngine, AuthzExplanation, AuthzRequest};
use nodus_backup::{BackupObject, BackupOrchestrator};
use nodus_catalog::{
    CatalogReader, CatalogSnapshot, CatalogWriter, PrincipalId, ResourceRef, ShardId, TableId,
};
use nodus_monitoring::{SlowQuery, SlowQueryLog};
use nodus_security::{SessionInfo, SessionRegistry};
use nodus_sharding::ShardOrchestrator;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

/// Shared handles the admin endpoints operate on. Grows as more control-plane
/// surfaces are wired in.
use nodus_security::{Authenticator, PasswordAuthenticator};

#[derive(Clone)]
pub struct AdminState {
    pub registry: Arc<SessionRegistry>,
    pub audit: Arc<dyn AuditQueryable>,
    pub authz: Arc<dyn AuthzEngine>,
    pub catalog: Arc<dyn CatalogReader>,
    pub catalog_writer: Arc<dyn CatalogWriter>,
    pub backup: Arc<BackupOrchestrator>,
    pub upgrade: Arc<dyn nodus_upgrade::UpgradeCoordinator>,
    pub shards: Arc<ShardOrchestrator>,
    /// Hosts this node's Raft groups; reconciled after placement changes so a
    /// newly placed shard is activated without waiting for a restart.
    pub manager: Arc<crate::multi_raft::MultiRaftManager>,
    pub slow_log: Arc<SlowQueryLog>,
    pub kv: Arc<dyn nodus_storage_api::KvEngine>,
    /// In-process query executor, used by the dump-import endpoint to replay
    /// translated statements without a network round-trip.
    pub executor: Arc<dyn nodus_executor::Executor>,
    pub wal_key: Option<[u8; 32]>,
    /// Shared with `AppState`; flipping it makes `/readyz` report not-ready.
    pub draining: Arc<AtomicBool>,
    pub authenticator: Arc<PasswordAuthenticator>,
    pub admin_token: Option<String>,
    pub raft_state: nodus_raftstore::server::RaftState,
    pub membership_lock: Arc<tokio::sync::Mutex<()>>,
    /// Serializes backup restores so two never interleave their catalog/KV
    /// mutations into a corrupt result.
    pub restore_lock: Arc<tokio::sync::Mutex<()>>,
    /// Set while a restore mutates the engine; the executor rejects statements
    /// while it is set, so no query observes a partially restored state.
    pub restoring: Arc<AtomicBool>,
    /// Drain/exclusion gate shared with the executor: a restore takes the write
    /// guard to drain in-flight statements and run with exclusive access.
    pub restore_gate: Arc<std::sync::RwLock<()>>,
}

/// Rejects requests lacking a valid `Authorization: Bearer <token>` header when
/// an admin token is configured. A no-op when no token is set.
async fn require_token(
    State(state): State<AdminState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let path = req.uri().path();
    let action = match path {
        p if p.starts_with("/api/v1/sessions") => nodus_authz::Action::ManageSessions,
        p if p.starts_with("/api/v1/audit") => nodus_authz::Action::ReadAudit,
        p if p.starts_with("/api/v1/authz/explain") => nodus_authz::Action::ReadAudit,
        p if p.starts_with("/api/v1/roles") => nodus_authz::Action::ManageGrants,
        p if p.starts_with("/api/v1/grants") => nodus_authz::Action::ManageGrants,
        p if p.starts_with("/api/v1/backups") => nodus_authz::Action::ManageBackups,
        p if p.starts_with("/api/v1/import") => nodus_authz::Action::ManageBackups,
        p if p.starts_with("/api/v1/upgrade") => nodus_authz::Action::ManageUpgrades,
        p if p.starts_with("/api/v1/shards") => nodus_authz::Action::ManageShards,
        p if p.starts_with("/api/v1/catalog") => nodus_authz::Action::ManageShards,
        p if p.starts_with("/api/v1/node") => nodus_authz::Action::ManageNode,
        p if p.starts_with("/api/v1/cluster") => nodus_authz::Action::ManageCluster,
        p if p.starts_with("/api/v1/queries") => nodus_authz::Action::ReadAudit,
        _ => return Err(StatusCode::FORBIDDEN),
    };

    let principal_id = if let Some(auth) = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    {
        if let Some(basic) = auth.strip_prefix("Basic ") {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(basic)
                .map_err(|_| StatusCode::UNAUTHORIZED)?;
            let s = String::from_utf8(decoded).map_err(|_| StatusCode::UNAUTHORIZED)?;
            let mut parts = s.splitn(2, ':');
            let user = parts.next().unwrap_or("");
            let pass = parts.next().unwrap_or("");
            match state.authenticator.authenticate(user, pass) {
                Ok(session) => session.principal_id,
                Err(_) => return Err(StatusCode::UNAUTHORIZED),
            }
        } else if let Some(bearer) = auth.strip_prefix("Bearer ") {
            if let Some(expected) = &state.admin_token {
                if bearer == expected {
                    let Ok(p) = state.catalog.get_principal_by_name("nodus") else {
                        return Err(StatusCode::UNAUTHORIZED);
                    };
                    p.id
                } else {
                    return Err(StatusCode::UNAUTHORIZED);
                }
            } else {
                return Err(StatusCode::UNAUTHORIZED);
            }
        } else {
            return Err(StatusCode::UNAUTHORIZED);
        }
    } else {
        if state.admin_token.is_none() {
            if let Ok(p) = state.catalog.get_principal_by_name("nodus") {
                p.id
            } else {
                return Err(StatusCode::UNAUTHORIZED);
            }
        } else {
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    let authz_req = nodus_authz::AuthzRequest {
        principal_id,
        active_roles: vec![],
        action: action.clone(),
        resource: nodus_catalog::ResourceRef::System,
        context: nodus_authz::AuthzContext { database_id: None },
    };

    let decision = state
        .authz
        .authorize(authz_req)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if !decision.allowed {
        tracing::debug!(
            "Authorization failed for principal {} on action {:?}",
            principal_id,
            action
        );
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(next.run(req).await)
}

pub fn admin_routes(state: AdminState) -> Router {
    Router::new()
        .route("/api/v1/sessions", get(list_sessions))
        .route("/api/v1/sessions/:id/kill", post(kill_session))
        .route("/api/v1/audit", get(query_audit))
        .route("/api/v1/authz/explain", get(explain_authz))
        .route("/api/v1/roles", get(list_roles).post(create_role))
        .route(
            "/api/v1/grants",
            get(list_grants).post(create_grant).delete(revoke_grant),
        )
        .route("/api/v1/backups", get(list_backups).post(create_backup))
        .route("/api/v1/import", post(import_dump))
        .route("/api/v1/backups/:id/verify", post(verify_backup))
        .route("/api/v1/backups/:id", axum::routing::delete(delete_backup))
        .route("/api/v1/backups/:id/restore", post(restore_backup))
        .route("/api/v1/upgrade", get(upgrade_state))
        .route("/api/v1/upgrade/start", post(upgrade_start))
        .route("/api/v1/upgrade/node-upgraded", post(upgrade_node_upgraded))
        .route("/api/v1/upgrade/finalize", post(upgrade_finalize))
        .route("/api/v1/upgrade/rollback", post(upgrade_rollback))
        .route("/api/v1/shards/:table/init", post(shards_init))
        .route("/api/v1/shards/:table", get(shards_map))
        .route("/api/v1/shards/:table/split", post(shards_split))
        .route("/api/v1/shards/:table/merge", post(shards_merge))
        .route("/api/v1/shards/:table/rebalance", post(shards_rebalance))
        .route("/api/v1/shards/:table/replica", post(shards_replica))
        .route("/api/v1/catalog/table", get(catalog_table))
        .route("/api/v1/cluster/groups", get(cluster_groups))
        .route("/api/v1/queries", get(slow_queries))
        .route("/api/v1/node/drain", post(node_drain))
        .route(
            "/api/v1/node/take-leadership/:shard_id",
            post(take_leadership),
        )
        .route("/api/v1/cluster/join", post(cluster_join))
        .route_layer(from_fn_with_state(state.clone(), require_token))
        .with_state(state)
}

#[derive(serde::Deserialize)]
pub struct JoinRequest {
    pub node_id: u64,
    pub raft_advertise_addr: String,
}

/// Adds a node to the `shard-meta` Raft group. Designed to be safe under the
/// client's retry loop:
///   - returns `503` (retryable) — never panics — when the meta group hasn't
///     been created yet or this node isn't the leader, so the joining node
///     simply tries another peer / backs off and retries;
///   - is idempotent: a node that is already a voter gets `200 joined` without
///     re-running a membership change.
async fn cluster_join(
    State(state): State<AdminState>,
    Json(req): Json<JoinRequest>,
) -> impl axum::response::IntoResponse {
    let _guard = state.membership_lock.lock().await;
    let Some(raft) = state
        .raft_state
        .rafts
        .read()
        .await
        .get("shard-meta")
        .cloned()
    else {
        // Still bootstrapping — no meta group yet. Retryable.
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "meta group not initialized yet" })),
        );
    };

    let metrics = raft.metrics().borrow().clone();
    let voters: std::collections::BTreeSet<u64> =
        metrics.membership_config.membership().voter_ids().collect();

    // Idempotent: already a member — succeed without touching membership.
    if voters.contains(&req.node_id) {
        return (
            StatusCode::OK,
            Json(json!({ "joined": true, "node_id": req.node_id, "already_member": true })),
        );
    }

    // Membership changes must run on the leader. If we're not it, tell the
    // caller to retry (it will cycle to another peer / back off).
    if metrics.current_leader != Some(metrics.id) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "not leader", "leader": metrics.current_leader })),
        );
    }

    let node = openraft::BasicNode::new(&req.raft_advertise_addr);
    if let Err(e) = raft.add_learner(req.node_id, node, true).await {
        // Transient (e.g. leadership changed mid-call) — retryable.
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": format!("add_learner failed: {}", e) })),
        );
    }

    let mut members = voters;
    members.insert(req.node_id);
    match raft.change_membership(members, true).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({ "joined": true, "node_id": req.node_id })),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": format!("change_membership failed: {}", e) })),
        ),
    }
}

async fn slow_queries(State(state): State<AdminState>) -> Json<Vec<SlowQuery>> {
    Json(state.slow_log.list())
}

/// Marks the node as draining: `/readyz` starts failing (so load balancers stop
/// new traffic) and all active sessions are cancelled.
async fn node_drain(
    headers: axum::http::HeaderMap,
    State(state): State<AdminState>,
) -> Json<Value> {
    state.draining.store(true, Ordering::Release);
    let sessions = state.registry.list();
    for s in &sessions {
        state.registry.kill(&s.session_id);
    }

    let mut transfers = 0;
    let client = reqwest::Client::new();
    let rafts = state.raft_state.rafts.read().await;

    // Grab the auth header to forward it to the peer
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    for (shard_id, raft) in rafts.iter() {
        let metrics = raft.metrics().borrow().clone();
        if metrics.state == openraft::ServerState::Leader {
            let voters: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
            for node in voters {
                if node != metrics.id {
                    if let Some(n) = metrics
                        .membership_config
                        .membership()
                        .nodes()
                        .find(|(k, _)| k == &&node)
                    {
                        let url = format!(
                            "http://{}/api/v1/node/take-leadership/{}",
                            n.1.addr, shard_id
                        );

                        let mut req = client.post(&url);
                        if let Some(auth) = auth_header {
                            req = req.header(axum::http::header::AUTHORIZATION, auth);
                        }

                        if let Ok(resp) = req.send().await {
                            if resp.status().is_success() {
                                transfers += 1;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    Json(
        json!({ "draining": true, "sessions_cancelled": sessions.len(), "leadership_transfers": transfers }),
    )
}

async fn take_leadership(
    Path(shard_id): Path<String>,
    State(state): State<AdminState>,
) -> impl axum::response::IntoResponse {
    let rafts = state.raft_state.rafts.read().await;
    if let Some(raft) = rafts.get(&shard_id) {
        if let Err(e) = raft.trigger().elect().await {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            );
        }
        (
            axum::http::StatusCode::OK,
            Json(json!({ "status": "election_triggered" })),
        )
    } else {
        (
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({ "error": "shard not found" })),
        )
    }
}

/// A session enriched with the authenticated user's name. The registry only
/// tracks the opaque `principal_id`; operators want the human-readable user, so
/// we resolve it against the catalog here (the registry has no catalog handle).
#[derive(serde::Serialize)]
struct SessionView {
    #[serde(flatten)]
    info: SessionInfo,
    /// Resolved name for `principal_id`, or `None` if the principal is anonymous
    /// (pre-authentication) or no longer in the catalog.
    user_name: Option<String>,
}

async fn list_sessions(State(state): State<AdminState>) -> Json<Vec<SessionView>> {
    let views = state
        .registry
        .list()
        .into_iter()
        .map(|info| {
            let user_name = state
                .catalog
                .get_principal_by_id(info.principal_id)
                .ok()
                .map(|p| p.name);
            SessionView { info, user_name }
        })
        .collect();
    Json(views)
}

// ---------------------------------------------------------------------------
// Role / grant management
//
// Mirrors the SQL `CREATE ROLE` / `GRANT` / `REVOKE` path through the same
// catalog writer, so operators can manage RBAC from the CLI/admin API without a
// SQL session. All routes require the `ManageGrants` privilege (see
// `require_token`).
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct CreateRoleBody {
    name: String,
}

#[derive(serde::Deserialize)]
struct GrantBody {
    principal: String,
    privilege: String,
    database: String,
    schema: String,
    table: String,
}

async fn list_roles(State(state): State<AdminState>) -> Json<Value> {
    let principals = state.catalog.list_principals().unwrap_or_default();
    let out: Vec<Value> = principals
        .iter()
        .map(|p| {
            json!({
                "id": p.id.to_string(),
                "name": p.name,
                "type": format!("{:?}", p.principal_type),
            })
        })
        .collect();
    Json(json!(out))
}

async fn create_role(
    State(state): State<AdminState>,
    Json(body): Json<CreateRoleBody>,
) -> Json<Value> {
    // Catalog writes route through Raft, whose `submit` blocks the calling
    // thread; offload it so we don't block (and panic on) the async reactor.
    let writer = state.catalog_writer.clone();
    let result = tokio::task::spawn_blocking(move || {
        writer.create_role(nodus_catalog::CreateRoleRequest {
            id: PrincipalId::new(),
            name: body.name,
            principal_type: nodus_catalog::PrincipalType::Role,
            database_id: None,
        })
    })
    .await;
    match result {
        Ok(Ok(p)) => Json(json!({ "created": true, "id": p.id.to_string(), "name": p.name })),
        Ok(Err(e)) => Json(json!({ "created": false, "error": e.to_string() })),
        Err(e) => Json(json!({ "created": false, "error": format!("task failed: {e}") })),
    }
}

async fn list_grants(State(state): State<AdminState>) -> Json<Value> {
    let grants = state.catalog.list_grants().unwrap_or_default();
    let out: Vec<Value> = grants
        .iter()
        .map(|g| {
            let principal = state
                .catalog
                .get_principal_by_id(g.principal_id)
                .map(|p| p.name)
                .unwrap_or_else(|_| g.principal_id.to_string());
            let resource = match &g.resource {
                ResourceRef::Table(id) => state
                    .catalog
                    .get_table_by_id(*id)
                    .map(|t| format!("table {}", t.name))
                    .unwrap_or_else(|_| format!("table {id}")),
                other => format!("{other:?}"),
            };
            json!({
                "id": g.id.to_string(),
                "principal": principal,
                "privilege": g.privilege,
                "resource": resource,
            })
        })
        .collect();
    Json(json!(out))
}

/// Resolves the `(principal, table)` named in a grant request to their ids.
fn resolve_grant_target(
    state: &AdminState,
    body: &GrantBody,
) -> Result<(PrincipalId, TableId), String> {
    let principal = state
        .catalog
        .get_principal_by_name(&body.principal)
        .map_err(|e| e.to_string())?;
    let table = state
        .catalog
        .get_table(&body.database, &body.schema, &body.table)
        .map_err(|e| e.to_string())?;
    Ok((principal.id, table.id))
}

async fn create_grant(State(state): State<AdminState>, Json(body): Json<GrantBody>) -> Json<Value> {
    let (principal_id, table_id) = match resolve_grant_target(&state, &body) {
        Ok(ids) => ids,
        Err(e) => return Json(json!({ "granted": false, "error": e })),
    };
    let writer = state.catalog_writer.clone();
    let privilege = body.privilege.to_uppercase();
    let result = tokio::task::spawn_blocking(move || {
        writer.grant_privileges(nodus_catalog::GrantPrivilegesRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id,
            resource: ResourceRef::Table(table_id),
            privilege,
        })
    })
    .await;
    match result {
        Ok(Ok(g)) => Json(json!({ "granted": true, "id": g.id.to_string() })),
        Ok(Err(e)) => Json(json!({ "granted": false, "error": e.to_string() })),
        Err(e) => Json(json!({ "granted": false, "error": format!("task failed: {e}") })),
    }
}

async fn revoke_grant(State(state): State<AdminState>, Json(body): Json<GrantBody>) -> Json<Value> {
    let (principal_id, table_id) = match resolve_grant_target(&state, &body) {
        Ok(ids) => ids,
        Err(e) => return Json(json!({ "revoked": false, "error": e })),
    };
    let writer = state.catalog_writer.clone();
    let privilege = body.privilege.to_uppercase();
    let result = tokio::task::spawn_blocking(move || {
        writer.revoke_privileges(nodus_catalog::RevokePrivilegesRequest {
            principal_id,
            resource: ResourceRef::Table(table_id),
            privilege,
        })
    })
    .await;
    match result {
        Ok(Ok(())) => Json(json!({ "revoked": true })),
        Ok(Err(e)) => Json(json!({ "revoked": false, "error": e.to_string() })),
        Err(e) => Json(json!({ "revoked": false, "error": format!("task failed: {e}") })),
    }
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

async fn list_backups(State(state): State<AdminState>) -> Json<Vec<String>> {
    Json(state.backup.list_backups().await.unwrap_or_default())
}

#[derive(serde::Deserialize)]
pub struct CreateBackupQuery {
    pub parent_backup_id: Option<String>,
}

/// Creates a full backup capturing a catalog snapshot and the audit trail.
async fn create_backup(
    State(state): State<AdminState>,
    Query(query): Query<CreateBackupQuery>,
) -> Json<Value> {
    let version = state
        .catalog
        .get_cluster_version()
        .map(|v| v.active_version)
        .unwrap_or(0);
    let parent_snapshot_ts = if let Some(parent_id) = query.parent_backup_id.as_ref() {
        match state.backup.load_manifest(parent_id).await {
            Ok(manifest) => Some(manifest.snapshot_ts),
            Err(e) => {
                return Json(json!({
                    "error": format!("Failed to load parent backup {parent_id}: {e}")
                }));
            }
        }
    } else {
        None
    };

    let snapshot = state.catalog.export_snapshot();
    let catalog_bytes = match serde_json::to_vec(&snapshot) {
        Ok(b) => b,
        Err(e) => return Json(json!({ "error": format!("Failed to serialize catalog: {e}") })),
    };

    let audit_events = match state.audit.query(&AuditQuery::default()) {
        Ok(events) => events,
        Err(e) => return Json(json!({ "error": format!("Failed to query audit: {e}") })),
    };
    let audit_bytes = match serde_json::to_vec(&audit_events) {
        Ok(b) => b,
        Err(e) => return Json(json!({ "error": format!("Failed to serialize audit: {e}") })),
    };

    let mut kv_dump = Vec::new();
    let mut max_kv_version = 0;
    let range = nodus_storage_api::KeyRange {
        start: Bytes::new(),
        end: Bytes::from(vec![255u8; 1024]),
    };

    if let Some(parent_ts) = parent_snapshot_ts {
        match state.kv.scan_versions(range, parent_ts, u64::MAX) {
            Ok(iter) => {
                for version_res in iter {
                    match version_res {
                        Ok(version) => {
                            // The HLC watermark is a clock *reservation* committed
                            // ahead of wall time (up to RESERVATION_WINDOW), so it
                            // must be backed up but must not define the snapshot's
                            // logical time — otherwise `snapshot_ts` lands in the
                            // future and PITR can't select this backup for a
                            // wall-clock `target_ts`.
                            if version.key.as_ref() != crate::HLC_WATERMARK_KEY {
                                max_kv_version = max_kv_version.max(version.version);
                            }
                            kv_dump.push(json!({
                                "key": version.key.to_vec(),
                                "value": version.value.as_ref().map(|value| value.to_vec()),
                                "deleted": version.value.is_none(),
                                "version": version.version,
                            }));
                        }
                        Err(e) => {
                            return Json(json!({
                                "error": format!("Failed to scan KV version: {e}")
                            }));
                        }
                    }
                }
            }
            Err(e) => {
                return Json(json!({
                    "error": format!("Failed to start KV version scan: {e}")
                }));
            }
        }
    } else {
        match state.kv.scan(range, u64::MAX) {
            Ok(iter) => {
                for pair_res in iter {
                    match pair_res {
                        Ok(pair) => {
                            // See the version-scan branch: exclude the HLC
                            // watermark reservation from the snapshot's logical
                            // time while still backing the key up.
                            if pair.key.as_ref() != crate::HLC_WATERMARK_KEY {
                                max_kv_version = max_kv_version.max(pair.version);
                            }
                            kv_dump.push(json!({
                                "key": pair.key.to_vec(),
                                "value": pair.value.to_vec(),
                                "deleted": false,
                                "version": pair.version,
                            }));
                        }
                        Err(e) => {
                            return Json(json!({
                                "error": format!("Failed to scan KV pair: {e}")
                            }));
                        }
                    }
                }
            }
            Err(e) => return Json(json!({ "error": format!("Failed to start KV scan: {e}") })),
        }
    }

    let kv_bytes = match serde_json::to_vec(&kv_dump) {
        Ok(b) => b,
        Err(e) => return Json(json!({ "error": format!("Failed to serialize KV dump: {e}") })),
    };

    let objects = vec![
        BackupObject {
            name: "catalog.json".into(),
            bytes: Bytes::from(catalog_bytes),
        },
        BackupObject {
            name: "audit.jsonl".into(),
            bytes: Bytes::from(audit_bytes),
        },
        BackupObject {
            name: "kv_data.json".into(),
            bytes: Bytes::from(kv_bytes),
        },
    ];
    let backup_ts = parent_snapshot_ts
        .map(|parent_ts| max_kv_version.max(parent_ts.saturating_add(1)))
        .unwrap_or(max_kv_version);
    if let Some(parent_id) = query.parent_backup_id {
        match state
            .backup
            .create_incremental_backup("local", &parent_id, backup_ts, version, version, objects)
            .await
        {
            Ok(manifest) => Json(json!({
                "backup_id": manifest.backup_id,
                "status": format!("{:?}", manifest.status),
                "files": manifest.files.len(),
            })),
            Err(e) => Json(json!({ "error": e.to_string() })),
        }
    } else {
        match state
            .backup
            .create_full_backup("local", backup_ts, version, version, objects)
            .await
        {
            Ok(manifest) => Json(json!({
                "backup_id": manifest.backup_id,
                "status": format!("{:?}", manifest.status),
                "files": manifest.files.len(),
            })),
            Err(e) => Json(json!({ "error": e.to_string() })),
        }
    }
}

async fn delete_backup(State(state): State<AdminState>, Path(id): Path<String>) -> Json<Value> {
    match state.backup.delete_backup(&id).await {
        Ok(()) => Json(json!({ "deleted": true })),
        Err(e) => Json(json!({ "deleted": false, "error": e.to_string() })),
    }
}

async fn verify_backup(State(state): State<AdminState>, Path(id): Path<String>) -> Json<Value> {
    match state.backup.verify(&id).await {
        Ok(()) => Json(json!({ "verified": true })),
        Err(e) => Json(json!({ "verified": false, "error": e.to_string() })),
    }
}

#[derive(serde::Deserialize)]
pub struct RestoreQuery {
    pub target_ts: Option<u64>,
}

/// Applies a restore to the engine, validating that every backup object parses
/// **before** mutating anything — so a malformed backup is rejected with zero
/// changes instead of leaving the catalog and KV in an inconsistent half-restored
/// state. Catalog is imported before KV so a catalog failure leaves no orphan
/// rows. Validation is separated from mutation so the caller can fence queries
/// only once mutation begins: a validation failure changes nothing, while a
/// failure after mutation has begun keeps the engine fenced (fail-closed) so a
/// partially restored state is never served.
type PitrInput = (
    nodus_backup::PitrRestorePlan,
    Vec<nodus_backup::PitrWalSegmentBytes>,
);

/// Validates that every backup object parses, mutating nothing. Returns the
/// parsed catalog snapshot (if present) for the mutation phase.
fn validate_restore_objects(objects: &[BackupObject]) -> anyhow::Result<Option<CatalogSnapshot>> {
    let mut catalog_snapshot = None;
    for obj in objects {
        match obj.name.as_str() {
            "catalog.json" => {
                catalog_snapshot = Some(
                    serde_json::from_slice::<CatalogSnapshot>(&obj.bytes)
                        .map_err(|e| anyhow::anyhow!("invalid catalog snapshot in backup: {e}"))?,
                );
            }
            "kv_data.json" => {
                serde_json::from_slice::<Vec<serde_json::Value>>(&obj.bytes)
                    .map_err(|e| anyhow::anyhow!("invalid kv_data.json in backup: {e}"))?;
            }
            _ => {}
        }
    }
    Ok(catalog_snapshot)
}

/// Applies a validated restore to the engine: catalog first, then KV, then WAL
/// replay. Mutates; must run with queries fenced.
fn apply_restore_mutations(
    objects: &[BackupObject],
    catalog_snapshot: Option<CatalogSnapshot>,
    pitr: Option<PitrInput>,
    kv: &dyn nodus_storage_api::KvEngine,
    catalog_writer: &dyn CatalogWriter,
    wal_key: Option<[u8; 32]>,
) -> anyhow::Result<nodus_backup::PitrReplayReport> {
    if let Some(snapshot) = catalog_snapshot {
        catalog_writer.import_snapshot(snapshot)?;
    }
    let base_report = BackupOrchestrator::restore_backup_objects_to_kv(objects, kv)?;
    match pitr {
        Some((plan, wal_segments)) => {
            let wal_report =
                BackupOrchestrator::replay_pitr_wal_segments(&plan, &wal_segments, kv, wal_key)?;
            Ok(BackupOrchestrator::merge_pitr_replay_reports(
                base_report,
                wal_report,
            ))
        }
        None => Ok(base_report),
    }
}

/// Validates then applies a restore in one step (used by tests).
#[cfg(test)]
fn apply_restore(
    objects: &[BackupObject],
    pitr: Option<PitrInput>,
    kv: &dyn nodus_storage_api::KvEngine,
    catalog_writer: &dyn CatalogWriter,
    wal_key: Option<[u8; 32]>,
) -> anyhow::Result<nodus_backup::PitrReplayReport> {
    let catalog_snapshot = validate_restore_objects(objects)?;
    apply_restore_mutations(objects, catalog_snapshot, pitr, kv, catalog_writer, wal_key)
}

async fn restore_backup(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Query(query): Query<RestoreQuery>,
) -> Json<Value> {
    // Serialize restores: two interleaving restores would corrupt the result.
    let _restore_guard = match state.restore_lock.try_lock() {
        Ok(guard) => guard,
        Err(_) => {
            return Json(json!({ "restored": 0, "error": "a restore is already in progress" }));
        }
    };

    let (objects, pitr) = if let Some(target_ts) = query.target_ts {
        let plan = match state.backup.plan_pitr_restore(target_ts).await {
            Ok(plan) => plan,
            Err(e) => return Json(json!({ "restored": 0, "error": e.to_string() })),
        };
        let objects = match state.backup.restore(&plan.base_backup_id).await {
            Ok(objects) => objects,
            Err(e) => return Json(json!({ "restored": 0, "error": e.to_string() })),
        };
        let wal_segments = match state.backup.load_pitr_wal_segments(&plan).await {
            Ok(segments) => segments,
            Err(e) => return Json(json!({ "restored": 0, "error": e.to_string() })),
        };
        (objects, Some((plan, wal_segments)))
    } else {
        let objects = match state.backup.restore(&id).await {
            Ok(objects) => objects,
            Err(e) => return Json(json!({ "restored": 0, "error": e.to_string() })),
        };
        (objects, None)
    };

    let names: Vec<String> = objects.iter().map(|o| o.name.clone()).collect();
    let pitr_response = pitr.as_ref().map(|(plan, _)| {
        json!({
            "base_backup_id": &plan.base_backup_id,
            "base_backup_chain": &plan.base_backup_chain,
            "target_ts": plan.target_ts,
            "wal_segments": plan.wal_segments.len(),
        })
    });

    // Catalog/KV writes route through the synchronous traits, which submit
    // to the async RaftRouter via `blocking_recv`; run them on the blocking
    // pool so we never park (or panic on) a reactor worker thread.
    let kv = state.kv.clone();
    let catalog_writer = state.catalog_writer.clone();
    let wal_key = state.wal_key;
    let restoring = state.restoring.clone();
    let restore_gate = state.restore_gate.clone();
    let replay = tokio::task::spawn_blocking(move || {
        // Validate first — a bad backup changes nothing and must not fence.
        let catalog_snapshot = validate_restore_objects(&objects)?;

        // Begin fencing: reject new statements, then take the exclusive gate,
        // which blocks until every in-flight statement has drained. From here no
        // query can observe the engine until we finish.
        restoring.store(true, std::sync::atomic::Ordering::Release);
        let _exclusive = restore_gate.write().unwrap();

        let result = apply_restore_mutations(
            &objects,
            catalog_snapshot,
            pitr,
            kv.as_ref(),
            catalog_writer.as_ref(),
            wal_key,
        );
        // On success, resume serving. On a mid-write failure, stay fenced
        // (fail-closed) so a partially restored state is never served; the
        // idempotent restore can be retried to completion.
        if result.is_ok() {
            restoring.store(false, std::sync::atomic::Ordering::Release);
        }
        result
    })
    .await;

    match replay {
        Ok(Ok(report)) => Json(json!({
            "restored": names.len(),
            "objects": names,
            "pitr": pitr_response,
            "replay": report,
        })),
        Ok(Err(e)) => Json(json!({
            "restored": 0,
            "error": e.to_string(),
            "queries_fenced": state.restoring.load(std::sync::atomic::Ordering::Acquire),
        })),
        Err(e) => Json(json!({
            "restored": 0,
            "error": format!("restore task failed: {e}"),
            "queries_fenced": state.restoring.load(std::sync::atomic::Ordering::Acquire),
        })),
    }
}

/// Runs a synchronous upgrade-coordinator write on the blocking pool. The
/// coordinator routes through the async `RaftRouter` (waiting via `blocking_recv`),
/// so the call must not run on a reactor worker thread.
async fn run_upgrade_op<F>(op: F) -> Result<(), anyhow::Error>
where
    F: FnOnce() -> Result<(), anyhow::Error> + Send + 'static,
{
    match tokio::task::spawn_blocking(op).await {
        Ok(res) => res,
        Err(join_err) => Err(anyhow::anyhow!("upgrade task failed: {join_err}")),
    }
}

/// Serializes the current upgrade state, or wraps an operation error alongside
/// the (unchanged) state so clients always get a consistent shape.
fn upgrade_response(state: &AdminState, op: Result<(), anyhow::Error>) -> Json<Value> {
    let current = state
        .upgrade
        .get_state()
        .ok()
        .and_then(|s| serde_json::to_value(s).ok())
        .unwrap_or_else(|| json!({}));
    match op {
        Ok(()) => Json(current),
        Err(e) => Json(json!({ "error": e.to_string(), "state": current })),
    }
}

async fn upgrade_state(State(state): State<AdminState>) -> Json<Value> {
    upgrade_response(&state, Ok(()))
}

async fn upgrade_start(
    State(state): State<AdminState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Value> {
    let target = params
        .get("target")
        .cloned()
        .unwrap_or_else(|| "next".to_string());
    let upgrade = state.upgrade.clone();
    let op = run_upgrade_op(move || upgrade.start_upgrade(target)).await;
    upgrade_response(&state, op)
}

async fn upgrade_node_upgraded(
    State(state): State<AdminState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Value> {
    let node = params.get("node").cloned().unwrap_or_else(|| "node".into());
    let upgrade = state.upgrade.clone();
    let op = run_upgrade_op(move || upgrade.report_node_upgraded(&node)).await;
    upgrade_response(&state, op)
}

async fn upgrade_finalize(State(state): State<AdminState>) -> Json<Value> {
    let upgrade = state.upgrade.clone();
    let op = run_upgrade_op(move || upgrade.finalize_upgrade()).await;
    upgrade_response(&state, op)
}

async fn upgrade_rollback(State(state): State<AdminState>) -> Json<Value> {
    let upgrade = state.upgrade.clone();
    let op = run_upgrade_op(move || upgrade.rollback()).await;
    upgrade_response(&state, op)
}

fn parse_table(id: &str) -> Option<TableId> {
    Uuid::parse_str(id).ok().map(TableId)
}

async fn shards_init(State(state): State<AdminState>, Path(table): Path<String>) -> Json<Value> {
    let Some(table_id) = parse_table(&table) else {
        return Json(json!({ "error": "invalid table id" }));
    };
    // The orchestrator's shard-map write replicates through Raft (blocking
    // submit), so run it off the reactor.
    let shards = state.shards.clone();
    let result = tokio::task::spawn_blocking(move || shards.init_single_shard(table_id)).await;
    match result {
        Ok(Ok(id)) => Json(json!({ "table": table_id.to_string(), "shard_id": id.to_string() })),
        Ok(Err(e)) => Json(json!({ "error": e.to_string() })),
        Err(e) => Json(json!({ "error": format!("task failed: {e}") })),
    }
}

async fn shards_map(State(state): State<AdminState>, Path(table): Path<String>) -> Json<Value> {
    let Some(table_id) = parse_table(&table) else {
        return Json(json!({ "error": "invalid table id" }));
    };
    match state.shards.shard_map(table_id) {
        Ok(map) => {
            let shards: Vec<Value> = map
                .shards
                .iter()
                .map(|s| {
                    json!({
                        "id": s.id.to_string(),
                        "start_key": s.start_key,
                        "end_key": s.end_key,
                    })
                })
                .collect();
            Json(json!({ "shards": shards }))
        }
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}

async fn shards_split(
    State(state): State<AdminState>,
    Path(table): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Value> {
    let Some(table_id) = parse_table(&table) else {
        return Json(json!({ "error": "invalid table id" }));
    };
    let shard_id = match params.get("shard").and_then(|s| Uuid::parse_str(s).ok()) {
        Some(u) => ShardId(u),
        None => return Json(json!({ "error": "missing or invalid shard id" })),
    };
    // Split key is given as a single unsigned byte for the MVP key space.
    let split_key: Vec<u8> = match params.get("key").and_then(|k| k.parse::<u8>().ok()) {
        Some(b) => vec![b],
        None => return Json(json!({ "error": "missing or invalid key (expected 0-255)" })),
    };
    // Physically relocates the shard's data into the children before flipping
    // routing, and decommissions the source group (no-op data move when the
    // shard isn't placed on this node).
    match state
        .manager
        .split_shard(&state.shards, table_id, shard_id, split_key)
        .await
    {
        Ok((l, r)) => Json(json!({ "left": l.to_string(), "right": r.to_string() })),
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}

/// Merges two adjacent shards, relocating both sources' data into the merged
/// group before flipping routing and decommissioning the sources (no-op data
/// move when the shards aren't placed on this node).
async fn shards_merge(
    State(state): State<AdminState>,
    Path(table): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Value> {
    let Some(table_id) = parse_table(&table) else {
        return Json(json!({ "error": "invalid table id" }));
    };
    let left = match params.get("left").and_then(|s| Uuid::parse_str(s).ok()) {
        Some(u) => ShardId(u),
        None => return Json(json!({ "error": "missing or invalid left shard id" })),
    };
    let right = match params.get("right").and_then(|s| Uuid::parse_str(s).ok()) {
        Some(u) => ShardId(u),
        None => return Json(json!({ "error": "missing or invalid right shard id" })),
    };
    match state
        .manager
        .merge_shards(&state.shards, table_id, left, right)
        .await
    {
        Ok(merged) => Json(json!({ "merged": merged.to_string() })),
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}

/// Brings hosted Raft groups in line with current placements after an admin
/// operation changed them, so a newly placed shard activates without a restart.
async fn reconcile_shards(state: &AdminState) {
    if let Err(e) = state.manager.reconcile(&state.shards.placements()).await {
        tracing::warn!("post-admin shard reconcile failed: {e}");
    }
}

async fn shards_rebalance(
    State(state): State<AdminState>,
    Path(table): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Value> {
    let Some(table_id) = parse_table(&table) else {
        return Json(json!({ "error": "invalid table id" }));
    };
    let nodes: Vec<String> = params
        .get("nodes")
        .map(|s| s.split(',').map(|n| n.trim().to_string()).collect())
        .unwrap_or_default();
    // Placement write replicates through Raft (blocking submit) — run off-reactor.
    let shards = state.shards.clone();
    let nodes_for_task = nodes.clone();
    let result =
        tokio::task::spawn_blocking(move || shards.rebalance(table_id, &nodes_for_task)).await;
    match result {
        Ok(Ok(())) => {
            reconcile_shards(&state).await;
            Json(json!({ "rebalanced": true, "nodes": nodes }))
        }
        Ok(Err(e)) => Json(json!({ "error": e.to_string() })),
        Err(e) => Json(json!({ "error": format!("task failed: {e}") })),
    }
}

/// Instantiates a local replica of data group `table` (a `shard-{id}` group id)
/// so it can receive the group's log from the primary. Idempotent — recreating
/// an existing group is a no-op that reloads its durable Raft state. Driven by
/// the group's primary during formation/reconcile, never by an operator.
async fn shards_replica(
    State(state): State<AdminState>,
    Path(group): Path<String>,
) -> (StatusCode, Json<Value>) {
    match state.manager.get_or_create_data(&group).await {
        Ok(_) => (StatusCode::OK, Json(json!({ "hosted": group }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// Resolves a table's catalog id (`uuid`) by name, so operators and tests can
/// turn a `db.schema.table` into the id the shard APIs and row keys are keyed
/// by. Defaults `db` to `default` and `schema` to `public`.
async fn catalog_table(
    State(state): State<AdminState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Value> {
    let db = params.get("db").map(String::as_str).unwrap_or("default");
    let schema = params.get("schema").map(String::as_str).unwrap_or("public");
    let Some(name) = params.get("name") else {
        return Json(json!({ "error": "missing 'name'" }));
    };
    match state.catalog.get_table(db, schema, name) {
        Ok(desc) => Json(json!({ "id": desc.id.to_string(), "name": name })),
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}

/// Reports the data-shard Raft groups this node hosts and their membership, so
/// operators and tests can confirm a group formed across the cluster. The meta
/// group is excluded.
async fn cluster_groups(State(state): State<AdminState>) -> Json<Value> {
    let rafts = state.raft_state.rafts.read().await;
    let mut groups: Vec<Value> = Vec::new();
    for (id, raft) in rafts.iter() {
        if id == crate::multi_raft::META_SHARD {
            continue;
        }
        let metrics = raft.metrics().borrow().clone();
        let voters: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
        groups.push(json!({
            "id": id,
            "voters": voters,
            "leader": metrics.current_leader,
            "applied": metrics.last_applied.map(|l| l.index),
        }));
    }
    Json(json!({ "groups": groups }))
}

/// Query parameters for the dump-import endpoint.
#[derive(serde::Deserialize)]
pub struct ImportQuery {
    /// `stop` to abort on the first failing statement; defaults to `continue`.
    pub on_error: Option<String>,
    /// Rows folded into each synthesized `INSERT` (defaults to 500).
    pub batch_rows: Option<usize>,
}

/// An [`nodus_import::ImportSink`] that replays translated statements through
/// the in-process executor. Each statement is parsed, planned, and executed
/// synchronously; DDL/DML therefore flows through the executor's normal
/// authorization and audit path.
struct ExecutorImportSink {
    executor: Arc<dyn nodus_executor::Executor>,
    ctx: nodus_executor::ExecutionContext,
}

impl nodus_import::ImportSink for ExecutorImportSink {
    fn execute(&mut self, stmt: &nodus_import::ImportStatement) -> anyhow::Result<u64> {
        for parsed in nodus_sql::parse_sql(&stmt.sql)? {
            let plan = nodus_executor::plan_statement(&parsed, &[])?;
            self.executor.execute_logical(&self.ctx, plan)?;
        }
        Ok(stmt.rows)
    }
}

/// Imports a plain-format PostgreSQL dump supplied as the request body. The dump
/// is translated by `nodus_import` and replayed against the in-process executor
/// on the blocking pool; the response is the versioned import report.
async fn import_dump(
    State(state): State<AdminState>,
    Query(query): Query<ImportQuery>,
    body: String,
) -> Json<Value> {
    let executor = state.executor.clone();
    let principal_id = state
        .catalog
        .get_principal_by_name("nodus")
        .map(|p| p.id)
        .unwrap_or_else(|_| PrincipalId::new());
    let ctx = nodus_executor::ExecutionContext {
        session_id: format!("admin-import-{}", Uuid::new_v4()),
        principal_id,
        active_roles: vec![],
        authz_catalog_version: 1,
    };
    let opts = nodus_import::ImportOptions {
        batch_rows: query.batch_rows.unwrap_or(500).max(1),
        on_error: match query.on_error.as_deref() {
            Some("stop") => nodus_import::OnError::Stop,
            _ => nodus_import::OnError::Continue,
        },
        ..nodus_import::ImportOptions::default()
    };

    // Executor writes route through synchronous traits, so run the whole replay
    // on the blocking pool to avoid parking a reactor worker.
    let result = tokio::task::spawn_blocking(move || {
        let mut sink = ExecutorImportSink { executor, ctx };
        nodus_import::import_str(&body, &opts, &mut sink)
    })
    .await;

    match result {
        Ok(report) => Json(json!({ "report": report })),
        Err(e) => Json(json!({ "error": format!("import task failed: {e}") })),
    }
}

#[cfg(test)]
mod import_tests {
    use super::*;
    use nodus_import::{ImportOptions, import_str};

    /// Imports a dump (schema + COPY data + folded PK) through the in-process
    /// executor, then confirms the rows are queryable.
    #[test]
    fn imports_dump_through_in_process_executor() {
        use nodus_catalog::{CreateRoleRequest, GrantPrivilegeRequest, PrincipalType};

        let audit = Arc::new(nodus_audit::MemoryAuditSink::new());
        let (executor, catalog) = nodus_executor::MemExecutor::shared(audit);

        // A superuser principal (ALL on System) so DDL/DML is authorized, the
        // same role the real endpoint resolves via `get_principal_by_name`.
        let admin = catalog
            .create_role(CreateRoleRequest {
                id: PrincipalId::new(),
                name: "importer".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        catalog
            .grant_privilege(GrantPrivilegeRequest {
                id: nodus_catalog::GrantId::new(),
                principal_id: admin.id,
                resource: ResourceRef::System,
                privilege: "ALL".into(),
            })
            .unwrap();

        let executor: Arc<dyn nodus_executor::Executor> = executor;
        let ctx = nodus_executor::ExecutionContext {
            session_id: "test-import".into(),
            principal_id: admin.id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        let dump = "\
CREATE TABLE t (id integer, name text);
COPY t (id, name) FROM stdin;
1\talpha
2\tbeta
\\.
ALTER TABLE ONLY t ADD CONSTRAINT t_pkey PRIMARY KEY (id);
";
        let mut sink = ExecutorImportSink {
            executor: executor.clone(),
            ctx: ctx.clone(),
        };
        let report = import_str(dump, &ImportOptions::default(), &mut sink);

        assert_eq!(report.tables_created, 1);
        assert_eq!(report.rows_inserted, 2);
        assert_eq!(report.constraints_folded, 1);
        assert!(
            report.is_clean(),
            "unexpected failures: {:?}",
            report.failures
        );

        let stmts = nodus_sql::parse_sql("SELECT id, name FROM t ORDER BY id").unwrap();
        let plan = nodus_executor::plan_statement(&stmts[0], &[]).unwrap();
        let out = executor.execute_logical(&ctx, plan).unwrap();
        assert_eq!(out.rows.len(), 2);
    }
}

#[cfg(test)]
mod restore_tests {
    use super::*;

    fn engine_and_catalog() -> (
        std::sync::Arc<dyn nodus_storage_api::KvEngine>,
        std::sync::Arc<nodus_catalog::MemoryCatalog>,
    ) {
        let (exec, cat) =
            nodus_executor::MemExecutor::shared(Arc::new(nodus_audit::MemoryAuditSink::new()));
        (exec.kv(), cat)
    }

    fn kv_object(json: serde_json::Value) -> BackupObject {
        BackupObject {
            name: "kv_data.json".into(),
            bytes: Bytes::from(serde_json::to_vec(&json).unwrap()),
        }
    }

    #[test]
    fn apply_restore_loads_valid_objects() {
        let (kv, cat) = engine_and_catalog();
        // key [107]='k', value [118]='v', committed at version 10.
        let objects = vec![kv_object(
            json!([{"key": [107], "value": [118], "version": 10}]),
        )];

        let report = apply_restore(&objects, None, kv.as_ref(), cat.as_ref(), None).unwrap();
        assert_eq!(report.base_kv_versions_restored, 1);
        assert_eq!(
            kv.get(b"k", u64::MAX).unwrap(),
            Some(Bytes::from_static(b"v"))
        );
    }

    #[test]
    fn apply_restore_rejects_malformed_kv_without_mutating() {
        let (kv, cat) = engine_and_catalog();
        let objects = vec![BackupObject {
            name: "kv_data.json".into(),
            bytes: Bytes::from_static(b"this is not json"),
        }];

        assert!(apply_restore(&objects, None, kv.as_ref(), cat.as_ref(), None).is_err());
        // Validate-first: nothing was written.
        assert!(kv.get(b"k", u64::MAX).unwrap().is_none());
    }

    #[test]
    fn apply_restore_rejects_malformed_catalog_before_touching_kv() {
        let (kv, cat) = engine_and_catalog();
        // A *valid* KV dump alongside a *malformed* catalog: the catalog failure
        // must abort the whole restore before any KV write.
        let objects = vec![
            BackupObject {
                name: "catalog.json".into(),
                bytes: Bytes::from_static(b"not a snapshot"),
            },
            kv_object(json!([{"key": [107], "value": [118], "version": 10}])),
        ];

        assert!(apply_restore(&objects, None, kv.as_ref(), cat.as_ref(), None).is_err());
        assert!(kv.get(b"k", u64::MAX).unwrap().is_none());
    }
}
