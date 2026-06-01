//! Admin HTTP API (`/api/v1/...`) backed by shared runtime handles.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post},
};
use bytes::Bytes;
use nodus_audit::{AuditEvent, AuditQuery, AuditQueryable, MemoryAuditSink};
use nodus_authz::{Action, AuthzContext, AuthzEngine, AuthzExplanation, AuthzRequest};
use nodus_backup::{BackupObject, BackupOrchestrator};
use nodus_catalog::{CatalogReader, PrincipalId, ResourceRef, ShardId, TableId};
use nodus_security::{SessionInfo, SessionRegistry};
use nodus_sharding::ShardOrchestrator;
use nodus_upgrade::{DefaultUpgradeCoordinator, UpgradeCoordinator};
use serde_json::{Value, json};
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
    pub backup: Arc<BackupOrchestrator>,
    pub upgrade: Arc<DefaultUpgradeCoordinator>,
    pub shards: Arc<ShardOrchestrator>,
}

pub fn admin_routes(state: AdminState) -> Router {
    Router::new()
        .route("/api/v1/sessions", get(list_sessions))
        .route("/api/v1/sessions/:id/kill", post(kill_session))
        .route("/api/v1/audit", get(query_audit))
        .route("/api/v1/authz/explain", get(explain_authz))
        .route("/api/v1/backups", get(list_backups).post(create_backup))
        .route("/api/v1/backups/:id/verify", post(verify_backup))
        .route("/api/v1/backups/:id/restore", post(restore_backup))
        .route("/api/v1/upgrade", get(upgrade_state))
        .route("/api/v1/upgrade/start", post(upgrade_start))
        .route("/api/v1/upgrade/node-upgraded", post(upgrade_node_upgraded))
        .route("/api/v1/upgrade/finalize", post(upgrade_finalize))
        .route("/api/v1/upgrade/rollback", post(upgrade_rollback))
        .route("/api/v1/shards/:table/init", post(shards_init))
        .route("/api/v1/shards/:table", get(shards_map))
        .route("/api/v1/shards/:table/split", post(shards_split))
        .route("/api/v1/shards/:table/rebalance", post(shards_rebalance))
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

async fn list_backups(State(state): State<AdminState>) -> Json<Vec<String>> {
    Json(state.backup.list_backups().await.unwrap_or_default())
}

/// Creates a full backup. As an MVP snapshot it captures the current audit
/// trail; the export set will broaden as catalog/shard serialization lands.
async fn create_backup(State(state): State<AdminState>) -> Json<Value> {
    let events = state
        .audit
        .query(&AuditQuery::default())
        .unwrap_or_default();
    let bytes = serde_json::to_vec(&events).unwrap_or_default();
    let version = state
        .catalog
        .get_cluster_version()
        .map(|v| v.active_version)
        .unwrap_or(0);
    let objects = vec![BackupObject {
        name: "audit.jsonl".into(),
        bytes: Bytes::from(bytes),
    }];
    match state
        .backup
        .create_full_backup("local", 0, version, version, objects)
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

async fn verify_backup(State(state): State<AdminState>, Path(id): Path<String>) -> Json<Value> {
    match state.backup.verify(&id).await {
        Ok(()) => Json(json!({ "verified": true })),
        Err(e) => Json(json!({ "verified": false, "error": e.to_string() })),
    }
}

async fn restore_backup(State(state): State<AdminState>, Path(id): Path<String>) -> Json<Value> {
    match state.backup.restore(&id).await {
        Ok(objects) => Json(json!({ "restored": objects.len() })),
        Err(e) => Json(json!({ "restored": 0, "error": e.to_string() })),
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
    let op = state.upgrade.start_upgrade(target);
    upgrade_response(&state, op)
}

async fn upgrade_node_upgraded(
    State(state): State<AdminState>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<Value> {
    let node = params.get("node").cloned().unwrap_or_else(|| "node".into());
    let op = state.upgrade.report_node_upgraded(&node);
    upgrade_response(&state, op)
}

async fn upgrade_finalize(State(state): State<AdminState>) -> Json<Value> {
    let op = state.upgrade.finalize_upgrade();
    upgrade_response(&state, op)
}

async fn upgrade_rollback(State(state): State<AdminState>) -> Json<Value> {
    let op = state.upgrade.rollback();
    upgrade_response(&state, op)
}

fn parse_table(id: &str) -> Option<TableId> {
    Uuid::parse_str(id).ok().map(TableId)
}

async fn shards_init(State(state): State<AdminState>, Path(table): Path<String>) -> Json<Value> {
    let Some(table_id) = parse_table(&table) else {
        return Json(json!({ "error": "invalid table id" }));
    };
    match state.shards.init_single_shard(table_id) {
        Ok(id) => Json(json!({ "table": table_id.to_string(), "shard_id": id.to_string() })),
        Err(e) => Json(json!({ "error": e.to_string() })),
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
    match state.shards.split(table_id, shard_id, split_key) {
        Ok((l, r)) => Json(json!({ "left": l.to_string(), "right": r.to_string() })),
        Err(e) => Json(json!({ "error": e.to_string() })),
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
    match state.shards.rebalance(table_id, &nodes) {
        Ok(()) => Json(json!({ "rebalanced": true, "nodes": nodes })),
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}
