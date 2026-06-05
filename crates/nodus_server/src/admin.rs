//! Admin HTTP API (`/api/v1/...`) backed by shared runtime handles.

use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{Next, from_fn_with_state},
    response::Response,
    routing::{get, post},
};
use bytes::Bytes;
use nodus_audit::{AuditEvent, AuditQuery, AuditQueryable};
use nodus_authz::{Action, AuthzContext, AuthzEngine, AuthzExplanation, AuthzRequest};
use nodus_backup::{BackupObject, BackupOrchestrator};
use nodus_catalog::{CatalogReader, CatalogWriter, PrincipalId, ResourceRef, ShardId, TableId};
use nodus_monitoring::{SlowQuery, SlowQueryLog};
use nodus_security::{SessionInfo, SessionRegistry};
use nodus_sharding::ShardOrchestrator;
use nodus_upgrade::{DefaultUpgradeCoordinator, UpgradeCoordinator};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

/// Shared handles the admin endpoints operate on. Grows as more control-plane
/// surfaces are wired in.
#[derive(Clone)]
pub struct AdminState {
    pub registry: Arc<SessionRegistry>,
    pub audit: Arc<dyn AuditQueryable>,
    pub authz: Arc<dyn AuthzEngine>,
    pub catalog: Arc<dyn CatalogReader>,
    pub catalog_writer: Arc<dyn CatalogWriter>,
    pub backup: Arc<BackupOrchestrator>,
    pub upgrade: Arc<DefaultUpgradeCoordinator>,
    pub shards: Arc<ShardOrchestrator>,
    pub slow_log: Arc<SlowQueryLog>,
    pub kv: Arc<dyn nodus_storage_api::KvEngine>,
    /// Shared with `AppState`; flipping it makes `/readyz` report not-ready.
    pub draining: Arc<AtomicBool>,
    /// Bearer token required on admin endpoints; `None` disables auth.
    pub admin_token: Option<String>,
}

/// Rejects requests lacking a valid `Authorization: Bearer <token>` header when
/// an admin token is configured. A no-op when no token is set.
async fn require_token(
    State(state): State<AdminState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if let Some(expected) = &state.admin_token {
        let provided = req
            .headers()
            .get(AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "));
        if provided != Some(expected.as_str()) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(next.run(req).await)
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
        .route("/api/v1/queries", get(slow_queries))
        .route("/api/v1/node/drain", post(node_drain))
        .route_layer(from_fn_with_state(state.clone(), require_token))
        .with_state(state)
}

async fn slow_queries(State(state): State<AdminState>) -> Json<Vec<SlowQuery>> {
    Json(state.slow_log.list())
}

/// Marks the node as draining: `/readyz` starts failing (so load balancers stop
/// new traffic) and all active sessions are cancelled.
async fn node_drain(State(state): State<AdminState>) -> Json<Value> {
    state.draining.store(true, Ordering::Release);
    let sessions = state.registry.list();
    for s in &sessions {
        state.registry.kill(&s.session_id);
    }
    Json(json!({ "draining": true, "sessions_cancelled": sessions.len() }))
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

/// Creates a full backup capturing a catalog snapshot and the audit trail.
async fn create_backup(State(state): State<AdminState>) -> Json<Value> {
    let version = state
        .catalog
        .get_cluster_version()
        .map(|v| v.active_version)
        .unwrap_or(0);

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
    let range = nodus_storage_api::KeyRange {
        start: Bytes::new(),
        end: Bytes::from(vec![255u8; 1024]),
    };

    match state.kv.scan(range, u64::MAX) {
        Ok(iter) => {
            for pair_res in iter {
                match pair_res {
                    Ok(pair) => {
                        kv_dump.push(json!({
                            "key": pair.key.to_vec(),
                            "value": pair.value.to_vec(),
                            "version": pair.version,
                        }));
                    }
                    Err(e) => {
                        return Json(json!({ "error": format!("Failed to scan KV pair: {e}") }));
                    }
                }
            }
        }
        Err(e) => return Json(json!({ "error": format!("Failed to start KV scan: {e}") })),
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
        Ok(objects) => {
            for obj in &objects {
                if obj.name == "catalog.json" {
                    if let Ok(snapshot) = serde_json::from_slice(&obj.bytes) {
                        let _ = state.catalog_writer.import_snapshot(snapshot);
                    }
                } else if obj.name == "kv_data.json" {
                    #[allow(clippy::collapsible_if)]
                    if let Ok(dump) = serde_json::from_slice::<Vec<serde_json::Value>>(&obj.bytes) {
                        for pair in dump {
                            if let (Some(k), Some(v), Some(ver)) = (
                                pair.get("key").and_then(|k| k.as_array()),
                                pair.get("value").and_then(|v| v.as_array()),
                                pair.get("version").and_then(|v| v.as_u64()),
                            ) {
                                let key_bytes: Vec<u8> = k
                                    .iter()
                                    .filter_map(|x| x.as_u64())
                                    .map(|x| x as u8)
                                    .collect();
                                let val_bytes: Vec<u8> = v
                                    .iter()
                                    .filter_map(|x| x.as_u64())
                                    .map(|x| x as u8)
                                    .collect();

                                let txn_id = nodus_storage_api::TxnId::new();
                                let _ = state.kv.write_intent(
                                    txn_id,
                                    Bytes::from(key_bytes),
                                    Bytes::from(val_bytes),
                                );
                                let _ = state.kv.commit(txn_id, ver);
                            }
                        }
                    }
                }
            }
            let names: Vec<String> = objects.into_iter().map(|o| o.name).collect();
            Json(json!({ "restored": names.len(), "objects": names }))
        }
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
