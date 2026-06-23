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
use nodus_catalog::{CatalogReader, CatalogWriter, PrincipalId, ResourceRef, ShardId, TableId};
use nodus_monitoring::{SlowQuery, SlowQueryLog};
use nodus_security::{SessionInfo, SessionRegistry};
use nodus_sharding::ShardOrchestrator;
use nodus_storage_wal::WalEngine;
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
    pub slow_log: Arc<SlowQueryLog>,
    pub kv: Arc<dyn nodus_storage_api::KvEngine>,
    pub wal_key: Option<[u8; 32]>,
    /// Shared with `AppState`; flipping it makes `/readyz` report not-ready.
    pub draining: Arc<AtomicBool>,
    pub authenticator: Arc<PasswordAuthenticator>,
    pub admin_token: Option<String>,
    pub raft_state: nodus_raftstore::server::RaftState,
    pub membership_lock: Arc<tokio::sync::Mutex<()>>,
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
        p if p.starts_with("/api/v1/backups") => nodus_authz::Action::ManageBackups,
        p if p.starts_with("/api/v1/upgrade") => nodus_authz::Action::ManageUpgrades,
        p if p.starts_with("/api/v1/shards") => nodus_authz::Action::ManageShards,
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
        .route("/api/v1/backups", get(list_backups).post(create_backup))
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
        .route("/api/v1/shards/:table/rebalance", post(shards_rebalance))
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

async fn cluster_join(
    State(state): State<AdminState>,
    Json(req): Json<JoinRequest>,
) -> impl axum::response::IntoResponse {
    let _guard = state.membership_lock.lock().await;
    let node = openraft::BasicNode::new(&req.raft_advertise_addr);
    let raft = state
        .raft_state
        .rafts
        .read()
        .await
        .get("shard-meta")
        .cloned()
        .unwrap();
    match raft.add_learner(req.node_id, node, true).await {
        Ok(_) => {
            // Once learner is added, attempt to promote it to a voter.
            let metrics = raft.metrics().borrow().clone();
            let mut members: std::collections::BTreeSet<u64> =
                metrics.membership_config.membership().voter_ids().collect();
            members.insert(req.node_id);
            match raft.change_membership(members, true).await {
                Ok(_) => (
                    StatusCode::OK,
                    Json(json!({ "joined": true, "node_id": req.node_id })),
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": format!("Failed to change membership: {}", e) })),
                ),
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Failed to add learner: {}", e) })),
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
    if let Some(parent_id) = query.parent_backup_id {
        match state
            .backup
            .create_incremental_backup("local", &parent_id, 0, version, version)
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

async fn restore_backup(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Query(query): Query<RestoreQuery>,
) -> Json<Value> {
    match state.backup.restore(&id).await {
        Ok(objects) => {
            // Pre-fetch archived WALs (async) before the blocking replay below.
            let archived_wals = match query.target_ts {
                Some(_) => state.backup.get_archived_wals().await.ok(),
                None => None,
            };
            let names: Vec<String> = objects.iter().map(|o| o.name.clone()).collect();

            // Catalog/KV writes route through the synchronous traits, which submit
            // to the async RaftRouter via `blocking_recv`; run them on the blocking
            // pool so we never park (or panic on) a reactor worker thread.
            let kv = state.kv.clone();
            let catalog_writer = state.catalog_writer.clone();
            let wal_key = state.wal_key;
            let target_ts = query.target_ts;
            let _ = tokio::task::spawn_blocking(move || {
                for obj in &objects {
                    if obj.name == "catalog.json" {
                        if let Ok(snapshot) = serde_json::from_slice(&obj.bytes) {
                            let _ = catalog_writer.import_snapshot(snapshot);
                        }
                    } else if obj.name == "kv_data.json" {
                        #[allow(clippy::collapsible_if)]
                        if let Ok(dump) =
                            serde_json::from_slice::<Vec<serde_json::Value>>(&obj.bytes)
                        {
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
                                    let _ = kv.write_intent(
                                        txn_id,
                                        Bytes::from(key_bytes.clone()),
                                        Bytes::from(val_bytes),
                                    );
                                    let _ = kv.commit(txn_id, ver);
                                    tracing::debug!(
                                        "Restored KV pair from baseline: key={:?}",
                                        String::from_utf8_lossy(&key_bytes)
                                    );
                                }
                            }
                        }
                    }
                }

                if let (Some(target_ts), Some(wals)) = (target_ts, archived_wals) {
                    let mut active_txns = std::collections::HashSet::new();
                    for (_name, bytes) in wals {
                        let tmp = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
                        std::fs::write(&tmp, bytes).unwrap();
                        if let Ok(wal_engine) =
                            nodus_storage_wal::FileWalEngine::with_encryption(&tmp, wal_key)
                        {
                            if let Ok(records) = wal_engine.recover() {
                                for record in records {
                                    let nodus_storage_wal::WalRecord::V1(rec) = record;
                                    match rec {
                                        nodus_storage_wal::WalRecordV1::WriteIntent {
                                            txn_id,
                                            key,
                                            value,
                                        } => {
                                            let _ = kv.write_intent(
                                                txn_id,
                                                Bytes::from(key),
                                                Bytes::from(value),
                                            );
                                            active_txns.insert(txn_id);
                                        }
                                        nodus_storage_wal::WalRecordV1::DeleteIntent {
                                            txn_id,
                                            key,
                                        } => {
                                            let _ = kv.delete_intent(txn_id, Bytes::from(key));
                                            active_txns.insert(txn_id);
                                        }
                                        nodus_storage_wal::WalRecordV1::CommitTxn {
                                            txn_id,
                                            commit_ts,
                                        } => {
                                            if commit_ts <= target_ts {
                                                tracing::debug!(
                                                    "Replayed commit_ts {} <= target_ts {}",
                                                    commit_ts,
                                                    target_ts
                                                );
                                                let _ = kv.commit(txn_id, commit_ts);
                                            } else {
                                                tracing::debug!(
                                                    "Skipped commit_ts {} > target_ts {}",
                                                    commit_ts,
                                                    target_ts
                                                );
                                                let _ = kv.abort(txn_id);
                                            }
                                            active_txns.remove(&txn_id);
                                        }
                                        nodus_storage_wal::WalRecordV1::AbortTxn { txn_id } => {
                                            let _ = kv.abort(txn_id);
                                            active_txns.remove(&txn_id);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        let _ = std::fs::remove_file(&tmp);
                    }
                    // Abort any pending transactions
                    for txn_id in active_txns {
                        let _ = kv.abort(txn_id);
                    }
                }
            })
            .await;

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
