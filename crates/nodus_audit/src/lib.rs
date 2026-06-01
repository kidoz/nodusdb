use chrono::{DateTime, Utc};
use nodus_catalog::{AuditEventId, PrincipalId, ResourceRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: AuditEventId,
    pub time: DateTime<Utc>,
    pub actor: PrincipalId,
    pub action: String,
    pub resource: Option<ResourceRef>,
    pub source_ip: Option<String>,
    pub request_id: Option<String>,
    pub session_id: Option<String>,
    pub query_id: Option<String>,
    pub reason: Option<String>,
    pub result: String, // e.g., "Success", "Denied"
    pub error: Option<String>,
    pub authz_catalog_version: Option<u64>,
}

pub trait AuditSink: Send + Sync {
    fn record_event(&self, event: AuditEvent) -> anyhow::Result<()>;
}

pub struct LogAuditSink;

impl AuditSink for LogAuditSink {
    fn record_event(&self, event: AuditEvent) -> anyhow::Result<()> {
        // Just print to standard out for MVP
        println!("AUDIT: {:?}", event);
        Ok(())
    }
}
