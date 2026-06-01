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

/// Filters for querying the audit trail. `None` fields match anything.
#[derive(Debug, Clone, Default)]
pub struct AuditQuery {
    pub actor: Option<PrincipalId>,
    pub action: Option<String>,
    pub result: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
}

impl AuditQuery {
    fn matches(&self, e: &AuditEvent) -> bool {
        self.actor.map(|a| a == e.actor).unwrap_or(true)
            && self
                .action
                .as_ref()
                .map(|a| a.eq_ignore_ascii_case(&e.action))
                .unwrap_or(true)
            && self
                .result
                .as_ref()
                .map(|r| r.eq_ignore_ascii_case(&e.result))
                .unwrap_or(true)
            && self.since.map(|s| e.time >= s).unwrap_or(true)
            && self.until.map(|u| e.time <= u).unwrap_or(true)
    }

    fn apply(&self, mut events: Vec<AuditEvent>) -> Vec<AuditEvent> {
        events.retain(|e| self.matches(e));
        if let Some(limit) = self.limit {
            events.truncate(limit);
        }
        events
    }
}

/// A queryable audit trail.
pub trait AuditQueryable: Send + Sync {
    fn query(&self, query: &AuditQuery) -> anyhow::Result<Vec<AuditEvent>>;
}

use std::sync::RwLock;

/// In-memory audit sink with query support. Suitable for tests and as a
/// front for a durable sink.
#[derive(Default)]
pub struct MemoryAuditSink {
    events: RwLock<Vec<AuditEvent>>,
}

impl MemoryAuditSink {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AuditSink for MemoryAuditSink {
    fn record_event(&self, event: AuditEvent) -> anyhow::Result<()> {
        self.events.write().unwrap().push(event);
        Ok(())
    }
}

impl AuditQueryable for MemoryAuditSink {
    fn query(&self, query: &AuditQuery) -> anyhow::Result<Vec<AuditEvent>> {
        Ok(query.apply(self.events.read().unwrap().clone()))
    }
}

use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

/// Durable audit sink that appends one JSON object per line (JSONL). The file
/// is append-only so the trail is tamper-evident at the storage layer.
pub struct FileAuditSink {
    path: PathBuf,
    lock: Mutex<()>,
}

impl FileAuditSink {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock: Mutex::new(()),
        }
    }
}

impl AuditSink for FileAuditSink {
    fn record_event(&self, event: AuditEvent) -> anyhow::Result<()> {
        let line = serde_json::to_string(&event)?;
        let _guard = self.lock.lock().unwrap();
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{line}")?;
        Ok(())
    }
}

impl AuditQueryable for FileAuditSink {
    fn query(&self, query: &AuditQuery) -> anyhow::Result<Vec<AuditEvent>> {
        let content = match std::fs::read_to_string(&self.path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e.into()),
        };
        let mut events = Vec::new();
        for line in content.lines().filter(|l| !l.trim().is_empty()) {
            events.push(serde_json::from_str::<AuditEvent>(line)?);
        }
        Ok(query.apply(events))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(actor: PrincipalId, action: &str, result: &str) -> AuditEvent {
        AuditEvent {
            id: AuditEventId::new(),
            time: Utc::now(),
            actor,
            action: action.into(),
            resource: None,
            source_ip: None,
            request_id: None,
            session_id: None,
            query_id: None,
            reason: None,
            result: result.into(),
            error: None,
            authz_catalog_version: Some(1),
        }
    }

    #[test]
    fn memory_sink_query_filters() {
        let sink = MemoryAuditSink::new();
        let alice = PrincipalId::new();
        let bob = PrincipalId::new();
        sink.record_event(event(alice, "SELECT", "Success"))
            .unwrap();
        sink.record_event(event(bob, "INSERT", "Denied")).unwrap();
        sink.record_event(event(alice, "DELETE", "Success"))
            .unwrap();

        assert_eq!(sink.query(&AuditQuery::default()).unwrap().len(), 3);
        let by_actor = AuditQuery {
            actor: Some(alice),
            ..Default::default()
        };
        assert_eq!(sink.query(&by_actor).unwrap().len(), 2);
        let denied = AuditQuery {
            result: Some("denied".into()),
            ..Default::default()
        };
        assert_eq!(sink.query(&denied).unwrap().len(), 1);
        let limited = AuditQuery {
            limit: Some(1),
            ..Default::default()
        };
        assert_eq!(sink.query(&limited).unwrap().len(), 1);
    }

    #[test]
    fn file_sink_persists_and_queries() {
        let path = std::env::temp_dir().join(format!("nodus_audit_{}.jsonl", AuditEventId::new()));
        let sink = FileAuditSink::new(&path);
        let alice = PrincipalId::new();
        sink.record_event(event(alice, "SELECT", "Success"))
            .unwrap();
        sink.record_event(event(PrincipalId::new(), "INSERT", "Success"))
            .unwrap();

        // A fresh sink reading the same file sees the persisted events.
        let reader = FileAuditSink::new(&path);
        assert_eq!(reader.query(&AuditQuery::default()).unwrap().len(), 2);
        let by_action = AuditQuery {
            action: Some("select".into()),
            ..Default::default()
        };
        assert_eq!(reader.query(&by_action).unwrap().len(), 1);

        let _ = std::fs::remove_file(&path);
    }
}
