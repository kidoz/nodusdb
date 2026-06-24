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
        tracing::debug!("AUDIT: {:?}", event);
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
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Durable audit sink that appends one JSON object per line (JSONL). The file
/// is append-only so the trail is tamper-evident at the storage layer.
///
/// When `max_size_bytes` is set the sink rotates `logrotate`-style: the live
/// file is renamed to `<path>.1` (shifting `.1`->`.2`, ...) once it would grow
/// past the cap, and the oldest segment beyond `max_files` is dropped. Queries
/// transparently span the live file and every retained segment, oldest first,
/// so rotation never hides events while they are retained.
pub struct FileAuditSink {
    path: PathBuf,
    /// Rotate once the live file would exceed this many bytes. `None` (or `0`)
    /// disables rotation — the file grows unbounded, as before.
    max_size_bytes: Option<u64>,
    /// Number of rotated segments to retain (`<path>.1` ..= `<path>.max_files`).
    max_files: usize,
    lock: Mutex<()>,
}

impl FileAuditSink {
    /// Creates a non-rotating sink (unbounded append-only file).
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_rotation(path, None, 0)
    }

    /// Creates a sink that rotates the live file once it would exceed
    /// `max_size_bytes`, keeping up to `max_files` rotated segments. A
    /// `max_size_bytes` of `None`/`Some(0)` disables rotation.
    pub fn with_rotation(
        path: impl Into<PathBuf>,
        max_size_bytes: Option<u64>,
        max_files: usize,
    ) -> Self {
        Self {
            path: path.into(),
            max_size_bytes,
            max_files,
            lock: Mutex::new(()),
        }
    }

    /// Path of the `n`th rotated segment (`<path>.n`).
    fn segment_path(&self, n: usize) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(format!(".{n}"));
        PathBuf::from(name)
    }

    /// Renames the live file to `.1`, shifting older segments up and discarding
    /// any beyond `max_files`. Called with the write lock held.
    fn rotate(&self) -> anyhow::Result<()> {
        // At least one rotated segment is kept whenever rotation is requested,
        // otherwise a size cap would simply discard the just-filled file.
        let keep = self.max_files.max(1);
        let _ = std::fs::remove_file(self.segment_path(keep));
        for i in (1..keep).rev() {
            let from = self.segment_path(i);
            if from.exists() {
                std::fs::rename(&from, self.segment_path(i + 1))?;
            }
        }
        if self.path.exists() {
            std::fs::rename(&self.path, self.segment_path(1))?;
        }
        Ok(())
    }

    fn read_segment(path: &Path, out: &mut Vec<AuditEvent>) -> anyhow::Result<()> {
        match std::fs::read_to_string(path) {
            Ok(content) => {
                for line in content.lines().filter(|l| !l.trim().is_empty()) {
                    out.push(serde_json::from_str::<AuditEvent>(line)?);
                }
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
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
        // Rotate before writing if the new line would push the live file past
        // the cap (and there is already content to preserve).
        if let Some(max) = self.max_size_bytes.filter(|m| *m > 0) {
            let current = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
            if current > 0 && current + line.len() as u64 + 1 > max {
                self.rotate()?;
            }
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
        // Hold the lock so a concurrent rotation can't make a segment briefly
        // invisible mid-rename.
        let _guard = self.lock.lock().unwrap();
        let mut events = Vec::new();
        // Oldest first: highest-numbered rotated segment down to `.1`, then the
        // live file, so chronological order is preserved across rotation.
        let keep = self.max_files.max(1);
        for i in (1..=keep).rev() {
            Self::read_segment(&self.segment_path(i), &mut events)?;
        }
        Self::read_segment(&self.path, &mut events)?;
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

    #[test]
    fn file_sink_rotates_and_query_spans_segments() {
        let dir = std::env::temp_dir().join(format!("nodus_audit_rot_{}", AuditEventId::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("audit.jsonl");
        // A cap smaller than any single event forces exactly one event per
        // segment, so retention is deterministic: live file + `max_files`
        // segments == 3 events kept out of the 12 written.
        let sink = FileAuditSink::with_rotation(&path, Some(50), 2);

        for _ in 0..12 {
            sink.record_event(event(PrincipalId::new(), "SELECT", "Success"))
                .unwrap();
        }

        // Rotation happened (a `.1` segment exists) and nothing beyond
        // `max_files` is retained (`.3` was dropped).
        assert!(path.exists(), "live audit file should exist");
        assert!(path.with_extension("jsonl.1").exists(), "expected rotation");
        assert!(
            !path.with_extension("jsonl.3").exists(),
            "oldest segment beyond max_files must be dropped"
        );

        // Query spans the live file + both retained segments, in chronological
        // order, and retention is bounded (old events rotated out).
        let all = sink.query(&AuditQuery::default()).unwrap();
        assert_eq!(all.len(), 3, "live file + {} retained segments", 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_sink_without_rotation_is_unbounded() {
        let path =
            std::env::temp_dir().join(format!("nodus_audit_norot_{}.jsonl", AuditEventId::new()));
        let sink = FileAuditSink::new(&path);
        for _ in 0..50 {
            sink.record_event(event(PrincipalId::new(), "INSERT", "Success"))
                .unwrap();
        }
        assert_eq!(sink.query(&AuditQuery::default()).unwrap().len(), 50);
        assert!(!path.with_extension("jsonl.1").exists());
        let _ = std::fs::remove_file(&path);
    }
}
