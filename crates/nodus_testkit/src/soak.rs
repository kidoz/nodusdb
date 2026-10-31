//! Versioned artifacts for scheduled mixed-activity soak drills.

use crate::benchmark::{MetricSamples, process_allocator_bytes, process_rss_bytes};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const SOAK_SCHEMA_VERSION: u32 = 1;

fn now_unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

pub fn process_resource_sample() -> (Option<u64>, Option<u64>) {
    (process_rss_bytes().ok(), process_allocator_bytes().ok())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SoakNodeSample {
    pub node_id: u64,
    pub metrics: Option<MetricSamples>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SoakEvent {
    pub schema_version: u32,
    pub kind: String,
    pub name: String,
    pub elapsed_seconds: f64,
    pub unix_micros: u64,
    pub success: Option<bool>,
    pub detail: Value,
    pub rss_bytes: Option<u64>,
    pub allocator_bytes: Option<u64>,
    pub nodes: Vec<SoakNodeSample>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ResourceAssessment {
    pub bounded_memory: Option<bool>,
    pub bounded_queues: Option<bool>,
    pub production_budgets_met: Option<bool>,
    pub raft_log_reclamation_observed: Option<bool>,
    pub post_activity_observation_window_seconds: f64,
    pub note: String,
}

impl ResourceAssessment {
    pub fn unassessed(observation_window: Duration) -> Self {
        Self {
            bounded_memory: None,
            bounded_queues: None,
            production_budgets_met: None,
            raft_log_reclamation_observed: None,
            post_activity_observation_window_seconds: observation_window.as_secs_f64(),
            note: "Short-run observations do not establish bounded resources or production SLO compliance."
                .into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SoakSummary {
    pub schema_version: u32,
    pub run_id: String,
    pub status: String,
    pub started_at_unix_micros: u64,
    pub finished_at_unix_micros: u64,
    pub error: Option<String>,
    pub source: Value,
    pub configuration: Value,
    pub workload: Value,
    pub activities: Vec<Value>,
    pub validation: Value,
    pub resources: Value,
    pub assessment: ResourceAssessment,
}

pub struct SoakArtifact {
    run_id: String,
    directory: PathBuf,
    events_path: PathBuf,
    output: Mutex<File>,
    started: Instant,
    started_at_unix_micros: u64,
}

impl SoakArtifact {
    pub fn create(directory: &Path, run_id: String) -> io::Result<Self> {
        std::fs::create_dir_all(directory)?;
        let events_path = directory.join("events.jsonl");
        let output = File::create_new(&events_path)?;
        eprintln!("mixed-activity soak artifacts: {}", directory.display());
        Ok(Self {
            run_id,
            directory: directory.to_path_buf(),
            events_path,
            output: Mutex::new(output),
            started: Instant::now(),
            started_at_unix_micros: now_unix_micros(),
        })
    }

    pub fn from_environment() -> io::Result<Self> {
        let run_id = format!("{}-{}", now_unix_micros(), std::process::id());
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let directory = std::env::var_os("NODUS_SOAK_ARTIFACT_DIR")
            .map(PathBuf::from)
            .map(|path| {
                if path.is_absolute() {
                    path
                } else {
                    workspace_root.join(path)
                }
            })
            .unwrap_or_else(|| {
                workspace_root
                    .join("target/mixed-activity-soak")
                    .join(&run_id)
            });
        Self::create(&directory, run_id)
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn events_path(&self) -> &Path {
        &self.events_path
    }

    pub fn summary_path(&self) -> PathBuf {
        self.directory.join("summary.json")
    }

    pub fn started_at_unix_micros(&self) -> u64 {
        self.started_at_unix_micros
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn append(&self, mut event: SoakEvent) -> io::Result<()> {
        event.schema_version = SOAK_SCHEMA_VERSION;
        event.elapsed_seconds = self.elapsed().as_secs_f64();
        event.unix_micros = now_unix_micros();
        let mut output = self
            .output
            .lock()
            .map_err(|_| io::Error::other("soak artifact lock is poisoned"))?;
        serde_json::to_writer(&mut *output, &event).map_err(io::Error::other)?;
        output.write_all(b"\n")?;
        output.flush()
    }

    pub fn activity(&self, name: &str, success: bool, detail: Value) -> io::Result<()> {
        self.append(SoakEvent {
            schema_version: SOAK_SCHEMA_VERSION,
            kind: "activity".into(),
            name: name.into(),
            elapsed_seconds: 0.0,
            unix_micros: 0,
            success: Some(success),
            detail,
            rss_bytes: None,
            allocator_bytes: None,
            nodes: Vec::new(),
        })
    }

    pub fn sample(&self, nodes: Vec<SoakNodeSample>, detail: Value) -> io::Result<()> {
        let (rss_bytes, allocator_bytes) = process_resource_sample();
        self.append(SoakEvent {
            schema_version: SOAK_SCHEMA_VERSION,
            kind: "sample".into(),
            name: "resources".into(),
            elapsed_seconds: 0.0,
            unix_micros: 0,
            success: None,
            detail,
            rss_bytes,
            allocator_bytes,
            nodes,
        })
    }

    pub fn write_summary(&self, mut summary: SoakSummary) -> io::Result<()> {
        summary.schema_version = SOAK_SCHEMA_VERSION;
        summary.run_id.clone_from(&self.run_id);
        summary.started_at_unix_micros = self.started_at_unix_micros;
        summary.finished_at_unix_micros = now_unix_micros();
        summary.assessment.post_activity_observation_window_seconds = summary
            .assessment
            .post_activity_observation_window_seconds
            .max(0.0);
        let mut output = File::create_new(self.summary_path())?;
        serde_json::to_writer_pretty(&mut output, &summary).map_err(io::Error::other)?;
        output.write_all(b"\n")?;
        output.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_round_trip_preserves_unavailable_observations() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = SoakArtifact::create(directory.path(), "test-run".into()).unwrap();
        artifact
            .sample(
                vec![SoakNodeSample {
                    node_id: 3,
                    metrics: None,
                    error: Some("node stopped".into()),
                }],
                serde_json::json!({"phase": "restart"}),
            )
            .unwrap();
        let body = std::fs::read_to_string(artifact.events_path()).unwrap();
        let event: SoakEvent = serde_json::from_str(body.trim_end()).unwrap();
        assert_eq!(event.schema_version, SOAK_SCHEMA_VERSION);
        assert_eq!(event.nodes[0].metrics, None);
        assert_eq!(event.nodes[0].error.as_deref(), Some("node stopped"));
        assert!(body.contains("\"metrics\":null"));
    }

    #[test]
    fn artifact_does_not_overwrite_an_existing_run() {
        let directory = tempfile::tempdir().unwrap();
        SoakArtifact::create(directory.path(), "first".into()).unwrap();
        let error = match SoakArtifact::create(directory.path(), "second".into()) {
            Ok(_) => panic!("second artifact unexpectedly replaced the first"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn summary_leaves_long_term_and_production_verdicts_unassessed() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = SoakArtifact::create(directory.path(), "test-run".into()).unwrap();
        artifact
            .write_summary(SoakSummary {
                schema_version: SOAK_SCHEMA_VERSION,
                run_id: String::new(),
                status: "passed".into(),
                started_at_unix_micros: 0,
                finished_at_unix_micros: 0,
                error: None,
                source: Value::Null,
                configuration: Value::Null,
                workload: Value::Null,
                activities: Vec::new(),
                validation: Value::Null,
                resources: Value::Null,
                assessment: ResourceAssessment::unassessed(Duration::from_secs(10)),
            })
            .unwrap();
        let summary: SoakSummary =
            serde_json::from_slice(&std::fs::read(artifact.summary_path()).unwrap()).unwrap();
        assert_eq!(summary.status, "passed");
        assert_eq!(summary.assessment.bounded_memory, None);
        assert_eq!(summary.assessment.bounded_queues, None);
        assert_eq!(summary.assessment.production_budgets_met, None);
        assert_eq!(
            summary.assessment.post_activity_observation_window_seconds,
            10.0
        );
    }
}
