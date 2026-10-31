//! Measurement helpers for real-server benchmarks.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const BENCHMARK_RECORD_SCHEMA_VERSION: u32 = 2;
const RESOURCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

pub type MetricSamples = BTreeMap<String, f64>;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct LatencySummary {
    pub count: u64,
    pub mean_seconds: f64,
    pub p50_seconds: f64,
    pub p95_seconds: f64,
    pub p99_seconds: f64,
    pub max_seconds: f64,
}

#[derive(Clone, Default)]
pub struct LatencyRecorder {
    samples: Arc<Mutex<Vec<f64>>>,
}

impl LatencyRecorder {
    pub fn record(&self, elapsed: Duration) {
        self.samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(elapsed.as_secs_f64());
    }

    pub fn summary(&self) -> LatencySummary {
        let mut samples = self
            .samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if samples.is_empty() {
            return LatencySummary::default();
        }
        samples.sort_by(f64::total_cmp);
        let count = samples.len();
        LatencySummary {
            count: count as u64,
            mean_seconds: samples.iter().sum::<f64>() / count as f64,
            p50_seconds: nearest_rank(&samples, 0.50),
            p95_seconds: nearest_rank(&samples, 0.95),
            p99_seconds: nearest_rank(&samples, 0.99),
            max_seconds: samples[count - 1],
        }
    }
}

fn nearest_rank(sorted: &[f64], percentile: f64) -> f64 {
    let rank = (percentile * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ByteSummary {
    pub start_bytes: Option<u64>,
    pub end_bytes: Option<u64>,
    pub high_water_bytes: Option<u64>,
}

pub type RssSummary = ByteSummary;
pub type AllocatorSummary = ByteSummary;

#[derive(Default)]
struct HighWater {
    observed: AtomicBool,
    bytes: AtomicU64,
}

impl HighWater {
    fn new(start: Option<u64>) -> Self {
        let tracker = Self::default();
        if let Some(start) = start {
            tracker.observe(start);
        }
        tracker
    }

    fn observe(&self, bytes: u64) {
        self.observed.store(true, Ordering::Relaxed);
        self.bytes.fetch_max(bytes, Ordering::Relaxed);
    }

    fn summary(&self, start: Option<u64>, end: Option<u64>) -> ByteSummary {
        ByteSummary {
            start_bytes: start,
            end_bytes: end,
            high_water_bytes: self
                .observed
                .load(Ordering::Relaxed)
                .then(|| self.bytes.load(Ordering::Relaxed)),
        }
    }
}

pub struct ProcessResourceMonitor {
    stop: Arc<AtomicBool>,
    rss: Arc<HighWater>,
    allocator: Arc<HighWater>,
    worker: Option<JoinHandle<()>>,
    rss_start: Option<u64>,
    allocator_start: Option<u64>,
}

impl ProcessResourceMonitor {
    pub fn start() -> Self {
        let rss_start = process_rss_bytes().ok();
        let allocator_start = process_allocator_bytes().ok();
        let stop = Arc::new(AtomicBool::new(false));
        let rss = Arc::new(HighWater::new(rss_start));
        let allocator = Arc::new(HighWater::new(allocator_start));
        let worker = {
            let stop = stop.clone();
            let rss = rss.clone();
            let allocator = allocator.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(bytes) = process_rss_bytes() {
                        rss.observe(bytes);
                    }
                    if let Ok(bytes) = process_allocator_bytes() {
                        allocator.observe(bytes);
                    }
                    std::thread::sleep(RESOURCE_SAMPLE_INTERVAL);
                }
            })
        };
        Self {
            stop,
            rss,
            allocator,
            worker: Some(worker),
            rss_start,
            allocator_start,
        }
    }

    pub fn finish(mut self) -> (RssSummary, AllocatorSummary) {
        let rss_end = process_rss_bytes().ok();
        let allocator_end = process_allocator_bytes().ok();
        if let Some(end) = rss_end {
            self.rss.observe(end);
        }
        if let Some(end) = allocator_end {
            self.allocator.observe(end);
        }
        self.stop_worker();
        (
            self.rss.summary(self.rss_start, rss_end),
            self.allocator.summary(self.allocator_start, allocator_end),
        )
    }

    fn stop_worker(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for ProcessResourceMonitor {
    fn drop(&mut self) {
        self.stop_worker();
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn process_rss_bytes() -> io::Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    parse_linux_rss(&status)
}

#[cfg(target_os = "linux")]
fn parse_linux_rss(status: &str) -> io::Result<u64> {
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmRSS is missing"))?
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    kib.checked_mul(1024)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmRSS overflows bytes"))
}

#[cfg(target_os = "macos")]
pub(crate) fn process_rss_bytes() -> io::Result<u64> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::uninit();
    // SAFETY: `info` points to writable storage of the exact structure requested by
    // RUSAGE_INFO_V4, and the call targets the current process. Success initializes it.
    let result = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as libc::pid_t,
            libc::RUSAGE_INFO_V4,
            info.as_mut_ptr().cast(),
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a zero return from proc_pid_rusage initialized `info`.
    Ok(unsafe { info.assume_init() }.ri_resident_size)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn process_rss_bytes() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "RSS collection is supported on Linux and macOS",
    ))
}

#[cfg(target_os = "macos")]
pub(crate) fn process_allocator_bytes() -> io::Result<u64> {
    // SAFETY: the default zone is process-global and remains valid for the process lifetime.
    let zone = unsafe { libc::malloc_default_zone() };
    if zone.is_null() {
        return Err(io::Error::other("default malloc zone is unavailable"));
    }
    let mut stats = std::mem::MaybeUninit::<libc::malloc_statistics_t>::uninit();
    // SAFETY: `zone` is valid and `stats` points to writable storage of the required type.
    unsafe { libc::malloc_zone_statistics(zone, stats.as_mut_ptr()) };
    // SAFETY: malloc_zone_statistics initializes the structure for a valid zone.
    Ok(unsafe { stats.assume_init() }.size_in_use as u64)
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) fn process_allocator_bytes() -> io::Result<u64> {
    // SAFETY: mallinfo2 takes no pointers and returns process-global allocator statistics.
    Ok(unsafe { libc::mallinfo2() }.uordblks as u64)
}

#[cfg(not(any(target_os = "macos", all(target_os = "linux", target_env = "gnu"))))]
pub(crate) fn process_allocator_bytes() -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "allocator collection is supported on macOS and GNU/Linux",
    ))
}

pub fn parse_prometheus_samples(text: &str) -> MetricSamples {
    text.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (sample, value) = line.rsplit_once(|character: char| character.is_whitespace())?;
            let value = value.parse::<f64>().ok()?;
            value
                .is_finite()
                .then(|| (sample.trim_end().to_owned(), value))
        })
        .collect()
}

pub fn metric_deltas(before: &MetricSamples, after: &MetricSamples) -> MetricSamples {
    before
        .keys()
        .chain(after.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|sample| {
            let delta = after.get(sample).copied().unwrap_or(0.0)
                - before.get(sample).copied().unwrap_or(0.0);
            (sample.clone(), delta)
        })
        .collect()
}

pub async fn scrape_metrics(
    client: &reqwest::Client,
    address: SocketAddr,
) -> reqwest::Result<MetricSamples> {
    let text = client
        .get(format!("http://{address}/metrics"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Ok(parse_prometheus_samples(&text))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NodeMetricRecord {
    pub node: String,
    pub before: MetricSamples,
    pub after: MetricSamples,
    pub delta: MetricSamples,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkRecord {
    pub schema_version: u32,
    pub run_id: String,
    pub workload: String,
    pub scale: String,
    pub write_batching: bool,
    pub client_count: usize,
    pub criterion_iterations: u64,
    pub request_kind: String,
    pub request_count: u64,
    pub row_count: Option<u64>,
    pub elapsed_seconds: f64,
    pub requests_per_second: f64,
    pub rows_per_second: Option<f64>,
    pub latency: LatencySummary,
    pub rss: RssSummary,
    #[serde(default)]
    pub allocator: AllocatorSummary,
    pub node_metrics: Vec<NodeMetricRecord>,
}

pub struct BenchmarkArtifact {
    run_id: String,
    path: PathBuf,
    output: Mutex<File>,
}

impl BenchmarkArtifact {
    fn new(directory: &Path, run_id: String) -> io::Result<Self> {
        std::fs::create_dir_all(directory)?;
        let path = directory.join("measurements.jsonl");
        let output = File::create_new(&path)?;
        eprintln!("server benchmark artifacts: {}", path.display());
        Ok(Self {
            run_id,
            path,
            output: Mutex::new(output),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn append(&self, mut record: BenchmarkRecord) -> io::Result<()> {
        record.run_id.clone_from(&self.run_id);
        let mut output = self
            .output
            .lock()
            .map_err(|_| io::Error::other("benchmark artifact lock is poisoned"))?;
        serde_json::to_writer(&mut *output, &record).map_err(io::Error::other)?;
        output.write_all(b"\n")?;
        output.flush()
    }
}

static GLOBAL_ARTIFACT: OnceLock<io::Result<BenchmarkArtifact>> = OnceLock::new();

pub fn global_artifact() -> io::Result<&'static BenchmarkArtifact> {
    match GLOBAL_ARTIFACT.get_or_init(|| {
        let run_id = format!(
            "{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            std::process::id()
        );
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let directory = std::env::var_os("NODUS_BENCH_ARTIFACT_DIR")
            .map(PathBuf::from)
            .map(|path| {
                if path.is_absolute() {
                    path
                } else {
                    workspace_root.join(path)
                }
            })
            .unwrap_or_else(|| workspace_root.join("target/server-bench").join(&run_id));
        BenchmarkArtifact::new(&directory, run_id)
    }) {
        Ok(artifact) => Ok(artifact),
        Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
    }
}

pub struct IterationMeasurement {
    workload: String,
    scale: String,
    write_batching: bool,
    client_count: usize,
    criterion_iterations: u64,
    request_kind: String,
    recorder: LatencyRecorder,
    resources: ProcessResourceMonitor,
    http: reqwest::Client,
    nodes: Vec<(String, SocketAddr)>,
    before: Vec<MetricSamples>,
}

impl IterationMeasurement {
    pub async fn begin(
        workload: &str,
        scale: &str,
        write_batching: bool,
        client_count: usize,
        criterion_iterations: u64,
        request_kind: &str,
        nodes: Vec<(String, SocketAddr)>,
    ) -> io::Result<Self> {
        let http = reqwest::Client::new();
        let mut before = Vec::with_capacity(nodes.len());
        for (_, address) in &nodes {
            before.push(
                scrape_metrics(&http, *address)
                    .await
                    .map_err(io::Error::other)?,
            );
        }
        Ok(Self {
            workload: workload.to_owned(),
            scale: scale.to_owned(),
            write_batching,
            client_count,
            criterion_iterations,
            request_kind: request_kind.to_owned(),
            recorder: LatencyRecorder::default(),
            resources: ProcessResourceMonitor::start(),
            http,
            nodes,
            before,
        })
    }

    pub fn recorder(&self) -> LatencyRecorder {
        self.recorder.clone()
    }

    pub async fn finish(
        self,
        elapsed: Duration,
        expected_requests: u64,
        row_count: Option<u64>,
    ) -> io::Result<()> {
        let mut after = Vec::with_capacity(self.nodes.len());
        for (_, address) in &self.nodes {
            after.push(
                scrape_metrics(&self.http, *address)
                    .await
                    .map_err(io::Error::other)?,
            );
        }
        let (rss, allocator) = self.resources.finish();
        let latency = self.recorder.summary();
        if latency.count != expected_requests {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} recorded {} requests, expected {expected_requests}",
                    self.workload, latency.count
                ),
            ));
        }
        let elapsed_seconds = elapsed.as_secs_f64();
        let requests_per_second = if elapsed_seconds > 0.0 {
            expected_requests as f64 / elapsed_seconds
        } else {
            0.0
        };
        let rows_per_second = row_count.map(|rows| {
            if elapsed_seconds > 0.0 {
                rows as f64 / elapsed_seconds
            } else {
                0.0
            }
        });
        let node_metrics = self
            .nodes
            .into_iter()
            .zip(self.before.into_iter().zip(after))
            .map(|((node, _), (before, after))| NodeMetricRecord {
                delta: metric_deltas(&before, &after),
                node,
                before,
                after,
            })
            .collect();
        global_artifact()?.append(BenchmarkRecord {
            schema_version: BENCHMARK_RECORD_SCHEMA_VERSION,
            run_id: String::new(),
            workload: self.workload,
            scale: self.scale,
            write_batching: self.write_batching,
            client_count: self.client_count,
            criterion_iterations: self.criterion_iterations,
            request_kind: self.request_kind,
            request_count: expected_requests,
            row_count,
            elapsed_seconds,
            requests_per_second,
            rows_per_second,
            latency,
            rss,
            allocator,
            node_metrics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_summary_uses_nearest_rank() {
        let recorder = LatencyRecorder::default();
        for milliseconds in [40, 10, 30, 20] {
            recorder.record(Duration::from_millis(milliseconds));
        }
        let summary = recorder.summary();
        assert_eq!(summary.count, 4);
        assert_eq!(summary.mean_seconds, 0.025);
        assert_eq!(summary.p50_seconds, 0.02);
        assert_eq!(summary.p95_seconds, 0.04);
        assert_eq!(summary.p99_seconds, 0.04);
        assert_eq!(summary.max_seconds, 0.04);
        assert_eq!(
            LatencyRecorder::default().summary(),
            LatencySummary::default()
        );
    }

    #[test]
    fn latency_recorder_aggregates_concurrent_workers() {
        let recorder = LatencyRecorder::default();
        let workers = (0..8)
            .map(|_| {
                let recorder = recorder.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        recorder.record(Duration::from_micros(100));
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(recorder.summary().count, 800);
    }

    #[test]
    fn parses_metric_identity_labels_and_finite_values() {
        let samples = parse_prometheus_samples(
            "# HELP ignored text\n\
             nodus_raft_log_entries{group=\"shard-meta\"} 42\n\
             nodus_queries_total_total 7.5\n\
             invalid_line\n\
             nodus_nan NaN\n",
        );
        assert_eq!(
            samples["nodus_raft_log_entries{group=\"shard-meta\"}"],
            42.0
        );
        assert_eq!(samples["nodus_queries_total_total"], 7.5);
        assert_eq!(samples.len(), 2);
    }

    #[test]
    fn metric_delta_covers_added_changed_and_removed_samples() {
        let before = BTreeMap::from([
            ("changed".into(), 4.0),
            ("removed".into(), 2.0),
            ("same".into(), 1.0),
        ]);
        let after = BTreeMap::from([
            ("added".into(), 3.0),
            ("changed".into(), 7.0),
            ("same".into(), 1.0),
        ]);
        assert_eq!(
            metric_deltas(&before, &after),
            BTreeMap::from([
                ("added".into(), 3.0),
                ("changed".into(), 3.0),
                ("removed".into(), -2.0),
                ("same".into(), 0.0),
            ])
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_rss_parser_converts_kibibytes() {
        assert_eq!(
            parse_linux_rss("Name:\tbench\nVmRSS:\t  123 kB\n").unwrap(),
            123 * 1024
        );
        assert!(parse_linux_rss("Name:\tbench\n").is_err());
    }

    #[test]
    fn high_water_tracks_start_and_largest_observation() {
        let tracker = HighWater::new(Some(10));
        tracker.observe(20);
        tracker.observe(15);
        assert_eq!(
            tracker.summary(Some(10), Some(12)),
            ByteSummary {
                start_bytes: Some(10),
                end_bytes: Some(12),
                high_water_bytes: Some(20),
            }
        );
    }

    #[cfg(any(target_os = "macos", all(target_os = "linux", target_env = "gnu")))]
    #[test]
    fn process_resource_monitor_reports_supported_allocations() {
        let monitor = ProcessResourceMonitor::start();
        let allocation = vec![1_u8; 64 * 1024];
        std::hint::black_box(&allocation);
        let (rss, allocator) = monitor.finish();
        for summary in [rss, allocator] {
            let start = summary.start_bytes.unwrap();
            let end = summary.end_bytes.unwrap();
            let high_water = summary.high_water_bytes.unwrap();
            assert!(start > 0 && end > 0);
            assert!(high_water >= start && high_water >= end);
        }
    }

    #[test]
    fn artifact_round_trip_preserves_schema_and_reads_legacy_records() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = BenchmarkArtifact::new(directory.path(), "test-run".into()).unwrap();
        artifact
            .append(BenchmarkRecord {
                schema_version: BENCHMARK_RECORD_SCHEMA_VERSION,
                run_id: String::new(),
                workload: "point_read".into(),
                scale: "smoke".into(),
                write_batching: false,
                client_count: 1,
                criterion_iterations: 1,
                request_kind: "select".into(),
                request_count: 1,
                row_count: Some(1),
                elapsed_seconds: 0.5,
                requests_per_second: 2.0,
                rows_per_second: Some(2.0),
                latency: LatencySummary {
                    count: 1,
                    mean_seconds: 0.5,
                    p50_seconds: 0.5,
                    p95_seconds: 0.5,
                    p99_seconds: 0.5,
                    max_seconds: 0.5,
                },
                rss: RssSummary::default(),
                allocator: AllocatorSummary {
                    start_bytes: Some(10),
                    end_bytes: Some(12),
                    high_water_bytes: Some(20),
                },
                node_metrics: Vec::new(),
            })
            .unwrap();
        let output = std::fs::read_to_string(artifact.path()).unwrap();
        let record: BenchmarkRecord = serde_json::from_str(output.trim_end()).unwrap();
        assert_eq!(record.schema_version, 2);
        assert_eq!(record.run_id, "test-run");
        assert_eq!(record.rss.high_water_bytes, None);
        assert_eq!(record.allocator.high_water_bytes, Some(20));

        let mut legacy: serde_json::Value = serde_json::from_str(output.trim_end()).unwrap();
        let object = legacy.as_object_mut().unwrap();
        object.insert("schema_version".into(), 1.into());
        object.remove("allocator");
        let legacy: BenchmarkRecord = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy.schema_version, 1);
        assert_eq!(legacy.allocator, AllocatorSummary::default());
    }
}
