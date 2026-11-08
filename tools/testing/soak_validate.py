#!/usr/bin/env python3
"""Independently validate and summarize mixed-activity soak artifacts.

Usage: soak_validate.py RUN_DIR [--console-log PATH]
       [--expected-sustained-seconds N] [--output PATH]

The correctness checks are duration-independent. Resource trends and error rates
are measurements only: no production budget or stabilization verdict is inferred.
JSONL and log inputs are streamed so validation does not retain the full run.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path

QUEUE_METRICS = {
    "router_queue_depth": "nodus_raft_router_queue_depth",
    "commit_queue_depth": "nodus_raft_commit_queue_depth",
}
REQUIRED_ACTIVITIES = {
    "full_backup_verified",
    "automatic_lsm_compaction",
    "first_snapshot_and_log_reclamation",
    "second_snapshot_and_log_reclamation_while_follower_down",
    "snapshot_required_follower_restart",
    "snapshot_catchup_verified",
    "split_and_rebalance_with_read_load",
    "exact_recovery_validation",
}


class Problems:
    def __init__(self, limit=100):
        self.limit = limit
        self.items = []
        self.count = 0

    def add(self, message):
        self.count += 1
        if len(self.items) < self.limit:
            self.items.append(message)

    def check(self, condition, message):
        if not condition:
            self.add(message)

    def report(self):
        return {
            "count": self.count,
            "messages": self.items,
            "omitted_messages": max(0, self.count - len(self.items)),
        }


class Trend:
    """Bounded-memory time-series summary with an ordinary-least-squares slope."""

    def __init__(self):
        self.count = 0
        self.start_x = None
        self.end_x = None
        self.start_y = None
        self.end_y = None
        self.minimum = None
        self.maximum = None
        self.mean_x = 0.0
        self.mean_y = 0.0
        self.sxx = 0.0
        self.sxy = 0.0

    def add(self, elapsed, value):
        if any(
            item is None or isinstance(item, bool) or not isinstance(item, (int, float))
            for item in (elapsed, value)
        ):
            return
        if not math.isfinite(elapsed) or not math.isfinite(value):
            return
        self.count += 1
        if self.start_x is None:
            self.start_x = elapsed
            self.start_y = value
            self.minimum = value
            self.maximum = value
        self.end_x = elapsed
        self.end_y = value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)
        delta_x = elapsed - self.mean_x
        delta_y = value - self.mean_y
        self.mean_x += delta_x / self.count
        self.mean_y += delta_y / self.count
        self.sxx += delta_x * (elapsed - self.mean_x)
        self.sxy += delta_x * (value - self.mean_y)

    def report(self):
        if self.count == 0:
            return None
        slope = self.sxy / self.sxx if self.count >= 2 and self.sxx > 0 else None
        return {
            "sample_count": self.count,
            "start_elapsed_seconds": self.start_x,
            "end_elapsed_seconds": self.end_x,
            "start": self.start_y,
            "end": self.end_y,
            "delta": self.end_y - self.start_y,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "slope_per_second": slope,
            "slope_per_hour": slope * 3600 if slope is not None else None,
        }


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path):
    try:
        with path.open() as source:
            return json.load(source)
    except FileNotFoundError as error:
        raise ValueError(f"missing required artifact: {path}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"{path} is not valid JSON: {error}") from error


def activity_map(summary, problems):
    activities = {}
    for activity in summary.get("activities", []):
        name = activity.get("name")
        if not name:
            problems.add("activity without a name")
            continue
        problems.check(name not in activities, f"duplicate activity: {name}")
        activities[name] = activity
        problems.check(activity.get("success") is True, f"activity did not pass: {name}")
    for name in sorted(REQUIRED_ACTIVITIES - set(activities)):
        problems.add(f"required activity is missing: {name}")
    return activities


def check_reclamation(activities, problems):
    for name in (
        "first_snapshot_and_log_reclamation",
        "second_snapshot_and_log_reclamation_while_follower_down",
    ):
        activity = activities.get(name)
        if not activity:
            continue
        observation = activity.get("detail", {}).get("observation", {})
        problems.check(
            observation.get("reclaimed_log_entries", 0) > 0,
            f"{name}: no Raft entries reclaimed",
        )
        problems.check(
            observation.get("reclaimed_serialized_log_bytes", 0) > 0,
            f"{name}: no serialized Raft bytes reclaimed",
        )

    split = activities.get("split_and_rebalance_with_read_load", {}).get("detail", {})
    for field in (
        "source_decommissioned_on_every_node",
        "physical_reclamation_verified",
        "source_namespaces_empty_on_every_node",
        "allocated_storage_not_increased_on_every_node",
        "allocated_reduction_observed",
    ):
        problems.check(split.get(field) is True, f"split reclamation check failed: {field}")
    problems.check(split.get("archived_wal_reclaimed") is False, "archived WAL was reclaimed")

    before = {item.get("node_id"): item for item in split.get("storage_before", [])}
    after = {item.get("node_id"): item for item in split.get("storage_after", [])}
    problems.check(set(before) == set(after), "split storage node sets differ")
    reduced = False
    for node_id in sorted(set(before) & set(after)):
        old = before[node_id].get("allocated_bytes")
        new = after[node_id].get("allocated_bytes")
        if old is None or new is None:
            problems.add(f"node {node_id}: split allocated storage is unavailable")
            continue
        problems.check(new <= old, f"node {node_id}: allocated storage increased after split")
        reduced |= new < old
    problems.check(reduced, "no allocated-storage reduction was observed after split")


def check_workload(summary, activities, expected_sustained_seconds, problems):
    configuration = summary.get("configuration", {})
    workload = summary.get("workload", {})
    requested = configuration.get("sustained_load_seconds")
    problems.check(
        isinstance(requested, int) and not isinstance(requested, bool) and requested >= 0,
        "sustained_load_seconds must be a nonnegative integer",
    )
    if not isinstance(requested, int) or isinstance(requested, bool) or requested < 0:
        return None
    if expected_sustained_seconds is not None:
        problems.check(
            requested == expected_sustained_seconds,
            f"configured sustained duration {requested} does not equal expected "
            f"{expected_sustained_seconds}",
        )

    base_rows = configuration.get("total_insert_target")
    valid_base_rows = isinstance(base_rows, int) and not isinstance(base_rows, bool) and base_rows >= 0
    problems.check(valid_base_rows, "total_insert_target must be a nonnegative integer")
    problems.check(
        workload.get("target_insert_count") == base_rows,
        "workload and configuration base-row targets differ",
    )
    sustained = activities.get("sustained_load")
    if requested == 0:
        problems.check(sustained is None, "zero-duration run contains a sustained activity")
        sustained_rows = 0
    else:
        if sustained is None:
            problems.add("sustained_load activity is missing")
            return None
        detail = sustained.get("detail", {})
        actual_seconds = detail.get("actual_seconds")
        throughput = detail.get("acknowledged_inserts_per_second")
        problems.check(detail.get("requested_seconds") == requested, "sustained duration differs")
        problems.check(
            isinstance(actual_seconds, (int, float)) and actual_seconds >= requested,
            "sustained window ended before the requested duration",
        )
        sustained_rows = detail.get("acknowledged_inserts")
        valid_sustained_rows = (
            isinstance(sustained_rows, int)
            and not isinstance(sustained_rows, bool)
            and sustained_rows > 0
        )
        problems.check(valid_sustained_rows, "no sustained writes were acknowledged")
        problems.check(
            isinstance(throughput, (int, float)) and throughput > 0,
            "sustained throughput was not measured",
        )
        problems.check(
            detail.get("production_budgets_met") is None,
            "sustained activity overclaimed production budgets",
        )
        if valid_base_rows and valid_sustained_rows:
            problems.check(
                detail.get("total_acknowledged") == base_rows + sustained_rows,
                "sustained total does not reconcile",
            )
        problems.check(
            workload.get("sustained_insert_count") == sustained_rows,
            "workload sustained count does not reconcile",
        )
        if not valid_sustained_rows:
            sustained_rows = 0

    expected_rows = base_rows + sustained_rows if valid_base_rows else None
    problems.check(
        workload.get("acknowledged_insert_count") == expected_rows,
        "workload total does not reconcile",
    )
    validation = summary.get("validation", {})
    problems.check(
        validation.get("exact_acknowledged_multiset_on_every_node") is True,
        "exact acknowledged-row validation is missing",
    )
    problems.check(validation.get("expected_rows") == expected_rows, "exact-row target differs")
    nodes = validation.get("nodes", [])
    node_count = configuration.get("nodes")
    valid_node_count = (
        isinstance(node_count, int) and not isinstance(node_count, bool) and node_count > 0
    )
    problems.check(valid_node_count, "configuration nodes must be a positive integer")
    expected_node_ids = set(range(1, node_count + 1)) if valid_node_count else set()
    problems.check(
        {node.get("node_id") for node in nodes} == expected_node_ids,
        "validation node set differs from the configured cluster",
    )
    for node in nodes:
        node_id = node.get("node_id")
        problems.check(node.get("rows") == expected_rows, f"node {node_id}: wrong row count")
        for field in ("missing", "unexpected", "duplicates", "mismatched_values"):
            problems.check(node.get(field) == 0, f"node {node_id}: {field} rows were reported")
        problems.check(
            node.get("indexed_rows") == 1 and node.get("indexed_lookup_verified") is True,
            f"node {node_id}: secondary-index validation failed",
        )
    return expected_rows


class EventAnalyzer:
    def __init__(self, node_count, sustained_requested, problems):
        self.node_ids = set(range(1, node_count + 1))
        self.sustained_requested = sustained_requested
        self.problems = problems
        self.event_count = 0
        self.sample_count = 0
        self.sustained_sample_count = 0
        self.quiet_sample_count = 0
        self.storage_errors = 0
        self.rss = Trend()
        self.allocator = Trend()
        self.storage = {
            node_id: {
                "logical_bytes": Trend(),
                "allocated_bytes": Trend(),
                "volume_available_bytes": Trend(),
            }
            for node_id in self.node_ids
        }
        self.queues = {
            node_id: {name: Trend() for name in QUEUE_METRICS} for node_id in self.node_ids
        }

    def analyze(self, path):
        try:
            source = path.open()
        except FileNotFoundError as error:
            raise ValueError(f"missing required artifact: {path}") from error
        with source:
            for line_number, line in enumerate(source, 1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f"{path}:{line_number} is not valid JSON: {error}") from error
                self.event_count += 1
                self.problems.check(
                    event.get("schema_version") == 1,
                    f"event {self.event_count}: unexpected schema version",
                )
                if event.get("kind") == "sample":
                    self.sample(event)

    def sample(self, event):
        self.sample_count += 1
        phase = event.get("detail", {}).get("phase")
        if phase == "post_activity_observation":
            self.quiet_sample_count += 1
            self.check_quiet(event)
        if phase != "sustained_load":
            return
        self.sustained_sample_count += 1
        elapsed = event.get("elapsed_seconds")
        self.rss.add(elapsed, event.get("rss_bytes"))
        self.allocator.add(elapsed, event.get("allocator_bytes"))
        nodes = event.get("nodes", [])
        actual_ids = [node.get("node_id") for node in nodes]
        self.problems.check(
            len(actual_ids) == len(set(actual_ids)) and set(actual_ids) == self.node_ids,
            f"sustained sample {self.sustained_sample_count}: unexpected node set",
        )
        for node in nodes:
            node_id = node.get("node_id")
            if node_id not in self.node_ids:
                continue
            metrics = node.get("metrics")
            self.problems.check(
                metrics is not None,
                f"sustained sample {self.sustained_sample_count}: node {node_id} metrics missing",
            )
            if metrics is not None:
                for name, metric in QUEUE_METRICS.items():
                    self.problems.check(
                        metric in metrics,
                        f"sustained sample: node {node_id} omitted {metric}",
                    )
                    self.queues[node_id][name].add(elapsed, metrics.get(metric))
            storage = node.get("storage", {})
            if storage.get("error") is not None:
                self.storage_errors += 1
            usage = storage.get("usage") or {}
            self.storage[node_id]["logical_bytes"].add(elapsed, usage.get("logical_bytes"))
            self.storage[node_id]["allocated_bytes"].add(elapsed, usage.get("allocated_bytes"))
            self.storage[node_id]["volume_available_bytes"].add(
                elapsed, storage.get("volume_available_bytes")
            )

    def check_quiet(self, event):
        nodes = event.get("nodes", [])
        actual_ids = [node.get("node_id") for node in nodes]
        self.problems.check(
            len(actual_ids) == len(set(actual_ids)) and set(actual_ids) == self.node_ids,
            "quiet sample has an unexpected node set",
        )
        for node in nodes:
            node_id = node.get("node_id")
            metrics = node.get("metrics")
            self.problems.check(metrics is not None, f"quiet sample omitted node {node_id} metrics")
            if metrics is None:
                continue
            for metric in QUEUE_METRICS.values():
                self.problems.check(
                    metrics.get(metric, 0) == 0,
                    f"quiet queue {metric} is nonzero on node {node_id}",
                )

    def finish(self):
        self.problems.check(self.event_count > 0, "events.jsonl contains no events")
        self.problems.check(self.quiet_sample_count > 0, "no quiet-window samples")
        if self.sustained_requested:
            self.problems.check(self.sustained_sample_count > 0, "no sustained resource samples")
            for name, trend in (("RSS", self.rss), ("allocator", self.allocator)):
                self.problems.check(
                    trend.count >= 2, f"insufficient sustained {name} samples for a trend"
                )
            for node_id in self.node_ids:
                for name in ("logical_bytes", "allocated_bytes"):
                    self.problems.check(
                        self.storage[node_id][name].count >= 2,
                        f"node {node_id}: insufficient sustained {name} samples for a trend",
                    )

    def report(self):
        return {
            "event_count": self.event_count,
            "sample_count": self.sample_count,
            "sustained_sample_count": self.sustained_sample_count,
            "post_activity_sample_count": self.quiet_sample_count,
            "storage_error_count": self.storage_errors,
            "sustained_process": {
                "rss_bytes": self.rss.report(),
                "allocator_bytes": self.allocator.report(),
                "bounded": None,
                "stabilized": None,
            },
            "sustained_storage": [
                {
                    "node_id": node_id,
                    **{name: trend.report() for name, trend in trends.items()},
                    "bounded": None,
                    "stabilized": None,
                }
                for node_id, trends in sorted(self.storage.items())
            ],
            "sustained_queues": [
                {
                    "node_id": node_id,
                    **{name: trend.report() for name, trend in trends.items()},
                    "bounded": None,
                }
                for node_id, trends in sorted(self.queues.items())
            ],
        }


def ratio(numerator, denominator):
    if any(
        value is None or isinstance(value, bool) or not isinstance(value, (int, float))
        for value in (numerator, denominator)
    ):
        return None
    return numerator / denominator if denominator > 0 else None


def error_measurements(workload):
    successful_reads = workload.get("successful_reads")
    read_retries = workload.get("read_retries")
    read_attempts = (
        successful_reads + read_retries
        if isinstance(successful_reads, (int, float)) and isinstance(read_retries, (int, float))
        else None
    )
    return {
        "write_retry_rate": ratio(workload.get("write_retries"), workload.get("write_attempts")),
        "read_retry_rate": ratio(read_retries, read_attempts),
        "recovered_uncertain_writes": workload.get("recovered_uncertain_writes"),
        "budget_met": None,
    }


def analyze_console(path, problems):
    result = {
        "raft_rpc_errors": 0,
        "connection_refused": 0,
        "http_404": 0,
        "json_decode_errors": 0,
        "immediate_retry_classifications": 0,
        "other_raft_rpc_errors": 0,
        "budget_met": None,
    }
    try:
        source = path.open(errors="replace")
    except FileNotFoundError as error:
        raise ValueError(f"missing console log: {path}") from error
    with source:
        for line in source:
            if "openraft::replication" not in line or "RPCError err=" not in line:
                continue
            result["raft_rpc_errors"] += 1
            requires_backoff = False
            if "Connection refused" in line:
                result["connection_refused"] += 1
                requires_backoff = True
            elif "404 Not Found" in line:
                result["http_404"] += 1
                requires_backoff = True
            elif "error decoding response body" in line:
                result["json_decode_errors"] += 1
            else:
                result["other_raft_rpc_errors"] += 1
            if requires_backoff and "RPCError err=Unreachable node:" not in line:
                result["immediate_retry_classifications"] += 1
    problems.check(result["json_decode_errors"] == 0, "JSON decoding errors returned")
    problems.check(
        result["immediate_retry_classifications"] == 0,
        "connection refusals or HTTP 404s retained immediate-retry classification",
    )
    return result


def validate_run(run_dir, console_log=None, expected_sustained_seconds=None):
    run_dir = Path(run_dir)
    summary_path = run_dir / "summary.json"
    events_path = run_dir / "events.jsonl"
    summary = load_json(summary_path)
    problems = Problems()
    problems.check(summary.get("schema_version") == 1, "unexpected summary schema version")
    problems.check(summary.get("status") == "passed", "drill did not pass")
    problems.check(summary.get("error") is None, "summary contains an error")
    for field in ("bounded_memory", "bounded_queues", "production_budgets_met"):
        problems.check(summary.get("assessment", {}).get(field) is None, f"{field} was overclaimed")
    problems.check(
        summary.get("resources", {}).get("storage_sampling", {}).get("physical_storage_bounded")
        is None,
        "physical storage was overclaimed",
    )

    activities = activity_map(summary, problems)
    check_reclamation(activities, problems)
    expected_rows = check_workload(
        summary, activities, expected_sustained_seconds, problems
    )
    sustained_seconds = summary.get("configuration", {}).get("sustained_load_seconds")
    node_count = summary.get("configuration", {}).get("nodes")
    if not isinstance(node_count, int) or isinstance(node_count, bool) or node_count <= 0:
        node_count = 0
    events = EventAnalyzer(
        node_count,
        bool(sustained_seconds) if isinstance(sustained_seconds, int) else False,
        problems,
    )
    events.analyze(events_path)
    events.finish()
    transport = analyze_console(Path(console_log), problems) if console_log else None

    started = summary.get("started_at_unix_micros")
    finished = summary.get("finished_at_unix_micros")
    duration = (finished - started) / 1_000_000 if started is not None and finished is not None else None
    sustained = activities.get("sustained_load")
    hashes = {"events_jsonl": sha256(events_path), "summary_json": sha256(summary_path)}
    if console_log:
        hashes["console_log"] = sha256(Path(console_log))
    return {
        "schema_version": 1,
        "status": "passed" if problems.count == 0 else "failed",
        "run_id": summary.get("run_id"),
        "source": summary.get("source"),
        "started_at_unix_micros": started,
        "finished_at_unix_micros": finished,
        "duration_seconds": duration,
        "expected_sustained_seconds": expected_sustained_seconds,
        "correctness": problems.report(),
        "sustained_activity": sustained.get("detail") if sustained else None,
        "workload": summary.get("workload"),
        "validation": summary.get("validation"),
        "expected_rows": expected_rows,
        "resources": {
            "whole_run": summary.get("resources"),
            **events.report(),
        },
        "error_measurements": error_measurements(summary.get("workload", {})),
        "transport_errors": transport,
        "assessment": {
            "bounded_memory": None,
            "bounded_queues": None,
            "physical_storage_bounded": None,
            "error_budgets_met": None,
            "production_budgets_met": None,
            "note": "Correctness and artifact checks do not establish production budgets or resource stabilization.",
        },
        "hashes": hashes,
    }


def nonnegative_integer(value):
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a nonnegative integer")
    return parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="directory containing summary.json and events.jsonl")
    parser.add_argument("--console-log", type=Path, help="captured test stdout/stderr")
    parser.add_argument("--expected-sustained-seconds", type=nonnegative_integer)
    parser.add_argument("--output", type=Path, help="report path; defaults to RUN_DIR/validation.json")
    args = parser.parse_args()
    output = args.output or args.run_dir / "validation.json"
    try:
        report = validate_run(args.run_dir, args.console_log, args.expected_sustained_seconds)
        with output.open("x") as destination:
            json.dump(report, destination, indent=2)
            destination.write("\n")
    except (OSError, ValueError) as error:
        raise SystemExit(f"FAIL: {error}") from error
    print(
        f"{report['status'].upper()}: {report['resources']['event_count']} events, "
        f"{report['resources']['sustained_sample_count']} sustained samples, "
        f"expected rows={report['expected_rows']}, "
        f"correctness failures={report['correctness']['count']}"
    )
    print(f"report: {output}")
    if report["status"] != "passed":
        for message in report["correctness"]["messages"]:
            print(f"FAIL: {message}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
