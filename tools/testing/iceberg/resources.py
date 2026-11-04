#!/usr/bin/env python3
"""Measure real Iceberg scans, exact results, SIGINT and deadlines on a private stack."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import sys
import tempfile
import time

import pyarrow as pa

from auth import assert_redacted, safe_detail
from measurement import DelayedS3, POLL_SECONDS, measure
from run import Gate, logical_type
from stack import COMPOSE, ROOT, Stack, command

MIB = 1024 * 1024
REPETITIONS = 3
SCHEMA = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("label", pa.string()),
    pa.field("payload", pa.binary()),
])
WORKLOAD = {
    "version": 1,
    "batch_size": 1024,
    "row_group_limit": 4096,
    "repetitions": REPETITIONS,
    "scenarios": {
        "small": {"files": 32, "rows_per_file": 4096, "width": 512},
        "large": {"files": 32, "rows_per_file": 32768, "width": 512},
        "many_files": {"files": 256, "rows_per_file": 1024, "width": 512},
        "wide": {"files": 32, "rows_per_file": 1024, "width": 8192},
    },
}
WORKLOAD_HASH = hashlib.sha256(json.dumps(WORKLOAD, sort_keys=True).encode()).hexdigest()
BUDGETS = Path(__file__).with_name("resource-budgets.json")


def label(row):
    return None if row % 17 == 0 else "" if row % 19 == 0 else f"row-{row}"


def payload(row, width):
    return None if row % 23 == 0 else hashlib.shake_256(f"row-{row}".encode()).digest(width)


def verify(batches, rows, width):
    """Exact values and exact ID coverage, independent of file/row order.

    One byte per expected row, bounded by these fixed workloads. No whole-result
    materialization, lossy aggregate or probabilistic multiset hash is used.
    """
    seen = bytearray(rows)
    count = 0
    for batch in batches:
        assert batch.schema.names == SCHEMA.names, "resource output column order differs"
        assert [logical_type(f.type) for f in batch.schema] == [
            "int64", "string", "binary"
        ], "resource output types differ"
        values = batch.to_pydict()
        for row, name, data in zip(*[values[n] for n in SCHEMA.names], strict=True):
            assert isinstance(row, int) and 0 <= row < rows, "invalid resource row ID"
            assert not seen[row], "resource output duplicated a row"
            assert name == label(row), "resource label/null differs"
            assert data == payload(row, width), "resource payload/null differs"
            seen[row] = 1
            count += 1
    assert count == rows and all(seen), "resource output omitted rows"
    return count


def describe(values):
    mean = statistics.mean(values)
    deviation = statistics.stdev(values)
    # Three repetitions, df=2 Student-t interval; descriptive, not a production SLO.
    radius = 4.302653 * deviation / math.sqrt(len(values))
    return {
        "min": min(values), "median": statistics.median(values), "max": max(values),
        "mean": mean, "sample_stdev": deviation,
        "mean_95_percent_interval": [max(0, mean - radius), mean + radius],
    }


def check_budget(metrics, budget):
    for metric, limit in budget.items():
        value = metrics[metric]
        assert value is not None and math.isfinite(value) and value <= limit, (
            f"resource budget exceeded: {metric}"
        )


def load_budgets(path):
    policy = json.loads(path.read_text())
    assert policy["version"] == 1 and policy["workload_sha256"] == WORKLOAD_HASH, (
        "resource budgets do not match this workload"
    )
    for section in [*policy["scenarios"].values(), policy["scaling"], policy["interrupts"]]:
        assert section and all(
            isinstance(value, (int, float)) and math.isfinite(value) and value > 0
            for value in section.values()
        ), "resource budgets must contain finite positive limits"
    assert set(policy["scenarios"]) == set(WORKLOAD["scenarios"])
    assert all(set(limits) == {
        "peak_rss_bytes", "first_batch_seconds", "elapsed_seconds"
    } for limits in policy["scenarios"].values()), "incomplete scenario budgets"
    assert set(policy["scaling"]) == {"rss_ratio", "rss_delta_bytes"}
    assert set(policy["interrupts"]) == {
        "sigint_exit_seconds", "deadline_grace_seconds"
    }
    assert policy["build_profile"] == "debug", "resource gate budgets require the dev build"
    return policy


def host_context():
    host = {
        "system": platform.system(), "release": platform.release(),
        "machine": platform.machine(), "logical_cpus": os.cpu_count(),
        "python": platform.python_version(), "pyarrow": pa.__version__,
        "local_rustc": command(["rustc", "-Vv"]).stdout.strip(),
    }
    if sys.platform == "darwin":
        host["cpu_model"] = command([
            "/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"
        ]).stdout.strip()
        host["memory_bytes"] = int(command([
            "/usr/sbin/sysctl", "-n", "hw.memsize"
        ]).stdout)
    else:
        host["cpu_model"] = next((
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ), platform.machine())
        memory = next(
            line for line in Path("/proc/meminfo").read_text().splitlines()
            if line.startswith("MemTotal:")
        )
        host["memory_bytes"] = int(memory.split()[1]) * 1024
    host["docker"] = json.loads(command([
        "docker", "info", "--format",
        '{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}},"storage_driver":{{json .Driver}}}',
    ]).stdout)
    disk = shutil.disk_usage(tempfile.gettempdir())
    host["scratch_disk_total_bytes"] = disk.total
    host["scratch_disk_free_bytes"] = disk.free
    return host


class ResourceGate:
    def __init__(self, gate, report, policy):
        self.gate = gate
        self.report = report
        self.policy = policy
        self.tables = {}
        self.work = gate.directory / "measured"
        self.work.mkdir()

    def create(self, name, spec):
        table = self.gate.catalog.create_table(
            ("interop", "resource_" + name), schema=SCHEMA,
            properties={
                "write.parquet.row-group-limit": str(WORKLOAD["row_group_limit"]),
                "write.target-file-size-bytes": str(32 * MIB),
                "write.parquet.compression-codec": "zstd",
            },
        )
        chunk = spec["rows_per_file"]
        for file in range(spec["files"]):
            ids = range(file * chunk, (file + 1) * chunk)
            batch = pa.Table.from_arrays([
                pa.array(ids, type=pa.int64()),
                pa.array([label(row) for row in ids], type=pa.string()),
                pa.array([payload(row, spec["width"]) for row in ids], type=pa.binary()),
            ], schema=SCHEMA)
            table.append(batch)
        tasks = list(table.scan().plan_files())
        assert len(tasks) == spec["files"], "writer changed the expected file layout"
        rows = spec["files"] * chunk
        # Independent reader verifies the immutable source once before sampling.
        verify(table.scan().to_arrow_batch_reader(), rows, spec["width"])
        self.tables[name] = table
        self.report["datasets"][name] = {
            **spec, "rows": rows, "snapshot_id": table.current_snapshot().snapshot_id,
            "data_files": len(tasks),
            "parquet_bytes": sum(t.file.file_size_in_bytes for t in tasks),
            "manifest_count": len(table.current_snapshot().manifests(table.io)),
            "independent_reader_verified": True,
        }

    def scan(self, name, sample, *, storage=None, interrupt=False, timeout=120):
        output = self.work / "scan.arrow"
        assert not output.exists(), "resource destination already exists"
        before = set(self.work.iterdir())
        args = [
            self.gate.binary, "iceberg", "scan", "--catalog-uri",
            self.gate.stack.url + "/catalog", "--warehouse", "interop",
            "--namespace", "interop", "--table", "resource_" + name,
            "--storage-properties", storage or self.gate.storage,
            "--batch-size", str(WORKLOAD["batch_size"]),
            "--timeout-seconds", str(timeout), "--output", output,
        ]
        metrics, stdout, stderr = measure(
            args, self.gate.env, self.work, output, interrupt=interrupt,
            wall_timeout=timeout + 10,
        )
        record = {"scenario": name, "sample": sample, **metrics, "verified": False}
        self.report["measurements"].append(record)
        assert_redacted(stdout + stderr, self.gate.stack.secrets + self.gate.credentials)
        assert metrics["peak_rss_bytes"] > 0, "missing native CLI RSS measurement"
        assert metrics["first_batch_seconds"] is not None, "no complete IPC batch observed"
        assert 0 < metrics["first_batch_rows"] <= WORKLOAD["batch_size"]
        if interrupt or timeout < 120:
            reason = "Iceberg read cancelled" if interrupt else "read deadline exceeded"
            assert metrics["exit_code"] != 0 and reason in stderr, (
                "scan failed for an unexpected reason"
            )
            assert not output.exists() and set(self.work.iterdir()) == before, (
                "interrupted scan left published or temporary output"
            )
            if interrupt:
                assert metrics["signal_sent"], "scan completed before SIGINT was sent"
            record["verified"] = True
            if self.policy:
                key = "sigint_exit_seconds" if interrupt else "deadline_grace_seconds"
                duration = (
                    metrics["signal_exit_seconds"] if interrupt
                    else max(0, metrics["elapsed_seconds"] - timeout)
                )
                assert duration <= self.policy["interrupts"][key], (
                    "interrupted scan exceeded exit budget"
                )
            return record
        assert metrics["exit_code"] == 0, "resource CLI scan failed"
        summary = json.loads(stdout)
        dataset = self.report["datasets"][name]
        assert summary["snapshot_id"] == dataset["snapshot_id"]
        assert summary["rows"] == dataset["rows"]
        assert summary["version"] == 1 and summary["format"] == "arrow-ipc-stream"
        with output.open("rb") as file:
            verify(pa.ipc.open_stream(file), dataset["rows"], dataset["width"])
        record["output_bytes"] = output.stat().st_size
        record["verified"] = True
        output.unlink()
        assert set(self.work.iterdir()) == before, "successful scan left temporary output"
        if self.policy and isinstance(sample, int):
            check_budget(metrics, self.policy["scenarios"][name])
        return record

    def run(self, checkpoint):
        for name, spec in WORKLOAD["scenarios"].items():
            self.report["phase"] = "create_" + name
            print("Creating resource dataset: " + name, flush=True)
            self.create(name, spec)
            for sample in range(REPETITIONS):
                self.report["phase"] = f"scan_{name}_{sample}"
                record = self.scan(name, sample)
                print(
                    f"MEASURE {name}/{sample}: RSS={record['peak_rss_bytes'] / MIB:.1f} MiB "
                    f"first={record['first_batch_seconds']:.3f}s "
                    f"scan={record['elapsed_seconds']:.3f}s", flush=True,
                )
                checkpoint()
        self.report["phase"] = "scaling"
        summaries = {}
        for name in WORKLOAD["scenarios"]:
            samples = [m for m in self.report["measurements"] if m["scenario"] == name]
            summaries[name] = {
                metric: describe([sample[metric] for sample in samples])
                for metric in ["peak_rss_bytes", "first_batch_seconds", "elapsed_seconds"]
            }
        self.report["summaries"] = summaries
        small = summaries["small"]["peak_rss_bytes"]["median"]
        large = summaries["large"]["peak_rss_bytes"]["median"]
        self.report["scaling"] = {
            "rss_ratio": large / small, "rss_delta_bytes": max(0, large - small),
            "row_ratio": (
                self.report["datasets"]["large"]["rows"]
                / self.report["datasets"]["small"]["rows"]
            ),
            "fixed_file_count": self.report["datasets"]["small"]["data_files"],
        }
        if self.policy:
            check_budget(self.report["scaling"], self.policy["scaling"])
        with DelayedS3(self.gate.stack.s3_url) as proxy:
            storage = self.gate.write_properties(
                "paced-storage.json", {**self.gate.properties, "s3.endpoint": proxy.url}
            )
            self.report["phase"] = "paced_control"
            self.scan("small", "paced_control", storage=storage)
            for mode in ["sigint", "deadline"]:
                self.report["phase"] = mode
                before = proxy.data_gets
                self.scan(
                    "many_files", mode, storage=storage,
                    interrupt=mode == "sigint", timeout=2 if mode == "deadline" else 120,
                )
                assert proxy.data_gets > before, "interrupt did not reach Parquet reads"
                checkpoint()
            self.report["pacing"] = {
                "parquet_get_delay_seconds": proxy.delay,
                "parquet_gets": proxy.data_gets,
            }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/nodus_cli")
    parser.add_argument(
        "--report", type=Path,
        default=ROOT / "target/iceberg-live/resource-report.json",
    )
    parser.add_argument("--budgets", type=Path, default=BUDGETS)
    parser.add_argument("--record-baseline", action="store_true")
    args = parser.parse_args()
    if not __debug__ or sys.platform not in ("linux", "darwin"):
        parser.error("run unoptimized Python on Linux or macOS")
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error("build the optional Iceberg CLI first")
    with binary.open("rb") as file:
        binary_hash = hashlib.file_digest(file, "sha256").hexdigest()
    policy = None
    report = {
        "version": 1, "status": "failed", "phase": "startup", "cleanup": "not_started",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "budgets_enforced": False,
        "workload": WORKLOAD, "workload_sha256": WORKLOAD_HASH,
        "budgets": policy,
        "binary_sha256": binary_hash,
        "build_profile": binary.parent.name,
        "compose_sha256": hashlib.sha256(COMPOSE.read_bytes()).hexdigest(),
        "lock_sha256": hashlib.sha256(Path(__file__).with_name("uv.lock").read_bytes()).hexdigest(),
        "host": {},
        "method": {
            "rss": "wait4.ru_maxrss for exact posix_spawn child",
            "rss_native_unit": "bytes" if sys.platform == "darwin" else "KiB",
            "first_batch": "first complete IPC batch visible to polling observer",
            "poll_seconds": POLL_SECONDS,
            "cache": "no flush; independent source read before sampling",
        },
        "datasets": {}, "measurements": [],
    }
    stack = Stack()
    stack_attempted = False
    gate = None

    def save():
        encoded = json.dumps(report, indent=2, allow_nan=False) + "\n"
        assert_redacted(encoded, stack.secrets + (gate.credentials if gate else []))
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(encoded)

    def interrupted(*_):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, interrupted)
    save()
    try:
        report["phase"] = "configuration"
        policy = None if args.record_baseline else load_budgets(args.budgets)
        if policy:
            assert binary.parent.name == policy["build_profile"], (
                "binary profile differs from resource budgets"
            )
        report["budgets"] = policy
        report["budgets_enforced"] = policy is not None
        report["host"] = host_context()
        report["phase"] = "startup"
        stack_attempted = True
        stack.start()
        with tempfile.TemporaryDirectory(prefix="nodus-iceberg-resource-") as directory:
            gate = Gate(stack, binary, Path(directory))
            ResourceGate(gate, report, policy).run(save)
        report["phase"] = "complete"
        report["status"] = "baseline_recorded" if args.record_baseline else "passed"
    except (Exception, KeyboardInterrupt) as error:
        report["failure"] = {"type": type(error).__name__}
        detail = safe_detail(error, stack.secrets + (gate.credentials if gate else []))
        if detail:
            report["failure"]["detail"] = detail
        print(f"FAIL {report['phase']}: {type(error).__name__}", file=sys.stderr)
    finally:
        try:
            if stack_attempted:
                stack.close()
                report["cleanup"] = "passed"
            else:
                stack.reservation.close()
                report["cleanup"] = "not_needed"
        except Exception:
            report["cleanup"] = "failed"
            report["status"] = "failed"
            print("Cleanup failed for Compose project " + stack.project, file=sys.stderr)
        save()
    print(f"Resource gate: {report['status']}; report: {args.report}", flush=True)
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
