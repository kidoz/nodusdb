"""Reject false correctness, resource, first-batch and child-cleanup evidence."""

import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa

from measurement import first_batch_rows, measure, rss_bytes
from resources import (
    SCHEMA, WORKLOAD, WORKLOAD_HASH, check_budget, label, load_budgets, payload, verify,
)


def batch(ids):
    return pa.RecordBatch.from_arrays([
        pa.array(ids, type=pa.int64()),
        pa.array([label(row) for row in ids], type=pa.string()),
        pa.array([payload(row, 32) for row in ids], type=pa.binary()),
    ], schema=SCHEMA)


class ExactResultChecks(unittest.TestCase):
    def test_every_value_and_duplicate_and_missing_row_are_checked(self):
        self.assertEqual(verify([batch([2, 0, 1])], 3, 32), 3)
        for ids in [[0, 1], [0, 1, 1], [0, 1, 3]]:
            with self.subTest(ids=ids), self.assertRaises(AssertionError):
                verify([batch(ids)], 3, 32)
        changed = batch([0, 1, 2]).to_pydict()
        changed["payload"][1] = b"wrong"
        with self.assertRaises(AssertionError):
            verify([pa.RecordBatch.from_pydict(changed, schema=SCHEMA)], 3, 32)

    def test_schema_and_partial_ipc_do_not_count_as_a_first_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "result.arrow"
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, SCHEMA)
            output.write_bytes(sink.getvalue().to_pybytes())
            self.assertIsNone(first_batch_rows(directory, output))
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, SCHEMA) as writer:
                writer.write_batch(batch([1]))
            complete = sink.getvalue().to_pybytes()
            output.write_bytes(complete[:-24])
            self.assertIsNone(first_batch_rows(directory, output))
            output.write_bytes(complete)
            self.assertEqual(first_batch_rows(directory, output), 1)


class BudgetChecks(unittest.TestCase):
    def test_missing_nonfinite_and_exceeded_measurements_cannot_pass(self):
        for value in [None, float("nan"), float("inf"), 101]:
            with self.subTest(value=value), self.assertRaises(AssertionError):
                check_budget({"peak_rss_bytes": value}, {"peak_rss_bytes": 100})
        check_budget({"peak_rss_bytes": 99}, {"peak_rss_bytes": 100})

    def test_policy_cannot_silently_drop_memory_or_latency_limits(self):
        policy = {
            "version": 1, "workload_sha256": WORKLOAD_HASH, "build_profile": "debug",
            "scenarios": {name: {"elapsed_seconds": 1} for name in WORKLOAD["scenarios"]},
            "scaling": {"rss_ratio": 2, "rss_delta_bytes": 1},
            "interrupts": {"sigint_exit_seconds": 1, "deadline_grace_seconds": 1},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.json"
            path.write_text(json.dumps(policy))
            with self.assertRaises(AssertionError):
                load_budgets(path)
            for limits in policy["scenarios"].values():
                limits.update({"peak_rss_bytes": 100, "first_batch_seconds": 1})
            path.write_text(json.dumps(policy))
            self.assertEqual(load_budgets(path), policy)
            policy["workload_sha256"] = "changed-workload"
            path.write_text(json.dumps(policy))
            with self.assertRaises(AssertionError):
                load_budgets(path)

    def test_platform_rss_units(self):
        self.assertEqual(rss_bytes(1024, "darwin"), 1024)
        self.assertEqual(rss_bytes(1024, "linux"), 1024 * 1024)

    def test_missing_policy_fails_with_report_without_starting_services(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            result = subprocess.run([
                sys.executable, "-B", str(Path(__file__).with_name("resources.py")),
                "--binary", sys.executable, "--budgets", directory + "/absent.json",
                "--report", str(report),
            ], capture_output=True, timeout=20)
            self.assertEqual(result.returncode, 1)
            evidence = json.loads(report.read_text())
            self.assertEqual(evidence["status"], "failed")
            self.assertEqual(evidence["phase"], "configuration")
            self.assertFalse(evidence["budgets_enforced"])
            self.assertEqual(evidence["cleanup"], "not_needed")


@unittest.skipUnless(sys.platform in ("darwin", "linux"), "POSIX resource gate")
class NativeMeasurementChecks(unittest.TestCase):
    def test_native_rss_is_per_child_and_does_not_accumulate(self):
        peaks = []
        with tempfile.TemporaryDirectory() as directory:
            for mib in [32, 1]:
                code = (
                    "import time; "
                    f"a=bytearray({mib}*1024*1024); "
                    "a[::4096]=b'x'*(len(a)//4096); time.sleep(0.05)"
                )
                metrics, _, _ = measure(
                    [sys.executable, "-c", code], os.environ.copy(),
                    directory, Path(directory) / "none.arrow",
                )
                self.assertEqual(metrics["exit_code"], 0)
                peaks.append(metrics["peak_rss_bytes"])
        self.assertGreaterEqual(peaks[0], 32 * 1024 * 1024)
        self.assertLess(peaks[1], peaks[0])

    def test_harness_timeout_kills_and_reaps_its_child(self):
        original = os.posix_spawn
        children = []

        def remember(*args, **kwargs):
            pid = original(*args, **kwargs)
            children.append(pid)
            return pid

        with tempfile.TemporaryDirectory() as directory:
            with patch("measurement.os.posix_spawn", side_effect=remember):
                with self.assertRaises(TimeoutError):
                    measure(
                        [sys.executable, "-c", "import time; time.sleep(30)"],
                        os.environ.copy(), directory, Path(directory) / "none.arrow",
                        wall_timeout=0.05,
                    )
        self.assertEqual(len(children), 1)
        with self.assertRaises(ChildProcessError):
            os.waitpid(children[0], os.WNOHANG)


if __name__ == "__main__":
    unittest.main()
