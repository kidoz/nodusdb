"""soak_validate regression checks: python3 -B -m unittest discover -s tools/testing."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

import soak_validate


def activity(name, detail=None):
    return {"name": name, "success": True, "detail": detail or {}}


def summary_fixture():
    activities = [
        activity("full_backup_verified"),
        activity("automatic_lsm_compaction"),
        activity(
            "first_snapshot_and_log_reclamation",
            {"observation": {"reclaimed_log_entries": 10, "reclaimed_serialized_log_bytes": 100}},
        ),
        activity("graceful_follower_stop"),
        activity(
            "second_snapshot_and_log_reclamation_while_follower_down",
            {"observation": {"reclaimed_log_entries": 20, "reclaimed_serialized_log_bytes": 200}},
        ),
        activity("snapshot_required_follower_restart"),
        activity("snapshot_catchup_verified"),
        activity(
            "split_and_rebalance_with_read_load",
            {
                "source_decommissioned_on_every_node": True,
                "physical_reclamation_verified": True,
                "source_namespaces_empty_on_every_node": True,
                "allocated_storage_not_increased_on_every_node": True,
                "allocated_reduction_observed": True,
                "archived_wal_reclaimed": False,
                "storage_before": [{"node_id": 1, "allocated_bytes": 100}],
                "storage_after": [{"node_id": 1, "allocated_bytes": 90}],
            },
        ),
        activity(
            "sustained_load",
            {
                "requested_seconds": 86400,
                "actual_seconds": 86401.0,
                "acknowledged_inserts": 156,
                "acknowledged_inserts_per_second": 0.0018,
                "total_acknowledged": 6156,
                "production_budgets_met": None,
            },
        ),
        activity("exact_recovery_validation"),
    ]
    return {
        "schema_version": 1,
        "run_id": "test-run",
        "status": "passed",
        "started_at_unix_micros": 1_000_000,
        "finished_at_unix_micros": 86_401_000_000,
        "error": None,
        "source": {"commit": "abc123", "dirty": False},
        "configuration": {"nodes": 1, "total_insert_target": 6000, "sustained_load_seconds": 86400},
        "workload": {
            "target_insert_count": 6000,
            "acknowledged_insert_count": 6156,
            "sustained_insert_count": 156,
            "write_attempts": 6174,
            "write_retries": 36,
            "recovered_uncertain_writes": 18,
            "successful_reads": 269021,
            "read_retries": 85,
        },
        "activities": activities,
        "validation": {
            "exact_acknowledged_multiset_on_every_node": True,
            "expected_rows": 6156,
            "nodes": [
                {
                    "node_id": 1,
                    "rows": 6156,
                    "missing": 0,
                    "unexpected": 0,
                    "duplicates": 0,
                    "mismatched_values": 0,
                    "indexed_rows": 1,
                    "indexed_lookup_verified": True,
                }
            ],
        },
        "resources": {
            "rss": {"high_water_bytes": 1000},
            "allocator": {"high_water_bytes": 900},
            "storage_sampling": {"physical_storage_bounded": None},
        },
        "assessment": {
            "bounded_memory": None,
            "bounded_queues": None,
            "production_budgets_met": None,
        },
    }


def sample(elapsed, phase, rss=100, allocator=50, logical=1000, allocated=2000, available=5000):
    queues = {"nodus_raft_router_queue_depth": 0, "nodus_raft_commit_queue_depth": 0}
    if phase == "sustained_load":
        queues = {"nodus_raft_router_queue_depth": 1, "nodus_raft_commit_queue_depth": 2}
    return {
        "schema_version": 1,
        "kind": "sample",
        "name": "resources",
        "elapsed_seconds": elapsed,
        "detail": {"phase": phase},
        "rss_bytes": rss,
        "allocator_bytes": allocator,
        "nodes": [
            {
                "node_id": 1,
                "metrics": queues,
                "storage": {
                    "usage": {"logical_bytes": logical, "allocated_bytes": allocated},
                    "volume_available_bytes": available,
                    "error": None,
                },
            }
        ],
    }


def event_fixture():
    return [
        sample(10, "sustained_load"),
        sample(20, "sustained_load", 300, 150, 1200, 2200, 4000),
        sample(30, "post_activity_observation", 200, 75),
    ]


class SoakValidateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="nodus-soak-validate-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def write_run(self, summary=None, events=None, console=""):
        summary = summary if summary is not None else summary_fixture()
        events = events if events is not None else event_fixture()
        (self.directory / "summary.json").write_text(json.dumps(summary))
        (self.directory / "events.jsonl").write_text(
            "".join(json.dumps(event) + "\n" for event in events)
        )
        log = self.directory / "console.log"
        log.write_text(console)
        return summary, log

    def validate(self, log=None, expected=86400):
        return soak_validate.validate_run(self.directory, log, expected)

    def assert_failed(self, report, text):
        self.assertEqual(report["status"], "failed")
        self.assertIn(text, "\n".join(report["correctness"]["messages"]))

    def test_long_duration_and_dynamic_rows_pass_without_budget_claims(self):
        _, log = self.write_run()
        report = self.validate(log)
        self.assertEqual(report["status"], "passed", report["correctness"])
        self.assertEqual(report["expected_rows"], 6156)
        self.assertEqual(report["expected_sustained_seconds"], 86400)
        self.assertAlmostEqual(report["duration_seconds"], 86400.0)
        for verdict in report["assessment"].values():
            if verdict is not None:
                self.assertIsInstance(verdict, str)
        self.assertIsNone(report["assessment"]["production_budgets_met"])
        self.assertIsNone(report["resources"]["sustained_process"]["stabilized"])

    def test_zero_duration_drill_is_supported(self):
        summary = summary_fixture()
        summary["configuration"]["sustained_load_seconds"] = 0
        summary["workload"]["acknowledged_insert_count"] = 6000
        summary["workload"]["sustained_insert_count"] = 0
        summary["activities"] = [
            item for item in summary["activities"] if item["name"] != "sustained_load"
        ]
        summary["validation"]["expected_rows"] = 6000
        summary["validation"]["nodes"][0]["rows"] = 6000
        self.write_run(summary, [sample(30, "post_activity_observation")])
        report = self.validate(expected=0)
        self.assertEqual(report["status"], "passed", report["correctness"])
        self.assertEqual(report["expected_rows"], 6000)
        self.assertIsNone(report["sustained_activity"])
        self.assertIsNone(report["resources"]["sustained_process"]["rss_bytes"])

    def test_duration_assertion_catches_wrong_run_configuration(self):
        self.write_run()
        report = self.validate(expected=3600)
        self.assert_failed(report, "does not equal expected 3600")

    def test_early_sustained_window_fails(self):
        summary = summary_fixture()
        detail = summary["activities"][8]["detail"]
        detail["actual_seconds"] = 100
        self.write_run(summary)
        self.assert_failed(self.validate(), "ended before the requested duration")

    def test_wrong_exact_row_count_fails(self):
        summary = summary_fixture()
        summary["validation"]["nodes"][0]["rows"] = 6155
        self.write_run(summary)
        self.assert_failed(self.validate(), "wrong row count")

    def test_nonzero_quiet_queue_fails(self):
        events = event_fixture()
        events[-1]["nodes"][0]["metrics"]["nodus_raft_router_queue_depth"] = 1
        self.write_run(events=events)
        self.assert_failed(self.validate(), "quiet queue nodus_raft_router_queue_depth is nonzero")

    def test_streamed_trends_include_delta_extrema_and_hourly_slope(self):
        self.write_run()
        report = self.validate()
        rss = report["resources"]["sustained_process"]["rss_bytes"]
        self.assertEqual(rss["sample_count"], 2)
        self.assertEqual(rss["start"], 100)
        self.assertEqual(rss["end"], 300)
        self.assertEqual(rss["delta"], 200)
        self.assertEqual(rss["slope_per_second"], 20)
        self.assertEqual(rss["slope_per_hour"], 72000)
        storage = report["resources"]["sustained_storage"][0]
        self.assertEqual(storage["allocated_bytes"]["slope_per_second"], 20)
        self.assertEqual(storage["volume_available_bytes"]["delta"], -1000)
        self.assertIsNone(storage["bounded"])

    def test_retry_rates_use_all_recorded_attempts(self):
        self.write_run()
        measurements = self.validate()["error_measurements"]
        self.assertAlmostEqual(measurements["write_retry_rate"], 36 / 6174)
        self.assertAlmostEqual(measurements["read_retry_rate"], 85 / (269021 + 85))
        self.assertIsNone(measurements["budget_met"])

    def test_missing_retry_counters_are_unknown_not_zero(self):
        summary = summary_fixture()
        for field in ("write_attempts", "write_retries", "successful_reads", "read_retries"):
            del summary["workload"][field]
        self.write_run(summary)
        measurements = self.validate()["error_measurements"]
        self.assertIsNone(measurements["write_retry_rate"])
        self.assertIsNone(measurements["read_retry_rate"])

    def test_invalid_node_count_fails_without_an_exception(self):
        summary = summary_fixture()
        summary["configuration"]["nodes"] = "three"
        self.write_run(summary)
        report = self.validate()
        self.assert_failed(report, "configuration nodes must be a positive integer")

    def test_transport_backoff_and_json_decode_errors_are_checked(self):
        console = "\n".join(
            [
                "openraft::replication RPCError err=Unreachable node: Connection refused",
                "openraft::replication RPCError err=Unreachable node: 404 Not Found",
                "openraft::replication RPCError err=Unreachable node: error decoding response body",
            ]
        )
        _, log = self.write_run(console=console)
        report = self.validate(log)
        self.assert_failed(report, "JSON decoding errors returned")
        self.assertEqual(report["transport_errors"]["connection_refused"], 1)
        self.assertEqual(report["transport_errors"]["http_404"], 1)
        self.assertEqual(report["transport_errors"]["json_decode_errors"], 1)

    def test_immediate_retry_classification_fails(self):
        console = "openraft::replication RPCError err=Network: Connection refused"
        _, log = self.write_run(console=console)
        report = self.validate(log)
        self.assert_failed(report, "immediate-retry classification")
        self.assertEqual(report["transport_errors"]["immediate_retry_classifications"], 1)

    def test_insufficient_resource_samples_fail_without_inventing_a_budget(self):
        self.write_run(events=[event_fixture()[0], event_fixture()[-1]])
        report = self.validate()
        self.assert_failed(report, "insufficient sustained RSS samples for a trend")
        self.assertIsNone(report["assessment"]["bounded_memory"])

    def test_invalid_jsonl_is_an_actionable_error(self):
        self.write_run()
        (self.directory / "events.jsonl").write_text("not-json\n")
        with self.assertRaisesRegex(ValueError, "events.jsonl:1"):
            self.validate()

    def test_problem_report_is_bounded(self):
        problems = soak_validate.Problems(limit=2)
        for index in range(5):
            problems.add(f"problem {index}")
        self.assertEqual(
            problems.report(),
            {
                "count": 5,
                "messages": ["problem 0", "problem 1"],
                "omitted_messages": 3,
            },
        )

    def test_input_summary_is_not_mutated(self):
        summary, _ = self.write_run()
        original = copy.deepcopy(summary)
        self.validate()
        self.assertEqual(json.loads((self.directory / "summary.json").read_text()), original)


if __name__ == "__main__":
    unittest.main()
