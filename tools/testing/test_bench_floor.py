"""bench_floor regression checks: python3 -B -m unittest discover -s tools/testing."""
import json
import tempfile
import unittest
from pathlib import Path

import bench_floor


def record(workload, requests_per_second, rows_per_second=None):
    return {
        "schema_version": 2,
        "workload": workload,
        "requests_per_second": requests_per_second,
        "rows_per_second": rows_per_second,
    }


class BenchFloorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="nodus-bench-floor-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def write(self, records):
        path = self.directory / "measurements.jsonl"
        path.write_text("".join(json.dumps(r) + "\n" for r in records))
        return path

    def test_missing_file_fails(self):
        with self.assertRaises(SystemExit) as caught:
            bench_floor.load_records(self.directory / "nonexistent")
        self.assertIn("no measurements file", str(caught.exception))

    def test_empty_file_fails(self):
        path = self.write([])
        with self.assertRaises(SystemExit) as caught:
            bench_floor.load_records(path)
        self.assertIn("no records", str(caught.exception))

    def test_records_above_floors_pass(self):
        records = [
            record("point_insert", 30.0),
            record("bulk_insert", 50.0, rows_per_second=500.0),
            record("range_scan", 210.0, rows_per_second=21000.0),
        ]
        self.assertEqual(bench_floor.check(records, set()), [])

    def test_best_sample_is_compared_not_the_worst(self):
        records = [
            record("point_insert", 1.0),  # a bad sample does not fail the run
            record("point_insert", 30.0),
        ]
        self.assertEqual(bench_floor.check(records, set()), [])

    def test_below_floor_fails(self):
        records = [record("point_insert", 1.0)]  # floor is 2.6
        self.assertEqual(bench_floor.check(records, set()), ["point_insert"])

    def test_rows_per_second_is_preferred_over_requests(self):
        # bulk_insert floor is on rows/s; batch rate alone would pass wrongly.
        records = [record("bulk_insert", 40.0, rows_per_second=1.0)]
        self.assertEqual(bench_floor.check(records, set()), ["bulk_insert"])

    def test_absent_workload_is_skipped_unless_required(self):
        records = [record("point_insert", 30.0)]
        self.assertEqual(bench_floor.check(records, set()), [])
        self.assertEqual(bench_floor.check(records, {"range_scan"}), ["range_scan"])

    def test_unknown_workload_is_reported_not_floored(self):
        records = [record("future_workload", 0.001)]
        self.assertEqual(bench_floor.check(records, set()), [])

    def test_legacy_records_without_rows_fall_back_to_request_rate(self):
        records = [{"schema_version": 1, "workload": "point_read",
                    "requests_per_second": 3000.0}]
        self.assertEqual(bench_floor.check(records, set()), [])

    def test_record_without_workload_fails(self):
        with self.assertRaises(SystemExit) as caught:
            bench_floor.check([{"requests_per_second": 1.0}], set())
        self.assertIn("without a workload field", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
