"""Guard against false passes in the live gate's result and failure checks."""

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from run import Gate, check_table, logical_type
from auth import AuthChecks, assert_redacted, safe_detail


class ResultChecks(unittest.TestCase):
    def test_row_order_can_differ_but_duplicates_cannot_disappear(self):
        expected = pa.table({"id": [1, 2, 2], "name": ["a", None, None]})
        check_table(expected.take([2, 0, 1]), expected)
        with self.assertRaises(AssertionError):
            check_table(expected.slice(0, 2), expected)

    def test_null_is_distinct_from_empty_string(self):
        expected = pa.table(
            {"id": [1], "name": [None]},
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("name", pa.string()),
                ]
            ),
        )
        with self.assertRaises(AssertionError):
            check_table(pa.table({"id": [1], "name": [""]}), expected)

    def test_type_width_precision_and_timezone_checks(self):
        self.assertEqual(
            logical_type(pa.timestamp("us", "UTC")),
            logical_type(pa.timestamp("us", "+00:00")),
        )
        self.assertNotEqual(
            logical_type(pa.timestamp("ms", "UTC")),
            logical_type(pa.timestamp("us", "UTC")),
        )
        self.assertNotEqual(
            logical_type(pa.timestamp("us")), logical_type(pa.timestamp("us", "UTC"))
        )
        self.assertNotEqual(
            logical_type(pa.decimal128(12, 2)), logical_type(pa.decimal128(12, 3))
        )
        self.assertNotEqual(logical_type(pa.int32()), logical_type(pa.int64()))

    def test_projection_order_is_checked(self):
        expected = pa.table({"id": [1], "name": ["a"]})
        with self.assertRaises(AssertionError):
            check_table(expected.select(["name", "id"]), expected)


class FailedScanChecks(unittest.TestCase):
    def test_nonzero_exit_is_not_enough_if_output_or_tempfile_remains(self):
        for residue in ("failed.arrow", "partial.tmp"):
            with (
                self.subTest(residue=residue),
                tempfile.TemporaryDirectory() as directory,
            ):
                gate = object.__new__(Gate)
                gate.directory = Path(directory)

                def failed(*args, **kwargs):
                    (gate.directory / residue).write_bytes(b"partial")
                    return subprocess.CompletedProcess([], 1, "", "expected-error")

                gate.invoke = failed
                with self.assertRaises(AssertionError):
                    gate.scan("failed", error="expected-error")

    def test_unrelated_failure_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            gate = object.__new__(Gate)
            gate.directory = Path(directory)
            gate.invoke = Mock(
                return_value=subprocess.CompletedProcess([], 1, "", "wrong-error")
            )
            with self.assertRaises(AssertionError):
                gate.scan("failed", error="expected-error")


class AuthEvidenceChecks(unittest.TestCase):
    def test_disclosure_checks_cover_known_secrets_and_unknown_jwts(self):
        for output, credentials in [
            ("credential=synthetic-secret", ["synthetic-secret"]),
            ('{"token":"eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.signature"}', []),
        ]:
            with self.subTest(output_type=bool(credentials)):
                with self.assertRaises(AssertionError):
                    assert_redacted(output, credentials)
        assert_redacted('{"checks":[],"status":"passed"}', ["synthetic-secret"])

    def test_report_failure_details_do_not_serialize_credentials(self):
        self.assertEqual(
            safe_detail(AssertionError("echo synthetic-secret"), ["synthetic-secret"]),
            "failure detail redacted",
        )
        self.assertIsNone(safe_detail(RuntimeError("synthetic-secret"), []))
        self.assertEqual(
            safe_detail(AssertionError("safe failure"), []), "safe failure"
        )

    def test_permission_denial_requires_an_expected_http_status(self):
        gate = AuthChecks()
        gate.catalog_response = Mock(return_value=(500, None))
        with self.assertRaises(AssertionError):
            gate.require_status("table", "token", {403, 404})
        gate.catalog_response.return_value = (200, {})
        with self.assertRaises(AssertionError):
            gate.require_status("config", None, {401})

    def test_token_override_preserves_admin_separation(self):
        gate = object.__new__(Gate)
        gate.stack = Mock(url="http://localhost", secrets=["catalog-owner-token"])
        gate.binary = "nodus_cli"
        gate.storage = "storage.json"
        gate.credentials = []
        gate.env = {
            "NODUS_ICEBERG_TOKEN": "catalog-owner-token",
            "NODUS_ADMIN_TOKEN": "admin-token",
        }
        failed = subprocess.CompletedProcess([], 1, "", "failure")
        with patch("run.command", return_value=failed) as command:
            gate.invoke("inspect", token=None)
            env = command.call_args.kwargs["env"]
            self.assertNotIn("NODUS_ICEBERG_TOKEN", env)
            self.assertEqual(env["NODUS_ADMIN_TOKEN"], "admin-token")
            gate.invoke("inspect", token="reader-token")
            self.assertEqual(
                command.call_args.kwargs["env"]["NODUS_ICEBERG_TOKEN"], "reader-token"
            )
            gate.invoke("inspect")
            self.assertEqual(
                command.call_args.kwargs["env"]["NODUS_ICEBERG_TOKEN"],
                "catalog-owner-token",
            )
        self.assertEqual(gate.env["NODUS_ICEBERG_TOKEN"], "catalog-owner-token")


if __name__ == "__main__":
    unittest.main()
