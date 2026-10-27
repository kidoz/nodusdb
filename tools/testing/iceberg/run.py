#!/usr/bin/env python3
"""Run the real Lakekeeper gate. No external service or credential is accepted."""

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
from urllib.parse import urlsplit

import boto3
from botocore.exceptions import ClientError
import pyarrow as pa
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import StringType

from stack import COMPOSE, ROOT, Stack, command
from writer import HostCatalog, add_position_delete


SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("name", pa.string()),
        ("active", pa.bool_()),
        ("amount", pa.decimal128(12, 2)),
        ("day", pa.date32()),
        ("at", pa.timestamp("us", tz="UTC")),
        ("payload", pa.binary()),
    ]
)
ROWS = [
    {
        "id": 1,
        "name": "alpha",
        "active": True,
        "amount": Decimal("123.45"),
        "day": date(2026, 1, 1),
        "at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "payload": b"\x00\xff",
    },
    {
        "id": 2,
        "name": None,
        "active": False,
        "amount": Decimal("-0.01"),
        "day": date(1970, 1, 1),
        "at": datetime(1970, 1, 1, tzinfo=timezone.utc),
        "payload": b"",
    },
    {
        "id": 3,
        "name": "",
        "active": None,
        "amount": None,
        "day": None,
        "at": None,
        "payload": None,
    },
]


def rows(table):
    return sorted(table.to_pylist(), key=lambda row: row["id"])


def logical_type(ty):
    # Offset widths and equivalent UTC labels differ between Arrow producers.
    if pa.types.is_string(ty) or pa.types.is_large_string(ty):
        return "string"
    if pa.types.is_binary(ty) or pa.types.is_large_binary(ty):
        return "binary"
    if pa.types.is_timestamp(ty) and ty.tz in ("UTC", "+00:00", "Etc/UTC"):
        return str(pa.timestamp(ty.unit, tz="UTC"))
    return str(ty)


def check_table(actual, expected):
    assert actual.column_names == expected.column_names, "output column order differs"
    assert rows(actual) == rows(expected), "output rows/values/nulls differ"
    for left, right in zip(actual.schema, expected.schema, strict=True):
        assert logical_type(left.type) == logical_type(right.type), (
            f"logical type of {left.name}: {left.type} != {right.type}"
        )


class Gate:
    def __init__(self, stack, binary, directory):
        self.stack = stack
        self.binary = binary
        self.directory = directory
        self.credentials = []
        self.env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("AWS_", "NODUS_", "ICEBERG_", "PYICEBERG_"))
        }
        self.env.update(
            {
                "AWS_EC2_METADATA_DISABLED": "true",
                "AWS_SHARED_CREDENTIALS_FILE": str(directory / "no-aws-credentials"),
                "AWS_CONFIG_FILE": str(directory / "no-aws-config"),
                "NODUS_ADMIN_TOKEN": "must-not-be-used-for-iceberg",
            }
        )
        self.s3 = boto3.client(
            "s3",
            endpoint_url=stack.s3_url,
            region_name="us-east-1",
            aws_access_key_id="nodus-iceberg-test",
            aws_secret_access_key=stack.password,
        )
        stack.create_warehouse(self.s3)
        self.catalog = HostCatalog(stack.url + "/catalog", stack.s3_url)
        self.catalog.create_namespace("interop")
        self.properties = {"s3.endpoint": stack.s3_url, "s3.allow-http": "true"}
        self.storage = self.write_properties("storage.json", self.properties)
        self.original = pa.Table.from_pylist(ROWS, schema=SCHEMA)

    def write_properties(self, name, properties):
        path = self.directory / name
        with path.open("x") as stream:
            path.chmod(0o600)
            json.dump(properties, stream)
        return path

    def invoke(self, operation, table="events", *extra, storage=None):
        args = [
            self.binary,
            "iceberg",
            operation,
            "--catalog-uri",
            self.stack.url + "/catalog",
            "--warehouse",
            "interop",
            "--namespace",
            "interop",
            "--table",
            table,
            "--storage-properties",
            storage or self.storage,
            "--timeout-seconds",
            "20",
            *extra,
        ]
        result = command(args, env=self.env, check=False, timeout=30)
        for secret in [self.stack.password, *self.credentials]:
            assert secret not in result.stdout + result.stderr, (
                "CLI disclosed a storage credential"
            )
        return result

    def scan(self, name, table="events", *extra, storage=None, error=None):
        output = self.directory / (name + ".arrow")
        before = set(self.directory.iterdir())
        result = self.invoke(
            "scan", table, "--output", str(output), *extra, storage=storage
        )
        if error is not None:
            assert result.returncode != 0, "expected read failure succeeded"
            assert error in result.stderr, "read failed for an unexpected reason"
            assert not output.exists(), "failed scan published output"
            assert set(self.directory.iterdir()) == before, (
                "failed scan left a temporary file"
            )
            return None
        assert result.returncode == 0, "CLI scan failed"
        summary = json.loads(result.stdout)
        assert summary["version"] == 1 and summary["format"] == "arrow-ipc-stream"
        with pa.ipc.open_stream(output) as stream:
            actual = stream.read_all()
        assert summary["rows"] == actual.num_rows, "row-count summary differs"
        return actual, summary

    def create(self, name, version="2", schema=SCHEMA):
        return self.catalog.create_table(
            ("interop", name), schema=schema, properties={"format-version": version}
        )

    def current_and_historical(self):
        self.table = self.create("events")
        self.table.append(self.original)
        self.first_snapshot = self.table.current_snapshot().snapshot_id
        check_table(self.table.scan().to_arrow(), self.original)
        current, summary = self.scan("current")
        check_table(current, self.original)
        assert summary["snapshot_id"] == self.first_snapshot
        self.table.append(pa.Table.from_pylist([{**ROWS[0], "id": 4}], schema=SCHEMA))
        historical, summary = self.scan(
            "historical", "events", "--snapshot-id", str(self.first_snapshot)
        )
        check_table(historical, self.original)
        assert summary["snapshot_id"] == self.first_snapshot
        latest, summary = self.scan("latest")
        check_table(latest, self.table.scan().to_arrow())
        assert summary["snapshot_id"] != self.first_snapshot, "new commit not observed"
        inspection = self.invoke("inspect")
        assert inspection.returncode == 0, "inspection failed"
        metadata = json.loads(inspection.stdout)
        assert (
            metadata["snapshot_id"] == summary["snapshot_id"]
            and metadata["format_version"] == 2
        )
        assert metadata["table_uuid"] == str(self.table.metadata.table_uuid)

    def projection_and_limit(self):
        projected, _ = self.scan(
            "projection", "events", "--column", "name", "--column", "id"
        )
        check_table(projected, self.table.scan().to_arrow().select(["name", "id"]))
        limited, _ = self.scan("limited", "events", "--limit", "2", "--batch-size", "1")
        assert limited.num_rows == 2 and all(
            row in rows(self.table.scan().to_arrow()) for row in rows(limited)
        )
        empty, _ = self.scan("zero-limit", "events", "--limit", "0")
        assert empty.num_rows == 0 and empty.column_names == SCHEMA.names

    def schema_and_partition_evolution(self):
        with self.table.update_schema() as update:
            update.rename_column("name", "label").add_column("note", StringType())
        with self.table.update_spec() as update:
            update.add_field("id", IdentityTransform(), "id_partition")
        evolved_schema = SCHEMA.set(1, pa.field("label", pa.string())).append(
            pa.field("note", pa.string())
        )
        evolved_row = {**ROWS[0], "id": 5, "label": "new", "note": "added"}
        del evolved_row["name"]
        self.table.append(pa.Table.from_pylist([evolved_row], schema=evolved_schema))
        expected_rows = [
            {
                **{k: v for k, v in row.items() if k != "name"},
                "label": row["name"],
                "note": None,
            }
            for row in [*ROWS, {**ROWS[0], "id": 4}]
        ] + [evolved_row]
        expected = pa.Table.from_pylist(expected_rows, schema=evolved_schema)
        check_table(self.table.scan().to_arrow(), expected)
        actual, _ = self.scan("evolved")
        check_table(actual, expected)
        assert len(self.table.metadata.partition_specs) == 2
        historical, _ = self.scan(
            "pre-evolution", "events", "--snapshot-id", str(self.first_snapshot)
        )
        check_table(historical, self.original)

    def v1_and_empty(self):
        table = self.create("v1", version="1")
        table.append(self.original)
        actual, _ = self.scan("v1", "v1")
        check_table(actual, self.original)
        self.create("empty")
        actual, summary = self.scan("empty", "empty")
        check_table(actual, pa.Table.from_pylist([], schema=SCHEMA))
        assert summary["snapshot_id"] is None

    def vended_credentials(self):
        table = self.catalog.load_table(("interop", "events"))
        config = table.io.properties
        assert config["s3.access-key-id"] != "nodus-iceberg-test", (
            "root credentials were returned"
        )
        assert config["s3.session-token"], "no temporary STS token returned"
        self.credentials = [
            config[key]
            for key in ("s3.access-key-id", "s3.secret-access-key", "s3.session-token")
        ]
        client = boto3.client(
            "s3",
            endpoint_url=self.stack.s3_url,
            region_name="us-east-1",
            aws_access_key_id=config["s3.access-key-id"],
            aws_secret_access_key=config["s3.secret-access-key"],
            aws_session_token=config["s3.session-token"],
        )
        key = urlsplit(
            next(iter(table.scan().plan_files())).file.file_path
        ).path.lstrip("/")
        client.head_object(Bucket=self.stack.bucket, Key=key)
        self.s3.put_object(
            Bucket=self.stack.bucket, Key="outside-table/sentinel", Body=b"denied"
        )
        try:
            client.get_object(Bucket=self.stack.bucket, Key="outside-table/sentinel")
        except ClientError as error:
            assert error.response["Error"]["Code"] == "AccessDenied", (
                "wrong credential-scope failure"
            )
        else:
            raise AssertionError("vended credentials accessed another prefix")
        actual, _ = self.scan("vended")
        check_table(actual, table.scan().to_arrow())

    def failures_do_not_publish(self):
        self.scan("missing-table", "absent", error="load table")
        self.scan(
            "missing-snapshot", "events", "--snapshot-id", "1", error="not retained"
        )
        self.scan(
            "unknown-column",
            "events",
            "--column",
            "absent",
            error="not a top-level field",
        )
        bad = self.write_properties(
            "bad-storage.json",
            {
                **self.properties,
                "s3.access-key-id": "invalid-test-key",
                "s3.secret-access-key": "invalid-test-secret",
                "s3.session-token": "invalid-test-token",
            },
        )
        self.credentials.extend(
            ["invalid-test-key", "invalid-test-secret", "invalid-test-token"]
        )
        self.scan(
            "bad-credentials", storage=bad, error="Iceberg manifest list size failed"
        )
        table = self.create("missing_data")
        table.append(self.original)
        key = urlsplit(
            next(iter(table.scan().plan_files())).file.file_path
        ).path.lstrip("/")
        self.s3.delete_object(Bucket=self.stack.bucket, Key=key)
        self.scan(
            "missing-data", "missing_data", error="Iceberg read data batch failed"
        )
        destination = self.directory / "existing.arrow"
        destination.write_bytes(b"preserve-this-file")
        result = self.invoke("scan", "events", "--output", str(destination))
        assert result.returncode != 0 and "output file already exists" in result.stderr
        assert destination.read_bytes() == b"preserve-this-file", (
            "existing output overwritten"
        )

    def reject_delete_manifest(self):
        table = self.create("position_deletes")
        table.append(self.original)
        table = add_position_delete(self.catalog, table)
        assert table.inspect.delete_files().num_rows == 1, (
            "delete fixture has no delete file"
        )
        check_table(table.scan().to_arrow(), self.original.slice(1))
        self.scan(
            "delete-manifest",
            "position_deletes",
            error="snapshots with delete manifests",
        )


CHECKS = [
    "current_and_historical",
    "projection_and_limit",
    "schema_and_partition_evolution",
    "v1_and_empty",
    "vended_credentials",
    "failures_do_not_publish",
    "reject_delete_manifest",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    if not __debug__:
        parser.error("run without Python optimization; assertions are part of this gate")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/nodus_cli")
    parser.add_argument(
        "--report", type=Path, default=ROOT / "target/iceberg-live/report.json"
    )
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error("build nodus_cli with --features iceberg first")
    with binary.open("rb") as stream:
        binary_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    report = {
        "version": 1,
        "status": "failed",
        "lakekeeper": "0.14.2",
        "pyiceberg": "0.12.0",
        "pyarrow": pa.__version__,
        "python": sys.version.split()[0],
        "binary_sha256": binary_hash,
        "compose_sha256": hashlib.sha256(COMPOSE.read_bytes()).hexdigest(),
        "lock_sha256": hashlib.sha256(
            Path(__file__).with_name("uv.lock").read_bytes()
        ).hexdigest(),
        "checks": [],
        "cleanup": "not_started",
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    stack = Stack()
    phase = "startup"

    def interrupted(*_):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, interrupted)
    try:
        stack.start()
        with tempfile.TemporaryDirectory(prefix="nodus-iceberg-") as directory:
            phase = "bootstrap_writer"
            gate = Gate(stack, binary, Path(directory))
            for phase in CHECKS:
                started = time.monotonic()
                getattr(gate, phase)()
                report["checks"].append(
                    {
                        "name": phase,
                        "status": "passed",
                        "seconds": round(time.monotonic() - started, 3),
                    }
                )
                print(f"PASS {phase}", flush=True)
        report["status"] = "passed"
    except (Exception, KeyboardInterrupt) as error:
        # External error text can include temporary tokens or request bodies.
        report["failure"] = {"phase": phase, "type": type(error).__name__}
        if isinstance(error, AssertionError):
            report["failure"]["detail"] = str(error)
        print(f"FAIL {phase}: {type(error).__name__}", file=sys.stderr)
    finally:
        try:
            stack.close()
            report["cleanup"] = "passed"
        except Exception:
            report["cleanup"] = "failed"
            report["status"] = "failed"
            print(
                f"Cleanup failed; remove Compose project {stack.project}",
                file=sys.stderr,
            )
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(
        f"{len(report['checks'])}/{len(CHECKS)} checks passed; report: {args.report}",
        flush=True,
    )
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
