#!/usr/bin/env python3
"""Compare native SQL with an independent PyIceberg scan on an owned stack."""
import hashlib
import json
import os
import signal
import subprocess
import tempfile
import threading
import time
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa

from predicates import prepare
from run import Gate
from stack import ROOT, Stack


def pg_text(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, bytes):
        return "\\x" + value.hex()
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S") + "+00"
    if isinstance(value, (date, Decimal)):
        return str(value)
    return str(value)


class Stall(BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(10)
        self.send_response(503)
        self.end_headers()

    def log_message(self, *_):
        pass


def run_gate(args, env, timeout=600):
    """Own Cargo, its Rust test server, and decoder workers as one process group."""
    process = subprocess.Popen(args, cwd=ROOT, env=env, start_new_session=True)
    try:
        return process.wait(timeout=timeout)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=3)
            finally:
                # Cargo can exit before a descendant which ignores SIGTERM.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


def main():
    report_path = ROOT / "target/iceberg-live/sql-report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {"version": 1, "status": "failed", "cleanup": "not_started"}

    def interrupted(*_):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, interrupted)
    stack = Stack()
    stalled = ThreadingHTTPServer(("127.0.0.1", 0), Stall)
    threading.Thread(target=stalled.serve_forever, daemon=True).start()
    try:
        stack.start()
        with tempfile.TemporaryDirectory(prefix="nodus-iceberg-sql-") as directory:
            directory = Path(directory)
            target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
            gate = Gate(stack, target / "debug/nodus_cli", directory)
            table = gate.create("events")
            table.append(gate.original)
            wide = gate.catalog.create_table(
                ("interop", "wide_events"), schema=pa.schema([("payload", pa.string())])
            )
            wide.append(pa.Table.from_arrays([
                pa.array([
                    hashlib.shake_256(f"sql-row-{row}".encode()).hexdigest(4096)
                    for row in range(4096)
                ])
            ], names=["payload"]))
            assert wide.scan().to_arrow().num_rows == 4096
            # Compare the exact independent scan order and all NULL/type values.
            independent = table.scan().to_arrow().to_pylist()
            expected = [
                [pg_text(row[name]) for name in gate.original.column_names]
                for row in independent
            ]
            expected_path = directory / "expected.json"
            expected_path.write_text(json.dumps(expected))
            profiles = directory / "profiles.json"
            profile = {
                "uri": stack.url + "/catalog", "warehouse": "interop",
                "bearer_token": None, "storage_properties": gate.properties,
            }
            filters, pruning = prepare(gate, profile, pg_text)
            filter_path = directory / "filters.json"
            filter_path.write_text(json.dumps(filters))
            report["file_pruning"] = pruning
            with profiles.open("x") as output:
                profiles.chmod(0o600)
                json.dump({"version": 1, "profiles": {
                    "lake": profile,
                    "stall": {**profile, "uri": f"http://127.0.0.1:{stalled.server_port}/catalog"},
                }}, output)
            env = {
                **gate.env, "NODUS_ICEBERG_SQL_PROFILES": str(profiles),
                "NODUS_ICEBERG_SQL_EXPECTED": str(expected_path),
                "NODUS_ICEBERG_SQL_FILTERS": str(filter_path),
            }
            result = run_gate([
                "cargo", "test", "-p", "nodus_cli", "--features", "iceberg",
                "--locked", "--test", "iceberg_sql_live", "--", "--ignored", "--nocapture",
            ], env)
            if result:
                raise RuntimeError("native SQL gate failed")
            report.update(status="passed", rows=len(expected), checks=[
                "PyIceberg exact values", "typed WHERE and NULL semantics", "partition evolution",
                "measured file pruning", "v1 worker compatibility", "extended typed projection", "LIMIT after residual",
                "read-only rejection", "CancelRequest before first row",
                "statement_timeout", "atomic failed registration", "restart and rollback",
                "real Parquet decoder memory termination and server recovery",
            ])
    except Exception as error:
        report["failure"] = type(error).__name__
        raise
    finally:
        try:
            stack.close()
            report["cleanup"] = "passed"
        except Exception:
            report.update(status="failed", cleanup="failed")
            raise
        finally:
            stalled.shutdown()
            stalled.server_close()
            report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"PASS native Iceberg SQL; report: {report_path}", flush=True)


if __name__ == "__main__":
    main()
