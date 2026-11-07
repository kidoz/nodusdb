#!/usr/bin/env python3
"""Order-of-magnitude throughput floor check for server_bench measurements.

Reads a schema-version-1/2 measurements.jsonl written by the server SQL
benchmark harness (`cargo bench -p nodus_integration_tests --bench server_bench`)
and fails when the best observed throughput of a known workload falls below an
order-of-magnitude floor. Floors are one tenth of the recorded same-host
baseline (benchmarks/reports/server-baseline-2026-10-29.md, legacy write path),
so they catch harness breakage and >10x regressions only. They are not
performance budgets: shared CI runners do not enforce stable budgets
(benchmarks/workloads/server_sql.md, "Budgets").

Usage: bench_floor.py PATH [--require WORKLOAD]...

PATH is a measurements.jsonl file or a directory containing one. Workloads
absent from the file are skipped with a note unless passed to --require.
"""
import argparse
import json
import sys
from pathlib import Path

# Workload name (as recorded in the JSONL `workload` field) -> minimum
# acceptable best-sample throughput in operations or rows per second.
FLOORS = {
    "point_insert": 2.6,
    "indexed_point_insert": 2.3,
    "point_read": 304.0,
    "mixed_oltp": 14.7,
    "bulk_insert": 4.6,
    "range_scan": 2067.0,
    "replicated_oltp": 7.9,
}


def load_records(path: Path):
    source = path / "measurements.jsonl" if path.is_dir() else path
    if not source.is_file():
        raise SystemExit(f"FAIL: no measurements file at {source}")
    records = []
    with source.open() as lines:
        for number, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise SystemExit(f"FAIL: {source}:{number} is not JSON: {error}")
    if not records:
        raise SystemExit(f"FAIL: {source} contains no records")
    return records


def throughput(record):
    """Effective per-second rate: rows when recorded, else requests."""
    rows = record.get("rows_per_second")
    if rows is not None:
        return float(rows)
    return float(record["requests_per_second"])


def check(records, required):
    best = {}
    for record in records:
        workload = record.get("workload")
        if workload is None:
            raise SystemExit(f"FAIL: record without a workload field: {record!r:.200}")
        rate = throughput(record)
        best[workload] = max(rate, best.get(workload, 0.0))

    failures = []
    for workload, floor in FLOORS.items():
        if workload not in best:
            note = "REQUIRED, MISSING" if workload in required else "absent, skipped"
            if workload in required:
                failures.append(workload)
            print(f"{workload:24s} {note}")
            continue
        rate = best[workload]
        verdict = "ok" if rate >= floor else "BELOW FLOOR"
        if rate < floor:
            failures.append(workload)
        print(f"{workload:24s} best {rate:12.2f}/s  floor {floor:10.2f}/s  {verdict}")
    unknown = sorted(set(best) - set(FLOORS))
    for workload in unknown:
        print(f"{workload:24s} best {best[workload]:12.2f}/s  (no floor defined, reported only)")
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="measurements.jsonl or its directory")
    parser.add_argument("--require", action="append", default=[],
                        help="workload that must appear in the records")
    args = parser.parse_args()
    records = load_records(args.path)
    failures = check(records, set(args.require))
    if failures:
        raise SystemExit(f"FAIL: floor check failed for: {', '.join(failures)}")
    print(f"PASS: {len(records)} records within order-of-magnitude floors")


if __name__ == "__main__":
    main()
