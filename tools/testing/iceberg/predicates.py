"""Independent WHERE fixtures and version-two worker pruning evidence."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import struct
import subprocess

import pyarrow as pa
from pyiceberg.expressions import (
    And, EqualTo, GreaterThan, GreaterThanOrEqual, IsNull, LessThan, LessThanOrEqual,
    NotEqualTo, NotNull,
)
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import StringType

from run import SCHEMA


def frames(binary, request, env):
    body = json.dumps(request).encode()
    result = subprocess.run(
        [binary, "iceberg", "worker"], input=struct.pack("<I", len(body)) + body,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env, timeout=30,
    )
    assert result.returncode == 0, "worker exited unsuccessfully"
    encoded = result.stdout
    response = []
    while encoded:
        assert len(encoded) >= 4, "partial worker frame length"
        size = struct.unpack("<I", encoded[:4])[0]
        assert 0 < size <= 65536 and len(encoded) >= size + 4, "invalid worker frame"
        response.append(json.loads(encoded[4:4 + size]))
        encoded = encoded[4 + size:]
    assert response and "Done" in response[-1], "worker did not finish the read"
    assert not any("Error" in frame for frame in response), "worker returned an error"
    return response


def prepare(gate, profile, pg_text):
    table = gate.create("filter_events")
    for chunk in range(4):
        if chunk == 2:
            with table.update_spec() as update:
                update.add_field("active", IdentityTransform(), "active_identity")
        rows = [{
            "id": row,
            "name": None if row % 7 == 0 else f"n{row}",
            "active": None if row % 5 == 0 else row % 2 == 0,
            "amount": Decimal(row) / 100,
            "day": None if row % 9 == 0 else date(2020, 1, 1) + timedelta(days=row),
            "at": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "payload": None if row % 3 == 0 else bytes([row]),
        } for row in range(chunk * 16, (chunk + 1) * 16)]
        table.append(pa.Table.from_pylist(rows, schema=SCHEMA))
    # Current names/default-null fields change without a new data snapshot.
    with table.update_schema() as update:
        update.rename_column("name", "label").add_column("note", StringType())
    cases = [
        ("id >= 48 AND active = true", And(GreaterThanOrEqual("id", 48), EqualTo("active", True)), ["id", "label"], None),
        ("label IS NULL", IsNull("label"), ["id"], None),
        ("active IS NOT NULL AND id < 25", And(NotNull("active"), LessThan("id", 25)), ["id"], None),
        ("id <= 10 AND id > 3 AND id != 5", And(LessThanOrEqual("id", 10), And(GreaterThan("id", 3), NotEqualTo("id", 5))), ["id"], None),
        ("label = 'n12'", EqualTo("label", "n12"), ["id"], None),
        ("label != 'n12'", And(NotNull("label"), NotEqualTo("label", "n12")), ["id"], None),
        ("day >= '2020-02-01' AND day < '2020-02-10'", And(GreaterThanOrEqual("day", "2020-02-01"), LessThan("day", "2020-02-10")), ["id", "day"], None),
        # Its matching row occurs after other rows in the surviving file.
        ("id = 63", EqualTo("id", 63), ["label"], 1),
        ("label IS NULL AND active = false", And(IsNull("label"), EqualTo("active", False)), ["id"], 2),
        ("id = NULL", None, ["id"], None),
        ("note IS NULL AND id >= 62", And(IsNull("note"), GreaterThanOrEqual("id", 62)), ["id"], None),
        ("note IS NOT NULL", NotNull("note"), ["id"], None),
    ]
    expected = []
    for condition, expression, columns, limit in cases:
        rows = [] if expression is None else table.scan(row_filter=expression).to_arrow().to_pylist()
        expected.append({
            "sql": f"SELECT {', '.join(columns)} FROM lake_filters WHERE {condition}"
                   + (f" LIMIT {limit}" if limit is not None else ""),
            "rows": [[pg_text(row[column]) for column in columns] for row in rows],
            "limit": limit,
        })
    request = {
        "version": 2, "profile": profile, "namespace": ["interop"],
        "table": "filter_events", "expected": None, "columns": [], "predicates": [],
        "limit": 0, "timeout_ms": 20000, "address_space_bytes": 2 * 1024**3,
    }
    schema = frames(gate.binary, request, gate.env)[0]["Schema"]
    descriptor = {
        "version": 1, "profile": "lake", "namespace": ["interop"],
        "table": "filter_events", "table_uuid": schema["table_uuid"], "fields": schema["fields"],
    }
    request.update(expected=descriptor, columns=["id"], limit=1000001)
    plain = frames(gate.binary, request, gate.env)
    field_id = next(field["id"] for field in schema["fields"] if field["name"] == "id")
    request["predicates"] = [{"field_id": field_id, "column": "id", "op": "Eq", "value": {"Int": 63}}]
    pruned = frames(gate.binary, request, gate.env)
    before = next(frame["ScanStats"] for frame in plain if "ScanStats" in frame)
    after = next(frame["ScanStats"] for frame in pruned if "ScanStats" in frame)
    independent_files = len(list(table.scan(row_filter=EqualTo("id", 63)).plan_files()))
    assert after["planned_files"] == independent_files < before["planned_files"]
    assert after["planned_file_bytes"] < before["planned_file_bytes"]
    assert 1 < after["rows_read"] < before["rows_read"], "test did not exercise an approximate pruning residual"
    # Version-one servers can still use a newer worker for unfiltered reads.
    request.update(version=1, predicates=[])
    request.pop("predicates")
    legacy = frames(gate.binary, request, gate.env)
    assert legacy[0]["Schema"]["version"] == 1
    assert not any("ScanStats" in frame for frame in legacy)
    return expected, {"before": before, "after": after, "pyiceberg_selected_files": independent_files}
