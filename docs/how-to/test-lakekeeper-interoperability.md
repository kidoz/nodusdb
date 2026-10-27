# Test Lakekeeper interoperability

Run the optional Iceberg CLI against a disposable Lakekeeper catalog and MinIO
object store. PyIceberg creates and independently reads the tables; the gate
compares NodusDB's Arrow output with known input rows and PyIceberg results.

## Run the gate

You need Rust, `just`, Docker with Compose v2 or newer, and `uv` (CI pins
0.13.6). Docker must be running. The first run downloads images and Python
packages and builds the CLI and the repository's MinIO image.

```sh
just test-iceberg-live
```

The recipe installs the hash-locked Python dependencies into
`target/iceberg-venv`, using Python 3.12. It starts a unique Compose project,
creates its own warehouse and bucket, runs every check, and removes that
project's containers, network, and data. Docker images and the Python environment
remain cached. The stack publishes only loopback ports and never attaches to an
existing catalog or bucket.

A successful run prints seven `PASS` lines and `7/7 checks passed`. Any missing
service, failed assertion, timeout, or cleanup failure causes a nonzero exit;
there is no skip-on-missing-Docker path.

The report is `target/iceberg-live/report.json`. It contains check results,
durations, versions, binary/configuration hashes, and cleanup status. It excludes
storage credentials and raw service responses. A failed run records its phase
and error class; assertion failures also include a controlled explanation.

To test an already-built binary or choose a separate report location:

```sh
UV_PROJECT_ENVIRONMENT="$PWD/target/iceberg-venv" \
  uv run --locked --project tools/testing/iceberg --python 3.12 \
  python -B tools/testing/iceberg/run.py \
  --binary target/debug/nodus_cli \
  --report target/iceberg-live/report.json
```

The runner handles Ctrl-C and termination with cleanup. A forced kill or Docker
daemon failure can prevent cleanup; the error identifies the unique Compose
project for manual removal. It never removes another project's resources.

## Check coverage

| Check | Required behavior |
| --- | --- |
| Current and historical snapshots | Exact rows and snapshot IDs; inspection UUID/version agrees with the writer |
| Projection and limits | Requested column order, bounded row count, schema-only zero-row output |
| Schema and partition evolution | Renamed field retains values by field ID; added optional field is null in old files; old and new partition specs coexist; historical schema remains readable |
| v1 and empty tables | v1 Parquet reads correctly; snapshotless tables preserve their schema |
| Vended credentials | Temporary STS credentials read their table but cannot read a sentinel outside its prefix; CLI succeeds without ambient AWS credentials |
| Failures and output publication | Missing table/snapshot/column/object and invalid storage credentials fail; failed scans leave no output or temporary files; existing destination is preserved |
| Position deletes | PyIceberg applies an independently encoded delete; NodusDB explicitly rejects its delete manifest before publishing output |

Values include integers, strings, nulls, empty strings, booleans, decimals, dates,
UTC timestamps, and binary data. Comparison preserves duplicates and exact
values. Equivalent Arrow string/binary offset widths and UTC timezone labels
are normalized; integer widths, decimal scale, and timestamp units remain strict.

PyIceberg 0.12's high-level delete API uses copy-on-write. The delete case uses
its low-level Avro manifest writer and PyArrow to create a real position-delete
file, then commits through Lakekeeper's REST API. PyIceberg must read the deleted
result correctly before the NodusDB rejection check runs.

## Versions and boundaries

The Compose file pins Lakekeeper 0.14.2 and PostgreSQL by image digest. MinIO uses
the existing source-pinned `deploy/docker/minio.Dockerfile`. Python dependencies
are locked in `tools/testing/iceberg/uv.lock`, including PyIceberg 0.12.0 and
PyArrow 26.0.0. No Python dependency enters the NodusDB runtime.

The stack deliberately has no catalog identity provider. It verifies actual S3
signature validation and downscoped temporary credentials, **not catalog OIDC or
RBAC**, TLS, real AWS, Polaris, large-table performance, or production readiness.
Passwords are generated per run and storage credentials remain in temporary
process memory or private files that are removed after the run.

Lakekeeper contacts storage through `http://minio:9000`. Host clients use its
published loopback endpoint. The PyIceberg adapter changes only that endpoint;
NodusDB uses its normal `--storage-properties` override. Neither substitutes
manifests or REST responses. For this release, catalog discovery uses the
warehouse **name** (`interop`), while Lakekeeper returns its UUID as the REST
namespace prefix.

CI runs this gate in the `Live Lakekeeper interoperability` job and uploads its
JSON report, including on failure. The fast protocol fixtures remain covered by
the ordinary optional-feature test job.

The harness itself has checks against false passes:

```sh
UV_PROJECT_ENVIRONMENT="$PWD/target/iceberg-venv" \
  uv run --locked --project tools/testing/iceberg --python 3.12 \
  python -B -m unittest discover -s tools/testing/iceberg -p 'test_*.py' -v
```

For upstream setup details, see the pinned
[Lakekeeper example](https://github.com/lakekeeper/lakekeeper/tree/v0.14.2/examples/minimal)
and [PyIceberg implementation](https://github.com/apache/iceberg-python/tree/pyiceberg-0.12.0).
