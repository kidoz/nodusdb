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

A successful run prints eight `PASS` lines and `8/8 checks passed`. Any missing
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

## Test catalog authentication and table permissions

Run the authenticated variant with:

```sh
just test-iceberg-auth-live
```

This runs the same eight interoperability groups through an OIDC-protected
Lakekeeper catalog, then four security groups. The overlay adds digest-pinned
Keycloak 26.6.0 and OpenFGA 1.14.0. Keycloak issues signed service-account tokens;
Lakekeeper validates them against its discovery document and JWKS, and OpenFGA
evaluates real table grants. No authentication or authorization proxy substitutes
for the catalog.

Success prints `12/12 checks passed` and writes
`target/iceberg-live/auth-report.json`. You can also pass `--authenticated` to
`run.py`, together with `--binary` or `--report`. The report includes the
authentication mode, provider versions, and hashes of the auth overlay and realm
configuration. Failures and cleanup status are recorded as in the ordinary gate.

The checks require HTTP 401 for missing, malformed, wrongly signed,
wrong-audience, and expired tokens. A short-lived token must first work, then
expire; a newly issued token for the same identity must restore access. The
expiry wait is bounded and includes the validator's clock-skew allowance.

A reader receives a SELECT grant on one table, reads its exact rows, and is
denied a second table that the owner can read. Removing the grant must revoke
access. Permission failures must be HTTP 403 or 404; transport errors and 5xx
responses cannot pass as denials. Every failed CLI scan must leave no output or
temporary file. CLI output, service logs, and the final JSON report are checked
for credentials and JWTs.

Client secrets are generated per run and injected into the imported realm using
Keycloak environment placeholders. The checked-in realm contains no secret.
OpenFGA uses a disposable in-memory datastore. Its port is internal; published
catalog, object-store, and token endpoints bind only to loopback. This setup
tests OIDC and catalog permissions over local HTTP. TLS, token refresh, and
production identity-provider deployment remain outside this gate.

## Check coverage

| Check | Required behavior |
| --- | --- |
| Current and historical snapshots | Exact rows and snapshot IDs; inspection UUID/version agrees with the writer |
| Projection and limits | Requested column order, bounded row count, schema-only zero-row output |
| Schema and partition evolution | Metadata-only rename/add/drop/type-promotion changes apply before another data write; re-added names do not resurrect dropped values; old and new partition specs coexist; historical schema remains readable |
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

The ordinary stack has no catalog identity provider; the authenticated overlay
adds the OIDC and OpenFGA checks above. Both modes verify actual S3 signatures and
downscoped temporary credentials. TLS, real AWS, Polaris, large-table performance,
and production readiness remain unverified. Passwords are generated per run and
credentials remain in temporary process memory, private files, or disposable
service containers that are removed after the run.

Lakekeeper contacts storage through `http://minio:9000`. Host clients use its
published loopback endpoint. The PyIceberg adapter changes only that endpoint;
NodusDB uses its normal `--storage-properties` override. Neither substitutes
manifests or REST responses. For this release, catalog discovery uses the
warehouse **name** (`interop`), while Lakekeeper returns its UUID as the REST
namespace prefix.

CI runs both variants in the `Live Lakekeeper interoperability` job and uploads
their JSON reports, including on failure. The fast protocol fixtures remain covered by
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
The authentication setup follows Lakekeeper's pinned
[Keycloak overlay](https://github.com/lakekeeper/lakekeeper/blob/v0.14.2/docker-compose/keycloak-overlay.yaml)
and [OpenFGA overlay](https://github.com/lakekeeper/lakekeeper/blob/v0.14.2/docker-compose/openfga-overlay.yaml),
with realm secrets supplied through [Keycloak placeholders](https://www.keycloak.org/server/importExport#_using_environment_variables_within_the_realm_configuration_files).

## Native SQL gate

Run `just test-iceberg-sql-live` for protected-profile registration, exact SQL
values compared with PyIceberg, typed extended projections, LIMIT, read-only
rejections, CancelRequest before rows, statement deadlines, atomic failed
registration, and restart recovery. The gate owns a fresh stack, private profiles,
and a stalled HTTP endpoint; it removes services and credentials on exit.
Evidence is written to `target/iceberg-live/sql-report.json`.
