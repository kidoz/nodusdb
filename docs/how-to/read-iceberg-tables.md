# Read an external Iceberg table

The optional `iceberg` CLI feature inspects an external Iceberg REST catalog and
reads a selected table snapshot to an Arrow IPC stream file. This is an
experimental integration. CLI inspection and export do not require a running
NodusDB server. The optional native SQL registration is described below.

## Build the client

```sh
cargo build -p nodus_cli --features iceberg --locked
./target/debug/nodus_cli iceberg --help
```

The Cargo binary is named `nodus_cli`; release packaging calls it `nodusctl`.
The default CLI build omits this feature and its analytical dependencies.

## Connect and inspect

Use the REST base URL **before `/v1`**. For Lakekeeper this commonly ends in
`/catalog`. Obtain a bearer token from the catalog's identity provider and supply
it through `NODUS_ICEBERG_TOKEN`. The client never reads `NODUS_ADMIN_TOKEN` for
catalog authentication, and rejects the global `--token` option for these
commands. An unset Iceberg token permits an unauthenticated development catalog.

HTTP redirects are rejected. A catalog URI override may change the base path
only within the configured origin: scheme, host and effective port must stay
the same. Cross-origin overrides and HTTPS downgrades fail before a table
request is sent. Catalog properties cannot replace the caller's bearer token
or supply authentication headers.

The following examples are illustrative: substitute your endpoint, warehouse,
namespace and table, and set `NODUS_ICEBERG_TOKEN` in your shell first.

```sh
./target/debug/nodus_cli iceberg inspect \
  --catalog-uri http://localhost:8181/catalog \
  --warehouse YOUR_WAREHOUSE_NAME \
  --namespace analytics --table events
```

Inspection prints a versioned JSON object with the table UUID, format version,
selected snapshot ID, the read schema, and output column names. Credentials,
storage locations, and catalog properties are omitted. Repeat `--namespace` for
a multi-level namespace; a dot inside a component is a literal dot.
Lakekeeper 0.14.2 expects the warehouse name for catalog discovery; use the
identifier required by your catalog release.

## Read a snapshot

```sh
./target/debug/nodus_cli iceberg scan \
  --catalog-uri http://localhost:8181/catalog \
  --warehouse YOUR_WAREHOUSE_NAME \
  --namespace analytics --table events \
  --column id --column name \
  --limit 1000 --batch-size 1024 --timeout-seconds 120 \
  --output events.arrow
```

Omit `--column` to select all columns. Names are case sensitive, and output
columns follow the requested order. Omit `--limit` to read all rows in the
selected snapshot. Row order is unspecified, so a limit is not a deterministic
sample. `--limit 0` writes a schema-only file after snapshot validation.

Use `--snapshot-id ID` to select a retained historical snapshot. Without it, the
operation loads current metadata once and pins both the current snapshot and
the current table schema. Schema-only changes such as column additions,
renames, drops, and type promotions take effect without a subsequent data write.
Explicit `--snapshot-id` reads use that snapshot's historical schema, even when
the ID equals the current snapshot. A missing or expired snapshot fails; the
client never silently falls back to current data.

The destination must not exist. The client writes a private temporary file in
the destination directory and publishes it only after all requested batches
and the Arrow stream end marker have been written successfully. A scan failure or
cancellation before publication removes the temporary file. Existing files,
including a file created concurrently, are preserved. Cancellation racing with
the final publication can leave a **complete** destination file. This is not a
backup durability contract or a NodusDB transaction.

On success, stdout contains a JSON summary with `version`, `snapshot_id`, `rows`,
`output`, and `format` (`arrow-ipc-stream`). Errors go to stderr with a nonzero
exit status. Arrow preserves types and nulls without converting them into JSON
numbers or strings. A Python application with PyArrow can consume the result
using `pyarrow.ipc.open_stream("events.arrow").read_all()`; this last call
materializes the result in the application's memory.

## Configure storage

S3 is the default backend. The client requests vended credentials from the REST
catalog, supports load-table `config` properties, and otherwise uses the
`object_store` AWS environment/credential chain. Local properties override
properties returned by the catalog; omit local keys when using vended credentials.

For an S3-compatible development service, a private JSON properties file may
contain the following illustrative values:

```json
{
  "s3.endpoint": "http://localhost:9000",
  "s3.region": "us-east-1",
  "s3.path-style-access": "true",
  "s3.allow-http": "true"
}
```

Pass it with `--storage-properties /path/to/storage.json`. The supported keys
are exactly those above plus `s3.access-key-id`, `s3.secret-access-key`, and
`s3.session-token`. Keep credential-bearing files private. Unsupported S3
properties fail instead of being silently ignored. Remote signing, role
assumption configured through Iceberg properties, OAuth client-credential
exchange, and automatic token refresh are not supported by this slice.

Use `--storage local` only with a trusted development catalog whose absolute
filesystem paths or `file://` URLs are accessible on the client host. S3 mode
cannot open local paths. The storage adapter rejects all write and delete
operations. Catalog and storage permissions authorize these external reads;
the standalone CLI does not use NodusDB RBAC or audit. SQL registration below
adds central authorization and audit.

## Supported subset and limits

- Iceberg v1/v2 with Parquet data, current or retained historical snapshots,
  and top-level column projection. V3 and any delete manifest are rejected.
  Deleted *data-file entries* in data manifests are correctly skipped.
- Namespace components and table names contain ASCII letters, digits, `_`, `-`,
  or `.`; `.` and `..` alone are rejected. SQL quoted identifiers are not parsed.
- Batch size is 1–65,536 rows. Data files and manifest work use concurrency 1.
  Manifest lists are limited to 16 MiB and 4,096 entries; declared manifest sizes
  are limited to 16 MiB. Individual storage range reads are limited to 64 MiB.
  Large Parquet column chunks can therefore fail explicitly.
- These limits do not provide a hard process-memory sandbox: catalog JSON,
  Avro/Parquet metadata decoding, and variable-width values can allocate memory
  beyond a batch's row count. Use trusted catalogs and datasets for this slice.
- The 1–86,400 second deadline covers catalog loading, planning and batch reads,
  including idle time between batches. Local output synchronization/publication
  is outside the read deadline. Ctrl-C cancels consumption.
- Metadata and data objects must remain immutable and retained during the scan.
  The reader does not acquire a retention lease. Deleted or unreadable objects
  fail the operation; they do not become an empty result.
- The CLI slice is separate from SQL registration below. Iceberg export, CDC,
  and cross-system transactions are not implemented.

## Verification

```sh
cargo test -p nodus_iceberg -p nodus_cli --features nodus_cli/iceberg --locked
```

CI runs this feature-enabled suite and clippy explicitly, in addition to the
default workspace checks.

Automated fixtures use REST configuration/table responses, generated Avro
manifests and Parquet files, and a local S3 HTTP range-read endpoint. They cover
snapshot selection, projections, nulls, empty tables, row limits, missing files,
errors, and output publication. The S3 fixture checks credential selection and
request shape, not AWS signature cryptography.

The separate [live Lakekeeper gate](test-lakekeeper-interoperability.md), run with
`just test-iceberg-live`, uses pinned Lakekeeper and MinIO services plus an
independent PyIceberg writer. It verifies schema/partition evolution, temporary
storage credentials, and read/failure behavior against real services. Its
authenticated variant, `just test-iceberg-auth-live`, adds Keycloak OIDC and
OpenFGA table grants, revocation, token expiry, and credential-redaction checks.
TLS, Polaris, real AWS, and large-table performance remain unverified.

See the [official Rust implementation](https://github.com/apache/iceberg-rust)
and [Iceberg REST specification](https://iceberg.apache.org/rest-catalog-spec/)
for the underlying protocol and table model.

## Register a read-only SQL view

The experimental native SQL slice runs the feature-enabled CLI as an isolated
worker. The server build keeps Arrow/Parquet outside its allocator. This slice
requires Linux or macOS and S3 storage; it does not expose local-file access.

Install the feature-enabled binary on every serving node. Configure the node
with absolute paths (the following paths are illustrative):

```toml
[iceberg]
enabled = true
worker_executable = "/opt/nodus/bin/nodusctl"
profiles_file = "/etc/nodus/iceberg-profiles.json"
timeout_seconds = 60
worker_memory_mib = 512
worker_address_space_mib = 2048
max_result_bytes = 16777216
max_rows = 100000
max_concurrent_scans = 2
```

Create a regular, private profiles file, readable by the server account, with
mode `0600`. Symlinks and group/other permissions are rejected. The file is at
most 1 MiB and is loaded at startup; restart nodes to rotate profiles. Each
profile's storage properties follow the supported `s3.*` keys above. For example:

```json
{
  "version": 1,
  "profiles": {
    "lake": {
      "uri": "https://catalog.example/catalog",
      "warehouse": "analytics",
      "bearer_token": "REPLACE_WITH_CATALOG_TOKEN",
      "storage_properties": {}
    }
  }
}
```

Use `null` for `bearer_token` with an unauthenticated development catalog.
Vended S3 credentials remain supported. The worker clears its inherited
environment and disables ambient AWS credentials and instance metadata. Tokens
and storage properties travel through a private stdin pipe, never arguments,
SQL text, persisted descriptors, or worker diagnostics.

As a principal with `MANAGE_CLUSTER` on the system and `CREATE` on the schema,
register one namespace component and the remote table name:

```sql
CREATE VIEW lake_events AS
SELECT * FROM nodus_iceberg('lake', 'analytics', 'events');

SELECT name AS label, id FROM lake_events LIMIT 100;
```

Registration inspects the source completely before one replicated catalog
publication. It cannot run inside a transaction/batch, use `OR REPLACE`, or
create a temporary view. Ordinary readers need `SELECT` on the registered view;
authorization and audit run before worker/source access. `LIMIT 0` and driver
Describe use the registered schema locally, after authorization.

A scan loads metadata once and pins the current snapshot. The descriptor stores
its version, profile alias, table UUID, field IDs, names, types and nullability.
The selected fields must still match; table replacement or selected-field drift
fails. Unselected new or changed fields do not alter the registered columns.
Drop and register the view again to adopt schema changes. Registration supports
1–256 primitive columns: boolean, int/long, float/double, string, binary, date,
microsecond timestamp/timestamptz, and decimal128. SQL exposes decimals as
`NUMERIC`; the descriptor retains source precision/scale for drift checks.
Column names must be ASCII identifiers (`[A-Za-z_][A-Za-z0-9_]*`) of at most
128 bytes; this avoids ambiguous quoted/qualified identifiers in persisted plans.
Nested values, time, UUID, and other types fail registration explicitly.

Direct reads support column projection, aliases, duplicates, and `LIMIT`.
Predicates, joins, ordering, aggregation, DISTINCT, and OFFSET on the external
scan fail explicitly. There is no SQL time travel, predicate pushdown, DML,
external schema alteration, or cross-system transaction. Each statement reads
its own external snapshot, independent of NodusDB's MVCC transaction snapshot.

The query deadline is the smaller of `statement_timeout` and
`iceberg.timeout_seconds`. PostgreSQL CancelRequest and client disconnect abort
consumption. An independent supervisor kills and reaps workers even under
socket backpressure. It samples worker RSS approximately every 10 ms; the RSS
threshold can overshoot between samples. Linux additionally imposes a hard
address-space limit before source access/decoding, including decoder buffers.
macOS uses RSS supervision and process isolation; it has no equivalent hard
address-space guarantee in this slice. Configure host/container limits when a
strict aggregate process memory ceiling is required.

Rows are limited to 16 KiB of accounted values and protocol frames to 64 KiB.
Total accounted result memory/output is bounded by `max_result_bytes`, including
streamed results. The worker decodes 64-row batches, but Parquet pages and
metadata can require more memory and are covered by worker supervision. At most
`max_concurrent_scans` workers run on a node; excess requests fail immediately.
A missing LIMIT reads at most `max_rows + 1` and fails on overflow. An explicit
LIMIT above `max_rows` fails. No resource limit silently truncates a result.
Streaming can deliver rows before a terminal read error; clients must check
command completion before accepting the result.

External views use the existing durable view/TableFunction codec. Older binaries
can decode it and fail closed on the unknown function; they cannot read the
external source. Keep the feature disabled during rollout until every serving
node has the worker and profiles. Nodes without configuration reject scans.
`DROP VIEW lake_events` removes registration without changing external data and
is the rollback path before reverting binaries. No new irreversible catalog
record or feature level is introduced.

Run `just test-iceberg-sql-live` to compare SQL values against an independent
PyIceberg scan and verify typed extended queries, cancellation before the first
row, deadlines, failed-publication atomicity, durable registration across
restart, and real wide-Parquet decoder termination at a 64 MiB RSS threshold
with continued SQL server availability.
