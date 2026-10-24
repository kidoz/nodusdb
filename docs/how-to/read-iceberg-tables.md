# Read an external Iceberg table

The optional `iceberg` CLI feature inspects an external Iceberg REST catalog and
reads a selected table snapshot to an Arrow IPC stream file. This is an
experimental client-side integration. It does not register tables in NodusDB SQL
or require a running NodusDB server.

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

The following examples are illustrative: substitute your endpoint, warehouse,
namespace and table, and set `NODUS_ICEBERG_TOKEN` in your shell first.

```sh
./target/debug/nodus_cli iceberg inspect \
  --catalog-uri http://localhost:8181/catalog \
  --warehouse YOUR_WAREHOUSE_ID \
  --namespace analytics --table events
```

Inspection prints a versioned JSON object with the table UUID, format version,
selected snapshot ID, its Iceberg schema, and output column names. Credentials,
storage locations, and catalog properties are omitted. Repeat `--namespace` for
a multi-level namespace; a dot inside a component is a literal dot.

## Read a snapshot

```sh
./target/debug/nodus_cli iceberg scan \
  --catalog-uri http://localhost:8181/catalog \
  --warehouse YOUR_WAREHOUSE_ID \
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
operation loads current metadata once and pins that snapshot. A missing or
expired snapshot fails; the client never silently falls back to current data.

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
no NodusDB admin API, RBAC resource, or audit sink participates.

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
- This is not native SQL federation, Iceberg export, CDC, or a cross-system
  transaction. No NodusDB durable records or upgrade gates are changed.

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
request shape, not AWS signature cryptography. A deployed Lakekeeper/Polaris
instance, real AWS credentials, partition evolution, and large-table performance
have not been certified by these fixtures.

See the [official Rust implementation](https://github.com/apache/iceberg-rust)
and [Iceberg REST specification](https://iceberg.apache.org/rest-catalog-spec/)
for the underlying protocol and table model.
