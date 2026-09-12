# PostgreSQL dump compatibility

What the dump importer does with each construct in a plain-format `pg_dump`
script. Every statement ends up in exactly one of three buckets — executed,
skipped, or failed — and all three are reported. Nothing is dropped silently.

For the procedure, see
[import a PostgreSQL dump](../how-to/import-a-postgresql-dump.md); for the
design, see [PostgreSQL dump import](../explanation/postgres-dump-import.md).

## Supported dump profile

The profile the importer is built against:

```bash
pg_dump --no-owner --no-privileges --no-comments \
        --inserts --rows-per-insert=500 \
        --quote-all-identifiers \
        -Fp -f dump.sql mydb
```

Plain format only. `COPY`-based dumps also work — the importer contains a
PostgreSQL text and CSV `COPY` decoder, and the server implements
`COPY ... FROM stdin` — but `--inserts` exercises the narrowest path.

## Statement handling

| Construct | Handling | Recorded as |
| --- | --- | --- |
| `CREATE SCHEMA`, `CREATE TABLE`, `CREATE INDEX` | Executed. | `schemas_created`, `tables_created`, `indexes_created` |
| `INSERT ... VALUES` | Executed, batched into groups of `batch_rows`. | `rows_inserted` |
| `COPY <table> FROM stdin` | Body decoded to rows and inserted. | `rows_inserted` |
| Post-data `ALTER TABLE ... ADD CONSTRAINT` (primary key, unique, check, foreign key) | Folded back into the buffered `CREATE TABLE` before it executes. | `constraints_folded` |
| Other `ALTER TABLE` forms | Skipped. | `skipped`, reason `unsupported ALTER TABLE` |
| `SET ...`, `SELECT pg_catalog.set_config(...)` | Skipped. | `skipped`, reason `unsupported statement` or `session setup ignored` |
| `CREATE SEQUENCE`, `ALTER SEQUENCE` | Skipped. | `skipped`, reason `sequences unsupported` |
| `SELECT setval(...)` | Captured, not replayed. | `captured_sequences` |
| `CREATE EXTENSION` | Skipped. | `skipped`, reason `extensions unsupported` |
| `CREATE FUNCTION`, `CREATE OR REPLACE FUNCTION` | Skipped. | `skipped`, reason `functions unsupported` |
| `COMMENT ON` | Skipped. | `skipped`, reason `comments unsupported` |
| Non-data queries (`SELECT` outside `setval`) | Skipped. | `skipped`, reason `non-data query` |
| Anything else `sqlparser` rejects | Skipped. | `skipped`, reason `unsupported statement` |
| Malformed `COPY` header or body | Skipped, with the parse error attached. | `skipped`, reason `unparsable COPY header: …` / `undecodable COPY body: …` |
| A statement that executes and errors | Reported with statement, table, and error. | `failures` |

## Lossy translations

These execute, but information is lost. Each one appends a `LossyNote` naming
the table and column:

| Source construct | Result | Note text |
| --- | --- | --- |
| Column `DEFAULT`, `GENERATED`/identity | The option is stripped; explicit values in the data section still load. | `dropped DEFAULT/identity (no sequences); explicit data values preserved` |
| `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ` | Stored as text. | `temporal type stored as text` |
| `NUMERIC`, `DECIMAL` | Coerced to float; exactness is lost. | `exact numeric coerced to float` |
| `UUID`, `BYTEA`, `INET` | Stored as text. | `type stored as text` |

An import is *clean* only when `lossy` is empty.

## Why constraints are folded

NodusDB enforces constraints immediately and has no `ALTER TABLE ... ADD
CONSTRAINT`. A stock dump puts constraints in the post-data section, after the
rows are loaded. The importer therefore buffers each `CREATE TABLE` until it has
seen that table's post-data constraints, then emits one `CREATE TABLE` with the
constraints inline. The resulting emission order is:

1. `CREATE SCHEMA`
2. `CREATE TABLE`, with folded constraints, in dependency order
3. data — `INSERT` or decoded `COPY`, parents before children
4. `CREATE INDEX`

Because foreign keys are enforced on write, parent rows must load before child
rows. `pg_dump` already emits its data section in dependency order. Circular
foreign keys cannot be satisfied this way; they are reported rather than
silently broken.

## Import report

The response is a versioned JSON document (`import_report_version: 1`):

| Field | Type | Meaning |
| --- | --- | --- |
| `import_report_version` | integer | Report format version. |
| `schemas_created` | integer | Schemas created. |
| `tables_created` | integer | Tables created. |
| `indexes_created` | integer | Indexes created. |
| `statements_executed` | integer | Statements executed successfully. |
| `statements_failed` | integer | Statements that executed and errored. |
| `rows_inserted` | integer | Rows written, from both `INSERT` and `COPY`. |
| `constraints_folded` | integer | Post-data constraints merged into a `CREATE TABLE`. |
| `stopped_early` | boolean | True when `on_error=stop` aborted the run. |
| `skipped` | array | `{statement, reason}` per skipped statement. |
| `lossy` | array | `{table, column, detail}` per lossy translation. |
| `captured_sequences` | array | `setval` calls recorded but not replayed. |
| `failures` | array | `{kind, table, statement, error}` per failed statement. |

## Not supported

- Sequences and identity columns as objects — `nextval` is not modelled, so
  generated keys must be present in the dump's data.
- `pg_restore` archive formats: custom (`-Fc`), directory (`-Fd`), and tar.
- Deferred constraints and `SET CONSTRAINTS`.
- `psql` meta-commands beyond the ones the splitter recognises and drops.
- Idempotent re-import: running the same dump twice reports conflicts rather
  than reconciling them.
