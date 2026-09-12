# Import a PostgreSQL dump

This guide loads a plain-format `pg_dump` script into a running NodusDB server
and shows you how to read the compatibility report that comes back.

It assumes a server is already running and reachable on its admin port
(`127.0.0.1:8088` in the example configuration).

## 1. Produce a dump NodusDB can read

The most conservative producer invocation:

```bash
pg_dump --no-owner --no-privileges --no-comments \
        --inserts --rows-per-insert=500 \
        --quote-all-identifiers \
        -Fp -f dump.sql mydb
```

- `--inserts --rows-per-insert=N` emits the data section as batched `INSERT`s,
  which NodusDB executes directly.
- `--no-owner --no-privileges --no-comments` strips constructs NodusDB has no
  concept of, so they never reach the report as warnings.
- Plain format (`-Fp`) keeps the script dependency-ordered and replayable
  without `pg_restore`.

Stock `COPY`-based dumps import too — the importer decodes `COPY ... FROM stdin`
blocks itself. Use `--inserts` when you want the least surprising path, and see
[PostgreSQL dump compatibility](../reference/postgres-dump-compatibility.md) for
the full list of what gets translated, skipped, or rejected.

## 2. Send the dump to the import endpoint

Post the script as `text/plain` to `/api/v1/import`:

```bash
curl -X POST \
     -H "Authorization: Bearer nodus-dev-token" \
     -H "Content-Type: text/plain" \
     --data-binary @dump.sql \
     "http://127.0.0.1:8088/api/v1/import?on_error=continue&batch_rows=500"
```

The endpoint requires the `ManageBackups` privilege. Both admin auth schemes
work — a bearer token from `[admin] token`, or ordinary database credentials:

```bash
curl -u "nodus:nodus" -X POST \
     -H "Content-Type: text/plain" \
     --data-binary @dump.sql \
     http://127.0.0.1:8088/api/v1/import
```

Two query parameters control the run:

| Parameter | Values | Effect |
| --- | --- | --- |
| `on_error` | `continue` (default), `stop` | Whether a failing statement aborts the import. |
| `batch_rows` | integer, default `500` | Rows folded into each synthesized `INSERT`. |

> **`nodus_cli import` cannot authenticate.** The CLI sends no `Authorization`
> header, so `nodus_cli import --file dump.sql` fails with `401 Unauthorized`
> against any server that has admin auth configured. Use `curl` until the CLI
> learns to pass credentials.

## 3. Read the report

The response body is a versioned `ImportReport`. A clean run of a small dump
looks like this:

```json
{"report":{
  "import_report_version":1,
  "schemas_created":0,
  "tables_created":1,
  "indexes_created":1,
  "statements_executed":4,
  "statements_failed":0,
  "rows_inserted":2,
  "constraints_folded":1,
  "stopped_early":false,
  "skipped":[
    {"reason":"unsupported statement","statement":"SET statement_timeout = 0"},
    {"reason":"unsupported statement","statement":"SET search_path = public"}
  ],
  "lossy":[],
  "captured_sequences":[],
  "failures":[]
}}
```

Read it in this order:

1. **`failures`** — must be empty. Each entry names the statement, the table, and
   the error.
2. **`lossy`** — type coercions that lost information (for example `NUMERIC`
   mapped to a float). An import is only "clean" when this is empty.
3. **`skipped`** — constructs that were dropped on purpose, each with a reason.
   `SET` statements appearing here is normal.
4. **`constraints_folded`** — post-data `ALTER TABLE ... ADD CONSTRAINT`
   statements folded back into their `CREATE TABLE`, because NodusDB enforces
   constraints immediately and cannot add them later.
5. **`rows_inserted`** — compare against the source database.

Nothing is dropped silently: every statement lands in exactly one of
`statements_executed`, `skipped`, or `failures`.

## 4. Re-run an import safely

Imports are not idempotent. Running the same dump twice against the same
database reports the conflicts rather than duplicating data:

```json
{"failures":[
  {"kind":"create_table","table":"public.city",
   "error":"relation \"city\" already exists",
   "statement":"CREATE TABLE public.city (id INTEGER NOT NULL, name TEXT NOT NULL, country TEXT, CONSTRAINT city_pkey PRIMARY KEY (id))"},
  {"kind":"insert","table":"public.city",
   "error":"Unique constraint violation on primary key",
   "statement":"INSERT INTO public.city VALUES (1, 'Berlin', 'DE')"}
]}
```

That `CREATE TABLE` is also the clearest way to see constraint folding: the
primary key arrived as a separate post-data statement in the dump and was merged
into the table definition before execution.

To retry from a known state, drop the affected tables (or start from a fresh
data directory) and import again.

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `401 Unauthorized` | No or invalid credentials. | Send `Authorization: Bearer <admin token>` or `-u user:password`. |
| `403 Forbidden` | The principal lacks `ManageBackups`. | Grant the privilege, or use the admin token. |
| Rows missing, no failures | Statements were skipped. | Read `skipped`; the construct is unsupported. |
| Foreign key errors on load | Data loaded before its parent rows. | Import in dependency order; circular foreign keys are reported, not resolved. |
