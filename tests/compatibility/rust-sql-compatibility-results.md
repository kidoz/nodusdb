# Rust PostgreSQL SQL compatibility results

Audit date: 2026-09-13. Driver: `tokio-postgres` 0.7.18 from the workspace lockfile.
NodusDB: local working tree, ephemeral `nodus_testkit::TestServer` servers.
Reference: PostgreSQL 18.4, local `postgres:18-alpine` container, verified with
`SELECT version()`. Each reference case used an isolated schema. No stored
procedures or PL/pgSQL execution were tested.

## Results

| Suite | NodusDB | PostgreSQL 18.4 |
| --- | --- | --- |
| Existing Rust compatibility targets (`pg18_*`, pgwire, SCRAM, TLS) | 51 passed | Not run |
| Existing Rust client integration target | 1 passed | Not run |
| SQL golden runner | Passed: 54 files, 810 records | Not run |
| New `rust_driver_sql` target | 3 passed, 6 failed | 9 passed |

The golden runner's two calibration/diagnostic helpers were intentionally not
run. Java and .NET suites were outside this Rust-only audit. Passing the existing
suites does not establish full PostgreSQL SQL compatibility: the new cases expose
behavior they do not cover. The reference comparison applies to the nine new
cases only, not to the repository's golden expectations.

## Confirmed differences

All nine new cases are ordinary, non-ignored tests. Run an individual reproducer
with `cargo test -p nodus_compatibility_tests --test rust_driver_sql TEST_NAME -- --nocapture`.
Their assertions pass unchanged on PostgreSQL 18.4.

| Test / operation | PostgreSQL expectation | NodusDB observation |
| --- | --- | --- |
| `inferred_parameter_types`: prepare `INSERT INTO items (id, name, enabled) VALUES ($1, $2, $3)` | Parameter types `INT4`, `TEXT`, `BOOL` inferred from the table | Three `UNKNOWN` types; normal Rust typed bindings cannot use them |
| `typed_parameters_and_dml_returning`: explicitly typed `UPDATE items SET name = $1 WHERE id = $2 RETURNING id, name` | Two typed result columns and one updated row | Driver rejects response: `DataRow field count does not match the number of columns` |
| `transaction_api_savepoints_and_error_recovery`: CHECK violation inside a savepoint, followed by SELECT | CHECK reports `23514`; subsequent SELECT reports `25P02` until rollback | CHECK reports `23514`, but SELECT succeeds in the failed transaction |
| `cursor_api_resumes_without_lost_or_duplicate_rows`: fetch four matching rows in batches of two, then fetch again | Batch lengths `2, 2, 0` | Batch lengths `2, 2, 2`: exhausted portal starts returning rows again |
| `extended_group_by_and_having`: grouped `COUNT(*)` and `SUM(INTEGER)` | Integer group key and two decodable `INT8` aggregate values | Rust cannot deserialize the COUNT result as `i64` |
| `copy_stream_api_round_trip`: COPY FROM followed by COPY TO through driver streams | COPY IN inserts three rows; COPY OUT exports those rows | Insert count and SELECT confirm all three rows; COPY OUT returns no data |

Passing new cases: parameterized CTE execution with explicitly supplied types,
NULL/NOT IN semantics, and typed LEFT JOIN results in the final run. A preliminary
run also observed a LEFT JOIN decode failure; it did not recur in the final run,
so that path warrants additional investigation rather than a reliability claim.
The DML case successfully checks INSERT RETURNING with Unicode, quoted text, and
NULL before failing on UPDATE. Later assertions within a failed case are not
claimed as verified on NodusDB (for example, DELETE RETURNING and post-error
savepoint recovery).

## Follow-up work

- Infer parameter types from parsed statements and catalog types, including the
  Bind path. `extended_query.rs::do_describe_statement` currently supplies
  `UNKNOWN` when the client omits type OIDs.
- Keep Describe metadata consistent with Execute row shape and binary encoding
  for UPDATE RETURNING and aggregate results; investigate the intermittent join
  decoding observation alongside these paths.
- Enforce the failed-transaction state on extended queries and preserve the
  completed state of an exhausted portal.
- Implement COPY OUT data streaming. The extended-query COPY OUT branch currently
  sends CopyOutResponse, CopyDone, and `COPY 0` without any CopyData messages.

This change adds tests, a runner command, and evidence; it does not repair the
server behaviors above or implement stored procedures. `just test-compat-rust`
and the general compatibility suite will remain red until those gaps are fixed.

## Validation commands

- `just test-compat-rust`: existing selected suites pass; the six new NodusDB
  failures above cause a nonzero exit. `--no-fail-fast` lets the SQL corpus run
  despite those failures.
- `NODUS_SQL_REFERENCE_URL=... cargo test -p nodus_compatibility_tests --test rust_driver_sql -- --nocapture`:
  all nine cases pass on the disposable PostgreSQL 18.4 reference.
- `cargo fmt --all -- --check`: passed.
- `just clippy` (workspace, all targets, warnings denied): passed.
- `git diff --check`: passed.

The full workspace runtime test suite was not run: this audit changes only
compatibility tests, their command, and documentation. The temporary reference
container was stopped and removed after validation.
