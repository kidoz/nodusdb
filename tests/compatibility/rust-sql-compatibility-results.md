# Rust PostgreSQL SQL compatibility results

Verified: 2026-09-14. Driver: workspace-locked `tokio-postgres` 0.7.18.
Target: the local NodusDB working tree using isolated `nodus_testkit::TestServer`
instances. Reference: PostgreSQL 18.4 in a disposable local `postgres:18-alpine`
container, with a separate schema for each case. Stored procedures, `CALL`,
user-defined routine bodies, and PL/pgSQL execution remain outside this work.

## Results

| Suite | NodusDB | PostgreSQL 18.4 |
| --- | --- | --- |
| Rust SQL driver cases (`rust_driver_sql`) | 14 passed | 14 passed |
| Other Rust compatibility targets (`pg18_*`, pgwire, SCRAM, TLS) | 52 passed | Not run |
| Rust client integration target | 1 passed | Not run |
| SQL golden runner | Passed: 55 files, 816 records | Not run |
| SQL, executor, and pgwire unit tests | 61 passed | Not applicable |

The golden runner's two calibration/diagnostic helpers were intentionally not
run. Java and .NET runtime tests were outside this Rust-driver task. The reference
comparison covers the 14 driver cases; it does not certify the complete SQL
language or all existing golden expectations as PostgreSQL-compatible.

## Fixes since the initial audit

The 2026-09-13 audit found six failures among nine new cases. All six are now
fixed, and five additional driver cases cover adjacent state transitions.

| Area | Previous failure | Current behavior |
| --- | --- | --- |
| Parameter inference | An untyped INSERT prepared three UNKNOWN parameters | Common INSERT VALUES, UPDATE, DELETE, and SELECT expression contexts infer types from the AST and authorized catalog columns; inferred types are stored for Bind as well as Describe |
| DML RETURNING | UPDATE returned rows without matching Describe metadata | INSERT/UPDATE/DELETE returning columns use a side-effect-free zero-row read probe |
| Failed transactions | SELECT succeeded after a CHECK violation | Both query protocols reject subsequent commands with `25P02`; rollback-to-savepoint recovers the transaction, and COMMIT in a failed transaction aborts its writes |
| Cursor exhaustion | A completed portal re-executed its query | Completed portals retain an empty result state, including fetch-all execution; row buffers are freed and session cleanup releases cursor state |
| Aggregate types | Rust could not decode COUNT/SUM because Describe and Execute disagreed | Aggregate result types come from expressions and declared input types, including before any row exists |
| COPY OUT | COPY returned no data and reported COPY 0 | Table and query output uses the bounded executor stream, with text, CSV, and binary encoding, actual column metadata, and actual row counts |

Expanded COPY checks also exposed empty text being coerced to NULL during
writes. Empty text now remains distinct from NULL, including text comparisons
and scalar/IN subqueries. A missing COPY table now reports `42P01`; failed COPY
updates the transaction state. Raw-wire coverage verifies the data messages,
command count, and ReadyForQuery transaction status. The former smoke assertion
that expected an empty COPY OUT stream now checks the exported values.

Aggregate type expectations follow the
[PostgreSQL 18 aggregate documentation](https://www.postgresql.org/docs/18/functions-aggregate.html).
Portal and COPY response ordering follows the
[PostgreSQL 18 protocol flow](https://www.postgresql.org/docs/18/protocol-flow.html),
with the driver behavior also checked against the reference server.

## Running the checks

```bash
just test-compat-rust
cargo test --no-fail-fast -p nodus_executor -p nodus_pgwire -p nodus_sql
cargo fmt --all -- --check
just clippy
```

All commands above passed. The Rust-only command includes `--no-fail-fast`, so a
future regression does not prevent the other selected targets from running.
Formatting, workspace Clippy (all targets, warnings denied), and
`git diff --check` passed. The full workspace runtime suite was not run; runtime
checks focused on SQL, executor, wire protocol, and client compatibility.

To repeat an individual driver case:

```bash
cargo test -p nodus_compatibility_tests --test rust_driver_sql TEST_NAME -- --nocapture
```

To run all 14 cases against a disposable PostgreSQL reference, set
`NODUS_SQL_REFERENCE_URL` as described in [the driver README](README.md).
The local reference container used for verification was stopped and removed.

## Scope and remaining limitations

- This proves the tested subset, not complete PostgreSQL compatibility.
- Parameter inference is conservative. More complex expressions and query scopes
  can still require explicitly supplied types with `prepare_typed` or
  `query_typed`; this is not a complete PostgreSQL type resolver.
- COPY OUT supports default text/CSV settings, binary format for supported wire
  codecs, and CSV HEADER. Custom delimiters, other options, and legacy option
  syntax are rejected explicitly. Server-side files/programs are not executed.
- Plain table COPY streams with bounded buffering. Complex query sources inherit
  the executor's existing materialization behavior for joins, grouping, sorting,
  and other operators.
- No durable record or storage format changed; no migration is required. The
  empty-text fix applies to new writes and cannot reconstruct values previously
  stored as NULL.
- Stored procedures remain deferred. Java/.NET driver matrices and broad
  concurrency, restart, and performance testing remain separate verification.
