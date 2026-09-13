# Driver Compatibility Matrix

These tests start a local NodusDB test server and run real PostgreSQL drivers
against the PostgreSQL protocol v3 surface.

Default matrix:

- pgJDBC: `42.7.11`, `42.7.7`, `42.6.2`
- Npgsql / EF Core provider: `10.0.3:10.0.2`, `9.0.4:9.0.4`

Override locally or in CI:

```bash
NODUS_PGJDBC_VERSIONS=42.7.11,42.7.7 cargo test -p nodus_compatibility_tests --test jdbc_smoke -- --nocapture
NODUS_NPGSQL_MATRIX=10.0.3:10.0.2,9.0.4:9.0.4 cargo test -p nodus_compatibility_tests --test npgsql_smoke -- --nocapture
```

The pgwire smoke suite also includes raw wire-level regressions for simple-query
batch result sequencing and binary COPY response metadata, independent of any
client library.

## Rust SQL compatibility audit

Run the Rust driver, protocol, and SQL golden suites without Java or .NET:

```bash
just test-compat-rust
```

This uses the workspace-locked `tokio-postgres` (tested with 0.7.18), starts
isolated local NodusDB servers, and runs all selected targets even if one fails.
It includes 55 SQL golden files, the `pg18_*` suites, wire/auth/TLS tests, and
`rust_driver_sql.rs`. Stored procedures, `CALL`, user-defined routine bodies,
and PL/pgSQL execution are outside this audit; built-in functions and catalog
metadata remain included.

The added driver cases check inferred and explicit parameter types, DML
`RETURNING`, Rust transaction/savepoint and cursor APIs, typed relational
results, NULL predicates, and COPY streams. Each case has a 30-second deadline.
The six failures from the initial audit are fixed. All 14 driver cases pass on
NodusDB and PostgreSQL 18.4 with the same assertions. See
[the results, fixes, and remaining limits](rust-sql-compatibility-results.md).

To validate the same 14 driver cases against an explicitly supplied,
disposable PostgreSQL reference:

```bash
NODUS_SQL_REFERENCE_URL='host=127.0.0.1 port=55432 user=nodus password=nodus dbname=default' \
  cargo test --locked -p nodus_compatibility_tests --test rust_driver_sql -- --nocapture
```

The reference user needs schema-creation privileges. Each case creates a unique
schema and removes it after success or an assertion failure; an interrupted or
timed-out run can leave its `rust_compat_*` schema behind. No reference server
is required for the normal NodusDB run. Keep `NODUS_SQL_REFERENCE_URL` unset
when testing NodusDB; it redirects only `rust_driver_sql`, not the other suites.
