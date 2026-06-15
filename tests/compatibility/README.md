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
