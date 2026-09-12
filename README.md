# NodusDB

> **⚠️ STATUS: EXPERIMENTAL**  
> This project is in an early, experimental stage. It is under active development, and features, APIs, and the storage format are subject to breaking changes without notice. It is **not** recommended for production use.

![Language](https://img.shields.io/badge/Language-Rust-orange.svg)
![Edition](https://img.shields.io/badge/Edition-2024-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

A PostgreSQL-wire-compatible distributed SQL database written in Rust.

NodusDB targets high-load OLTP workloads by combining the familiar PostgreSQL interface with a distributed, strongly-consistent backend powered by Raft and MVCC.

## Features

- **PostgreSQL Wire Compatibility**: Connect using standard `psql` or any Postgres-compatible driver (powered by `pgwire`).
- **Distributed Architecture**: Shared-nothing, multi-shard routing with local secondary index support.
- **Raft Consensus**: Strong consistency per shard via `openraft`.
- **MVCC Storage**: Versioned key-value storage API isolating concurrent transactions securely.
- **Robust Access Control (RBAC)**: Deny-by-default central authorization engine with roles, database roles, and future/default grants.
- **Built-in Web Console**: Real-time cluster overview, active query monitoring, and visual RBAC access explanations via `axum`.
- **Comprehensive Auditing**: Built-in `nodus_audit` tracking critical security and DDL events.
- **Zero-Downtime Rolling Upgrades**: Versioned catalogs and network/storage format negotiation enabling uninterrupted deployments.
- **Online Backup & Restore**: Streamlined physical snapshotting and Point-In-Time-Recovery (PITR) mechanisms.
- **Observability First**: Built-in Prometheus metrics (`/metrics`), `/healthz`, and `/readyz` endpoints out-of-the-box.

### Shard administration limitation

Shard initialization, split, merge, and rebalance currently return HTTP `501`
with code `shard_migration_unavailable`. These operations are disabled until
durable migration and cluster-wide fencing can preserve committed data during
routing changes. This includes initialization of an empty table, which can race
concurrent writes. Read-only shard inspection and replication of existing shard
placements remain available. This safeguard does not repair data made unreachable
by earlier shard operations.

For existing sharded tables, row scans visit every intersecting shard in key
order at the transaction's fixed MVCC read timestamp. Version scans include
tombstones. An assigned shard without a local replica returns SQLSTATE `40001`
(retry the transaction after replica reconciliation); it never falls back to
unsharded storage. Metadata decoding and invalid range coverage also fail
explicitly. A routing descriptor change observed while consuming a scan aborts
that scan with `40001`.

With `SET nodus.linearizable_reads = on`, a range spanning multiple shards
returns SQLSTATE `0A000`: shared snapshot coordination is not implemented.
Independent per-shard Raft barriers would not establish that guarantee. Ordinary
MVCC scans remain available; remote KV reads and durable migration epochs are
still pending. The router opens one shard iterator at a time and adds no row
buffering; underlying in-memory scans and LSM version scans still materialize
their selected shard data. Secondary index keys retain their existing metadata
namespace, with base-row lookup errors now propagated to the query.
LSM reads also report missing or unreadable SSTables, truncated footers, and
decoding failures instead of silently omitting those sources. This change does
not alter persisted formats or repair damaged files.

## Getting Started
To run the server locally with durable storage and explicit dev credentials:

```bash
NODUS_CONFIG=nodus.toml.example cargo run --bin nodus_server
```

To run the CLI:
```bash
cargo run --bin nodus_cli -- help
```

The example config listens on `127.0.0.1:5432`, stores data under
`.local/nodus/`, and uses the local-only `nodus` / `nodus` database user.
Copy it to `nodus.toml` and change `[admin]` values before sharing an instance.

To run the Docker Compose test stack:

```bash
docker compose up --build
```

To rebuild and rerun it through the task runner:

```bash
just compose-rerun
```

## Development
The project uses `just` as its task runner.

To format, lint, and run all tests:
```bash
just check
```

For nonmutating formatting verification, use `cargo fmt --all --check`.
`just test-partition` runs the real-TCP Raft partition regression and propagates
failures; `just test-sim` is a compatibility alias, not a deterministic simulator.
CI also builds and lints the frontend using Node.js 24.
