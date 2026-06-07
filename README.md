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

## Getting Started
To run the server locally:
```bash
just run
```

To run the CLI:
```bash
cargo run --bin nodus_cli -- help
```

## Development
The project uses `just` as its task runner.

To format, lint, and run all tests:
```bash
just check
```
