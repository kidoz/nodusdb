# Crate map

Which crate owns what. Consult this before adding a crate or placing new code:
the answer is usually an existing crate.

## SQL and protocol

| Crate | Owns |
| --- | --- |
| `nodus_pgwire` | PostgreSQL wire protocol state, message codecs, `COPY` sub-protocol, SQLSTATE mapping. |
| `nodus_sql` | Parsing and SQL-facing planning concepts (wraps `sqlparser` with the PostgreSQL dialect). |
| `nodus_executor` | Planning, execution, operator orchestration, expression evaluation, DML, constraints. |
| `nodus_import` | Sink-agnostic import of plain-format PostgreSQL dumps: splitter, classifier, rewriter, `COPY` decoder, import report. |

## Catalog and metadata

| Crate | Owns |
| --- | --- |
| `nodus_catalog` | Database, schema, table, index, and role descriptors; catalog reader/writer/store. |
| `nodus_meta` | Durable cluster metadata: shard maps, placements, and the records the migration protocol writes. |
| `nodus_index` | Secondary index maintenance and backfill. |

## Storage

| Crate | Owns |
| --- | --- |
| `nodus_storage_api` | Storage traits and the stable request/response types every engine implements. |
| `nodus_storage_lsm` | The LSM engine: memtable, SSTables, manifest, compaction, local durability. |
| `nodus_storage_mem` | The in-memory engine used when no data directory is configured, and in tests. |
| `nodus_storage_wal` | Write-ahead log record format, segment metadata, and segment lifecycle. |
| `nodus_mvcc` | Version chains and visibility rules. |
| `nodus_txn` | Transaction manager, intents, and concurrency control. |

## Distribution

| Crate | Owns |
| --- | --- |
| `nodus_raftstore` | The `openraft` storage and network adapter, Raft snapshots, and the migration protocol's participant commands. |
| `nodus_sharding` | Shard routing: routing snapshots, key and range location, split/merge/rebalance planning. |
| `nodus_upgrade` | Rolling-upgrade state, feature gates, and cluster finalization. |

## Security

| Crate | Owns |
| --- | --- |
| `nodus_security` | Authentication, credential handling, and cryptographic primitives. |
| `nodus_authz` | The deny-by-default authorization engine: actions, roles, grants. |
| `nodus_audit` | Audit event model and sinks. |

## Operations

| Crate | Owns |
| --- | --- |
| `nodus_backup` | Repository backends, manifests, backup orchestration, restore planning and execution, verification. |
| `nodus_monitoring` | Prometheus metrics and health signals. |
| `nodus_telemetry` | OpenTelemetry tracing setup. |
| `nodus_web_console` | The built-in web console's server-side surface. |

## Entry points and shared code

| Crate | Owns |
| --- | --- |
| `nodus_server` | The server binary: admin HTTP API, multi-Raft wiring, session handling. Orchestration only — not a home for core database logic. |
| `nodus_cli` | Operator commands. Orchestration only, same rule. |
| `nodus_config` | Node configuration loading and `NODUS_`-prefixed environment overrides. |
| `nodus_common` | Primitives shared across crates, including versioned-record helpers. |
| `nodus_error` | Shared error types. |
| `nodus_testkit` | Test harness: in-process clusters, fault injection, fixtures. |

## Placement rules

- When a file grows too large, split it into private modules inside the same
  crate first.
- Extract a new crate only for a real architectural boundary, a separate
  dependency set, or a stable API several crates need.
- Keep crate roots small: private modules plus a deliberate `pub use` surface.
  Avoid `pub mod` as a shortcut.
- Low-level crates must not gain dependencies that would leak into storage,
  format, or protocol APIs.
