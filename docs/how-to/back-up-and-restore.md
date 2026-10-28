# Back up and restore

This guide creates and verifies full and incremental backups, restores a fresh
single node, runs point-in-time recovery (PITR), and proves the result with a
restore drill.

## Choose a repository

Backups go to the repository named by `[backup] repository_uri` in the server
configuration. The repository decision is made once at startup; every backup,
WAL archive, and restore on that server uses it.

- **Filesystem** — `file:///var/lib/nodus/backups` or a bare path. An empty URI
  falls back to a unique temp directory for development and tests. Put the
  directory on a distinct, durable backup volume. A backup on the same node is
  not protection against node loss.
- **S3-compatible object store** — `s3://bucket[/prefix]`. This is the production
  default: use an off-cluster, durable, versioned repository with encryption at
  rest and immutable retention where available. Credentials, region, and endpoint
  are never read from the config file. They come from the AWS-compatible
  environment consumed by `object_store`:
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
  `AWS_ENDPOINT_URL`, and `AWS_ALLOW_HTTP=true` for a plain-HTTP test endpoint.
  Any other URI scheme fails startup with an error naming the scheme.

Catalog snapshots include role definitions, grants, and SCRAM verifiers. Protect
the repository and its encryption keys accordingly.

## Backup format compatibility

The durable upgrade level controls the format a server writes:

| Source compatibility level | Manifest / object format | Security coverage |
| --- | --- | --- |
| Level 1 (before `snapshot-v2` finalization) | V2 / V1 | Principals and grants; role memberships are not emitted, preserving the old-reader boundary. |
| Level 2 or later (after `snapshot-v2` finalization) | V3 / V2 | Principals, grants, role memberships, catalog, rows, and tombstones. |

New binaries read both format pairs. Older binaries reject V3 rather than
silently restoring incomplete security state. V2 backups did not record role
memberships, so they cannot prove complete membership recovery; create a fresh
V3 backup after finalization when that evidence is required. Restore preserves
the destination's upgrade-authority policy rather than importing the source's.

Every route below requires `ManageBackups`. The examples use a bearer token; see
[admin API authorization](../reference/admin-api-authorization.md).

## Create a full backup

```bash
curl -X POST -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups
```

```json
{"backup_id":"88767e7d-4394-4bb9-b3c1-3cda4fec6af3","files":3,"status":"Completed"}
```

Only `"status":"Completed"` means the backup is restorable. The final
`manifest.json` is published after every referenced object passes checksum
verification. A failed or interrupted backup is never advertised as complete.

On clusters that finalized the migration level (`migration-v3`), every backup
is additionally journaled on the meta group: the job is cluster-visible, an
incremental export's parent snapshot is pinned against GC for the job's
lifetime, and backups and shard migrations mutually exclude — a backup request
during an in-flight migration (or a migration request during an export) is
refused with a conflict so the operator can retry after it settles. If the
exporting node dies mid-backup, the meta leader marks the abandoned job
`Failed`; it never appears as a restorable backup.

## Create an incremental backup

Pass the parent backup ID:

```bash
curl -X POST -H "Authorization: Bearer nodus-dev-token" \
     "http://127.0.0.1:8088/api/v1/backups?parent_backup_id=<full_backup_id>"
```

The incremental contains changed MVCC versions and tombstones since its verified
parent. Restore resolves the complete parent chain. An incomplete chain, an
unavailable parent, or required history that is no longer available fails the
operation; take a new full backup instead.

## List and verify backups

```bash
curl -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups

curl -X POST -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups/<backup_id>/verify
```

Verification checks the completed manifest, object existence, and SHA-256
checksums. It is necessary but not sufficient: a semantic restore drill below
proves that the catalog, permissions, indexes, and rows are usable.

## Restore into a fresh single node or cluster

Restore requires an empty destination engine. In-place restore over live data
is refused with HTTP `409` (`restore_destination_not_empty`). On a cluster the
restore runs only while the cluster is fresh (no data anywhere); a cluster
holding data is refused with HTTP `409` (`restore_clustered_not_empty`). The
replay replicates to every member through the groups' Raft logs, so all
members serve the restored state afterwards. Restore a fresh cluster before
opening it for traffic: the restore fences apply to the node serving the
request, so other members must have nothing to serve yet.

```bash
curl -X POST -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups/<backup_id>/restore
```

The response reports the restored objects and replay counts. Restores are
serialized, input is validated before mutation, and queries are fenced while
mutation runs. A failed mid-mutation restore stays fenced and can be resumed only
with the same backup identity. To abandon it, restart with another empty data
directory.

Node-local and consensus-local records (Raft votes/logs, recovery generation,
upgrade authority, and migration control keys) are neither exported nor replayed.
See [backup contracts](../reference/backup-contracts.md) for the exact set.

### Isolate the restored node's archive

A restored node starts allocating local WAL segment IDs from its own history.
Its repository prefix must not reuse the source node's active archive. WAL
publication is idempotent for identical retries and rejects an attempt to replace
an existing segment or index with different bytes; this protects the source
archive, but a collision prevents the restored node from archiving that segment.

Before resuming normal WAL archiving on the restored node, stop it and configure
a new empty `s3://bucket/prefix` or filesystem repository directory. Keep the
source repository unchanged for audit and further restores. Timeline allocation
is not yet automatic.

## Restore to a point in time

Add `target_ts` to replay archived WAL records up to a timestamp in microseconds
since the Unix epoch:

```bash
curl -X POST -H "Authorization: Bearer nodus-dev-token" \
     "http://127.0.0.1:8088/api/v1/backups/<backup_id>/restore?target_ts=1750000000000"
```

The server selects the newest completed backup at or before the target, verifies
its full/incremental chain and archived WAL, and stops replay at the target.
Commits after the target and incomplete transactions stay invisible. If no base
backup or continuous segment lineage can reach the timestamp, the response
contains an error and the destination is not changed.

PITR requires the WAL archiver to have run and the required segments to remain in
the repository. Retention must preserve the complete chain from the selected
base backup through the target.

## Delete a backup

```bash
curl -X DELETE -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups/<backup_id>
```

Deletion is manual and chain-aware. The server refuses to remove a parent needed
by a retained incremental, and refuses deletion while any manifest is unreadable.
There is no scheduled retention policy yet. Size retention from your recovery
point objective, full/incremental cadence, WAL rate, chain length, and restore
time objective; keep enough free space for a complete new full backup plus its
verification window.

## Use the CLI

`nodusctl backup create|list|verify|restore` wraps the same routes. Supply the
admin token globally or through the environment:

```bash
nodusctl --token nodus-dev-token backup list --addr http://127.0.0.1:8088
NODUS_ADMIN_TOKEN=nodus-dev-token nodusctl backup verify <backup_id>
```

## Run restore drills

The integration drill creates a disposable persistent source, finalizes
`snapshot-v2`, creates a table and secondary index, configures a transitive role
grant, and writes inserts, updates, and deletes. It restores full, incremental,
and PITR backups into separate empty destinations, checks rows and tombstones
through SQL, logs in as the restored role, checks index and privilege metadata,
and restarts each destination to repeat the checks.

Run the filesystem drill:

```bash
cargo test -p nodus_integration_tests --test restore_drill \
  persistent_full_incremental_pitr_restore_drill --locked -- --nocapture
```

It writes:

- `target/restore-drill/filesystem.json` — schema-2 status, source and deployment
  metadata, expected and actual rows, permission/index/restart results, and
  per-scenario RPO/RTO measurements;
- `target/restore-drill/filesystem.prom` — Prometheus textfile metrics for the
  latest run and the last successful/failed run, including
  `nodus_restore_drill_restore_to_ready_seconds` by scenario.

The S3 drill runs when the live S3 environment is configured. It additionally
requires the AWS variables used by the server:

```bash
NODUS_S3_ENDPOINT=http://127.0.0.1:9000 \
NODUS_S3_BUCKET=nodus-test \
NODUS_S3_KEY=nodus \
NODUS_S3_SECRET=nodus-secret \
AWS_ACCESS_KEY_ID=nodus \
AWS_SECRET_ACCESS_KEY=nodus-secret \
AWS_REGION=us-east-1 \
AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
AWS_ALLOW_HTTP=true \
  cargo test -p nodus_integration_tests --test restore_drill \
  s3_full_incremental_pitr_restore_drill --locked -- --nocapture
```

Use an isolated bucket for each run. The report is
`target/restore-drill/s3.json` with `target/restore-drill/s3.prom` beside it.
Set `NODUS_RESTORE_DRILL_REPORT` or `NODUS_S3_RESTORE_DRILL_REPORT` to choose
other paths.

Each scenario's `recovery_objectives.rpo` records missing expected rows and
whether zero acknowledged-write loss was observed. Its
`recovery_objectives.rto.restore_to_ready_ms` starts immediately before the
restore request and ends after SQL rows, index metadata, and permissions pass
validation. It excludes destination startup and pre-restore object verification.
`restore_api_ms` measures only the request; `total_recovery_ms` additionally
covers shutdown, restart, and repeated validation. The RTO `budget_ms` and
`within_budget` fields remain null until maintainers approve a numeric budget.

The schema-2 report records the source commit and dirty state when Git metadata
is available, build profile, host OS/architecture, and deployment/failure model.
The zero-loss evidence covers this drill's exact fixture after a clean source
shutdown with WAL archiving caught up through the final acknowledged write. It
does not by itself prove crash, distributed, or arbitrary-workload recovery.

The drill has no built-in production scheduler. Run it from your scheduler or CI,
publish the `.prom` file through a Prometheus textfile collector, and retain the
JSON report as evidence. `deploy/docker/alerts.yml` and the optional Helm
`restoreDrillAlerts` rules alert when a successful drill is missing, the latest
drill fails, or the last success is more than 26 hours old. Adjust the Helm
`staleAfterSeconds` value to the deployment's recovery-verification policy.
