# Back up and restore

This guide creates a backup of a running server, verifies it, and restores it —
including restoring to a point in time.

Backups go to the repository named by `[backup] repository_uri` in the server
configuration. The example configuration uses a local directory
(`file://./.local/nodus/backups`); production deployments use an S3-compatible
repository.

Every route below requires the `ManageBackups` privilege. See
[admin API authorization](../reference/admin-api-authorization.md) for the auth
schemes; the examples use a bearer token.

## Create a backup

```bash
curl -X POST -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups
```

```json
{"backup_id":"88767e7d-4394-4bb9-b3c1-3cda4fec6af3","files":3,"status":"Completed"}
```

Only `"status":"Completed"` means the backup is restorable. A failed or
interrupted backup is never advertised as complete — that is a contract, not an
implementation detail, and the reasoning is in
[backup contracts](../reference/backup-contracts.md).

Record the `backup_id`; every other operation takes it.

## List backups

```bash
curl -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups
```

```json
["88767e7d-4394-4bb9-b3c1-3cda4fec6af3"]
```

## Verify a backup

Verification checks that the manifest exists, every file it names exists, and
every checksum matches. Do this before you need the backup, not after:

```bash
curl -X POST -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups/<backup_id>/verify
```

Object verification is the cheapest of three levels; the other two — restoring
into a scratch directory, and querying the restored database — are described in
[backup architecture](../explanation/backup-architecture.md).

## Restore a backup

```bash
curl -X POST -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups/<backup_id>/restore
```

The response reports how many objects were applied:

```json
{"restored": 42}
```

Two behaviours are worth knowing before you run this:

- **Restores are serialized.** A second concurrent restore is refused with
  `{"restored":0,"error":"a restore is already in progress"}` rather than
  interleaving with the first.
- **Validation happens before mutation.** Every backup object is parsed first,
  so a malformed backup is rejected having changed nothing.

## Restore to a point in time

Add `target_ts` to replay archived write-ahead log records up to a timestamp:

```bash
curl -X POST -H "Authorization: Bearer nodus-dev-token" \
     "http://127.0.0.1:8088/api/v1/backups/<backup_id>/restore?target_ts=1750000000000"
```

The server plans the restore itself: it selects a base backup whose snapshot is
at or before `target_ts`, loads the archived WAL segments that cover the gap, and
stops replay at the target. If no base backup or segment chain can reach that
timestamp, the response carries an `error` and nothing is changed.

Point-in-time recovery only works if the WAL archiver has been running and its
segments are still in the repository.

## Delete a backup

```bash
curl -X DELETE -H "Authorization: Bearer nodus-dev-token" \
     http://127.0.0.1:8088/api/v1/backups/<backup_id>
```

Deletion is manual today. Retention is not yet chain-aware, so check that no
restore point you care about depends on the backup before removing it.

## The CLI equivalents

`nodus_cli backup create|list|verify|restore` wrap these routes, but the CLI
sends no `Authorization` header and fails with `401 Unauthorized` against a
server with admin auth configured. Use `curl` until that gap is closed.
