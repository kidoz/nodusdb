# Correctness invariants

Properties every change must preserve. They are listed here so a review can cite
them by name; the reasoning behind several of them lives in the
[explanation](../explanation/) pages.

## Transactions and storage

| Invariant | Meaning |
| --- | --- |
| Committed transactions survive | A transaction acknowledged as committed survives crash and restart. |
| Aborted transactions stay invisible | An aborted transaction never becomes visible, at any timestamp, before or after a crash. |
| Versions outlive their readers | MVCC versions are reclaimed only below the garbage-collection watermark. |
| Reads fail rather than lie | An unreadable source — a missing SSTable, an unavailable shard replica — fails the read instead of silently returning a shorter result. |

## Backup and recovery

| Invariant | Meaning |
| --- | --- |
| A `COMPLETE` backup is restorable | Completion is only signalled once every object exists and verifies. |
| A failed backup is never advertised as restorable | Partial uploads and pending manifests are excluded from listings. |

## Replication and sharding

| Invariant | Meaning |
| --- | --- |
| Leaders do not acknowledge unreplicated writes | A Raft leader acknowledges a committed write only after replication requirements are met. |
| Shard operations neither lose nor duplicate keys | Split, merge, and move preserve a contiguous, non-overlapping cover of the key space. |

## Catalog, formats, and upgrades

| Invariant | Meaning |
| --- | --- |
| Catalog changes are transactional and versioned | A DDL change is atomic and carries a version. |
| New binaries read old persistent formats | Format changes add versioned fields and a documented migration path. |
| Mixed-version clusters do not enable irreversible formats early | An irreversible format becomes active only after cluster finalization. |

## Security

| Invariant | Meaning |
| --- | --- |
| Admin and web actions are authorized and audited | Every privileged action passes the authorization path and emits an audit event. |

## Compatibility surfaces

A change touching any of the following is a compatibility surface, and must add
or update version fields and document migration behaviour:

- storage engine on-disk layout;
- write-ahead log records;
- SSTable format;
- catalog records;
- Raft messages and commands;
- backup metadata and manifests;
- network protocol records.
