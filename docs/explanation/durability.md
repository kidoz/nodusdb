# How durability works

NodusDB promises that an acknowledged commit survives a crash. This page
explains the machinery that makes that promise keepable, and why it is built
this way. For the promise itself — stated precisely, with the tests that check
it — see the [durability contract](../reference/durability-contract.md).

The shape of the problem is old and well understood: a storage engine must be
able to be killed at any instruction and still come back knowing exactly which
writes it had already claimed to have. Every mechanism below exists to make one
more class of "killed at the wrong moment" harmless.

## The write-ahead log

Each key-value mutation is appended to the active WAL segment, and `COMMIT` or
`ABORT` `fsync`s the segment before returning to the client. That ordering is
the whole guarantee: the acknowledgement is never allowed to overtake the disk.

Records are framed as `[len][crc32][payload]`. The CRC is what makes a crash
mid-append survivable. On recovery, a torn or corrupt trailing record is
detected and the log is truncated cleanly at that point, rather than treated as
an error. The alternative — refusing to start because the last record is
incomplete — would turn every unlucky crash into an outage, while gaining no
safety: a record that was never fully written was never acknowledged either.

## SSTables and atomic publication

A flush serialises the committed portion of the memtable into an SSTable:
key-sorted data blocks, a sparse block index, a bloom filter, and a footer.

Publication is atomic by construction. The file is written to `*.sst.tmp`,
`fsync`ed, renamed into place, and the containing directory is `fsync`ed. A
crash mid-build therefore leaves only a `*.tmp` file, which recovery ignores.
There is no window in which a half-written SSTable can be mistaken for a
complete one, because the rename is the only thing that makes it visible.

Reads treat an unreadable SSTable as a failure, not as an empty one. A missing
file, a truncated footer, or a decoding error fails the read rather than
returning a shorter result — because the alternative is silently hiding
committed rows, which is indistinguishable, from the client's perspective, from
data loss.

## Flush is intent-safe

A flush writes only fully committed keys and retains keys carrying uncommitted
intents in the memtable.

This matters more than it first appears. If a flush could move an intent into an
immutable SSTable, the later commit of that transaction would have to find and
rewrite it there. Keeping intents in the memtable until they resolve means a
flush can never strand one, and the commit path never has to reach into
published files.

## The manifest is the commit point

A `MANIFEST` file records the authoritative file set — the live SSTable ids and
the active WAL segment — and is written atomically. A new SSTable becomes live
only when the manifest names it.

That single indirection is what makes flush and compaction crash-safe:

- A crash *before* the manifest swap orphans the new file. Recovery ignores it.
- A crash *after* the swap orphans the old files. Recovery ignores those too.

There is no in-between state, because the swap is atomic. If the manifest itself
is missing or corrupt, recovery falls back to scanning the SSTable files on
disk — a slower path that trades the manifest's precision for the ability to
start at all.

## Compaction

Once enough SSTables accumulate they are merged into one: each key's version
chain is combined, and versions reclaimable below the garbage-collection
watermark are dropped. The result is published durably and committed with the
same manifest swap, after which the old files are deleted.

This bounds read amplification and reclaims space without inventing a second
notion of "committed" — compaction uses exactly the mechanism a flush uses.

## The catalog lives in the store

Schema and role state is persisted through the same LSM store as user data,
under a reserved key, rather than in a separate file.

The temptation to give the catalog its own file is strong and worth resisting: a
second durable mechanism means a second recovery path, a second crash matrix,
and the possibility of the two disagreeing about what happened. Putting the
catalog in the store means DDL is durable as soon as it is committed, by the
same rules as everything else.

The cost is that each DDL writes a full-state blob rather than an incremental
entry. That is a real limitation, and an acceptable one at current catalog
sizes.

## Recovery

On startup the engine reopens the SSTables named by the manifest — or found by a
directory scan, if the manifest is unusable — and replays the active WAL segment
into the memtable. The catalog then loads its state from the store, like any
other reader.

Older WAL segments are deliberately retained on disk for the backup WAL archiver
and point-in-time recovery. Reclaiming them is not yet automatic, which is the
price of not deleting something a restore might still need.

## What this does not cover

This is local durability: one node, one disk. It says nothing about surviving
the loss of that node. Cross-node durability is a property of the Raft
replication layer, which is a separate mechanism with a separate failure model —
and one that assumes the local guarantees described here.
