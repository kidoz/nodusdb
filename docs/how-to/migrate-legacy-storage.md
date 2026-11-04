# Migrate a historical pre-manifest store

Use this procedure only for a verified historical store whose SSTable/WAL
formats this binary can read, with SSTables numbered through N and the active
WAL numbered N+1. It replaces automatic pre-manifest discovery. A modern store
with a lost manifest needs its matching manifest or a consistent backup restored.

1. Stop the node and exclude every writer to the data directory. Back up the
   entire directory before proceeding. This offline command uses filesystem
   permissions and does not acquire an inter-process lock or use an admin token.
2. Establish the original SSTable inventory and active WAL from the deployment's
   history. Do not infer the authoritative set from crash residue or orphan
   files. Every final SSTable must be listed. The active WAL must already exist;
   no WAL may have a higher id. Modern WAL segment headers and batched commits
   are rejected. These checks cannot distinguish every unrotated modern store
   from a historical one, so provenance remains necessary.
3. Run migration with the explicit ids. For a verified store containing `1.sst`
   and active `2.log`, use:

   ```sh
   nodusctl storage migrate-legacy \
     --data-dir /var/lib/nodus-legacy \
     --sstable 1 \
     --active-wal 2
   ```

   Repeat `--sstable` for each final SSTable. For a historical WAL-only store,
   omit `--sstable` and specify `--active-wal 1`. The WAL must still exist even
   if it is empty. From a source checkout, replace `nodusctl` with
   `cargo run --locked -p nodus_cli --`.
4. For encrypted WALs, append `--wal-key-file /secure/nodus-wal-key`. The protected
   file must contain the original 64 hexadecimal key characters, optionally
   followed by a newline, matching the server's `storage.encryption_key`.
   The key is neither accepted as a command-line value nor printed in output.
5. Check the command's exit status and JSON report. Success reports
   `"status": "migrated"` and the manifest path and selected ids. Validation
   scans every final SSTable and numeric WAL without changing their bytes.
   Torn tails, CRC failures, wrong keys, unreadable files, symlinked data files,
   inconsistent inventories, or existing manifest publication artifacts cause
   failure before publication. Partial `*.sst.tmp` files are left untouched and
   excluded from the selected set.
6. Restart normally against the same directory with the original encryption key.
   Verify the expected rows and catalog before returning the node to service.

Migration publishes the existing manifest format, with replay floor zero, using
a synced `LEGACY_MANIFEST.tmp` file and an atomic hard link that cannot replace
an existing `MANIFEST`. The filesystem must support hard links and directory
sync. No SSTable or WAL is rewritten, and no new durable record version is
introduced.

If interrupted or an I/O error occurs during publication, preserve the directory
and inspect it offline. A complete `MANIFEST` may already have been published;
do not rerun migration over it. A staging file alone is not authoritative, and
normal startup will refuse to initialize that directory. Preserve the staging
file with the backup before resolving residue and repeating validation on a
verified legacy copy. Never delete a modern manifest or manufacture an empty WAL
to force this migration path. See the [manifest recovery contract](../reference/durability-contract.md#manifest-recovery).
