# Recover from an unreadable backup manifest

A backup manifest (`<backup_id>/manifest.json` in the repository) is
unreadable when its bytes no longer decode, or when it decodes but references
objects that are missing from the repository. While any manifest is
unreadable, the server fails closed: it treats the manifest as protective,
because it may still anchor a restore point. This guide diagnoses the state
and restores normal operation.

## Recognise the condition

Any of these, alone or together:

- Server logs warn, naming the offending id:

  ```text
  WARN unreadable backup manifest; treating as protective backup_id=<id> error=<cause>
  WARN backup manifest references missing objects; treating as protective backup_id=<id>
  ```

- `GET /api/v1/backups` (`nodusctl backup list`) omits a backup whose objects
  are present in the repository. Only decodable `Completed` manifests with all
  referenced objects present are listed.
- Deleting **any** backup is refused, naming the unreadable ids:

  ```json
  {"deleted": false, "error": "cannot delete backup <id>; 1 unreadable manifest(s) may still depend on it: <unreadable_id>"}
  ```

- Verifying the affected backup fails:
  `{"verified": false, "error": ...}`.
- Housekeeping stalls. Archived WAL segments stop being reclaimed and the
  MVCC garbage collector stops advancing, so the repository and the engine
  grow until the condition is resolved. Logs show the cleanup passes being
  denied.

A manifest left behind as `manifest.pending.json`, or a final manifest whose
status is not `Completed`, is not protective and does not cause this
condition.

## Diagnose

1. Collect the unreadable ids from the delete error above or the log
   warnings. If neither is at hand, list the repository and compare against
   `nodusctl backup list`; every `<id>/manifest.json` object that is missing
   from the list is a candidate. Filesystem repository:

   ```bash
   ls /var/lib/nodus/backups
   ```

   S3-compatible repository (illustrative; adapt to your tooling):

   ```bash
   aws s3api list-objects-v2 --bucket <bucket> --prefix <prefix>/ \
       --query 'Contents[?ends_with(Key, `manifest.json`)].Key'
   ```

2. Read the raw manifest to classify the failure:

   ```bash
   # filesystem
   cat /var/lib/nodus/backups/<backup_id>/manifest.json

   # S3-compatible (illustrative)
   aws s3 cp "s3://<bucket>/<prefix>/<backup_id>/manifest.json" -
   ```

   - **Does not decode** (not JSON, or schema rejected): the bytes are
     corrupt or truncated.
   - **Decodes**: the manifest references objects that are missing. Its
     `files` list names them; check which are absent.

3. Check whether other backups depend on the unreadable one. Every decodable
   manifest carries `parent_backup_id`; an incremental whose parent is
   unreadable can no longer be restored through that chain. The unreadable
   manifest itself cannot be read, so check each listed backup's manifest:

   ```bash
   grep -l "\"parent_backup_id\": \"<unreadable_id>\"" \
       /var/lib/nodus/backups/*/manifest.json
   ```

The corrupt backup is already unusable as a restore base: restore planning
considers only decodable `Completed` manifests, so no restore or PITR
operation will select it.

## Repair, or accept the loss and remove

**Repair first when the repository is versioned.** If the object store keeps
prior versions (the recommended configuration), restore the previous version
of the corrupt `manifest.json` or of the missing objects it references, then
verify:

```bash
nodusctl backup verify <backup_id>
```

Do not edit or rewrite a manifest by hand. Verification checks recorded
SHA-256 checksums, so a manifest whose contents were reconstructed manually
still fails verification, and a fabricated manifest can trick retention into
reclaiming data a real restore point needs.

**Remove the prefix when the manifest is unrecoverable.** There is no server
command for this: the delete route refuses while any manifest is unreadable,
by design. Remove the entire backup prefix with object-store tooling. This
permanently destroys whatever restore point the manifest anchored, including
any incremental chains that name it as a parent — take a fresh full backup
first if the remaining chain leaves a gap in your recovery objectives.

```bash
# filesystem
rm -rf /var/lib/nodus/backups/<backup_id>

# S3-compatible (illustrative)
aws s3 rm --recursive "s3://<bucket>/<prefix>/<backup_id>/"
```

Also remove any dependent incrementals identified above; their chains are
broken and they cannot serve a restore.

## Confirm recovery

Once the unreadable objects are gone:

- `nodusctl backup list` reflects the true repository contents again.
- `DELETE /api/v1/backups/<id>` succeeds for backups that are not parents of
  a retained incremental.
- The log warnings stop; WAL segment cleanup and MVCC garbage collection
  resume on their next passes. No restart is needed.

## Prevent recurrence

- Use a versioned, off-cluster S3-compatible repository with immutable
  retention, as recommended in [Back up and restore](back-up-and-restore.md).
- Alert on the `treating as protective` log patterns and on repository growth
  while cleanup is denied; an unreadable manifest is silent in the API until
  someone lists or deletes backups.
- Take a new full backup after resolving the condition so the retained chain
  does not depend on repaired objects.
