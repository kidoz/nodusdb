# R6a maintenance source

`r6a-maintenance.patch` is the source diff from R6a
`31897cd7d7bbbf7b51cc3d522a2a77f4e7ec908a` to the maintenance tree based on
`65313a236ce54adb510dde1835e4a652ece3a0e8` on the local
`compat/r6a-maintenance` branch. It contains these backports:

| Original commit | Backport | Change |
| --- | --- | --- |
| `225a5dcc68629bba44ac942303ed19c5397f0fda` | `203909f` | Bootstrap authentication after catalog replacement |
| `f82094c740e9cfa520310cf823cd2dc8787f62a9` | `65313a2` | Archived WAL retention below the durable replay floor |
| `16442b1` | tree-only patch | Bounded atomic Raft log purge and recovery |
| — | tree-only patch | Drop a snapshot descriptor older than the reconciled purge watermark so a torn restart rebuilds the snapshot instead of panicking on `purge_upto > snapshot_last_log_id` |

The backports change no upgrade authority or admission protocol code. They include
the original regression tests and documentation. This is a patched maintenance
baseline, not the original historical executable or a published release.

The runner pins the patch SHA-256 (`33f63b8799a704a93b914987bbb22428fd004726d69bb54805bb12727880b38b`)
and resulting Git tree `879a5fd893f41dd59d735cb086ba4600076e92ef`.
It reconstructs that tree using a temporary Git index and archives the verified
tree. A fresh main-branch checkout therefore does not need the maintenance branch
or commit object. The source-pin tests exercise exactly that case, reject changed
pins, and verify that the user's index and working files are preserved.

To regenerate the patch after an intentional maintenance update, reconstruct the
reviewed maintenance tree and produce the diff without changing branch history:

```bash
git diff --binary --full-index 31897cd7d7bbbf7b51cc3d522a2a77f4e7ec908a <maintenance-tree> --output=tools/testing/fixtures/r6a-maintenance.patch
```

Review the backport, update its revision/tree/patch pins together, and run
`just test-mixed-binary-tools` followed by `just test-mixed-binary`. Changing only
the hashes is not compatibility evidence; the actual old reader must still reject
admission commands and the matrix must pass without diagnostic restarts.
