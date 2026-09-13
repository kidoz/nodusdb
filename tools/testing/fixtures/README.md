# R6a maintenance source

`r6a-maintenance.patch` is the source diff from R6a
`31897cd7d7bbbf7b51cc3d522a2a77f4e7ec908a` to maintenance commit
`65313a236ce54adb510dde1835e4a652ece3a0e8` on the local
`compat/r6a-maintenance` branch. It contains these backports:

| Original commit | Backport | Change |
| --- | --- | --- |
| `225a5dcc68629bba44ac942303ed19c5397f0fda` | `203909f` | Bootstrap authentication after catalog replacement |
| `f82094c740e9cfa520310cf823cd2dc8787f62a9` | `65313a2` | Archived WAL retention below the durable replay floor |

The backport changes no Raft commands or admission protocol code. It includes the
original regression tests and documentation. It is a patched maintenance baseline,
not the original historical executable or a published release.

The runner pins the patch SHA-256 and resulting Git tree
`9dd2beafcee4ae17bde6be0acc74d70d249d4d6d`. It reconstructs that tree using a
temporary Git index and archives the verified tree. A fresh main-branch checkout
therefore does not need the maintenance branch or commit object. The source-pin
tests exercise exactly that case, reject changed pins, and verify that the user's
index and working files are preserved.

To regenerate the patch after an intentional maintenance update:

```bash
git diff --binary --full-index 31897cd7d7bbbf7b51cc3d522a2a77f4e7ec908a compat/r6a-maintenance --output=tools/testing/fixtures/r6a-maintenance.patch
```

Review the backport, update its revision/tree/patch pins together, and run
`just test-mixed-binary-tools` followed by `just test-mixed-binary`. Changing only
the hashes is not compatibility evidence; the actual old reader must still reject
admission commands and the matrix must pass without diagnostic restarts.
