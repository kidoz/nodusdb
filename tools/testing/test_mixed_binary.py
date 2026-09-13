"""Source-pin regression checks: python3 -B -m unittest discover -s tools/testing."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import mixed_binary as runner


class BackportSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="nodus-source-test-")
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        self.git("init", "-q")
        (self.repo / "source").write_text("before\n")
        self.git("add", "source")
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                 "commit", "--no-gpg-sign", "-qm", "Create fixture")
        self.base = self.git("rev-parse", "HEAD").decode().strip()
        (self.repo / "source").write_text("after\n")
        self.git("add", "source")
        self.tree = self.git("write-tree").decode().strip()
        self.patch = self.repo / "backport.patch"
        self.patch.write_bytes(self.git("diff", "--cached", "--binary"))
        self.patch_sha = runner.sha(self.patch)
        self.git("read-tree", self.base)

    def git(self, *args):
        # Fixture plumbing must not inherit an index override from its caller.
        env = dict(os.environ)
        env.pop("GIT_INDEX_FILE", None)
        return subprocess.check_output(["git", *args], cwd=self.repo, env=env,
                                       stderr=subprocess.PIPE)

    def test_reconstruction_preserves_worktree_and_staged_changes(self):
        (self.repo / "source").write_text("user's unstaged edit\n")
        (self.repo / "staged").write_text("user's staged file\n")
        self.git("add", "staged")
        index_before = (self.repo / ".git/index").read_bytes()
        tree = runner.patched_tree(self.base, self.patch, self.patch_sha,
                                   self.tree, repo=self.repo)
        self.assertEqual(tree, self.tree)
        self.assertEqual(self.git("show", f"{tree}:source"), b"after\n")
        self.assertEqual((self.repo / ".git/index").read_bytes(), index_before)
        self.assertEqual((self.repo / "source").read_text(), "user's unstaged edit\n")

    def test_changed_patch_or_tree_pin_is_rejected(self):
        with self.assertRaisesRegex(SystemExit, "source tree changed"):
            runner.patched_tree(self.base, self.patch, self.patch_sha,
                                "0" * 40, repo=self.repo)
        self.patch.write_bytes(self.patch.read_bytes() + b"\n")
        with self.assertRaisesRegex(SystemExit, "patch hash changed"):
            runner.patched_tree(self.base, self.patch, self.patch_sha,
                                self.tree, repo=self.repo)

    def test_real_backport_reconstructs_without_maintenance_commit(self):
        cold = self.repo / "cold.git"
        self.git("init", "--bare", "-q", str(cold))
        subprocess.run(["git", "-C", str(cold), "fetch", "--quiet", "--no-tags",
                        "--depth=1", str(runner.ROOT), runner.OLD_BASE], check=True)
        missing = subprocess.run(["git", "-C", str(cold), "cat-file", "-e", runner.OLD],
                                 capture_output=True)
        self.assertNotEqual(missing.returncode, 0)
        tree = runner.patched_tree(runner.OLD_BASE, runner.OLD_PATCH,
                                   runner.OLD_PATCH_SHA, runner.OLD_TREE, repo=cold)
        # Backporting fixes must not turn the historical command decoder into
        # an admission-capable reader. The process gate checks this over the wire.
        for path in ("crates/nodus_raftstore", "crates/nodus_server/src/raft_upgrade.rs"):
            diff = subprocess.check_output(["git", "-C", str(cold), "diff",
                                            runner.OLD_BASE, tree, "--", path])
            self.assertEqual(diff, b"")


if __name__ == "__main__":
    unittest.main()
