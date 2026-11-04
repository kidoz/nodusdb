"""The live SQL controller must terminate its subprocess on a deadline."""
import os
import subprocess
import sys
import time
import unittest

from sql import run_gate


class SqlControllerChecks(unittest.TestCase):
    def test_timeout_kills_and_reaps_the_owned_process(self):
        start = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            run_gate([sys.executable, "-c", "import time; time.sleep(30)"], os.environ, timeout=0.02)
        self.assertLess(time.monotonic() - start, 3)

    def test_child_exit_status_is_preserved(self):
        self.assertEqual(run_gate([sys.executable, "-c", "raise SystemExit(7)"], os.environ), 7)
