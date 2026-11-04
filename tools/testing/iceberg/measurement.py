"""Per-process resource measurements and a bounded, transparent S3 delay fixture."""

from contextlib import AbstractContextManager
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit

import pyarrow as pa

POLL_SECONDS = 0.02


def rss_bytes(maxrss, platform=sys.platform):
    if platform == "darwin":
        return int(maxrss)
    if platform == "linux":
        return int(maxrss) * 1024
    raise RuntimeError("resource gate requires Linux or macOS wait4 RSS accounting")


def first_batch_rows(directory, output):
    """Observe a complete IPC batch, never just file growth or the schema header."""
    for path in [output, *Path(directory).glob(".tmp*")]:
        try:
            with path.open("rb") as file:
                batch = next(pa.ipc.open_stream(file))
                if batch.num_rows:
                    return batch.num_rows
        except (OSError, pa.ArrowInvalid, pa.ArrowIOError, EOFError, StopIteration):
            # A private IPC stream can be between writes. Retry on the next poll.
            continue
    return None


def measure(args, env, directory, output, *, interrupt=False, wall_timeout=130):
    """Spawn without inheriting the writer's RSS; reap this exact child via wait4.

    posix_spawn avoids a fork of the Arrow/PyIceberg writer's large address space.
    No Popen.poll/wait or cumulative RUSAGE_CHILDREN counters participate. A first
    batch timestamp is a polled visibility upper bound, including IPC decoding.
    """
    if sys.platform not in ("darwin", "linux"):
        raise RuntimeError("resource gate requires Linux or macOS")
    args = [str(arg) for arg in args]
    pid = None
    reaped = False
    start = time.monotonic()
    first = None
    first_rows = None
    signal_at = None
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        try:
            pid = os.posix_spawn(
                args[0], args, env, setsid=True,
                setsigmask=(), setsigdef=(signal.SIGINT, signal.SIGTERM),
                file_actions=[
                    (os.POSIX_SPAWN_DUP2, stdout.fileno(), 1),
                    (os.POSIX_SPAWN_DUP2, stderr.fileno(), 2),
                ],
            )
            while True:
                child, status, usage = os.wait4(pid, os.WNOHANG)
                if child:
                    reaped = True
                if first is None:
                    rows = first_batch_rows(directory, output)
                    if rows is not None:
                        first = time.monotonic() - start
                        first_rows = rows
                if child:
                    elapsed = time.monotonic() - start
                    break
                if interrupt and first is not None and signal_at is None:
                    signal_at = time.monotonic()
                    os.killpg(pid, signal.SIGINT)
                if time.monotonic() - start > wall_timeout:
                    raise TimeoutError("CLI exceeded the resource gate wall deadline")
                time.sleep(POLL_SECONDS)
            stdout.seek(0)
            stderr.seek(0)
            out = stdout.read(1024 * 1024 + 1)
            err = stderr.read(1024 * 1024 + 1)
            assert len(out) <= 1024 * 1024 and len(err) <= 1024 * 1024, (
                "CLI diagnostics exceeded 1 MiB"
            )
            metrics = {
                "exit_code": os.waitstatus_to_exitcode(status),
                "elapsed_seconds": elapsed,
                "first_batch_seconds": first,
                "first_batch_rows": first_rows,
                "peak_rss_bytes": rss_bytes(usage.ru_maxrss),
                "user_cpu_seconds": usage.ru_utime,
                "system_cpu_seconds": usage.ru_stime,
                "signal_sent": signal_at is not None,
                "signal_exit_seconds": (
                    time.monotonic() - signal_at if signal_at is not None else None
                ),
            }
            return metrics, out.decode(errors="replace"), err.decode(errors="replace")
        finally:
            if pid is not None and not reaped:
                # Own process group only. Always reap it, including on Ctrl-C.
                try:
                    os.killpg(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                os.wait4(pid, 0)


class DelayedS3(AbstractContextManager):
    """Forward signed GET/HEAD requests unchanged; delay Parquet GETs only.

    Host and Authorization are preserved for end-to-end MinIO signature checks.
    Bodies are streamed in 64 KiB chunks. No request URL, header or body is logged.
    """

    def __init__(self, upstream, delay=0.1):
        parsed = urlsplit(upstream)
        assert parsed.scheme == "http" and parsed.hostname == "127.0.0.1"
        self.upstream = (parsed.hostname, parsed.port)
        self.delay = delay
        self.data_gets = 0
        self.lock = threading.Lock()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_HEAD(self):
                self.forward()

            def do_GET(self):
                self.forward()

            def forward(self):
                self.connection.settimeout(5)
                data = self.command == "GET" and urlsplit(self.path).path.endswith(".parquet")
                if data:
                    with fixture.lock:
                        fixture.data_gets += 1
                    time.sleep(fixture.delay)
                connection = http.client.HTTPConnection(*fixture.upstream, timeout=5)
                hop = {"connection", "proxy-connection", "transfer-encoding"}
                try:
                    connection.request(
                        self.command, self.path,
                        headers={k: v for k, v in self.headers.items() if k.lower() not in hop},
                    )
                    response = connection.getresponse()
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in hop:
                            self.send_header(key, value)
                    self.end_headers()
                    if self.command != "HEAD":
                        while chunk := response.read(64 * 1024):
                            self.wfile.write(chunk)
                except (OSError, http.client.HTTPException):
                    # Cancellation can close the client while storage is replying.
                    self.close_connection = True
                finally:
                    connection.close()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05}
        )

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=6)
        assert not self.thread.is_alive(), "S3 pacing fixture did not stop"
