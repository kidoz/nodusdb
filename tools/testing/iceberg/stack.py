"""Own one disposable Compose project; never attach to an existing warehouse."""

import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ROOT / "deploy/iceberg/compose.yaml"


def command(args, *, env=None, check=True, timeout=120):
    result = subprocess.run(
        [str(arg) for arg in args],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if check and result.returncode:
        # Output can contain temporary storage credentials; retain only the code.
        raise RuntimeError(f"{args[0]} exited with status {result.returncode}")
    return result


def request(url, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        body = response.read()
        return json.loads(body) if body else None


def wait_http(url, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    raise TimeoutError("integration service did not become ready")


class Stack:
    def __init__(self):
        self.project = "nodus-iceberg-" + uuid.uuid4().hex[:12]
        self.password = secrets.token_hex(24)
        # Reserve a candidate port until startup. Compose fails on a bind race;
        # the runner never probes/reuses somebody else's catalog.
        self.reservation = socket.socket()
        self.reservation.bind(("127.0.0.1", 0))
        self.port = self.reservation.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.env = {
            **os.environ,
            "ICEBERG_TEST_PASSWORD": self.password,
            "ICEBERG_TEST_CATALOG_PORT": str(self.port),
            "ICEBERG_TEST_CATALOG_URL": self.url,
        }
        self.base = ["docker", "compose", "--project-name", self.project, "-f", COMPOSE]

    def compose(self, *args, **kwargs):
        return command([*self.base, *args], env=self.env, **kwargs)

    def start(self):
        self.reservation.close()
        print("Starting isolated Lakekeeper/PostgreSQL/MinIO stack", flush=True)
        self.compose(
            "up", "--detach", "--build", "--wait", "--wait-timeout", "120", timeout=900
        )
        published = self.compose("port", "minio", "9000").stdout.strip()
        if not re.fullmatch(r"127\.0\.0\.1:\d+", published):
            raise RuntimeError("MinIO did not publish a loopback-only endpoint")
        self.s3_url = "http://" + published
        wait_http(self.s3_url + "/minio/health/ready")
        request(self.url + "/management/v1/bootstrap", {"accept-terms-of-use": True})

    def create_warehouse(self, s3):
        self.bucket = "nodus-iceberg-test"
        s3.create_bucket(Bucket=self.bucket)
        result = request(
            self.url + "/management/v1/warehouse",
            {
                "warehouse-name": "interop",
                "project-id": "00000000-0000-0000-0000-000000000000",
                "storage-profile": {
                    "type": "s3",
                    "bucket": self.bucket,
                    "key-prefix": "tables",
                    "endpoint": "http://minio:9000",
                    "sts-endpoint": "http://minio:9000",
                    "region": "us-east-1",
                    "path-style-access": True,
                    "flavor": "s3-compat",
                    "sts-enabled": True,
                    "remote-signing-enabled": False,
                },
                "storage-credential": {
                    "type": "s3",
                    "credential-type": "access-key",
                    "access-key-id": "nodus-iceberg-test",
                    "secret-access-key": self.password,
                },
            },
        )
        self.warehouse = result["warehouse-id"]

    def close(self):
        self.reservation.close()
        self.compose("down", "--volumes", "--remove-orphans", timeout=120)
