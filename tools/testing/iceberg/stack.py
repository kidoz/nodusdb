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
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ROOT / "deploy/iceberg/compose.yaml"
AUTH_COMPOSE = ROOT / "deploy/iceberg/compose.auth.yaml"
REALM = ROOT / "deploy/iceberg/interop-realm.json"


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


def request(url, body=None, *, token=None):
    data = None if body is None else json.dumps(body).encode()
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, headers=headers)
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
    def __init__(self, authenticated=False):
        self.authenticated = authenticated
        self.project = "nodus-iceberg-" + uuid.uuid4().hex[:12]
        self.password = secrets.token_hex(24)
        self.secrets = [self.password]
        self.admin_token = None
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
        if authenticated:
            self.base.extend(["-f", AUTH_COMPOSE])

    def compose(self, *args, **kwargs):
        return command([*self.base, *args], env=self.env, **kwargs)

    def start(self):
        self.reservation.close()
        print("Starting isolated Iceberg integration stack", flush=True)
        self.compose(
            "up", "--detach", "--build", "--wait", "--wait-timeout", "240", timeout=900
        )
        self.s3_url = self.service_url("minio", "9000")
        wait_http(self.s3_url + "/minio/health/ready")
        if self.authenticated:
            self.oidc_url = self.service_url("keycloak", "8080") + "/realms/interop"
            self.admin_token = self.issue_token("owner")
        request(
            self.url + "/management/v1/bootstrap",
            {"accept-terms-of-use": True},
            token=self.admin_token,
        )

    def service_url(self, service, port):
        published = self.compose("port", service, port).stdout.strip()
        if not re.fullmatch(r"127\.0\.0\.1:\d+", published):
            raise RuntimeError(
                "integration service did not publish a loopback-only endpoint"
            )
        return "http://" + published

    def issue_token(self, client):
        body = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": client,
                "client_secret": self.password,
            }
        ).encode()
        req = urllib.request.Request(
            self.oidc_url + "/protocol/openid-connect/token",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            token = json.loads(response.read())["access_token"]
        self.secrets.append(token)
        return token

    def management(self, path, body=None):
        return request(
            self.url + "/management/v1/" + path, body, token=self.admin_token
        )

    def create_warehouse(self, s3):
        self.bucket = "nodus-iceberg-test"
        s3.create_bucket(Bucket=self.bucket)
        result = self.management(
            "warehouse",
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
