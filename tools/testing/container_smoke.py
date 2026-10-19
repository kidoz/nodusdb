#!/usr/bin/env python3
import argparse
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

POSTGRES_CLIENT = "postgres:18-bookworm@sha256:afc7e2d441324c0388fa80c3d24f733b4194a4eb7f47dd8ee2b08eb1a24a647c"


def command(args, *, check=True, timeout=30):
    result = subprocess.run(
        args,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command exited with {result.returncode}:\n{result.stdout}{result.stderr}"
        )
    return result


class ContainerSmoke:
    def __init__(self, image, client_image):
        self.image = image
        self.client_image = client_image
        suffix = uuid.uuid4().hex[:12]
        self.name = f"nodus-smoke-{suffix}"
        self.network = f"nodus-smoke-{suffix}"
        self.alias = "nodus-smoke"
        self.base_url = None

    def start(self):
        command(["docker", "network", "create", self.network])
        command(
            [
                "docker",
                "run",
                "--detach",
                "--name",
                self.name,
                "--network",
                self.network,
                "--network-alias",
                self.alias,
                "--publish",
                "127.0.0.1::8088",
                "--env",
                "NODUS_ADMIN__PASSWORD=nodus",
                "--env",
                "NODUS_ADMIN__TOKEN=container-smoke-token",
                self.image,
            ]
        )
        published = command(["docker", "port", self.name, "8088/tcp"]).stdout
        match = re.search(r"127\.0\.0\.1:(\d+)", published)
        if not match:
            raise RuntimeError(f"could not resolve published HTTP port: {published!r}")
        self.base_url = f"http://127.0.0.1:{match.group(1)}"

    def cleanup(self):
        command(["docker", "rm", "--force", self.name], check=False)
        command(["docker", "network", "rm", self.network], check=False)

    def logs(self):
        return command(
            ["docker", "logs", "--tail", "200", self.name], check=False
        ).stdout

    def request(self, path, *, token=None):
        headers = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(self.base_url + path, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read(), response.headers
        except urllib.error.HTTPError as error:
            return error.code, error.read(), error.headers

    def wait_ready(self):
        deadline = time.monotonic() + 90
        last_error = None
        while time.monotonic() < deadline:
            state = command(
                ["docker", "inspect", "--format", "{{.State.Status}}", self.name]
            ).stdout.strip()
            if state != "running":
                raise RuntimeError(f"container entered state {state}")
            try:
                health = self.request("/healthz")[0]
                ready = self.request("/readyz")[0]
                if health == 200 and ready == 200:
                    return
            except (OSError, urllib.error.URLError) as error:
                last_error = error
            time.sleep(0.5)
        raise TimeoutError(f"container did not become ready: {last_error}")

    def check_admin(self):
        unauthenticated = self.request("/api/v1/backups")[0]
        if unauthenticated != 401:
            raise RuntimeError(f"unauthenticated admin returned {unauthenticated}")
        authenticated = self.request(
            "/api/v1/backups", token="container-smoke-token"
        )[0]
        if authenticated != 200:
            raise RuntimeError(f"authenticated admin returned {authenticated}")

    def check_uid(self):
        script = (
            "while read -r key value rest; do "
            "if [ \"$key\" = \"Uid:\" ]; then "
            "[ \"$value\" = \"10001\" ] && exit 0; "
            "echo \"unexpected server uid: $value\" >&2; exit 1; fi; "
            "done < /proc/1/status; exit 1"
        )
        command(["docker", "exec", self.name, "sh", "-c", script])

    def check_sql(self):
        result = command(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                self.network,
                "--env",
                "PGPASSWORD=nodus",
                "--entrypoint",
                "psql",
                self.client_image,
                "--host",
                self.alias,
                "--port",
                "5432",
                "--username",
                "nodus",
                "--dbname",
                "default",
                "--tuples-only",
                "--no-align",
                "--command",
                "SELECT 40 + 2",
            ],
            timeout=90,
        )
        if result.stdout.strip() != "42":
            raise RuntimeError(f"unexpected SQL result: {result.stdout!r}")

    def check_console(self):
        status, index, _ = self.request("/")
        if status != 200 or not index.strip():
            raise RuntimeError(f"console index returned {status} with an empty body")
        html = index.decode("utf-8")
        assets = re.findall(r"(?:src|href)=\"(/assets/[^\"]+\.(?:js|css))\"", html)
        if not any(asset.endswith(".js") for asset in assets):
            raise RuntimeError("console index references no JavaScript asset")
        if not any(asset.endswith(".css") for asset in assets):
            raise RuntimeError("console index references no CSS asset")
        for asset in sorted(set(assets)):
            status, body, headers = self.request(asset)
            if status != 200 or not body or body == index:
                raise RuntimeError(f"console asset {asset} was not served correctly")
            if headers.get_content_type() == "text/html":
                raise RuntimeError(f"console asset {asset} returned the HTML fallback")

    def run(self):
        try:
            self.start()
            self.wait_ready()
            self.check_admin()
            self.check_uid()
            self.check_sql()
            self.check_console()
        except Exception:
            self.failure_logs = self.logs()
            raise
        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(description="Smoke-test the production container")
    parser.add_argument("--image", default="nodusdb:dev")
    parser.add_argument("--client-image", default=POSTGRES_CLIENT)
    args = parser.parse_args()
    smoke = ContainerSmoke(args.image, args.client_image)
    try:
        smoke.run()
    except Exception as error:
        print(f"container smoke failed: {error}", file=sys.stderr)
        logs = getattr(smoke, "failure_logs", "")
        if logs:
            print(logs, file=sys.stderr)
        return 1
    print("container smoke passed: readiness, admin auth, UID 10001, SQL, console assets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
