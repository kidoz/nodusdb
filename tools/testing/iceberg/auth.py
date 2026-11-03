"""Live OIDC and OpenFGA checks, using tokens issued by the pinned Keycloak."""

import base64
import json
import re
import secrets
import time
import urllib.error
import urllib.request

INHERIT_TOKEN = object()
JWT = re.compile(r"\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")
AUTH_CHECKS = [
    "invalid_catalog_tokens",
    "catalog_permissions",
    "expired_catalog_token",
    "credential_redaction",
]


def assert_redacted(text, credentials):
    assert all(not secret or secret not in text for secret in credentials), (
        "output disclosed a credential"
    )
    assert JWT.search(text) is None, "output disclosed a JWT"


def safe_detail(error, credentials):
    # Never serialize arbitrary upstream exception text. Even assertions must
    # pass the same disclosure checks as CLI output and service logs.
    if not isinstance(error, AssertionError):
        return None
    detail = str(error)
    try:
        assert_redacted(detail, credentials)
    except AssertionError:
        return "failure detail redacted"
    return detail


def token_claims(token):
    # Decoding is only used for fixture subjects and expiry timing. Lakekeeper
    # independently verifies the signature, issuer, audience and expiry.
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


class AuthChecks:
    def catalog_response(self, path, token):
        headers = {"X-Iceberg-Access-Delegation": "vended-credentials"}
        if token is not None:
            headers["Authorization"] = "Bearer " + token
        req = urllib.request.Request(
            self.stack.url + "/catalog/v1/" + path, headers=headers
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            status = error.code
            error.close()
            return status, None

    def require_status(self, path, token, expected):
        status, body = self.catalog_response(path, token)
        assert status in expected, f"unexpected catalog HTTP status: {status}"
        return body

    def table_path(self, name):
        return f"{self.stack.warehouse}/namespaces/interop/tables/{name}"

    def register(self, client, token):
        user = "oidc~" + token_claims(token)["sub"]
        self.stack.management(
            "user",
            {
                "id": user, "name": client, "user-type": "application",
                "update-if-exists": True,
            },
        )
        return user

    def assignments(self, target, user, *, revoke=False):
        self.stack.management(
            "permissions/" + target + "/assignments",
            {
                "writes": [] if revoke else [{"user": user, "type": "select"}],
                "deletes": [{"user": user, "type": "select"}] if revoke else [],
            },
        )

    def invalid_catalog_tokens(self):
        signature = self.stack.admin_token.split(".")[2]
        altered = ("A" if signature[0] != "A" else "B") + signature[1:]
        tampered = self.stack.admin_token.rsplit(".", 1)[0] + "." + altered
        cases = {
            "missing": None,
            "malformed": "invalid-catalog-token-" + secrets.token_hex(12),
            "bad-signature": tampered,
            "wrong-audience": self.stack.issue_token("wrong-audience"),
        }
        self.stack.secrets.extend(token for token in cases.values() if token)
        for name, token in cases.items():
            self.require_status("config?warehouse=interop", token, {401})
            inspected = self.invoke("inspect", token=token)
            assert (
                inspected.returncode != 0
                and "catalog configuration" in inspected.stderr
            ), "invalid token did not fail catalog inspection"
            self.scan("auth-" + name, token=token, error="catalog configuration")

    def catalog_permissions(self):
        denied = self.create("restricted")
        denied.append(self.original)
        self.require_status(
            self.table_path("restricted"), self.stack.admin_token, {200}
        )
        token = self.stack.issue_token("reader")
        user = self.register("reader", token)
        target = (
            f"warehouse/{self.stack.warehouse}/table/{self.table.metadata.table_uuid}"
        )
        self.assignments(target, user)
        # A table grant permits discovery through warehouse visibility without
        # granting access to every table in that warehouse.
        self.require_status("config?warehouse=interop", token, {200})
        self.require_status(self.table_path("events"), token, {200})
        result = self.invoke("inspect", token=token)
        assert result.returncode == 0, "permitted reader could not inspect its table"
        actual, _ = self.scan("auth-permitted", token=token)
        self.check_table(actual, self.table.scan().to_arrow())
        # Some catalogs hide inaccessible resources with 404. A 5xx or transport
        # failure is never accepted as evidence of permission enforcement.
        self.require_status(self.table_path("restricted"), token, {403, 404})
        result = self.invoke("inspect", "restricted", token=token)
        assert result.returncode != 0 and "load table" in result.stderr, (
            "reader inspected a table without a grant"
        )
        self.scan("auth-restricted", "restricted", token=token, error="load table")
        self.assignments(target, user, revoke=True)
        self.require_status("config?warehouse=interop", token, {403, 404})
        self.scan("auth-revoked", token=token, error="catalog configuration")

    def expired_catalog_token(self):
        token = self.stack.issue_token("short-lived")
        user = self.register("short-lived", token)
        self.assignments("warehouse/" + self.stack.warehouse, user)
        claims = token_claims(token)
        assert 0 < claims["exp"] - claims["iat"] <= 15, (
            "fixture token is not short lived"
        )
        self.require_status("config?warehouse=interop", token, {200})
        assert self.invoke("inspect", token=token).returncode == 0, (
            "short-lived token was not accepted before expiry"
        )
        # Bound the wait, including the validator's clock-skew allowance. The
        # very same signed token must transition from acceptance to HTTP 401.
        deadline = time.monotonic() + 100
        while True:
            status, _ = self.catalog_response("config?warehouse=interop", token)
            if status == 401:
                break
            assert status == 200, "expiry probe failed for a reason other than expiry"
            assert time.monotonic() < deadline, (
                "expired catalog token was still accepted"
            )
            time.sleep(0.5)
        assert time.time() >= claims["exp"], "token rejected before its expiration"
        result = self.invoke("inspect", token=token)
        assert result.returncode != 0 and "catalog configuration" in result.stderr
        self.scan("auth-expired", token=token, error="catalog configuration")
        fresh = self.stack.issue_token("short-lived")
        assert self.invoke("inspect", token=fresh).returncode == 0, (
            "fresh token did not restore access after expiry"
        )

    def credential_redaction(self):
        logs = self.stack.compose("logs", "--no-color")
        assert_redacted(
            logs.stdout + logs.stderr, self.stack.secrets + self.credentials
        )
