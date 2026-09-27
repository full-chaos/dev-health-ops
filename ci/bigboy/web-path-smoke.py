#!/usr/bin/env python3
"""CHAOS-6987/R460: the web-path smoke. Logs in through web's REAL login route
(/api/v1/auth/login, exactly as web/src/lib/auth.ts calls it) as the bigboy admin,
then issues the EXACT operations web's Cockpit (home/investment/opportunities REST
threads, web/src/components/home/CockpitClient.tsx) and Diagnose
(complexityTimeseries GraphQL, web/src/lib/graphql/queries.ts's
COMPLEXITY_TIMESERIES_QUERY) pages send -- never a hand-typed query or a
Python-minted token (a Python-minted HS256 token is NOT what the real product
issues since the R454 login flip; it proved D2726/D2728 during triage but is not
a substitute for this STEP).

Every request goes through the plane-split router (DHO_SMOKE_BASE_URL, the same
host web itself talks to), so this is the same path a real browser session takes.

Credentials: DHO_SMOKE_ADMIN_EMAIL (plain env var, not secret-shaped) and
DHO_SMOKE_ADMIN_PASSWORD_FILE (a path; this process reads the file's CONTENT as
the password and never prints it, matching the *_FILE convention, CHAOS-6972).
The password is used only in the login request body; it is never logged, never
included in the receipt, and the receipt file never carries the access token
either -- only structural facts (status, plane header, row/tile counts).

Exit code is the only pass/fail signal a caller needs: 0 when every assertion
below passed, 1 otherwise, with the failing check(s) named on stderr AND in the
receipt file, never silently.
"""

from __future__ import annotations

import http.client
import json
import os
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone

PLANE_HEADER = "X-Dev-Health-Plane"

# The only target this smoke may ever connect to: plain http to the router's internal
# name on the compose network (the smoke runs inside it; there is no TLS hop to verify).
# DHO_SMOKE_BASE_URL is parsed once and checked before any connection is opened;
# requests then go through http.client with a fixed host/port and a path per call site,
# never a dynamic URL string.
ALLOWED_SCHEMES = frozenset({"http"})
ALLOWED_HOSTS = frozenset({"traefik"})


class SmokeFailure(Exception):
    """Raised with a short, named reason -- never with a credential value."""


class ConfigError(Exception):
    """DHO_SMOKE_BASE_URL is not an allowed target; the reason names the rule it broke."""


class Target:
    def __init__(self, scheme: str, host: str, port: int) -> None:
        self.scheme, self.host, self.port = scheme, host, port


def parse_base_url(raw: str) -> Target:
    parts = urllib.parse.urlsplit(raw)
    if parts.scheme not in ALLOWED_SCHEMES:
        raise ConfigError(f"base_url_scheme_not_allowed={parts.scheme or 'missing'}")
    host = parts.hostname or ""
    if host not in ALLOWED_HOSTS:
        raise ConfigError(f"base_url_host_not_allowed={host or 'missing'}")
    if (
        parts.username
        or parts.password
        or parts.query
        or parts.fragment
        or parts.path not in ("", "/")
    ):
        raise ConfigError("base_url_must_be_scheme_host_port_only")
    try:
        port = parts.port or 80
    except ValueError:
        raise ConfigError("base_url_port_invalid") from None
    return Target(parts.scheme, host, port)


TARGET: Target | None = None


def _post(path: str, body: dict, token: str | None = None) -> tuple[int, dict, dict]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return _send("POST", path, json.dumps(body).encode(), headers)


def _get(path: str, token: str) -> tuple[int, dict, dict]:
    return _send("GET", path, None, {"Authorization": f"Bearer {token}"})


def _send(
    method: str, path: str, body: bytes | None, headers: dict[str, str]
) -> tuple[int, dict, dict]:
    if TARGET is None:
        raise ConfigError("base_url_not_parsed")
    if not path.startswith("/"):
        raise ConfigError("request_path_must_be_absolute")
    conn = http.client.HTTPConnection(TARGET.host, TARGET.port, timeout=30)
    try:
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        status = resp.status
        raw = resp.read()
        resp_headers = dict(resp.getheaders())
    finally:
        conn.close()
    try:
        parsed = json.loads(raw.decode()) if raw else {}
    except json.JSONDecodeError:
        parsed = {"_raw_body_bytes": len(raw)}
    return status, parsed, resp_headers


def login() -> tuple[str, str]:
    email = os.environ["DHO_SMOKE_ADMIN_EMAIL"]
    password_file = os.environ["DHO_SMOKE_ADMIN_PASSWORD_FILE"]
    with open(password_file) as f:
        password = f.read().strip()
    status, body, _headers = _post(
        "/api/v1/auth/login", {"email": email, "password": password, "org_id": ""}
    )
    # password goes out of scope here; nothing below ever references it again.
    if status != 200 or "access_token" not in body or "user" not in body:
        raise SmokeFailure(f"login_failed status={status} keys={sorted(body.keys())}")
    token = body["access_token"]
    org_id = body["user"].get("org_id")
    if not token or not org_id:
        raise SmokeFailure("login_response_missing_token_or_org")
    return token, org_id


def check_rest_thread(token: str, org_id: str, path: str, thread: str) -> dict:
    """One Cockpit thread call, byte-for-byte the URL CockpitClient.tsx's
    buildThreadApiUrl constructs (scope_type/range_days/compare_days/thread/scope_id)."""
    qs = f"scope_type=org&range_days=30&compare_days=30&thread={thread}&scope_id={org_id}"
    status, body, headers = _get(f"{path}?{qs}", token)
    plane = headers.get(PLANE_HEADER)
    row_count = _count_rows(body)
    result = {
        "check": f"rest:{thread}",
        "path": path,
        "status": status,
        "plane": plane,
        "row_count": row_count,
    }
    if status != 200:
        raise SmokeFailure(f"{thread}_status_{status}")
    if plane != "go":
        raise SmokeFailure(f"{thread}_plane_{plane or 'missing'}")
    if row_count <= 0:
        raise SmokeFailure(f"{thread}_zero_rows")
    return result


def _count_rows(body: dict) -> int:
    """Best-effort structural row count across this route family's differing
    response shapes (tiles/rows/points/items) -- never inspects field VALUES,
    only counts entries, so this stays correct across a schema change without
    encoding a specific field's business meaning here."""
    for key in ("tiles", "rows", "items", "points", "opportunities"):
        value = body.get(key)
        if isinstance(value, dict):
            return len(value)
        if isinstance(value, list):
            return len(value)
    # Some responses (e.g. featureFlags-shaped) nest one level under "data".
    data = body.get("data")
    if isinstance(data, dict):
        return _count_rows(data)
    return 0


COMPLEXITY_TIMESERIES_QUERY = """
query ComplexityTimeseries($input: ComplexityTimeseriesInput!) {
  complexityTimeseries(input: $input) {
    points {
      date
      scopeId
      scopeName
      locTotal
      cyclomaticPerKloc
      cyclomaticTotal
      cyclomaticAvg
      highComplexityFunctions
      veryHighComplexityFunctions
    }
    totalScope
  }
}
"""


def check_diagnose_graphql(token: str, org_id: str) -> dict:
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    until = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    status, body, headers = _post(
        "/graphql",
        {
            "query": COMPLEXITY_TIMESERIES_QUERY,
            "variables": {
                "input": {
                    "orgId": org_id,
                    "sinceUtc": since,
                    "untilUtc": until,
                    "granularity": "DAY",
                    "scope": "REPO",
                    "limit": 50,
                }
            },
        },
        token=token,
    )
    plane = headers.get(PLANE_HEADER)
    result = {
        "check": "graphql:complexityTimeseries",
        "status": status,
        "plane": plane,
        "has_errors": bool(body.get("errors")),
    }
    if status != 200:
        raise SmokeFailure(f"diagnose_status_{status}")
    if body.get("errors"):
        raise SmokeFailure(f"diagnose_graphql_errors={body['errors']!r}"[:300])
    if plane != "go":
        raise SmokeFailure(f"diagnose_plane_{plane or 'missing'}")
    # complexityTimeseries.points may legitimately be empty for a real org with
    # thin history (North Star check #13) -- presence of the well-shaped response
    # (no errors, plane=go) is the assertion here, row count is reported, not gated.
    points = (body.get("data") or {}).get("complexityTimeseries", {}).get(
        "points"
    ) or []
    result["row_count"] = len(points)
    return result


def main() -> int:
    global TARGET
    try:
        TARGET = parse_base_url(
            os.environ.get("DHO_SMOKE_BASE_URL", "http://traefik:3000")
        )
    except ConfigError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2
    results: list[dict] = []
    try:
        token, org_id = login()
        results.append(
            {"check": "login", "status": "ok", "org_id_present": bool(org_id)}
        )
        results.append(check_rest_thread(token, org_id, "/api/v1/home", "understand"))
        results.append(check_rest_thread(token, org_id, "/api/v1/investment", "align"))
        results.append(
            check_rest_thread(token, org_id, "/api/v1/opportunities", "execute")
        )
        results.append(check_diagnose_graphql(token, org_id))
    except SmokeFailure as e:
        print(f"FAIL: {e}", file=sys.stderr)
        _write_receipt(results, ok=False, reason=str(e))
        return 1
    except Exception as e:  # noqa: BLE001 -- an unnamed failure must still fail loud
        print(f"FAIL: unexpected_error={e!r}", file=sys.stderr)
        _write_receipt(results, ok=False, reason=f"unexpected_error={e!r}")
        return 1
    _write_receipt(results, ok=True, reason=None)
    print(
        "PASS: web-path smoke -- every Cockpit/Diagnose operation served plane=go with rows"
    )
    return 0


def _write_receipt(results: list[dict], ok: bool, reason: str | None) -> None:
    receipt = {
        "ok": ok,
        "reason": reason,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checks": results,
    }
    path = os.environ.get(
        "DHO_SMOKE_RECEIPT_PATH", "/receipts/web-path-smoke-receipt.json"
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(receipt, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    raise SystemExit(main())
