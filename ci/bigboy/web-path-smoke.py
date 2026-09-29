#!/usr/bin/env python3
"""CHAOS-6987/R460: the web-path smoke -- a REAL browser session, end to end.

Every request goes to traefik (DHO_SMOKE_BASE_URL) with the PUBLIC Host header
(DHO_SMOKE_PUBLIC_HOST), exactly as cloudflared delivers a browser request. So each
call takes the path a real user's browser takes:

    traefik (public-host router) -> web -> proxy.ts (session cookie -> bearer)
        -> BACKEND_URL (traefik, Host: traefik) -> plane-split router -> go-api / query-api / api

The session is a real Auth.js session: /api/auth/csrf, then the Credentials
provider callback (/api/auth/callback/credentials), which calls the backend login
exactly as web/src/lib/auth.ts does. No hand-minted token is used anywhere.

What is derived from web source (DHO_SMOKE_WEB_SRC, a read-only mount of web/src),
never hand-typed:
  * GraphQL documents: the `export const ..._QUERY` text from lib/graphql/queries.ts
    and lib/testops/queries.ts, put through urql's formatDocument transform
    (__typename added to every non-root selection set) and graphql print -- the
    bytes web actually sends. Each document's sha256 must equal the registered
    digest in the edge catalog (DHO_SMOKE_CATALOG); a mismatch means the edge would
    NOT route web's request to Go, and fails as `document_digest_mismatch`.
  * REST paths: every path this smoke calls must still appear as a literal in the
    web source file that calls it; a missing literal fails as `web_source_path_missing`.
  * The `f=` filter parameter: web's encodeFilter (sorted-key JSON, base64url) over
    web's defaultMetricFilter normalized to org scope (lib/api/_shared.ts).

KNOWN-MISSING operations come from ci/bigboy/routing-ops.txt (the same list the
routing-ledger check uses). A known-missing check is reported by name and makes the
exit code 3, never 0; if it starts passing, the smoke fails so the marker is removed.

Credentials: DHO_SMOKE_ADMIN_EMAIL and DHO_SMOKE_ADMIN_PASSWORD_FILE (the file's
CONTENT is the password, *_FILE convention, CHAOS-6972). The password, the session
cookies, the access token and the org id are held in memory only: never printed,
never in the receipt. The receipt carries structural facts only (status, plane,
counts, booleans).

Exit: 0 every check passed; 1 a check failed (named on stderr and in the receipt);
3 every check passed except named KNOWN-MISSING checks.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import os
import re
import sys
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.message import Message
from pathlib import Path
from typing import Any

PUBLIC_HOST = os.environ.get("DHO_SMOKE_PUBLIC_HOST", "www.commanderkeen.dev")
PUBLIC_ORIGIN = f"https://{PUBLIC_HOST}"
WEB_SRC = Path(os.environ.get("DHO_SMOKE_WEB_SRC", "/web-src"))
CATALOG = Path(os.environ.get("DHO_SMOKE_CATALOG", "/catalog/catalog.json"))
ROUTING_OPS = Path(
    os.environ.get(
        "DHO_SMOKE_ROUTING_OPS", str(Path(__file__).with_name("routing-ops.txt"))
    )
)
PLANE_HEADER = "X-Dev-Health-Plane"
SERVICE_UNAVAILABLE_TEXT = (
    "Data service unavailable"  # web/src/components/ServiceUnavailable.tsx
)

# web/src/lib/filters/defaults.ts defaultMetricFilter, after lib/api/_shared.ts
# normalizeFilters (team scope with no ids -> org scope).
DEFAULT_FILTER: dict[str, Any] = {
    "time": {"range_days": 14, "compare_days": 14},
    "scope": {"level": "org", "ids": []},
    "who": {},
    "what": {},
    "why": {},
    "how": {},
}

# REST path -> the web source file that calls it (relative to web/src).
REST_SOURCES: dict[str, str] = {
    "/api/v1/investment/explain": "lib/api/investment.ts",
    "/api/v1/work-units": "lib/api/investment.ts",
    "/api/v1/drilldown/prs": "lib/api/investment.ts",
    "/api/v1/investment": "lib/api/investment.ts",
    "/api/v1/filters/options": "components/filters/useFilterOptions.ts",
    "/api/v1/flame": "lib/api/visuals.ts",
    "/health": "lib/api/system.ts",
}

# GraphQL operation -> (web source file, exported constant).
GRAPHQL_SOURCES: dict[str, tuple[str, str]] = {
    "complexityTimeseries": ("lib/graphql/queries.ts", "COMPLEXITY_TIMESERIES_QUERY"),
    "workGraphFlow": ("lib/graphql/queries.ts", "WORK_GRAPH_FLOW_QUERY"),
    "testopsRisk": ("lib/testops/queries.ts", "TESTOPS_RISK_QUERY"),
    # CHAOS-7190: the three read operations seeded by `routing seed` (CHAOS-7165).
    "home": ("lib/graphql/queries.ts", "HOME_QUERY"),
    "recommendations": ("lib/graphql/queries.ts", "RECOMMENDATIONS_QUERY"),
    "workItemTeamAttributions": (
        "lib/graphql/queries.ts",
        "WORK_ITEM_TEAM_ATTRIBUTIONS_QUERY",
    ),
}


class SmokeFailure(Exception):
    """Raised with a short, named reason -- never with a credential or body value."""


class KnownMissing(Exception):
    """A check for a KNOWN-MISSING operation did not pass (expected, reported by name)."""


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in tests/tooling/test_bigboy_web_path_smoke.py)
# ---------------------------------------------------------------------------


def encode_filter(filters: dict[str, Any]) -> str:
    """web/src/lib/filters/encode.ts encodeFilter: stableStringify + base64url, no padding."""
    serialized = json.dumps(
        filters, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return (
        base64.urlsafe_b64encode(serialized.encode("utf-8")).decode("ascii").rstrip("=")
    )


def ts_exports(text: str) -> dict[str, str]:
    """`export const NAME = `...`;` template literals with no ${} interpolation."""
    found = dict(re.findall(r"export const (\w+) = `([^`]*)`;", text))
    return {k: v for k, v in found.items() if "${" not in v}


def urql_format(document: str) -> str:
    """urql formatDocument (add __typename to every non-root selection set) + graphql print."""
    from graphql import parse, print_ast
    from graphql.language import (
        FieldNode,
        NameNode,
        OperationDefinitionNode,
        SelectionSetNode,
        Visitor,
        visit,
    )

    class _AddTypename(Visitor):
        def enter_selection_set(
            self, node: SelectionSetNode, _key: Any, parent: Any, *_rest: Any
        ) -> Any:
            if isinstance(parent, OperationDefinitionNode):
                return None
            if any(
                isinstance(s, FieldNode) and s.name.value == "__typename"
                for s in node.selections
            ):
                return None
            typename = FieldNode(
                name=NameNode(value="__typename"), arguments=(), directives=()
            )
            return SelectionSetNode(selections=(*node.selections, typename))

    return str(print_ast(visit(parse(document), _AddTypename())))


def document_digest(text: str) -> str:
    """go_api_document_digest: sha256 of the trimmed text (ASCII whitespace suffices here)."""
    return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()


def known_missing_ops(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in text.splitlines():
        parts = raw.split("#", 1)[0].split()
        if len(parts) >= 3 and parts[1] == "KNOWN-MISSING":
            out[parts[0]] = parts[2]
    return out


@dataclass
class CookieJar:
    """Name -> value. Secure cookies are kept although the hop to traefik is plain http:
    the browser's hop is https, and this jar stands in for the browser."""

    values: dict[str, str] = field(default_factory=dict)

    def update(self, headers: Message | None) -> None:
        raw_cookies = (
            (headers.get_all("Set-Cookie") or []) if headers is not None else []
        )
        for raw in raw_cookies:
            first, _, attrs = raw.partition(";")
            name, _, value = first.strip().partition("=")
            attr_text = attrs.lower().replace(" ", "")
            expired = "max-age=0" in attr_text or "expires=thu,01jan1970" in attr_text
            if expired or value == "":
                self.values.pop(name, None)
            else:
                self.values[name] = value

    def header(self) -> str:
        return "; ".join(f"{k}={v}" for k, v in self.values.items())

    def find(self, suffix: str) -> str | None:
        for name, value in self.values.items():
            if name.endswith(suffix):
                return value
        return None

    def has_session(self) -> bool:
        return any("session-token" in name for name in self.values)


def read_secret_file(path: str) -> tuple[str, int]:
    """The *_FILE convention: file content minus one trailing CR/LF and surrounding whitespace.

    Returns (value, file byte length). The byte length is the only fact about the file
    that may be reported; the value is never printed.
    """
    with open(path, "rb") as f:
        raw = f.read()
    text = raw.decode("utf-8")
    if text.endswith("\r\n"):
        text = text[:-2]
    elif text.endswith(("\n", "\r")):
        text = text[:-1]
    return text.strip(), len(raw)


# Response shapes the counter knows, by the key that carries the rows (from web source
# types, e.g. web/src/lib/types.ts). A dict value counts its non-empty entries (a
# distribution map such as InvestmentResponse.theme_distribution); a list counts items.
ROW_KEYS = (
    "tiles",
    "rows",
    "items",
    "points",
    "opportunities",
    "frames",
    "theme_distribution",
)


def count_rows(body: Any) -> int:
    """Structural entry count by known response shape; never inspects values beyond emptiness.

    An unknown shape FAILS (SmokeFailure naming the top-level keys) -- counting zero for a
    shape the counter does not know would read as "no data" when it is "not measured".
    """
    if isinstance(body, list):
        return len(body)
    if not isinstance(body, dict):
        raise SmokeFailure(f"unknown_response_shape type={type(body).__name__}")
    for key in ROW_KEYS:
        value = body.get(key)
        if isinstance(value, list):
            return len(value)
        if isinstance(value, dict):
            return sum(1 for v in value.values() if v not in (None, 0, "", [], {}))
    data = body.get("data")
    if isinstance(data, dict):
        return count_rows(data)
    raise SmokeFailure(
        f"unknown_response_shape keys={','.join(sorted(map(str, body)))[:200]}"
    )


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


# The smoke runs inside the compose network and connects ONLY to the router, over plain
# http (there is no TLS hop inside the network to verify). The browser's public hostname
# travels in the Host header, never as a connection target. DHO_SMOKE_BASE_URL and
# DHO_SMOKE_PUBLIC_HOST are checked before any connection is opened; requests then go
# through http.client with a fixed host/port and a path per call site, never a dynamic
# URL string. http.client never follows redirects, which the session checks rely on.
ALLOWED_SCHEMES = frozenset({"http"})
ALLOWED_HOSTS = frozenset({"traefik"})
ALLOWED_PUBLIC_HOSTS = frozenset({"www.commanderkeen.dev", "commanderkeen.dev"})
ALLOWED_HOST_HEADERS = ALLOWED_HOSTS | ALLOWED_PUBLIC_HOSTS


class ConfigError(Exception):
    """DHO_SMOKE_BASE_URL is not an allowed target; the reason names the rule it broke."""


@dataclass(frozen=True)
class Target:
    scheme: str
    host: str
    port: int


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


def _send(
    method: str,
    path: str,
    body: bytes | None,
    headers: dict[str, str],
    timeout: int = 60,
) -> tuple[int, bytes, Message]:
    if TARGET is None:
        raise ConfigError("base_url_not_parsed")
    if not path.startswith("/"):
        raise ConfigError("request_path_must_be_absolute")
    if headers.get("Host") not in ALLOWED_HOST_HEADERS:
        raise ConfigError("host_header_not_allowed")
    conn = http.client.HTTPConnection(TARGET.host, TARGET.port, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        return resp.status, resp.read(), resp.msg
    finally:
        conn.close()


@dataclass
class Response:
    status: int
    body: Any
    headers: Message | None
    raw_len: int
    text: str

    def header(self, name: str) -> str | None:
        return self.headers.get(name) if self.headers is not None else None


def request(
    method: str,
    path: str,
    *,
    host: str = PUBLIC_HOST,
    jar: CookieJar | None = None,
    json_body: Any = None,
    form: dict[str, str] | None = None,
    timeout: int = 60,
) -> Response:
    headers = {
        "Host": host,
        "X-Forwarded-Proto": "https",
        "Accept": "application/json, text/html",
    }
    data: bytes | None = None
    if json_body is not None:
        data = json.dumps(json_body).encode()
        headers["Content-Type"] = "application/json"
    elif form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if jar is not None and jar.values:
        headers["Cookie"] = jar.header()
    status, raw, resp_headers = _send(method, path, data, headers, timeout)
    if jar is not None:
        jar.update(resp_headers)
    text = raw.decode("utf-8", errors="replace")
    body: Any
    try:
        body = json.loads(text) if raw else {}
    except json.JSONDecodeError:
        body = None
    return Response(status, body, resp_headers, len(raw), text)


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


@dataclass
class Ctx:
    jar: CookieJar
    org_id: str = ""
    documents: dict[str, str] = field(default_factory=dict)
    known_missing: dict[str, str] = field(default_factory=dict)
    flame_entity: str | None = None
    team_id: str | None = None


def check_web_source(ctx: Ctx) -> dict[str, Any]:
    missing = []
    for path, rel in REST_SOURCES.items():
        src = WEB_SRC / rel
        if not src.is_file() or f'"{path}"' not in src.read_text(encoding="utf-8"):
            missing.append(path)
    if missing:
        raise SmokeFailure(f"web_source_path_missing={','.join(missing)}")
    catalog = {
        o["operation"]: o["digest"]
        for o in json.loads(CATALOG.read_text(encoding="utf-8"))
    }
    mismatched = []
    for op, (rel, const) in GRAPHQL_SOURCES.items():
        exports = ts_exports((WEB_SRC / rel).read_text(encoding="utf-8"))
        if const not in exports:
            raise SmokeFailure(f"web_source_document_missing={const}")
        doc = urql_format(exports[const])
        if catalog.get(op) != document_digest(doc):
            mismatched.append(op)
        ctx.documents[op] = doc
    if mismatched:
        raise SmokeFailure(f"document_digest_mismatch={','.join(mismatched)}")
    return {
        "rest_paths": len(REST_SOURCES),
        "graphql_documents": len(GRAPHQL_SOURCES),
        "digests_match": True,
    }


def check_public_host_unauth(_ctx: Ctx) -> dict[str, Any]:
    """D2735: a browser request without a session must land on web (303), never a plane."""
    r = request("GET", "/api/v1/work-units?f=" + encode_filter(DEFAULT_FILTER))
    location = urllib.parse.urlparse(r.header("Location") or "").path
    plane = r.header(PLANE_HEADER)
    if r.status != 303 or location != "/auth/signin" or plane:
        raise SmokeFailure(
            f"public_host_unauth_not_on_web status={r.status} plane={plane or 'none'}"
        )
    return {"status": r.status, "plane": plane, "location_path": location}


def login(ctx: Ctx) -> dict[str, Any]:
    email = os.environ["DHO_SMOKE_ADMIN_EMAIL"]
    password, file_bytes = read_secret_file(os.environ["DHO_SMOKE_ADMIN_PASSWORD_FILE"])
    used_len = len(password)
    csrf = request("GET", "/api/auth/csrf", jar=ctx.jar)
    token = csrf.body.get("csrfToken") if isinstance(csrf.body, dict) else None
    if csrf.status != 200 or not token:
        raise SmokeFailure(f"csrf_failed status={csrf.status}")
    form = {
        "csrfToken": token,
        "email": email,
        "password": password,
        "org_id": "",
        "callbackUrl": f"{PUBLIC_ORIGIN}/dashboard",
    }
    del password
    r = request("POST", "/api/auth/callback/credentials", jar=ctx.jar, form=form)
    form.clear()
    location = r.header("Location") or ""
    if r.status not in (302, 303) or "error=" in location or not ctx.jar.has_session():
        error_code = urllib.parse.parse_qs(urllib.parse.urlparse(location).query).get(
            "error", [""]
        )[0]
        raise SmokeFailure(
            f"login_failed status={r.status} session_cookie={ctx.jar.has_session()} "
            f"error={error_code or 'none'} password_file_bytes={file_bytes} password_used_len={used_len}"
        )
    s = request("GET", "/api/auth/session", jar=ctx.jar)
    user = s.body.get("user") if isinstance(s.body, dict) else None
    ctx.org_id = (user or {}).get("org_id") or ""
    if s.status != 200 or not ctx.org_id:
        raise SmokeFailure(f"session_missing_org status={s.status}")
    return {"status": "ok", "org_id_present": True}


def check_callback_origin(ctx: Ctx) -> dict[str, Any]:
    """D2736: Auth.js's callback-url cookie must carry the public origin, not the Node bind address."""
    raw = ctx.jar.find("authjs.callback-url")
    parsed = urllib.parse.urlparse(urllib.parse.unquote(raw or ""))
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme else ""
    if origin != PUBLIC_ORIGIN:
        raise SmokeFailure(
            f"callback_url_origin={origin or 'missing'} expected={PUBLIC_ORIGIN}"
        )
    return {"cookie_present": True, "origin_is_public": True}


def _assert_go(
    name: str,
    r: Response,
    rows: int | Callable[[], int] | None,
    *,
    need_rows: bool = True,
) -> dict[str, Any]:
    plane = r.header(PLANE_HEADER)
    result: dict[str, Any] = {"status": r.status, "plane": plane}
    if r.status != 200:
        raise SmokeFailure(f"{name}_status_{r.status}")
    if plane != "go":
        raise SmokeFailure(f"{name}_plane_{plane or 'missing'}")
    # Rows are counted only after status and plane passed, so an error body is reported
    # as the status it is, not as an unknown shape.
    if callable(rows):
        try:
            rows = rows()
        except SmokeFailure as e:
            raise SmokeFailure(f"{name}_{e}") from None
    if rows is not None:
        result["row_count"] = rows
    if need_rows and rows is not None and rows <= 0:
        raise SmokeFailure(f"{name}_zero_rows")
    return result


def check_rest_thread(ctx: Ctx, path: str, thread: str) -> dict[str, Any]:
    """Cockpit thread call, the URL CockpitClient.tsx buildThreadApiUrl builds."""
    qs = urllib.parse.urlencode(
        {
            "scope_type": "org",
            "range_days": 30,
            "compare_days": 30,
            "thread": thread,
            "scope_id": ctx.org_id,
        }
    )
    r = request("GET", f"{path}?{qs}", jar=ctx.jar)
    return {"path": path, **_assert_go(f"rest_{thread}", r, lambda: count_rows(r.body))}


def check_filter_options(ctx: Ctx) -> dict[str, Any]:
    r = request("GET", "/api/v1/filters/options", jar=ctx.jar)
    body = r.body if isinstance(r.body, dict) else {}
    nonempty = sum(1 for v in body.values() if isinstance(v, list) and v)
    teams = body.get("teams")
    if isinstance(teams, list) and teams:
        first = teams[0]
        ctx.team_id = str(first.get("id")) if isinstance(first, dict) else str(first)
    return _assert_go("filters_options", r, nonempty)


def check_investment_explain(ctx: Ctx) -> dict[str, Any]:
    """lib/api/investment.ts explainInvestmentMix: POST body + f= and llm_provider=auto."""
    qs = urllib.parse.urlencode(
        {"f": encode_filter(DEFAULT_FILTER), "llm_provider": "auto"}
    )
    body = {"filters": DEFAULT_FILTER, "theme": None, "subcategory": None}
    r = request(
        "POST",
        f"/api/v1/investment/explain?{qs}",
        jar=ctx.jar,
        json_body=body,
        timeout=180,
    )
    keys = len(r.body) if isinstance(r.body, dict) else 0
    return _assert_go("investment_explain", r, keys)


def check_work_units(ctx: Ctx) -> dict[str, Any]:
    """lib/api/investment.ts getWorkUnits with include_textual=true."""
    qs = urllib.parse.urlencode(
        {"f": encode_filter(DEFAULT_FILTER), "include_textual": "true"}
    )
    body = {"filters": DEFAULT_FILTER, "include_textual": True}
    r = request(
        "POST", f"/api/v1/work-units?{qs}", jar=ctx.jar, json_body=body, timeout=120
    )
    return _assert_go("work_units", r, lambda: count_rows(r.body))


def check_drilldown_prs(ctx: Ctx) -> dict[str, Any]:
    """lib/api/investment.ts getDrilldown; the first PR row feeds the flame check
    (explore/page.tsx builds the flame link as `${repo_id}:${number}`)."""
    for days in (14, 90):
        filters = {**DEFAULT_FILTER, "time": {"range_days": days, "compare_days": days}}
        r = request(
            "POST",
            "/api/v1/drilldown/prs",
            jar=ctx.jar,
            json_body={"filters": filters, "limit": 50},
        )
        items = r.body.get("items") if isinstance(r.body, dict) else None
        result = {
            **_assert_go("drilldown_prs", r, len(items or []), need_rows=False),
            "range_days": days,
        }
        for item in items or []:
            if isinstance(item.get("repo_id"), str) and isinstance(
                item.get("number"), int
            ):
                ctx.flame_entity = f"{item['repo_id']}:{item['number']}"
                return result
    raise SmokeFailure("drilldown_prs_no_pr_row_for_flame")


def check_flame(ctx: Ctx) -> dict[str, Any]:
    """lib/api/visuals.ts getFlame, entity from a real drilldown row (prs/[pr_id]/page.tsx)."""
    if not ctx.flame_entity:
        raise SmokeFailure("flame_no_entity")
    qs = urllib.parse.urlencode({"entity_type": "pr", "entity_id": ctx.flame_entity})
    r = request("GET", f"/api/v1/flame?{qs}", jar=ctx.jar)
    return _assert_go("flame", r, lambda: count_rows(r.body))


def check_backend_health(_ctx: Ctx) -> dict[str, Any]:
    """lib/api/system.ts checkApiHealth, as web's server side sends it (BACKEND_URL, Host: traefik)."""
    r = request("GET", "/health", host="traefik")
    ok = isinstance(r.body, dict) and r.body.get("status") == "ok"
    if r.status != 200 or not ok:
        raise SmokeFailure(f"backend_health status={r.status} status_ok={ok}")
    return {"status": r.status, "plane": r.header(PLANE_HEADER), "status_ok": ok}


def _graphql(ctx: Ctx, op: str, variables: dict[str, Any]) -> Response:
    return request(
        "POST",
        "/graphql",
        jar=ctx.jar,
        json_body={"query": ctx.documents[op], "variables": variables},
    )


def _gql_data(r: Response, op: str) -> dict[str, Any]:
    data = r.body.get("data") if isinstance(r.body, dict) else None
    value = (data or {}).get(op)
    return value if isinstance(value, dict) else {}


def check_complexity(ctx: Ctx) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    variables = {
        "input": {
            "orgId": ctx.org_id,
            "sinceUtc": (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "untilUtc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "granularity": "DAY",
            "scope": "REPO",
            "limit": 50,
        }
    }
    r = _graphql(ctx, "complexityTimeseries", variables)
    if isinstance(r.body, dict) and r.body.get("errors"):
        raise SmokeFailure("complexity_graphql_errors")
    points = _gql_data(r, "complexityTimeseries").get("points") or []
    # Thin history is legitimate (North Star #13): row count is reported, not gated.
    return _assert_go("complexity", r, len(points), need_rows=False)


def check_work_graph_flow(ctx: Ctx) -> dict[str, Any]:
    """Diagnose > Work Graph > Inflow-Outflow (useWorkGraphFlow: variables orgId + filters)."""
    r = _graphql(ctx, "workGraphFlow", {"orgId": ctx.org_id})
    if isinstance(r.body, dict) and r.body.get("errors"):
        raise SmokeFailure("work_graph_flow_graphql_errors")
    rows = _gql_data(r, "workGraphFlow").get("rows") or []
    result = _assert_go("work_graph_flow", r, len(rows))
    empty = sum(1 for row in rows if not (row or {}).get("nodeType"))
    result["empty_node_type"] = empty
    if empty:
        raise SmokeFailure(f"work_graph_flow_empty_node_type={empty}")
    return result


def _check_seeded_read(
    ctx: Ctx, op: str, variables: dict[str, Any], shape: type
) -> dict[str, Any]:
    """CHAOS-7190: a seeded read op must be answered by the Go plane through the web path.

    Order matters: status and plane first (a Python-plane answer to a Go-only field is the
    D3057 signature and is named `<op>_plane_python`), then GraphQL errors, then the shape
    of `data.<op>`. Row counts are reported, not gated (thin history is legitimate)."""
    r = _graphql(ctx, op, variables)
    result = _assert_go(op, r, None)
    if isinstance(r.body, dict) and r.body.get("errors"):
        raise SmokeFailure(f"{op}_graphql_errors")
    data = r.body.get("data") if isinstance(r.body, dict) else None
    value = (data or {}).get(op)
    if not isinstance(value, shape):
        raise SmokeFailure(f"{op}_data_shape")
    if isinstance(value, list):
        result["row_count"] = len(value)
    return result


def check_home(ctx: Ctx) -> dict[str, Any]:
    variables = {
        "orgId": ctx.org_id,
        "window": {"rangeDays": 14, "compareDays": 14},
    }
    return _check_seeded_read(ctx, "home", variables, dict)


def check_recommendations(ctx: Ctx) -> dict[str, Any]:
    variables = {
        "orgId": ctx.org_id,
        "team": ctx.team_id or "smoke-probe",
        "window": {"unit": "DAY", "value": 14},
    }
    return _check_seeded_read(ctx, "recommendations", variables, list)


def check_work_item_team_attributions(ctx: Ctx) -> dict[str, Any]:
    variables: dict[str, Any] = {"orgId": ctx.org_id, "workItemIds": [], "teamId": None}
    return _check_seeded_read(ctx, "workItemTeamAttributions", variables, list)


def check_testops_risk(ctx: Ctx) -> dict[str, Any]:
    """testops/risk page: fetchRiskMetrics (lib/testops/fetchers.ts) with the page's dateRange."""
    today = datetime.now(timezone.utc).date()
    date_range = {
        "startDate": (today - timedelta(days=14)).isoformat(),
        "endDate": today.isoformat(),
    }
    r = _graphql(ctx, "testopsRisk", {"orgId": ctx.org_id, "input": date_range})
    errors = bool(isinstance(r.body, dict) and r.body.get("errors"))
    plane = r.header(PLANE_HEADER)
    known = ctx.known_missing.get("testopsRisk")
    if r.status != 200 or errors or plane != "go":
        if known:
            raise KnownMissing(
                f"graphql:testopsRisk ({known}) status={r.status} plane={plane or 'missing'}"
            )
        raise SmokeFailure(
            f"testops_risk status={r.status} plane={plane or 'missing'} errors={errors}"
        )
    if known:
        raise SmokeFailure(
            "testops_risk_known_missing_now_served_by_go: remove the marker in routing-ops.txt"
        )
    return {"status": r.status, "plane": plane, "has_errors": errors}


def check_testops_risk_page(ctx: Ctx) -> dict[str, Any]:
    """The page chris saw fail: 200 and no ServiceUnavailable panel."""
    r = request("GET", "/testops/risk", jar=ctx.jar, timeout=120)
    unavailable = SERVICE_UNAVAILABLE_TEXT in r.text
    if r.status != 200 or unavailable:
        known = ctx.known_missing.get("testopsRisk")
        if known:
            raise KnownMissing(
                f"page:testops/risk ({known}) status={r.status} unavailable={unavailable}"
            )
        raise SmokeFailure(
            f"testops_risk_page status={r.status} unavailable={unavailable}"
        )
    return {"status": r.status, "service_unavailable": False, "html_bytes": r.raw_len}


def logout(ctx: Ctx) -> dict[str, Any]:
    """Sign-out must redirect to the public origin (D2736), not the Node bind address."""
    csrf = request("GET", "/api/auth/csrf", jar=ctx.jar)
    token = csrf.body.get("csrfToken") if isinstance(csrf.body, dict) else None
    if not token:
        raise SmokeFailure(f"logout_csrf_failed status={csrf.status}")
    r = request(
        "POST",
        "/api/auth/signout",
        jar=ctx.jar,
        form={"csrfToken": token, "callbackUrl": f"{PUBLIC_ORIGIN}/"},
    )
    loc = urllib.parse.urlparse(r.header("Location") or "")
    origin = f"{loc.scheme}://{loc.netloc}" if loc.scheme else ""
    if r.status not in (302, 303) or origin != PUBLIC_ORIGIN:
        raise SmokeFailure(
            f"logout_redirect_origin={origin or 'missing'} status={r.status}"
        )
    if ctx.jar.has_session():
        raise SmokeFailure("logout_session_cookie_not_cleared")
    return {
        "status": r.status,
        "redirect_origin_is_public": True,
        "session_cleared": True,
    }


def run() -> tuple[list[dict[str, Any]], list[str], list[str]]:
    ctx = Ctx(
        jar=CookieJar(),
        known_missing=known_missing_ops(ROUTING_OPS.read_text(encoding="utf-8")),
    )
    results: list[dict[str, Any]] = []
    failures: list[str] = []
    known: list[str] = []

    def step(
        name: str, fn: Callable[..., dict[str, Any]], *args: Any, fatal: bool = False
    ) -> None:
        try:
            results.append({"check": name, "ok": True, **fn(ctx, *args)})
            return
        except KnownMissing as e:
            known.append(str(e))
            results.append({"check": name, "ok": False, "known_missing": True})
            return
        except SmokeFailure as e:
            reason = str(e)
        except Exception as e:  # an unnamed failure must still fail loud, by type only
            reason = f"unexpected_error={type(e).__name__}"
        failures.append(f"{name}: {reason}")
        results.append({"check": name, "ok": False, "reason": reason})
        if fatal:
            raise SmokeFailure(name)

    try:
        step("web_source", check_web_source, fatal=True)
        step("public_host_unauth_on_web", check_public_host_unauth)
        step("login", login, fatal=True)
        step("callback_url_origin", check_callback_origin)
        step("backend_health", check_backend_health)
        step("rest:understand", check_rest_thread, "/api/v1/home", "understand")
        step("rest:align", check_rest_thread, "/api/v1/investment", "align")
        step("rest:execute", check_rest_thread, "/api/v1/opportunities", "execute")
        step("rest:filters_options", check_filter_options)
        step("rest:investment_explain", check_investment_explain)
        step("rest:work_units", check_work_units)
        step("rest:drilldown_prs", check_drilldown_prs)
        step("rest:flame", check_flame)
        step("graphql:complexityTimeseries", check_complexity)
        step("graphql:workGraphFlow", check_work_graph_flow)
        step("graphql:home", check_home)
        step("graphql:recommendations", check_recommendations)
        step(
            "graphql:workItemTeamAttributions",
            check_work_item_team_attributions,
        )
        step("graphql:testopsRisk", check_testops_risk)
        step("page:testops_risk", check_testops_risk_page)
        step("logout", logout)
    except SmokeFailure:
        pass  # a fatal step already recorded its failure
    return results, failures, known


def main() -> int:
    global TARGET
    try:
        TARGET = parse_base_url(
            os.environ.get("DHO_SMOKE_BASE_URL", "http://traefik:3000")
        )
    except ConfigError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 2
    if PUBLIC_HOST not in ALLOWED_PUBLIC_HOSTS:
        print(f"FAIL: public_host_not_allowed={PUBLIC_HOST}", file=sys.stderr)
        return 2
    results, failures, known = run()
    _write_receipt(results, ok=not failures, failures=failures, known=known)
    for f in failures:
        print(f"FAIL: {f}", file=sys.stderr)
    for k in known:
        print(f"KNOWN-MISSING: {k}")
    passed = sum(1 for r in results if r.get("ok"))
    verdict = "FAIL" if failures else ("KNOWN-GAP" if known else "PASS")
    print(
        f"WEB_PATH_SMOKE verdict={verdict} checks={len(results)} passed={passed} "
        f"failed={len(failures)} known_missing={len(known)}"
    )
    if failures:
        return 1
    return 3 if known else 0


def _write_receipt(
    results: list[dict[str, Any]], *, ok: bool, failures: list[str], known: list[str]
) -> None:
    receipt = {
        "ok": ok,
        "failures": failures,
        "known_missing": known,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checks": results,
    }
    path = os.environ.get(
        "DHO_SMOKE_RECEIPT_PATH", "/receipts/web-path-smoke-receipt.json"
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(receipt, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    raise SystemExit(main())
