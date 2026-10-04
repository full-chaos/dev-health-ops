"""CHAOS-6987/R460: ci/bigboy/web-path-smoke.py -- derivation helpers and verdict semantics.

The smoke's GraphQL documents are derived from web source through urql's
formatDocument transform; the oracle here is the REAL registered documents the Go
plane serves (internal/queryapi/server/query_route.go) and the real edge catalog
(contracts/graphql/v1/go_api_operations.json): stripping __typename from a
registered document and re-deriving it must give back the same bytes and digest.
"""

from __future__ import annotations

import base64
import importlib.util
import json
import re
import sys
from email.message import Message
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "bigboy" / "web-path-smoke.py"
QUERY_ROUTE = ROOT / "internal" / "queryapi" / "server" / "query_route.go"
CATALOG = ROOT / "contracts" / "graphql" / "v1" / "go_api_operations.json"
ROUTING_OPS = ROOT / "ci" / "bigboy" / "routing-ops.txt"

REGISTERED = {
    "complexityTimeseries": "registeredComplexityTimeseriesDocument",
    "workGraphFlow": "registeredWorkGraphFlowDocument",
    "testopsRisk": "registeredTestopsRiskDocument",
    "home": "registeredHomeDocument",
    "recommendations": "registeredRecommendationsDocument",
    "workItemTeamAttributions": "registeredWorkItemTeamAttributionsDocument",
}


@pytest.fixture(scope="module")
def smoke() -> ModuleType:
    spec = importlib.util.spec_from_file_location("web_path_smoke", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _registered(const: str) -> str:
    m = re.search(
        rf"^const {const} = `([^`]*)`",
        QUERY_ROUTE.read_text(encoding="utf-8"),
        flags=re.M,
    )
    assert m, f"{const} not found in query_route.go"
    return m.group(1)


@pytest.mark.parametrize("op", sorted(REGISTERED))
def test_urql_format_reproduces_registered_document(smoke: ModuleType, op: str) -> None:
    registered = _registered(REGISTERED[op])
    web_like = "\n" + re.sub(r"\n\s*__typename", "", registered) + "\n"
    assert "__typename" not in web_like
    derived = smoke.urql_format(web_like)
    assert derived == registered
    catalog = {
        o["operation"]: o["digest"]
        for o in json.loads(CATALOG.read_text(encoding="utf-8"))
    }
    assert smoke.document_digest(derived) == catalog[op]


def test_ts_exports_skips_interpolated_templates(smoke: ModuleType) -> None:
    src = "export const A_QUERY = `\nquery A { a }\n`;\nexport const B = `x ${frag}`;\n"
    assert smoke.ts_exports(src) == {"A_QUERY": "\nquery A { a }\n"}


def test_encode_filter_is_sorted_base64url_without_padding(smoke: ModuleType) -> None:
    enc = smoke.encode_filter(smoke.DEFAULT_FILTER)
    assert "=" not in enc and "+" not in enc and "/" not in enc
    decoded = base64.urlsafe_b64decode(enc + "=" * (-len(enc) % 4)).decode()
    assert decoded == (
        '{"how":{},"scope":{"ids":[],"level":"org"},"time":{"compare_days":14,"range_days":14},'
        '"what":{},"who":{},"why":{}}'
    )


def test_cookie_jar_sets_and_expires(smoke: ModuleType) -> None:
    jar = smoke.CookieJar()
    h = Message()
    h["Set-Cookie"] = "__Secure-authjs.session-token=abc; Path=/; HttpOnly; Secure"
    h["Set-Cookie"] = "__Secure-authjs.callback-url=https%3A%2F%2Fwww.example; Path=/"
    jar.update(h)
    assert jar.has_session()
    assert jar.find("authjs.callback-url") == "https%3A%2F%2Fwww.example"
    gone = Message()
    gone["Set-Cookie"] = "__Secure-authjs.session-token=; Path=/; Max-Age=0"
    jar.update(gone)
    assert not jar.has_session()


def test_tracked_routing_ops_marks_testops_risk(smoke: ModuleType) -> None:
    known = smoke.known_missing_ops(ROUTING_OPS.read_text(encoding="utf-8"))
    assert known == {"testopsRisk": "CHAOS-6993"}


def test_tracked_routing_ops_keeps_every_listed_operation() -> None:
    # An operation dropped from the list would make the smoke skip it without a sign.
    # Count the operation lines (comment-only and blank lines are not operations).
    lines = [
        ln.split("#", 1)[0].split()
        for ln in ROUTING_OPS.read_text(encoding="utf-8").splitlines()
    ]
    assert len([parts for parts in lines if parts]) == 58


# --- verdict semantics against a scripted fake of the whole web path --------------


def _resp(
    smoke: ModuleType,
    status: int,
    body: Any = None,
    headers: dict[str, list[str]] | None = None,
    text: str = "",
) -> Any:
    h = Message()
    for k, vs in (headers or {}).items():
        for v in vs:
            h[k] = v
    raw = json.dumps(body) if body is not None else text
    return smoke.Response(status, body if body is not None else None, h, len(raw), raw)


def _fake_web(smoke: ModuleType, overrides: dict[str, Any]) -> Any:
    go = {"X-Dev-Health-Plane": ["go"]}

    def fake(
        method: str,
        path: str,
        *,
        host: str = "",
        jar: Any = None,
        json_body: Any = None,
        form: Any = None,
        timeout: int = 0,
    ) -> Any:
        key = path.split("?")[0]
        if json_body and "query" in json_body:
            key = "gql:" + json_body["query"].split("(")[0].split()[-1]
        if key in overrides:
            r = overrides[key]
            if jar is not None:
                jar.update(r.headers)
            return r
        table: dict[str, Any] = {
            "/api/v1/work-units": _resp(smoke, 200, [{"id": 1}], go)
            if jar and jar.has_session()
            else _resp(
                smoke,
                303,
                {},
                {
                    "Location": [
                        "https://www.commanderkeen.dev/auth/signin?callbackUrl=x"
                    ]
                },
            ),
            "/api/auth/csrf": _resp(
                smoke,
                200,
                {"csrfToken": "t"},
                {
                    "Set-Cookie": [
                        "__Secure-authjs.callback-url=https%3A%2F%2Fwww.commanderkeen.dev; Path=/"
                    ]
                },
            ),
            "/api/auth/callback/credentials": _resp(
                smoke,
                302,
                {},
                {
                    "Location": ["https://www.commanderkeen.dev/dashboard"],
                    "Set-Cookie": ["__Secure-authjs.session-token=s; Path=/"],
                },
            ),
            "/api/auth/session": _resp(smoke, 200, {"user": {"org_id": "o"}}),
            "/health": _resp(smoke, 200, {"status": "ok"}, go),
            "/api/v1/home": _resp(smoke, 200, {"tiles": [1]}, go),
            "/api/v1/investment": _resp(smoke, 200, {"tiles": [1]}, go),
            "/api/v1/opportunities": _resp(smoke, 200, {"tiles": [1]}, go),
            "/api/v1/filters/options": _resp(
                smoke, 200, {"teams": ["t"], "repos": []}, go
            ),
            "/api/v1/investment/explain": _resp(smoke, 200, {"summary": "s"}, go),
            "/api/v1/drilldown/prs": _resp(
                smoke, 200, {"items": [{"repo_id": "r", "number": 7}]}, go
            ),
            "/api/v1/flame": _resp(smoke, 200, {"frames": [1]}, go),
            "gql:ComplexityTimeseries": _resp(
                smoke, 200, {"data": {"complexityTimeseries": {"points": []}}}, go
            ),
            "gql:WorkGraphFlow": _resp(
                smoke,
                200,
                {"data": {"workGraphFlow": {"rows": [{"nodeType": "ISSUE"}]}}},
                go,
            ),
            "gql:Home": _resp(smoke, 200, {"data": {"home": {"freshness": {}}}}, go),
            "gql:Recommendations": _resp(
                smoke, 200, {"data": {"recommendations": []}}, go
            ),
            "gql:WorkItemTeamAttributions": _resp(
                smoke, 200, {"data": {"workItemTeamAttributions": []}}, go
            ),
            "gql:TestOpsRisk": _resp(
                smoke,
                200,
                {"errors": [{"message": "x"}]},
                {"X-Dev-Health-Plane": ["python"]},
            ),
            "/testops/risk": _resp(
                smoke, 200, None, text="<h1>Data service unavailable</h1>"
            ),
            "/api/auth/signout": _resp(
                smoke,
                302,
                {},
                {
                    "Location": ["https://www.commanderkeen.dev/"],
                    "Set-Cookie": ["__Secure-authjs.session-token=; Max-Age=0"],
                },
            ),
        }
        r = table[key]
        if jar is not None:
            jar.update(r.headers)
        return r

    return fake


@pytest.fixture()
def web_env(smoke: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    web = tmp_path / "web-src"
    for path, rel in smoke.REST_SOURCES.items():
        f = web / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(
            (f.read_text() if f.exists() else "") + f'"{path}"\n', encoding="utf-8"
        )
    for op, (rel, const) in smoke.GRAPHQL_SOURCES.items():
        f = web / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        doc = re.sub(r"\n\s*__typename", "", _registered(REGISTERED[op]))
        f.write_text(
            (f.read_text() if f.exists() else "")
            + f"export const {const} = `\n{doc}\n`;\n",
            encoding="utf-8",
        )
    pw = tmp_path / "pw"
    pw.write_text("synthetic-password", encoding="utf-8")
    monkeypatch.setattr(smoke, "WEB_SRC", web)
    monkeypatch.setattr(smoke, "CATALOG", CATALOG)
    monkeypatch.setattr(smoke, "ROUTING_OPS", ROUTING_OPS)
    monkeypatch.setenv("DHO_SMOKE_ADMIN_EMAIL", "smoke@example.invalid")
    monkeypatch.setenv("DHO_SMOKE_ADMIN_PASSWORD_FILE", str(pw))
    monkeypatch.setenv("DHO_SMOKE_RECEIPT_PATH", str(tmp_path / "r" / "receipt.json"))
    return tmp_path


def test_known_missing_testops_risk_gives_exit_3(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(smoke, "request", _fake_web(smoke, {}))
    assert smoke.main() == 3
    out = capsys.readouterr()
    assert "verdict=KNOWN-GAP" in out.out
    assert "CHAOS-6993" in out.out
    receipt = (web_env / "r" / "receipt.json").read_text(encoding="utf-8")
    assert (
        "synthetic-password" not in receipt and "smoke@example.invalid" not in receipt
    )


def test_python_plane_on_work_units_fails(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bad = _resp(smoke, 200, [{"id": 1}], {"X-Dev-Health-Plane": ["python"]})
    fake = _fake_web(smoke, {})

    def patched(method: str, path: str, **kw: Any) -> Any:
        if (
            path.startswith("/api/v1/work-units")
            and kw.get("jar")
            and kw["jar"].has_session()
        ):
            return bad
        return fake(method, path, **kw)

    monkeypatch.setattr(smoke, "request", patched)
    assert smoke.main() == 1
    assert "work_units_plane_python" in capsys.readouterr().err


def test_public_host_reaching_a_plane_fails(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """D2735 regression shape: an unauthenticated public-host request answered by a plane."""
    fake = _fake_web(smoke, {})

    def patched(method: str, path: str, **kw: Any) -> Any:
        if path.startswith("/api/v1/work-units") and not (
            kw.get("jar") and kw["jar"].has_session()
        ):
            return _resp(
                smoke,
                401,
                {"detail": "Not authenticated"},
                {"X-Dev-Health-Plane": ["go"]},
            )
        return fake(method, path, **kw)

    monkeypatch.setattr(smoke, "request", patched)
    assert smoke.main() == 1
    assert "public_host_unauth_not_on_web" in capsys.readouterr().err


def test_bind_address_callback_origin_fails(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """D2736 regression shape: Auth.js derives its origin from the Node bind address."""
    bad_csrf = _resp(
        smoke,
        200,
        {"csrfToken": "t"},
        {
            "Set-Cookie": [
                "__Secure-authjs.callback-url=http%3A%2F%2F%5B%3A%3A%5D%3A3000; Path=/"
            ]
        },
    )
    monkeypatch.setattr(
        smoke, "request", _fake_web(smoke, {"/api/auth/csrf": bad_csrf})
    )
    assert smoke.main() == 1
    assert "callback_url_origin=http://[::]:3000" in capsys.readouterr().err


def test_empty_node_type_fails(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rows = _resp(
        smoke,
        200,
        {
            "data": {
                "workGraphFlow": {"rows": [{"nodeType": ""}, {"nodeType": "ISSUE"}]}
            }
        },
        {"X-Dev-Health-Plane": ["go"]},
    )
    monkeypatch.setattr(smoke, "request", _fake_web(smoke, {"gql:WorkGraphFlow": rows}))
    assert smoke.main() == 1
    assert "work_graph_flow_empty_node_type=1" in capsys.readouterr().err


def test_known_missing_now_served_by_go_fails(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    served = _resp(
        smoke, 200, {"data": {"testopsRisk": {}}}, {"X-Dev-Health-Plane": ["go"]}
    )
    monkeypatch.setattr(smoke, "request", _fake_web(smoke, {"gql:TestOpsRisk": served}))
    assert smoke.main() == 1
    assert "remove the marker" in capsys.readouterr().err


def test_web_document_drift_is_fatal(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    q = web_env / "web-src" / "lib" / "graphql" / "queries.ts"
    q.write_text(
        q.read_text(encoding="utf-8").replace(
            "inflow\n", "inflow\n      extraField\n", 1
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(smoke, "request", _fake_web(smoke, {}))
    assert smoke.main() == 1
    err = capsys.readouterr().err
    assert "document_digest_mismatch=workGraphFlow" in err


def test_redirect_carrying_a_plane_header_fails(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The plane clause on its own: a 303 to sign-in that a plane (not web) sent still fails."""
    fake = _fake_web(smoke, {})

    def patched(method: str, path: str, **kw: Any) -> Any:
        if path.startswith("/api/v1/work-units") and not (
            kw.get("jar") and kw["jar"].has_session()
        ):
            return _resp(
                smoke,
                303,
                {},
                {"Location": ["/auth/signin"], "X-Dev-Health-Plane": ["go"]},
            )
        return fake(method, path, **kw)

    monkeypatch.setattr(smoke, "request", patched)
    assert smoke.main() == 1
    assert (
        "public_host_unauth_not_on_web status=303 plane=go" in capsys.readouterr().err
    )


def test_logout_to_bind_address_fails(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """D2736's user-visible symptom: sign-out redirected to http://[::]:3000/."""
    bad = _resp(
        smoke,
        302,
        {},
        {
            "Location": ["http://[::]:3000/"],
            "Set-Cookie": ["__Secure-authjs.session-token=; Max-Age=0"],
        },
    )
    monkeypatch.setattr(smoke, "request", _fake_web(smoke, {"/api/auth/signout": bad}))
    assert smoke.main() == 1
    assert "logout_redirect_origin=http://[::]:3000" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (b"syntheticPW12", "syntheticPW12"),
        (b"syntheticPW12\n", "syntheticPW12"),
        (b"syntheticPW12\r\n", "syntheticPW12"),
        (b"  syntheticPW12 \r", "syntheticPW12"),
    ],
)
def test_read_secret_file_strips_line_endings(
    smoke: ModuleType, tmp_path: Path, content: bytes, expected: str
) -> None:
    f = tmp_path / "pw"
    f.write_bytes(content)
    value, nbytes = smoke.read_secret_file(str(f))
    assert value == expected
    assert nbytes == len(content)


def test_login_failure_reports_lengths_only(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    denied = _resp(
        smoke,
        302,
        {},
        {
            "Location": [
                "https://www.commanderkeen.dev/auth/signin?error=CredentialsSignin&code=credentials"
            ]
        },
    )
    monkeypatch.setattr(
        smoke, "request", _fake_web(smoke, {"/api/auth/callback/credentials": denied})
    )
    assert smoke.main() == 1
    err = capsys.readouterr().err
    assert "error=CredentialsSignin" in err
    assert "password_file_bytes=18 password_used_len=18" in err
    assert "synthetic-password" not in err
    receipt = (web_env / "r" / "receipt.json").read_text(encoding="utf-8")
    assert "synthetic-password" not in receipt


def test_public_host_outside_allowlist_is_refused(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(smoke, "PUBLIC_HOST", "evil.example")
    assert smoke.main() == 2
    assert "public_host_not_allowed=evil.example" in capsys.readouterr().err


def test_host_header_outside_allowlist_is_refused_before_connecting(
    smoke: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened: list[object] = []
    monkeypatch.setattr(
        smoke.http.client, "HTTPConnection", lambda *a, **k: opened.append(a)
    )
    monkeypatch.setattr(smoke, "TARGET", smoke.parse_base_url("http://traefik:3000"))
    with pytest.raises(smoke.ConfigError, match="host_header_not_allowed"):
        smoke._send("GET", "/health", None, {"Host": "evil.example"})
    assert opened == []


def test_counter_counts_non_empty_distribution_entries(smoke: ModuleType) -> None:
    body = {
        "theme_distribution": {"Feature Delivery": 0.6, "Maintenance": 0.4, "Risk": 0},
        "unit": "pct",
    }
    assert smoke.count_rows(body) == 2


def test_counter_fails_loud_on_unknown_shape(smoke: ModuleType) -> None:
    with pytest.raises(
        smoke.SmokeFailure, match="unknown_response_shape keys=something,unit"
    ):
        smoke.count_rows({"something": [1], "unit": "x"})


def test_align_thread_with_distribution_body_passes(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    body = _resp(
        smoke,
        200,
        {
            "theme_distribution": {"Feature Delivery": 1.0},
            "subcategory_distribution": {},
        },
        {"X-Dev-Health-Plane": ["go"]},
    )
    monkeypatch.setattr(
        smoke, "request", _fake_web(smoke, {"/api/v1/investment": body})
    )
    assert smoke.main() == 3
    assert "rest:align" not in capsys.readouterr().err


def test_unknown_shape_on_a_thread_fails_named(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    body = _resp(smoke, 200, {"surprise": 1}, {"X-Dev-Health-Plane": ["go"]})
    monkeypatch.setattr(
        smoke, "request", _fake_web(smoke, {"/api/v1/investment": body})
    )
    assert smoke.main() == 1
    assert "rest_align_unknown_response_shape keys=surprise" in capsys.readouterr().err


def test_error_status_reported_before_shape(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    body = _resp(smoke, 500, {"detail": "boom"}, {"X-Dev-Health-Plane": ["go"]})
    monkeypatch.setattr(
        smoke, "request", _fake_web(smoke, {"/api/v1/investment": body})
    )
    assert smoke.main() == 1
    assert "rest_align_status_500" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("op", "key"),
    [
        ("home", "gql:Home"),
        ("recommendations", "gql:Recommendations"),
        ("workItemTeamAttributions", "gql:WorkItemTeamAttributions"),
    ],
)
def test_seeded_read_op_on_python_plane_fails_by_name(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    op: str,
    key: str,
) -> None:
    """D3057 shape: web sends a Go-only document, no routing row, Strawberry answers."""
    bad = _resp(
        smoke,
        200,
        {"errors": [{"message": "Cannot query field"}]},
        {"X-Dev-Health-Plane": ["python"]},
    )
    monkeypatch.setattr(smoke, "request", _fake_web(smoke, {key: bad}))
    assert smoke.main() == 1
    assert f"{op}_plane_python" in capsys.readouterr().err


def test_seeded_read_op_graphql_errors_on_go_plane_fail(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bad = _resp(
        smoke, 200, {"errors": [{"message": "x"}]}, {"X-Dev-Health-Plane": ["go"]}
    )
    monkeypatch.setattr(smoke, "request", _fake_web(smoke, {"gql:Home": bad}))
    assert smoke.main() == 1
    assert "home_graphql_errors" in capsys.readouterr().err


def test_seeded_read_op_wrong_data_shape_fails(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bad = _resp(
        smoke,
        200,
        {"data": {"recommendations": None}},
        {"X-Dev-Health-Plane": ["go"]},
    )
    monkeypatch.setattr(
        smoke, "request", _fake_web(smoke, {"gql:Recommendations": bad})
    )
    assert smoke.main() == 1
    assert "recommendations_data_shape" in capsys.readouterr().err


@pytest.mark.parametrize("home", [{}, {"freshness": None}])
def test_home_without_freshness_fails_by_name(
    smoke: ModuleType,
    web_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    home: dict[str, Any],
) -> None:
    """A Go-plane 200 with the right root type but no page data must not pass."""
    bad = _resp(smoke, 200, {"data": {"home": home}}, {"X-Dev-Health-Plane": ["go"]})
    monkeypatch.setattr(smoke, "request", _fake_web(smoke, {"gql:Home": bad}))
    assert smoke.main() == 1
    assert "home_field_missing=freshness" in capsys.readouterr().err


def test_attribution_probe_sends_exactly_one_unmatched_id(
    smoke: ModuleType, web_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty workItemIds list skips the resolver's filter and scans the whole org; a large
    list is a heavy query: the probe sends exactly one id that matches no row."""
    seen: list[Any] = []
    fake = _fake_web(smoke, {})

    def spy(method: str, path: str, **kw: Any) -> Any:
        body = kw.get("json_body")
        if body and "workItemTeamAttributions(" in body.get("query", ""):
            seen.append(body["variables"]["workItemIds"])
        return fake(method, path, **kw)

    monkeypatch.setattr(smoke, "request", spy)
    smoke.main()
    assert seen and all(ids == ["smoke-probe-no-such-item"] for ids in seen)
