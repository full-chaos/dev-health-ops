"""CHAOS-6987 (D2735): bigboy's plane-split router rules match Host(`traefik`) ONLY.

Traefik on bigboy has ONE entrypoint (:3000) shared by browser-origin traffic and
web's own server-side BACKEND_URL traffic, so the Host() clause is the only thing
that keeps a browser request on `web` (whose proxy.ts turns the session cookie
into a bearer). The first generated router also matched the public hostnames;
browser XHRs for the split paths then skipped web, reached the Go planes with no
Authorization header, and got 401 "Not authenticated". These tests parse every
rule the generator can emit (dynamic file config and docker labels) plus the
checked-in example, and fail if any Host other than `traefik` appears, or if any
rule can match without the Host clause.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
GENERATOR = ROOT / "ci" / "bigboy" / "generate-plane-split-router.py"
EXAMPLE = ROOT / "ci" / "bigboy" / "traefik-dynamic-planes.yml.example"

VALUES = {
    "ingress": {
        "goApiPaths": [
            {"path": "/api/v1/external-ingest/schemas"},
            {"path": "/api/v1/external-ingest/schemas/{schema_id}"},
            {"path": "/health"},
        ],
        "queryApiPaths": [
            {"path": "/api/v1/work-units", "pathType": "Exact"},
            {
                "path": "/api/v1/people/[^/]+/metric",
                "pathType": "ImplementationSpecific",
            },
        ],
    }
}

HOST_CALL = re.compile(
    r"\b(Host|HostRegexp|HostSNI|HostSNIRegexp|ClientIP|Header|HeaderRegexp)\(`([^`]*)`\)"
)
PATH_CALL = re.compile(r"PathRegexp\(`[^`]*`\)")


def _load_generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "generate_plane_split_router", GENERATOR
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def gen() -> ModuleType:
    return _load_generator()


@pytest.fixture()
def values_file(tmp_path: Path) -> Path:
    p = tmp_path / "values.prod.yaml"
    p.write_text(yaml.safe_dump(VALUES), encoding="utf-8")
    return p


def assert_internal_only(name: str, rule: str) -> None:
    """A rule is safe only if it is `Host(`traefik`)` or `Host(`traefik`) && PathRegexp(...)`."""
    matchers = HOST_CALL.findall(rule)
    assert matchers, f"{name}: rule has no Host() clause at all: {rule[:120]}"
    assert matchers == [("Host", "traefik")], (
        f"{name}: rule must name Host(`traefik`) only, got {matchers}"
    )
    # Remove the path regex body (it legitimately contains `|`), then the shape must be exact.
    skeleton = PATH_CALL.sub("PathRegexp(P)", rule).strip()
    # CHAOS-7047: the Python allow-list router ANDs the Host clause with a parenthesised
    # group of literal Path()/PathPrefix() terms.
    skeleton = re.sub(
        r"\((?:Path|PathPrefix)\(`[^`]+`\)(?: \|\| (?:Path|PathPrefix)\(`[^`]+`\))*\)",
        "(PATHS)",
        skeleton,
    )
    assert skeleton in {
        "Host(`traefik`)",
        "Host(`traefik`) && PathRegexp(P)",
        "Host(`traefik`) && (PATHS)",
    }, f"{name}: rule can match without the internal Host clause: {skeleton}"


def test_dynamic_config_rules_are_internal_only(
    gen: ModuleType, values_file: Path
) -> None:
    go_paths, query_paths = gen.load_paths(str(values_file))
    doc = yaml.safe_load(
        gen.emit_dynamic_config(
            gen.combined_regex(go_paths), gen.combined_regex(query_paths)
        )
    )
    routers = doc["http"]["routers"]
    assert set(routers) == {
        "go-api-paths",
        "query-api-paths",
        "python-allowlist",
        "api-internal-catchall",
    }
    for name, router in routers.items():
        assert_internal_only(name, router["rule"])
        assert router["entryPoints"] == ["web"]


def test_label_rules_are_internal_only(gen: ModuleType, values_file: Path) -> None:
    go_paths, query_paths = gen.load_paths(str(values_file))
    text = gen.emit_labels(
        gen.combined_regex(go_paths), gen.combined_regex(query_paths)
    )
    rules = re.findall(
        r'traefik\.http\.routers\.([\w-]+)\.rule: "(.*)"$', text, flags=re.M
    )
    assert {n for n, _ in rules} == {"go-api-paths", "query-api-paths"}
    for name, rule in rules:
        assert_internal_only(name, rule)


def test_checked_in_example_is_internal_only() -> None:
    doc = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
    routers = doc["http"]["routers"]
    assert routers, "example has no routers"
    for name, router in routers.items():
        assert_internal_only(name, router["rule"])


def test_generated_paths_cover_the_values(gen: ModuleType, values_file: Path) -> None:
    go_paths, query_paths = gen.load_paths(str(values_file))
    go = re.compile(gen.combined_regex(go_paths))
    query = re.compile(gen.combined_regex(query_paths))
    assert go.match("/api/v1/external-ingest/schemas/abc")
    assert not go.match("/api/v1/external-ingest/schemas/abc/def")
    assert query.match("/api/v1/work-units")
    assert not query.match("/api/v1/work-units/extra")
    assert query.match("/api/v1/people/p1/metric")


@pytest.mark.parametrize(
    "bad_rule",
    [
        "Host(`www.commanderkeen.dev`) || Host(`traefik`) && PathRegexp(`^(/a)$`)",
        "Host(`commanderkeen.dev`) && PathRegexp(`^(/a)$`)",
        "PathRegexp(`^(/a)$`)",
        "Host(`traefik`) || PathRegexp(`^(/a)$`)",
        "HostRegexp(`.+`) && PathRegexp(`^(/a)$`)",
    ],
)
def test_checker_rejects_public_or_hostless_rules(bad_rule: str) -> None:
    """The checker itself must fail on each D2735-class shape (guard observed failing)."""
    with pytest.raises(AssertionError):
        assert_internal_only("planted", bad_rule)


def _route(doc: dict, path: str) -> str:
    """Resolve `path` on Host(`traefik`) the way traefik does: highest priority matching
    router wins. Understands only the rule forms the generator emits."""
    best = None
    for router in doc["http"]["routers"].values():
        rule = router["rule"]
        m = re.search(r"PathRegexp\(`([^`]+)`\)", rule)
        if m:
            ok = re.match(m.group(1), path) is not None
        elif "&&" in rule:
            terms = re.findall(r"(Path|PathPrefix)\(`([^`]+)`\)", rule)
            ok = any(
                (k == "Path" and path == v)
                or (k == "PathPrefix" and path.startswith(v))
                for k, v in terms
            )
        else:
            ok = True
        if ok and (best is None or router["priority"] > best["priority"]):
            best = router
    assert best is not None
    return doc["http"]["services"][best["service"]]["loadBalancer"]["servers"][0]["url"]


def test_default_backend_is_go_and_allow_list_stays_python(
    gen: ModuleType, values_file: Path
) -> None:
    """CHAOS-7047: an unknown path reaches the Go api; only the allow-list reaches Python."""
    go_paths, query_paths = gen.load_paths(str(values_file))
    doc = yaml.safe_load(
        gen.emit_dynamic_config(
            gen.combined_regex(go_paths), gen.combined_regex(query_paths)
        )
    )
    assert _route(doc, "/no/such/path") == "http://go-api:8000"
    assert _route(doc, "/graphqlx") == "http://go-api:8000"
    assert _route(doc, "/api/v1/work-units") == "http://query-api:8090"
    assert _route(doc, "/health") == "http://go-api:8000"
    for py in (
        "/graphql",
        "/api/v1/admin/llm-settings/readiness",
        "/api/v1/internal/acr/health",
        "/docs",
        "/openapi.json",
    ):
        assert _route(doc, py) == "http://api:8000", py
    assert _route(doc, "/api/v1/admin/llm-settings/readiness/x") == "http://go-api:8000"


def test_allow_list_override_must_cover_internal(gen: ModuleType) -> None:
    doc = {
        "ops": {
            "ingress": {"pythonAllowList": [{"path": "/graphql", "pathType": "Prefix"}]}
        }
    }
    with pytest.raises(SystemExit):
        gen.python_allow_list_from_doc(doc)


@pytest.mark.parametrize(
    ("path", "path_type"),
    [
        ("/api/v1/int", "Prefix"),
        ("/api/v1/internal-x", "Prefix"),
        ("/api/v1/internals", "Prefix"),
        ("/api/v1/internal", "Exact"),
    ],
)
def test_allow_list_cover_is_segment_exact(
    gen: ModuleType, path: str, path_type: str
) -> None:
    doc = {
        "ops": {"ingress": {"pythonAllowList": [{"path": path, "pathType": path_type}]}}
    }
    with pytest.raises(SystemExit):
        gen.python_allow_list_from_doc(doc)
