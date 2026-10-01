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
        "/docs",
        "/openapi.json",
    ):
        assert _route(doc, py) == "http://api:8000", py
    # CHAOS-6263: /graphql is no longer Python's by default. With no queryApiPaths entry for
    # it, it is an unknown path (the Go default backend), never the Python api.
    assert _route(doc, "/graphql") == "http://go-api:8000"
    # CHAOS-7255: the Go public listener serves no /api/v1/internal/* route, so the default list no longer
    # carries it; the request lands on the default backend (Go's native 404).
    assert _route(doc, "/api/v1/internal/acr/health") == "http://go-api:8000"
    assert ("/api/v1/internal", "Prefix") not in gen.DEFAULT_PYTHON_ALLOW_LIST
    # CHAOS-7198: llm-settings/readiness is Go-served, no longer on the Python allow-list.
    assert _route(doc, "/api/v1/admin/llm-settings/readiness") == "http://go-api:8000"
    assert (
        "/api/v1/admin/llm-settings/readiness",
        "Exact",
    ) not in gen.DEFAULT_PYTHON_ALLOW_LIST


def test_allow_list_override_needs_no_internal_cover(gen: ModuleType) -> None:
    """CHAOS-7255: an override no longer has to carry /api/v1/internal (and may still list it)."""
    for entries in (
        [{"path": "/graphql", "pathType": "Prefix"}],
        [{"path": "/graphql$", "pathType": "ImplementationSpecific"}],
        [
            {"path": "/graphql$", "pathType": "ImplementationSpecific"},
            {"path": "/api/v1/internal", "pathType": "Prefix"},
        ],
    ):
        doc = {"ops": {"ingress": {"pythonAllowList": entries}}}
        assert gen.python_allow_list_from_doc(doc) == [
            (e["path"], e["pathType"]) for e in entries
        ]


def test_anchored_allow_list_entry_emits_one_exact_path_term(gen: ModuleType) -> None:
    """CHAOS-7243: an ImplementationSpecific `<literal>$` entry matches exactly that path in traefik too."""
    rule = gen._traefik_path_rule(
        [
            ("/graphql$", "ImplementationSpecific"),
            ("/openapi\\.json$", "ImplementationSpecific"),
            ("/api/v1/internal", "Prefix"),
        ]
    )
    assert "Path(`/graphql`)" in rule
    assert "PathPrefix(`/graphql/`)" not in rule
    assert "Path(`/openapi.json`)" in rule
    assert "$" not in rule and "\\" not in rule
    assert "PathPrefix(`/api/v1/internal/`)" in rule


def test_default_allow_list_no_longer_carries_graphql(gen: ModuleType) -> None:
    """CHAOS-6263: query-api answers /graphql, so the default allow-list is empty."""
    assert gen.DEFAULT_PYTHON_ALLOW_LIST == []


def _graphql_values(allow: list[dict] | None) -> dict:
    doc = yaml.safe_load(yaml.safe_dump(VALUES))
    doc["ingress"]["queryApiPaths"].append({"path": "/graphql", "pathType": "Exact"})
    if allow is not None:
        doc["ops"] = {"ingress": {"pythonAllowList": allow}}
    return doc


def test_graphql_in_query_api_paths_routes_to_query_api(gen: ModuleType) -> None:
    """CHAOS-6263: the flip is one queryApiPaths entry: /graphql reaches query-api, exactly."""
    doc = _graphql_values(None)
    go_paths, query_paths = gen.paths_from_doc(doc)
    go_regex, query_regex = (
        gen.combined_regex(go_paths),
        gen.combined_regex(query_paths),
    )
    allow = gen.python_allow_list_from_doc(doc)
    assert gen.paths_on_two_planes(doc) == []
    routed = yaml.safe_load(gen.emit_dynamic_config(go_regex, query_regex, allow))
    assert _route(routed, "/graphql") == "http://query-api:8090"
    for lookalike in ("/graphqlx", "/graphql/", "/graphql/x"):
        assert _route(routed, lookalike) == "http://go-api:8000", lookalike


@pytest.mark.parametrize(
    "allow",
    [
        [{"path": "/graphql$", "pathType": "ImplementationSpecific"}],
        [{"path": "/graphql", "pathType": "Exact"}],
        [{"path": "/graphql", "pathType": "Prefix"}],
        [
            {"path": "/api/v1/internal", "pathType": "Prefix"},
            {"path": "/graphql$", "pathType": "ImplementationSpecific"},
        ],
    ],
)
def test_a_path_on_the_allow_list_and_a_go_plane_is_refused(
    gen: ModuleType, tmp_path: Path, allow: list[dict], capsys: pytest.CaptureFixture
) -> None:
    """One path on two backends is a tie-break, not a route: the generator refuses (exit 4)
    and names the path, so a flip that forgets the allow-list cannot be proven on bigboy."""
    values = tmp_path / "values.prod.yaml"
    values.write_text(yaml.safe_dump(_graphql_values(allow)))
    assert gen.main([str(values), "--format", "dynamic"]) == 4
    captured = capsys.readouterr()
    assert captured.out == "" and "REFUSED: one path, two backends" in captured.err
    assert "ops.ingress.pythonAllowList entry {path: /graphql" in captured.err
    assert "and ingress.queryApiPaths entry /graphql" in captured.err


def test_an_allow_list_without_the_moved_path_is_accepted(
    gen: ModuleType, tmp_path: Path
) -> None:
    values = tmp_path / "values.prod.yaml"
    values.write_text(
        yaml.safe_dump(
            _graphql_values(
                [{"path": "/metrics$", "pathType": "ImplementationSpecific"}]
            )
        )
    )
    assert gen.main([str(values), "--format", "dynamic"]) == 0


def _values(
    *,
    go: list[str] | None = None,
    query: list[tuple[str, str]] | None = None,
    allow: list[tuple[str, str]] | None = None,
    hosts: list[dict] | None = None,
) -> dict:
    """VALUES plus extra Go plane paths, a shared allow-list and per-host settings."""
    doc = yaml.safe_load(yaml.safe_dump(VALUES))
    doc["ingress"]["goApiPaths"] += [{"path": p} for p in go or []]
    doc["ingress"]["queryApiPaths"] += [
        {"path": p, "pathType": t} for p, t in query or []
    ]
    ops_ingress: dict = {}
    if allow is not None:
        ops_ingress["pythonAllowList"] = [{"path": p, "pathType": t} for p, t in allow]
    if hosts is not None:
        ops_ingress["hosts"] = hosts
    if ops_ingress:
        doc["ops"] = {"ingress": ops_ingress}
    return doc


def _run(gen: ModuleType, tmp_path: Path, doc: dict) -> int:
    values = tmp_path / "values.prod.yaml"
    values.write_text(yaml.safe_dump(doc), encoding="utf-8")
    return gen.main([str(values), "--format", "dynamic"])


def _planes_matching(routed: dict, path: str) -> set[str]:
    """The services whose own path rule matches `path` (the catch-all is not a path rule)."""
    hit = set()
    for name, router in routed["http"]["routers"].items():
        rule = router["rule"]
        m = re.search(r"PathRegexp\(`([^`]+)`\)", rule)
        if m:
            if re.match(m.group(1), path):
                hit.add(name)
        elif "&&" in rule:
            terms = re.findall(r"(Path|PathPrefix)\(`([^`]+)`\)", rule)
            if any(
                (k == "Path" and path == v)
                or (k == "PathPrefix" and path.startswith(v))
                for k, v in terms
            ):
                hit.add(name)
    return hit


# Each row: what the values add, and one request path BOTH generated rules match (None when
# only prod's ingress would see the overlap: a host's own list, or a Prefix that ingress-nginx
# reads as "starts with" on a host in regex mode).
OVERLAPS = {
    "a Prefix over a Go path under it": (
        {"query": [("/graphql/sub", "Exact")], "allow": [("/graphql", "Prefix")]},
        "/graphql/sub",
    ),
    "a Prefix over a deeper Go path": (
        {"go": ["/graphql/a/b"], "allow": [("/graphql", "Prefix")]},
        "/graphql/a/b",
    ),
    "a Prefix over a Go path with a wildcard under it": (
        {"allow": [("/api/v1/people", "Prefix")]},
        "/api/v1/people/p1/metric",
    ),
    "a Prefix that ends inside a wildcard segment": (
        {"allow": [("/api/v1/people/p1", "Prefix")]},
        "/api/v1/people/p1/metric",
    ),
    "a Prefix with a trailing slash": (
        {"query": [("/graphql/sub", "Exact")], "allow": [("/graphql/", "Prefix")]},
        "/graphql/sub",
    ),
    "a Prefix over the same path": (
        {"query": [("/graphql", "Exact")], "allow": [("/graphql", "Prefix")]},
        "/graphql",
    ),
    "a Prefix read as starts-with (regex mode in prod)": (
        {"query": [("/graphqlx", "Exact")], "allow": [("/graphql", "Prefix")]},
        None,
    ),
    "an Exact entry inside a wildcard path": (
        {"allow": [("/api/v1/people/p1/metric", "Exact")]},
        "/api/v1/people/p1/metric",
    ),
    "an anchored entry inside a {param} path": (
        {"allow": [("/api/v1/external-ingest/schemas/s1$", "ImplementationSpecific")]},
        "/api/v1/external-ingest/schemas/s1",
    ),
    "a bigboy-local Exact path a Go plane claims": (
        {"query": [("/metrics", "Exact")]},
        "/metrics",
    ),
    "a bigboy-local Prefix path a Go plane reaches under": (
        {"go": ["/docs/{page}"]},
        "/docs/intro",
    ),
    "a host's own list, with the shared list clean": (
        {
            "query": [("/graphql", "Exact")],
            "allow": [],
            "hosts": [
                {"host": "public.example", "pythonAllowList": True},
                {
                    "host": "in-cluster.example",
                    "pythonAllowList": [
                        {"path": "/graphql$", "pathType": "ImplementationSpecific"}
                    ],
                },
            ],
        },
        None,
    ),
}


@pytest.mark.parametrize("case", sorted(OVERLAPS))
def test_every_overlap_of_a_python_path_and_a_go_plane_path_is_refused(
    gen: ModuleType, tmp_path: Path, case: str, capsys: pytest.CaptureFixture
) -> None:
    """The guard decides on the paths the rules MATCH, not on the text of an entry: a Prefix
    reaches under itself, a wildcard reaches an Exact entry, the local-only paths and every
    host's own list are Python paths too. Each row is refused (exit 4, nothing emitted)."""
    spec, both_match = OVERLAPS[case]
    doc = _values(**spec)
    assert _run(gen, tmp_path, doc) == 4, case
    captured = capsys.readouterr()
    assert captured.out == "" and "REFUSED: one path, two backends" in captured.err
    if both_match is None:
        return
    # The refusal is not about text: the two generated rules really match one request.
    go_paths, query_paths = gen.paths_from_doc(doc)
    routed = yaml.safe_load(
        gen.emit_dynamic_config(
            gen.combined_regex(go_paths),
            gen.combined_regex(query_paths),
            gen.python_allow_list_from_doc(doc),
        )
    )
    planes = _planes_matching(routed, both_match)
    assert "python-allowlist" in planes and planes & {
        "go-api-paths",
        "query-api-paths",
    }, (case, planes)


def test_a_refusal_names_the_list_the_entry_and_the_go_plane_path(
    gen: ModuleType, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    spec, _ = OVERLAPS["a bigboy-local Exact path a Go plane claims"]
    assert _run(gen, tmp_path, _values(**spec)) == 4
    err = capsys.readouterr().err
    assert (
        f"{gen.BIGBOY_LOCAL_SOURCE} entry {{path: /metrics, pathType: Exact}}"
        " and ingress.queryApiPaths entry /metrics"
    ) in err
    spec, _ = OVERLAPS["a host's own list, with the shared list clean"]
    assert _run(gen, tmp_path, _values(**spec)) == 4
    err = capsys.readouterr().err
    assert (
        "ops.ingress.hosts[in-cluster.example].pythonAllowList entry"
        " {path: /graphql$, pathType: ImplementationSpecific} and ingress.queryApiPaths entry /graphql"
    ) in err
    assert (
        "public.example" not in err and "ops.ingress.pythonAllowList entry" not in err
    )
    spec, _ = OVERLAPS["a Prefix over a deeper Go path"]
    assert _run(gen, tmp_path, _values(**spec)) == 4
    assert (
        "ops.ingress.pythonAllowList entry {path: /graphql, pathType: Prefix}"
        " and ingress.goApiPaths entry /graphql/a/b"
    ) in capsys.readouterr().err


DISJOINT = {
    "a Prefix beside an unrelated Go path": {"allow": [("/api/v1/internal", "Prefix")]},
    "a Prefix longer than the Go path": {
        "query": [("/graphql", "Exact")],
        "allow": [("/graphql/sub", "Prefix")],
    },
    "a Prefix whose last segment a Go literal does not start with": {
        "query": [("/graphq", "Exact")],
        "allow": [("/graphql", "Prefix")],
    },
    "a Prefix that differs in an earlier segment": {
        "allow": [("/api/v2/work", "Prefix")]
    },
    "an Exact entry longer than a Go literal it starts with": {
        "query": [("/graphq", "Exact")],
        "allow": [("/graphql", "Exact")],
    },
    "an Exact entry one segment short of a wildcard path": {
        "allow": [("/api/v1/people/p1", "Exact")]
    },
    "an Exact entry one segment past a Go path": {
        "allow": [("/api/v1/work-units/extra", "Exact")]
    },
    "an anchored entry beside a look-alike Go path": {
        "query": [("/graphqlx", "Exact")],
        "allow": [("/graphql$", "ImplementationSpecific")],
    },
    "a wildcard is not an empty segment": {
        "allow": [("/api/v1/people//metric", "Exact")]
    },
    "the flip done on every list": {
        "query": [("/graphql", "Exact")],
        "allow": [("/api/v1/internal", "Prefix")],
        "hosts": [
            {"host": "public.example", "pythonAllowList": True},
            {
                "host": "in-cluster.example",
                "pythonAllowList": [
                    {"path": "/metrics$", "pathType": "ImplementationSpecific"}
                ],
            },
        ],
    },
}

SAMPLE_PATHS = [
    "/graphql",
    "/graphql/",
    "/graphql/sub",
    "/graphqlx",
    "/graphq",
    "/metrics",
    "/docs",
    "/docs/intro",
    "/redoc",
    "/openapi.json",
    "/health",
    "/api/v1/internal",
    "/api/v1/internal/acr/health",
    "/api/v1/work-units",
    "/api/v1/work-units/extra",
    "/api/v1/people/p1",
    "/api/v1/people/p1/metric",
    "/api/v1/people//metric",
    "/api/v1/external-ingest/schemas",
    "/api/v1/external-ingest/schemas/s1",
]


@pytest.mark.parametrize("case", sorted(DISJOINT))
def test_disjoint_python_and_go_paths_are_accepted_and_no_request_matches_both(
    gen: ModuleType, tmp_path: Path, case: str
) -> None:
    """The guard does not refuse what does not overlap, and for an accepted values file no
    sample request is matched by the Python rule and a Go plane rule together."""
    doc = _values(**DISJOINT[case])
    assert gen.paths_on_two_planes(doc) == [], case
    assert _run(gen, tmp_path, doc) == 0, case
    go_paths, query_paths = gen.paths_from_doc(doc)
    routed = yaml.safe_load(
        gen.emit_dynamic_config(
            gen.combined_regex(go_paths),
            gen.combined_regex(query_paths),
            gen.python_allow_list_from_doc(doc),
        )
    )
    for path in SAMPLE_PATHS:
        planes = _planes_matching(routed, path)
        assert not (
            "python-allowlist" in planes
            and planes & {"go-api-paths", "query-api-paths"}
        ), (case, path, planes)


def test_the_check_and_the_emitted_regex_agree_on_every_sample_path(
    gen: ModuleType,
) -> None:
    """The check and the emitted PathRegexp are derived from one form of a Go plane path. If
    they drift, the check decides about paths the rule does not match: for every Go plane path
    and every sample request, "an Exact Python entry for this request overlaps" is exactly
    "the emitted regex matches this request"."""
    doc = _values(go=["/docs/{page}", "/a.b/{x}-{y}/c"], query=[("/metrics", "Exact")])
    claimed = gen.go_plane_paths(doc)
    go_paths, query_paths = gen.paths_from_doc(doc)
    assert [gen._segments_regex(segments) for _, _, segments in claimed] == (
        go_paths + query_paths
    )
    compared = 0
    for _, _, segments in claimed:
        emitted = re.compile(gen.combined_regex([gen._segments_regex(segments)]))
        for path in SAMPLE_PATHS + [
            "/a.b/1-2/c",
            "/aXb/1-2/c",
            "/a.b/1-/c",
            "/a.b/12/c",
        ]:
            assert gen.python_entry_overlaps(path, "Exact", segments) == bool(
                emitted.match(path)
            ), (segments, path)
            compared += 1
    assert compared == len(claimed) * (len(SAMPLE_PATHS) + 4) and compared > 100


def test_the_check_reads_the_entries_the_python_rule_is_generated_from(
    gen: ModuleType,
) -> None:
    """The Python rule is generated from the shared list plus the local-only paths; the check
    reads those same entries (and each host's own list, which only prod routes)."""
    doc = _values(
        allow=[("/api/v1/internal", "Prefix")],
        hosts=[
            {"host": "h1", "pythonAllowList": True},
            {"host": "h2"},
            {
                "host": "h3",
                "pythonAllowList": [{"path": "/x", "pathType": "Exact"}],
            },
        ],
    )
    sources = gen.python_sources(doc)
    assert [name for name, _ in sources] == [
        "ops.ingress.pythonAllowList",
        "ops.ingress.hosts[h3].pythonAllowList",
        gen.BIGBOY_LOCAL_SOURCE,
    ]
    allow = gen.python_allow_list_from_doc(doc)
    assert sources[0][1] + sources[-1][1] == gen.routed_python_entries(allow)
    assert sources[1][1] == [("/x", "Exact")]
    rule = gen._traefik_path_rule(gen.routed_python_entries(allow))
    go_paths, query_paths = gen.paths_from_doc(doc)
    emitted = gen.emit_dynamic_config(
        gen.combined_regex(go_paths), gen.combined_regex(query_paths), allow
    )
    assert f'rule: "{gen.HOST_RULE} && {rule}"' in emitted
    # With no shared list in the values the default list is what both read.
    assert gen.python_sources(_values())[0][1] == gen.DEFAULT_PYTHON_ALLOW_LIST
    assert gen.routed_python_entries(None) == gen.DEFAULT_PYTHON_ALLOW_LIST + list(
        gen.BIGBOY_LOCAL_PYTHON_PATHS
    )


@pytest.mark.parametrize(
    "entry",
    [
        ("/graphql", "ImplementationSpecific"),
        ("/graphql.*$", "ImplementationSpecific"),
        ("/graphql$$", "ImplementationSpecific"),
        ("/api/v1/[^/]+$", "ImplementationSpecific"),
        ("/graphql", "Regex"),
    ],
)
def test_an_entry_of_a_shape_the_chart_does_not_accept_is_refused(
    gen: ModuleType,
    tmp_path: Path,
    entry: tuple[str, str],
    capsys: pytest.CaptureFixture,
) -> None:
    """An entry whose match set this generator cannot state is not guessed at: the ops chart
    refuses the same shapes, and here it is refused by name instead of being read as a literal."""
    assert _run(gen, tmp_path, _values(allow=[entry])) == 4
    err = capsys.readouterr().err
    assert (
        f"{{path: {entry[0]}, pathType: {entry[1]}}} is not a shape the ops chart accepts"
        in err
    )


def test_a_dot_is_a_literal_dot_on_both_sides(gen: ModuleType) -> None:
    """An anchored entry writes a dot as `\\.`; a Go plane path writes it plain. Both mean the
    one character, never "any character"."""
    dotted = gen._queryapi_entry_segments("/openapi.json", "Exact")
    assert gen.python_entry_overlaps(
        "/openapi\\.json$", "ImplementationSpecific", dotted
    )
    assert gen.python_entry_overlaps("/openapi.json", "Exact", dotted)
    assert not gen.python_entry_overlaps("/openapiXjson", "Exact", dotted)
    assert not gen.python_entry_overlaps(
        "/openapi\\.json$",
        "ImplementationSpecific",
        gen._queryapi_entry_segments("/openapiXjson", "Exact"),
    )


def test_an_exact_go_plane_path_is_literal_even_where_it_looks_like_a_wildcard(
    gen: ModuleType,
) -> None:
    """Only an ImplementationSpecific entry carries a wildcard. An Exact path is its own text."""
    exact = gen._queryapi_entry_to_regex("/lit/[^/]+", "Exact")
    assert re.fullmatch(exact, "/lit/[^/]+") and not re.fullmatch(exact, "/lit/x")
    segments = gen._queryapi_entry_segments("/lit/[^/]+", "Exact")
    assert gen.python_entry_overlaps("/lit/[^/]+", "Exact", segments)
    assert not gen.python_entry_overlaps("/lit/x", "Exact", segments)
    wild = gen._queryapi_entry_segments("/lit/[^/]+", "ImplementationSpecific")
    assert gen.python_entry_overlaps("/lit/x", "Exact", wild)
