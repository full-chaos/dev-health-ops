#!/usr/bin/env python3
"""CHAOS-6987 (D2724): derive bigboy's traefik plane-split router labels from deploy's own
values.prod.yaml ingress.goApiPaths + ingress.queryApiPaths -- never hand-maintained, so bigboy's
router cannot drift silently from what prod's ingress actually does (same class as CHAOS-6967(b)'s
generate-query-api-enabled-flags.py).

Bigboy has no k8s Ingress; traefik (already in compose.yml, providers.docker=true, label-based)
is the router. web's own labels give it a catch-all Host() rule with default (low) priority --
this script emits HIGHER-priority Host()+PathRegexp() router labels for go-api and query-api so
matching paths reach those services first; everything else still falls through to web's rule
(web then calls its BACKEND_URL, this router, on the internal host name).

Path syntax: prod's goApiPaths use {param} whole-segment templates (never a hand-written regex,
the k8s Ingress controller/chart expands and anchors them); queryApiPaths use pathType Exact
(literal path) or ImplementationSpecific (an inline [^/]+ placeholder already regex-shaped). Both
are converted here to one traefik PathRegexp per plane: {param} -> [^/]+, Exact paths escaped and
used literally, ImplementationSpecific paths used as-is (already regex).

An entry of the ops chart's allow-list (ops.ingress.pythonAllowList) goes to the Python api,
unless it names its backend with `service: query-api`: prod changes the backend of a path that
way INSIDE the Ingress object that holds it (ingress-nginx's admission denies a path that moves
to another live object). Such an entry is a query-api path of this router.

Case (CHAOS-7925): an ANCHORED allow-list entry (`/literal$`) is matched case-insensitively, as ingress-nginx
does on the host that carries it (regex mode, `~*`): /GRAPHQL goes where /graphql goes. Every other path of this
router is case-sensitive.

Usage:
  generate-plane-split-router.py <values.prod.yaml> [--format labels|dynamic]
  generate-plane-split-router.py --deploy-repo <deploy repo> --deploy-sha <sha> --expect-ops-sha <ops sha>
                                 [--format labels|dynamic]
  labels (default): the docker-compose label lines for go-api's and query-api's services.
  dynamic: the traefik file-provider config (what bigboy loads from .traefik-dynamic/planes.yml).

Pinned mode (R467): the values come from `values.prod.yaml` AT --deploy-sha, never a working tree
or deploy main, and the run REFUSES (exit 3) unless that deploy commit's vendor/dev-health-ops pin
equals --expect-ops-sha (the build bigboy is actually running). Deploy main can route paths to Go
handlers the running build does not have yet; generating from it would send traffic to a 404.
"""

from __future__ import annotations

import re
import subprocess
import sys

import yaml

# `traefik` ONLY -- NEVER commanderkeen.dev/www.commanderkeen.dev here (D2733 regression,
# CHAOS-6987 follow-up: an earlier version of this rule matched the public host too, which let
# ANY real browser request for one of these paths bypass `web` (and its proxy.ts middleware --
# the ONLY place that turns the NextAuth session cookie into an Authorization: Bearer header)
# and land directly on go-api/query-api with NO auth carrier at all. Confirmed live: a bare
# request straight to traefik with Host: commanderkeen.dev and no Authorization header 401'd
# identically on /api/v1/work-units, /api/v1/investment and /api/v1/home alike -- proving it was
# never route-specific, just whichever page's data happened to be fetched client-side (apiClient
# deliberately omits Authorization in the browser, relying entirely on proxy.ts) versus
# server-side (apiClient DOES attach it there, so that path never needed proxy.ts at all).
#
# `traefik` is web's own server-side BACKEND_URL target (Next.js's rewrite proxy sets the
# outgoing Host header to the TARGET url's own hostname) -- those calls already carry a real
# Authorization header from apiClient itself, so routing them directly here is safe. A real
# browser request on commanderkeen.dev must ALWAYS land on `web`'s own pre-existing docker-label
# router first (no Path restriction, so it still catches every /api/v1/* path these two routers
# don't grab), which runs proxy.ts and re-issues the request server-side via BACKEND_URL --
# arriving back here as Host: traefik, now correctly authenticated.
HOST_RULE = "Host(`traefik`)"


def _unescape_hyphen(escaped: str) -> str:
    # re.escape() escapes "-" even though it has no special meaning outside a character class;
    # the literal backslash-hyphen it produces breaks YAML double-quoted scalars downstream (\-
    # isn't a valid YAML escape) for no regex benefit -- strip it back to a plain "-".
    return escaped.replace("\\-", "-")


# A Go plane path is literal text plus wildcards that each stand for one or more characters of
# ONE segment (never a "/"). That is the whole language: a goApiPaths {param}, a queryApiPaths
# ImplementationSpecific `[^/]+`. A path is kept as its segments, each segment as the literal
# parts around its wildcards, and BOTH the emitted regex and the two-backends check are derived
# from that one form, so the check decides on exactly the paths the emitted rule matches.
_WILDCARD = "\x00"
_WILDCARD_REGEX = "[^/]+"
Segments = list[list[str]]


def _segments(text: str) -> Segments:
    return [segment.split(_WILDCARD) for segment in text.split("/")]


def _goapi_path_segments(path: str) -> Segments:
    return _segments(re.sub(r"\{[a-zA-Z0-9_]+\}", _WILDCARD, path))


def _queryapi_entry_segments(path: str, path_type: str) -> Segments:
    if path_type == "Exact":
        return _segments(path)
    # ImplementationSpecific: literal text with the `[^/]+` placeholder inline (e.g.
    # "/api/v1/people/[^/]+/metric"); everything except that placeholder is literal.
    return _segments(path.replace(_WILDCARD_REGEX, _WILDCARD))


def _segments_regex(segments: Segments) -> str:
    return "/".join(
        _WILDCARD_REGEX.join(_unescape_hyphen(re.escape(part)) for part in segment)
        for segment in segments
    )


def _goapi_path_to_regex(path: str) -> str:
    return _segments_regex(_goapi_path_segments(path))


def _queryapi_entry_to_regex(path: str, path_type: str) -> str:
    return _segments_regex(_queryapi_entry_segments(path, path_type))


class PinError(Exception):
    """The pinned deploy sha cannot be used for the running build (message names both shas)."""


def _git(repo: str, *args: str) -> str:
    res = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True)
    if res.returncode != 0:
        raise PinError(
            f"git {' '.join(args[:2])} failed in {repo}: {res.stderr.strip()[:200]}"
        )
    return res.stdout


def values_at_sha(
    repo: str, deploy_sha: str, expect_ops_sha: str
) -> tuple[str, str, str]:
    """(full deploy sha, vendored ops sha, values.prod.yaml text) -- refuses a vendor pin mismatch."""
    full = _git(repo, "rev-parse", "--verify", f"{deploy_sha}^{{commit}}").strip()
    tree = _git(repo, "ls-tree", full, "vendor/dev-health-ops").split()
    if len(tree) < 3 or tree[1] != "commit":
        raise PinError(f"deploy {full[:12]} has no vendor/dev-health-ops gitlink")
    vendored = tree[2]
    want = expect_ops_sha.strip().lower()
    if (
        len(want) < 7
        or not re.fullmatch(r"[0-9a-f]+", want)
        or not vendored.startswith(want)
    ):
        raise PinError(
            f"deploy {full[:12]} vendors ops {vendored[:12]}, but the running build is {want[:12] or '<empty>'}"
            " -- refusing to route paths the running build may not serve (R467)"
        )
    return full, vendored, _git(repo, "show", f"{full}:values.prod.yaml")


def load_paths(values_path: str) -> tuple[list[str], list[str]]:
    with open(values_path) as f:
        return paths_from_doc(yaml.safe_load(f))


def paths_from_doc(doc: dict) -> tuple[list[str], list[str]]:
    ingress = doc["ingress"]
    go_paths = [_goapi_path_to_regex(e["path"]) for e in ingress["goApiPaths"]]
    query_paths = [
        _queryapi_entry_to_regex(e["path"], e["pathType"])
        for e in ingress["queryApiPaths"]
    ]
    # An allow-list entry that names query-api (`service: query-api`) is a query-api path of
    # this router too, from whichever list in use it is on. main refuses values in which the
    # hosts do not agree on such a path (query_api_paths_that_differ_by_host), so the lists
    # name the same paths and each is routed once.
    for _, _, _, segments in query_api_allow_list_entries(doc):
        # CHAOS-7925: an anchored entry matches case-insensitively on the host that carries it (regex mode).
        if (regex := f"(?i:{_segments_regex(segments)})") not in query_paths:
            query_paths.append(regex)
    if not go_paths:
        raise SystemExit(
            "generate-plane-split-router: found ZERO goApiPaths entries -- refusing to emit an empty router (values.prod.yaml shape may have changed)"
        )
    if not query_paths:
        raise SystemExit(
            "generate-plane-split-router: found ZERO queryApiPaths entries -- refusing to emit an empty router (values.prod.yaml shape may have changed)"
        )
    return go_paths, query_paths


def combined_regex(paths: list[str]) -> str:
    return "^(" + "|".join(paths) + ")$"


def emit_labels(go_regex: str, query_regex: str) -> str:
    lines = [
        "      # go-api service labels (add to its compose block):",
        '      traefik.enable: "true"',
        "      traefik.scope: dev-health",
        f'      traefik.http.routers.go-api-paths.rule: "{HOST_RULE} && PathRegexp(`{go_regex}`)"',
        "      traefik.http.routers.go-api-paths.entrypoints: web",
        '      traefik.http.routers.go-api-paths.priority: "1000"',
        '      traefik.http.services.go-api-paths.loadbalancer.server.port: "8000"',
        "",
        "      # query-api service labels (add to its compose block):",
        '      traefik.enable: "true"',
        "      traefik.scope: dev-health",
        f'      traefik.http.routers.query-api-paths.rule: "{HOST_RULE} && PathRegexp(`{query_regex}`)"',
        "      traefik.http.routers.query-api-paths.entrypoints: web",
        '      traefik.http.routers.query-api-paths.priority: "1000"',
        '      traefik.http.services.query-api-paths.loadbalancer.server.port: "8090"',
    ]
    return "\n".join(lines)


# CHAOS-7047: the paths the Python api still answers when the default backend is the Go api.
# Mirrors the ops chart's ingress.pythonAllowList default (deploy/helm/dev-health/values.yaml);
# a deploy values file may override it at ops.ingress.pythonAllowList. Each entry is
# (path, pathType). Empty since CHAOS-6263: query-api answers /graphql (a deploy values file
# routes it there with an ingress.queryApiPaths entry), so no path is Python's by default.
DEFAULT_PYTHON_ALLOW_LIST: list[tuple[str, str]] = []
# The paths bigboy alone kept on the Python api. CHAOS-8361: none. bigboy runs no Python api, so
# /docs, /redoc, /openapi.json and /metrics reach the default backend (the Go api's native 404),
# as in prod. Never part of the ops chart's allow-list.
BIGBOY_LOCAL_PYTHON_PATHS: list[tuple[str, str]] = []


SHARED_ALLOW_LIST = "ops.ingress.pythonAllowList"


def entry_service(entry: dict, source: str) -> str:
    """The backend an allow-list entry names: `api` (the Python api, also when it has no
    `service` key) or `query-api`. The ops chart accepts no other value; neither does this."""
    service = entry["service"] if "service" in entry else "api"
    if service not in ("api", "query-api"):
        raise SystemExit(
            f"generate-plane-split-router: {source} entry {entry.get('path')!r} has service {service!r}:"
            " an allow-list entry routes to api (the Python api, the default) or to query-api"
        )
    return service


def _allow_lists(doc: dict) -> list[tuple[str, list[dict]]]:
    """Every allow-list of the values, by the name a values author knows it by: the shared
    one, then each host's own."""
    ingress = (doc.get("ops") or {}).get("ingress") or {}
    lists = [(SHARED_ALLOW_LIST, ingress.get("pythonAllowList") or [])]
    for host in ingress.get("hosts") or []:
        own = host.get("pythonAllowList") if isinstance(host, dict) else None
        if isinstance(own, list):
            lists.append(
                (f"ops.ingress.hosts[{host.get('host')}].pythonAllowList", own)
            )
    return lists


def _ops_hosts(doc: dict) -> list[dict]:
    hosts = ((doc.get("ops") or {}).get("ingress") or {}).get("hosts") or []
    return [host for host in hosts if isinstance(host, dict)]


def _host_list_name(host: dict) -> str:
    return f"ops.ingress.hosts[{host.get('host')}].pythonAllowList"


def _allow_lists_in_use(doc: dict) -> list[tuple[str, list[dict]]]:
    """The allow-lists the ops chart renders rules from: the shared one when a host opts into
    it (`pythonAllowList: true`), and each host's own. A list that no host uses renders
    nothing. Values that list no host at all have only the shared list to read."""
    ingress = (doc.get("ops") or {}).get("ingress") or {}
    hosts = _ops_hosts(doc)
    lists = []
    if not hosts or any(host.get("pythonAllowList") is True for host in hosts):
        lists.append((SHARED_ALLOW_LIST, ingress.get("pythonAllowList") or []))
    lists += [
        (_host_list_name(host), host["pythonAllowList"])
        for host in hosts
        if isinstance(host.get("pythonAllowList"), list)
    ]
    return lists


def query_api_allow_list_entries(doc: dict) -> list[tuple[str, str, str, Segments]]:
    """(list name, path as written, pathType, segments) for every entry that names query-api
    on an allow-list in use.

    Such an entry must be ANCHORED (ImplementationSpecific `/literal$`). That is the one shape
    which means one path on every host of the ops chart: the chart reads an Exact entry by
    the host it is on (it refuses one beside an anchored entry, and regex mode on the host
    changes what it matches), and this router has no rule for a subtree on query-api. A shape
    this generator would have to read the way the chart does is refused, not read.
    """
    found = []
    for source, entries in _allow_lists_in_use(doc):
        for entry in entries:
            if entry_service(entry, source) != "query-api":
                continue
            path, path_type = entry["path"], entry["pathType"]
            if not (
                path_type == "ImplementationSpecific"
                and _ANCHORED_ENTRY.fullmatch(path)
            ):
                raise SystemExit(
                    f"generate-plane-split-router: {source} entry {{path: {path}, pathType: {path_type}}}"
                    " names query-api but is not an anchored entry (pathType ImplementationSpecific,"
                    " path `/literal$`): write it anchored, the one shape that is one path on every host"
                )
            literal = allow_list_literal(path, path_type)
            # The ops chart refuses a `.` or `..` segment; an anchored entry can spell one.
            if any(segment in (".", "..") for segment in literal.split("/")):
                raise SystemExit(
                    f"generate-plane-split-router: {source} entry {{path: {path}, pathType: {path_type}}}"
                    " names query-api but has a `.` or `..` segment, which the ops chart refuses"
                )
            found.append((source, path, path_type, _segments(literal)))
    return found


def query_api_paths_that_differ_by_host(doc: dict) -> list[str]:
    """One line for each host of the ops Ingress that does not send to query-api a path an
    allow-list entry sends there.

    This router has ONE host; prod has several. It can prove a path's backend only when every
    prod host that serves the api gives the path that backend. So each host must be one of
    two kinds: a host whose allow-list in use carries the entry and whose own paths are the
    default `/` only, or a web host (all its paths go to web: it does not serve the api). On
    any other host another backend answers the path, or can: by the host's default, or by a
    path of its own that the chart refuses beside the entry or that matches the same request.
    The router would prove a route that host does not have. (A Python entry for the path on
    another list is the same defect; paths_on_two_planes names that one.)
    """
    carried: dict[str, list[str]] = {}
    for source, path, _, _ in query_api_allow_list_entries(doc):
        carried.setdefault(path, []).append(source)
    found = []
    for host in _ops_hosts(doc):
        own = host.get("pythonAllowList")
        if isinstance(own, list):
            source = _host_list_name(host)
        elif own is True:
            source = SHARED_ALLOW_LIST
        else:
            services = sorted(
                {
                    str(p.get("service"))
                    for p in host.get("paths") or []
                    if isinstance(p, dict)
                }
            )
            if services != ["web"]:
                found += [
                    f"{path} is sent to query-api by {', '.join(have)}, and host {host.get('host')}"
                    f" has no allow-list: its own paths answer it (services: {', '.join(services) or 'none'})"
                    for path, have in carried.items()
                ]
            continue
        found += [
            f"{path} is sent to query-api by {', '.join(have)} and not by {source}"
            f" (host {host.get('host')})"
            for path, have in carried.items()
            if source not in have
        ]
        own_paths = [
            str(p.get("path"))
            for p in host.get("paths") or []
            if isinstance(p, dict) and p.get("path") != "/"
        ]
        if own_paths:
            found += [
                f"{path} is sent to query-api by {', '.join(have)}, and host {host.get('host')}"
                f" has paths of its own beside `/`: {', '.join(own_paths)}"
                for path, have in carried.items()
            ]
    return found


def python_allow_list_from_doc(doc: dict) -> list[tuple[str, str]]:
    """The shared list's entries that the Python api answers (an entry that names query-api is
    not one of them)."""
    entries = ((doc.get("ops") or {}).get("ingress") or {}).get("pythonAllowList")
    if entries is None:
        return list(DEFAULT_PYTHON_ALLOW_LIST)
    return [
        (e["path"], e["pathType"])
        for e in entries
        if entry_service(e, SHARED_ALLOW_LIST) == "api"
    ]


def allow_list_literal(path: str, path_type: str) -> str:
    """The literal path an allow-list entry names (an anchored entry without its `$` and with its escaped dots as dots)."""
    if path_type == "ImplementationSpecific":
        return path.removesuffix("$").replace(chr(92) + ".", ".")
    return path


def routed_python_entries(
    python_allow: list[tuple[str, str]] | None,
) -> list[tuple[str, str]]:
    """Every entry this router sends to the Python api: the allow-list plus the local-only paths."""
    allow = python_allow if python_allow is not None else DEFAULT_PYTHON_ALLOW_LIST
    return list(allow) + list(BIGBOY_LOCAL_PYTHON_PATHS)


BIGBOY_LOCAL_SOURCE = (
    "the bigboy-local Python paths (BIGBOY_LOCAL_PYTHON_PATHS in this script)"
)
# The anchored shape the ops chart accepts (deploy/helm/dev-health/templates/ingress.yaml):
# literal segments, `\.` for a dot, exactly one trailing `$`.
_ANCHORED_ENTRY = re.compile(r"(/([A-Za-z0-9_-]|\\\.)+)+\$")


def python_sources(doc: dict) -> list[tuple[str, list[tuple[str, str]]]]:
    """Every list that puts a path on the Python api, by the name a values author knows it by.

    The shared allow-list and the local-only paths are what THIS router routes. A host's own
    list is what prod's ingress routes on that host (the in-cluster host, which web's
    server-side calls use, carries one), so it is checked too: a flip that takes a path off the
    shared list and leaves it on a host's list is half a flip.
    """
    sources = [(SHARED_ALLOW_LIST, python_allow_list_from_doc(doc))]
    for source, entries in _allow_lists(doc)[1:]:
        sources.append(
            (
                source,
                [
                    (e["path"], e["pathType"])
                    for e in entries
                    if entry_service(e, source) == "api"
                ],
            )
        )
    sources.append((BIGBOY_LOCAL_SOURCE, list(BIGBOY_LOCAL_PYTHON_PATHS)))
    return sources


def go_plane_paths(doc: dict) -> list[tuple[str, str, Segments]]:
    """(values key, path as written, segments) for every path a Go plane claims."""
    ingress = doc["ingress"]
    claimed = [
        ("ingress.goApiPaths", e["path"], _goapi_path_segments(e["path"]))
        for e in ingress["goApiPaths"]
    ]
    claimed += [
        (
            "ingress.queryApiPaths",
            e["path"],
            _queryapi_entry_segments(e["path"], e["pathType"]),
        )
        for e in ingress["queryApiPaths"]
    ]
    # An allow-list entry that names query-api is a path a Go plane claims, on whichever list
    # it is: a Python entry for the same path on ANOTHER list is half a move, and is refused.
    claimed += [
        (f"{source} (service: query-api)", path, segments)
        for source, path, _, segments in query_api_allow_list_entries(doc)
    ]
    return claimed


def paths_in_two_objects(doc: dict) -> list[str]:
    """One line for each allow-list entry that names query-api while ingress.goApiPaths or
    ingress.queryApiPaths also claims its path. Prod renders the allow-list in one Ingress
    object and those two tables in others, and its ingress controller's admission webhook
    denies two live objects that claim one host and path: such values cannot roll."""
    ingress = doc["ingress"]
    tables = [
        ("ingress.goApiPaths", e["path"], _goapi_path_segments(e["path"]))
        for e in ingress["goApiPaths"]
    ] + [
        (
            "ingress.queryApiPaths",
            e["path"],
            _queryapi_entry_segments(e["path"], e["pathType"]),
        )
        for e in ingress["queryApiPaths"]
    ]
    found = []
    for source, path, path_type, _ in query_api_allow_list_entries(doc):
        for table, table_path, segments in tables:
            if python_entry_overlaps(path, path_type, segments):
                found.append(
                    f"{source} entry {{path: {path}, pathType: {path_type}, service: query-api}}"
                    f" and {table} entry {table_path}"
                )
    return found


def _segment_is(parts: list[str], text: str) -> bool:
    """A Go plane segment matches exactly this text."""
    pattern = _WILDCARD_REGEX.join(re.escape(part) for part in parts)
    return re.fullmatch(pattern, text) is not None


def _segment_can_start_with(parts: list[str], head: str) -> bool:
    """A Go plane segment matches some text that starts with head."""
    if parts[0].startswith(head):
        return True
    # A wildcard follows the first literal part: it takes the rest of head and more.
    return len(parts) > 1 and head.startswith(parts[0])


def python_entry_overlaps(path: str, path_type: str, segments: Segments) -> bool:
    """Whether some request path is matched by BOTH this Python entry and this Go plane path.

    Exact, and an anchored entry, match one path. A Prefix is taken at its widest reading,
    every path that starts with its text: that is what ingress-nginx renders for a Prefix on
    a host in regex mode (a host with an anchored entry), and it contains this router's own
    reading (the path, or anything under it). A Go plane wildcard never crosses a "/", so the
    answer is exact, segment by segment.
    """
    if path_type == "Prefix":
        want = path.split("/")
        if len(segments) < len(want):
            return False
        return all(
            _segment_is(segments[i], want[i]) for i in range(len(want) - 1)
        ) and _segment_can_start_with(segments[len(want) - 1], want[-1])
    want = allow_list_literal(path, path_type).split("/")
    return len(segments) == len(want) and all(
        _segment_is(parts, text) for parts, text in zip(segments, want)
    )


def paths_on_two_planes(doc: dict) -> list[str]:
    """One line for each Python entry that a Go plane path overlaps, and for each entry of a
    shape that cannot be checked.

    One path on two backends is not a route, it is a tie-break: this router would send it to
    the Go plane (priority 1000 over 500) while prod's ingress controller picks by its own
    rule order, so bigboy would prove a route prod may not have. A values file that moves a
    path to a Go plane must also take it off every Python list; the caller refuses.
    """
    claimed = go_plane_paths(doc)
    found = []
    for source, entries in python_sources(doc):
        for path, path_type in entries:
            entry = f"{source} entry {{path: {path}, pathType: {path_type}}}"
            if path_type not in ("Prefix", "Exact", "ImplementationSpecific") or (
                path_type == "ImplementationSpecific"
                and not _ANCHORED_ENTRY.fullmatch(path)
            ):
                found.append(
                    f"{entry} is not a shape the ops chart accepts (Prefix, Exact, or an anchored"
                    " ImplementationSpecific `/literal$`), so it cannot be checked against the Go planes"
                )
                continue
            for plane, go_path, segments in claimed:
                if python_entry_overlaps(path, path_type, segments):
                    found.append(f"{entry} and {plane} entry {go_path}")
    return found


def _traefik_path_rule(entries: list[tuple[str, str]]) -> str:
    # Prefix = the path itself OR anything under it (whole segment), like ingress-nginx's own Prefix.
    # CHAOS-7243: an ANCHORED entry (ImplementationSpecific, "<literal>$", `\\.` for a dot) matches exactly
    # that path, so it emits a single Path() term.
    terms: list[str] = []
    for path, path_type in entries:
        if path_type == "ImplementationSpecific":
            # CHAOS-7925: the chart puts a host with an anchored entry in ingress-nginx regex mode, where every
            # location is a case-insensitive regex (`~*`): /DOCS reaches the same backend as /docs there. Traefik's
            # Path() is case-sensitive, so the anchored entry is one case-insensitive whole-path regex. A dot is
            # written `[.]`: the rule sits in a double-quoted YAML scalar, which cannot carry a `\.` escape.
            literal = allow_list_literal(path, path_type)
            body = _unescape_hyphen(re.escape(literal)).replace("\\.", "[.]")
            terms.append(f"PathRegexp(`(?i)^{body}$`)")
            continue
        terms.append(f"Path(`{path}`)")
        if path_type == "Prefix":
            terms.append(f"PathPrefix(`{path.rstrip('/')}/`)")
    return "(" + " || ".join(terms) + ")"


def emit_dynamic_config(
    go_regex: str,
    query_regex: str,
    python_allow: list[tuple[str, str]] | None = None,
) -> str:
    """Traefik FILE-PROVIDER dynamic config (not docker labels): go-api and query-api are named
    by address only (http://go-api:8000 etc) -- this file is the only thing that needs to change
    to add/adjust routing, so applying it only touches traefik (which reloads the file live) and
    web (whose BACKEND_URL points at the router). The planes themselves are never recreated.

    CHAOS-8361: the Python router and its service are written only when an allow-list entry of
    the values names the Python api. With none (the prod values, and no bigboy-local path) the
    file names no Python backend at all: a rule with no path term is not a rule traefik loads."""
    python_router = python_service = ""
    if python_entries := routed_python_entries(python_allow):
        python_rule = _traefik_path_rule(python_entries)
        python_router = f"""    # CHAOS-7047: the paths the Python api answers (an entry of the ops chart's
    # ingress.pythonAllowList that names it), below the two plane routers, above the default backend.
    python-allowlist:
      rule: "{HOST_RULE} && {python_rule}"
      entryPoints: [web]
      priority: 500
      service: python-allowlist
"""
        python_service = """    python-allowlist:
      loadBalancer:
        servers:
          - url: "http://api:8000"
"""
    return f"""# GENERATED by generate-plane-split-router.py from deploy/values.prod.yaml's
# ingress.goApiPaths + ingress.queryApiPaths -- do not hand-edit the rule values, regenerate.
# Traefik file-provider dynamic config (mounted into the traefik container, hot-reloaded).
http:
  routers:
    go-api-paths:
      rule: "{HOST_RULE} && PathRegexp(`{go_regex}`)"
      entryPoints: [web]
      priority: 1000
      service: go-api-paths
    query-api-paths:
      rule: "{HOST_RULE} && PathRegexp(`{query_regex}`)"
      entryPoints: [web]
      priority: 1000
      service: query-api-paths
{python_router}    # Everything else on the internal traefik hostname (web's own server-side BACKEND_URL calls)
    # falls through to the GO api (its native 404 answers an unknown path), mirroring prod's
    # default backend. Without a rule, a non-split path called via BACKEND_URL=http://traefik:3000
    # would 404 at traefik itself, with no plane header.
    api-internal-catchall:
      rule: "Host(`traefik`)"
      entryPoints: [web]
      priority: 1
      service: api-internal-catchall
  services:
    go-api-paths:
      loadBalancer:
        servers:
          - url: "http://go-api:8000"
    query-api-paths:
      loadBalancer:
        servers:
          - url: "http://query-api:8090"
{python_service}    api-internal-catchall:
      loadBalancer:
        servers:
          - url: "http://go-api:8000"
"""


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("values", nargs="?")
    ap.add_argument("--format", default="labels", choices=["labels", "dynamic"])
    ap.add_argument("--deploy-repo")
    ap.add_argument("--deploy-sha")
    ap.add_argument("--expect-ops-sha")
    ap.add_argument(
        "--values-out",
        help="pinned mode: also write values.prod.yaml at --deploy-sha here",
    )
    a = ap.parse_args(argv)
    pinned = [a.deploy_repo, a.deploy_sha, a.expect_ops_sha]
    if a.values and any(pinned):
        print(
            "give EITHER a values file OR --deploy-repo/--deploy-sha/--expect-ops-sha",
            file=sys.stderr,
        )
        return 2
    if a.values:
        with open(a.values) as f:
            doc = yaml.safe_load(f)
        go_paths, query_paths = paths_from_doc(doc)
        python_allow = python_allow_list_from_doc(doc)
    elif all(pinned):
        try:
            full, vendored, text = values_at_sha(
                a.deploy_repo, a.deploy_sha, a.expect_ops_sha
            )
        except PinError as e:
            print(f"REFUSED: {e}", file=sys.stderr)
            return 3
        doc = yaml.safe_load(text)
        go_paths, query_paths = paths_from_doc(doc)
        python_allow = python_allow_list_from_doc(doc)
        if a.values_out:
            with open(a.values_out, "w", encoding="utf-8") as f:
                f.write(text)
        print(f"# deploy_sha={full} ops_vendor={vendored}", file=sys.stderr)
    else:
        print(__doc__, file=sys.stderr)
        return 2
    # The rule is written inside a double-quoted YAML scalar, which cannot carry a regex escape
    # (`\.` is not a YAML escape): the file would not load, and traefik would keep its old
    # router with no failing step. Refuse instead of emitting it.
    if unwritable := [
        f"{plane} entry {path}"
        for plane, path, segments in go_plane_paths(doc)
        if "\\" in _segments_regex(segments)
    ]:
        print(
            "REFUSED: no valid rule can be emitted for "
            + ", ".join(unwritable)
            + ": the path has a character that needs a regex escape (a dot, for one), and the"
            " emitted rule cannot carry an escape. This generator does not support such a path yet.",
            file=sys.stderr,
        )
        return 5
    go_regex = combined_regex(go_paths)
    query_regex = combined_regex(query_paths)
    if both := paths_on_two_planes(doc):
        print(
            "REFUSED: one path, two backends. A path the Python api is given and a path a Go plane is"
            " given can match the same request. Take the path off every Python list in the same values"
            " change that moves it to the Go plane:\n  " + "\n  ".join(both),
            file=sys.stderr,
        )
        return 4
    if per_host := query_api_paths_that_differ_by_host(doc):
        print(
            "REFUSED: one path, a backend that differs by host. An allow-list entry sends the path"
            " to query-api, and a host of the ops Ingress answers it from another backend. This"
            " router has one host and cannot show both. Every host must carry the entry on the"
            " allow-list it uses (the shared one or its own) and have no path of its own beside"
            " `/`, or be a web host (all paths to web):\n  " + "\n  ".join(per_host),
            file=sys.stderr,
        )
        return 4
    if twice := paths_in_two_objects(doc):
        print(
            "REFUSED: one path, two Ingress objects. An allow-list entry that names query-api and an"
            " entry of ingress.goApiPaths or ingress.queryApiPaths can match the same request. Prod's"
            " ingress admission denies two live Ingress objects for one host and path. Keep the path"
            " in ONE place:\n  " + "\n  ".join(twice),
            file=sys.stderr,
        )
        return 4
    if a.format == "dynamic":
        out = emit_dynamic_config(go_regex, query_regex, python_allow)
    else:
        out = emit_labels(go_regex, query_regex)
    print(out)
    print(
        f"# {len(go_paths)} goApiPaths, {len(query_paths)} query-api paths"
        f" ({len(doc['ingress']['queryApiPaths'])} queryApiPaths,"
        f" {len(query_paths) - len(doc['ingress']['queryApiPaths'])} allow-list entries that name query-api)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
