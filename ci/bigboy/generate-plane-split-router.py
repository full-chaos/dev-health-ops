#!/usr/bin/env python3
"""CHAOS-6987 (D2724): derive bigboy's traefik plane-split router labels from deploy's own
values.prod.yaml ingress.goApiPaths + ingress.queryApiPaths -- never hand-maintained, so bigboy's
router cannot drift silently from what prod's ingress actually does (same class as CHAOS-6967(b)'s
generate-query-api-enabled-flags.py).

Bigboy has no k8s Ingress; traefik (already in compose.yml, providers.docker=true, label-based)
is the router. web's own labels give it a catch-all Host() rule with default (low) priority --
this script emits HIGHER-priority Host()+PathRegexp() router labels for go-api and query-api so
matching paths reach those services first; everything else still falls through to web's rule
(which then forwards to Python api via BACKEND_URL, unchanged).

Path syntax: prod's goApiPaths use {param} whole-segment templates (never a hand-written regex,
the k8s Ingress controller/chart expands and anchors them); queryApiPaths use pathType Exact
(literal path) or ImplementationSpecific (an inline [^/]+ placeholder already regex-shaped). Both
are converted here to one traefik PathRegexp per plane: {param} -> [^/]+, Exact paths escaped and
used literally, ImplementationSpecific paths used as-is (already regex).

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


def _goapi_path_to_regex(path: str) -> str:
    escaped = _unescape_hyphen(re.escape(path))
    # re.escape turns "{" into "\{" -- match that escaped form.
    return re.sub(r"\\\{[a-zA-Z0-9_]+\\\}", "[^/]+", escaped)


def _queryapi_entry_to_regex(path: str, path_type: str) -> str:
    if path_type == "Exact":
        return _unescape_hyphen(re.escape(path))
    # ImplementationSpecific: already contains a literal [^/]+ regex placeholder inline (e.g.
    # "/api/v1/people/[^/]+/metric") -- escape everything EXCEPT that literal placeholder.
    parts = path.split("[^/]+")
    return "[^/]+".join(_unescape_hyphen(re.escape(p)) for p in parts)


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
# (path, pathType).
DEFAULT_PYTHON_ALLOW_LIST: list[tuple[str, str]] = [
    ("/graphql", "Prefix"),
    ("/api/v1/internal", "Prefix"),
]
# Kept on Python on bigboy/local ONLY (D2983: prod blocks these at the ingress; local keeps
# them). Never part of the ops chart's allow-list.
BIGBOY_LOCAL_PYTHON_PATHS: list[tuple[str, str]] = [
    ("/docs", "Prefix"),
    ("/redoc", "Exact"),
    ("/openapi.json", "Exact"),
    ("/metrics", "Exact"),
]


_WILD = "\x00"


def _wild_segments(path: str) -> list[str]:
    """Path segments of a go-api/query-api entry with every `{token}` / inline `[^/]+` replaced by a
    wildcard marker BEFORE splitting on "/" (the inline placeholder itself contains a slash)."""
    marked = re.sub(r"\{[a-zA-Z0-9_]+\}", _WILD, path).replace("[^/]+", _WILD)
    return [s for s in marked.strip("/").split("/") if s != ""]


def _seg_regex(segment: str) -> re.Pattern[str]:
    """A wildcard-marked segment as the regex the generated router matches it with."""
    return re.compile("[^/]+".join(re.escape(p) for p in segment.split(_WILD)))


def _served_overlaps(allow_path: str, allow_type: str, served_path: str) -> bool:
    """Does the generated Go/query router (an anchored full-path match, segment by segment) also
    match a path the Python allow-list rule matches? Exact: the same segment count with every
    segment matching. Prefix: some served path lies at or below the prefix."""
    allow = [s for s in allow_path.strip("/").split("/") if s != ""]
    served = _wild_segments(served_path)
    if allow_type == "Exact":
        if len(allow) != len(served):
            return False
    elif len(served) < len(allow):
        return False
    return all(_seg_regex(sv).fullmatch(al) for al, sv in zip(allow, served))


def python_allow_list_from_doc(doc: dict) -> list[tuple[str, str]]:
    entries = ((doc.get("ops") or {}).get("ingress") or {}).get("pythonAllowList")
    if entries is None:
        out = list(DEFAULT_PYTHON_ALLOW_LIST)
    else:
        out = [(e["path"], e["pathType"]) for e in entries]
    # Segment-exact, like ingress-nginx Prefix: only these Prefix paths cover /api/v1/internal.
    if not any(
        t == "Prefix" and p.rstrip("/") in ("", "/api", "/api/v1", "/api/v1/internal")
        for p, t in out
    ):
        raise SystemExit(
            "generate-plane-split-router: ops.ingress.pythonAllowList must carry /api/v1/internal"
            " (Prefix): the Go api serves /api/v1/internal/acr/* with no credential check"
        )
    # CHAOS-7198: a Python allow-list rule that overlaps a path go-api/query-api serve loses to the
    # higher-priority Go/query router, so the operator's intent silently goes the wrong way. Compare
    # by the generated matchers (anchored segment-wise regexes vs Exact/whole-segment Prefix), on the EFFECTIVE list
    # (built-in defaults included), never by string equality.
    ingress = doc.get("ingress") or {}
    # The generator emits EVERY go-api/query-api entry as an anchored full-path regex (see
    # _goapi_path_to_regex/_queryapi_entry_to_regex), whatever pathType the values file declares.
    served = [
        e["path"]
        for e in (ingress.get("goApiPaths") or [])
        + (ingress.get("queryApiPaths") or [])
    ]
    clash = sorted({a for a, at in out for s in served if _served_overlaps(a, at, s)})
    if clash:
        raise SystemExit(
            "generate-plane-split-router: pythonAllowList paths overlap paths served by"
            f" go-api/query-api (the Go/query router would win): {clash}"
        )
    return out


def _traefik_path_rule(entries: list[tuple[str, str]]) -> str:
    # Prefix = the path itself OR anything under it (whole segment), like ingress-nginx.
    terms: list[str] = []
    for path, path_type in entries:
        terms.append(f"Path(`{path}`)")
        if path_type == "Prefix":
            terms.append(f"PathPrefix(`{path.rstrip('/')}/`)")
    return "(" + " || ".join(terms) + ")"


def emit_dynamic_config(
    go_regex: str,
    query_regex: str,
    python_allow: list[tuple[str, str]] | None = None,
) -> str:
    python_rule = _traefik_path_rule(
        (python_allow if python_allow is not None else DEFAULT_PYTHON_ALLOW_LIST)
        + BIGBOY_LOCAL_PYTHON_PATHS
    )
    """Traefik FILE-PROVIDER dynamic config (not docker labels): go-api/query-api/api are named
    by address only (http://go-api:8000 etc) -- this file is the only thing that needs to change
    to add/adjust routing, so applying it only touches traefik (which reloads the file live) and
    web (whose BACKEND_URL points at the router). go-api/query-api/api themselves are never
    recreated."""
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
    # CHAOS-7047: the paths Python still answers (ops chart ingress.pythonAllowList + the
    # local-only docs/metrics paths), below the two plane routers, above the default backend.
    python-allowlist:
      rule: "{HOST_RULE} && {python_rule}"
      entryPoints: [web]
      priority: 500
      service: python-allowlist
    # Everything else on the internal traefik hostname (web's own server-side BACKEND_URL calls)
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
    python-allowlist:
      loadBalancer:
        servers:
          - url: "http://api:8000"
    api-internal-catchall:
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
        go_paths, query_paths = load_paths(a.values)
        with open(a.values) as f:
            python_allow = python_allow_list_from_doc(yaml.safe_load(f))
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
    go_regex = combined_regex(go_paths)
    query_regex = combined_regex(query_paths)
    if a.format == "dynamic":
        out = emit_dynamic_config(go_regex, query_regex, python_allow)
    else:
        out = emit_labels(go_regex, query_regex)
    print(out)
    print(
        f"# {len(go_paths)} goApiPaths, {len(query_paths)} queryApiPaths",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
