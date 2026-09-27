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

Usage: generate-plane-split-router.py <path to deploy repo's values.prod.yaml> [--format labels|compose]
  labels (default): the docker-compose label lines for go-api's and query-api's services.
  compose: a full YAML services: fragment (go-api/query-api label blocks) ready to diff/paste.
"""

from __future__ import annotations

import re
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


def load_paths(values_path: str) -> tuple[list[str], list[str]]:
    with open(values_path) as f:
        doc = yaml.safe_load(f)
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


def emit_dynamic_config(go_regex: str, query_regex: str) -> str:
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
    # Everything else on the internal traefik hostname (web's own server-side BACKEND_URL calls)
    # falls through to the Python api, mirroring compose.yml's own browser-origin web router
    # (which only matches commanderkeen.dev/localhost, never the internal `traefik` hostname).
    # Without this, a non-split path called via BACKEND_URL=http://traefik:3000 would 404.
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
    api-internal-catchall:
      loadBalancer:
        servers:
          - url: "http://api:8000"
"""


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    go_paths, query_paths = load_paths(sys.argv[1])
    go_regex = combined_regex(go_paths)
    query_regex = combined_regex(query_paths)
    fmt = "labels"
    if len(sys.argv) > 3 and sys.argv[2] == "--format":
        fmt = sys.argv[3]
    if fmt == "dynamic":
        out = emit_dynamic_config(go_regex, query_regex)
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
