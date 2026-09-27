#!/usr/bin/env python3
"""R467: every path the plane-split router sends to a Go plane must have a handler in the
RUNNING build. Deploy values can run ahead of the deployed image (a PR that routes a path merges
before the handler ships); traffic for such a path would reach a plane that answers 404.

Mechanism: one request per routed path with the non-standard method ROUTEPROBE. Both Go planes
use net/http's method-aware mux, which answers a path it has a handler for with 405 (method not
allowed) and a path it has none for with 404 -- before any handler, auth or database code runs.
So the probe executes nothing and needs no credential. A handler wrapped in auth middleware may
answer 401 first; that also proves a handler is registered (an unknown path is a bare 404).

LIMIT: a registered STUB (e.g. a go-served 500 placeholder) is indistinguishable from a real
handler here. The control for "values ahead of the build" is the pinned deploy sha (R467):
generate-plane-split-router.py --deploy-sha refuses a deploy commit whose ops vendor pin is not
the running build; this check adds the 404 class on top.

Usage: check-route-coverage.py <values.prod.yaml> [--go-api-url http://127.0.0.1:8093]
                                [--query-api-url http://127.0.0.1:8091]
Exit: 0 every routed path registered; 1 any path missing/unexpected (named on stderr); 2 usage.
"""

from __future__ import annotations

import argparse
import http.client
import re
import sys
import urllib.parse

import yaml

PROBE_METHOD = "ROUTEPROBE"
PLACEHOLDER = "routeprobe"
ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost"})


def probe_paths(doc: dict) -> tuple[list[str], list[str]]:
    ingress = doc["ingress"]
    go = [
        re.sub(r"\{[A-Za-z0-9_]+\}", PLACEHOLDER, e["path"])
        for e in ingress["goApiPaths"]
    ]
    query = [e["path"].replace("[^/]+", PLACEHOLDER) for e in ingress["queryApiPaths"]]
    if not go or not query:
        raise SystemExit(
            "check-route-coverage: values has ZERO goApiPaths or queryApiPaths -- refusing to pass"
        )
    return go, query


def target(url: str) -> tuple[str, int]:
    parts = urllib.parse.urlsplit(url)
    if (
        parts.scheme != "http"
        or (parts.hostname or "") not in ALLOWED_HOSTS
        or parts.path not in ("", "/")
    ):
        raise SystemExit(
            f"check-route-coverage: plane URL must be http://127.0.0.1:<port>, got {url!r}"
        )
    return parts.hostname or "", parts.port or 80


def probe(host: str, port: int, path: str) -> int:
    conn = http.client.HTTPConnection(host, port, timeout=10)
    try:
        conn.request(PROBE_METHOD, path)
        resp = conn.getresponse()
        resp.read()
        return resp.status
    finally:
        conn.close()


def classify(statuses: dict[str, int]) -> tuple[list[str], list[str]]:
    missing = [p for p, s in statuses.items() if s == 404]
    unexpected = [f"{p}={s}" for p, s in statuses.items() if s not in (401, 404, 405)]
    return missing, unexpected


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("values")
    ap.add_argument("--go-api-url", default="http://127.0.0.1:8093")
    ap.add_argument("--query-api-url", default="http://127.0.0.1:8091")
    a = ap.parse_args(argv)
    with open(a.values, encoding="utf-8") as f:
        go, query = probe_paths(yaml.safe_load(f))
    failed = False
    for plane, url, paths in (
        ("go-api", a.go_api_url, go),
        ("query-api", a.query_api_url, query),
    ):
        host, port = target(url)
        statuses = {p: probe(host, port, p) for p in paths}
        missing, unexpected = classify(statuses)
        for p in missing:
            print(
                f"DRIFT: {plane} has NO handler for routed path {p} (404) -- values run ahead of the running build",
                file=sys.stderr,
            )
        for u in unexpected:
            print(
                f"DRIFT: {plane} answered an unexpected status for {u} (want 405)",
                file=sys.stderr,
            )
        print(
            f"ROUTE_COVERAGE plane={plane} routed={len(paths)} registered={len(paths) - len(missing) - len(unexpected)} missing={len(missing)} unexpected={len(unexpected)}"
        )
        failed = failed or bool(missing or unexpected)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
