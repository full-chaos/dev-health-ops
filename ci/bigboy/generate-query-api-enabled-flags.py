#!/usr/bin/env python3
"""Derives the GO_API_*_ENABLED env vars bigboy's compose overlay must set on query-api, from the
deploy repo's OWN values.prod.yaml -- never a hand-copied list (CHAOS-6967(b): a stale hand-copy is
exactly what let bigboy's overlay drift silent for at least two cuts, so the REST proof leg 404'd on
every correctly-pathed, correctly-bearer'd request via a code path no test covers: these routes'
Go-plane switch is a plain in-memory flag flipped ONCE at query-api container boot from its own bare
env var, never Postgres, never the routeswitch ledger `dho goapi routing enable` writes (that ledger
is wired only into /query, GraphQL) -- see internal/api/query-api or the route registration these
names gate, and deploy/values.prod.yaml:492-494's own comment: "The ported /api/v1 routes below
answer only in-cluster ... enabling them here exposes them to the REST proof only.").

The source of truth is `ops.queryApi.extraEnv` in values.prod.yaml (a plain Helm values list, no Go
template syntax at this level, so a real YAML parse works -- no regex, no line-range citation to keep
in sync by hand). Every entry named GO_API_*_ENABLED with value "true" is emitted; nothing else in
that list (Postgres/ClickHouse DSNs, the internal listener address, etc.) is touched.

Usage:
  generate-query-api-enabled-flags.py <path to deploy repo's values.prod.yaml> [--format yaml|compose-env]

  yaml (default): a `environment:` mapping fragment, ready to paste/diff into a compose overlay's
    query-api service (2-space indent under `environment:`, matching compose.bigboy.images.yml's own
    style).
  compose-env: `NAME=value` lines, one per var, for tooling that wants a plain env file instead.

Read-only: never writes to the compose file itself (bigboy-cut.sh's own staging step diffs this
script's output against the checked-in overlay and fails loudly on drift; it does not auto-patch).
"""

from __future__ import annotations

import sys

import yaml


def enabled_flags(values_prod_yaml_path: str) -> list[tuple[str, str]]:
    with open(values_prod_yaml_path) as f:
        doc = yaml.safe_load(f)
    env = doc["ops"]["queryApi"]["extraEnv"]
    out = []
    for entry in env:
        name = entry.get("name", "")
        if (
            name.startswith("GO_API_")
            and name.endswith("_ENABLED")
            and str(entry.get("value")) == "true"
        ):
            out.append((name, "true"))
    if not out:
        raise SystemExit(
            "generate-query-api-enabled-flags: found ZERO GO_API_*_ENABLED=true entries under "
            "ops.queryApi.extraEnv -- this almost certainly means the values.prod.yaml shape changed "
            "(key renamed/moved) rather than that every route was disabled. Refusing to emit an "
            "empty set silently; update this script's path (ops.queryApi.extraEnv) to match."
        )
    return sorted(out)


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    path = sys.argv[1]
    fmt = "yaml"
    if len(sys.argv) > 2 and sys.argv[2] == "--format":
        fmt = sys.argv[3]
    flags = enabled_flags(path)
    if fmt == "compose-env":
        for name, value in flags:
            print(f"{name}={value}")
    else:
        for name, value in flags:
            print(f'      {name}: "{value}"')
    print(f"# {len(flags)} flags", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
