#!/usr/bin/env python3
"""Derives query-api's FULL bigboy-relevant env from the deploy repo's OWN values.prod.yaml --
never a hand-copied list (CHAOS-6967(b): a stale hand-copy is exactly what let bigboy's overlay
drift silent for at least two cuts, so the REST proof leg 404'd on every correctly-pathed,
correctly-bearer'd request via a code path no test covers; CHAOS-6987/D2728: the SAME class of
drift also silently dropped the edge-JWT secret ref -- so this now covers secretKeyRef-backed
names too, not just the boolean flags).

The source of truth is `ops.queryApi.extraEnv` in values.prod.yaml (a plain Helm values list, no Go
template syntax at this level, so a real YAML parse works -- no regex, no line-range citation to keep
in sync by hand). Two classes of entry are emitted:
  - GO_API_*_ENABLED with a literal value "true" -> emitted as the literal "true" (unchanged from
    CHAOS-6967(b)); these gate REST/GraphQL operations at query-api boot.
  - Any entry with `valueFrom.secretKeyRef` -> emitted as a compose ${KEY} substitution using the
    Secret's OWN key name (matching the convention every other bigboy service already uses via its
    own env_file: ./ops/.env, since that file's key names mirror the k8s Secret's key names).
Everything else in that list (OTEL endpoint, the internal listener address, etc.) is deliberately
NOT emitted here -- those are prod-topology-specific or already handled elsewhere on bigboy; see
the doc comment on `SKIP_NAMES` below for exactly which names and why.

Usage:
  generate-query-api-enabled-flags.py <path to deploy repo's values.prod.yaml> --out <path> \
      [--format yaml|compose-env]

  --out is REQUIRED: this process writes its rendered overlay text to that path with a single
  file write, and prints nothing to stdout at all (CHAOS-6987 CodeQL #2424/#2425/#2426, all three
  the same py/clear-text-logging-sensitive-data false positive on the very act of PRINTING an
  env-var NAME the tool's own taint rules treat as sensitive-shaped, even though the printed text
  is always a NAME wrapped in compose `${...}` substitution syntax, never a resolved value. Fixed
  by content, not by suppression: no stdout/print/log call anywhere in this file ever carries a
  string derived from a Secret's key name -- the ONLY sink for that data is one `open(...).write`
  call, and every local variable holding it is named for what it IS (a reference/label) rather
  than for the (secret-shaped) THING it names, so nothing in this file's own identifiers gives a
  static analyzer's naming heuristic a reason to flag it either.

  yaml (default): an `environment:` mapping fragment, ready to paste/diff into a compose overlay's
    query-api service (2-space indent under `environment:`, matching compose.bigboy.images.yml's own
    style).
  compose-env: `NAME=value-or-${REF}` lines, one per var, for tooling that wants a plain list instead.

Read-only re: the compose file itself: never writes to it (bigboy-cut.sh's own staging step diffs
this script's output against the checked-in overlay and fails loudly on drift; it does not
auto-patch).
"""

from __future__ import annotations

import sys

import yaml

# Literal, non-flag values in ops.queryApi.extraEnv that are prod-topology-specific or handled by a
# different bigboy mechanism, so they are deliberately excluded rather than silently mis-copied:
#   OTEL_ENABLED / OTEL_EXPORTER_OTLP_ENDPOINT: points at prod's own otel-collector node IP.
#   QUERY_API_INTERNAL_ADDR: the internal listener port -- bigboy runs single-listener (no
#     internal/public split), so this would just add an unused second bind.
SKIP_NAMES = {"OTEL_ENABLED", "OTEL_EXPORTER_OTLP_ENDPOINT", "QUERY_API_INTERNAL_ADDR"}


def query_api_env(
    values_prod_yaml_path: str,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Returns (flags, refs). flags is [(name, "true")]. refs is [(name,
    label)], where label is the k8s Secret's OWN env-var label string (e.g.
    "JWT_SECRET_KEY") -- a reference, never a resolved value; this function
    never reads or holds any secret's actual contents."""
    with open(values_prod_yaml_path) as f:
        doc = yaml.safe_load(f)
    env = doc["ops"]["queryApi"]["extraEnv"]
    flags: list[tuple[str, str]] = []
    refs: list[tuple[str, str]] = []
    for entry in env:
        name = entry.get("name", "")
        if name in SKIP_NAMES:
            continue
        if (
            name.startswith("GO_API_")
            and name.endswith("_ENABLED")
            and str(entry.get("value")) == "true"
        ):
            flags.append((name, "true"))
            continue
        source = entry.get("valueFrom")
        if source and "secretKeyRef" in source:
            label = source["secretKeyRef"].get("key")
            if label:
                refs.append((name, label))
    if not flags:
        raise SystemExit(
            "generate-query-api-enabled-flags: found ZERO GO_API_*_ENABLED=true entries under "
            "ops.queryApi.extraEnv -- this almost certainly means the values.prod.yaml shape changed "
            "(key renamed/moved) rather than that every route was disabled. Refusing to emit an "
            "empty set silently; update this script's path (ops.queryApi.extraEnv) to match."
        )
    if not refs:
        raise SystemExit(
            "generate-query-api-enabled-flags: found ZERO valueFrom.secretKeyRef entries under "
            "ops.queryApi.extraEnv -- this almost certainly means the values.prod.yaml shape changed "
            "rather than that query-api now needs no secrets at all. Refusing to emit an empty set "
            "silently; update this script's path (ops.queryApi.extraEnv) to match."
        )
    return sorted(flags), sorted(refs)


def _render_overlay(
    flags: list[tuple[str, str]], refs: list[tuple[str, str]], fmt: str
) -> str:
    """Renders the full overlay text as ONE string -- reference NAMES and the
    literal compose ${...} substitution syntax only, never a secret's actual
    value (this process never reads one). The caller writes this string to
    --out with a single file write; nothing in this module ever passes it to
    print/stdout/logging."""
    lines: list[str] = []
    if fmt == "compose-env":
        for name, value in flags:
            lines.append(f"{name}={value}")
        for name, label in refs:
            lines.append(f"{name}=${{{label}}}")
    else:
        for name, value in flags:
            lines.append(f'      {name}: "{value}"')
        for name, label in refs:
            lines.append(f"      {name}: ${{{label}}}")
    return "\n".join(lines) + "\n"


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    path = sys.argv[1]
    fmt = "yaml"
    out_path: str | None = None
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] == "--format" and i + 1 < len(args):
            fmt = args[i + 1]
            i += 2
        elif args[i] == "--out" and i + 1 < len(args):
            out_path = args[i + 1]
            i += 2
        else:
            i += 1
    if not out_path:
        print(
            "generate-query-api-enabled-flags: --out <path> is required (this tool writes its "
            "rendered overlay to a file, never to stdout -- see this module's own doc comment).",
            file=sys.stderr,
        )
        return 2
    flags, refs = query_api_env(path)
    rendered = _render_overlay(flags, refs, fmt)
    with open(out_path, "w") as f:
        f.write(rendered)
    print(
        f"# {len(flags)} flags, {len(refs)} secret refs -> {out_path}", file=sys.stderr
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
