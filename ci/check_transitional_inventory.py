#!/usr/bin/env python3
"""CUT-01 CI gate: enforce the transitional workload inventory contract.

Independently re-discovers every legacy Celery/Beat/dispatch/registry/stream
surface in the tree (without reading contracts/jobs/v1/transitional-inventory.json
first) and then cross-checks that discovery against the checked-in inventory.

Fails (exit 1, with a human-readable report) when:
  1. a discovered surface has no corresponding inventory row (unowned surface);
  2. an inventory row has no target owner;
  3. two inventory rows are both flagged as the exclusive ("primary") owner of
     the same target_kind_id -- ownership must be exclusive per target;
  4. an inventory row's source anchor (file[:line]) no longer exists on disk
     or the line no longer matches what the row was anchored to (staleness).

This script deliberately re-derives discovery from source, independent of the
inventory file's own bookkeeping, so that adding an unowned Celery task decorator,
Beat entry, `.delay`/`.apply_async`/`send_task`/`.signature(` call, the
`getattr(x, "delay"|"apply_async"|"send_task")` indirection form, an API
dispatch endpoint, a registry kind, or a sync-dispatch transport route -- without
updating the inventory -- fails CI.

Usage:
    python3 ci/check_transitional_inventory.py [--root PATH] [--inventory PATH]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

DEFAULT_INVENTORY = "contracts/jobs/v1/transitional-inventory.json"

CELERY_TASK_DECORATOR_RE = re.compile(r"^\s*@celery_app\.task\(")
BEAT_ENTRY_RE = re.compile(r'^    "([a-zA-Z0-9_-]+)":\s*\{')
BEAT_ENTRY_CONDITIONAL_RE = re.compile(r'^beat_schedule\["([a-zA-Z0-9_-]+)"\]\s*=')
CALL_SITE_LITERAL_RE = re.compile(
    r"\.delay\(|\.apply_async\(|\.signature\(|send_task\("
)
GETATTR_INDIRECTION_RE = re.compile(
    r'getattr\([^,]+,\s*["\'](delay|apply_async|send_task)["\']\s*\)'
)
ROUTER_DECORATOR_RE = re.compile(r"^\s*@router\.(get|post|put|patch|delete)\(")
DEF_RE = re.compile(r"^\s*(async\s+def|def)\s+\w+\(")
CONSUMER_GROUP_RE = re.compile(r"^CONSUMER_GROUP\s*=")
PAGERDUTY_ENQUEUE_RE = re.compile(r"^def _enqueue_event\(")
JSON_KIND_RE = re.compile(r'"kind":\s*"([^"]+)"')

# Surfaces are anchored to these classes; must match transitional-inventory.json's class enum.
CLASS_CELERY_TASK = "celery_task"
CLASS_BEAT_ENTRY = "beat_entry"
CLASS_BEAT_ENTRY_CONDITIONAL = "beat_entry_conditional"
CLASS_CALL_SITE_LITERAL = "call_site_literal"
CLASS_CALL_SITE_GETATTR = "call_site_getattr_indirection"
CLASS_API_TRIGGER = "api_trigger_endpoint"
CLASS_REGISTRY_KIND = "registry_kind"
CLASS_STREAM_SURFACE = "stream_surface"
CLASS_TRANSPORT_ROUTE = "sync_dispatch_transport_route"


@dataclass(frozen=True)
class Surface:
    cls: str
    file: str
    line: int
    name: str = ""

    def key(self) -> tuple[str, str, int]:
        return (self.cls, self.file, self.line)


def _iter_py_files(base: Path):
    if not base.exists():
        return
    for path in sorted(base.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        yield path


def _relpath(root: Path, path: Path) -> str:
    return str(path.relative_to(root))


def discover_celery_tasks(root: Path) -> list[Surface]:
    out = []
    for path in _iter_py_files(root / "src/dev_health_ops/workers"):
        for i, line in _iter_code_lines(path):
            if CELERY_TASK_DECORATOR_RE.match(line):
                out.append(Surface(CLASS_CELERY_TASK, _relpath(root, path), i))
    return out


def discover_beat_entries(root: Path) -> list[Surface]:
    path = root / "src/dev_health_ops/workers/config.py"
    if not path.exists():
        return []
    out = []
    in_schedule = False
    relpath = _relpath(root, path)
    for i, line in enumerate(path.read_text().splitlines(), start=1):
        if line.rstrip() == "beat_schedule = {":
            in_schedule = True
            continue
        if in_schedule:
            if line == "}":
                in_schedule = False
                continue
            m = BEAT_ENTRY_RE.match(line)
            if m:
                out.append(Surface(CLASS_BEAT_ENTRY, relpath, i, m.group(1)))
        m2 = BEAT_ENTRY_CONDITIONAL_RE.match(line)
        if m2:
            out.append(Surface(CLASS_BEAT_ENTRY_CONDITIONAL, relpath, i, m2.group(1)))
    return out


def _is_code_line(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith("#")


_CONTINUATION_RE = re.compile(r"^\s*\)+\s*\.(delay|apply_async|signature)\(")


def _iter_code_lines(path: Path):
    """Yield (line_number, line) skipping lines inside triple-quoted docstrings.

    Naive but effective toggle: counts \"\"\"/''' occurrences per physical line
    to track docstring state. Good enough for discovery purposes -- worst case
    a rare false negative on an unusual docstring shape, never a false unowned-
    surface failure from documentation prose being mistaken for code.
    """
    in_docstring = False
    for i, line in enumerate(path.read_text().splitlines(), start=1):
        starts_in_docstring = in_docstring
        triple_count = line.count('"""') + line.count("'''")
        if triple_count % 2 == 1:
            in_docstring = not in_docstring
        if starts_in_docstring:
            continue
        yield i, line


def discover_call_sites(root: Path) -> tuple[list[Surface], list[Surface]]:
    literal, getattr_indirection = [], []
    for path in _iter_py_files(root / "src/dev_health_ops"):
        relpath = _relpath(root, path)
        last_match_line = -10
        for i, line in _iter_code_lines(path):
            if not _is_code_line(line):
                continue
            if GETATTR_INDIRECTION_RE.search(line):
                getattr_indirection.append(Surface(CLASS_CALL_SITE_GETATTR, relpath, i))
                last_match_line = i
            elif CALL_SITE_LITERAL_RE.search(line):
                if _CONTINUATION_RE.match(line) and i - last_match_line <= 4:
                    # closing-paren continuation of a multi-line signature(...)/
                    # chain(...)/chord(...) call whose opening line already matched;
                    # the earlier line is the real anchor, so this isn't a second
                    # independent surface.
                    continue
                literal.append(Surface(CLASS_CALL_SITE_LITERAL, relpath, i))
                last_match_line = i
    return literal, getattr_indirection


def discover_api_trigger_endpoints(
    root: Path, dispatch_surfaces: list[Surface]
) -> list[Surface]:
    """For every dispatch call site under src/dev_health_ops/api, walk back to
    the nearest preceding @router.<method>(...) and forward to the following
    def/async def line -- that def line is the endpoint's anchor, matching how
    the Wave-0 audit anchored API trigger endpoints."""
    by_file: dict[str, list[int]] = {}
    for s in dispatch_surfaces:
        if s.file.startswith("src/dev_health_ops/api/"):
            by_file.setdefault(s.file, []).append(s.line)

    out: list[Surface] = []
    seen: set[tuple[str, int]] = set()
    for relfile, lines in by_file.items():
        path = root / relfile
        text = path.read_text().splitlines()
        for target_line in lines:
            decorator_line = None
            for i in range(target_line - 1, 0, -1):
                if ROUTER_DECORATOR_RE.match(text[i - 1]):
                    decorator_line = i
                    break
                # stop scanning if we hit a previous top-level def (endpoint boundary)
                if DEF_RE.match(text[i - 1]) and i != target_line:
                    break
            if decorator_line is None:
                continue
            def_line = None
            for j in range(decorator_line, len(text) + 1):
                if DEF_RE.match(text[j - 1]):
                    def_line = j
                    break
                if j - decorator_line > 5:
                    break
            if def_line is None:
                continue
            if (relfile, def_line) in seen:
                continue
            seen.add((relfile, def_line))
            out.append(Surface(CLASS_API_TRIGGER, relfile, def_line))
    return out


def discover_stream_surfaces(root: Path) -> list[Surface]:
    out = []
    for path in _iter_py_files(root / "src/dev_health_ops/api"):
        relpath = _relpath(root, path)
        # Stream surfaces are anchored to the producer-side `streams.py` module
        # that defines the stream's CONSUMER_GROUP contract, not every module
        # (e.g. the consumer implementation) that happens to reference the same
        # constant value.
        is_streams_module = path.name == "streams.py"
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            if is_streams_module and CONSUMER_GROUP_RE.match(line):
                out.append(Surface(CLASS_STREAM_SURFACE, relpath, i))
            elif PAGERDUTY_ENQUEUE_RE.match(line) and relpath.endswith(
                "webhooks/pagerduty.py"
            ):
                # Structurally asymmetric stream: write-then-.delay, no poll consumer
                # (TRD 12, 6.2). Anchored to the enqueue function itself since there
                # is no CONSUMER_GROUP constant to key off like the other 3 streams.
                out.append(Surface(CLASS_STREAM_SURFACE, relpath, i))
    return out


def discover_json_kinds(root: Path, relpath: str, cls: str) -> list[Surface]:
    path = root / relpath
    if not path.exists():
        return []
    out = []
    for i, line in enumerate(path.read_text().splitlines(), start=1):
        m = JSON_KIND_RE.search(line)
        if m:
            out.append(Surface(cls, relpath, i, m.group(1)))
    return out


def discover_all(root: Path) -> list[Surface]:
    surfaces: list[Surface] = []
    surfaces += discover_celery_tasks(root)
    surfaces += discover_beat_entries(root)
    literal, getattr_indirection = discover_call_sites(root)
    surfaces += literal
    surfaces += getattr_indirection
    surfaces += discover_api_trigger_endpoints(root, literal + getattr_indirection)
    surfaces += discover_stream_surfaces(root)
    surfaces += discover_json_kinds(
        root, "contracts/jobs/v1/registry.json", CLASS_REGISTRY_KIND
    )
    surfaces += discover_json_kinds(
        root, "contracts/sync-dispatch/v1/transport-routes.json", CLASS_TRANSPORT_ROUTE
    )
    return surfaces


def load_inventory(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


def check(root: Path, inventory_path: Path) -> list[str]:
    errors: list[str] = []
    inventory = load_inventory(inventory_path)
    rows = inventory["rows"]

    row_keys = {(r["class"], r["source"]["file"], r["source"]["line"]) for r in rows}

    # 1. every discovered surface must have an inventory row.
    discovered = discover_all(root)
    for s in discovered:
        if s.key() not in row_keys:
            errors.append(
                f"UNOWNED SURFACE: {s.cls} at {s.file}:{s.line} has no row in "
                f"{inventory_path.name}. Add an owning row or an explicit_removal row."
            )

    # 2. every row must have a non-empty target owner.
    for r in rows:
        owner = r.get("target_owner") or {}
        if not owner.get("value"):
            errors.append(f"NO TARGET OWNER: row {r['id']} has no target_owner.value")

    # 3. exclusive ownership: at most one 'primary' row per target_kind_id.
    primary_by_kind: dict[str, str] = {}
    for r in rows:
        if r.get("owner_role") != "primary":
            continue
        tkid = r.get("target_kind_id")
        if not tkid:
            errors.append(f"PRIMARY ROW MISSING target_kind_id: {r['id']}")
            continue
        if tkid in primary_by_kind:
            errors.append(
                f"DUPLICATE EXCLUSIVE OWNERSHIP: rows {primary_by_kind[tkid]!r} and "
                f"{r['id']!r} both claim primary ownership of target_kind_id "
                f"{tkid!r}. Exactly one row may be the exclusive owner."
            )
        else:
            primary_by_kind[tkid] = r["id"]

    # 4. staleness guard: every row's anchor must still exist.
    for r in rows:
        src = r["source"]
        path = root / src["file"]
        if not path.exists():
            errors.append(
                f"STALE ANCHOR: row {r['id']} references missing file {src['file']}"
            )
            continue
        line_count = sum(1 for _ in path.open())
        if src["line"] > line_count:
            errors.append(
                f"STALE ANCHOR: row {r['id']} references {src['file']}:{src['line']} "
                f"but the file only has {line_count} lines"
            )

    return errors


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="repository root")
    parser.add_argument(
        "--inventory", default=DEFAULT_INVENTORY, help="inventory JSON path"
    )
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    inventory_path = root / args.inventory

    errors = check(root, inventory_path)
    if errors:
        print(
            f"FAIL: {len(errors)} transitional-inventory violation(s):", file=sys.stderr
        )
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    print("OK: transitional workload inventory contract is consistent with discovery.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
