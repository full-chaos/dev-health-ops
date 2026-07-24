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
  4. a row's target_kind_id names something that was never actually
     discovered in source (closed-vocabulary check -- a row can't dodge #3 by
     renaming its target_kind_id to an unregistered variant);
  5. an inventory row's source anchor (file[:line]) no longer exists on disk,
     is past EOF, or its current line content no longer matches the pattern
     for that row's class (staleness / content drift).

This script deliberately re-derives discovery from source, independent of the
inventory file's own bookkeeping, so that adding an unowned Celery task
decorator or its aliases (`@celery_app.task(`, `@app.task(`, `@shared_task`),
Beat entry (including the indented conditional-rollout form), a literal
`.delay`/`.apply_async`/`send_task`/`.signature(` call, the bare celery-canvas
invocation form (`chord(...)()`/`chain(...)()`/`group(...)()`), a
bound-method-alias or `functools.partial` dispatch alias, the
`getattr(x, "delay"|"apply_async"|"send_task")` indirection form, an import of
`chain`/`chord`/`group` from celery (fail-closed: any new canvas usage
requires its own inventory row even where the exact invocation shape can't be
statically parsed), an API dispatch endpoint (REST or GraphQL resolver), a
registry kind, or a sync-dispatch transport route -- without updating the
inventory -- fails CI.

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

# A Celery task decorator, however the app/decorator object is named
# (`celery_app.task(`, `app.task(`, or the bare `@shared_task`). Scoped to
# src/dev_health_ops/workers -- verified zero non-Celery `@X.task(`/
# `@shared_task` decorators exist anywhere else in the tree, so broadening
# this beyond `celery_app` carries no current false-positive risk while
# covering an aliased-app-object registration form.
CELERY_TASK_DECORATOR_RE = re.compile(r"^\s*@(?:\w+\.)?(?:task|shared_task)\(")
BEAT_ENTRY_RE = re.compile(r'^    "([a-zA-Z0-9_-]+)":\s*\{')
# NOTE: intentionally NOT anchored to column zero -- the real conditional
# rollout-seam entry in config.py is indented under an `if env_flag(...):`.
BEAT_ENTRY_CONDITIONAL_RE = re.compile(r'^\s*beat_schedule\["([a-zA-Z0-9_-]+)"\]\s*=')
CALL_SITE_LITERAL_RE = re.compile(
    r"\.delay\(|\.apply_async\(|\.signature\(|send_task\("
)
GETATTR_INDIRECTION_RE = re.compile(
    r'getattr\([^,]+,\s*["\'](delay|apply_async|send_task)["\']\s*\)'
)
# `chord(`/`chain(`/`group(` invoked bare (no `.apply_async()`/`.delay()`
# suffix -- the canvas object itself is called, e.g. `chord([...], cb)()`).
# Anchors to the opening line of a multi-line canvas literal; single-line
# forms like `chain(a, b).apply_async()` are already matched by
# CALL_SITE_LITERAL_RE on that same line, so this only needs the
# "nothing else on the opening line" shape to avoid double-counting.
CANVAS_BARE_INVOCATION_RE = re.compile(r"^\s*(?:chord|chain|group)\(\s*$")
# Aliasing a dispatch method to a new name (`enqueue = task.apply_async`) --
# a *reference*, not a call: nothing may follow the method name but
# whitespace/comment. An actual call (`x = task.apply_async(...)`) has a `(`
# immediately after and is already covered by CALL_SITE_LITERAL_RE.
BOUND_ALIAS_RE = re.compile(
    r"^\s*[A-Za-z_][A-Za-z0-9_]*(?:\s*:\s*[\w.\[\], ]+)?\s*=\s*"
    r"[A-Za-z_][\w.]*\.(?:apply_async|delay|send_task)\s*$"
)
PARTIAL_ALIAS_RE = re.compile(
    r"\bpartial\(\s*[A-Za-z_][\w.]*\.(?:apply_async|delay|send_task)\b"
)
CELERY_CANVAS_IMPORT_RE = re.compile(r"^from celery(?:\.canvas)?\s+import\s+(.+)$")
_CANVAS_NAMES = {"chain", "chord", "group"}
ROUTER_DECORATOR_RE = re.compile(r"^\s*@router\.(get|post|put|patch|delete)\(")
DEF_RE = re.compile(r"^\s*(async\s+def|def)\s+\w+\(")
DEF_NAME_RE = re.compile(r"^\s*(?:async\s+def|def)\s+(\w+)\(")
CONSUMER_GROUP_VALUE_RE = re.compile(r'^CONSUMER_GROUP\s*=\s*"([^"]+)"')
PAGERDUTY_ENQUEUE_RE = re.compile(r"^def _enqueue_event\(")
PAGERDUTY_STREAM_NAME = "pagerduty_direct_dispatch"
JSON_KIND_RE = re.compile(r'"kind":\s*"([^"]+)"')
ONE_LINE_STRING_RE = re.compile(r'^\s*("""|\'\'\')(?:(?!\1).)*\1\s*$')

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
CLASS_CANVAS_IMPORT = "celery_canvas_import"


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


def _iter_code_lines_from(lines: list[str]):
    """Yield (line_number, line) skipping lines inside a multi-line
    triple-quoted block. Naive but effective toggle: counts \"\"\"/''' per
    physical line to track docstring state. Good enough for discovery
    purposes -- worst case a rare false negative on an unusual docstring
    shape, never a false unowned-surface failure from documentation prose
    being mistaken for code."""
    in_docstring = False
    for i, line in enumerate(lines, start=1):
        starts_in_docstring = in_docstring
        triple_count = line.count('"""') + line.count("'''")
        if triple_count % 2 == 1:
            in_docstring = not in_docstring
        if starts_in_docstring:
            continue
        yield i, line


def _iter_code_lines(path: Path):
    yield from _iter_code_lines_from(path.read_text().splitlines())


def _is_code_line(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith("#")


def _strip_line_noise(line: str) -> str:
    """Suppress two known false-positive shapes before pattern matching:
    a whole one-line docstring/string statement (e.g.
    `\"\"\"Use task.delay() here.\"\"\"`), and a trailing `#` comment (e.g.
    `value = 1  # task.apply_async()`). Does NOT blank ordinary string
    literals inside a real expression -- the getattr(x, "apply_async")
    indirection form needs its quotes intact to match at all."""
    if ONE_LINE_STRING_RE.match(line):
        return ""
    out = []
    quote = None
    i = 0
    n = len(line)
    while i < n:
        ch = line[i]
        if quote:
            out.append(ch)
            if ch == "\\" and i + 1 < n:
                out.append(line[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch == "#":
            break
        if ch in ("'", '"'):
            quote = ch
        out.append(ch)
        i += 1
    return "".join(out)


def _task_name_after_decorator(lines: list[str], decorator_line: int) -> str | None:
    """The decorator may itself span several lines (kwargs one per line);
    walk forward to the first def/async def line and take its name."""
    for j in range(decorator_line, min(decorator_line + 20, len(lines)) + 1):
        m = DEF_NAME_RE.match(lines[j - 1])
        if m:
            return m.group(1)
    return None


def discover_celery_tasks(root: Path) -> list[Surface]:
    out = []
    for path in _iter_py_files(root / "src/dev_health_ops/workers"):
        relpath = _relpath(root, path)
        lines = path.read_text().splitlines()
        for i, line in _iter_code_lines_from(lines):
            if CELERY_TASK_DECORATOR_RE.match(line):
                name = _task_name_after_decorator(lines, i) or ""
                out.append(Surface(CLASS_CELERY_TASK, relpath, i, name))
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


_CONTINUATION_RE = re.compile(r"^\s*\)+\s*\.(delay|apply_async|signature)\(")


def discover_call_sites(root: Path) -> tuple[list[Surface], list[Surface]]:
    literal, getattr_indirection = [], []
    for path in _iter_py_files(root / "src/dev_health_ops"):
        relpath = _relpath(root, path)
        last_match_line = -10
        for i, line in _iter_code_lines(path):
            if not _is_code_line(line):
                continue
            code = _strip_line_noise(line)
            if not code.strip():
                continue
            if GETATTR_INDIRECTION_RE.search(code):
                getattr_indirection.append(Surface(CLASS_CALL_SITE_GETATTR, relpath, i))
                last_match_line = i
                continue
            if CANVAS_BARE_INVOCATION_RE.match(code):
                literal.append(Surface(CLASS_CALL_SITE_LITERAL, relpath, i))
                last_match_line = i
                continue
            if BOUND_ALIAS_RE.match(code) or PARTIAL_ALIAS_RE.search(code):
                literal.append(Surface(CLASS_CALL_SITE_LITERAL, relpath, i))
                last_match_line = i
                continue
            if CALL_SITE_LITERAL_RE.search(code):
                if _CONTINUATION_RE.match(code) and i - last_match_line <= 4:
                    # closing-paren continuation of a multi-line signature(...)/
                    # chain(...)/chord(...) call whose opening line already matched;
                    # the earlier line is the real anchor, so this isn't a second
                    # independent surface.
                    continue
                literal.append(Surface(CLASS_CALL_SITE_LITERAL, relpath, i))
                last_match_line = i
    return literal, getattr_indirection


def discover_celery_canvas_imports(root: Path) -> list[Surface]:
    """Fail-closed guard: importing chain/chord/group from celery anywhere in
    the application means *some* canvas dispatch exists in that module, even
    where the specific invocation shape (bare call, stored then invoked
    later, passed to a helper, etc.) can't be statically enumerated. Every
    such import must have its own inventory row."""
    out = []
    for path in _iter_py_files(root / "src/dev_health_ops"):
        relpath = _relpath(root, path)
        for i, line in _iter_code_lines(path):
            m = CELERY_CANVAS_IMPORT_RE.match(line)
            if not m:
                continue
            names = {
                part.strip().split(" as ")[0].strip() for part in m.group(1).split(",")
            }
            if names & _CANVAS_NAMES:
                out.append(Surface(CLASS_CANVAS_IMPORT, relpath, i))
    return out


def _enclosing_def_line(def_lines: list[int], target_line: int) -> int | None:
    """The innermost preceding def for a flat (non-nested) module -- the
    largest def line at or before target_line."""
    candidates = [d for d in def_lines if d <= target_line]
    return max(candidates) if candidates else None


def _decorators_by_def_line(text: list[str]) -> dict[int, list[int]]:
    """Map each def/async def line to the decorator-start line numbers (in
    source order) that immediately precede it -- correctly handling a
    decorator whose argument list spans multiple lines (e.g.
    `@router.post(\\n    "/x",\\n    status_code=202,\\n)`), which a naive
    "walk up one line at a time" scan mis-stops on the closing `)` line.

    Tracks bracket depth with a single forward pass: while depth > 0 we are
    inside an unclosed decorator (or def-signature) argument list, so
    intermediate lines are skipped rather than treated as scan-stopping
    non-decorator content.
    """
    result: dict[int, list[int]] = {}
    pending: list[int] = []
    depth = 0
    for i, line in enumerate(text, start=1):
        if depth == 0:
            stripped = line.strip()
            if not stripped:
                pass
            elif stripped.startswith("@"):
                pending.append(i)
            elif DEF_RE.match(line):
                result[i] = pending
                pending = []
            else:
                # Some other top-level statement -- don't leak decorators
                # across unrelated code.
                pending = []
        depth += line.count("(") + line.count("[") + line.count("{")
        depth -= line.count(")") + line.count("]") + line.count("}")
        depth = max(depth, 0)
    return result


def discover_api_trigger_endpoints(
    root: Path, dispatch_surfaces: list[Surface]
) -> list[Surface]:
    """For every dispatch call site under src/dev_health_ops/api, find its
    enclosing function and anchor to that endpoint's @router.<method>(...)
    decorator line -- matching how the Wave-0 audit anchored REST API trigger
    endpoints (verified against all 7 REST rows: the anchor is the decorator
    line itself, not the def line or the call site).

    Two fallbacks match the two shapes that aren't "call site directly inside
    a @router-decorated function":

    - GraphQL resolver modules (src/dev_health_ops/api/graphql/resolvers/)
      have no per-function route decorator at all -- strawberry wires
      resolvers up elsewhere. There the call site's own line is the anchor,
      matching the one audit row anchored this way (reports.py).
    - A dispatch call site can sit inside a shared, undecorated helper that
      several @router-decorated endpoints call (e.g. webhooks/router.py's
      three provider routes all funnel through `_dispatch_webhook_task`).
      One hop of same-file call-graph lookup finds those callers; the
      lowest-line caller's decorator represents the shared dispatch path,
      matching the audit's choice to inventory that fan-in as one row.
    """
    by_file: dict[str, list[int]] = {}
    for s in dispatch_surfaces:
        if s.file.startswith("src/dev_health_ops/api/"):
            by_file.setdefault(s.file, []).append(s.line)

    def _router_anchor(
        decorators_by_def: dict[int, list[int]], text: list[str], def_line: int
    ) -> int | None:
        for d in decorators_by_def.get(def_line, []):
            if ROUTER_DECORATOR_RE.match(text[d - 1]):
                return d
        return None

    out: list[Surface] = []
    seen: set[tuple[str, int]] = set()
    for relfile, lines in sorted(by_file.items()):
        path = root / relfile
        text = path.read_text().splitlines()
        decorators_by_def = _decorators_by_def_line(text)
        # Only module-level (column-zero) defs qualify as an endpoint/resolver
        # boundary -- FastAPI routes and GraphQL resolvers are always
        # top-level functions, whereas a dispatch call site can sit inside a
        # nested local helper (e.g. a closure defined inside the endpoint
        # body for a tier-limit check); such a helper's own def line is not
        # itself an endpoint and must not be treated as one.
        def_lines = sorted(
            d for d in decorators_by_def if not text[d - 1][:1].isspace()
        )
        is_graphql_resolver = "/graphql/resolvers/" in relfile

        for target_line in sorted(lines):
            own_def = _enclosing_def_line(def_lines, target_line)
            if own_def is None:
                continue

            anchor = _router_anchor(decorators_by_def, text, own_def)

            if anchor is None and is_graphql_resolver:
                anchor = target_line

            if anchor is None:
                # One-hop call-graph fallback: this dispatch sits in an
                # undecorated helper -- find its callers in the same file.
                name_match = DEF_NAME_RE.match(text[own_def - 1])
                if name_match:
                    fname = name_match.group(1)
                    call_re = re.compile(rf"\b{re.escape(fname)}\(")
                    caller_anchors = []
                    for i, line in enumerate(text, start=1):
                        if i == own_def or not call_re.search(line):
                            continue
                        caller_def = _enclosing_def_line(def_lines, i)
                        if caller_def is None:
                            continue
                        caller_anchor = _router_anchor(
                            decorators_by_def, text, caller_def
                        )
                        if caller_anchor is not None:
                            caller_anchors.append(caller_anchor)
                    if caller_anchors:
                        anchor = min(caller_anchors)

            if anchor is None:
                continue
            if (relfile, anchor) in seen:
                continue
            seen.add((relfile, anchor))
            out.append(Surface(CLASS_API_TRIGGER, relfile, anchor))
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
            if is_streams_module:
                m = CONSUMER_GROUP_VALUE_RE.match(line)
                if m:
                    out.append(Surface(CLASS_STREAM_SURFACE, relpath, i, m.group(1)))
            elif PAGERDUTY_ENQUEUE_RE.match(line) and relpath.endswith(
                "webhooks/pagerduty.py"
            ):
                # Structurally asymmetric stream: write-then-.delay, no poll consumer
                # (TRD 12, 6.2). Anchored to the enqueue function itself since there
                # is no CONSUMER_GROUP constant to key off like the other 3 streams.
                out.append(
                    Surface(CLASS_STREAM_SURFACE, relpath, i, PAGERDUTY_STREAM_NAME)
                )
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
    surfaces += discover_celery_canvas_imports(root)
    return surfaces


def load_inventory(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Staleness / content-drift validation: per-class "does this anchored line
# still look like the kind of surface this row claims to be" checks.
# ---------------------------------------------------------------------------


def _call_site_literal_content_ok(line: str) -> bool:
    code = _strip_line_noise(line)
    return bool(
        CALL_SITE_LITERAL_RE.search(code)
        or CANVAS_BARE_INVOCATION_RE.match(code)
        or BOUND_ALIAS_RE.match(code)
        or PARTIAL_ALIAS_RE.search(code)
    )


def _api_trigger_content_ok(line: str) -> bool:
    code = _strip_line_noise(line)
    return bool(
        ROUTER_DECORATOR_RE.match(line)
        or CALL_SITE_LITERAL_RE.search(code)
        or GETATTR_INDIRECTION_RE.search(code)
    )


def _kind_content_ok(line: str, expected_name: str) -> bool:
    m = JSON_KIND_RE.search(line)
    return bool(m and m.group(1) == expected_name)


def _stream_content_ok(line: str) -> bool:
    return bool(CONSUMER_GROUP_VALUE_RE.match(line) or PAGERDUTY_ENQUEUE_RE.match(line))


def _canvas_import_content_ok(line: str) -> bool:
    m = CELERY_CANVAS_IMPORT_RE.match(line)
    if not m:
        return False
    names = {part.strip().split(" as ")[0].strip() for part in m.group(1).split(",")}
    return bool(names & _CANVAS_NAMES)


_CONTENT_CHECKERS = {
    CLASS_CELERY_TASK: lambda line, name: bool(CELERY_TASK_DECORATOR_RE.match(line)),
    CLASS_BEAT_ENTRY: lambda line, name: bool(BEAT_ENTRY_RE.match(line)),
    CLASS_BEAT_ENTRY_CONDITIONAL: lambda line, name: bool(
        BEAT_ENTRY_CONDITIONAL_RE.match(line)
    ),
    CLASS_CALL_SITE_LITERAL: lambda line, name: _call_site_literal_content_ok(line),
    CLASS_CALL_SITE_GETATTR: lambda line, name: bool(
        GETATTR_INDIRECTION_RE.search(_strip_line_noise(line))
    ),
    CLASS_API_TRIGGER: lambda line, name: _api_trigger_content_ok(line),
    CLASS_REGISTRY_KIND: lambda line, name: _kind_content_ok(line, name),
    CLASS_TRANSPORT_ROUTE: lambda line, name: _kind_content_ok(line, name),
    CLASS_STREAM_SURFACE: lambda line, name: _stream_content_ok(line),
    CLASS_CANVAS_IMPORT: lambda line, name: _canvas_import_content_ok(line),
}


def _closed_vocabulary(discovered: list[Surface]) -> dict[str, set[str]]:
    """Real, source-derived vocabulary for each target_kind_id namespace,
    built from the same discovery pass already used for the unowned-surface
    check -- not a hand-maintained allowlist. A row can't dodge duplicate-
    primary detection by renaming its target_kind_id to an unregistered
    variant (e.g. `kind:metrics.remaining.capacity-v2`) since that name would
    never appear in this vocabulary."""
    vocab: dict[str, set[str]] = {
        "kind": set(),
        "beat": set(),
        "stream": set(),
        "route": set(),
        "task": set(),
    }
    for s in discovered:
        if not s.name:
            continue
        if s.cls == CLASS_REGISTRY_KIND:
            vocab["kind"].add(s.name)
        elif s.cls in (CLASS_BEAT_ENTRY, CLASS_BEAT_ENTRY_CONDITIONAL):
            vocab["beat"].add(s.name)
        elif s.cls == CLASS_STREAM_SURFACE:
            vocab["stream"].add(s.name)
        elif s.cls == CLASS_TRANSPORT_ROUTE:
            vocab["route"].add(s.name)
        elif s.cls == CLASS_CELERY_TASK:
            vocab["task"].add(s.name)
    return vocab


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

    # 4. closed-vocabulary check: target_kind_id must name something real.
    vocabulary = _closed_vocabulary(discovered)
    for r in rows:
        tkid = r.get("target_kind_id")
        if not tkid:
            continue
        prefix, sep, name = tkid.partition(":")
        if not sep:
            errors.append(
                f"INVALID target_kind_id: row {r['id']} has {tkid!r} "
                "(expected 'prefix:name' shape)"
            )
            continue
        allowed = vocabulary.get(prefix)
        if allowed is not None and name not in allowed:
            errors.append(
                f"UNKNOWN target_kind_id: row {r['id']} claims {tkid!r} but "
                f"{name!r} was not discovered as a real {prefix} in source "
                "(closed-vocabulary check)"
            )

    # 5. staleness guard: anchor must exist, be in range, and its current
    #    line content must still look like the row's declared class.
    for r in rows:
        src = r["source"]
        path = root / src["file"]
        if not path.exists():
            errors.append(
                f"STALE ANCHOR: row {r['id']} references missing file {src['file']}"
            )
            continue
        lines = path.read_text().splitlines()
        if src["line"] > len(lines) or src["line"] < 1:
            errors.append(
                f"STALE ANCHOR: row {r['id']} references {src['file']}:{src['line']} "
                f"but the file only has {len(lines)} lines"
            )
            continue
        checker = _CONTENT_CHECKERS.get(r["class"])
        if checker is not None:
            content = lines[src["line"] - 1]
            if not checker(content, r.get("surface", "")):
                errors.append(
                    f"STALE ANCHOR: row {r['id']} references {src['file']}:"
                    f"{src['line']} but that line no longer matches a "
                    f"{r['class']} surface (content drift -- re-anchor or "
                    "remove this row)"
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
