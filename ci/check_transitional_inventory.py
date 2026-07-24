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
# Two shapes: the opening line of a multi-line canvas literal (nothing else
# on that line), and a complete single-line bare invocation
# (`chain(a, b)()`) -- single-line forms WITH a `.apply_async()`/`.delay()`
# suffix are already matched by CALL_SITE_LITERAL_RE on that same line, so
# this is only for the bare-call-with-no-method-suffix shape. The per-file
# bare/qualified name sets built by `_scan_celery_canvas` extend both shapes
# to aliased (`c = chord; c(...)()`) and qualified (`canvas.chord(...)()`)
# invocations too -- see discover_call_sites and _bare_invocation_re_for.
CANVAS_BARE_INVOCATION_RE = re.compile(
    r"^\s*(?:chord|chain|group)\(\s*$"
    r"|^\s*(?:chord|chain|group)\(.*\)\(\s*\)\s*$"
)
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
_CANVAS_NAMES = {"chain", "chord", "group"}
# `from celery import chain, chord`, `from celery.canvas import chain as c`,
# or `from celery import canvas` (binds the *module*, not the functions --
# handled specially in _scan_celery_canvas). Operates on a logically-joined
# import line (see _join_logical_import_lines) so a parenthesized multi-line
# import list is seen whole regardless of wrapping.
CELERY_FROM_IMPORT_RE = re.compile(r"^from celery(?:\.canvas)?\s+import\s+(.+)$")
# `import celery.canvas` / `import celery.canvas as X`.
CELERY_IMPORT_CANVAS_MODULE_RE = re.compile(
    r"^import\s+celery\.canvas(?:\s+as\s+(\w+))?\s*$"
)
# `canvas = importlib.import_module("celery.canvas")` (or unassigned) -- only
# the static-string-literal form is checkable; a non-literal argument (e.g.
# `importlib.import_module(name_from_config)`) is undecidable at this level
# and is a documented known limitation.
IMPORTLIB_CANVAS_IMPORT_RE = re.compile(
    r"^(?:(\w+)\s*=\s*)?importlib\.import_module\(\s*[\"']celery\.canvas[\"']\s*\)\s*$"
)
# A bare `name = other_name` assignment, used to propagate a canvas binding
# through a simple local alias (`c = chord` or `cv = canvas`).
_BARE_NAME_ALIAS_RE = re.compile(r"^(\w+)\s*=\s*(\w+)\s*$")
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


def _docstring_safe_lines(lines: list[str]) -> list[str]:
    """Same length/positions as `lines`, but any line inside a multi-line
    triple-quoted block is blanked out. Lets line-number-sensitive scanners
    (import joining, decorator/def mapping) ignore documentation prose
    without shifting anything's line number."""
    safe = list(lines)
    in_docstring = False
    for i, line in enumerate(lines):
        starts_in_docstring = in_docstring
        triple_count = line.count('"""') + line.count("'''")
        if triple_count % 2 == 1:
            in_docstring = not in_docstring
        if starts_in_docstring:
            safe[i] = ""
    return safe


def _join_logical_import_lines(lines: list[str]) -> list[tuple[int, str]]:
    """Join a parenthesized multi-line `from X import (...)` (or a plain
    `import ...`) into one logical (start_line, text) entry, so the
    celery-canvas-import scanner sees the whole name list regardless of how
    it's wrapped."""
    out: list[tuple[int, str]] = []
    i = 0
    n = len(lines)
    while i < n:
        stripped = lines[i].strip()
        if stripped.startswith("from ") or stripped.startswith("import "):
            start = i
            text = stripped
            depth = stripped.count("(") - stripped.count(")")
            j = i
            while depth > 0 and j + 1 < n:
                j += 1
                text += " " + lines[j].strip()
                depth += lines[j].count("(") - lines[j].count(")")
            out.append((start + 1, text))
            i = j + 1
            continue
        i += 1
    return out


def _scan_celery_canvas(lines: list[str]) -> tuple[list[int], set[str], set[str]]:
    """Per-file celery-canvas binding scan. Returns:

    - import_lines: line numbers that are themselves a celery-canvas import
      surface (fail-closed guard row required).
    - direct_names: local names that are themselves canvas callables
      (`chain`/`chord`/`group`, an `as`-aliased import of one, or a simple
      `c = chord`-style local alias) -- invoked bare as `name(...)`.
    - module_aliases: local names that refer to the celery.canvas *module*
      (`from celery import canvas`, `import celery.canvas as X`,
      `importlib.import_module("celery.canvas")`, or a simple alias of any
      of those) -- invoked qualified as `alias.chord(...)`.

    Known limitation: `importlib.import_module(some_variable)` with a
    non-literal argument is undecidable statically and is not detected (see
    docs/architecture/transitional-workload-inventory.md).
    """
    safe_lines = _docstring_safe_lines(lines)
    import_lines: list[int] = []
    direct_names: set[str] = set()
    module_aliases: set[str] = set()

    for line_no, text in _join_logical_import_lines(safe_lines):
        m = CELERY_FROM_IMPORT_RE.match(text)
        if m:
            saw_canvas = False
            body = m.group(1).strip()
            if body.startswith("("):
                body = body[1:]
            body = body.rstrip(")").rstrip(",")
            for part in body.split(","):
                part = part.strip()
                if not part:
                    continue
                original, _, alias = part.partition(" as ")
                original = original.strip()
                local = alias.strip() or original
                if original in _CANVAS_NAMES:
                    direct_names.add(local)
                    saw_canvas = True
                elif original == "canvas":
                    module_aliases.add(local)
                    saw_canvas = True
            if saw_canvas:
                import_lines.append(line_no)
            continue
        m = CELERY_IMPORT_CANVAS_MODULE_RE.match(text)
        if m:
            module_aliases.add(m.group(1) or "canvas")
            import_lines.append(line_no)
            continue

    # importlib.import_module("celery.canvas") isn't an `import`/`from`
    # statement lexically, so it isn't found by the joiner above -- it can
    # appear as a bare call or (commonly) assigned to a variable anywhere in
    # the module.
    for i, line in enumerate(safe_lines, start=1):
        m = IMPORTLIB_CANVAS_IMPORT_RE.match(line.strip())
        if m:
            module_aliases.add(m.group(1) or "canvas")
            import_lines.append(i)

    # Propagate through simple bare-name aliases (`c = chord`, `cv = canvas`,
    # or a chain of those) to a fixed point.
    for _ in range(4):
        changed = False
        for line in safe_lines:
            m = _BARE_NAME_ALIAS_RE.match(line.strip())
            if not m:
                continue
            target, source = m.group(1), m.group(2)
            if source in direct_names and target not in direct_names:
                direct_names.add(target)
                changed = True
            if source in module_aliases and target not in module_aliases:
                module_aliases.add(target)
                changed = True
        if not changed:
            break

    return import_lines, direct_names, module_aliases


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


def _qualified_canvas_bare_invocation_re(module_aliases: set[str]) -> re.Pattern | None:
    if not module_aliases:
        return None
    alt = "|".join(re.escape(a) for a in sorted(module_aliases))
    return re.compile(
        rf"^\s*(?:{alt})\.(?:chord|chain|group)\(\s*$"
        rf"|^\s*(?:{alt})\.(?:chord|chain|group)\(.*\)\(\s*\)\s*$"
    )


def _aliased_canvas_bare_invocation_re(direct_names: set[str]) -> re.Pattern | None:
    extra = direct_names - _CANVAS_NAMES
    if not extra:
        return None
    alt = "|".join(re.escape(a) for a in sorted(extra))
    return re.compile(rf"^\s*(?:{alt})\(\s*$|^\s*(?:{alt})\(.*\)\(\s*\)\s*$")


def _call_site_bare_invocation_patterns(lines: list[str]) -> list[re.Pattern]:
    """The complete, per-file set of patterns that make a line a bare
    celery-canvas invocation (direct, aliased, or module-qualified). This is
    the SINGLE source of truth shared by discover_call_sites (fresh
    discovery) and the call_site_literal content-drift re-verifier -- Codex
    round-3 HIGH-3 found the two had drifted apart, so discovery would find
    a qualified/aliased canvas call, but re-verifying the row the gate then
    demanded for it would fail with STALE ANCHOR because the re-verifier
    only knew the bare, unaliased pattern."""
    _, direct_names, module_aliases = _scan_celery_canvas(lines)
    patterns = [CANVAS_BARE_INVOCATION_RE]
    qualified_re = _qualified_canvas_bare_invocation_re(module_aliases)
    if qualified_re is not None:
        patterns.append(qualified_re)
    aliased_re = _aliased_canvas_bare_invocation_re(direct_names)
    if aliased_re is not None:
        patterns.append(aliased_re)
    return patterns


def discover_call_sites(root: Path) -> tuple[list[Surface], list[Surface]]:
    literal, getattr_indirection = [], []
    for path in _iter_py_files(root / "src/dev_health_ops"):
        relpath = _relpath(root, path)
        lines = path.read_text().splitlines()
        canvas_patterns = _call_site_bare_invocation_patterns(lines)
        last_match_line = -10
        for i, line in _iter_code_lines_from(lines):
            if not _is_code_line(line):
                continue
            code = _strip_line_noise(line)
            if not code.strip():
                continue
            if GETATTR_INDIRECTION_RE.search(code):
                getattr_indirection.append(Surface(CLASS_CALL_SITE_GETATTR, relpath, i))
                last_match_line = i
                continue
            if any(p.match(code) for p in canvas_patterns):
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
    """Fail-closed guard: importing chain/chord/group from celery (directly,
    via the celery.canvas submodule, module-qualified, or via a static
    importlib.import_module("celery.canvas") literal) anywhere in the
    application means *some* canvas dispatch exists in that module, even
    where the specific invocation shape (bare call, qualified call, aliased
    call, stored then invoked later, passed to a helper, etc.) can't be
    statically enumerated. Every such import must have its own inventory
    row."""
    out = []
    for path in _iter_py_files(root / "src/dev_health_ops"):
        relpath = _relpath(root, path)
        lines = path.read_text().splitlines()
        import_lines, _, _ = _scan_celery_canvas(lines)
        for i in import_lines:
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


def _router_aliases(lines: list[str]) -> set[str]:
    """Local names that refer to the module's APIRouter instance. Seeded
    with the repo-wide convention `router`, extended through simple bare
    aliasing (`r = router`) to a fixed point. Known limitation: an alias
    introduced any other way (a function return value, a container/dict
    lookup, an attribute on another object) is not tracked -- see
    docs/architecture/transitional-workload-inventory.md."""
    aliases = {"router"}
    safe_lines = _docstring_safe_lines(lines)
    for _ in range(4):
        changed = False
        for line in safe_lines:
            m = _BARE_NAME_ALIAS_RE.match(line.strip())
            if not m:
                continue
            target, source = m.group(1), m.group(2)
            if source in aliases and target not in aliases:
                aliases.add(target)
                changed = True
        if not changed:
            break
    return aliases


def _router_decorator_re_for(aliases: set[str]) -> re.Pattern:
    alt = "|".join(re.escape(a) for a in sorted(aliases))
    return re.compile(rf"^\s*@(?:{alt})\.(get|post|put|patch|delete)\(")


def _add_route_re_for(aliases: set[str]) -> re.Pattern:
    alt = "|".join(re.escape(a) for a in sorted(aliases))
    return re.compile(rf"^\s*(?:{alt})\.(?:add_api_route|add_route)\(")


def discover_api_trigger_endpoints(
    root: Path, dispatch_surfaces: list[Surface]
) -> list[Surface]:
    """For every dispatch call site under src/dev_health_ops/api, find its
    enclosing function and anchor to that endpoint's @router.<method>(...)
    decorator line -- matching how the Wave-0 audit anchored REST API trigger
    endpoints (verified against all 7 REST rows: the anchor is the decorator
    line itself, not the def line or the call site).

    Three fallbacks/extensions cover shapes that aren't "call site directly
    inside a @router-decorated function":

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
    - `router.add_api_route(...)`/`.add_route(...)` register an endpoint
      without a decorator at all. Every such call, in every api/ module (not
      only ones already holding a discovered dispatch call site), is its own
      directly-discovered api_trigger_endpoint surface -- registering an
      already-inventoried helper this way must still force a new row.

    Both the decorator and the add_api_route/add_route forms are aliasing-
    aware: `r = router; @r.post(...)` and `r.add_api_route(...)` are
    recognized via `_router_aliases`, not just the literal name `router`.
    """
    by_file: dict[str, list[int]] = {}
    for s in dispatch_surfaces:
        if s.file.startswith("src/dev_health_ops/api/"):
            by_file.setdefault(s.file, []).append(s.line)

    def _router_anchor(
        decorators_by_def: dict[int, list[int]],
        text: list[str],
        def_line: int,
        router_re: re.Pattern,
    ) -> int | None:
        for d in decorators_by_def.get(def_line, []):
            if router_re.match(text[d - 1]):
                return d
        return None

    def _transitive_router_anchor(
        decorators_by_def: dict[int, list[int]],
        text: list[str],
        def_lines: list[int],
        start_def: int,
        router_re: re.Pattern,
        max_hops: int = 12,
    ) -> int | None:
        """Same-file call-graph BFS: a dispatch two (or more) helper calls
        away from its @router-decorated endpoint is an ORDINARY shape in
        this codebase (e.g. billing/router.py's stripe_webhook ->
        _process_subscription_event -> _enqueue_billing_notification), not
        an exotic one -- so this is a fixed-point search over same-file call
        edges, not a single hop. Bounded by max_hops purely as a runaway
        backstop (real call chains here are 1-3 hops); an unbounded search
        would be fine too on files this size. Cross-file call graphs remain
        out of scope (see docs/architecture/transitional-workload-inventory.md
        Known limitations)."""
        visited = {start_def}
        frontier = [start_def]
        found: list[int] = []
        for _ in range(max_hops):
            if not frontier or found:
                break
            next_frontier: list[int] = []
            for def_line in frontier:
                name_match = DEF_NAME_RE.match(text[def_line - 1])
                if not name_match:
                    continue
                fname = name_match.group(1)
                call_re = re.compile(rf"\b{re.escape(fname)}\(")
                for i, line in enumerate(text, start=1):
                    if i == def_line or not call_re.search(line):
                        continue
                    caller_def = _enclosing_def_line(def_lines, i)
                    if caller_def is None or caller_def in visited:
                        continue
                    visited.add(caller_def)
                    caller_anchor = _router_anchor(
                        decorators_by_def, text, caller_def, router_re
                    )
                    if caller_anchor is not None:
                        found.append(caller_anchor)
                    else:
                        next_frontier.append(caller_def)
            frontier = next_frontier
        return min(found) if found else None

    out: list[Surface] = []
    seen: set[tuple[str, int]] = set()
    for relfile, lines in sorted(by_file.items()):
        path = root / relfile
        text = path.read_text().splitlines()
        aliases = _router_aliases(text)
        router_re = _router_decorator_re_for(aliases)
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

            anchor = _router_anchor(decorators_by_def, text, own_def, router_re)

            if anchor is None and is_graphql_resolver:
                anchor = target_line

            if anchor is None:
                # Transitive call-graph fallback: this dispatch sits in an
                # undecorated helper, possibly several calls deep, inside a
                # @router-decorated endpoint.
                anchor = _transitive_router_anchor(
                    decorators_by_def, text, def_lines, own_def, router_re
                )

            if anchor is None:
                continue
            if (relfile, anchor) in seen:
                continue
            seen.add((relfile, anchor))
            out.append(Surface(CLASS_API_TRIGGER, relfile, anchor))

    # add_api_route/add_route registrations: scanned across every api/ module,
    # independent of whether that module already holds a discovered dispatch
    # call site, since the registration itself is the surface being missed.
    for path in _iter_py_files(root / "src/dev_health_ops/api"):
        relfile = _relpath(root, path)
        text = path.read_text().splitlines()
        aliases = _router_aliases(text)
        add_route_re = _add_route_re_for(aliases)
        for i, line in _iter_code_lines_from(text):
            if add_route_re.match(_strip_line_noise(line)):
                if (relfile, i) in seen:
                    continue
                seen.add((relfile, i))
                out.append(Surface(CLASS_API_TRIGGER, relfile, i))
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
# still look like the kind of surface this row claims to be, AND does it
# still name the SAME specific task/target the row records" checks. Every
# checker has the signature (lines, line_no, row) -> bool so it can look
# beyond the single anchor line (a multi-line statement's identifying token
# is often a few lines below the anchor) and compare against the row's own
# recorded identity, not just re-derive the same shape regex that let the
# row through discovery in the first place.
# ---------------------------------------------------------------------------

# Hard backstop on how many lines a single logical statement may span before
# _statement_window gives up extending it (real statements here are a
# handful of lines; this only guards against a pathological unbalanced-
# bracket file).
_MAX_STATEMENT_LINES = 20

_SKIP_TOKEN_SURFACE_MARKERS = (
    "chain(",
    "chord(",
    "group(",
    "dynamic",
    "unclear",
    "Go bridge",
    "signature (",
)
_QUOTED_SIGNATURE_TOKEN_RE = re.compile(r"signature\(\s*['\"]([\w.]+)['\"]")
_GETATTR_TOKEN_RE = re.compile(r"getattr\(\s*([A-Za-z_]\w*)")
_DOTTED_CALL_TOKEN_RE = re.compile(
    r"^([A-Za-z_]\w*)\.(?:delay|apply_async|signature)\b"
)


def _expected_dispatch_token(row: dict) -> str | None:
    """Best-effort extraction of the specific task/target identifier a
    call_site_literal/call_site_getattr_indirection row's `surface` records,
    so content-drift can check for that specific name rather than only the
    generic dispatch shape. Returns None for surfaces that don't name a
    single concrete target (chain/chord/group compound descriptions, or a
    genuinely dynamic/unclear dispatch) -- those keep the shape-only check,
    a documented known limitation."""
    surface = row.get("surface", "")
    if any(marker in surface for marker in _SKIP_TOKEN_SURFACE_MARKERS):
        return None
    m = _QUOTED_SIGNATURE_TOKEN_RE.search(surface)
    if m:
        return m.group(1)
    m = _GETATTR_TOKEN_RE.search(surface)
    if m:
        return m.group(1)
    m = _DOTTED_CALL_TOKEN_RE.match(surface)
    if m:
        return m.group(1)
    return None


def _celery_task_content_ok(lines: list[str], line_no: int, row: dict) -> bool:
    if not CELERY_TASK_DECORATOR_RE.match(lines[line_no - 1]):
        return False
    expected = row.get("surface")
    if not expected:
        return True
    return _task_name_after_decorator(lines, line_no) == expected


def _beat_entry_content_ok(lines: list[str], line_no: int, row: dict) -> bool:
    m = BEAT_ENTRY_RE.match(lines[line_no - 1])
    if not m:
        return False
    expected = row.get("surface")
    return not expected or m.group(1) == expected


def _beat_entry_conditional_content_ok(
    lines: list[str], line_no: int, row: dict
) -> bool:
    m = BEAT_ENTRY_CONDITIONAL_RE.match(lines[line_no - 1])
    if not m:
        return False
    expected = row.get("surface")
    return not expected or m.group(1) == expected


def _statement_window(lines: list[str], line_no: int) -> str:
    """The logical statement starting at `line_no`: lines that are all part
    of one bracket-balanced expression, with each line's trailing `#`
    comment (and any whole one-line docstring/string) stripped BEFORE
    matching -- so a comment or log message mentioning an old task name
    can't false-pass content-drift (Codex round-3 HIGH-2: an unrestricted
    forward window matched the old name anywhere, including comments).
    Bounded by _MAX_STATEMENT_LINES as a runaway backstop."""
    parts = []
    depth = 0
    for i in range(line_no - 1, min(line_no - 1 + _MAX_STATEMENT_LINES, len(lines))):
        code = _strip_line_noise(lines[i])
        parts.append(code)
        depth += code.count("(") + code.count("[") + code.count("{")
        depth -= code.count(")") + code.count("]") + code.count("}")
        if depth <= 0:
            break
    return "\n".join(parts)


def _dispatch_token_present(lines: list[str], line_no: int, row: dict) -> bool:
    token = _expected_dispatch_token(row)
    if token is None:
        return True
    return token in _statement_window(lines, line_no)


def _call_site_literal_content_ok(lines: list[str], line_no: int, row: dict) -> bool:
    code = _strip_line_noise(lines[line_no - 1])
    canvas_patterns = _call_site_bare_invocation_patterns(lines)
    shape_ok = bool(
        CALL_SITE_LITERAL_RE.search(code)
        or any(p.match(code) for p in canvas_patterns)
        or BOUND_ALIAS_RE.match(code)
        or PARTIAL_ALIAS_RE.search(code)
    )
    if not shape_ok:
        return False
    return _dispatch_token_present(lines, line_no, row)


def _call_site_getattr_content_ok(lines: list[str], line_no: int, row: dict) -> bool:
    if not GETATTR_INDIRECTION_RE.search(_strip_line_noise(lines[line_no - 1])):
        return False
    return _dispatch_token_present(lines, line_no, row)


_REST_SURFACE_RE = re.compile(r"^(?:GET|POST|PUT|PATCH|DELETE)\s+(\S+)$")


def _expected_rest_path(row: dict) -> str | None:
    """The specific route path a REST api_trigger_endpoint row records, so
    content-drift can confirm the SAME path still appears near the anchor,
    not just that some decorator/registration is still there (Codex
    round-3 MED: editing the path string used to pass). Returns None for
    surfaces that don't cleanly encode one path -- e.g. the compressed
    `POST /webhooks/github|gitlab|jira` notation for three sibling routes
    sharing one dispatch helper, which isn't a single literal to check --
    that stays shape-only, a documented known limitation."""
    surface = row.get("surface", "")
    if "|" in surface:
        return None
    m = _REST_SURFACE_RE.match(surface)
    return m.group(1) if m else None


_ROUTE_PATH_LITERAL_RE = re.compile(r"""["']([^"']+)["']""")


def _actual_route_path(lines: list[str], line_no: int) -> str | None:
    """The first quoted string literal in the decorator/registration
    statement -- the route's local path argument. APIRouter path arguments
    are frequently only a *suffix* of the row's recorded full effective path
    (the router's own `prefix=` mount, declared elsewhere, supplies the
    rest -- e.g. `@router.post("/{binding_id}")` under a router mounted at
    `/webhooks/pagerduty`), so this is compared to the row's expected path
    via a suffix relationship, not exact equality."""
    m = _ROUTE_PATH_LITERAL_RE.search(_statement_window(lines, line_no))
    return m.group(1) if m else None


def _api_trigger_content_ok(lines: list[str], line_no: int, row: dict) -> bool:
    line = lines[line_no - 1]
    aliases = _router_aliases(lines)
    router_re = _router_decorator_re_for(aliases)
    add_route_re = _add_route_re_for(aliases)
    code = _strip_line_noise(line)
    if router_re.match(line) or add_route_re.match(code):
        # REST registration (decorator or add_api_route/add_route): also
        # confirm the specific path still appears near the anchor, when the
        # row's surface encodes exactly one path. Compared via a suffix
        # relationship (see _actual_route_path) to tolerate router mount
        # prefixes composing with a locally-declared path suffix.
        expected_path = _expected_rest_path(row)
        if expected_path is None:
            return True
        actual_path = _actual_route_path(lines, line_no)
        if actual_path is None:
            return False
        return expected_path.endswith(actual_path) or actual_path.endswith(
            expected_path
        )
    if CALL_SITE_LITERAL_RE.search(code) or GETATTR_INDIRECTION_RE.search(code):
        # GraphQL-resolver-anchored row: the anchor line IS the dispatch call
        # site, so the same specific-name check applies.
        return _dispatch_token_present(lines, line_no, row)
    return False


def _kind_content_ok(lines: list[str], line_no: int, row: dict) -> bool:
    m = JSON_KIND_RE.search(lines[line_no - 1])
    return bool(m and m.group(1) == row.get("surface"))


def _stream_content_ok(lines: list[str], line_no: int, row: dict) -> bool:
    line = lines[line_no - 1]
    m = CONSUMER_GROUP_VALUE_RE.match(line)
    if m:
        tkid = row.get("target_kind_id") or ""
        expected = tkid.partition(":")[2] if tkid.startswith("stream:") else None
        return not expected or m.group(1) == expected
    return bool(PAGERDUTY_ENQUEUE_RE.match(line))


_IMPORT_SURFACE_NAMES_RE = re.compile(r"^from celery(?:\.canvas)?\s+import\s+(.+)$")


def _expected_import_names(row: dict) -> set[str] | None:
    """The specific name(s) a celery_canvas_import row's `surface` records
    (e.g. `from celery import chain, chord` -> {"chain", "chord"}), so
    content-drift can catch swapping the imported names (Codex round-3 MED:
    `from celery import chain, chord` -> `from celery import group` used to
    still pass, since the old check only asked "is this still some canvas
    import"). Returns None for a surface shape this can't cleanly parse
    (e.g. a future row describing `import celery.canvas as X` or an
    importlib form) -- those stay shape-only, a documented known
    limitation."""
    m = _IMPORT_SURFACE_NAMES_RE.match(row.get("surface", ""))
    if not m:
        return None
    return {
        part.strip().split(" as ")[0].strip()
        for part in m.group(1).split(",")
        if part.strip()
    }


def _canvas_import_content_ok(lines: list[str], line_no: int, row: dict) -> bool:
    import_lines, _, _ = _scan_celery_canvas(lines)
    if line_no not in import_lines:
        return False
    expected = _expected_import_names(row)
    if expected is None:
        return True
    safe_lines = _docstring_safe_lines(lines)
    for start, text in _join_logical_import_lines(safe_lines):
        if start != line_no:
            continue
        m = CELERY_FROM_IMPORT_RE.match(text)
        if not m:
            # Not a `from celery import ...` shape at this line (e.g. it's
            # now `import celery.canvas` or an importlib form instead) --
            # the row's surface text no longer describes this line's shape
            # at all, which is itself a form of drift.
            return False
        body = m.group(1).strip()
        if body.startswith("("):
            body = body[1:]
        body = body.rstrip(")").rstrip(",")
        actual = {
            part.strip().split(" as ")[0].strip()
            for part in body.split(",")
            if part.strip()
        }
        return actual == expected
    return True


_CONTENT_CHECKERS = {
    CLASS_CELERY_TASK: _celery_task_content_ok,
    CLASS_BEAT_ENTRY: _beat_entry_content_ok,
    CLASS_BEAT_ENTRY_CONDITIONAL: _beat_entry_conditional_content_ok,
    CLASS_CALL_SITE_LITERAL: _call_site_literal_content_ok,
    CLASS_CALL_SITE_GETATTR: _call_site_getattr_content_ok,
    CLASS_API_TRIGGER: _api_trigger_content_ok,
    CLASS_REGISTRY_KIND: _kind_content_ok,
    CLASS_TRANSPORT_ROUTE: _kind_content_ok,
    CLASS_STREAM_SURFACE: _stream_content_ok,
    CLASS_CANVAS_IMPORT: _canvas_import_content_ok,
}

# A closed, curated allowlist of task names that may legitimately be the
# exclusive ("primary") owner of a `task:` target_kind_id -- these are the
# six standalone Celery tasks the TRD gap analysis calls out as needing
# their own native/removal decision with no Beat entry, registry kind, or
# transport route already claiming them (see STANDALONE_PRIMARY in the
# inventory generator). Deliberately NOT derived from "every discovered
# Celery task name": that set has 46 members today, and an unclaimed one
# (e.g. run_complexity_job) is not automatically a valid primary-ownership
# target just because it happens to exist (Codex round-2 MED-1).
TRD_MAPPED_TASK_TARGETS = frozenset(
    {
        "health_check",
        "sync_team_drift",
        "process_pagerduty_webhook_event",
        "flush_external_ingest_recompute",
        "run_daily_metrics",
        "dispatch_external_ingest_recompute_bridge",
    }
)


def _closed_vocabulary(discovered: list[Surface]) -> dict[str, set[str]]:
    """Real, source-derived vocabulary for each target_kind_id namespace,
    built from the same discovery pass already used for the unowned-surface
    check -- not a hand-maintained allowlist, EXCEPT `task`, which is
    deliberately the curated TRD_MAPPED_TASK_TARGETS allowlist rather than
    every discovered Celery task name (see its docstring). A row can't dodge
    duplicate-primary detection by renaming its target_kind_id to an
    unregistered variant (e.g. `kind:metrics.remaining.capacity-v2`, or an
    arbitrary other discovered-but-unclaimed task name) since that name
    would never appear in this vocabulary."""
    vocab: dict[str, set[str]] = {
        "kind": set(),
        "beat": set(),
        "stream": set(),
        "route": set(),
        "task": set(TRD_MAPPED_TASK_TARGETS),
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
        # NOTE: CLASS_CELERY_TASK is intentionally NOT folded in here -- see
        # TRD_MAPPED_TASK_TARGETS above.
    return vocab


def check(root: Path, inventory_path: Path) -> list[str]:
    errors: list[str] = []
    inventory = load_inventory(inventory_path)
    rows = inventory["rows"]

    row_keys = {(r["class"], r["source"]["file"], r["source"]["line"]) for r in rows}

    # 1. bidirectional row/discovery-key identity, per class. Both a missed
    #    surface (discovered, no row) and a phantom row (row, nothing
    #    discovered there) are errors -- comparing only totals or one
    #    direction lets a miss and a phantom net to zero (Codex round-2
    #    MED-3).
    discovered = discover_all(root)
    discovered_keys = {s.key() for s in discovered}
    for s in discovered:
        if s.key() not in row_keys:
            errors.append(
                f"UNOWNED SURFACE: {s.cls} at {s.file}:{s.line} has no row in "
                f"{inventory_path.name}. Add an owning row or an explicit_removal row."
            )
    for r in rows:
        rk = (r["class"], r["source"]["file"], r["source"]["line"])
        if rk not in discovered_keys:
            errors.append(
                f"PHANTOM ROW: row {r['id']} claims a {r['class']} surface at "
                f"{r['source']['file']}:{r['source']['line']} that independent "
                "discovery did not find there. Fix the anchor or remove the row."
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
    #    An unrecognized *prefix* is itself an error (not silently skipped)
    #    -- a row can't invent a new namespace to dodge validation
    #    (Codex round-2 MED-1).
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
        if prefix not in vocabulary:
            errors.append(
                f"UNKNOWN target_kind_id PREFIX: row {r['id']} claims {tkid!r} "
                f"but {prefix!r} is not a recognized target_kind_id namespace "
                f"(expected one of {sorted(vocabulary)!r})"
            )
            continue
        if name not in vocabulary[prefix]:
            errors.append(
                f"UNKNOWN target_kind_id: row {r['id']} claims {tkid!r} but "
                f"{name!r} was not discovered as a real {prefix} in source "
                "(closed-vocabulary check)"
            )

    # 5. staleness guard: anchor must exist, be in range, and its current
    #    line content must still look like the row's declared class AND
    #    still name the same specific task/target the row records.
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
        if checker is not None and not checker(lines, src["line"], r):
            errors.append(
                f"STALE ANCHOR: row {r['id']} references {src['file']}:"
                f"{src['line']} but that line no longer matches a "
                f"{r['class']} surface, or no longer names the same target "
                "(content drift -- re-anchor or remove this row)"
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
