"""Contract guard: every legacy error-text persistence site goes through
``sanitize_error_text`` (CHAOS-2766).

There is no type-system way to enforce "this exception must be sanitized
before it reaches a DB column" -- ``sanitize_error_text`` looks like any
other function to mypy. This is the same problem CHAOS-2757's
``test_rate_limit_policy_doc.py`` solves for docs drift: a source-level guard
that runs in CI instead of relying on review discipline forever.

The check is a precise AST walk (not a `grep`, which would false-positive on
``logger.warning(..., extra={"error": str(exc)})`` sites -- those are
diagnostic logs, not persisted DB columns, and are intentionally out of this
ticket's scope) over the worker/sync modules that write
``sync_run_units.error`` and its siblings (``sync_runs.error``,
``sync_run_reference_discovery.error``, ``sync_dispatch_outbox.last_error``).
It flags any *raw* ``str(exc)``-shaped call or bare ``f"...{exc}"``
interpolation that is not inside a ``logger.<method>(...)`` call -- which is
exactly what a persistence site producing unsanitized text looks like, since
every real persistence site in these files has already been converted to
call ``sanitize_error_text`` instead of stringifying the exception directly.

Two internal-classification helpers (``_classify_error`` in
``workers/sync_units.py`` and ``_is_retryable_discovery_error`` in
``workers/reference_discovery.py``) are exempted: they lowercase
``str(exc)`` purely to pattern-match it against a fixed, curated vocabulary
(mirroring the existing ``_normalized_rate_limit_reason`` allow-list
precedent) and return only a category string -- the raw text itself is
discarded, never persisted.

``api/admin/routers/sync.py`` (added for the CHAOS-2766 codex review
finding: a Celery/broker enqueue-failure exception can embed the broker/
result-backend URL, credentials included, and ``JobRun.error`` /
``BackfillJob.error_message`` surface it through admin responses) is a large
router file with many unrelated ``except ... str(exc)`` sites for ordinary
HTTP validation errors (invalid cron expressions, GitLab project lookups,
...) that never touch the sync-run/job-run/backfill durable columns this
guard protects. Rather than whole-file scanning, it is SCOPED to only the
functions that actually write those columns (``_SCOPED_FUNCTIONS_BY_FILE``).
"""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]

_TARGET_FILES = (
    "src/dev_health_ops/workers/sync_units.py",
    "src/dev_health_ops/workers/reference_discovery.py",
    "src/dev_health_ops/workers/sync_reconciler.py",
    "src/dev_health_ops/sync/dispatch_outbox.py",
    "src/dev_health_ops/sync/trigger_routing.py",
    "src/dev_health_ops/sync/execution_trigger.py",
    "src/dev_health_ops/api/admin/routers/sync.py",
)

# Files that must actually import/use the helper -- guards against someone
# satisfying the negative check below by deleting the sanitize calls outright
# instead of the exception text they guard.
_FILES_REQUIRING_HELPER_USAGE = (
    "src/dev_health_ops/workers/sync_units.py",
    "src/dev_health_ops/workers/reference_discovery.py",
    "src/dev_health_ops/sync/dispatch_outbox.py",
    "src/dev_health_ops/sync/execution_trigger.py",
    "src/dev_health_ops/api/admin/routers/sync.py",
)

# Files where only SOME functions write the durable columns this guard
# protects (see module docstring) -- violations outside the listed functions
# are out of this guard's scope, not silently exempted from a coverage gap
# (the file has no OTHER str(exc)-into-a-sync/job/backfill-column site to
# miss). Absent from this dict means "scan the whole file", the default.
_SCOPED_FUNCTIONS_BY_FILE: dict[str, frozenset[str]] = {
    "src/dev_health_ops/api/admin/routers/sync.py": frozenset(
        {
            "_mark_job_run_failed",
            "_mark_backfill_job_failed",
            "trigger_sync_config",
            "trigger_sync_config_backfill",
        }
    ),
}

_EXC_VAR_NAMES = {"exc", "error", "e"}
_LOGGER_METHODS = {"debug", "info", "warning", "error", "exception", "critical", "log"}

# (file, function) pairs whose raw `str(exc)` is classification-only and
# never persisted -- see module docstring.
_CLASSIFICATION_ONLY_EXEMPTIONS = {
    ("src/dev_health_ops/workers/sync_units.py", "_classify_error"),
    (
        "src/dev_health_ops/workers/reference_discovery.py",
        "_is_retryable_discovery_error",
    ),
}


def _is_logger_call(node: ast.Call) -> bool:
    func = node.func
    if not isinstance(func, ast.Attribute) or func.attr not in _LOGGER_METHODS:
        return False
    target = func.value
    return isinstance(target, ast.Name) and target.id == "logger"


def _build_parent_map(tree: ast.AST) -> dict[int, ast.AST]:
    parent_of: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parent_of[id(child)] = parent
    return parent_of


def _enclosing_nodes(node: ast.AST, parent_of: dict[int, ast.AST]):
    current = node
    while True:
        parent = parent_of.get(id(current))
        if parent is None:
            return
        yield parent
        current = parent


def _enclosing_function_name(
    node: ast.AST, parent_of: dict[int, ast.AST]
) -> str | None:
    for ancestor in _enclosing_nodes(node, parent_of):
        if isinstance(ancestor, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return ancestor.name
    return None


def _is_inside_logger_call(node: ast.AST, parent_of: dict[int, ast.AST]) -> bool:
    return any(
        isinstance(ancestor, ast.Call) and _is_logger_call(ancestor)
        for ancestor in _enclosing_nodes(node, parent_of)
    )


def _raw_exception_stringify_violations(
    relative_path: str,
) -> list[tuple[int, str]]:
    path = _REPO_ROOT / relative_path
    tree = ast.parse(path.read_text(), filename=str(path))
    parent_of = _build_parent_map(tree)
    violations: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        flagged: ast.Call | ast.FormattedValue | None = None
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "str"
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id in _EXC_VAR_NAMES
        ):
            flagged = node
        elif (
            isinstance(node, ast.FormattedValue)
            and isinstance(node.value, ast.Name)
            and node.value.id in _EXC_VAR_NAMES
        ):
            flagged = node
        if flagged is None:
            continue

        if _is_inside_logger_call(flagged, parent_of):
            continue

        function_name = _enclosing_function_name(flagged, parent_of)
        if (relative_path, function_name) in _CLASSIFICATION_ONLY_EXEMPTIONS:
            continue

        scoped_functions = _SCOPED_FUNCTIONS_BY_FILE.get(relative_path)
        if scoped_functions is not None and function_name not in scoped_functions:
            continue

        violations.append((flagged.lineno, ast.unparse(flagged)))

    return violations


def test_no_raw_exception_stringification_outside_logging_or_sanitize():
    all_violations: dict[str, list[tuple[int, str]]] = {}
    for relative_path in _TARGET_FILES:
        violations = _raw_exception_stringify_violations(relative_path)
        if violations:
            all_violations[relative_path] = violations

    assert not all_violations, (
        "Found raw exception stringification outside of a logger call and "
        "outside the documented classification-only exemptions. Every "
        "persisted error-text column must go through "
        "dev_health_ops.sync.error_sanitize.sanitize_error_text instead of "
        f"str(exc)/f'{{exc}}' directly (CHAOS-2766): {all_violations!r}"
    )


def test_sanitize_error_text_helper_is_actually_used():
    for relative_path in _FILES_REQUIRING_HELPER_USAGE:
        source = (_REPO_ROOT / relative_path).read_text()
        assert "sanitize_error_text" in source, (
            f"{relative_path} no longer references sanitize_error_text -- "
            "the CHAOS-2766 guard above only proves no *raw* str(exc) sites "
            "remain, not that they were fixed by redaction rather than by "
            "deleting the persistence entirely."
        )
