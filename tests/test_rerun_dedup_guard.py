"""Static guard: re-run-deduplicated daily rollups must be read with FINAL.

``work_item_metrics_daily`` and ``work_item_user_metrics_daily`` are
``ReplacingMergeTree(computed_at)`` (ClickHouse migration 055). A sync re-run
writes a fresh row per ``(sorting-key, day)``; any raw ``FROM`` / ``JOIN`` of
these tables that is not immediately followed by ``FINAL`` silently
double-counts those re-runs.

New reads must use :func:`dev_health_ops.clickhouse_dedup.dedup_from` (which
appends ``FINAL``) or an explicit ``FINAL``. The metric-config read path in
``api/queries/metrics.py`` instead wraps the tables in a tested
``argMax(..., computed_at)`` subquery and is allow-listed below.
"""

from __future__ import annotations

import re
from pathlib import Path

from dev_health_ops.clickhouse_dedup import RERUN_DEDUPED_DAILY_TABLES

_SRC = Path(__file__).resolve().parent.parent / "src" / "dev_health_ops"

# Files allowed to reference these tables in a FROM/JOIN without an adjacent
# FINAL (they dedup another tested way, or define the helper itself).
_ALLOWLIST_FILES = {
    "clickhouse_dedup.py",
    "api/queries/metrics.py",  # argMax(..., computed_at) dedup subquery (tested)
}
# Directories whose files are exempt (migrations operate on backtick-quoted,
# variable table names, never a literal ``FROM work_item_metrics_daily``).
_ALLOWLIST_DIRS = ("migrations/",)


def _iter_source_files():
    for ext in ("*.py", "*.sql"):
        for path in _SRC.rglob(ext):
            rel = path.relative_to(_SRC).as_posix()
            if rel in _ALLOWLIST_FILES:
                continue
            if any(rel.startswith(d) for d in _ALLOWLIST_DIRS):
                continue
            yield rel, path


def test_rerun_deduped_rollups_are_read_with_final() -> None:
    # FROM/JOIN <table> NOT immediately followed by FINAL. \s spans newlines, so
    # multi-line FROM clauses and a FINAL on the next line are both handled.
    patterns = {
        table: re.compile(
            rf"\b(?:FROM|JOIN)\s+{re.escape(table)}\b(?!\s+FINAL\b)",
            re.IGNORECASE,
        )
        for table in RERUN_DEDUPED_DAILY_TABLES
    }

    violations: list[str] = []
    for rel, path in _iter_source_files():
        text = path.read_text(encoding="utf-8")
        for table, pattern in patterns.items():
            for match in pattern.finditer(text):
                # A FROM/JOIN without an adjacent FINAL is still safe if the
                # enclosing SELECT deduplicates via argMax(..., computed_at)
                # (the established CHAOS-2377 pattern). Accept that mechanism.
                window = text[max(0, match.start() - 800) : match.start()].lower()
                if "argmax(" in window and "computed_at" in window:
                    continue
                line = text.count("\n", 0, match.start()) + 1
                violations.append(
                    f"{rel}:{line}: `{match.group(0).strip()}` is missing FINAL "
                    f"({table} is ReplacingMergeTree; use clickhouse_dedup.dedup_from)"
                )

    assert not violations, (
        "Re-run-deduplicated daily rollups read without FINAL "
        "(double-counts sync re-runs):\n  " + "\n  ".join(sorted(violations))
    )
