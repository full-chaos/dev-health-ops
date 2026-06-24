"""Canonical dedup source for re-run-safe daily rollup tables (CHAOS-2645).

``work_item_metrics_daily`` and ``work_item_user_metrics_daily`` are
``ReplacingMergeTree(computed_at)`` (see ClickHouse migration 055). A sync that
restarts from scratch (a Celery retry, or a rate-limit deferral re-enqueue from
CHAOS-2644) re-writes a fresh row per ``(sorting-key, day)``; duplicate versions
are only collapsed by ``FINAL`` (or an eventual background merge).

Therefore EVERY read of these tables must deduplicate to the latest
``computed_at`` per sorting key, otherwise re-runs are double-counted. Use
:func:`dedup_from` in the ``FROM`` / ``JOIN`` clause of any read. The
metric-config read path in ``api/queries/metrics.py`` instead wraps these tables
in an ``argMax(..., computed_at)`` subquery (the established CHAOS-2377 pattern);
both approaches yield one logical row per key.

A static guard test (``tests/test_rerun_dedup_guard.py``) fails CI if a raw
``FROM``/``JOIN`` of these tables is introduced without ``FINAL``.
"""

from __future__ import annotations

# Tables converted to ReplacingMergeTree(computed_at) in CH migration 055.
RERUN_DEDUPED_DAILY_TABLES = frozenset(
    {
        "work_item_metrics_daily",
        "work_item_user_metrics_daily",
    }
)


def dedup_from(table: str) -> str:
    """Return the ``FROM`` / ``JOIN`` source for ``table``.

    Appends ``FINAL`` when ``table`` is a re-run-deduplicated
    ReplacingMergeTree rollup so a single re-run does not double-count; returns
    the bare name otherwise.
    """
    if table in RERUN_DEDUPED_DAILY_TABLES:
        return f"{table} FINAL"
    return table
