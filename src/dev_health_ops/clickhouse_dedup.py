"""Canonical dedup sources for re-run-safe daily rollup tables.

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

Older daily tables remain append-only ``MergeTree`` tables.  For those tables,
``FINAL`` cannot collapse a repeated compute generation; readers instead select
the latest ``computed_at`` row for each physical daily key.  This keeps old
data and new re-drives compatible without a destructive table rewrite.

A static guard test (``tests/test_rerun_dedup_guard.py``) fails CI if a raw
``FROM``/``JOIN`` of the ReplacingMergeTree tables is introduced without
``FINAL``.
"""

from __future__ import annotations

# Tables converted to ReplacingMergeTree(computed_at) in CH migration 055.
RERUN_DEDUPED_DAILY_TABLES = frozenset(
    {
        "work_item_metrics_daily",
        "work_item_user_metrics_daily",
    }
)

# Legacy append-only daily tables written once per compute generation.  Values
# are logically unique at these keys, but a post-sync re-drive appends a newer
# generation.  The table order from migration 027 is used deliberately so an
# identity alias or a second repository is never collapsed with another row.
_APPEND_ONLY_DAILY_KEYS: dict[str, tuple[str, ...]] = {
    "repo_metrics_daily": ("org_id", "repo_id", "day"),
    "user_metrics_daily": ("org_id", "repo_id", "author_email", "day"),
    "team_metrics_daily": ("org_id", "team_id", "day"),
}


def dedup_from(table: str) -> str:
    """Return the ``FROM`` / ``JOIN`` source for ``table``.

    Appends ``FINAL`` when ``table`` is a re-run-deduplicated
    ReplacingMergeTree rollup.  For legacy append-only daily tables, returns a
    latest-generation source using ``ORDER BY computed_at DESC LIMIT 1 BY``.
    Aliases are preserved in both forms, so variable-table callers such as the
    quadrant reader cannot accidentally bypass deduplication with ``AS m``.
    """
    base_table, separator, alias = table.partition(" AS ")
    alias_sql = f" AS {alias}" if separator else ""
    if base_table in RERUN_DEDUPED_DAILY_TABLES:
        return f"{base_table} FINAL{alias_sql}"
    natural_key = _APPEND_ONLY_DAILY_KEYS.get(base_table)
    if natural_key is not None:
        source_alias = alias or base_table
        return f"""(
            SELECT *
            FROM {base_table}
            ORDER BY computed_at DESC
            LIMIT 1 BY {", ".join(natural_key)}
        ) AS {source_alias}"""
    return table
