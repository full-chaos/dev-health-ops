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
# generation.  Each table's physical daily key is paired with ``org_id`` so an
# identity alias, tenant, or second repository is never collapsed into another
# logical row.
_APPEND_ONLY_DAILY_KEYS: dict[str, tuple[str, ...]] = {
    "repo_metrics_daily": ("org_id", "repo_id", "day"),
    "user_metrics_daily": ("org_id", "repo_id", "author_email", "day"),
    # CHAOS-4329: repo_id added so a team owning N repos keeps every repo's
    # row instead of collapsing to one via LIMIT 1 BY (org_id, team_id, day).
    # Legacy rows (written before repo_id existed) all share repo_id='' --
    # see migration 080's comment -- so they still collapse to exactly the
    # one row they always did; only NEW per-repo rows are kept apart. A
    # caller that wants the team-day TOTAL must SUM the additive counts
    # across the rows this yields and recompute the ratio -- it is not
    # additive -- never average the per-repo ratios directly.
    "team_metrics_daily": ("org_id", "team_id", "repo_id", "day"),
    "testops_pipeline_metrics_daily": ("org_id", "repo_id", "day"),
    "testops_test_metrics_daily": ("org_id", "repo_id", "day"),
    "testops_coverage_metrics_daily": ("org_id", "repo_id", "day"),
    "testops_quality_drag": ("org_id", "repo_id", "day"),
    # CHAOS-4242: was missing entirely -- dedup_from() fell through to
    # `return table`, handing reports/charts.py the RAW table. Natural key
    # matches compute_dora.py's own documented contract, "one row per
    # (repo, metric_name, day)" (metric_name included, or two metrics for
    # the same repo/day would collapse into one row).
    "dora_metrics_daily": ("org_id", "repo_id", "day", "metric_name"),
    # CHAOS-4246: registering these 5 closes the last gap -- every other
    # append-only daily table already had a dedup entry somewhere (this
    # registry or _DEDUP_BY_COMPUTED_AT in api/queries/metrics.py); these did
    # not, so a raw reader (home.py's "CI Success Rate" widget, confirmed) was
    # silently exposed to double-counting the moment a day was ever
    # recomputed. Recomputing a day is exactly what the CHAOS-4246 fix makes
    # happen (native_post_sync.go now re-triggers metrics.daily on a
    # cicd/deployments/incidents post-sync event), so this registration must
    # land in the same change, not after.
    "cicd_metrics_daily": ("org_id", "repo_id", "day"),
    "deploy_metrics_daily": ("org_id", "repo_id", "day"),
    "incident_metrics_daily": ("org_id", "repo_id", "day"),
    "testops_release_confidence": ("org_id", "repo_id", "day"),
    "testops_pipeline_stability": ("org_id", "repo_id", "day"),
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
