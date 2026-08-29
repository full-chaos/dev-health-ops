"""CHAOS-4459 (codex review, P1): file_metrics_daily must be registered for
read-time dedup.

``file_metrics_daily`` is a plain (append-only) MergeTree, same class as
``dora_metrics_daily`` (see ``test_clickhouse_dedup_dora_registration.py``) --
a redrive/recompute (CHAOS-4459's ``metrics partition-recompute`` verb, or an
ordinary post-sync re-drive) writes a SECOND ``(org_id, repo_id, day, path)``
row with a fresher ``computed_at``, never replacing the first.

Before this fix, ``file_metrics_daily`` was in neither
``RERUN_DEDUPED_DAILY_TABLES`` nor ``_APPEND_ONLY_DAILY_KEYS`` --
``api/queries/heatmap.py``'s ``fetch_hotspot_risk``/``fetch_hotspot_evidence``
summed the raw table directly, with no dedup at all (codex round-1 review on
PR #1990: a redriven day's hotspot/churn scores silently doubled).
"""

from __future__ import annotations

from dev_health_ops.clickhouse_dedup import dedup_from


def test_file_metrics_daily_source_is_deduplicated_on_read() -> None:
    source = dedup_from("file_metrics_daily")
    assert source != "file_metrics_daily", (
        "dedup_from('file_metrics_daily') returned the RAW table name -- "
        "file_metrics_daily is not registered in either RERUN_DEDUPED_DAILY_TABLES "
        "or _APPEND_ONLY_DAILY_KEYS, so a hotspot/churn read double-counts any "
        "re-drive or partition-recompute."
    )
    assert "LIMIT 1 BY" in source
    assert "computed_at DESC" in source
    # The natural key must include path: file_metrics_daily's own sorting
    # key is (org_id, repo_id, day, path) (migration 027). Collapsing
    # without path would silently pick one file's row for every file in the
    # same repo/day.
    for column in ("org_id", "repo_id", "day", "path"):
        assert column in source, f"file_metrics_daily dedup key is missing {column}"


def test_file_metrics_daily_alias_is_preserved() -> None:
    # heatmap.py's readers reference `file_metrics_daily.repo_id` /
    # `file_metrics_daily.org_id` after the FROM clause -- dedup_from must
    # keep that alias so those references stay valid.
    source = dedup_from("file_metrics_daily")
    assert source.rstrip().endswith("AS file_metrics_daily")


def test_file_hotspot_daily_source_is_deduplicated_on_read() -> None:
    # CHAOS-4459 (codex review round 3): file_hotspot_daily is the
    # file_risk_hotspots family's own output table, missing from the
    # GENERIC report-registry dedup path (reports/charts.py's
    # dedup_from(definition.source_table)) even though its hand-rolled
    # readers already dedup correctly.
    source = dedup_from("file_hotspot_daily")
    assert source != "file_hotspot_daily", (
        "dedup_from('file_hotspot_daily') returned the RAW table name -- "
        "the generic report-registry chart path (reports/charts.py) reads "
        "it unguarded."
    )
    assert "LIMIT 1 BY" in source
    assert "computed_at DESC" in source
    for column in ("org_id", "repo_id", "day", "file_path"):
        assert column in source, f"file_hotspot_daily dedup key is missing {column}"
