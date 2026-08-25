"""CHAOS-4242: dora_metrics_daily must be registered for read-time dedup.

``dora_metrics_daily`` is a plain (append-only) MergeTree -- deliberately, by
chris's own ruling: "the append was there to allow metric calculations to be
recalculated if jobs failed and more fidelity. aka Metrics algos took care of
dedupe." That is the WRITE-side half of the contract; this pins the READ-side
half, which was missing: ``reports/charts.py`` builds every chart's source
via ``clickhouse_dedup.dedup_from(definition.source_table)``, and
``reports/metric_registry.py`` names ``dora_metrics_daily`` as exactly such a
source (``RegistrySource(DORAMetricsRecord, "dora_metrics_daily", ...)``).
``dedup_from`` only deduplicates a table it recognizes -- either
``RERUN_DEDUPED_DAILY_TABLES`` (ReplacingMergeTree + FINAL) or
``_APPEND_ONLY_DAILY_KEYS`` (a LIMIT-1-BY-computed_at subquery). Before this
fix, ``dora_metrics_daily`` was in neither: ``dedup_from`` fell through to
its last line, ``return table``, and handed callers the RAW table name --
exactly the shape CHAOS-4242's own recovery procedure (retrying a discarded
job re-computes and re-writes a day) would silently double-count on the next
chart render.
"""

from __future__ import annotations

from dev_health_ops.clickhouse_dedup import dedup_from
from dev_health_ops.reports import metric_registry


def test_dora_metrics_daily_source_is_deduplicated_on_read() -> None:
    source = dedup_from("dora_metrics_daily")
    assert source != "dora_metrics_daily", (
        "dedup_from('dora_metrics_daily') returned the RAW table name -- "
        "dora_metrics_daily is not registered in either RERUN_DEDUPED_DAILY_TABLES "
        "or _APPEND_ONLY_DAILY_KEYS, so a chart reading it double-counts any "
        "re-drive (a retried CHAOS-4242 recovery job, a re-run backfill)."
    )
    assert "LIMIT 1 BY" in source
    assert "computed_at DESC" in source
    # The natural key must include metric_name: compute_dora.py's own
    # contract is "one row per (repo, metric_name, day)" -- collapsing
    # without metric_name would silently pick one metric's value for all of
    # them.
    for column in ("org_id", "repo_id", "day", "metric_name"):
        assert column in source, f"dora_metrics_daily dedup key is missing {column}"


def test_every_dora_report_registry_source_is_a_known_dedup_table() -> None:
    # reports/charts.py always wraps RegistrySource.source_table in
    # dedup_from(); a registry source whose base table dedup_from cannot
    # recognize renders exactly like this ticket's regression, silently, on
    # first chart view rather than failing any test.
    from dev_health_ops.clickhouse_dedup import (
        _APPEND_ONLY_DAILY_KEYS,
        RERUN_DEDUPED_DAILY_TABLES,
    )

    known = RERUN_DEDUPED_DAILY_TABLES | _APPEND_ONLY_DAILY_KEYS.keys()
    for source in metric_registry.REGISTRY_SOURCES:
        if "dora" in source.source_table:
            assert source.source_table in known, (
                f"{source.source_table} is a report registry source dedup_from "
                "does not recognize -- it will be read raw"
            )
