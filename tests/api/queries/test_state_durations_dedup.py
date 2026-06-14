"""CHAOS-2377: state-duration readers must dedup re-run rows before summing.

The scheduled daily job (``run_daily_metrics_job``) now writes
``work_item_state_durations_daily`` on every run. That table is a plain
``MergeTree``, so re-running or backfilling the same day appends a *second*
copy of each (day, provider, work_scope_id, team_id, status) row with a fresh
``computed_at``. The /metrics Flow Sankey and aggregated Flame readers
aggregate with ``sum(...)``; without a per-key ``argMax(..., computed_at)``
dedup they would silently inflate flow weights and touched counts on every
re-run.

These tests pin the dedup shape on the two readers that previously summed the
raw table (the Operating Review reader already deduped). They capture the
emitted SQL and assert it collapses to the latest ``computed_at`` per natural
key before aggregating — mirroring ``test_clickhouse_computed_at_aliases``.
"""

from __future__ import annotations

from datetime import date
from typing import Any, cast

import pytest

import dev_health_ops.connectors  # noqa: F401  # break providers<->connectors cycle
from dev_health_ops.api.queries import aggregated_flame, metrics, sankey
from dev_health_ops.metrics.sinks.base import BaseMetricsSink

_NATURAL_KEY_GROUP_BY = "GROUP BY day, provider, work_scope_id, team_id, status"


def _assert_dedup_before_sum(query: str) -> None:
    # The raw table is read inside an inner subquery that collapses to the
    # latest computed_at per natural key, and the outer query sums the deduped
    # rows. A flat ``FROM work_item_state_durations_daily ... GROUP BY status``
    # with a top-level sum would double-count re-runs.
    normalized = " ".join(query.split())
    assert "argMax(items_touched, computed_at)" in normalized
    assert _NATURAL_KEY_GROUP_BY in normalized
    # The dedup subquery must sit *under* the outer sum(), i.e. the table is not
    # summed directly.
    table_pos = normalized.index("work_item_state_durations_daily")
    inner_group_pos = normalized.index(_NATURAL_KEY_GROUP_BY)
    assert inner_group_pos > table_pos, "dedup GROUP BY must follow the table read"


@pytest.mark.asyncio
async def test_sankey_state_status_counts_dedups_reruns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    async def fake_query_dicts(_sink: Any, query: str, _params: Any) -> list[Any]:
        captured["query"] = query
        return []

    monkeypatch.setattr(sankey, "query_dicts", fake_query_dicts)

    await sankey.fetch_state_status_counts(
        cast(BaseMetricsSink, object()),
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        scope_filter=" AND team_id = %(team_id)s",
        scope_params={"team_id": "core"},
        org_id="org-a",
    )

    _assert_dedup_before_sum(captured["query"])


@pytest.mark.asyncio
async def test_aggregated_flame_cycle_breakdown_dedups_reruns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[Any]:
        captured["query"] = query
        return []

    monkeypatch.setattr(aggregated_flame, "query_dicts", fake_query_dicts)

    await aggregated_flame.fetch_cycle_breakdown(
        object(),
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        team_id="core",
        org_id="org-a",
    )

    normalized = " ".join(captured["query"].split())
    assert "argMax(duration_hours, computed_at)" in normalized
    assert "argMax(items_touched, computed_at)" in normalized
    assert _NATURAL_KEY_GROUP_BY in normalized
    table_pos = normalized.index("work_item_state_durations_daily")
    inner_group_pos = normalized.index(_NATURAL_KEY_GROUP_BY)
    assert inner_group_pos > table_pos


@pytest.mark.asyncio
async def test_blocked_hours_dedups_reruns(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[Any]:
        captured["query"] = query
        return []

    monkeypatch.setattr(metrics, "query_dicts", fake_query_dicts)

    await metrics.fetch_blocked_hours(
        object(),
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        scope_filter=" AND team_id IN %(team_ids)s",
        scope_params={"team_ids": ["core"]},
        org_id="org-a",
    )

    normalized = " ".join(captured["query"].split())
    # blocked-hours is the panel Codex flagged for silent inflation.
    assert "argMax(duration_hours, computed_at)" in normalized
    assert _NATURAL_KEY_GROUP_BY in normalized
    table_pos = normalized.index("work_item_state_durations_daily")
    inner_group_pos = normalized.index(_NATURAL_KEY_GROUP_BY)
    assert inner_group_pos > table_pos


def _assert_value_expr_dedup(query: str, *, value_expr: str) -> None:
    """The /explain & /home generic readers must dedup re-runs for this table.

    Without the per-key argMax(..., computed_at) subquery the outer
    sum(duration_hours) double-counts every duplicate daily run/backfill, so the
    blocked_work headline (current AND comparison) inflates by the re-run count.
    """
    normalized = " ".join(query.split())
    assert "argMax(duration_hours, computed_at)" in normalized
    assert _NATURAL_KEY_GROUP_BY in normalized
    # The table must be read *inside* the dedup subquery, not summed directly.
    table_pos = normalized.index("work_item_state_durations_daily")
    inner_group_pos = normalized.index(_NATURAL_KEY_GROUP_BY)
    assert inner_group_pos > table_pos, "dedup GROUP BY must follow the table read"
    # org_id stays filtered in the inner WHERE (before the dedup GROUP BY).
    org_pos = normalized.index("org_id = %(org_id)s")
    assert org_pos < inner_group_pos, "org_id filter must sit inside the subquery"
    # The outer aggregation runs over the deduped alias, not the raw table.
    assert value_expr in normalized


@pytest.mark.asyncio
async def test_fetch_metric_value_dedups_state_durations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This is the 4th reader path (CHAOS-2377 re-review): /explain blocked_work
    # current + comparison headline both flow through fetch_metric_value.
    captured: dict[str, str] = {}

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[Any]:
        captured["query"] = query
        return [{"value": 0.0}]

    monkeypatch.setattr(metrics, "query_dicts", fake_query_dicts)

    await metrics.fetch_metric_value(
        cast(BaseMetricsSink, object()),
        table="work_item_state_durations_daily",
        column="duration_hours",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        scope_filter=" AND team_id IN %(team_ids)s",
        scope_params={"team_ids": ["core"]},
        aggregator="sum",
        org_id="org-a",
    )

    _assert_value_expr_dedup(captured["query"], value_expr="sum(duration_hours)")


@pytest.mark.asyncio
async def test_fetch_metric_series_dedups_state_durations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[Any]:
        captured["query"] = query
        return []

    monkeypatch.setattr(metrics, "query_dicts", fake_query_dicts)

    await metrics.fetch_metric_series(
        cast(BaseMetricsSink, object()),
        table="work_item_state_durations_daily",
        column="duration_hours",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        scope_filter=" AND team_id IN %(team_ids)s",
        scope_params={"team_ids": ["core"]},
        aggregator="sum",
        org_id="org-a",
    )

    _assert_value_expr_dedup(captured["query"], value_expr="sum(duration_hours)")
    # The series query still groups the deduped rows by day for the sparkline.
    normalized = " ".join(captured["query"].split())
    assert normalized.rstrip().endswith("GROUP BY day ORDER BY day")


@pytest.mark.asyncio
async def test_fetch_metric_value_leaves_other_tables_flat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The generic reader must NOT add argMax dedup for unaffected tables.
    captured: dict[str, str] = {}

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[Any]:
        captured["query"] = query
        return [{"value": 0.0}]

    monkeypatch.setattr(metrics, "query_dicts", fake_query_dicts)

    await metrics.fetch_metric_value(
        cast(BaseMetricsSink, object()),
        table="work_item_metrics_daily",
        column="items_completed",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        scope_filter=" AND team_id IN %(team_ids)s",
        scope_params={"team_ids": ["core"]},
        aggregator="sum",
        org_id="org-a",
    )

    normalized = " ".join(captured["query"].split())
    assert "argMax" not in normalized
    assert _NATURAL_KEY_GROUP_BY not in normalized
    assert "FROM work_item_metrics_daily WHERE" in normalized
