"""CHAOS-2645: variable-table readers must dedup the ReplacingMergeTree rollups.

The static guard (``tests/test_rerun_dedup_guard.py``) catches literal
``FROM <table>`` reads, but the quadrant and people query builders take the
table name as a parameter (``FROM {table}``), so the guard cannot see them.
These tests invoke those builders and assert the emitted SQL applies ``FINAL``
for the two re-run-deduplicated tables (and NOT for a plain ``MergeTree``
table), and that the metric-config path dedups via ``argMax(..., computed_at)``.
"""

from __future__ import annotations

from datetime import date
from typing import Any, cast

import pytest

import dev_health_ops.connectors  # noqa: F401  # break providers<->connectors cycle
from dev_health_ops.api.queries import metrics, people, quadrant
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


def _capture(monkeypatch: pytest.MonkeyPatch, module: Any) -> dict[str, str]:
    captured: dict[str, str] = {}

    async def fake_query_dicts(
        _sink: Any, query: str, _params: Any = None
    ) -> list[Any]:
        captured["query"] = query
        return []

    monkeypatch.setattr(module, "query_dicts", fake_query_dicts)
    return captured


@pytest.mark.asyncio
async def test_quadrant_metric_dedups_rmt_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, quadrant)
    await quadrant.fetch_quadrant_metric(
        cast(BaseMetricsSink, object()),
        table="work_item_metrics_daily",
        value_expr="sum(items_completed)",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        bucket="week",
        entity_expr="team_id",
        label_expr="team_name",
        org_id="org-a",
    )
    assert "work_item_metrics_daily FINAL" in captured["query"]


@pytest.mark.asyncio
async def test_quadrant_metric_dedups_append_only_table_with_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, quadrant)
    await quadrant.fetch_quadrant_metric(
        cast(BaseMetricsSink, object()),
        table="user_metrics_daily AS m",
        value_expr="sum(m.reviews_given)",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        bucket="week",
        entity_expr="m.team_id",
        label_expr="m.team_id",
        org_id="org-a",
    )
    normalized = " ".join(captured["query"].split())
    assert (
        "ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, author_email, day"
        in normalized
    )
    assert ") AS m" in normalized


@pytest.mark.asyncio
async def test_quadrant_metric_keeps_final_when_rmt_table_has_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, quadrant)
    await quadrant.fetch_quadrant_metric(
        cast(BaseMetricsSink, object()),
        table="work_item_user_metrics_daily AS m",
        value_expr="sum(m.items_completed)",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        bucket="week",
        entity_expr="m.user_identity",
        label_expr="m.user_identity",
        org_id="org-a",
    )
    assert "work_item_user_metrics_daily FINAL AS m" in captured["query"]


@pytest.mark.asyncio
async def test_quadrant_team_work_item_metric_uses_primary_attribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, quadrant)
    await quadrant.fetch_work_item_team_quadrant_metric(
        cast(BaseMetricsSink, object()),
        metric="throughput",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        bucket="week",
        org_id="org-a",
    )

    assert "FROM work_item_team_attributions FINAL" in captured["query"]
    assert "FROM work_item_cycle_times AS wct FINAL" in captured["query"]
    assert "is_primary = 1" in captured["query"]
    assert "(work_item_id, computed_at) IN" in captured["query"]
    assert "max(computed_at)" in captured["query"]
    assert "work_item_metrics_daily" not in captured["query"]


@pytest.mark.asyncio
async def test_person_metric_value_dedups_rmt_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, people)
    await people.fetch_person_metric_value(
        cast(BaseMetricsSink, object()),
        table="work_item_user_metrics_daily",
        column="items_completed",
        aggregator="sum",
        identity_column="user_identity",
        identities=["alice"],
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        org_id="org-a",
    )
    assert "work_item_user_metrics_daily FINAL" in captured["query"]


@pytest.mark.asyncio
async def test_person_metric_series_dedups_rmt_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, people)
    await people.fetch_person_metric_series(
        cast(BaseMetricsSink, object()),
        table="work_item_user_metrics_daily",
        column="items_completed",
        aggregator="sum",
        identity_column="user_identity",
        identities=["alice"],
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        org_id="org-a",
    )
    assert "work_item_user_metrics_daily FINAL" in captured["query"]


@pytest.mark.asyncio
async def test_person_metric_series_dedups_append_only_user_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, people)
    await people.fetch_person_metric_series(
        cast(BaseMetricsSink, object()),
        table="user_metrics_daily",
        column="loc_touched",
        aggregator="sum",
        identity_column="identity_id",
        identities=["alice"],
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        org_id="org-a",
    )
    normalized = " ".join(captured["query"].split())
    assert (
        "ORDER BY computed_at DESC LIMIT 1 BY org_id, repo_id, author_email, day"
        in normalized
    )
    assert ") AS user_metrics_daily" in normalized


@pytest.mark.asyncio
async def test_person_collaboration_dedups_append_only_user_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, people)
    await people.fetch_person_collaboration(
        cast(BaseMetricsSink, object()),
        identities=["alice"],
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        org_id="org-a",
    )
    normalized = " ".join(captured["query"].split())
    assert "argMax(reviews_given, computed_at) AS reviews_given" in normalized
    assert "GROUP BY day, repo_id, author_email" in normalized


@pytest.mark.asyncio
async def test_person_breakdown_dedups_rmt_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, people)
    await people.fetch_person_breakdown(
        cast(BaseMetricsSink, object()),
        table="work_item_user_metrics_daily",
        column="items_completed",
        aggregator="sum",
        identity_column="user_identity",
        identities=["alice"],
        group_expr="team_id",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        org_id="org-a",
    )
    assert "work_item_user_metrics_daily FINAL" in captured["query"]


@pytest.mark.asyncio
async def test_metric_series_argmax_dedups_rmt_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, metrics)
    await metrics.fetch_metric_series(
        cast(BaseMetricsSink, object()),
        table="work_item_metrics_daily",
        column="items_completed",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        scope_filter="",
        scope_params={},
        aggregator="sum",
        org_id="org-a",
    )
    normalized = " ".join(captured["query"].split())
    assert "argMax(items_completed, computed_at)" in normalized


@pytest.mark.asyncio
async def test_metric_series_argmax_dedups_append_only_repo_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _capture(monkeypatch, metrics)
    await metrics.fetch_metric_series(
        cast(BaseMetricsSink, object()),
        table="repo_metrics_daily",
        column="total_loc_touched",
        start_day=date(2026, 5, 1),
        end_day=date(2026, 5, 2),
        scope_filter="",
        scope_params={},
        aggregator="sum",
        org_id="org-a",
    )
    normalized = " ".join(captured["query"].split())
    assert "argMax(total_loc_touched, computed_at)" in normalized
    assert "GROUP BY day, repo_id" in normalized
