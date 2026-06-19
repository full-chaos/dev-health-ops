from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, cast

import pytest

from dev_health_ops.api.queries import investment as investment_queries
from dev_health_ops.api.services import investment_mix_explain
from dev_health_ops.api.services.provenance import (
    reset_mock_fixture_warning_state,
    warn_once_for_mock_fixture_rows,
)
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


def test_mock_fixture_rows_emit_exactly_one_warning(caplog):
    reset_mock_fixture_warning_state()
    rows = [{"categorization_model_version": "synthetic-v1", "provider": "jira"}]

    with caplog.at_level(logging.WARNING):
        warn_once_for_mock_fixture_rows(org_id="org-a", surface="investment", rows=rows)
        warn_once_for_mock_fixture_rows(org_id="org-a", surface="investment", rows=rows)

    warnings = [
        record
        for record in caplog.records
        if "Mock/fixture-sourced investment rows served" in record.getMessage()
    ]
    assert len(warnings) == 1


def test_real_rows_emit_no_warning(caplog):
    reset_mock_fixture_warning_state()
    rows = [{"categorization_model_version": "provider=openai;model=gpt-5-mini"}]

    with caplog.at_level(logging.WARNING):
        warn_once_for_mock_fixture_rows(org_id="org-a", surface="investment", rows=rows)

    assert not [
        record
        for record in caplog.records
        if "Mock/fixture-sourced investment rows served" in record.getMessage()
    ]


@pytest.mark.asyncio
async def test_mock_fixture_count_query_applies_category_filters(monkeypatch):
    captured: dict[str, Any] = {}

    async def fake_query_dicts(
        _sink: BaseMetricsSink, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"count": 1}]

    monkeypatch.setattr(investment_queries, "query_dicts", fake_query_dicts)

    count = await investment_queries.fetch_mock_fixture_investment_row_count(
        cast(BaseMetricsSink, object()),
        start_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_ts=datetime(2026, 1, 2, tzinfo=timezone.utc),
        scope_filter="",
        scope_params={},
        org_id="org-a",
        themes=["feature_delivery"],
        subcategories=["feature_delivery.customer"],
    )

    assert count == 1
    assert "ARRAY JOIN" in captured["sql"]
    assert "splitByChar('.', subcategory_kv.1)[1] IN %(themes)s" in captured["sql"]
    assert "subcategory_kv.1 IN %(subcategories)s" in captured["sql"]
    assert captured["params"]["themes"] == ["feature_delivery"]
    assert captured["params"]["subcategories"] == ["feature_delivery.customer"]


@pytest.mark.asyncio
async def test_investment_mix_token_write_is_offloaded_and_failure_warns(
    monkeypatch, caplog
):
    called: dict[str, Any] = {}

    async def fake_to_thread(func, /, *args, **kwargs):
        called["func"] = func
        called["kwargs"] = kwargs
        raise RuntimeError("clickhouse unavailable")

    monkeypatch.setattr(investment_mix_explain.asyncio, "to_thread", fake_to_thread)

    with caplog.at_level(logging.WARNING):
        await investment_mix_explain._persist_investment_mix_token_usage(
            db_url="clickhouse://localhost:8123/default",
            org_id="org-a",
            provider="openai",
            model="gpt-5-mini",
            input_tokens=10,
            output_tokens=5,
        )

    assert called["func"] is investment_mix_explain._write_investment_mix_token_usage
    assert called["kwargs"]["org_id"] == "org-a"
    assert any(
        "Token usage storage failed source=investment_mix_explain"
        in record.getMessage()
        for record in caplog.records
    )
