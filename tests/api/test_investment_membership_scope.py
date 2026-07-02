from __future__ import annotations

from typing import Any, cast

import pytest

import dev_health_ops.api.queries.investment_membership_scope as scope
from dev_health_ops.metrics.sinks.base import BaseMetricsSink


def test_extract_scope_state_accepts_stale_fallback() -> None:
    state = scope.extract_scope_state_from_rows(
        [{"scope_mode": "unscoped_fallback", "lag_seconds": 3660}]
    )

    assert state.scope_mode == "unscoped_fallback"
    assert state.lag_seconds == 3660


def test_extract_scope_state_defaults_unknown_mode_to_no_marker() -> None:
    state = scope.extract_scope_state_from_rows(
        [{"scope_mode": "unexpected", "lag_seconds": 5}]
    )

    assert state.scope_mode == "unscoped_no_marker"
    assert state.lag_seconds == 5


def test_scope_state_uses_strict_membership_freshness_boundary() -> None:
    sql = scope.INVESTMENT_MEMBERSHIP_SCOPE_STATE_CTES

    assert "latest_investment_computed_at <= latest_run_completed_at" in sql
    assert "latest_investment_computed_at > latest_run_completed_at" in sql
    assert "INTERVAL" not in sql


@pytest.mark.asyncio
async def test_record_stale_investment_membership_scope_emits_metric(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[int, str]] = []

    async def fake_query_dicts(
        _sink: BaseMetricsSink, query: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        assert "investment_membership_scope_state AS" in query
        assert params["org_id"] == "org-1"
        return [{"scope_mode": "unscoped_fallback", "lag_seconds": 42}]

    def fake_record(*, lag_seconds: int, scope_mode: str) -> None:
        captured.append((lag_seconds, scope_mode))

    monkeypatch.setattr(scope, "query_dicts", fake_query_dicts)
    monkeypatch.setattr(scope, "record_investment_membership_scope_stale", fake_record)

    await scope.record_stale_investment_membership_scope(
        cast(BaseMetricsSink, object()), org_id="org-1"
    )

    assert captured == [(42, "unscoped_fallback")]


@pytest.mark.asyncio
async def test_record_stale_investment_membership_scope_skips_fresh_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[int, str]] = []

    async def fake_query_dicts(
        _sink: BaseMetricsSink, _query: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return [{"scope_mode": "scoped", "lag_seconds": 0}]

    def fake_record(*, lag_seconds: int, scope_mode: str) -> None:
        captured.append((lag_seconds, scope_mode))

    monkeypatch.setattr(scope, "query_dicts", fake_query_dicts)
    monkeypatch.setattr(scope, "record_investment_membership_scope_stale", fake_record)

    await scope.record_stale_investment_membership_scope(
        cast(BaseMetricsSink, object()), org_id="org-1"
    )

    assert captured == []
