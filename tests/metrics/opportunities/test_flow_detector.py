"""Unit tests for FlowOpportunityDetector (CHAOS-2218, Phase 1).

Uses a fake ClickHouse client that returns synthetic list[dict] rows.
All tests are pure unit tests — no live ClickHouse, no @pytest.mark.clickhouse.

Test coverage:
- Each of the 7 rule functions: fires above threshold, skips below, score monotonicity
- Deterministic opportunity_id for the same (kind, entity_id)
- Org-scoping (org_id injected in params)
- asyncio.gather parallelism (both queries called)
- Total failure → [] (no exception raised to caller)
- Rule exception isolation (bad row doesn't blank result)
- Limit capping
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.metrics.opportunities.flow_detector import (
    _CHANGE_FAILURE_THRESHOLD,
    _CYCLE_TIME_THRESHOLD_HOURS,
    _HIGH_CHURN_THRESHOLD,
    _LOW_THROUGHPUT_THRESHOLD,
    _MIN_DATA_DAYS,
    _REVIEW_LATENCY_THRESHOLD_HOURS,
    _REWORK_RATIO_THRESHOLD,
    _WIP_CONGESTION_THRESHOLD,
    FlowOpportunityDetector,
    _rule_high_change_failure,
    _rule_high_churn,
    _rule_high_review_latency,
    _rule_high_rework,
    _rule_high_wip,
    _rule_low_throughput,
    _rule_slow_cycle_time,
)
from dev_health_ops.metrics.opportunities.models import (
    FlowScopeInput,
    ImproveOpportunityKind,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REPO_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
TEAM_ID = "team-alpha"
ORG_ID = "org-test"


def _repo_row(**kwargs: Any) -> dict[str, Any]:
    """Build a synthetic repo_metrics_daily aggregate row above all thresholds."""
    defaults: dict[str, Any] = {
        "entity_id": REPO_ID,
        "data_days": _MIN_DATA_DAYS + 1,
        "pr_first_review_p50_hours": _REVIEW_LATENCY_THRESHOLD_HOURS * 2,
        "pr_rework_ratio": _REWORK_RATIO_THRESHOLD * 2,
        "rework_churn_ratio_30d": _HIGH_CHURN_THRESHOLD * 2,
        "change_failure_rate": _CHANGE_FAILURE_THRESHOLD * 2,
        "total_loc_touched": 10000,
        "window_days": 30,
    }
    defaults.update(kwargs)
    return defaults


def _team_row(**kwargs: Any) -> dict[str, Any]:
    """Build a synthetic work_item_metrics_daily aggregate row above all thresholds."""
    defaults: dict[str, Any] = {
        "entity_id": TEAM_ID,
        "data_days": _MIN_DATA_DAYS + 1,
        "cycle_time_p50_hours": _CYCLE_TIME_THRESHOLD_HOURS * 2,
        "wip_congestion_ratio": _WIP_CONGESTION_THRESHOLD * 2,
        "items_completed": _LOW_THROUGHPUT_THRESHOLD / 2,  # below threshold
        "defect_intro_rate": 0.05,
        "window_days": 30,
    }
    defaults.update(kwargs)
    return defaults


# ---------------------------------------------------------------------------
# Rule unit tests
# ---------------------------------------------------------------------------


class TestRuleHighReviewLatency:
    def test_fires_above_threshold(self) -> None:
        row = _repo_row(pr_first_review_p50_hours=_REVIEW_LATENCY_THRESHOLD_HOURS * 2)
        opp = _rule_high_review_latency(row)
        assert opp is not None
        assert opp.kind is ImproveOpportunityKind.HIGH_REVIEW_LATENCY

    def test_skips_at_threshold(self) -> None:
        row = _repo_row(pr_first_review_p50_hours=_REVIEW_LATENCY_THRESHOLD_HOURS)
        assert _rule_high_review_latency(row) is None

    def test_skips_below_threshold(self) -> None:
        row = _repo_row(pr_first_review_p50_hours=1.0)
        assert _rule_high_review_latency(row) is None

    def test_skips_none_value(self) -> None:
        row = _repo_row(pr_first_review_p50_hours=None)
        assert _rule_high_review_latency(row) is None

    def test_score_monotone_increasing(self) -> None:
        values = [
            _REVIEW_LATENCY_THRESHOLD_HOURS * m for m in (1.1, 1.5, 2.0, 3.0, 5.0)
        ]
        scores = [
            _rule_high_review_latency(_repo_row(pr_first_review_p50_hours=v)).score  # type: ignore[union-attr]
            for v in values
        ]
        assert scores == sorted(scores)

    def test_evidence_refs_format(self) -> None:
        opp = _rule_high_review_latency(
            _repo_row(pr_first_review_p50_hours=_REVIEW_LATENCY_THRESHOLD_HOURS * 2)
        )
        assert opp is not None
        assert opp.evidence_refs[0].startswith(
            "repo_metrics_daily:pr_first_review_p50_hours:"
        )


class TestRuleHighRework:
    def test_fires_above_threshold(self) -> None:
        row = _repo_row(pr_rework_ratio=_REWORK_RATIO_THRESHOLD * 2)
        opp = _rule_high_rework(row)
        assert opp is not None
        assert opp.kind is ImproveOpportunityKind.HIGH_REWORK

    def test_skips_at_threshold(self) -> None:
        assert (
            _rule_high_rework(_repo_row(pr_rework_ratio=_REWORK_RATIO_THRESHOLD))
            is None
        )

    def test_skips_below_threshold(self) -> None:
        assert _rule_high_rework(_repo_row(pr_rework_ratio=0.0)) is None

    def test_score_monotone_increasing(self) -> None:
        values = [_REWORK_RATIO_THRESHOLD * m for m in (1.1, 1.5, 2.0, 3.0)]
        scores = [
            _rule_high_rework(_repo_row(pr_rework_ratio=v)).score  # type: ignore[union-attr]
            for v in values
        ]
        assert scores == sorted(scores)


class TestRuleHighChurn:
    def test_fires_above_threshold(self) -> None:
        opp = _rule_high_churn(
            _repo_row(rework_churn_ratio_30d=_HIGH_CHURN_THRESHOLD * 2)
        )
        assert opp is not None
        assert opp.kind is ImproveOpportunityKind.HIGH_CHURN

    def test_skips_at_threshold(self) -> None:
        assert (
            _rule_high_churn(_repo_row(rework_churn_ratio_30d=_HIGH_CHURN_THRESHOLD))
            is None
        )

    def test_skips_below_threshold(self) -> None:
        assert _rule_high_churn(_repo_row(rework_churn_ratio_30d=0.0)) is None

    def test_score_monotone_increasing(self) -> None:
        values = [_HIGH_CHURN_THRESHOLD * m for m in (1.1, 1.5, 2.0, 3.0)]
        scores = [
            _rule_high_churn(_repo_row(rework_churn_ratio_30d=v)).score  # type: ignore[union-attr]
            for v in values
        ]
        assert scores == sorted(scores)


class TestRuleHighChangeFailure:
    def test_fires_above_threshold(self) -> None:
        opp = _rule_high_change_failure(
            _repo_row(change_failure_rate=_CHANGE_FAILURE_THRESHOLD * 2)
        )
        assert opp is not None
        assert opp.kind is ImproveOpportunityKind.HIGH_CHANGE_FAILURE

    def test_skips_at_threshold(self) -> None:
        assert (
            _rule_high_change_failure(
                _repo_row(change_failure_rate=_CHANGE_FAILURE_THRESHOLD)
            )
            is None
        )

    def test_skips_below_threshold(self) -> None:
        assert _rule_high_change_failure(_repo_row(change_failure_rate=0.0)) is None

    def test_score_monotone_increasing(self) -> None:
        values = [_CHANGE_FAILURE_THRESHOLD * m for m in (1.1, 1.5, 2.0, 3.0)]
        scores = [
            _rule_high_change_failure(_repo_row(change_failure_rate=v)).score  # type: ignore[union-attr]
            for v in values
        ]
        assert scores == sorted(scores)


class TestRuleSlowCycleTime:
    def test_fires_above_threshold(self) -> None:
        opp = _rule_slow_cycle_time(
            _team_row(cycle_time_p50_hours=_CYCLE_TIME_THRESHOLD_HOURS * 2)
        )
        assert opp is not None
        assert opp.kind is ImproveOpportunityKind.SLOW_CYCLE_TIME

    def test_skips_at_threshold(self) -> None:
        assert (
            _rule_slow_cycle_time(
                _team_row(cycle_time_p50_hours=_CYCLE_TIME_THRESHOLD_HOURS)
            )
            is None
        )

    def test_skips_below_threshold(self) -> None:
        assert _rule_slow_cycle_time(_team_row(cycle_time_p50_hours=10.0)) is None

    def test_skips_none(self) -> None:
        assert _rule_slow_cycle_time(_team_row(cycle_time_p50_hours=None)) is None

    def test_score_monotone_increasing(self) -> None:
        values = [_CYCLE_TIME_THRESHOLD_HOURS * m for m in (1.1, 1.5, 2.0, 3.0)]
        scores = [
            _rule_slow_cycle_time(_team_row(cycle_time_p50_hours=v)).score  # type: ignore[union-attr]
            for v in values
        ]
        assert scores == sorted(scores)


class TestRuleHighWip:
    def test_fires_above_threshold(self) -> None:
        opp = _rule_high_wip(
            _team_row(wip_congestion_ratio=_WIP_CONGESTION_THRESHOLD * 2)
        )
        assert opp is not None
        assert opp.kind is ImproveOpportunityKind.HIGH_WIP

    def test_skips_at_threshold(self) -> None:
        assert (
            _rule_high_wip(_team_row(wip_congestion_ratio=_WIP_CONGESTION_THRESHOLD))
            is None
        )

    def test_skips_below_threshold(self) -> None:
        assert _rule_high_wip(_team_row(wip_congestion_ratio=0.0)) is None

    def test_score_monotone_increasing(self) -> None:
        values = [_WIP_CONGESTION_THRESHOLD * m for m in (1.1, 1.5, 2.0, 3.0)]
        scores = [
            _rule_high_wip(_team_row(wip_congestion_ratio=v)).score  # type: ignore[union-attr]
            for v in values
        ]
        assert scores == sorted(scores)


class TestRuleLowThroughput:
    def test_fires_below_threshold(self) -> None:
        opp = _rule_low_throughput(
            _team_row(items_completed=_LOW_THROUGHPUT_THRESHOLD / 2)
        )
        assert opp is not None
        assert opp.kind is ImproveOpportunityKind.LOW_THROUGHPUT

    def test_skips_at_threshold(self) -> None:
        assert (
            _rule_low_throughput(_team_row(items_completed=_LOW_THROUGHPUT_THRESHOLD))
            is None
        )

    def test_skips_above_threshold(self) -> None:
        assert (
            _rule_low_throughput(
                _team_row(items_completed=_LOW_THROUGHPUT_THRESHOLD * 2)
            )
            is None
        )

    def test_skips_none(self) -> None:
        assert _rule_low_throughput(_team_row(items_completed=None)) is None

    def test_score_present(self) -> None:
        opp = _rule_low_throughput(_team_row(items_completed=0.0))
        assert opp is not None
        assert 0.0 <= opp.score <= 1.0


# ---------------------------------------------------------------------------
# Deterministic ID tests
# ---------------------------------------------------------------------------


class TestDeterministicIds:
    def test_same_inputs_produce_same_id(self) -> None:
        row = _repo_row(
            entity_id=REPO_ID,
            pr_first_review_p50_hours=_REVIEW_LATENCY_THRESHOLD_HOURS * 2,
        )
        opp1 = _rule_high_review_latency(row)
        opp2 = _rule_high_review_latency(row)
        assert opp1 is not None and opp2 is not None
        assert opp1.opportunity_id == opp2.opportunity_id

    def test_different_entities_produce_different_ids(self) -> None:
        row1 = _repo_row(
            entity_id="repo-1",
            pr_first_review_p50_hours=_REVIEW_LATENCY_THRESHOLD_HOURS * 2,
        )
        row2 = _repo_row(
            entity_id="repo-2",
            pr_first_review_p50_hours=_REVIEW_LATENCY_THRESHOLD_HOURS * 2,
        )
        opp1 = _rule_high_review_latency(row1)
        opp2 = _rule_high_review_latency(row2)
        assert opp1 is not None and opp2 is not None
        assert opp1.opportunity_id != opp2.opportunity_id


# ---------------------------------------------------------------------------
# Detector integration tests (with fake query_dicts)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_detector_returns_opportunities_for_valid_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detector surfaces findings when rows are above thresholds."""
    repo_rows = [_repo_row()]
    team_rows = [_team_row()]

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[dict]:
        if "repo_metrics_daily" in query:
            return repo_rows
        return team_rows

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await FlowOpportunityDetector(MagicMock()).detect(ORG_ID, limit=20)
    assert len(result) > 0
    kinds = {o.kind for o in result}
    # Repo rules should fire (review latency, rework, churn, change failure)
    assert ImproveOpportunityKind.HIGH_REVIEW_LATENCY in kinds
    assert ImproveOpportunityKind.HIGH_REWORK in kinds
    # Team rules should fire (slow cycle, wip, low throughput)
    assert ImproveOpportunityKind.SLOW_CYCLE_TIME in kinds
    assert ImproveOpportunityKind.LOW_THROUGHPUT in kinds


@pytest.mark.asyncio
async def test_detector_empty_rows_returns_empty_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query_dicts(_client: Any, _query: str, _params: Any) -> list[dict]:
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await FlowOpportunityDetector(MagicMock()).detect(ORG_ID)
    assert result == []


@pytest.mark.asyncio
async def test_detector_respects_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Detector never returns more than limit items."""
    # Generate many repo rows all above every threshold
    many_rows = [_repo_row(entity_id=f"repo-{i}") for i in range(50)]

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[dict]:
        if "repo_metrics_daily" in query:
            return many_rows
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await FlowOpportunityDetector(MagicMock()).detect(ORG_ID, limit=5)
    assert len(result) == 5


@pytest.mark.asyncio
async def test_detector_injects_org_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """org_id is injected into the params dict for both queries."""
    captured_params: list[dict] = []

    async def fake_query_dicts(_client: Any, _query: str, params: Any) -> list[dict]:
        captured_params.append(dict(params))
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    await FlowOpportunityDetector(MagicMock()).detect("my-org-id")

    assert len(captured_params) == 2, "Expected exactly two queries (repo + team)"
    for params in captured_params:
        assert params.get("org_id") == "my-org-id"


@pytest.mark.asyncio
async def test_detector_both_queries_called(monkeypatch: pytest.MonkeyPatch) -> None:
    """asyncio.gather causes both repo and team queries to be called."""
    queries_seen: list[str] = []

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[dict]:
        queries_seen.append(query)
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    await FlowOpportunityDetector(MagicMock()).detect(ORG_ID)

    assert any("repo_metrics_daily" in q for q in queries_seen)
    assert any("work_item_metrics_daily" in q for q in queries_seen)
    assert len(queries_seen) == 2


@pytest.mark.asyncio
async def test_detector_total_failure_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If both queries raise, detect() returns [] rather than propagating."""

    async def fake_query_dicts(_client: Any, _query: str, _params: Any) -> list[dict]:
        raise RuntimeError("ClickHouse is unavailable")

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await FlowOpportunityDetector(MagicMock()).detect(ORG_ID)
    assert result == []


@pytest.mark.asyncio
async def test_detector_bad_row_skipped_others_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row that causes a rule exception doesn't blank the whole result."""
    bad_row = {"entity_id": "bad-repo", "pr_first_review_p50_hours": object()}
    good_row = _repo_row(entity_id="good-repo")

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[dict]:
        if "repo_metrics_daily" in query:
            return [bad_row, good_row]
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await FlowOpportunityDetector(MagicMock()).detect(ORG_ID)
    entity_ids = {o.entity_id for o in result}
    assert "good-repo" in entity_ids


@pytest.mark.asyncio
async def test_detector_sorted_by_score_descending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Results are sorted highest-score-first."""
    # Create rows that produce different scores
    rows = [
        _repo_row(
            entity_id=f"repo-{i}",
            pr_first_review_p50_hours=_REVIEW_LATENCY_THRESHOLD_HOURS * (i + 2),
        )
        for i in range(5)
    ]

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[dict]:
        if "repo_metrics_daily" in query:
            return rows
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await FlowOpportunityDetector(MagicMock()).detect(ORG_ID, limit=20)
    review_latency_opps = [
        o for o in result if o.kind is ImproveOpportunityKind.HIGH_REVIEW_LATENCY
    ]
    scores = [o.score for o in review_latency_opps]
    assert scores == sorted(scores, reverse=True)


@pytest.mark.asyncio
async def test_detector_scope_filters_repo(monkeypatch: pytest.MonkeyPatch) -> None:
    """repo_id scope is passed as a query param."""
    import uuid as _uuid

    captured: list[dict] = []

    async def fake_query_dicts(_client: Any, _query: str, params: Any) -> list[dict]:
        captured.append(dict(params))
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    repo_id = _uuid.UUID(REPO_ID)
    scope = FlowScopeInput(repo_id=repo_id)
    await FlowOpportunityDetector(MagicMock()).detect(ORG_ID, scope=scope)

    repo_params = [p for p in captured if "repo_id" in p]
    assert repo_params, "repo_id should be in the repo query params"
    assert repo_params[0]["repo_id"] == str(repo_id)


@pytest.mark.asyncio
async def test_detector_scope_filters_team(monkeypatch: pytest.MonkeyPatch) -> None:
    """team_id scope is passed as a query param."""
    captured: list[dict] = []

    async def fake_query_dicts(_client: Any, _query: str, params: Any) -> list[dict]:
        captured.append(dict(params))
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    scope = FlowScopeInput(team_id=TEAM_ID)
    await FlowOpportunityDetector(MagicMock()).detect(ORG_ID, scope=scope)

    team_params = [p for p in captured if "team_id" in p]
    assert team_params, "team_id should be in the team query params"
    assert team_params[0]["team_id"] == TEAM_ID


@pytest.mark.asyncio
async def test_detector_parallelism(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both queries are launched concurrently via asyncio.gather."""
    started: list[str] = []

    async def fake_query_dicts(_client: Any, query: str, _params: Any) -> list[dict]:
        if "repo_metrics_daily" in query:
            started.append("repo")
        elif "work_item_metrics_daily" in query:
            started.append("team")
        await asyncio.sleep(0)  # yield so both can start
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    await FlowOpportunityDetector(MagicMock()).detect(ORG_ID)
    assert "repo" in started
    assert "team" in started
