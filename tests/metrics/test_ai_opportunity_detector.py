from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.graphql.models.ai import AIOpportunityKind, AIScopeInput
from dev_health_ops.metrics.opportunities.ai_detector import AIOpportunityDetector

REPO_ID = "11111111-1111-1111-1111-111111111111"


@pytest.mark.asyncio
async def test_detector_emits_metric_opportunities(monkeypatch: pytest.MonkeyPatch):
    async def fake_query_dicts(_client, query, _params):
        if "ai_impact_metrics_daily" not in query:
            return []
        return [
            {
                "repo_id": REPO_ID,
                "team_id": "team-a",
                "attribution_bucket": "ai_assisted",
                "prs_total": 12,
                "reviews_per_pr": 4.2,
                "cycle_time_avg_hours": 30.0,
                "rework_prs": 4,
                "test_gap_prs": 7,
            },
            {
                "repo_id": REPO_ID,
                "team_id": "team-a",
                "attribution_bucket": "human",
                "prs_total": 20,
                "reviews_per_pr": 2.0,
                "cycle_time_avg_hours": 20.0,
                "rework_prs": 2,
                "test_gap_prs": 2,
            },
        ]

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", fake_query_dicts)

    result = await AIOpportunityDetector(MagicMock()).detect(
        "org-a", AIScopeInput(team_id="team-a"), limit=10
    )

    kinds = {item.kind for item in result}
    assert AIOpportunityKind.HIGH_REVIEW_LOAD in kinds
    assert AIOpportunityKind.HIGH_REWORK in kinds
    assert AIOpportunityKind.SLOW_CYCLE in kinds
    assert AIOpportunityKind.UNCOVERED_TEST_AREA in kinds
    by_kind = {item.kind: item for item in result}
    assert "4.2 reviews vs 2.0" in by_kind[AIOpportunityKind.HIGH_REVIEW_LOAD].rationale
    assert "33% rework rate vs 10%" in by_kind[AIOpportunityKind.HIGH_REWORK].rationale
    assert "30.0 cycle hours vs 20.0" in by_kind[AIOpportunityKind.SLOW_CYCLE].rationale
    assert "58% test gap rate" in by_kind[AIOpportunityKind.UNCOVERED_TEST_AREA].rationale
    assert all(item.repo_id == REPO_ID for item in result)
    assert all(item.team_id == "team-a" for item in result)
    assert all(item.evidence_refs for item in result)


@pytest.mark.asyncio
async def test_detector_honors_scope_and_limit(monkeypatch: pytest.MonkeyPatch):
    calls: list[dict] = []

    async def fake_query_dicts(_client, query, params):
        calls.append(params)
        if "ai_impact_metrics_daily" not in query:
            return []
        return [
            {
                "repo_id": REPO_ID,
                "team_id": None,
                "attribution_bucket": "ai_assisted",
                "prs_total": 12,
                "reviews_per_pr": 4.0,
                "cycle_time_avg_hours": 10.0,
                "rework_prs": 0,
                "test_gap_prs": 7,
            },
            {
                "repo_id": REPO_ID,
                "team_id": None,
                "attribution_bucket": "human",
                "prs_total": 20,
                "reviews_per_pr": 2.0,
                "cycle_time_avg_hours": 9.0,
                "rework_prs": 0,
                "test_gap_prs": 1,
            },
        ]

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", fake_query_dicts)

    result = await AIOpportunityDetector(MagicMock()).detect(
        "org-a", AIScopeInput(repo_id=REPO_ID), limit=1
    )

    assert len(result) == 1
    assert calls[0]["repo_id"] == REPO_ID
    assert calls[0]["org_id"] == "org-a"


@pytest.mark.asyncio
async def test_detector_emits_repetitive_change(monkeypatch: pytest.MonkeyPatch):
    async def fake_query_dicts(_client, query, _params):
        if "ai_impact_metrics_daily" in query:
            return []
        return [
            {
                "repo_id": REPO_ID,
                "team_id": "",
                "prs_total": 6,
                "title_prefix": "bump deps",
                "pr_refs": [f"git_pull_requests:{REPO_ID}:{number}" for number in range(1, 7)],
            }
        ]

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", fake_query_dicts)

    result = await AIOpportunityDetector(MagicMock()).detect("org-a", limit=10)

    assert len(result) == 1
    assert result[0].kind is AIOpportunityKind.REPETITIVE_CHANGE
    assert result[0].evidence_refs[0].startswith("git_pull_requests:")


@pytest.mark.asyncio
async def test_detector_clamps_limit_to_one_hundred(monkeypatch: pytest.MonkeyPatch):
    async def fake_query_dicts(_client, query, _params):
        if "ai_impact_metrics_daily" not in query:
            return []
        rows = []
        for index in range(101):
            repo_id = f"00000000-0000-0000-0000-{index + 1:012d}"
            rows.extend(
                [
                    {
                        "repo_id": repo_id,
                        "team_id": None,
                        "attribution_bucket": "ai_assisted",
                        "prs_total": 12,
                        "reviews_per_pr": 6.0,
                        "cycle_time_avg_hours": 10.0,
                        "rework_prs": 0,
                        "test_gap_prs": 0,
                    },
                    {
                        "repo_id": repo_id,
                        "team_id": None,
                        "attribution_bucket": "human",
                        "prs_total": 12,
                        "reviews_per_pr": 1.0,
                        "cycle_time_avg_hours": 10.0,
                        "rework_prs": 0,
                        "test_gap_prs": 0,
                    },
                ]
            )
        return rows

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", fake_query_dicts)

    result = await AIOpportunityDetector(MagicMock()).detect("org-a", limit=500)

    assert len(result) == 100


@pytest.mark.asyncio
async def test_detector_empty_data_returns_empty_list(monkeypatch: pytest.MonkeyPatch):
    async def fake_query_dicts(_client, _query, _params):
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", fake_query_dicts)

    result = await AIOpportunityDetector(MagicMock()).detect("org-a", limit=10)

    assert result == []
