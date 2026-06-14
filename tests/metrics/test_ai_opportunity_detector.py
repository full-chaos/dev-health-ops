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

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

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
    assert (
        "58% test gap rate" in by_kind[AIOpportunityKind.UNCOVERED_TEST_AREA].rationale
    )
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

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await AIOpportunityDetector(MagicMock()).detect(
        "org-a", AIScopeInput(repo_id=REPO_ID), limit=1
    )

    assert len(result) == 1
    assert calls[0]["repo_id"] == REPO_ID
    assert calls[0]["org_id"] == "org-a"


@pytest.mark.asyncio
async def test_detector_emits_repetitive_change(monkeypatch: pytest.MonkeyPatch):
    async def fake_query_dicts(_client, query, _params):
        # Route by query shape: only the repetitive-change query selects a
        # title_prefix column (CHAOS-2189 added more git_pull_requests rules).
        if "title_prefix" not in query:
            return []
        return [
            {
                "repo_id": REPO_ID,
                "team_id": "",
                "prs_total": 6,
                "title_prefix": "bump deps",
                "pr_refs": [
                    f"git_pull_requests:{REPO_ID}:{number}" for number in range(1, 7)
                ],
            }
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await AIOpportunityDetector(MagicMock()).detect("org-a", limit=10)

    assert len(result) == 1
    assert result[0].kind is AIOpportunityKind.REPETITIVE_CHANGE
    assert result[0].evidence_refs[0].startswith("git_pull_requests:")
    assert result[0].work_graph_drilldowns[0].root_type == "pr"
    assert result[0].work_graph_drilldowns[0].root_id == f"{REPO_ID}#1"


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

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await AIOpportunityDetector(MagicMock()).detect("org-a", limit=500)

    assert len(result) == 100


@pytest.mark.asyncio
async def test_detector_empty_data_returns_empty_list(monkeypatch: pytest.MonkeyPatch):
    async def fake_query_dicts(_client, _query, _params):
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await AIOpportunityDetector(MagicMock()).detect("org-a", limit=10)

    assert result == []


@pytest.mark.asyncio
async def test_detector_emits_test_generation_from_human_gap(
    monkeypatch: pytest.MonkeyPatch,
):
    """TEST_GENERATION (CHAOS-2189) gates on the human bucket, not AI volume."""

    async def fake_query_dicts(_client, query, _params):
        if "ai_impact_metrics_daily" not in query:
            return []
        return [
            {
                "repo_id": REPO_ID,
                "team_id": "team-a",
                "attribution_bucket": "human",
                "prs_total": 20,
                "reviews_per_pr": 2.0,
                "cycle_time_avg_hours": 20.0,
                "rework_prs": 2,
                "test_gap_prs": 12,
            },
            {
                # AI bucket below the 10-PR minimum: AI rules must stay silent.
                "repo_id": REPO_ID,
                "team_id": "team-a",
                "attribution_bucket": "ai_assisted",
                "prs_total": 2,
                "reviews_per_pr": 9.0,
                "cycle_time_avg_hours": 90.0,
                "rework_prs": 2,
                "test_gap_prs": 2,
            },
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await AIOpportunityDetector(MagicMock()).detect(
        "org-a", AIScopeInput(team_id="team-a"), limit=10
    )

    assert [item.kind for item in result] == [AIOpportunityKind.TEST_GENERATION]
    assert "60% test gap rate" in result[0].rationale
    assert result[0].evidence_refs == [
        f"ai_impact_metrics_daily:test_gap_rate:{REPO_ID}"
    ]


@pytest.mark.asyncio
async def test_detector_emits_title_pattern_toil_kinds(
    monkeypatch: pytest.MonkeyPatch,
):
    """DEPENDENCY_UPDATES + MECHANICAL_MIGRATIONS fire from title clusters."""

    async def fake_query_dicts(_client, query, _params):
        if "LEFT ANTI JOIN" not in query:
            return []
        if "depend" in query:
            prs_total = 6
        elif "migrat" in query:
            prs_total = 8
        else:
            return []
        return [
            {
                "repo_id": REPO_ID,
                "prs_total": prs_total,
                "pr_refs": [
                    f"git_pull_requests:{REPO_ID}:{number}"
                    for number in range(1, prs_total + 1)
                ],
            }
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await AIOpportunityDetector(MagicMock()).detect("org-a", limit=10)

    by_kind = {item.kind: item for item in result}
    assert AIOpportunityKind.DEPENDENCY_UPDATES in by_kind
    assert AIOpportunityKind.MECHANICAL_MIGRATIONS in by_kind
    assert (
        "6 dependency-update PRs"
        in by_kind[AIOpportunityKind.DEPENDENCY_UPDATES].rationale
    )
    assert (
        "8 migration-style PRs"
        in by_kind[AIOpportunityKind.MECHANICAL_MIGRATIONS].rationale
    )
    deps = by_kind[AIOpportunityKind.DEPENDENCY_UPDATES]
    assert deps.evidence_refs[0].startswith("git_pull_requests:")
    assert deps.evidence_refs[-1] == (
        f"git_pull_requests:dependency_update_prs:{REPO_ID}"
    )
    assert deps.work_graph_drilldowns[0].root_type == "pr"


@pytest.mark.asyncio
async def test_detector_emits_doc_drift(monkeypatch: pytest.MonkeyPatch):
    async def fake_query_dicts(_client, query, _params):
        if "doc_changes" not in query:
            return []
        return [{"repo_id": REPO_ID, "code_commits": 42, "doc_changes": 0}]

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await AIOpportunityDetector(MagicMock()).detect("org-a", limit=10)

    assert [item.kind for item in result] == [AIOpportunityKind.DOCUMENTATION_DRIFT]
    assert "42 code commits" in result[0].rationale
    assert result[0].evidence_refs == [f"git_commit_stats:doc_changes:{REPO_ID}"]


@pytest.mark.asyncio
async def test_detector_emits_flaky_test_triage(monkeypatch: pytest.MonkeyPatch):
    async def fake_query_dicts(_client, query, _params):
        if "testops_test_metrics_daily" not in query:
            return []
        return [
            {
                "repo_id": REPO_ID,
                "cases_total": 400,
                "weighted_flake_rate": 0.12,
            }
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await AIOpportunityDetector(MagicMock()).detect("org-a", limit=10)

    assert [item.kind for item in result] == [AIOpportunityKind.FLAKY_TEST_TRIAGE]
    assert "12.0%" in result[0].rationale
    assert "400 executions" in result[0].rationale
    assert result[0].evidence_refs == [
        f"testops_test_metrics_daily:flake_rate:{REPO_ID}"
    ]


@pytest.mark.asyncio
async def test_sql_backed_kinds_skip_team_scope(monkeypatch: pytest.MonkeyPatch):
    """Team-scoped requests must not run the unscoped raw-table rules."""
    seen_queries: list[str] = []

    async def fake_query_dicts(_client, query, _params):
        seen_queries.append(query)
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    result = await AIOpportunityDetector(MagicMock()).detect(
        "org-a", AIScopeInput(team_id="team-a"), limit=10
    )

    assert result == []
    # Only the ai_impact_metrics_daily load runs under a team scope.
    assert len(seen_queries) == 1
    assert "ai_impact_metrics_daily" in seen_queries[0]


@pytest.mark.asyncio
async def test_repetitive_changes_query_scopes_pull_requests_by_org(
    monkeypatch: pytest.MonkeyPatch,
):
    """CHAOS-2396: the repetitive-changes detector joins ai_attribution to
    git_pull_requests by (repo_id, number/work_item_id). Filtering only
    attr.org_id let a same-(repo_id, number) PR from another tenant join and
    leak its title/author. The driving git_pull_requests side must be org-scoped
    too (made reachable now that GitHub writes bare PR-number subject_ids)."""
    captured: list[str] = []

    async def fake_query_dicts(_client, query, _params):
        captured.append(query)
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )
    await AIOpportunityDetector(MagicMock()).detect("org-a", limit=10)

    repetitive = [
        q for q in captured if "git_pull_requests AS pr" in q and "prs_total" in q
    ]
    assert repetitive, "repetitive-changes query was not issued"
    assert "pr.org_id = {org_id:String}" in repetitive[0]
    assert "attr.org_id = {org_id:String}" in repetitive[0]
