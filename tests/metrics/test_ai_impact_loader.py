"""Unit tests for AIImpactClickHouseLoader defect fixes.

Covers:
* CHAOS-2184 (AI-19) — team_id must be derived from the teams table join, not
  hardcoded as an empty string.
* CHAOS-2190 (AI-25) — reviewer_concentration Gini must be scoped to repos
  that have AI-attributed PRs, not to the full org.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest

from dev_health_ops.metrics.loaders.ai_impact import AIImpactClickHouseLoader

ORG_ID = "org-test"
REPO_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
REPO_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
TEAM_A = "team-alpha"
TEAM_B = "team-beta"

START = datetime(2026, 5, 1, tzinfo=timezone.utc)
END = datetime(2026, 5, 8, tzinfo=timezone.utc)
START_DAY = date(2026, 5, 1)
END_DAY = date(2026, 5, 7)


# ---------------------------------------------------------------------------
# CHAOS-2184 (AI-19) — team_id SQL join
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_ai_pr_attributions_always_uses_empty_team_id_cast():
    """SQL must always use CAST('', 'String') AS team_id in both UNION branches.

    Team resolution happens in application code via RepoPatternTeamResolver
    (see resolve_ai_attributed_prs in resolvers/ai.py).  teams.repo_patterns is
    Array(String) of fnmatch glob patterns over repo full-names, so a SQL JOIN
    on repo UUID would never match and is therefore omitted entirely.
    """
    captured_queries: list[str] = []

    async def fake_qd(_client: Any, query: str, _params: Any) -> list[dict]:
        captured_queries.append(query)
        return []

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await loader.load_ai_pr_attributions(start=START, end=END)

    assert len(captured_queries) == 1
    sql = captured_queries[0]
    # Both UNION branches must use the empty-string cast (never a teams JOIN).
    count = sql.count("CAST('', 'String') AS team_id")
    assert count == 2, (
        f"Expected CAST('', 'String') AS team_id in both UNION branches, found {count}"
    )
    # The teams table must NOT appear in the loader SQL at all.
    assert "teams" not in sql, (
        "Loader SQL must not reference the teams table; team resolution is in app code"
    )


@pytest.mark.asyncio
async def test_load_ai_pr_attributions_empty_org_id_uses_empty_team_id_cast():
    """Even without org_id the SQL uses CAST('', 'String') AS team_id (no teams join)."""
    captured_queries: list[str] = []

    async def fake_qd(_client: Any, query: str, _params: Any) -> list[dict]:
        captured_queries.append(query)
        return []

    loader = AIImpactClickHouseLoader(object(), org_id="")
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await loader.load_ai_pr_attributions(start=START, end=END)

    sql = captured_queries[0]
    assert "CAST('', 'String') AS team_id" in sql
    assert "teams" not in sql


@pytest.mark.asyncio
async def test_load_ai_pr_attributions_returns_team_id_from_row():
    """Row-level team_id returned by the DB is surfaced in the output."""

    async def fake_qd(_client: Any, _query: str, _params: Any) -> list[dict]:
        return [
            {
                "repo_id": REPO_A,
                "number": 42,
                "kind": "ai_assisted",
                "work_type": "pull_request",
                "team_id": TEAM_A,
                "title": "AI PR",
                "merged_at": None,
            }
        ]

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        rows = await loader.load_ai_pr_attributions(start=START, end=END)

    assert len(rows) == 1
    assert rows[0]["team_id"] == TEAM_A


# ---------------------------------------------------------------------------
# CHAOS-2190 (AI-25) — reviewer_concentration scoped to AI repos
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reviewer_concentration_query_scoped_to_ai_repos():
    """The Gini query must INNER JOIN through ai_attribution_resolved, not scan
    all of user_metrics_daily."""
    captured_queries: list[str] = []

    async def fake_qd(_client: Any, query: str, _params: Any) -> list[dict]:
        captured_queries.append(query)
        return []

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await loader.load_reviewer_concentration(start_day=START_DAY, end_day=END_DAY)

    assert len(captured_queries) == 1
    sql = captured_queries[0]
    assert "ai_attribution_resolved" in sql, (
        "Gini query must scope reviewers to AI-attributed repos"
    )
    assert "INNER JOIN" in sql, "INNER JOIN required to exclude non-AI repos"


@pytest.mark.asyncio
async def test_reviewer_concentration_returns_none_when_no_ai_repos():
    """If no AI-attributed repos match, result is (None, 0)."""

    async def fake_qd(_client: Any, _query: str, _params: Any) -> list[dict]:
        return []  # no rows → no AI repos in scope

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        gini, count = await loader.load_reviewer_concentration(
            start_day=START_DAY, end_day=END_DAY
        )

    assert gini is None
    assert count == 0


@pytest.mark.asyncio
async def test_reviewer_concentration_repo_id_filter_uses_qualified_column():
    """With repo_id scope the filter must be qualified with the `umd.` table alias.

    The query has an INNER JOIN subquery (`ai_repos`) that also exposes a
    `repo_id` column.  Without the alias qualification ClickHouse raises
    "Ambiguous column name" at runtime.
    """
    captured_queries: list[str] = []

    async def fake_qd(_client: Any, query: str, _params: Any) -> list[dict]:
        captured_queries.append(query)
        return []

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await loader.load_reviewer_concentration(
            start_day=START_DAY, end_day=END_DAY, repo_id=REPO_A
        )

    sql = captured_queries[0]
    # Filter must be qualified — no bare `repo_id = ...` in the WHERE clause.
    assert "umd.repo_id = {repo_id:UUID}" in sql, (
        "repo_id filter must be qualified with umd. alias to avoid ambiguity"
    )
    assert "AND repo_id = {repo_id:UUID}" not in sql, (
        "Unqualified repo_id filter causes ClickHouse 'Ambiguous column' error"
    )


@pytest.mark.asyncio
async def test_reviewer_concentration_gini_computed_from_ai_scoped_rows():
    """Gini is computed over the reviews_given values returned (AI-scoped)."""
    # Simulate two reviewers: one heavy (10 reviews), one light (0 reviews)
    ai_rows = [
        {"reviews_given": 10},
        {"reviews_given": 0},
    ]

    async def fake_qd(_client: Any, _query: str, _params: Any) -> list[dict]:
        return ai_rows

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        gini, count = await loader.load_reviewer_concentration(
            start_day=START_DAY, end_day=END_DAY
        )

    assert count == 2
    assert gini is not None
    # A reviewer distribution of [0, 10] is maximally unequal → Gini close to 1.
    # _gini([0, 10]): sorted=[0,10], total=10, weighted_sum=0*1+10*2=20
    # gini = 2*20/(2*10) - 3/2 = 2.0 - 1.5 = 0.5
    assert gini == pytest.approx(0.5)
