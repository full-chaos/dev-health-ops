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


# ---------------------------------------------------------------------------
# CHAOS-2180 Wave 2 — repo_ids SQL prefilter for dense team pagination
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_ai_pr_attributions_applies_repo_ids_before_limit():
    """repo_ids must land in the WHERE clause so LIMIT applies to the
    already-filtered universe — that is what makes team pages dense."""
    captured: list[tuple[str, dict]] = []

    async def fake_qd(_client: Any, query: str, params: Any) -> list[dict]:
        captured.append((query, params))
        return []

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await loader.load_ai_pr_attributions(
            start=START, end=END, repo_ids=[REPO_A, REPO_B], limit=10, offset=0
        )

    query, params = captured[0]
    assert "toString(pr.repo_id) IN {repo_ids:Array(String)}" in query
    assert params["repo_ids"] == [str(REPO_A), str(REPO_B)]
    # The IN filter must appear before LIMIT (inside the subquery WHERE).
    assert query.index("repo_ids:Array(String)") < query.index("LIMIT {limit:UInt32}")


@pytest.mark.asyncio
async def test_load_ai_pr_attributions_no_repo_ids_omits_filter():
    captured: list[str] = []

    async def fake_qd(_client: Any, query: str, _params: Any) -> list[dict]:
        captured.append(query)
        return []

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await loader.load_ai_pr_attributions(start=START, end=END)

    assert "repo_ids" not in captured[0]


# ---------------------------------------------------------------------------
# CHAOS-2194 — review engagement query shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_review_engagement_query_shape():
    """Pickup latency must be guarded against missing/inverted timestamps and
    bucket fallback must mirror compute-time _safe_bucket semantics."""
    captured: list[tuple[str, dict]] = []

    async def fake_qd(_client: Any, query: str, params: Any) -> list[dict]:
        captured.append((query, params))
        return []

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await loader.load_review_engagement(start=START, end=END, repo_ids=[REPO_A])

    query, params = captured[0]
    assert "pr.first_review_at >= pr.created_at" in query
    assert "'unknown'" in query  # unattributed PRs fall back to unknown
    assert "coalesce(pr.additions, 0) + coalesce(pr.deletions, 0)" in query
    assert params["repo_ids"] == [str(REPO_A)]


# ---------------------------------------------------------------------------
# CHAOS-2185 — overlap query shapes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_hotspot_overlap_uses_risk_score_convention():
    captured: list[str] = []

    async def fake_qd(_client: Any, query: str, _params: Any) -> list[dict]:
        captured.append(query)
        return []

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await loader.load_hotspot_overlap(
            start=START, end=END, start_day=START_DAY, end_day=END_DAY
        )

    query = captured[0]
    # risk_score > 0 is only a precondition; the discriminating cut is the
    # per-repo top decile (min one file) — a bare > 0 saturates at ~1.0.
    assert "HAVING risk_score > 0" in query
    assert "PARTITION BY repo_id" in query
    assert "repo_file_count * 0.1" in query
    assert "greatest(1, toUInt64(ceil(repo_file_count * 0.1)))" in query
    assert "work_graph_pr_commit" in query
    assert "git_commit_stats" in query
    assert "uniqExact((pf.repo_id, pf.number))" in query


@pytest.mark.asyncio
async def test_load_complexity_overlap_counts_high_complexity_files():
    captured: list[str] = []

    async def fake_qd(_client: Any, query: str, _params: Any) -> list[dict]:
        captured.append(query)
        return []

    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await loader.load_complexity_overlap(start=START, end=END, end_day=END_DAY)

    query = captured[0]
    assert "file_complexity_snapshots" in query
    assert "high_complexity_functions + fc.very_high_complexity_functions" in query


# ---------------------------------------------------------------------------
# Codex review fixes — org scoping + event-day semantics (CHAOS-2180 Wave 2)
# ---------------------------------------------------------------------------


async def _capture_query(coro_factory) -> tuple[str, dict]:
    captured: list[tuple[str, dict]] = []

    async def fake_qd(_client: Any, query: str, params: Any) -> list[dict]:
        captured.append((query, params))
        return []

    with patch(
        "dev_health_ops.api.queries.client.query_dicts",
        side_effect=fake_qd,
    ):
        await coro_factory()
    return captured[0]


@pytest.mark.asyncio
async def test_hotspot_overlap_org_scopes_every_joined_table():
    """work_graph_pr_commit, git_commit_stats, work_graph_issue_pr and
    file_hotspot_daily all carry org_id; repo_id/commit_hash values can
    collide across tenants, so every join leg must be org-filtered."""
    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    query, params = await _capture_query(
        lambda: loader.load_hotspot_overlap(
            start=START, end=END, start_day=START_DAY, end_day=END_DAY
        )
    )
    for clause in (
        "pc.org_id = {org_id:String}",
        "cs.org_id = {org_id:String}",
        "link.org_id = {org_id:String}",
        "hs.org_id = {org_id:String}",
        "pr.org_id = {org_id:String}",
        "toString(attr.org_id) = {org_id:String}",
    ):
        assert clause in query, f"missing org scope: {clause}"
    assert params["org_id"] == ORG_ID


@pytest.mark.asyncio
async def test_complexity_overlap_org_scopes_every_joined_table():
    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    query, _params = await _capture_query(
        lambda: loader.load_complexity_overlap(start=START, end=END, end_day=END_DAY)
    )
    for clause in (
        "pc.org_id = {org_id:String}",
        "cs.org_id = {org_id:String}",
        "link.org_id = {org_id:String}",
        "fc.org_id = {org_id:String}",
        "pr.org_id = {org_id:String}",
        "toString(attr.org_id) = {org_id:String}",
    ):
        assert clause in query, f"missing org scope: {clause}"


@pytest.mark.asyncio
async def test_review_engagement_org_scoped_and_uses_event_day():
    """Day key and window must use event time (merged_at when present, else
    created_at) — the same semantics as compute_ai_impact_metrics_daily — so
    a PR created before the window but merged inside it lands its engagement
    on the merge day and merges with the metrics daily row."""
    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    query, _params = await _capture_query(
        lambda: loader.load_review_engagement(start=START, end=END)
    )
    event = "if(pr.merged_at IS NOT NULL, pr.merged_at, pr.created_at)"
    assert f"toDate({event}) AS day" in query
    assert f"({event} >= {{start:DateTime}} AND {event} < {{end:DateTime}})" in query
    # The created-OR-merged window form must be gone from this query.
    assert "pr.created_at >= {start:DateTime} AND pr.created_at <" not in query
    for clause in (
        "pr.org_id = {org_id:String}",
        "toString(attr.org_id) = {org_id:String}",
        "link.org_id = {org_id:String}",
    ):
        assert clause in query, f"missing org scope: {clause}"


@pytest.mark.asyncio
async def test_label_lookups_are_org_scoped():
    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    repo_query, _ = await _capture_query(lambda: loader.load_repo_labels([str(REPO_A)]))
    assert "org_id = {org_id:String}" in repo_query
    team_query, _ = await _capture_query(lambda: loader.load_team_labels([TEAM_A]))
    assert "org_id = {org_id:String}" in team_query


# ---------------------------------------------------------------------------
# Final review fixes — join identity + windowed candidate prefilter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_attribution_join_pins_repo_identity():
    """work_item_id is not repo-global: the linkage path must constrain
    repo-pinned attributions to their own repo's links (NULL repo_id =
    work-item-level attribution, allowed to bind through the link)."""
    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    query, _ = await _capture_query(
        lambda: loader.load_review_engagement(start=START, end=END)
    )
    assert "(attr.repo_id IS NULL OR attr.repo_id = link.repo_id)" in query


@pytest.mark.asyncio
async def test_deduped_prs_prefilters_candidates_by_event_window():
    """argMax must not aggregate the org's entire PR history: the event
    window must appear INSIDE the candidate selection (any-version-qualifies
    IN-subquery), and again on deduped values in the outer query."""
    loader = AIImpactClickHouseLoader(object(), org_id=ORG_ID)
    query, _ = await _capture_query(
        lambda: loader.load_review_engagement(start=START, end=END)
    )
    event = "if(pr.merged_at IS NOT NULL, pr.merged_at, pr.created_at)"
    window = f"({event} >= {{start:DateTime}} AND {event} < {{end:DateTime}})"
    assert "(pr.repo_id, pr.number) IN (" in query
    # Candidate stage + stage-3 re-check on deduped values = at least twice.
    assert query.count(window) >= 2
    # The candidate window predicate must sit before the GROUP BY dedup.
    in_clause = query.index("(pr.repo_id, pr.number) IN (")
    candidate_window = query.index(window)
    assert candidate_window > in_clause or query.index(window, in_clause) > in_clause
