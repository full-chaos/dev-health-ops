from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dev_health_ops.metrics.compute_work_items import resolve_team_attribution
from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.providers.teams import TeamResolver


@pytest.mark.asyncio
async def test_loader_builds_team_attribution_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)
    calls: list[str] = []

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        calls.append(query)
        if "team_project_ownership" in query:
            return [
                {
                    "provider": "linear",
                    "team_id": "team-project",
                    "team_name": "Project Team",
                    "project_id": "project-1",
                    "project_key": "PROJ",
                    "is_primary": 1,
                    "specificity": 80,
                    "priority": 5,
                    "updated_at": now,
                }
            ]
        if "team_repo_ownership" in query:
            return [
                {
                    "provider": "github",
                    "team_id": "team-repo",
                    "team_name": "Repo Team",
                    "repo_id": None,
                    "repo_full_name": "full-chaos/dev-health",
                    "is_primary": 1,
                    "specificity": 60,
                    "priority": 10,
                    "updated_at": now,
                }
            ]
        if "team_memberships" in query:
            return [
                {
                    "provider": "jira",
                    "team_id": "team-member",
                    "team_name": "Member Team",
                    "member_id": "member-1",
                    "raw_provider_user_id": "jira-user-1",
                    "raw_email": "ADA@EXAMPLE.COM",
                    "identity_facets": [
                        "jira-user-1",
                        "jira:accountid:member-1",
                        "canonicalb@example.com",
                    ],
                    "is_primary": 1,
                    "specificity": 50,
                    "priority": 20,
                    "updated_at": now,
                }
            ]
        # manual_attribution_fallbacks
        return [
            {
                "provider": "github",
                "scope_type": "repo",
                "scope_id": "full-chaos/dev-health",
                "team_id": "team-manual",
                "team_name": "Manual Team",
                "reason": "ops override",
                "priority": 5,
            }
        ]

    monkeypatch.setattr(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query,
    )

    context = await ClickHouseDataLoader(
        object(), org_id="org-1"
    ).load_team_attribution_context(as_of=now)

    # Four reads now: project / repo / membership ownership + manual fallbacks.
    assert len(calls) == 4
    project_query = next(q for q in calls if "team_project_ownership" in q)
    # Ownership reads dedup per logical scope via argMax (NOT FINAL, which is
    # ineffective while valid_from is in the table sort key), and stay org-scoped.
    assert "argMax" in project_query
    assert "GROUP BY" in project_query
    assert "FINAL" not in project_query
    assert "org_id = {org_id:String}" in project_query

    assert context.project_by_id[("linear", "project-1")][0].team_id == "team-project"
    assert context.project_by_key[("linear", "PROJ")][0].team_id == "team-project"
    assert (
        context.repo_by_name[("github", "full-chaos/dev-health")][0].team_id
        == "team-repo"
    )
    assert (
        context.member_by_identity[("jira", "ada@example.com")][0].team_id
        == "team-member"
    )
    assert (
        context.member_by_identity[("jira", "canonicalb@example.com")][0].team_id
        == "team-member"
    )
    membership_query = next(q for q in calls if "team_memberships" in q)
    assert "identity_facets" in membership_query

    assert len(context.manual_fallbacks) == 1
    manual = context.manual_fallbacks[0]
    assert manual.provider == "github"
    assert manual.scope_type == "repo"
    assert manual.scope_id == "full-chaos/dev-health"
    assert manual.team_id == "team-manual"
    assert manual.priority == 5
    manual_query = next(q for q in calls if "manual_attribution_fallbacks" in q)
    assert "FINAL" in manual_query


@pytest.mark.asyncio
async def test_loader_identity_facets_feed_assignee_membership_before_roster_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        if "team_project_ownership" in query or "team_repo_ownership" in query:
            return []
        if "team_memberships" in query:
            return [
                {
                    "provider": "github",
                    "team_id": "team-platform",
                    "team_name": "Platform Team",
                    "member_id": "gh:lead",
                    "raw_provider_user_id": "canonicala@example.com",
                    "raw_email": "personal@example.com",
                    "identity_facets": [
                        "canonicala@example.com",
                        "github:lead",
                        "canonicalb@example.com",
                        "personal@example.com",
                    ],
                    "is_primary": 1,
                    "specificity": 100,
                    "priority": 10,
                    "updated_at": now,
                }
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query,
    )

    context = await ClickHouseDataLoader(
        object(), org_id="org-1"
    ).load_team_attribution_context(as_of=now)
    assert (
        context.member_by_identity[("github", "canonicalb@example.com")][0].evidence
        == "assignee_membership=gh:lead"
    )

    item = WorkItem(
        work_item_id="gh:full-chaos/dev-health#2625",
        provider="github",
        title="Distinct canonical assignee",
        type="issue",
        status="done",
        status_raw="Done",
        created_at=now,
        updated_at=now,
        assignees=["canonicalb@example.com"],
        labels=[],
    )
    roster_fallback = TeamResolver(
        member_to_team={"canonicalb@example.com": ("team-platform", "Platform Team")}
    )

    team_id, _, candidates = resolve_team_attribution(
        item,
        roster_fallback,
        None,
        attribution_context=context,
    )

    assert team_id == "team-platform"
    assert candidates[0].source == "assignee_membership"
    assert candidates[0].evidence == "assignee_membership=gh:lead"
    assert candidates[0].specificity == 100
    assert any(
        candidate.evidence == "assignee=canonicalb@example.com"
        for candidate in candidates
    )
    assert candidates.index(
        next(
            candidate
            for candidate in candidates
            if candidate.evidence == "assignee_membership=gh:lead"
        )
    ) < candidates.index(
        next(
            candidate
            for candidate in candidates
            if candidate.evidence == "assignee=canonicalb@example.com"
        )
    )
