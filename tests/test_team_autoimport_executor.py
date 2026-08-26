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
        if "FROM identities FINAL" in query:
            return [
                {
                    "canonical_id": "jira:member-1",
                    "email": "ADA@EXAMPLE.COM",
                    "provider_identities": '{"jira": ["jira-user-1"]}',
                    "team_ids": ["team-member"],
                    "updated_at": now,
                }
            ]
        if "FROM teams FINAL" in query:
            return [
                {"id": "team-member", "name": "Member Team", "members": []},
            ]
        if "team_memberships" in query:
            # CHAOS-4321: provider auto-import fallback layer -- empty here,
            # exercised separately below.
            return []
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

    # Six reads: project / repo ownership + admin identities + admin teams
    # (CHAOS-4321: the admin-authored `identities`/`teams` catalog is the
    # override layer) + provider `team_memberships` (fallback layer, chris
    # 08:30 PT: "manual is override -- if the override exists, use it, else
    # use attribution from providers") + manual fallbacks.
    assert len(calls) == 6
    project_query = next(q for q in calls if "team_project_ownership" in q)
    # Ownership reads dedup per logical scope via argMax (NOT FINAL, which is
    # ineffective while valid_from is in the table sort key), and stay org-scoped.
    assert "argMax" in project_query
    assert "GROUP BY" in project_query
    assert "FINAL" not in project_query
    assert "org_id = {org_id:String}" in project_query
    assert "argMax(name, (updated_at, last_synced, name))" in project_query
    assert "ORDER BY g.provider, g.project_id, g.team_id" in project_query

    assert context.project_by_id[("linear", "project-1")][0].team_id == "team-project"
    assert context.project_by_key[("linear", "PROJ")][0].team_id == "team-project"
    assert (
        context.repo_by_name[("github", "full-chaos/dev-health")][0].team_id
        == "team-repo"
    )
    # Every facet of the admin-mapped identity resolves to its team: the
    # provider raw id, the identity's email, and its canonical_id.
    assert (
        context.member_by_identity[("jira", "jira-user-1")][0].team_id == "team-member"
    )
    assert (
        context.member_by_identity[("jira", "ada@example.com")][0].team_id
        == "team-member"
    )
    assert (
        context.member_by_identity[("jira", "jira:member-1")][0].team_id
        == "team-member"
    )
    identities_query = next(q for q in calls if "FROM identities FINAL" in q)
    assert "is_active = 1" in identities_query
    teams_query = next(q for q in calls if "FROM teams FINAL" in q)
    assert "is_active = 1" in teams_query

    repo_query = next(q for q in calls if "team_repo_ownership" in q)
    assert "argMax(name, (updated_at, last_synced, name))" in repo_query
    assert "ORDER BY g.provider, g.repo_full_name, g.team_id" in repo_query

    assert len(context.manual_fallbacks) == 1
    manual = context.manual_fallbacks[0]
    assert manual.provider == "github"
    assert manual.scope_type == "repo"
    assert manual.scope_id == "full-chaos/dev-health"
    assert manual.team_id == "team-manual"
    assert manual.priority == 5
    manual_query = next(q for q in calls if "manual_attribution_fallbacks" in q)
    assert "FINAL" in manual_query
    manual_order = manual_query.split("ORDER BY", 1)[1]
    positions = [
        manual_order.index(column)
        for column in (
            "o.provider",
            "o.scope_type",
            "o.scope_id",
            "o.priority",
            "o.team_id",
            "o.team_name",
            "o.reason",
        )
    ]
    assert positions == sorted(positions)


@pytest.mark.asyncio
async def test_loader_preserves_explicit_zero_manual_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        if "manual_attribution_fallbacks" not in query:
            return []
        return [
            {
                "provider": "github",
                "scope_type": "repo",
                "scope_id": "repo-zero",
                "team_id": "team-zero",
                "team_name": "Zero Team",
                "reason": "explicit highest priority",
                "priority": 0,
            },
            {
                "provider": "github",
                "scope_type": "repo",
                "scope_id": "repo-default",
                "team_id": "team-default",
                "team_name": "Default Team",
                "reason": "legacy nullable row",
                "priority": None,
            },
        ]

    monkeypatch.setattr(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query,
    )

    context = await ClickHouseDataLoader(
        object(), org_id="org-1"
    ).load_team_attribution_context(as_of=now)

    assert [rule.priority for rule in context.manual_fallbacks] == [0, 100]


@pytest.mark.asyncio
async def test_loader_identity_facets_feed_assignee_membership_before_roster_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4321: an admin-authored identity's facets (canonical_id, email,
    every provider raw id) all resolve to its admin-mapped team, and that
    admin-authored candidate outranks the legacy global-roster
    ``TeamResolver`` fallback (unused in production -- ``load_team_resolver``
    reads an empty ``config/team_mapping.yaml`` -- but exercised here to pin
    the ordering contract)."""
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        if "team_project_ownership" in query or "team_repo_ownership" in query:
            return []
        if "FROM identities FINAL" in query:
            return [
                {
                    "canonical_id": "gh:lead",
                    "email": "canonicalb@example.com",
                    "provider_identities": '{"github": ["canonicala@example.com"]}',
                    "team_ids": ["team-platform"],
                    "updated_at": now,
                }
            ]
        if "FROM teams FINAL" in query:
            return [
                {"id": "team-platform", "name": "Platform Team", "members": []},
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
    assert (
        context.member_by_identity[("github", "canonicala@example.com")][0].team_id
        == "team-platform"
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
    # 60: admin-authored (identities/teams) beats the legacy 50-specificity
    # global roster fallback within the same assignee_membership source.
    assert candidates[0].specificity == 60
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
