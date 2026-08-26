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


@pytest.mark.asyncio
async def test_loader_bare_teams_members_facet_resolves_via_fallback_not_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4321 fix (chris, 2026-08-26 10:39 PT, after a codex adversarial
    review HIGH finding, "the new membership layer can turn provider-imported
    rosters into authoritative, provider-neutral admin overrides"):
    teams.members mixes admin-curated entries with UNREVIEWED provider
    auto-import roster writes (team_autoimport_github.py's
    AUTO_APPLY_POLICY path writes straight into it), so it is NOT
    admin-exclusive and cannot be the override layer. A bare teams.members
    facet with no teams.manual_members entry and no identities row must
    still resolve -- via the provider-FALLBACK tier
    (provider_member_by_untyped_facet), not the admin override
    (member_by_untyped_facet)."""
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        if "FROM teams FINAL" in query:
            return [
                {
                    "id": "team-fallback",
                    "name": "Fallback Team",
                    "members": ["bob@example.com"],
                    "manual_members": [],
                },
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query,
    )

    context = await ClickHouseDataLoader(
        object(), org_id="org-1"
    ).load_team_attribution_context(as_of=now)

    assert context.member_by_untyped_facet == {}
    assert (
        context.provider_member_by_untyped_facet["bob@example.com"][0].team_id
        == "team-fallback"
    )

    item = WorkItem(
        work_item_id="gh:full-chaos/dev-health#1",
        provider="github",
        title="Bare teams.members facet",
        type="issue",
        status="todo",
        status_raw="Open",
        created_at=now,
        updated_at=now,
        assignees=["bob@example.com"],
        labels=[],
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, None, attribution_context=context
    )
    assert team_id == "team-fallback"
    primary = next(candidate for candidate in candidates if candidate.is_primary == 1)
    assert primary.source == "assignee_membership"
    assert primary.confidence == "high"
    assert primary.specificity == 50


@pytest.mark.asyncio
async def test_loader_manual_members_overrides_a_conflicting_teams_members_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half of the fix above: a teams.manual_members entry IS the
    admin override, and wins outright even when the SAME identity also
    appears in a DIFFERENT team's bare teams.members roster (the shape a
    provider auto-import row takes) -- the admin layer short-circuits before
    the (ambiguous) fallback pool is ever consulted."""
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        if "FROM teams FINAL" in query:
            return [
                {
                    "id": "team-fallback",
                    "name": "Fallback Team",
                    "members": ["carol@example.com"],
                    "manual_members": [],
                },
                {
                    "id": "team-override",
                    "name": "Override Team",
                    # add_members mirrors manual writes into `members` too
                    # (legacy TeamResolver compat) -- this row's own presence
                    # in the fallback pool must not matter once the admin
                    # layer resolves unambiguously.
                    "members": ["carol@example.com"],
                    "manual_members": ["carol@example.com"],
                },
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query,
    )

    context = await ClickHouseDataLoader(
        object(), org_id="org-1"
    ).load_team_attribution_context(as_of=now)

    item = WorkItem(
        work_item_id="gh:full-chaos/dev-health#2",
        provider="github",
        title="Admin override wins over a conflicting fallback entry",
        type="issue",
        status="todo",
        status_raw="Open",
        created_at=now,
        updated_at=now,
        assignees=["carol@example.com"],
        labels=[],
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, None, attribution_context=context
    )
    assert team_id == "team-override"
    primary = next(candidate for candidate in candidates if candidate.is_primary == 1)
    assert primary.source == "assignee_membership"
    assert primary.confidence == "high"
    assert primary.specificity == 60


@pytest.mark.asyncio
async def test_loader_scopes_teams_members_fallback_facet_by_confirmed_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4321 round 3 (team-lead ruling, 2026-08-26, codex adversarial
    review HIGH finding): a bare (non-email) teams.members facet the loader
    can confirm -- via identities.provider_identities -- belongs to ONE
    specific provider must only resolve items from THAT provider. Demoting
    teams.members to the fallback tier is not enough on its own: without
    this, a GitHub-imported roster login could still attribute a
    Jira/GitLab/Linear item sharing the same raw string, at fallback
    priority instead of override priority."""
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        if "FROM identities FINAL" in query:
            return [
                {
                    "canonical_id": "github:lead-user",
                    "email": None,
                    "provider_identities": '{"github": ["lead"]}',
                    "team_ids": [],
                    "updated_at": now,
                },
            ]
        if "FROM teams FINAL" in query:
            return [
                {
                    "id": "team-eng",
                    "name": "Engineering",
                    "members": ["lead", "alice@example.com"],
                    "manual_members": [],
                },
            ]
        return []

    monkeypatch.setattr(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query,
    )

    context = await ClickHouseDataLoader(
        object(), org_id="org-1"
    ).load_team_attribution_context(as_of=now)

    # The bare login "lead" is confirmed for github ONLY -- routed into the
    # typed provider_member_by_identity pool, not the untyped one.
    assert "lead" not in context.provider_member_by_untyped_facet
    assert (
        context.provider_member_by_identity[("github", "lead")][0].team_id == "team-eng"
    )
    # The email-shaped facet stays untyped (CHAOS-2609 bare-email matching).
    assert (
        context.provider_member_by_untyped_facet["alice@example.com"][0].team_id
        == "team-eng"
    )

    jira_item = WorkItem(
        work_item_id="jira:PROJ-1",
        provider="jira",
        title="Cross-provider probe",
        type="issue",
        status="todo",
        status_raw="Open",
        created_at=now,
        updated_at=now,
        assignees=["lead"],
        labels=[],
    )
    team_id, _, candidates = resolve_team_attribution(
        jira_item, None, None, attribution_context=context
    )
    assert team_id is None, "a github-tagged login must not attribute a jira item"
    assert candidates[0].source == "unassigned"
    assert candidates[0].evidence == "no_candidate:no_membership"

    github_item = WorkItem(
        work_item_id="gh:full-chaos/dev-health#3",
        provider="github",
        title="Same-provider probe",
        type="issue",
        status="todo",
        status_raw="Open",
        created_at=now,
        updated_at=now,
        assignees=["lead"],
        labels=[],
    )
    team_id, _, candidates = resolve_team_attribution(
        github_item, None, None, attribution_context=context
    )
    assert team_id == "team-eng", "the same login must still attribute its own provider"
