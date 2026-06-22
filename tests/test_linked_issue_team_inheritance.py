"""Tests for cross-provider linked-issue team inheritance.

A PR/MR that maps to no team of its own inherits the team of an issue it links
to via ``work_item_dependencies``. The mechanism is provider-agnostic: a GitHub
PR or GitLab MR can inherit from a Linear or Jira issue (matched through a
``extkey:KEY`` edge), and same-provider links work too. This is the recovery
path that lets PRs share a team dimension with the issue trackers in the
allocation-coverage and team-exchange views.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, cast

from dev_health_ops.metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from dev_health_ops.metrics.compute_work_items import (
    ManualFallbackRule,
    TeamAttributionCandidate,
    TeamAttributionContext,
    build_linked_issue_team_resolver,
    compute_work_item_metrics_daily,
    compute_work_item_team_attributions,
    resolve_base_team,
    resolve_team_attribution,
)
from dev_health_ops.models.work_items import (
    WorkItem,
    WorkItemDependency,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.github.normalize import extract_github_dependencies
from dev_health_ops.providers.gitlab.normalize import extract_gitlab_dependencies
from dev_health_ops.providers.teams import ProjectKeyTeamResolver, TeamResolver

START = datetime(2026, 6, 1, tzinfo=timezone.utc)


def _wi(work_item_id: str, provider: str, **kw: object) -> WorkItem:
    defaults: dict[str, object] = dict(
        title="t",
        type="task",
        status="done",
        status_raw="Done",
        created_at=START - timedelta(days=2),
        updated_at=START + timedelta(hours=2),
        started_at=START,
        completed_at=START + timedelta(hours=2),
        closed_at=START + timedelta(hours=2),
        labels=[],
    )
    defaults.update(kw)
    return WorkItem(work_item_id=work_item_id, provider=provider, **defaults)  # type: ignore[arg-type]


def _chaos_resolver() -> ProjectKeyTeamResolver:
    return ProjectKeyTeamResolver(
        project_key_to_team={"CHAOS": ("CHAOS", "Chaos Team")}
    )


# --------------------------------------------------------------------------- #
# Resolver / builder unit behaviour
# --------------------------------------------------------------------------- #


def test_pr_inherits_team_from_linked_linear_issue_via_extkey() -> None:
    linear = _wi("linear:CHAOS-2400", "linear", project_key="CHAOS")
    pr = _wi("ghpr:full-chaos/ops#12", "github", type="pr", project_id="full-chaos/ops")
    deps = [
        WorkItemDependency(
            source_work_item_id=pr.work_item_id,
            target_work_item_id="extkey:CHAOS-2400",
            relationship_type="relates_to",
            relationship_type_raw="external_issue_key",
        )
    ]
    resolver = build_linked_issue_team_resolver(
        work_items=[linear, pr],
        dependencies=deps,
        project_key_resolver=_chaos_resolver(),
    )
    assert resolver.resolve(pr.work_item_id) == ("CHAOS", "Chaos Team")
    # The donor itself resolves to a real team directly, so it needs no
    # inherited entry.
    assert resolver.resolve(linear.work_item_id) == (None, None)


def test_same_provider_github_pr_to_issue_inheritance() -> None:
    # GitHub issue mapped to a team by repo-keyed project resolver; the PR lives
    # in a different, unmapped repo (so it can't self-resolve) and links to it.
    issue = _wi("gh:team-a/svc#5", "github", project_id="team-a/svc")
    pr = _wi("ghpr:contrib/forks#9", "github", type="pr", project_id="contrib/forks")
    pkr = ProjectKeyTeamResolver(
        project_key_to_team={"team-a/svc": ("team-a", "Team A")}
    )
    deps = [
        WorkItemDependency(
            source_work_item_id=pr.work_item_id,
            target_work_item_id=issue.work_item_id,
            relationship_type="relates_to",
            relationship_type_raw="relates_to",
        )
    ]
    resolver = build_linked_issue_team_resolver(
        work_items=[issue, pr], dependencies=deps, project_key_resolver=pkr
    )
    assert resolver.resolve(pr.work_item_id) == ("team-a", "Team A")


def test_inheritance_never_overrides_native_team() -> None:
    owned_pr = _wi("ghpr:x/y#1", "github", project_id="x/y", native_team_key="CHAOS")
    other = _wi("linear:OTHER-1", "linear", project_key="OTHER")
    deps = [
        WorkItemDependency(
            source_work_item_id=owned_pr.work_item_id,
            target_work_item_id="extkey:OTHER-1",
            relationship_type="relates_to",
            relationship_type_raw="external_issue_key",
        )
    ]
    pkr = ProjectKeyTeamResolver(
        project_key_to_team={"CHAOS": ("CHAOS", "Chaos Team"), "OTHER": ("OTHER", "O")}
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[owned_pr, other], dependencies=deps, project_key_resolver=pkr
    )
    assert resolver.resolve(owned_pr.work_item_id) == (None, None)
    assert resolve_base_team(owned_pr, None, pkr) == ("CHAOS", "Chaos Team")


def test_blocking_relationships_do_not_drive_inheritance() -> None:
    # blocks / blocked_by / is_blocked_by routinely span teams, so they must
    # NOT transfer a team. Only "does-the-work-of"/duplicate links do.
    donor = _wi("linear:CHAOS-1", "linear", project_key="CHAOS")
    pkr = _chaos_resolver()
    for rel in ("blocks", "blocked_by", "is_blocked_by", "other"):
        pr = _wi("ghpr:x/y#1", "github", type="pr", project_id="x/y")
        deps = [WorkItemDependency(pr.work_item_id, donor.work_item_id, rel, rel)]
        resolver = build_linked_issue_team_resolver(
            work_items=[donor, pr], dependencies=deps, project_key_resolver=pkr
        )
        assert resolver.resolve(pr.work_item_id) == (None, None), rel
    # ...but a relates/closing edge to the same donor does inherit.
    pr = _wi("ghpr:x/y#1", "github", type="pr", project_id="x/y")
    deps = [
        WorkItemDependency(pr.work_item_id, donor.work_item_id, "relates_to", "fixes")
    ]
    resolver = build_linked_issue_team_resolver(
        work_items=[donor, pr], dependencies=deps, project_key_resolver=pkr
    )
    assert resolver.resolve(pr.work_item_id) == ("CHAOS", "Chaos Team")


def test_newer_blocking_edge_supersedes_stale_inheritable_edge() -> None:
    # An edge corrected from relates_to -> blocked_by must stop inheriting, even
    # if the stale relates_to row is still present (different ReplacingMergeTree
    # version). Latest last_synced per (source,target) wins.
    donor = _wi("linear:CHAOS-1", "linear", project_key="CHAOS")
    pr = _wi("ghpr:x/y#1", "github", type="pr", project_id="x/y")
    old = WorkItemDependency(
        pr.work_item_id,
        donor.work_item_id,
        "relates_to",
        "fixes",
        last_synced=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    new = WorkItemDependency(
        pr.work_item_id,
        donor.work_item_id,
        "blocked_by",
        "blocked by",
        last_synced=datetime(2026, 6, 1, tzinfo=timezone.utc),
    )
    # Order-independent: stale relates_to never wins once superseded.
    for deps in ([old, new], [new, old]):
        resolver = build_linked_issue_team_resolver(
            work_items=[donor, pr],
            dependencies=deps,
            project_key_resolver=_chaos_resolver(),
        )
        assert resolver.resolve(pr.work_item_id) == (None, None)
    # The reverse correction (blocking -> relates) re-enables inheritance.
    newer_relates = WorkItemDependency(
        pr.work_item_id,
        donor.work_item_id,
        "relates_to",
        "fixes",
        last_synced=datetime(2026, 7, 1, tzinfo=timezone.utc),
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[donor, pr],
        dependencies=[new, newer_relates],
        project_key_resolver=_chaos_resolver(),
    )
    assert resolver.resolve(pr.work_item_id) == ("CHAOS", "Chaos Team")


def test_ambiguous_extkey_across_providers_is_dropped() -> None:
    # extkey carries no provider. If the same key exists in BOTH Linear and
    # Jira, the link is genuinely ambiguous and must not be guessed.
    linear = _wi("linear:CHAOS-1", "linear", project_key="CHAOS")
    jira = _wi("jira:CHAOS-1", "jira", project_key="CHAOS")
    pr = _wi("ghpr:x/y#1", "github", type="pr", project_id="x/y")
    deps = [WorkItemDependency(pr.work_item_id, "extkey:CHAOS-1", "relates_to", "k")]
    pkr = ProjectKeyTeamResolver(project_key_to_team={"CHAOS": ("CHAOS", "Chaos Team")})
    # Order-independent: ambiguity is detected regardless of which item is seen
    # first.
    for items in ([linear, jira, pr], [jira, linear, pr]):
        resolver = build_linked_issue_team_resolver(
            work_items=items, dependencies=deps, project_key_resolver=pkr
        )
        assert resolver.resolve(pr.work_item_id) == (None, None)


def test_unresolvable_extkey_yields_no_inheritance() -> None:
    pr = _wi("ghpr:x/y#1", "github", type="pr", project_id="x/y")
    deps = [
        WorkItemDependency(
            source_work_item_id=pr.work_item_id,
            target_work_item_id="extkey:GHOST-9",  # no such issue in the batch
            relationship_type="relates_to",
            relationship_type_raw="external_issue_key",
        )
    ]
    resolver = build_linked_issue_team_resolver(
        work_items=[pr], dependencies=deps, project_key_resolver=_chaos_resolver()
    )
    assert resolver.resolve(pr.work_item_id) == (None, None)


def test_multiple_donors_resolved_deterministically_regardless_of_order() -> None:
    # ClickHouse rows have no inherent order; multi-donor selection must not
    # depend on edge order. Smallest canonical target wins, both orderings.
    pr = _wi("ghpr:x/y#1", "github", type="pr", project_id="x/y")
    a = _wi("linear:AAA-1", "linear", project_key="AAA")
    b = _wi("linear:BBB-1", "linear", project_key="BBB")
    pkr = ProjectKeyTeamResolver(
        project_key_to_team={"AAA": ("aaa", "A"), "BBB": ("bbb", "B")}
    )
    forward = [
        WorkItemDependency(pr.work_item_id, "extkey:AAA-1", "relates_to", "k"),
        WorkItemDependency(pr.work_item_id, "extkey:BBB-1", "relates_to", "k"),
    ]
    reverse = list(reversed(forward))
    r1 = build_linked_issue_team_resolver(
        work_items=[pr, a, b], dependencies=forward, project_key_resolver=pkr
    )
    r2 = build_linked_issue_team_resolver(
        work_items=[pr, a, b], dependencies=reverse, project_key_resolver=pkr
    )
    # linear:AAA-1 < linear:BBB-1, so 'aaa' wins for both input orders.
    assert r1.resolve(pr.work_item_id) == ("aaa", "A")
    assert r2.resolve(pr.work_item_id) == ("aaa", "A")


def test_donor_completed_outside_metrics_window_still_attributes() -> None:
    # Regression for the daily-path donor-completeness fix: the donor issue
    # completed long before the metrics day. The resolver is built from a
    # window-independent donor superset, so the PR (computed on a later day)
    # still inherits. The resolver itself is date-agnostic; this documents the
    # contract the job relies on by supplying an old donor + a recent PR.
    old_donor = _wi(
        "linear:CHAOS-1",
        "linear",
        project_key="CHAOS",
        created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        completed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    pr = _wi("ghpr:full-chaos/ops#99", "github", type="pr", project_id="full-chaos/ops")
    deps = [WorkItemDependency(pr.work_item_id, "extkey:CHAOS-1", "relates_to", "k")]
    resolver = build_linked_issue_team_resolver(
        work_items=[old_donor, pr],
        dependencies=deps,
        project_key_resolver=_chaos_resolver(),
    )
    assert resolver.resolve(pr.work_item_id) == ("CHAOS", "Chaos Team")


# --------------------------------------------------------------------------- #
# End-to-end through the per-day metrics compute (the cycle-times producer)
# --------------------------------------------------------------------------- #


def test_compute_stamps_inherited_team_on_cycle_times() -> None:
    day = date(2026, 6, 1)
    linear = _wi("linear:CHAOS-2400", "linear", project_key="CHAOS")
    pr = _wi("ghpr:full-chaos/ops#12", "github", type="pr", project_id="full-chaos/ops")
    deps = [
        WorkItemDependency(
            source_work_item_id=pr.work_item_id,
            target_work_item_id="extkey:CHAOS-2400",
            relationship_type="relates_to",
            relationship_type_raw="external_issue_key",
        )
    ]
    pkr = _chaos_resolver()
    resolver = build_linked_issue_team_resolver(
        work_items=[linear, pr], dependencies=deps, project_key_resolver=pkr
    )

    _, _, cycle_times = compute_work_item_metrics_daily(
        day=day,
        work_items=[linear, pr],
        transitions=[],
        computed_at=START,
        project_key_resolver=pkr,
        linked_issue_resolver=resolver,
    )
    by_id = {c.work_item_id: c for c in cycle_times}
    assert by_id[pr.work_item_id].team_id == "CHAOS"
    assert by_id[pr.work_item_id].team_name == "Chaos Team"
    assert by_id[linear.work_item_id].team_id == "CHAOS"


def test_cycle_times_and_state_durations_agree_on_inherited_team() -> None:
    # The same PR must read with the SAME team in both work_item_cycle_times
    # and work_item_state_durations — otherwise BI rollups that join the two
    # see contradictory team slices.
    day = date(2026, 6, 1)
    linear = _wi("linear:CHAOS-2400", "linear", project_key="CHAOS")
    pr = _wi("ghpr:full-chaos/ops#12", "github", type="pr", project_id="full-chaos/ops")
    deps = [WorkItemDependency(pr.work_item_id, "extkey:CHAOS-2400", "relates_to", "k")]
    pkr = _chaos_resolver()
    resolver = build_linked_issue_team_resolver(
        work_items=[linear, pr], dependencies=deps, project_key_resolver=pkr
    )
    # state-durations needs a transition to emit a row.
    transitions = [
        WorkItemStatusTransition(
            work_item_id=pr.work_item_id,
            provider="github",
            occurred_at=START + timedelta(hours=1),
            from_status_raw=None,
            to_status_raw="closed",
            from_status="in_progress",
            to_status="done",
        )
    ]
    _, _, cycle_times = compute_work_item_metrics_daily(
        day=day,
        work_items=[pr],
        transitions=transitions,
        computed_at=START,
        project_key_resolver=pkr,
        linked_issue_resolver=resolver,
    )
    state_rows = compute_work_item_state_durations_daily(
        day=day,
        work_items=[pr],
        transitions=transitions,
        computed_at=START + timedelta(hours=6),
        project_key_resolver=pkr,
        linked_issue_resolver=resolver,
    )
    assert cycle_times[0].team_id == "CHAOS"
    assert state_rows, "expected at least one state-duration row"
    assert {r.team_id for r in state_rows} == {"CHAOS"}


def test_compute_without_resolver_leaves_pr_unassigned() -> None:
    # Regression guard: absent the resolver, the PR is still 'unassigned'
    # (proves the inheritance — not some other path — is what fixes it).
    day = date(2026, 6, 1)
    pr = _wi("ghpr:full-chaos/ops#12", "github", type="pr", project_id="full-chaos/ops")
    _, _, cycle_times = compute_work_item_metrics_daily(
        day=day,
        work_items=[pr],
        transitions=[],
        computed_at=START,
        project_key_resolver=_chaos_resolver(),
        linked_issue_resolver=None,
    )
    assert cycle_times[0].team_id == "unassigned"


def test_assignee_membership_wins_over_linked_issue() -> None:
    # CHAOS-2600 CS2: linked_issue is now rank 5 (below assignee_membership rank 4),
    # so a PR's own assignee team wins over a team inherited from a linked issue.
    day = date(2026, 6, 1)
    pr = _wi(
        "ghpr:full-chaos/ops#12",
        "github",
        type="pr",
        project_id="full-chaos/ops",
        assignees=["bob@example.com"],
    )
    other = _wi("linear:OTHER-1", "linear", project_key="OTHER")
    deps = [WorkItemDependency(pr.work_item_id, "extkey:OTHER-1", "relates_to", "k")]
    pkr = ProjectKeyTeamResolver(project_key_to_team={"OTHER": ("other", "Other")})
    team_resolver = TeamResolver(
        member_to_team={"bob@example.com": ("platform", "Platform")}
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[pr, other],
        dependencies=deps,
        team_resolver=team_resolver,
        project_key_resolver=pkr,
    )
    _, _, cycle_times = compute_work_item_metrics_daily(
        day=day,
        work_items=[pr],
        transitions=[],
        computed_at=START,
        team_resolver=team_resolver,
        project_key_resolver=pkr,
        linked_issue_resolver=resolver,
    )
    assert cycle_times[0].team_id == "platform"


def test_state_duration_assignee_membership_wins_over_linked_issue() -> None:
    day = date(2026, 6, 1)
    pr = _wi(
        "ghpr:full-chaos/ops#12",
        "github",
        type="pr",
        project_id="full-chaos/ops",
        assignees=["bob@example.com"],
    )
    other = _wi("linear:OTHER-1", "linear", project_key="OTHER")
    deps = [WorkItemDependency(pr.work_item_id, "extkey:OTHER-1", "relates_to", "k")]
    transitions = [
        WorkItemStatusTransition(
            work_item_id=pr.work_item_id,
            provider="github",
            occurred_at=START + timedelta(hours=1),
            from_status_raw=None,
            to_status_raw="closed",
            from_status="in_progress",
            to_status="done",
        )
    ]
    pkr = ProjectKeyTeamResolver(project_key_to_team={"OTHER": ("other", "Other")})
    team_resolver = TeamResolver(
        member_to_team={"bob@example.com": ("platform", "Platform")}
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[pr, other],
        dependencies=deps,
        team_resolver=team_resolver,
        project_key_resolver=pkr,
    )

    state_rows = compute_work_item_state_durations_daily(
        day=day,
        work_items=[pr],
        transitions=transitions,
        computed_at=START + timedelta(hours=6),
        team_resolver=team_resolver,
        project_key_resolver=pkr,
        linked_issue_resolver=resolver,
    )

    assert state_rows
    assert {row.team_id for row in state_rows} == {"platform"}


def test_compute_emits_attribution_candidates_and_primary_cycle_team() -> None:
    day = date(2026, 6, 1)
    linear = _wi("linear:CHAOS-2400", "linear", project_key="CHAOS")
    pr = _wi(
        "ghpr:full-chaos/ops#12",
        "github",
        type="pr",
        project_id="full-chaos/ops",
        assignees=["bob@example.com"],
    )
    deps = [WorkItemDependency(pr.work_item_id, "extkey:CHAOS-2400", "relates_to", "k")]
    pkr = _chaos_resolver()
    team_resolver = TeamResolver(
        member_to_team={"bob@example.com": ("platform", "Platform")}
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[linear, pr],
        dependencies=deps,
        team_resolver=team_resolver,
        project_key_resolver=pkr,
    )

    _, _, cycle_times = compute_work_item_metrics_daily(
        day=day,
        work_items=[pr],
        transitions=[],
        computed_at=START,
        team_resolver=team_resolver,
        project_key_resolver=pkr,
        linked_issue_resolver=resolver,
    )
    attributions = compute_work_item_team_attributions(
        work_items=[pr],
        computed_at=START,
        team_resolver=team_resolver,
        project_key_resolver=pkr,
        linked_issue_resolver=resolver,
    )

    # CHAOS-2600 CS2: assignee_membership (rank 4) now wins over linked_issue (rank 5);
    # the linked_issue candidate is still emitted for provenance, just non-primary.
    assert cycle_times[0].team_id == "platform"
    by_source = {row.source: row for row in attributions}
    assert by_source["assignee_membership"].is_primary == 1
    assert by_source["assignee_membership"].team_id == "platform"
    assert by_source["linked_issue"].is_primary == 0
    assert by_source["linked_issue"].team_id == "CHAOS"


def test_issue_project_wins_over_linked_issue() -> None:
    # CHAOS-2600 CS2: an item's OWN project key (issue_project, rank 1) outranks a
    # linked-issue donor (rank 5). The linked candidate is still emitted, non-primary.
    item = _wi(
        "linear:PROJ-9", "linear", project_key="CHAOS", assignees=["bob@example.com"]
    )
    donor = _wi("linear:OTHER-1", "linear", project_key="OTHER")
    deps = [WorkItemDependency(item.work_item_id, "extkey:OTHER-1", "relates_to", "k")]
    pkr = ProjectKeyTeamResolver(
        project_key_to_team={"CHAOS": ("CHAOS", "Chaos"), "OTHER": ("other", "Other")}
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[item, donor], dependencies=deps, project_key_resolver=pkr
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, pkr, linked_issue_resolver=resolver
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "CHAOS"
    assert by_source["issue_project"].is_primary == 1
    assert by_source["linked_issue"].is_primary == 0


def test_project_ownership_wins_over_linked_issue() -> None:
    item = _wi("gh:full-chaos/ops#5", "github", type="pr", project_id="proj-x")
    donor = _wi("linear:OTHER-1", "linear", project_key="OTHER")
    deps = [WorkItemDependency(item.work_item_id, "extkey:OTHER-1", "relates_to", "k")]
    pkr = ProjectKeyTeamResolver(project_key_to_team={"OTHER": ("other", "Other")})
    resolver = build_linked_issue_team_resolver(
        work_items=[item, donor], dependencies=deps, project_key_resolver=pkr
    )
    context = TeamAttributionContext(
        project_by_id={
            ("github", "proj-x"): [
                TeamAttributionCandidate(
                    source="project_ownership",
                    team_id="owner-team",
                    team_name="Owner",
                    confidence="high",
                    evidence="project_id=proj-x",
                )
            ]
        }
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, pkr, linked_issue_resolver=resolver, attribution_context=context
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "owner-team"
    assert by_source["project_ownership"].is_primary == 1
    assert by_source["linked_issue"].is_primary == 0


def test_repo_ownership_wins_over_linked_issue() -> None:
    item = _wi("gh:full-chaos/ops#6", "github", type="pr", project_id="full-chaos/ops")
    donor = _wi("linear:OTHER-1", "linear", project_key="OTHER")
    deps = [WorkItemDependency(item.work_item_id, "extkey:OTHER-1", "relates_to", "k")]
    pkr = ProjectKeyTeamResolver(project_key_to_team={"OTHER": ("other", "Other")})
    resolver = build_linked_issue_team_resolver(
        work_items=[item, donor], dependencies=deps, project_key_resolver=pkr
    )
    context = TeamAttributionContext(
        repo_by_name={
            ("github", "full-chaos/ops"): [
                TeamAttributionCandidate(
                    source="repo_ownership",
                    team_id="repo-team",
                    team_name="Repo",
                    confidence="medium",
                    evidence="repo_full_name=full-chaos/ops",
                )
            ]
        }
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, pkr, linked_issue_resolver=resolver, attribution_context=context
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "repo-team"
    assert by_source["repo_ownership"].is_primary == 1
    assert by_source["linked_issue"].is_primary == 0


def test_issue_project_deconflicts_duplicate_project_ownership_by_key() -> None:
    # CHAOS-2600 CS2: the issue's own project key resolved as issue_project must NOT also be
    # emitted as an imported project_ownership row for the SAME team (one fact, one provenance
    # row). A genuinely different team claimed by-key is a real lower-precedence signal and is kept.
    item = _wi("linear:PROJ-1", "linear", project_key="PROJ")
    pkr = ProjectKeyTeamResolver(project_key_to_team={"PROJ": ("team-project", "Proj")})
    context = TeamAttributionContext(
        project_by_key={
            ("linear", "PROJ"): [
                TeamAttributionCandidate(
                    source="project_ownership",
                    team_id="team-project",  # duplicate of issue_project — must be suppressed
                    team_name="Proj",
                    confidence="high",
                    evidence="project_key=PROJ",
                ),
                TeamAttributionCandidate(
                    source="project_ownership",
                    team_id="team-other",  # different ownership claim — must be retained
                    team_name="Other",
                    confidence="high",
                    evidence="project_key=PROJ",
                ),
            ]
        }
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, pkr, attribution_context=context
    )
    by_source_teams = [(c.source, c.team_id) for c in candidates]
    assert team_id == "team-project"
    assert ("issue_project", "team-project") in by_source_teams
    assert ("project_ownership", "team-project") not in by_source_teams
    assert ("project_ownership", "team-other") in by_source_teams


def test_jira_issue_project_wins_over_linked_issue() -> None:
    # CHAOS-2600 CS2: precedence is provider-agnostic. A JIRA issue whose own project key
    # resolves to a team (issue_project, rank 1) is not overridden by a linked donor (rank 5).
    item = _wi("jira:PROJ-5", "jira", project_key="PROJ")
    donor = _wi("linear:OTHER-1", "linear", project_key="OTHER")
    deps = [WorkItemDependency(item.work_item_id, "extkey:OTHER-1", "relates_to", "k")]
    pkr = ProjectKeyTeamResolver(
        project_key_to_team={
            "PROJ": ("jira-team", "Jira Team"),
            "OTHER": ("other", "Other"),
        }
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[item, donor], dependencies=deps, project_key_resolver=pkr
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, pkr, linked_issue_resolver=resolver
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "jira-team"
    assert by_source["issue_project"].is_primary == 1
    assert by_source["linked_issue"].is_primary == 0


def test_assignee_membership_wins_over_jira_linked_donor() -> None:
    # CHAOS-2600 CS2: a PR inheriting from a JIRA donor issue is still demoted below the PR's
    # own assignee membership (proves Jira donors obey the same rank-5 fallback as Linear).
    pr = _wi(
        "ghpr:full-chaos/ops#21",
        "github",
        type="pr",
        project_id="full-chaos/ops",
        assignees=["bob@example.com"],
    )
    jira_donor = _wi("jira:OPS-1", "jira", project_key="OPS")
    deps = [WorkItemDependency(pr.work_item_id, "extkey:OPS-1", "relates_to", "k")]
    pkr = ProjectKeyTeamResolver(project_key_to_team={"OPS": ("ops", "Ops")})
    team_resolver = TeamResolver(
        member_to_team={"bob@example.com": ("platform", "Platform")}
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[pr, jira_donor],
        dependencies=deps,
        team_resolver=team_resolver,
        project_key_resolver=pkr,
    )
    team_id, _, candidates = resolve_team_attribution(
        pr, team_resolver, pkr, linked_issue_resolver=resolver
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "platform"
    assert by_source["assignee_membership"].is_primary == 1
    assert (
        by_source["linked_issue"].team_id == "ops"
    )  # Jira donor inherited, but non-primary
    assert by_source["linked_issue"].is_primary == 0


def test_gitlab_mr_resolver_precedence_with_gitlab_donor() -> None:
    # CHAOS-2600 CS2: GitLab at the RESOLVER level (resolve_team_attribution), both as the
    # attributed item (a GitLab MR) and as the linked donor (a same-provider GitLab issue).
    # The MR's own assignee (rank 4) wins over the team inherited from the GitLab donor (rank 5).
    mr = _wi(
        "gitlab:full-chaos/ops!5",
        "gitlab",
        type="pr",
        project_id="full-chaos/ops",
        assignees=["bob@example.com"],
    )
    gl_donor = _wi("gitlab:team-svc/repo#10", "gitlab", project_key="SVC")
    deps = [
        WorkItemDependency(
            mr.work_item_id, gl_donor.work_item_id, "relates_to", "relates_to"
        )
    ]
    pkr = ProjectKeyTeamResolver(project_key_to_team={"SVC": ("svc", "Svc")})
    team_resolver = TeamResolver(
        member_to_team={"bob@example.com": ("platform", "Platform")}
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[mr, gl_donor],
        dependencies=deps,
        team_resolver=team_resolver,
        project_key_resolver=pkr,
    )
    team_id, _, candidates = resolve_team_attribution(
        mr, team_resolver, pkr, linked_issue_resolver=resolver
    )
    by_source = {c.source: c for c in candidates}
    assert (
        team_id == "platform"
    )  # assignee_membership (4) beats GitLab-donor linked_issue (5)
    assert by_source["assignee_membership"].is_primary == 1
    assert (
        by_source["linked_issue"].team_id == "svc"
    )  # inherited from the GitLab issue donor
    assert by_source["linked_issue"].is_primary == 0


def test_manual_fallback_applies_only_when_nothing_stronger() -> None:
    # CHAOS-2600 CS3: a repo-scoped manual fallback resolves a PR that has no
    # native/imported/linked team. It is rank 6 (just above unassigned).
    item = _wi(
        "ghpr:full-chaos/ops#30", "github", type="pr", project_id="full-chaos/ops"
    )
    ctx = TeamAttributionContext(
        manual_fallbacks=[
            ManualFallbackRule(
                provider="github",
                scope_type="repo",
                scope_id="full-chaos/ops",
                team_id="ops",
                team_name="Ops",
                reason="explicit",
            )
        ]
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, None, attribution_context=ctx
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "ops"
    assert by_source["manual_fallback"].is_primary == 1
    assert by_source["manual_fallback"].confidence == "manual"


def test_manual_fallback_never_overrides_native_team() -> None:
    item = _wi("ghpr:x/y#1", "github", project_id="x/y", native_team_key="CHAOS")
    pkr = ProjectKeyTeamResolver(project_key_to_team={"CHAOS": ("CHAOS", "Chaos")})
    ctx = TeamAttributionContext(
        manual_fallbacks=[
            ManualFallbackRule(
                provider="github",
                scope_type="repo",
                scope_id="x/y",
                team_id="ops",
                team_name="Ops",
            )
        ]
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, pkr, attribution_context=ctx
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "CHAOS"
    assert by_source["native_team"].is_primary == 1
    assert by_source["manual_fallback"].is_primary == 0


def test_manual_fallback_never_overrides_linked_issue_donor() -> None:
    # linked_issue (rank 5) beats manual_fallback (rank 6).
    pr = _wi("ghpr:full-chaos/ops#31", "github", type="pr", project_id="full-chaos/ops")
    donor = _wi("linear:OTHER-1", "linear", project_key="OTHER")
    deps = [WorkItemDependency(pr.work_item_id, "extkey:OTHER-1", "relates_to", "k")]
    pkr = ProjectKeyTeamResolver(project_key_to_team={"OTHER": ("other", "Other")})
    resolver = build_linked_issue_team_resolver(
        work_items=[pr, donor], dependencies=deps, project_key_resolver=pkr
    )
    ctx = TeamAttributionContext(
        manual_fallbacks=[
            ManualFallbackRule(
                provider="github",
                scope_type="repo",
                scope_id="full-chaos/ops",
                team_id="ops",
                team_name="Ops",
            )
        ]
    )
    team_id, _, candidates = resolve_team_attribution(
        pr, None, pkr, linked_issue_resolver=resolver, attribution_context=ctx
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "other"
    assert by_source["linked_issue"].is_primary == 1
    assert by_source["manual_fallback"].is_primary == 0


def test_issue_key_prefix_without_donor_matches_manual_not_linked_issue() -> None:
    # A Linear issue with key prefix CHAOS, no native/ownership team, no linked donor:
    # it must NOT become linked_issue; an issue_key_prefix manual fallback resolves it.
    item = _wi("linear:CHAOS-77", "linear")
    ctx = TeamAttributionContext(
        manual_fallbacks=[
            ManualFallbackRule(
                provider="linear",
                scope_type="issue_key_prefix",
                scope_id="CHAOS",
                team_id="chaos",
                team_name="Chaos",
            )
        ]
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, None, attribution_context=ctx
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "chaos"
    assert by_source["manual_fallback"].is_primary == 1
    assert "linked_issue" not in by_source


def test_issue_key_prefix_is_provider_neutral_and_only_manual() -> None:
    # issue_key_prefix is provider-neutral and only ever emits manual_fallback.
    item = _wi("jira:OPS-5", "jira")
    ctx = TeamAttributionContext(
        manual_fallbacks=[
            ManualFallbackRule(
                provider="linear",
                scope_type="issue_key_prefix",
                scope_id="OPS",
                team_id="ops",
                team_name="Ops",
            )
        ]
    )
    team_id, _, candidates = resolve_team_attribution(
        item, None, None, attribution_context=ctx
    )
    by_source = {c.source: c for c in candidates}
    assert team_id == "ops"
    assert by_source["manual_fallback"].source == "manual_fallback"


def test_manual_fallback_donor_is_not_laundered_into_linked_issue() -> None:
    # A donor whose ONLY resolution is manual_fallback (here an issue_key_prefix
    # rule) must never become a linked-issue donor: that would relabel a rank-6
    # fallback as rank-5 `linked_issue` provenance on the dependent. The PR links
    # to that donor but has no team of its own, so it must stay unassigned with
    # NO linked_issue candidate emitted.
    donor = _wi("linear:CHAOS-77", "linear")
    pr = _wi("ghpr:full-chaos/web#9", "github", type="pr", project_id="full-chaos/web")
    deps = [
        WorkItemDependency(pr.work_item_id, "extkey:CHAOS-77", "relates_to", "fixes")
    ]
    ctx = TeamAttributionContext(
        manual_fallbacks=[
            ManualFallbackRule(
                provider="linear",
                scope_type="issue_key_prefix",
                scope_id="CHAOS",
                team_id="chaos",
                team_name="Chaos",
            )
        ]
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[donor, pr],
        dependencies=deps,
        attribution_context=ctx,
    )
    team_id, _, candidates = resolve_team_attribution(
        pr, None, None, linked_issue_resolver=resolver, attribution_context=ctx
    )
    by_source = {c.source: c for c in candidates}
    assert "linked_issue" not in by_source
    assert team_id != "chaos"
    assert by_source["unassigned"].is_primary == 1


def test_context_project_repo_membership_tiebreak_and_unassigned() -> None:
    project_item = _wi(
        "linear:PROJ-1",
        "linear",
        project_id="project-1",
        project_key="PROJ",
        native_team_key=None,
    )
    repo_item = _wi(
        "gh:full-chaos/dev-health#9",
        "github",
        project_id="full-chaos/dev-health",
    )
    member_item = _wi(
        "jira:MEM-1",
        "jira",
        project_key="MEM",
        assignees=["ada@example.com"],
    )
    orphan = _wi("gh:unknown/repo#1", "github", project_id="unknown/repo")
    older = START - timedelta(days=1)
    newer = START
    context = TeamAttributionContext(
        project_by_id={
            ("linear", "project-1"): [
                TeamAttributionCandidate(
                    source="project_ownership",
                    team_id="team-b",
                    team_name="Team B",
                    confidence="high",
                    evidence="project_id=project-1",
                    is_primary=0,
                    specificity=10,
                    priority=10,
                    updated_at=newer,
                ),
                TeamAttributionCandidate(
                    source="project_ownership",
                    team_id="team-a",
                    team_name="Team A",
                    confidence="high",
                    evidence="project_id=project-1",
                    is_primary=1,
                    specificity=10,
                    priority=10,
                    updated_at=older,
                ),
            ]
        },
        repo_by_name={
            ("github", "full-chaos/dev-health"): [
                TeamAttributionCandidate(
                    source="repo_ownership",
                    team_id="repo-team",
                    team_name="Repo Team",
                    confidence="high",
                    evidence="repo_full_name=full-chaos/dev-health",
                )
            ]
        },
        member_by_identity={
            ("jira", "ada@example.com"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="member-team",
                    team_name="Member Team",
                    confidence="high",
                    evidence="raw_email=ada@example.com",
                )
            ]
        },
    )

    project_team_id, _, project_candidates = resolve_team_attribution(
        project_item, None, None, attribution_context=context
    )
    repo_team_id, _, _ = resolve_team_attribution(
        repo_item, None, None, attribution_context=context
    )
    member_team_id, _, _ = resolve_team_attribution(
        member_item, None, None, attribution_context=context
    )
    orphan_team_id, orphan_team_name, orphan_candidates = resolve_team_attribution(
        orphan, None, None, attribution_context=context
    )

    assert project_team_id == "team-a"
    assert [c.team_id for c in project_candidates[:2]] == ["team-a", "team-b"]
    assert repo_team_id == "repo-team"
    assert member_team_id == "member-team"
    assert orphan_team_id is None
    assert orphan_team_name is None
    assert orphan_candidates[0].source == "unassigned"


def test_context_repo_candidate_dedupes_id_and_name_match() -> None:
    repo_item = _wi(
        "gh:full-chaos/dev-health#9",
        "github",
        project_id="full-chaos/dev-health",
        repo_id="repo-1",
    )
    candidate = TeamAttributionCandidate(
        source="repo_ownership",
        team_id="repo-team",
        team_name="Repo Team",
        confidence="high",
        evidence="repo_ownership=repo-1",
        is_primary=1,
        specificity=100,
        priority=1,
        updated_at=START,
    )
    context = TeamAttributionContext(
        repo_by_id={("github", "repo-1"): [candidate]},
        repo_by_name={("github", "full-chaos/dev-health"): [candidate]},
    )

    team_id, _, candidates = resolve_team_attribution(
        repo_item, None, None, attribution_context=context
    )

    assert team_id == "repo-team"
    assert [(c.team_id, c.source, c.is_primary) for c in candidates] == [
        ("repo-team", "repo_ownership", 1)
    ]


# --------------------------------------------------------------------------- #
# Provider link capture emits the edges the mechanism consumes
# --------------------------------------------------------------------------- #


def test_github_captures_external_keys_from_body_and_branch() -> None:
    class _Head:
        ref = "chris/chaos-2400-fix"

    class _PR:
        body = "This PR closes CHAOS-2401 and fixes #5."
        head = _Head()

    deps = extract_github_dependencies(
        work_item_id="ghpr:full-chaos/ops#12",
        issue_or_pr=cast(Any, _PR()),
        repo_full_name="full-chaos/ops",
    )
    targets = {d.target_work_item_id for d in deps}
    assert "extkey:CHAOS-2400" in targets  # from branch name
    assert "extkey:CHAOS-2401" in targets  # from body magic word
    assert "gh:full-chaos/ops#5" in targets  # same-repo ref preserved


def test_github_external_key_relationship_type_follows_keyword() -> None:
    # Blocking intent must produce a non-inheritable relationship; closing /
    # branch keys must produce inheritable ones.
    class _Head:
        ref = "user/eng-7-feature"

    class _PR:
        body = "Blocked by CHAOS-1. Fixes PROJ-2."
        head = _Head()

    deps = extract_github_dependencies(
        work_item_id="ghpr:o/r#1", issue_or_pr=cast(Any, _PR()), repo_full_name="o/r"
    )
    rel_by_target = {
        d.target_work_item_id: d.relationship_type
        for d in deps
        if d.target_work_item_id.startswith("extkey:")
    }
    assert rel_by_target["extkey:CHAOS-1"] == "blocked_by"  # not inheritable
    assert rel_by_target["extkey:PROJ-2"] == "relates_to"  # inheritable
    assert rel_by_target["extkey:ENG-7"] == "external_issue_key"  # branch, inheritable


def test_blocked_by_external_key_does_not_drive_inheritance_end_to_end() -> None:
    # The capture types it blocked_by and the resolver excludes that type, so a
    # PR that merely says "blocked by CHAOS-1" never inherits the CHAOS team.
    class _Head:
        ref = ""

    class _PR:
        body = "Blocked by CHAOS-1."
        head = _Head()

    deps = extract_github_dependencies(
        work_item_id="ghpr:o/r#1", issue_or_pr=cast(Any, _PR()), repo_full_name="o/r"
    )
    donor = _wi("linear:CHAOS-1", "linear", project_key="CHAOS")
    pr = _wi("ghpr:o/r#1", "github", type="pr", project_id="o/r")
    resolver = build_linked_issue_team_resolver(
        work_items=[donor, pr],
        dependencies=deps,
        project_key_resolver=_chaos_resolver(),
    )
    assert resolver.resolve(pr.work_item_id) == (None, None)


def test_gitlab_captures_external_key_from_description() -> None:
    wi = _wi(
        "gitlab:grp/proj#7",
        "gitlab",
        type="issue",
        description="Depends on PROJ-99 for the backend work",
    )
    deps = extract_gitlab_dependencies(
        work_item_id=wi.work_item_id,
        issue=wi,
        project_full_path="grp/proj",
    )
    assert any(d.target_work_item_id == "extkey:PROJ-99" for d in deps)


def test_jira_native_link_drives_inheritance() -> None:
    # Jira issues link to each other natively; an unassigned Jira issue inherits
    # from a linked, team-attributed Jira issue.
    owned = _wi("jira:CHAOS-1", "jira", project_key="CHAOS")
    orphan = _wi("jira:LOOSE-9", "jira", project_key="LOOSE")  # no team mapping
    deps = [
        WorkItemDependency(
            source_work_item_id=orphan.work_item_id,
            target_work_item_id=owned.work_item_id,
            relationship_type="relates_to",
            relationship_type_raw="relates",
        )
    ]
    resolver = build_linked_issue_team_resolver(
        work_items=[owned, orphan],
        dependencies=deps,
        project_key_resolver=_chaos_resolver(),
    )
    assert resolver.resolve(orphan.work_item_id) == ("CHAOS", "Chaos Team")
