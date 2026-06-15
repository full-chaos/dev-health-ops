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

from dev_health_ops.metrics.compute_work_items import (
    build_linked_issue_team_resolver,
    compute_work_item_metrics_daily,
    resolve_base_team,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemDependency
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


def test_inheritance_never_overrides_a_real_team() -> None:
    # Source already resolves to its own team; the edge must not change it.
    owned_pr = _wi("ghpr:x/y#1", "github", project_id="x/y", project_key="CHAOS")
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
    # resolve_base_team already returns CHAOS, so the source isn't in the
    # inherited map at all.
    assert resolver.resolve(owned_pr.work_item_id) == (None, None)
    assert resolve_base_team(owned_pr, None, pkr) == ("CHAOS", "Chaos Team")


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


def test_first_donor_edge_wins() -> None:
    pr = _wi("ghpr:x/y#1", "github", type="pr", project_id="x/y")
    a = _wi("linear:AAA-1", "linear", project_key="AAA")
    b = _wi("linear:BBB-1", "linear", project_key="BBB")
    deps = [
        WorkItemDependency(pr.work_item_id, "extkey:AAA-1", "relates_to", "k"),
        WorkItemDependency(pr.work_item_id, "extkey:BBB-1", "relates_to", "k"),
    ]
    pkr = ProjectKeyTeamResolver(
        project_key_to_team={"AAA": ("aaa", "A"), "BBB": ("bbb", "B")}
    )
    resolver = build_linked_issue_team_resolver(
        work_items=[pr, a, b], dependencies=deps, project_key_resolver=pkr
    )
    assert resolver.resolve(pr.work_item_id) == ("aaa", "A")


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


def test_membership_team_still_wins_over_inheritance() -> None:
    # A PR whose author is a team member resolves via membership; inheritance
    # is only a fallback and must not run.
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
        issue_or_pr=_PR(),
        repo_full_name="full-chaos/ops",
    )
    targets = {d.target_work_item_id for d in deps}
    assert "extkey:CHAOS-2400" in targets  # from branch name
    assert "extkey:CHAOS-2401" in targets  # from body magic word
    assert "gh:full-chaos/ops#5" in targets  # same-repo ref preserved


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
