"""CHAOS-4321 (chris's ruling, 2026-08-26 06:24-06:26 PT, Urgent): team
attribution comes ONLY from project/repo OWNERSHIP -- no owning team means
``unassigned``. Nothing about who authored or was assigned an item may ever
become a team candidate: a person can be on N teams, so member -> team is not
a function, and picking one (as ``author_membership``/CHAOS-4244 and the
older, pre-4244 ``assignee_membership`` both did) is fabrication.

RED-FIRST (ticket step 2): every case in this module fails against the
pre-CHAOS-4321 ``resolve_team_attribution``, which still stamps a primary
team candidate from ``team_memberships`` alone (via ``attribution_context.
member_by_identity`` for both the author/reporter path -- CHAOS-4244 -- and
the context-based assignee path, and via the legacy ``TeamResolver`` for the
older assignee path predating CHAOS-4244). This module intentionally does
NOT touch ``tests/metrics/test_pr_author_team_attribution.py`` (the CHAOS-4244
suite asserting the now-forbidden behavior) -- that suite is replaced in the
same commit that removes ``author_membership``/``assignee_membership`` from
``resolve_team_attribution`` (see ``docs/contribute/architecture/
team-attribution.md`` Sec 0 for the post-fix precedence ladder).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from dev_health_ops.metrics.compute_work_items import (
    TeamAttributionCandidate,
    TeamAttributionContext,
    resolve_team_attribution,
)
from dev_health_ops.metrics.prometheus import work_item_team_attribution_metric_source
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.providers.teams import TeamResolver

COMPUTED_AT = datetime(2026, 8, 26, tzinfo=timezone.utc)
REPO_ID = uuid.UUID("c7198fbc-1945-3717-05d8-eb78866b4e79")


def _pr_work_item(
    *,
    reporter: str | None = None,
    assignees: list[str] | None = None,
    repo_id: uuid.UUID | None = None,
) -> WorkItem:
    """A GitHub PR-shaped WorkItem with no native team key and no project
    key -- the shape a PR takes when nothing but ownership/membership facts
    could possibly resolve it."""
    return WorkItem(
        work_item_id="ghpr:full-chaos/dev-health-ops#4321",
        provider="github",
        title="Ownership-only attribution",
        type="pr",
        status="in_progress",
        status_raw="open",
        reporter=reporter,
        assignees=assignees or [],
        repo_id=repo_id,
        created_at=COMPUTED_AT,
        updated_at=COMPUTED_AT,
    )


def _member_candidate(team_id: str, team_name: str) -> TeamAttributionCandidate:
    # Mirrors what the ClickHouse-loaded member_by_identity path stores today
    # (source stamped "assignee_membership" at load time; the reporter path
    # relabels it "author_membership" at the point of use). Post-fix, neither
    # label may ever reach resolve_team_attribution's output.
    return TeamAttributionCandidate(
        source="assignee_membership",
        team_id=team_id,
        team_name=team_name,
        confidence="medium",
        evidence=f"assignee_membership={team_id}",
        is_primary=1,
        specificity=50,
    )


def test_author_in_a_team_that_does_not_own_the_repo_is_unassigned():
    item = _pr_work_item(reporter="alice", repo_id=REPO_ID)
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [_member_candidate("team-ops", "Ops Team")]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]
    assert candidates[0].evidence == "no_candidate:no_owning_team"


def test_author_on_two_teams_is_unassigned_no_arbitrary_pick():
    item = _pr_work_item(reporter="alice")
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                _member_candidate("team-ops", "Ops Team"),
                _member_candidate("team-platform", "Platform Team"),
            ]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]
    assert candidates[0].evidence == "no_candidate:no_owning_team"


def test_assignee_in_a_team_that_does_not_own_the_repo_is_unassigned_context_path():
    item = _pr_work_item(assignees=["bob"], repo_id=REPO_ID)
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "bob"): [_member_candidate("team-ops", "Ops Team")]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]


def test_assignee_in_a_team_that_does_not_own_the_repo_is_unassigned_legacy_path():
    """The OLDER (pre-CHAOS-4244) assignee mechanism -- the standalone
    ``TeamResolver`` passed as ``team_resolver`` -- must be removed too, not
    just the context-based one. A populated legacy resolver mapping the
    assignee to a team must not resolve when nothing owns the repo."""
    item = _pr_work_item(assignees=["bob"], repo_id=REPO_ID)
    team_id, team_name, candidates = resolve_team_attribution(
        item,
        team_resolver=TeamResolver(member_to_team={"bob": ("team-ops", "Ops Team")}),
        project_key_resolver=None,
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]


def test_assignee_on_two_teams_is_unassigned_no_arbitrary_pick():
    item = _pr_work_item(assignees=["bob"])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "bob"): [
                _member_candidate("team-ops", "Ops Team"),
                _member_candidate("team-platform", "Platform Team"),
            ]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]


def test_ownership_wins_regardless_of_membership():
    """Repo ownership resolves the team even when the author AND assignee
    (the same person here) are BOTH mapped to a DIFFERENT team via
    team_memberships -- membership must contribute NOTHING to the candidate
    list, not merely lose a precedence fight."""
    item = _pr_work_item(reporter="alice", assignees=["alice"], repo_id=REPO_ID)
    context = TeamAttributionContext(
        repo_by_id={
            # TeamAttributionContext keys are (provider, str(key)) --
            # _context_candidates str()s the lookup key (item.repo_id, a
            # UUID) before the dict.get, so the fixture key must too.
            ("github", str(REPO_ID)): [
                TeamAttributionCandidate(
                    source="repo_ownership",
                    team_id="team-repo",
                    team_name="Repository Team",
                    confidence="high",
                    evidence=f"repo_ownership={REPO_ID}",
                    is_primary=1,
                    specificity=70,
                )
            ]
        },
        member_by_identity={
            ("github", "alice"): [_member_candidate("team-other", "Other Team")]
        },
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == ("team-repo", "Repository Team")
    assert [c.source for c in candidates] == ["repo_ownership"]


def test_bot_author_with_matching_membership_row_still_unassigned():
    item = _pr_work_item(reporter="github:dependabot[bot]")
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "github:dependabot[bot]"): [
                _member_candidate("team-ops", "Ops Team")
            ]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]
    # Pinned to the new uniform reason, not the old "bot_author" one -- the
    # bot check itself is removed along with the rest of the author path, so
    # this is red-first too, not merely a same-outcome-different-reason case.
    assert candidates[0].evidence == "no_candidate:no_owning_team"


def test_no_owning_team_telemetry_reason_surfaces_through_metric_mapper():
    """Ticket step 4: an unassigned outcome must carry a telemetry-visible
    reason. work_item_team_attribution_metric_source already strips the
    ``no_candidate:`` prefix generically (used previously for bot_author /
    ambiguous_membership) -- this proves the new no_owning_team reason rides
    the same mechanism with no separate mapper change required."""
    item = _pr_work_item(reporter="alice")
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [_member_candidate("team-ops", "Ops Team")]
        }
    )
    _, _, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    primary = candidates[0]
    assert (
        work_item_team_attribution_metric_source(primary.source, primary.evidence)
        == "no_owning_team"
    )
