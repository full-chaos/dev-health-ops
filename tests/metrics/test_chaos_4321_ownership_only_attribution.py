"""CHAOS-4321 (chris's ruling, 2026-08-26): only the CHAOS-4244 author_membership
path (a PR/MR's reporter walked through ``team_memberships``) is removed.
``assignee_membership`` -- the pre-4244, rank-4 mechanism -- STAYS: chris
confirmed membership-based attribution is legitimate under the manual
override (see docs/contribute/architecture/team-attribution.md Sec 0 for the
gate reasoning). An author is simply whoever opened the item, with none of
the deliberate-curation character an assignment has; nothing about who
authored an item may become a team candidate.

RED-FIRST (ticket step 2, author scope only): every case here fails against
the pre-CHAOS-4321 ``resolve_team_attribution``, which still stamps a primary
``author_membership`` candidate from ``team_memberships`` via
``attribution_context.member_by_identity`` (the reporter path, CHAOS-4244).
This module does NOT touch ``tests/metrics/test_pr_author_team_attribution.py``
(the CHAOS-4244 suite asserting the now-forbidden author behavior; its
assignee-scoped tests, e.g. ``test_assignee_still_outranks_nothing_and_
author_never_overrides_a_real_assignee``, describe UNCHANGED behavior and
must survive) -- that suite's author-specific tests are replaced in the same
commit that removes ``author_membership`` from ``resolve_team_attribution``.

An earlier revision of this module also asserted assignee-in-non-owning-team
-> unassigned. That was WRONG: chris's ruling keeps assignee_membership
legitimate regardless of repo/project ownership, so those cases (and the
legacy ``TeamResolver``-based assignee path) are removed here, not asserted.
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
    # relabels it "author_membership" at the point of use). Post-fix, the
    # "author_membership" label may never reach resolve_team_attribution's
    # output again -- "assignee_membership" still can, unchanged.
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
    # Target reason for the eventual combined (Python + Go) telemetry change
    # (ticket step 4) -- deferred alongside the source removal, see handoff.
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


def test_ownership_wins_over_author_membership():
    """Repo ownership resolves the team even when the author is mapped to a
    DIFFERENT team via team_memberships -- the author signal must contribute
    NOTHING to the candidate list at all (not merely lose a precedence
    fight), unlike assignee_membership, which DOES still appear as a
    non-primary candidate in the equivalent situation (unchanged,
    ``tests/metrics/test_pr_author_team_attribution.py::
    test_assignee_still_outranks_nothing_and_author_never_overrides_a_real_assignee``).
    No assignee is set here, so the only candidates possible are
    repo_ownership and (pre-fix) author_membership."""
    item = _pr_work_item(reporter="alice", repo_id=REPO_ID)
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
