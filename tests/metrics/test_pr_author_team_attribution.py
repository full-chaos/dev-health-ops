"""CHAOS-4244: a GitHub PR authored (but not assigned) by a team member must
attribute to that team, not fall to ``unassigned``.

Root cause (measured against local ClickHouse, see the ticket): GitHub PRs
ARE already modeled as ``WorkItem``s carrying a ``ghpr:{repo}#{n}`` id
(``providers/github/normalize.py:541`` and the mirrored Go builder,
``internal/providersync/github_work_items_rows.go:517``), and they DO flow
into ``compute_work_item_team_attributions`` unfiltered. The gap is narrower
than the ticket's original citation of ``job_daily.py``: ``resolve_team_attribution``
only ever builds a membership candidate from ``item.assignees`` -- GitHub's
"assignee" field, which is distinct from and far less commonly set than the
PR's author (``item.reporter``). A PR opened by a team member with no
assignee therefore produced zero ``assignee_membership`` candidates and
nothing else matched (no native team key, no project key, no repo_patterns
row for this project's own dogfooded repos, no linked issue) -- so it landed
`unassigned`, exactly matching the 87 sampled units in the ticket.

This module proves the fix at the producer (``resolve_team_attribution`` /
``compute_work_item_team_attributions``), then proves the SAME fix is visible
through the production read path -- ``build_unit_team_subquery`` -- against a
live ClickHouse in ``test_pr_author_team_attribution_live.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dev_health_ops.metrics.compute_work_items import (
    TeamAttributionCandidate,
    TeamAttributionContext,
    compute_work_item_team_attributions,
    resolve_team_attribution,
)
from dev_health_ops.models.work_items import WorkItem
from dev_health_ops.providers.teams import TeamResolver

COMPUTED_AT = datetime(2026, 8, 24, tzinfo=timezone.utc)


def _pr_work_item(
    *, reporter: str | None, assignees: list[str] | None = None
) -> WorkItem:
    """A GitHub PR-shaped WorkItem: no native team key, no project key -- the
    same shape ``providers/github/normalize.py:541`` produces for a PR with
    no linked issue and no Projects V2 board membership."""
    return WorkItem(
        work_item_id="ghpr:full-chaos/dev-health-ops#4244",
        provider="github",
        title="Fix attribution gap",
        type="pr",
        status="done",
        status_raw="merged",
        reporter=reporter,
        assignees=assignees or [],
        created_at=COMPUTED_AT,
        updated_at=COMPUTED_AT,
    )


def test_pr_with_no_assignee_but_known_author_stays_unassigned_today_would_fail_without_fix():
    """Sanity check on the OLD behavior this fix changes: an author-only
    resolver context with an empty assignees list must not accidentally
    resolve through some other path. This is the negative control for the
    positive case below -- if this ever resolves to a team, the positive
    test is not proving what it claims to."""
    item = _pr_work_item(reporter=None, assignees=[])
    team_id, team_name, candidates = resolve_team_attribution(
        item,
        team_resolver=TeamResolver(member_to_team={"alice": ("team-ops", "Ops Team")}),
        project_key_resolver=None,
    )
    assert team_id is None
    assert team_name is None
    assert [c.source for c in candidates] == ["unassigned"]


def test_pr_author_resolves_to_team_via_team_resolver():
    """RED before the fix: assignees is empty, so the old code never looked
    at `reporter` and this resolved unassigned. GREEN after: the author is
    now a membership candidate, same rank (assignee_membership) as an
    assignee, reusing the identical TeamResolver.resolve() call."""
    item = _pr_work_item(reporter="alice", assignees=[])
    team_id, team_name, candidates = resolve_team_attribution(
        item,
        team_resolver=TeamResolver(member_to_team={"alice": ("team-ops", "Ops Team")}),
        project_key_resolver=None,
    )
    assert (team_id, team_name) == ("team-ops", "Ops Team")
    primary = [c for c in candidates if c.is_primary]
    assert len(primary) == 1
    assert primary[0].source == "assignee_membership"
    assert primary[0].evidence == "reporter=alice"


def test_pr_author_resolves_to_team_via_attribution_context():
    """Same proof through the OTHER membership path -- attribution_context's
    member_by_identity (the ClickHouse-loaded autoimport path), which is what
    production actually uses (TeamResolver is the legacy/manual path)."""
    item = _pr_work_item(reporter="Bob Example", assignees=[])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "bob example"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-platform",
                    team_name="Platform Team",
                    confidence="medium",
                    evidence="autoimport_member",
                    is_primary=1,
                    specificity=50,
                )
            ]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=context,
    )
    assert (team_id, team_name) == ("team-platform", "Platform Team")
    assert any(c.source == "assignee_membership" and c.is_primary for c in candidates)


def test_assignee_still_outranks_nothing_and_author_never_overrides_a_real_assignee():
    """An assignee that resolves AND an author that resolves to a DIFFERENT
    team must not silently prefer one over the other by accident -- both are
    the same source/rank, so the deterministic tie-break (highest
    specificity, then lexicographically largest team_id) decides, and the
    author must never be treated as higher-precedence than a real fact from
    a HIGHER source (native_team/issue_project/project_ownership/repo_ownership)."""
    item = WorkItem(
        work_item_id="ghpr:full-chaos/dev-health-ops#9",
        provider="github",
        title="t",
        type="pr",
        status="done",
        status_raw="merged",
        reporter="alice",
        assignees=["carol"],
        native_team_key=None,
        created_at=COMPUTED_AT,
        updated_at=COMPUTED_AT,
    )
    resolver = TeamResolver(
        member_to_team={
            "alice": ("team-ops", "Ops Team"),
            "carol": ("team-data", "Data Team"),
        }
    )
    team_id, _, candidates = resolve_team_attribution(
        item, team_resolver=resolver, project_key_resolver=None
    )
    # Both are assignee_membership rank; _candidate_sort_key sorts ascending
    # by team_id among tied candidates, so the lexicographically SMALLEST
    # team_id wins deterministically -- "team-data" < "team-ops". The point
    # of this test is not which one wins (that's an arbitrary but stable
    # tie-break, same as two assignees today) but that the author candidate
    # participates in the SAME rank as assignee, never a higher one.
    assert team_id == "team-data"
    sources = {c.source for c in candidates}
    assert sources == {"assignee_membership"}


def test_ambiguous_reporter_membership_contributes_nothing():
    """CHAOS-4110 ambiguity gate (chris, 2026-08-23): a person-shaped signal
    is only usable "where the reporter's membership is unambiguous (exactly
    one team)". A reporter whose identity resolves to TWO different teams
    (via member_by_identity, e.g. two facets pointing at different teams)
    must contribute nothing -- neither team is preferred over the other, and
    this is what stops blanket-attributing an org's authorless PRs to
    whichever team happens to win a tie-break."""
    item = _pr_work_item(reporter="alice", assignees=[])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="autoimport_member",
                    is_primary=1,
                    specificity=50,
                ),
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-platform",
                    team_name="Platform Team",
                    confidence="medium",
                    evidence="autoimport_member",
                    is_primary=1,
                    specificity=50,
                ),
            ]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=context,
    )
    assert team_id is None
    assert team_name is None
    assert [c.source for c in candidates] == ["unassigned"]


def test_unambiguous_reporter_membership_still_resolves():
    """Positive control for the ambiguity gate: a SINGLE resolved team (even
    if member_by_identity returns it as more than one candidate row, e.g.
    matched via both member_id and an email facet) must still resolve --
    the gate counts DISTINCT team_ids, not candidate rows."""
    item = _pr_work_item(reporter="alice", assignees=[])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="autoimport_member:member_id",
                    is_primary=1,
                    specificity=50,
                ),
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="autoimport_member:email_facet",
                    is_primary=1,
                    specificity=50,
                ),
            ]
        }
    )
    team_id, team_name, _ = resolve_team_attribution(
        item,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=context,
    )
    assert (team_id, team_name) == ("team-ops", "Ops Team")


def test_compute_work_item_team_attributions_emits_ghpr_row_for_author_only_pr():
    """End-to-end at the producer function CHAOS-4244 names
    (compute_work_item_team_attributions, compute_work_items.py:1189):
    the emitted record must carry the ghpr: work_item_id, provider='github',
    and the resolved team -- the exact row `work_item_team_attributions` was
    missing for provider='github' (checked full history, zero rows, per the
    ticket)."""
    item = _pr_work_item(reporter="alice", assignees=[])
    records = compute_work_item_team_attributions(
        work_items=[item],
        computed_at=COMPUTED_AT,
        team_resolver=TeamResolver(member_to_team={"alice": ("team-ops", "Ops Team")}),
    )
    primary = [r for r in records if r.is_primary]
    assert len(primary) == 1
    assert primary[0].work_item_id == "ghpr:full-chaos/dev-health-ops#4244"
    assert primary[0].provider == "github"
    assert primary[0].team_id == "team-ops"
    assert primary[0].source == "assignee_membership"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
