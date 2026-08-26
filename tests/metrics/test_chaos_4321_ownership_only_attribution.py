"""CHAOS-4321 (chris's ruling, 2026-08-26 07:09 PT, refined 08:30 PT --
supersedes the earlier 06:24/06:43 framings this module previously tested):
membership-based team attribution (assignee AND author alike) is a
TWO-LAYER resolution, plain wording (chris-approved, use verbatim
elsewhere): "A work item gets a team from the project/repo it lives in.
That is team attribution. If that finds nothing, we look at the person on
the item (assignee, or PR author). If that person is mapped to one team,
the item goes to that team. If the person is mapped to two or more teams,
we do not guess -- the item stays unassigned." "Mapped" = the ClickHouse
team mappings (the built override): layer 1 is the admin-authored
``identities`` (canonical_id -> team_ids) / ``teams`` (id -> members facet
roster) catalog written by ``/org/admin/identities`` and
``/org/admin/teams``; layer 2 is provider-imported ``team_memberships``
(auto-import), consulted ONLY when layer 1 has NO candidate at all for that
identity (chris, 08:30 PT: "manual is override -- if the override exists,
use it, else use attribution from providers"). An AMBIGUOUS layer-1 mapping
does NOT fall through to layer 2 -- the admin mapping needs fixing, not
bypassing.

``resolve_team_attribution`` never sees the raw tables -- it reads
``attribution_context.member_by_identity`` (identities, provider-scoped) ∪
``.member_by_untyped_facet`` (bare ``teams.members`` entries with no backing
identity, matched without a provider tag) for layer 1, and
``.provider_member_by_identity`` for layer 2, via the shared
``_resolve_membership`` helper. A person can be admin-mapped to N teams (an
identity's ``team_ids`` is a list); picking one is fabrication, so N>1
resolves to ``unassigned`` with reason ``ambiguous_admin_membership:<sorted
team ids>`` (or ``ambiguous_provider_membership:<...>`` if the ambiguity is
in layer 2) -- for BOTH assignee and author (assignee previously had no such
gate at all: an ambiguous member's `_ranked` specificity ordering silently
picked an arbitrary winner, which is exactly the defect this ticket exists
to remove). No mapping in either layer -> ``no_membership``.

This supersedes three earlier revisions of this module: round 1 ("remove
both sources"), round 3 ("author removed, assignee unconditionally stays"),
and the first CHAOS-4321 cut ("admin-only, provider auto-import excluded
entirely from attribution") -- all wrong per chris's 08:30 PT refinement:
provider auto-import stays as the fallback layer, not excluded.
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
    could possibly resolve it. ``type="pr"`` keeps the reporter/author path
    eligible (``_REPORTER_ELIGIBLE_TYPES``)."""
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
    # Mirrors what load_team_attribution_context now stores in
    # member_by_identity: sourced from admin-authored `identities`/`teams`
    # rows only, stamped "assignee_membership" at load time (the reporter
    # path relabels it "author_membership" at the point of use).
    return TeamAttributionCandidate(
        source="assignee_membership",
        team_id=team_id,
        team_name=team_name,
        confidence="high",
        evidence=f"assignee_membership={team_id}",
        is_primary=1,
        specificity=60,
    )


def _provider_candidate(team_id: str, team_name: str) -> TeamAttributionCandidate:
    # Mirrors what load_team_attribution_context's restored fallback layer
    # stores in provider_member_by_identity: sourced from provider
    # auto-import team_memberships (unchanged shape from before CHAOS-4321).
    return TeamAttributionCandidate(
        source="assignee_membership",
        team_id=team_id,
        team_name=team_name,
        confidence="medium",
        evidence=f"assignee_membership={team_id}",
        is_primary=1,
        specificity=50,
    )


def test_assignee_with_no_admin_mapping_is_unassigned():
    """(a) A member who exists only in provider auto-import data (never
    admin-mapped) is represented by an identity key absent from
    member_by_identity -- the loader excludes team_memberships entirely, so
    such a member never reaches this dict at all."""
    item = _pr_work_item(assignees=["alice"])
    context = TeamAttributionContext(member_by_identity={})
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]
    assert candidates[0].evidence == "no_candidate:no_membership"


def test_author_with_no_admin_mapping_is_unassigned():
    """(a) Same as above, author/reporter role."""
    item = _pr_work_item(reporter="alice")
    context = TeamAttributionContext(member_by_identity={})
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]
    assert candidates[0].evidence == "no_candidate:no_membership"


def test_assignee_admin_mapped_to_one_team_is_attributed():
    """(b) Exactly one admin-mapped team -> that team, source assignee_membership."""
    item = _pr_work_item(assignees=["alice"])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [_member_candidate("team-ops", "Ops Team")]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == ("team-ops", "Ops Team")
    assert [c.source for c in candidates] == ["assignee_membership"]


def test_author_admin_mapped_to_one_team_is_attributed():
    """(b) Same, author/reporter role -- author_membership is NOT removed by
    this ticket, only gated: it stays as a rank-6 signal, below linked_issue,
    when the reporter resolves to exactly one admin-mapped team."""
    item = _pr_work_item(reporter="alice")
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [_member_candidate("team-ops", "Ops Team")]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == ("team-ops", "Ops Team")
    assert [c.source for c in candidates] == ["author_membership"]
    assert candidates[0].evidence == "reporter=alice"


def test_assignee_admin_mapped_to_two_teams_is_unassigned_no_arbitrary_pick():
    """(c) A person can be on N teams; picking one is fabrication. This is
    the defect this ticket exists to close for ASSIGNEE specifically --
    before this fix, `_ranked`'s specificity/priority ordering silently
    picked an arbitrary winner among an ambiguous member's teams."""
    item = _pr_work_item(assignees=["alice"])
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
    assert (
        candidates[0].evidence
        == "no_candidate:ambiguous_admin_membership:team-ops,team-platform"
    )


def test_author_admin_mapped_to_two_teams_is_unassigned_no_arbitrary_pick():
    """(c) Same, author/reporter role -- the pre-existing CHAOS-4110 ambiguity
    gate, unchanged in shape, now fed by admin-only data."""
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
    assert (
        candidates[0].evidence
        == "no_candidate:ambiguous_admin_membership:team-ops,team-platform"
    )


def test_bot_author_never_attributed_even_when_admin_mapped():
    """A bot/App reporter carries no team meaning regardless of whether it
    happens to match an admin-authored membership row -- unaffected by this
    ticket's scope change, re-asserted here since the surrounding gate
    changed shape."""
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
    assert candidates[0].evidence == "no_candidate:bot_author"


def test_ownership_wins_over_assignee_and_author_membership():
    """(d) Ownership always wins, even when BOTH assignee and author are
    admin-mapped to a DIFFERENT team -- membership candidates still appear
    as non-primary provenance rows, they just never outrank a real
    repo_ownership fact."""
    item = _pr_work_item(reporter="alice", assignees=["bob"], repo_id=REPO_ID)
    context = TeamAttributionContext(
        repo_by_id={
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
            ("github", "alice"): [_member_candidate("team-other", "Other Team")],
            ("github", "bob"): [_member_candidate("team-other", "Other Team")],
        },
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == ("team-repo", "Repository Team")
    sources = {c.source for c in candidates}
    assert sources == {"repo_ownership", "assignee_membership", "author_membership"}
    primary = next(c for c in candidates if c.is_primary)
    assert primary.source == "repo_ownership"


def test_admin_membership_telemetry_reasons_surface_through_metric_mapper():
    """Ticket step 6 (telemetry): an unassigned outcome from an admin-
    membership gap must carry a telemetry-visible reason.
    work_item_team_attribution_metric_source already strips the
    ``no_candidate:`` prefix generically (previously used for bot_author /
    ambiguous_membership) -- this proves both new reasons ride the same
    mechanism with no separate mapper change required, and that the reason
    string is identical to what Go's githubWorkItemTeamAttributionMetricSource
    must also emit (parity requirement, AGENTS.md)."""
    no_mapping_item = _pr_work_item(reporter="alice")
    _, _, no_mapping_candidates = resolve_team_attribution(
        no_mapping_item,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=TeamAttributionContext(member_by_identity={}),
    )
    no_mapping_primary = no_mapping_candidates[0]
    assert (
        work_item_team_attribution_metric_source(
            no_mapping_primary.source, no_mapping_primary.evidence
        )
        == "no_membership"
    )

    ambiguous_item = _pr_work_item(reporter="alice")
    _, _, ambiguous_candidates = resolve_team_attribution(
        ambiguous_item,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=TeamAttributionContext(
            member_by_identity={
                ("github", "alice"): [
                    _member_candidate("team-ops", "Ops Team"),
                    _member_candidate("team-platform", "Platform Team"),
                ]
            }
        ),
    )
    ambiguous_primary = ambiguous_candidates[0]
    assert (
        work_item_team_attribution_metric_source(
            ambiguous_primary.source, ambiguous_primary.evidence
        )
        == "ambiguous_admin_membership"
    )


def test_teams_members_only_mapping_is_attributed_no_identities_row():
    """(e) A `teams.members` facet with no backing `identities` row is still
    an admin mapping (added directly on `/org/admin/teams/[id]/edit`) --
    matched via `member_by_untyped_facet`, without a provider tag."""
    item = _pr_work_item(assignees=["alice@example.com"])
    context = TeamAttributionContext(
        member_by_untyped_facet={
            "alice@example.com": [_member_candidate("team-ops", "Ops Team")]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == ("team-ops", "Ops Team")
    assert [c.source for c in candidates] == ["assignee_membership"]


def test_provider_only_single_team_is_attributed_via_fallback_layer():
    """(f) No admin mapping at all for this identity, but provider
    auto-import (`team_memberships`) resolves exactly one team -- chris,
    08:30 PT: "manual is override -- if the override exists, use it, else
    use attribution from providers." """
    item = _pr_work_item(reporter="alice")
    context = TeamAttributionContext(
        provider_member_by_identity={
            ("github", "alice"): [_provider_candidate("team-ops", "Ops Team")]
        }
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == ("team-ops", "Ops Team")
    assert [c.source for c in candidates] == ["author_membership"]
    assert candidates[0].evidence == "reporter=alice"


def test_admin_mapping_overrides_a_conflicting_provider_membership():
    """(g) The SAME identity resolves to a DIFFERENT team in each layer --
    the admin mapping (layer 1) wins outright; the provider layer's
    candidate never even reaches the candidate list (layer 2 is consulted
    only when layer 1 has nothing)."""
    item = _pr_work_item(assignees=["alice"])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [_member_candidate("team-ops", "Ops Team")]
        },
        provider_member_by_identity={
            ("github", "alice"): [_provider_candidate("team-other", "Other Team")]
        },
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == ("team-ops", "Ops Team")
    assert [c.source for c in candidates] == ["assignee_membership"]


def test_ambiguous_admin_mapping_does_not_fall_through_to_provider():
    """(h) The admin mapping is ambiguous (2 teams); the provider layer has
    a perfectly clean single-team answer for the SAME identity. The admin
    mapping is authoritative even when ambiguous -- it must be fixed, not
    bypassed by falling through to provider data."""
    item = _pr_work_item(assignees=["alice"])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                _member_candidate("team-ops", "Ops Team"),
                _member_candidate("team-platform", "Platform Team"),
            ]
        },
        provider_member_by_identity={
            ("github", "alice"): [_provider_candidate("team-clean", "Clean Team")]
        },
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item, team_resolver=None, project_key_resolver=None, attribution_context=context
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]
    assert (
        candidates[0].evidence
        == "no_candidate:ambiguous_admin_membership:team-ops,team-platform"
    )
