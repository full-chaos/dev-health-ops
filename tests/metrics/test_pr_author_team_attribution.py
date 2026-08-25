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
    build_linked_issue_team_resolver,
    compute_work_item_team_attributions,
    resolve_team_attribution,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemDependency
from dev_health_ops.providers.teams import (
    LinkedIssueTeamResolver,
    ProjectKeyTeamResolver,
    TeamResolver,
)

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


def test_reporter_never_resolves_through_the_legacy_team_resolver():
    """Negative control (codex, 2026-08-24): a reporter must resolve ONLY
    through the org-scoped attribution_context/member_by_identity path, NEVER
    through the legacy TeamResolver -- TeamResolver can load from a global,
    non-org-scoped config with no ambiguity concept of its own, so a second
    reporter lookup through it would both bypass the ambiguity gate and risk
    one tenant's mapping stamping another tenant's PR. A populated
    team_resolver with a matching entry for the reporter, and NO
    attribution_context at all, must still resolve unassigned. (The
    pre-existing assignee legacy path is untouched -- see
    test_assignee_still_outranks_nothing_and_author_never_overrides_a_real_assignee
    for that unchanged behavior.)"""
    item = _pr_work_item(reporter="alice", assignees=[])
    team_id, team_name, candidates = resolve_team_attribution(
        item,
        team_resolver=TeamResolver(member_to_team={"alice": ("team-ops", "Ops Team")}),
        project_key_resolver=None,
    )
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]


def test_pr_author_resolves_to_team_via_attribution_context_with_relabeled_evidence():
    """GREEN: the author resolves through the org-scoped attribution_context
    (the only reporter path that exists), and its evidence is REWRITTEN to
    `reporter=<identity>` (not passed through verbatim) so a reporter-
    resolved row is distinguishable from an assignee-resolved one -- both for
    a human reading the row and for
    WORK_ITEM_TEAM_ATTRIBUTIONS_WRITTEN_TOTAL's author/assignee split
    (codex, 2026-08-24: the un-rewritten evidence made "author" unreachable
    in real data)."""
    item = _pr_work_item(reporter="alice", assignees=[])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=alice",
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
    assert (team_id, team_name) == ("team-ops", "Ops Team")
    primary = [c for c in candidates if c.is_primary]
    assert len(primary) == 1
    assert primary[0].source == "author_membership"
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
    assert any(c.source == "author_membership" and c.is_primary for c in candidates)


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
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=alice",
                    is_primary=1,
                    specificity=50,
                )
            ]
        }
    )
    records = compute_work_item_team_attributions(
        work_items=[item],
        computed_at=COMPUTED_AT,
        attribution_context=context,
    )
    primary = [r for r in records if r.is_primary]
    assert len(primary) == 1
    assert primary[0].work_item_id == "ghpr:full-chaos/dev-health-ops#4244"
    assert primary[0].provider == "github"
    assert primary[0].team_id == "team-ops"
    assert primary[0].source == "author_membership"
    assert primary[0].evidence == "reporter=alice"


def test_author_never_outranks_a_linked_issue_donor():
    """CHAOS-4244 precedence ruling (chris, 2026-08-24): a PR with a
    team-mapped author AND a linked_issue donor for a DIFFERENT team must
    resolve to the linked issue's team -- author_membership (rank 6) sits
    BELOW linked_issue (rank 5). This directly falsifies codex round 1's
    finding 2 (author, sharing assignee_membership's rank 4, could beat a
    real linked_issue donor) now that author has its own lower rank."""
    item = _pr_work_item(reporter="alice", assignees=[])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=alice",
                    is_primary=1,
                    specificity=50,
                )
            ]
        }
    )
    linked_issue_resolver = LinkedIssueTeamResolver(
        _inherited={item.work_item_id: ("team-platform", "Platform Team")}
    )
    team_id, team_name, candidates = resolve_team_attribution(
        item,
        team_resolver=None,
        project_key_resolver=None,
        linked_issue_resolver=linked_issue_resolver,
        attribution_context=context,
    )
    assert (team_id, team_name) == ("team-platform", "Platform Team")
    by_source = {c.source: c for c in candidates}
    assert by_source["author_membership"].is_primary == 0
    assert by_source["author_membership"].team_id == "team-ops"
    assert by_source["linked_issue"].is_primary == 1
    assert by_source["linked_issue"].team_id == "team-platform"


def test_causal_author_never_outranks_a_real_linked_issue_donor():
    """Codex round-2 finding (2026-08-24, MEDIUM): the test above injects the
    linked_issue candidate directly via LinkedIssueTeamResolver(_inherited=...),
    proving only that an ALREADY-SUPPLIED linked_issue candidate outranks
    author_membership -- it never exercises donor discovery/eligibility
    (`_DONOR_SOURCES`), so it could stay green even if donor construction were
    broken. This test drives the REAL production builder
    (`build_linked_issue_team_resolver`) end to end: a Linear issue that
    resolves to team CHAOS via its OWN project key (a first-class,
    donor-eligible source) is linked to a GitHub PR whose author resolves to a
    DIFFERENT team via attribution_context. The PR must inherit the donor's
    team, not the author's."""
    donor = WorkItem(
        work_item_id="linear:CHAOS-2400",
        provider="linear",
        title="t",
        type="task",
        status="done",
        status_raw="Done",
        project_key="CHAOS",
        created_at=COMPUTED_AT,
        updated_at=COMPUTED_AT,
    )
    pr = _pr_work_item(reporter="alice", assignees=[])
    dependency = WorkItemDependency(
        source_work_item_id=pr.work_item_id,
        target_work_item_id=donor.work_item_id,
        relationship_type="relates_to",
        relationship_type_raw="relates_to",
    )
    project_key_resolver = ProjectKeyTeamResolver(
        project_key_to_team={"CHAOS": ("CHAOS", "Chaos Team")}
    )
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=alice",
                    is_primary=1,
                    specificity=50,
                )
            ]
        }
    )
    real_resolver = build_linked_issue_team_resolver(
        work_items=[donor, pr],
        dependencies=[dependency],
        project_key_resolver=project_key_resolver,
        attribution_context=context,
    )
    assert real_resolver.resolve(pr.work_item_id) == ("CHAOS", "Chaos Team")

    team_id, team_name, candidates = resolve_team_attribution(
        pr,
        team_resolver=None,
        project_key_resolver=project_key_resolver,
        linked_issue_resolver=real_resolver,
        attribution_context=context,
    )
    assert (team_id, team_name) == ("CHAOS", "Chaos Team")
    by_source = {c.source: c for c in candidates}
    assert by_source["linked_issue"].is_primary == 1
    assert by_source["linked_issue"].team_id == "CHAOS"
    assert by_source["author_membership"].is_primary == 0
    assert by_source["author_membership"].team_id == "team-ops"


def test_causal_author_only_donor_never_becomes_a_linked_issue_donor():
    """The other half of the codex round-2 finding: an item whose ONLY
    resolvable team comes from author_membership must NOT register as a
    linked_issue donor at all (`_DONOR_SOURCES` excludes it, ranks 0-4 only),
    so a dependent item pointing at it gets NO inherited team -- an
    author-only donor must never be laundered into rank-5 linked_issue
    provenance on a dependent. Drives the same real `build_linked_issue_team_resolver`
    builder the positive test above does."""
    donor = WorkItem(
        work_item_id="ghpr:full-chaos/dev-health-ops#100",
        provider="github",
        title="t",
        type="pr",
        status="done",
        status_raw="merged",
        reporter="bob",
        assignees=[],
        created_at=COMPUTED_AT,
        updated_at=COMPUTED_AT,
    )
    dependent = WorkItem(
        work_item_id="ghpr:full-chaos/dev-health-ops#101",
        provider="github",
        title="t",
        type="pr",
        status="done",
        status_raw="merged",
        created_at=COMPUTED_AT,
        updated_at=COMPUTED_AT,
    )
    dependency = WorkItemDependency(
        source_work_item_id=dependent.work_item_id,
        target_work_item_id=donor.work_item_id,
        relationship_type="relates_to",
        relationship_type_raw="relates_to",
    )
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "bob"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-donor-only",
                    team_name="Donor Only Team",
                    confidence="medium",
                    evidence="assignee_membership=bob",
                    is_primary=1,
                    specificity=50,
                )
            ]
        }
    )
    # Sanity: the donor itself DOES resolve to a team, via author_membership only.
    donor_team_id, _, donor_candidates = resolve_team_attribution(
        donor,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=context,
    )
    assert donor_team_id == "team-donor-only"
    assert (
        next(c.source for c in donor_candidates if c.is_primary) == "author_membership"
    )

    real_resolver = build_linked_issue_team_resolver(
        work_items=[donor, dependent],
        dependencies=[dependency],
        attribution_context=context,
    )
    assert real_resolver.resolve(dependent.work_item_id) == (None, None)


def test_author_membership_never_applies_to_a_non_pr_issue():
    """Codex round-3 finding (2026-08-24, MEDIUM): `author_membership` is
    documented and tested throughout as a PR/MR author attribution mechanism
    ("Why this exists": GitHub PRs, item.reporter as the PR's opener) -- the
    resolver must NOT silently widen this to every WorkItem type. A Jira bug
    (or any non-PR/MR issue) created by a mapped member must stay unassigned
    on this signal alone, exactly as it did before CHAOS-4244."""
    item = WorkItem(
        work_item_id="jira:CHAOS-500",
        provider="jira",
        title="t",
        type="bug",
        status="todo",
        status_raw="To Do",
        reporter="alice",
        assignees=[],
        created_at=COMPUTED_AT,
        updated_at=COMPUTED_AT,
    )
    context = TeamAttributionContext(
        member_by_identity={
            ("jira", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=alice",
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
    assert (team_id, team_name) == (None, None)
    assert [c.source for c in candidates] == ["unassigned"]


def test_bot_author_never_attributed():
    """chris's precision condition (2026-08-24): a bot/App author (dependabot,
    github-actions, ...) carries no team meaning and must be excluded
    outright, even when its identity happens to match a real member_by_identity
    row (a coincidence a config could produce, but must never be honored)."""
    item = _pr_work_item(reporter="github:dependabot[bot]", assignees=[])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "dependabot[bot]"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=dependabot[bot]",
                    is_primary=1,
                    specificity=50,
                )
            ]
        }
    )
    records = compute_work_item_team_attributions(
        work_items=[item],
        computed_at=COMPUTED_AT,
        attribution_context=context,
    )
    primary = [r for r in records if r.is_primary]
    assert len(primary) == 1
    assert primary[0].team_id is None
    assert primary[0].source == "unassigned"
    assert primary[0].evidence == "no_candidate:bot_author"


def test_ambiguous_reporter_evidence_tags_the_unassigned_row():
    """The `unassigned` row must be traceable: when nothing else resolves
    either, its evidence carries WHY the reporter path specifically
    declined, not a bare 'no_candidate' (CHAOS-4150 doctrine -- make the
    miss loud)."""
    item = _pr_work_item(reporter="alice", assignees=[])
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=alice",
                    is_primary=1,
                    specificity=50,
                ),
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-platform",
                    team_name="Platform Team",
                    confidence="medium",
                    evidence="assignee_membership=alice",
                    is_primary=1,
                    specificity=50,
                ),
            ]
        }
    )
    records = compute_work_item_team_attributions(
        work_items=[item],
        computed_at=COMPUTED_AT,
        attribution_context=context,
    )
    primary = [r for r in records if r.is_primary]
    assert len(primary) == 1
    assert primary[0].evidence == "no_candidate:ambiguous_membership"


def test_reporter_and_assignee_same_person_same_team_stay_distinct_provenance():
    """When the author IS the assignee and both resolve to the SAME team,
    `resolve_team_attribution` keeps BOTH candidates as provenance: one
    `assignee_membership` row (rank 4, evidence "assignee=...") and one
    `author_membership` row (rank 6, evidence "reporter=..."). Splitting the
    source (CHAOS-4244's precedence ruling) makes them structurally distinct
    at the `work_item_team_attributions` ReplacingMergeTree storage key --
    (org_id, repo_id, work_item_id, team_id, source), which does NOT include
    evidence (migration 051) -- source itself now differs, so the earlier
    same-source collision this test used to guard against (codex, 2026-08-24)
    is no longer possible via the source split alone. The assignee_membership
    row must win primary: it outranks author_membership even for the
    identical team."""
    item = WorkItem(
        work_item_id="ghpr:full-chaos/dev-health-ops#77",
        provider="github",
        title="t",
        type="pr",
        status="done",
        status_raw="merged",
        reporter="alice",
        assignees=["alice"],
        created_at=COMPUTED_AT,
        updated_at=COMPUTED_AT,
    )
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=alice",
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
    assert (team_id, team_name) == ("team-ops", "Ops Team")
    by_source = {c.source: c for c in candidates if c.team_id == "team-ops"}
    assert set(by_source) == {"assignee_membership", "author_membership"}
    assert by_source["assignee_membership"].is_primary == 1
    assert by_source["author_membership"].is_primary == 0
    assert (
        by_source["assignee_membership"].evidence
        != by_source["author_membership"].evidence
    )


def test_metric_source_label_splits_author_from_assignee_on_real_rows():
    """Metric-vocabulary regression (codex, 2026-08-24): the un-rewritten
    evidence made "author" unreachable for real context-resolved rows, since
    the underlying fact's own evidence is always "assignee_membership=<id>"
    regardless of whether an assignee or a reporter identity matched it. The
    evidence-rewrite fix must make work_item_team_attribution_metric_source
    correctly split a REAL resolver-produced reporter row from a REAL
    resolver-produced assignee row -- not just a handcrafted "reporter="
    literal."""
    from dev_health_ops.metrics.prometheus import (
        work_item_team_attribution_metric_source,
    )

    reporter_item = _pr_work_item(reporter="alice", assignees=[])
    assignee_item = WorkItem(
        work_item_id="ghpr:full-chaos/dev-health-ops#88",
        provider="github",
        title="t",
        type="pr",
        status="done",
        status_raw="merged",
        assignees=["bob"],
        created_at=COMPUTED_AT,
        updated_at=COMPUTED_AT,
    )
    context = TeamAttributionContext(
        member_by_identity={
            ("github", "alice"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=alice",
                    is_primary=1,
                    specificity=50,
                )
            ],
            ("github", "bob"): [
                TeamAttributionCandidate(
                    source="assignee_membership",
                    team_id="team-ops",
                    team_name="Ops Team",
                    confidence="medium",
                    evidence="assignee_membership=bob",
                    is_primary=1,
                    specificity=50,
                )
            ],
        }
    )
    _, _, reporter_candidates = resolve_team_attribution(
        reporter_item,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=context,
    )
    _, _, assignee_candidates = resolve_team_attribution(
        assignee_item,
        team_resolver=None,
        project_key_resolver=None,
        attribution_context=context,
    )
    reporter_primary = next(c for c in reporter_candidates if c.is_primary)
    assignee_primary = next(c for c in assignee_candidates if c.is_primary)
    assert (
        work_item_team_attribution_metric_source(
            reporter_primary.source, reporter_primary.evidence
        )
        == "author"
    )
    assert (
        work_item_team_attribution_metric_source(
            assignee_primary.source, assignee_primary.evidence
        )
        == "assignee"
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
