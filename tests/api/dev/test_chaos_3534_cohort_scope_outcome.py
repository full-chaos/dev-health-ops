"""RED-first coverage for CHAOS-3534: a fully-resolved cohort publishes
``unresolved`` on the wire.

The defect, and why it is CHAOS-3497 one branch short:

A question naming two real repositories resolves BOTH exactly, commits a real
``dev_subject_set.v1``, and then terminates on D1 -- the v1 surface cannot
render a cohort (``subject_preflight``'s ``committed_cohort_v1_only``). The
``scope.resolved`` event that terminal publishes is built by
``contracts_v2.compat.scope_resolution_from_frame``, which has exactly three
branches: ``clarification_candidates`` -> AMBIGUOUS, ``subject_ref`` ->
AMBIGUOUS, else -> **UNRESOLVED**.

A committed-cohort frame carries neither of the first two. So the run reports
``unresolved`` for a run that resolved every named subject exactly -- the same
class of defect CHAOS-3497 was created to fix (a non-answer terminal
misreporting its own scope decision), missing the one branch nobody had a
cohort to notice.

The ticket's stated mechanism was different and is refuted on the ticket
itself: the mentions are not failing to extract, and D2 is never consulted
(D2 exists for PARTIAL cohorts -- this one has no unresolved mention at all).

WHY THE SUBJECT SET, AND NOT THE FRAME, IS THE SOURCE OF TRUTH HERE: the
frame structurally cannot carry a cohort, so re-deriving from it can only
ever produce the wrong answer. The orchestrator has the preflight's own
``subject_set`` in hand at that seam. ``DevSubjectSet.cohort_complete`` is
the discriminator rather than a re-derived "did everything resolve" -- its
own model validator guarantees it is False whenever any mention was omitted
(``a cohort with omitted mentions cannot be marked complete``), so a partial
cohort cannot reach the exact branch by accident.

NOTE the coupling to CHAOS-3533: that change made the terminate ledger
survive, AND made the committed subject set survive a ledger-write rollback.
Without the second half, a run that lost its subject set to an unrelated
ledger fault would publish ``unresolved`` again here -- this very bug, back
through a different door.
"""

from __future__ import annotations

import uuid

import pytest

from dev_health_ops.api.dev.contracts import (
    V1_SCOPE_LIST_LIMIT,
    ScopeResolutionOutcome,
)
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    ORG_ID,
    run_preflight_orchestrator,
)

#: CHAOS-3534's own corpus case, verbatim from
#: ``case-scope.bounded-subject-set.json``.
WEB_APP = AuthorizedEntity(
    EntityKind.REPOSITORY, "meridian/web-app", "meridian/web-app"
)
API_GATEWAY = AuthorizedEntity(
    EntityKind.REPOSITORY, "meridian/api-gateway", "meridian/api-gateway"
)
COHORT_QUESTION = (
    'What\'s the status of repo "meridian/web-app" and repo "meridian/api-gateway"?'
)


@pytest.mark.asyncio
async def test_a_fully_resolved_cohort_publishes_exact_not_unresolved() -> None:
    """The defect in its exact corpus shape.

    Asserts the state an auditor reads -- the resolution this run publishes on
    the wire -- not that a branch executed. ``unresolved`` here is a false
    statement about a run that committed both named subjects.
    """

    output = await run_preflight_orchestrator(
        question=COHORT_QUESTION,
        entities=[(ORG_ID, WEB_APP), (ORG_ID, API_GATEWAY)],
        script_id="chaos-3534-cohort",
    )

    # Setup control: this really is the committed-cohort terminate. Without
    # it a change that made the run fail earlier would satisfy the assertion
    # below for entirely the wrong reason.
    assert output.recorder is not None
    assert output.recorder.preflight_diagnostics == [("committed_cohort_v1_only", None)]

    resolution = output.result.scope_resolution
    assert resolution is not None, (
        "CHAOS-3497 requires every terminal to publish its scope decision"
    )
    assert resolution.outcome is ScopeResolutionOutcome.EXACT, (
        "CHAOS-3534: a run that resolved both named repositories exactly and "
        "committed a dev_subject_set.v1 must not report 'unresolved' -- that "
        "is a false statement about the run's own scope decision, and it is "
        f"what the corpus case measured. Got {resolution.outcome!r}."
    )
    # The conventions here are INHERITED from committed_resolution_for, not
    # chosen: a repository commit carries its ids in
    # authorized_repository_ids and leaves authorized_entity_ids empty, and
    # the list is sorted rather than in question order. This assertion
    # originally got that backwards, which is exactly the drift a second
    # producer written from scratch introduces -- caught by executing it, and
    # the reason the cohort builder sits beside the singular one.
    assert resolution.authorized_repository_ids == [
        "meridian/api-gateway",
        "meridian/web-app",
    ], (
        "an 'exact' outcome must name what it committed -- an exact "
        "resolution carrying nothing is the vacuous-pass shape CHAOS-3497 "
        "already found once on this same event"
    )
    assert resolution.authorized_entity_ids == []
    assert resolution.resolved_scope is not None, (
        "DevScopeResolution's own validator requires a resolved_scope for a "
        "resolved outcome; an 'exact' with none would not even construct"
    )
    assert sorted(resolution.resolved_scope.repositories) == [
        "meridian/api-gateway",
        "meridian/web-app",
    ]


@pytest.mark.asyncio
async def test_a_partial_cohort_does_not_claim_exact() -> None:
    """Negative control, and the reason ``cohort_complete`` is the gate.

    A cohort that omitted a named member did NOT resolve everything the
    question named, so claiming ``exact`` for it would be the same class of
    false statement in the opposite direction -- and this is precisely the
    shape a re-derived "are there committed entities?" test would get wrong.

    ``Nightfall`` is never seeded, so it is omitted; the two Atlas projects
    are distinct real entities that resolve exactly. The run still commits a
    cohort, but an incomplete one.
    """

    output = await run_preflight_orchestrator(
        question="Compare project Atlas One and project Atlas Two and project Nightfall",
        entities=[
            (
                ORG_ID,
                AuthorizedEntity(EntityKind.PROJECT, "project-atlas-1", "Atlas One"),
            ),
            (
                ORG_ID,
                AuthorizedEntity(EntityKind.PROJECT, "project-atlas-2", "Atlas Two"),
            ),
            (ORG_ID, ASK_DEV_PROJECT),
        ],
        script_id="chaos-3534-partial",
    )

    resolution = output.result.scope_resolution
    assert resolution is not None
    assert resolution.outcome is not ScopeResolutionOutcome.EXACT, (
        "a cohort with an omitted member never resolved everything the "
        "question named, so it must not report 'exact' -- cohort_complete is "
        f"the gate for exactly this. Got {resolution.outcome!r}."
    )


@pytest.mark.asyncio
async def test_an_ambiguous_terminal_still_publishes_ambiguous() -> None:
    """Preserved-behaviour control: the branch CHAOS-3497 already got right
    must keep working. If adding the cohort branch ever shadowed this one,
    a clarification would start reporting the wrong outcome and the
    candidates an auditor scans would vanish with it."""

    output = await run_preflight_orchestrator(
        question="What's the status of the Atlas project?",
        entities=[(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)],
        script_id="chaos-3534-ambiguous-control",
    )

    resolution = output.result.scope_resolution
    assert resolution is not None
    assert resolution.outcome is ScopeResolutionOutcome.AMBIGUOUS
    assert resolution.candidates, (
        "an ambiguous resolution must still carry the candidates it offered "
        "-- an empty list here is the vacuous pass CHAOS-3497 fixed"
    )


@pytest.mark.asyncio
async def test_a_not_found_terminal_still_publishes_unresolved() -> None:
    """The other preserved-behaviour control, and the one that keeps the fix
    honest: ``unresolved`` must remain reachable and correct. A change that
    simply stopped emitting it would make the first test pass while breaking
    the eight CHAOS-3533 cases that legitimately expect it."""

    output = await run_preflight_orchestrator(
        question="What is the status of the Nightfall project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="chaos-3534-not-found-control",
    )

    resolution = output.result.scope_resolution
    assert resolution is not None
    assert resolution.outcome is ScopeResolutionOutcome.UNRESOLVED
    assert not resolution.candidates
    assert not resolution.authorized_entity_ids


@pytest.mark.asyncio
async def test_a_complete_project_cohort_keeps_its_prior_outcome_and_never_crashes() -> (
    None
):
    """The DOCUMENTED RESIDUAL, pinned so it cannot be quietly forgotten.

    v1 cannot represent a multi-entity direct scope for any kind except
    repository. ``DevScope.validate_direct_scope`` requires EXACTLY ONE
    matching ``entity_ref`` for project/work-unit/issue/pull-request/team
    ("direct entity scope requires one matching entity"), while
    ``repositories`` is a list. So a complete PROJECT cohort resolved every
    member exactly and still cannot say so on the wire -- the same false
    ``unresolved`` this ticket fixes for repositories, left in place for
    project cohorts because closing it needs a v1 contract change.

    Found the hard way: the first cut of this fix ignored the distinction,
    built an invalid ``DevScope``, and turned this run's honest
    ``feature_not_enabled`` into an opaque ``internal_error`` -- strictly
    worse than the defect being fixed. Both existing CHAOS-3301 cohort
    controls caught it.

    So this test pins BOTH halves: the run must keep its honest terminal
    (never ``internal_error``), and the residual is asserted as a residual
    rather than described in a comment nobody re-reads.
    """

    output = await run_preflight_orchestrator(
        question="Compare project Atlas One and project Atlas Two",
        entities=[
            (
                ORG_ID,
                AuthorizedEntity(EntityKind.PROJECT, "project-atlas-1", "Atlas One"),
            ),
            (
                ORG_ID,
                AuthorizedEntity(EntityKind.PROJECT, "project-atlas-2", "Atlas Two"),
            ),
        ],
        script_id="chaos-3534-project-cohort-residual",
    )

    assert output.recorder is not None
    assert output.recorder.preflight_diagnostics == [("committed_cohort_v1_only", None)]
    assert output.result.error is not None
    assert output.result.error.code == "feature_not_enabled", (
        "a project cohort must keep its honest terminal -- an internal_error "
        "here means the repository-only restriction was dropped and an "
        "unrepresentable scope was constructed anyway"
    )

    resolution = output.result.scope_resolution
    assert resolution is not None
    assert resolution.outcome is ScopeResolutionOutcome.UNRESOLVED, (
        "RESIDUAL, tracked not fixed: a complete PROJECT cohort still "
        "publishes 'unresolved' despite resolving every member exactly, "
        "because v1 has no multi-entity project scope. If this assertion "
        "ever fails because the outcome became 'exact', the contract gained "
        "a representation and this residual is closed -- update it "
        "deliberately rather than loosening it."
    )


def test_the_cohort_builder_refuses_an_unrepresentable_kind() -> None:
    """And the backstop refuses loudly at its own boundary.

    The orchestrator gates on repository kind, but a future caller will not
    remember to. This proves the builder itself raises a NAMED error rather
    than constructing an invalid ``DevScope`` and failing deep inside
    pydantic -- which is exactly how the first cut produced an opaque
    ``internal_error``.

    The subject set is built by the REAL producer
    (``ScopeResolutionService.committed_subject_set_for``), never
    hand-authored: a hand-written fixture passes a unit test while diverging
    from what production actually constructs, and the first attempt at this
    test did exactly that -- inventing a ``set_id`` the contract rejects.
    """

    from datetime import UTC, datetime

    from dev_health_ops.api.dev.contracts import DevScope
    from dev_health_ops.api.dev.scope_service import (
        ScopeRequestCache,
        ScopeResolutionService,
    )
    from tests._chaos_3292_preflight import SeededCatalog, scope_dict

    service = ScopeResolutionService(SeededCatalog([]), cache=ScopeRequestCache())
    subject_set = service.committed_subject_set_for(
        [
            AuthorizedEntity(EntityKind.PROJECT, "project-atlas-1", "Atlas One"),
            AuthorizedEntity(EntityKind.PROJECT, "project-atlas-2", "Atlas Two"),
        ],
        set_id=str(uuid.uuid4()),
        original_mention_count=2,
    )
    assert subject_set.cohort_complete, (
        "setup control: an incomplete cohort would be refused by the caller's "
        "own gate, so this test would prove nothing about the kind check"
    )

    with pytest.raises(ValueError, match="no multi-entity v1 scope representation"):
        ScopeResolutionService.committed_cohort_resolution_for(
            subject_set,
            org_id=ORG_ID,
            base_scope=DevScope.model_validate(scope_dict(organization_id=ORG_ID)),
            resolved_at=datetime.now(UTC),
        )


@pytest.mark.asyncio
async def test_a_21_repository_cohort_does_not_crash_the_terminal() -> None:
    """Codex adversarial review, HIGH: the v1 list caps are LOWER than the
    subject set's own bound, and the exact-cohort publisher copies every
    committed ref into both bounded lists.

    ``DevSubjectSet.committed_entity_refs`` allows up to 25.
    ``DevScope.repositories`` and ``DevScopeResolution.
    authorized_repository_ids`` are both capped at 20. So a fully-resolved,
    fully-authorized cohort of 21-25 repositories would raise during terminal
    construction and turn an honest ``feature_not_enabled`` into an opaque
    ``internal_error``.

    That is the SAME defect the project-cohort restriction already fixed,
    at a different boundary -- a bound that is legal upstream and illegal
    downstream. Finding it twice in one change is the argument for asserting
    the boundary rather than reasoning about it: the first instance was
    caught by an existing test, this one only by review.

    A cohort that does not fit v1's lists keeps the prior frame-derived
    terminal rather than crashing. It is not disclosed as exact -- v1 cannot
    say what it committed -- and that under-disclosure is the honest failure
    mode, the same call the project-cohort residual makes.
    """

    repositories = [
        AuthorizedEntity(
            EntityKind.REPOSITORY,
            f"meridian/repo-{index:02d}",
            f"meridian/repo-{index:02d}",
        )
        for index in range(V1_SCOPE_LIST_LIMIT + 1)
    ]
    quoted = " and ".join(f'repo "{repo.canonical_id}"' for repo in repositories)

    output = await run_preflight_orchestrator(
        question=f"What's the status of {quoted}?",
        entities=[(ORG_ID, repo) for repo in repositories],
        script_id="chaos-3534-oversized-cohort",
    )

    # Setup control: this really is a committed cohort of >20 members, not a
    # run that was rejected earlier for some other reason. Without it a
    # change to the mention bound would make this test vacuous.
    assert output.recorder is not None
    assert output.recorder.preflight_diagnostics == [("committed_cohort_v1_only", None)]

    assert output.result.error is not None
    assert output.result.error.code != "internal_error", (
        "a fully-resolved cohort larger than v1's 20-item scope lists must "
        "not crash the terminal -- publishing the exact outcome is optional, "
        "keeping a coherent terminal is not"
    )

    # Codex adversarial review round 2 (MEDIUM, confirmed): asserting only
    # "not internal_error" and "some resolution exists" left the real failure
    # mode uncovered. An implementation that TRUNCATED 21 refs into a 20-item
    # exact scope would have satisfied both -- and that is a WORSE defect than
    # the crash this test was written for, because it claims `exact` while
    # silently dropping a repository the user named. This codebase's own rule
    # is "bounds are rejections, never truncations" (subject_preflight, on the
    # mention bound). The full contract is asserted instead.
    resolution = output.result.scope_resolution
    assert resolution is not None, (
        "it must still publish SOME scope decision (CHAOS-3497)"
    )
    assert resolution.outcome is ScopeResolutionOutcome.UNRESOLVED, (
        "an oversized cohort keeps the frame-derived outcome -- v1 cannot "
        f"list what it committed, so it must not claim exact. Got "
        f"{resolution.outcome!r}."
    )
    assert resolution.authorized_repository_ids == [], (
        "and it must publish NO repository list at all: a 20-item list for a "
        "21-repository cohort is a silent truncation, which discloses a "
        "subject set the run did not actually commit"
    )
    assert resolution.resolved_scope is None, (
        "no resolved_scope either -- there is no faithful v1 scope for this "
        "cohort, and inventing a bounded one is the same truncation by "
        "another route"
    )


@pytest.mark.asyncio
async def test_a_cohort_at_exactly_the_v1_limit_still_publishes_exact() -> None:
    """The other half of the boundary pair.

    A guard that refuses at 21 proves nothing on its own -- one written as
    ``<`` instead of ``<=`` would also refuse at 20 and silently cost the
    largest publishable cohort its disclosure, with no test noticing. Both
    sides of the bound are asserted, and the values were confirmed by
    execution rather than reasoning: 19 and 20 publish ``exact`` carrying
    every id, 21 falls back.
    """

    repositories = [
        AuthorizedEntity(
            EntityKind.REPOSITORY,
            f"meridian/repo-{index:02d}",
            f"meridian/repo-{index:02d}",
        )
        for index in range(V1_SCOPE_LIST_LIMIT)
    ]
    quoted = " and ".join(f'repo "{repo.canonical_id}"' for repo in repositories)

    output = await run_preflight_orchestrator(
        question=f"What's the status of {quoted}?",
        entities=[(ORG_ID, repo) for repo in repositories],
        script_id="chaos-3534-limit-cohort",
    )

    assert output.recorder is not None
    assert output.recorder.preflight_diagnostics == [("committed_cohort_v1_only", None)]

    resolution = output.result.scope_resolution
    assert resolution is not None
    assert resolution.outcome is ScopeResolutionOutcome.EXACT, (
        "a cohort of exactly V1_SCOPE_LIST_LIMIT members fits v1's lists and "
        "must still be disclosed -- an off-by-one here costs the largest "
        "publishable cohort its scope outcome, silently"
    )
    assert len(resolution.authorized_repository_ids) == V1_SCOPE_LIST_LIMIT
