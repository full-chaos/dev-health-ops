"""CHAOS-3366: one bounded closest-matches search for an unresolved named subject.

A *typed* named subject that resolves to ``NO_AUTHORIZED_MATCH`` terminates the
run with ``not_found`` today, naming nothing back to the user. The live failure
this closes: the organization has 23 work items titled ``Go workers…`` and no
project of that name, so "the Go workers project" dead-ends even though the
catalog holds the obvious closest matches one kind over.

Every assertion here is about the *seam*, not the copy: candidates must reach
``dev_answer_frame.v1.clarification_candidates`` through the existing
``AMBIGUOUS_CANDIDATES`` → ``needs_clarification`` mapping, bounded, tenant
scoped, and **never** auto-committed — not even a sole result whose label
equals the name outright.
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2 import (
    PublicOutcome,
    ResolutionOutcome,
)
from dev_health_ops.api.dev.preflight_outcomes import (
    CLARIFICATION_COPY,
    NOT_FOUND_CLOSE_MATCHES_KEY,
)
from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
from dev_health_ops.api.dev.scope_service import (
    MAX_CANDIDATES,
    AuthorizedEntity,
    EntityKind,
    ScopeRequestCache,
    ScopeResolutionService,
)
from dev_health_ops.api.dev.subject_preflight import (
    NOT_FOUND_FALLBACK_LIMIT,
    PREFLIGHT_DIAGNOSTICS,
    PreflightDecision,
    SubjectPreflight,
)
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    NIGHTFALL_PROJECT,
    ORG_ID,
    OTHER_ORG_ID,
    SeededCatalog,
    fixed_now,
    request_for,
    sequential_ids,
    versions,
)

#: The question shape under test. Quoted rather than bare-capitalized because
#: the kind-noun grammar's ``_NAME`` requires every word of an unquoted span to
#: be capitalized -- "the Go workers project" would type no mention at all,
#: which is a different (untyped) path entirely.
GO_WORKERS_QUESTION = 'What is the status of the "Go workers" project?'


def go_workers_issues(count: int) -> list[AuthorizedEntity]:
    """Work items titled ``Go workers…``, mirroring the live organization."""

    return [
        AuthorizedEntity(
            kind=EntityKind.ISSUE,
            canonical_id=f"linear:CHAOS-{3000 + index}",
            label=f"Go workers: stage {index:02d}",
            repository_id=None,
        )
        for index in range(count)
    ]


class LimitRecordingCatalog(SeededCatalog):
    """Records the ``limit`` every search was actually asked for.

    The bound must be provable *at the query*, not only in the returned list:
    asserting output length alone cannot tell a fallback that asked for five
    from one that asked for twenty-five and happened to be handed five.
    """

    def __init__(self, entities: Any) -> None:
        super().__init__(entities)
        self.search_limits: list[tuple[int, int]] = []

    async def search(
        self,
        org_id: str,
        query: str,
        kinds: tuple[EntityKind, ...],
        *,
        limit: int,
        include_alias_matches: bool = False,
    ) -> list[AuthorizedEntity]:
        self.search_limits.append((len(kinds), limit))
        return await super().search(
            org_id,
            query,
            kinds,
            limit=limit,
            include_alias_matches=include_alias_matches,
        )


class CrossTenantLeakingCatalog(SeededCatalog):
    """A catalog that ignores ``org_id`` on the wide fallback search.

    Not a fixture for the happy path — it is the planted defect the tenant
    test exists to catch, used only by the mutation harness.
    """

    async def search(
        self,
        org_id: str,
        query: str,
        kinds: tuple[EntityKind, ...],
        *,
        limit: int,
        include_alias_matches: bool = False,
    ) -> list[AuthorizedEntity]:
        if len(kinds) == 1:
            return await super().search(
                org_id,
                query,
                kinds,
                limit=limit,
                include_alias_matches=include_alias_matches,
            )
        needle = query.casefold()
        matched = [
            entity
            for _owner, entity in self.entities
            if entity.kind in kinds and needle in entity.label.casefold()
        ]
        matched.sort(key=lambda entity: (entity.label.casefold(), entity.canonical_id))
        return matched[:limit]


class MultiKindFailingCatalog(SeededCatalog):
    """Succeeds for a single-kind search, raises for the fallback's wide one.

    The fallback must degrade to today's ``not_found``, never turn a resolvable
    termination into an unhandled exception or a mistyped catalog outage.
    """

    async def search(
        self,
        org_id: str,
        query: str,
        kinds: tuple[EntityKind, ...],
        *,
        limit: int,
        include_alias_matches: bool = False,
    ) -> list[AuthorizedEntity]:
        if len(kinds) > 1:
            raise RuntimeError("catalog unavailable for the fallback search")
        return await super().search(
            org_id,
            query,
            kinds,
            limit=limit,
            include_alias_matches=include_alias_matches,
        )


def _preflight(
    entities: Any, *, catalog_cls: Any = SeededCatalog, catalog: Any = None
) -> SubjectPreflight:
    mint = sequential_ids()
    return SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(
            catalog if catalog is not None else catalog_cls(entities),
            cache=ScopeRequestCache(),
        ),
        versions=versions(),
        mint_id=mint,
        now=fixed_now,
    )


async def _run(preflight: SubjectPreflight, request: Any, **kwargs: Any) -> Any:
    return await preflight.run(
        request=request,
        org_id=kwargs.pop("org_id", ORG_ID),
        permission_fingerprint="permissions_01",
        authorized_scope=request.scope,
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        **kwargs,
    )


def _latest_outcome(result: Any) -> ResolutionOutcome:
    latest = result.ledger.latest_by_mention()
    (entry,) = latest.values()
    return entry.outcome


# ---------------------------------------------------------------------------
# RED -- the acceptance case
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_named_project_with_no_match_offers_bounded_close_matches() -> None:
    """The live case: 23 ``Go workers`` work items, no project of that name."""

    result = await _run(
        _preflight(
            [(ORG_ID, ASK_DEV_PROJECT)]
            + [(ORG_ID, issue) for issue in go_workers_issues(23)]
        ),
        request_for(GO_WORKERS_QUESTION),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.committed_resolution is None

    # The candidates reach the frame through the landed CHAOS-3325 channel --
    # no new outcome plumbing.
    assert result.answer is not None
    candidates = result.answer.frame.clarification_candidates
    assert candidates, "a close-matches termination must carry candidates"
    assert len(candidates) == NOT_FOUND_FALLBACK_LIMIT
    assert {candidate.entity_ref.entity_kind.value for candidate in candidates} == {
        "issue"
    }
    assert all(
        candidate.entity_ref.display_label.startswith("Go workers")
        for candidate in candidates
    )

    # And the orchestrator gets the ledger row that authorizes them.
    assert result.terminating_resolution_entry is not None
    assert (
        result.terminating_resolution_entry.candidates
        == result.answer.frame.clarification_candidates
    )
    assert _latest_outcome(result) is ResolutionOutcome.AMBIGUOUS_CANDIDATES
    assert result.diagnostic == "unresolved_close_matches"
    assert (
        result.answer.frame.direct_answer
        == CLARIFICATION_COPY[NOT_FOUND_CLOSE_MATCHES_KEY]
    )


# ---------------------------------------------------------------------------
# CHAOS-3388 -- acronym / parenthetical-alias closest matches
# ---------------------------------------------------------------------------

#: The real production catalog row this ticket's live repro named (org
#: 70d529e0-3c06-4597-8480-794fd02328b6, read live from ClickHouse
#: `projects` -- not a hand-authored stand-in): "ACR" is the acronym of
#: "Agent Context Runtime" inside the full display name.
ACR_PROJECT = AuthorizedEntity(
    kind=EntityKind.PROJECT,
    canonical_id="60997592-f9f4-462b-87c3-ef82671df270",
    label="Dev Health Agent Context Runtime (Context Fabric)",
    repository_id=None,
)

#: The literal repro question from the live incident report.
ACR_QUESTION = (
    "What's the status of the ACR Project and what drivers are blocking it? "
    "What's left to complete?"
)


@pytest.mark.asyncio
async def test_an_acronym_mention_offers_the_matching_project_as_a_close_match() -> (
    None
):
    """CHAOS-3388 acceptance: "ACR" must never dead-end as a bare no-match.

    Before acronym-aware closest matches, "ACR" is not a substring of "Dev
    Health Agent Context Runtime (Context Fabric)", so this terminated
    ``not_found`` with zero candidates -- the run either fell through to
    org-wide investigation (before the interpreter case fix) or landed here
    with nothing to offer (before this fix). Never auto-committed either
    way: an acronym is a derived signal, not a literal name the user typed.
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, ACR_PROJECT)]),
        request_for(ACR_QUESTION),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.committed_resolution is None
    assert result.diagnostic == "unresolved_close_matches"

    assert result.answer is not None
    candidates = result.answer.frame.clarification_candidates
    assert len(candidates) == 1
    assert candidates[0].entity_ref.display_label == ACR_PROJECT.label
    assert candidates[0].entity_ref.entity_id == ACR_PROJECT.canonical_id


@pytest.mark.asyncio
async def test_an_acronym_match_survives_substring_noise_past_the_bound() -> None:
    """CHAOS-3388 live finding: literal-substring noise must never crowd out
    the real acronym match.

    The live probe against the real org repro'd a genuine defect here: the
    org holds more than ``NOT_FOUND_FALLBACK_LIMIT`` issues whose titles
    contain "acr" as a plain substring (e.g. "Harden ACR runtime client
    boundary"), sorted alphabetically ahead of the actual project's full
    name -- so a combined alphabetical sort truncated the acronym-matched
    project out of the returned page entirely, and the closest-matches
    answer named everything BUT the thing "ACR" actually stood for.
    """

    noisy_issues = [
        AuthorizedEntity(
            kind=EntityKind.ISSUE,
            canonical_id=f"issue-acr-{index}",
            # Alphabetically ahead of "Dev Health Agent Context Runtime
            # (Context Fabric)" -- starts with a digit, which sorts before
            # any letter.
            label=f"{index} ACR boundary hardening task",
            repository_id=None,
        )
        for index in range(NOT_FOUND_FALLBACK_LIMIT + 3)
    ]
    result = await _run(
        _preflight([(ORG_ID, entity) for entity in [ACR_PROJECT, *noisy_issues]]),
        request_for(ACR_QUESTION),
    )

    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.answer is not None
    labels = {
        candidate.entity_ref.display_label
        for candidate in result.answer.frame.clarification_candidates
    }
    assert ACR_PROJECT.label in labels, (
        "the real acronym match must survive the bound even when outnumbered "
        "by incidental substring hits"
    )


@pytest.mark.asyncio
async def test_multiple_acronym_matches_are_offered_without_picking() -> None:
    """Two catalog projects sharing an acronym must both surface as candidates.

    Never a coin flip: CHAOS-3289 history is "ambiguity -> candidates, never
    a pick", so an acronym shared by more than one authorized project is
    exactly as unresolved as two projects sharing a literal name.
    """

    analytics_core_runtime = AuthorizedEntity(
        kind=EntityKind.PROJECT,
        canonical_id="project-analytics-core-runtime",
        label="Analytics Core Runtime",
        repository_id=None,
    )
    result = await _run(
        _preflight([(ORG_ID, ACR_PROJECT), (ORG_ID, analytics_core_runtime)]),
        request_for(ACR_QUESTION),
    )

    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.committed_resolution is None
    assert result.answer is not None
    labels = {
        candidate.entity_ref.display_label
        for candidate in result.answer.frame.clarification_candidates
    }
    assert labels == {ACR_PROJECT.label, "Analytics Core Runtime"}


@pytest.mark.asyncio
async def test_a_nonsense_name_never_fabricates_an_acronym_match() -> None:
    """CHAOS-3388 acceptance: "Zebra Project" must stay a bare no-match.

    The real catalog (``ACR_PROJECT`` plus a few of its live siblings) holds
    nothing "Zebra" is remotely an acronym or alias of -- this must terminate
    ``not_found`` with zero candidates, never a guessed entity.
    """

    result = await _run(
        _preflight(
            [
                (ORG_ID, ASK_DEV_PROJECT),
                (ORG_ID, ACR_PROJECT),
                (ORG_ID, NIGHTFALL_PROJECT),
            ]
        ),
        request_for("What is the status of the Zebra Project?"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NOT_FOUND
    assert result.diagnostic == "unresolved_no_authorized_match"
    assert result.answer is not None
    assert result.answer.frame.clarification_candidates == ()


@pytest.mark.asyncio
async def test_a_literal_parenthetical_alias_stays_candidate_only() -> None:
    """CHAOS-3388 codex re-review (HIGH, confirmed): a derived parenthetical
    match is never auto-commit eligible, only ever a candidate.

    A parenthetical segment is not an asserted alternate name the catalog
    itself carries as a distinct alias field -- it is text this module
    *derives* from splitting one label string, exactly as derived a signal as
    an acronym. "Payments (Legacy)" proves the point: "Legacy" reads as a
    qualifier on the primary label, not a real alternate name, and a unique
    substring hit on it must not silently commit a wrong-guess project (see
    the sibling ``test_a_qualifier_like_parenthetical_never_commits`` for the
    negative-case fixtures). "Context Fabric" is a real alternate name in the
    ordinary-language sense, but nothing about the catalog schema
    distinguishes it from "Legacy" -- there is no explicit alias field, only
    a parenthesis -- so both are held to the same candidate-only rule.
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, ACR_PROJECT)]),
        request_for('What is the status of the "Context Fabric" project?'),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.committed_resolution is None
    assert result.answer is not None
    candidates = result.answer.frame.clarification_candidates
    assert len(candidates) == 1
    assert candidates[0].entity_ref.display_label == ACR_PROJECT.label
    assert candidates[0].entity_ref.entity_id == ACR_PROJECT.canonical_id


@pytest.mark.asyncio
async def test_a_qualifier_like_parenthetical_never_commits() -> None:
    """CHAOS-3388 codex re-review (HIGH, confirmed) acceptance repro.

    The live defect: a catalog label like "Payments (Legacy)" carries a
    parenthetical that reads as a qualifier ("this is the legacy one"), not
    an asserted alternate name -- yet the prior auto-commit check treated
    every parenthetical identically, so "What is the status of the Legacy
    project?" proceeded with ``payments-legacy`` COMMITTED, answering about
    an entity the user never actually named. There is no catalog alias field
    distinguishing a real alternate name from a qualifier, so parentheticals
    are held to the same rule as an acronym: candidate-only, never a pick.
    """

    payments_legacy = AuthorizedEntity(
        kind=EntityKind.PROJECT,
        canonical_id="project-payments-legacy",
        label="Payments (Legacy)",
        repository_id=None,
    )
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, payments_legacy)]),
        request_for("What is the status of the Legacy project?"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.committed_resolution is None
    assert result.answer is not None
    candidates = result.answer.frame.clarification_candidates
    assert len(candidates) == 1
    assert candidates[0].entity_ref.display_label == payments_legacy.label
    assert candidates[0].entity_ref.entity_id == payments_legacy.canonical_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("label", "question"),
    [
        ("Reports (Archived)", "What is the status of the Archived project?"),
        ("Roadmap (Team A)", 'What is the status of the "Team A" project?'),
    ],
)
async def test_qualifier_like_parentheticals_never_commit(
    label: str, question: str
) -> None:
    """More qualifier-shaped parentheticals from the reviewer's negative list.

    "(Archived)" and "(Team A)" read exactly like "(Legacy)" -- a status or
    ownership qualifier appended to the primary label, not a real alternate
    name -- and must stay candidate-only for the same reason.
    """

    slug = re.sub(r"[^a-z0-9]+", "-", label.casefold()).strip("-")
    entity = AuthorizedEntity(
        kind=EntityKind.PROJECT,
        canonical_id=f"project-{slug}",
        label=label,
        repository_id=None,
    )
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, entity)]),
        request_for(question),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.committed_resolution is None
    assert result.answer is not None
    candidates = result.answer.frame.clarification_candidates
    assert len(candidates) == 1
    assert candidates[0].entity_ref.display_label == entity.label
    assert candidates[0].entity_ref.entity_id == entity.canonical_id


# ---------------------------------------------------------------------------
# Mutation targets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_candidate_bound_holds_against_more_matches_than_the_limit() -> None:
    """Plant more matches than the limit; the bound is what must survive.

    Kills a mutation of the fallback's own ``limit=`` clause: with the bound
    widened to ``MAX_CANDIDATES`` this run returns 23 candidates, not 5.
    """

    seeded = go_workers_issues(NOT_FOUND_FALLBACK_LIMIT * 3)
    assert len(seeded) > NOT_FOUND_FALLBACK_LIMIT, "the fixture must exceed the bound"
    catalog = LimitRecordingCatalog([(ORG_ID, issue) for issue in seeded])
    result = await _run(
        _preflight(None, catalog=catalog),
        request_for(GO_WORKERS_QUESTION),
    )

    assert result.answer is not None
    assert len(result.answer.frame.clarification_candidates) == NOT_FOUND_FALLBACK_LIMIT

    # And the bound was applied *at the query*, not after the fact.
    wide_searches = [
        limit for kind_count, limit in catalog.search_limits if kind_count > 1
    ]
    assert wide_searches == [NOT_FOUND_FALLBACK_LIMIT], (
        "the fallback must ask the catalog for exactly the bound, once"
    )


@pytest.mark.asyncio
async def test_one_question_costs_exactly_one_fallback_search() -> None:
    """Codex review (medium): the amplification bound.

    Only the lowest-ordinal unresolved mention can terminate the run, so only
    that mention's fallback result is ever read. Searching every unresolved
    mention meant a 25-mention question issued 25 serialized wide catalog
    searches -- each one a watermark query plus six per-kind ClickHouse
    queries -- and discarded 24 of the answers unread.
    """

    named = " and ".join(f'project "Ghost {index:02d}"' for index in range(8))
    catalog = LimitRecordingCatalog([(ORG_ID, issue) for issue in go_workers_issues(3)])
    result = await _run(
        _preflight(None, catalog=catalog),
        request_for(f"Compare {named}"),
    )

    assert len(result.interpretation.mentions) == 8, "the fixture must be multi-mention"
    assert result.decision is PreflightDecision.TERMINATE
    wide_searches = [
        limit for kind_count, limit in catalog.search_limits if kind_count > 1
    ]
    assert len(wide_searches) <= 1, (
        "at most one bounded fallback search may be issued per question, "
        f"got {len(wide_searches)}"
    )


@pytest.mark.asyncio
async def test_a_same_kind_close_match_is_not_described_as_another_kind() -> None:
    """Codex review (low): the context-ref path returns same-kind candidates.

    A stale context ref resolves with ``exact=True``, so the mention's own
    kind never ran a fuzzy search -- and the fallback, which searches every
    searchable kind, can then legitimately return a candidate of exactly the
    kind the user named. A reason string asserting "under a different kind"
    would be a false statement persisted onto canonical v2 state.
    """

    beacon = AuthorizedEntity(
        kind=EntityKind.PROJECT,
        canonical_id="project-beacon-migration",
        label="Beacon Migration",
        repository_id=None,
    )
    result = await _run(
        _preflight([(ORG_ID, beacon)]),
        request_for(
            'What is the status of the "beacon" project?',
            scope_overrides={
                "direct_scope": "project",
                "entity_refs": [
                    {
                        "entity_type": "project",
                        "entity_id": "beacon",
                        "display_label": "Beacon",
                        "repository_id": None,
                    }
                ],
            },
        ),
    )

    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.answer is not None
    (candidate,) = result.answer.frame.clarification_candidates
    assert candidate.entity_ref.entity_kind.value == "project", (
        "this fixture must exercise the same-kind path, or it proves nothing"
    )
    assert "kind" not in candidate.reason.casefold()


@pytest.mark.asyncio
async def test_a_genuinely_ambiguous_mention_is_never_re_searched() -> None:
    """The fallback is gated on ``no_authorized_match`` specifically.

    An ambiguous mention already holds real candidates; re-searching it would
    replace the entities that actually carry the typed name with substring
    near-misses, and spend a catalog round trip to do it.
    """

    catalog = LimitRecordingCatalog(
        [(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)]
    )
    result = await _run(
        _preflight(None, catalog=catalog),
        request_for("What is the status of the Atlas project?"),
    )

    assert result.diagnostic == "unresolved_ambiguous_candidates"
    # Both Atlas projects match on exact label, so ``resolve_mention`` never
    # reaches its own fuzzy search either -- this run must issue no catalog
    # ``search`` at all.
    assert catalog.search_limits == []


@pytest.mark.asyncio
async def test_the_tenant_assertion_would_catch_a_leaking_catalog() -> None:
    """Negative control for the test below.

    The ``org_id`` filter lives in ``scope_catalog``'s SQL, which the seeded
    harness stands in for -- so "it stayed not_found" is only evidence if a
    catalog that *did* leak would have been seen. Plant exactly that defect
    and observe the difference: same fixture, same question, cross-tenant
    matches now reach the frame.
    """

    entities = [(ORG_ID, ASK_DEV_PROJECT)] + [
        (OTHER_ORG_ID, issue) for issue in go_workers_issues(23)
    ]
    leaked = await _run(
        _preflight(None, catalog=CrossTenantLeakingCatalog(entities)),
        request_for(GO_WORKERS_QUESTION),
    )

    assert leaked.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert leaked.answer is not None
    assert len(leaked.answer.frame.clarification_candidates) == NOT_FOUND_FALLBACK_LIMIT


@pytest.mark.asyncio
async def test_close_matches_never_cross_the_tenant_boundary() -> None:
    """The same ``Go workers`` work items, owned by another organization.

    Cross-tenant is modelled as a *populated catalog owned by someone else*, so
    a fallback that dropped ``org_id`` would surface them. It must stay
    ``not_found``, exactly as today.
    """

    result = await _run(
        _preflight(
            [(ORG_ID, ASK_DEV_PROJECT)]
            + [(OTHER_ORG_ID, issue) for issue in go_workers_issues(23)]
        ),
        request_for(GO_WORKERS_QUESTION),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NOT_FOUND
    assert result.diagnostic == "unresolved_no_authorized_match"
    assert result.terminating_resolution_entry is None
    assert _latest_outcome(result) is ResolutionOutcome.NO_AUTHORIZED_MATCH


@pytest.mark.asyncio
async def test_zero_close_matches_leaves_not_found_exactly_as_today() -> None:
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT)]),
        request_for('What is the status of the "Nightfall" project?'),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NOT_FOUND
    assert result.diagnostic == "unresolved_no_authorized_match"
    assert result.answer is not None
    assert result.answer.frame.clarification_candidates == ()
    assert _latest_outcome(result) is ResolutionOutcome.NO_AUTHORIZED_MATCH


@pytest.mark.asyncio
async def test_a_sole_exactly_named_close_match_is_still_never_committed() -> None:
    """The invariant at ``scope_service`` :706-711, one layer up.

    The single work item's label equals the typed name outright. It is still
    not what the user asked for -- they named a *project* -- so it is offered
    back, never auto-committed. A fuzzy match must never become a subject.
    """

    exact_label_issue = AuthorizedEntity(
        kind=EntityKind.ISSUE,
        canonical_id="linear:CHAOS-3040",
        label="Go workers",
        repository_id=None,
    )
    result = await _run(
        _preflight([(ORG_ID, exact_label_issue)]),
        request_for(GO_WORKERS_QUESTION),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.committed_resolution is None
    assert result.committed_subjects is None
    assert result.has_committed_subject is False
    assert _latest_outcome(result) is not ResolutionOutcome.EXACT_MATCH
    assert result.answer is not None
    assert len(result.answer.frame.clarification_candidates) == 1


# ---------------------------------------------------------------------------
# Structural guards
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_fallback_appends_and_never_erases_the_not_found_entry() -> None:
    result = await _run(
        _preflight([(ORG_ID, issue) for issue in go_workers_issues(3)]),
        request_for(GO_WORKERS_QUESTION),
    )

    assert result.ledger is not None
    outcomes = [entry.outcome for entry in result.ledger.entries]
    assert outcomes == [
        ResolutionOutcome.NO_AUTHORIZED_MATCH,
        ResolutionOutcome.AMBIGUOUS_CANDIDATES,
    ], "the original not-found entry must survive as history, not be rewritten"


@pytest.mark.asyncio
async def test_the_persisted_entry_matches_the_pre_existing_ambiguity_shape() -> None:
    """Codex confirmation review (medium), scoped: no NEW persistence gap.

    The orchestrator persists exactly one resolution row -- the terminating
    entry the frame's candidates are authorized against (CHAOS-3325) -- so a
    durable ledger has never been a contiguous prefix. This pins that the
    fallback lands in the same shape the pre-existing genuine-ambiguity path
    already lands in, and that the two stay distinguishable durably: the
    diagnostic on ``dev_runs.preflight_outcome`` is what says which happened.

    Widening persistence to store the whole prefix would change what
    ``_authorize_clarification_candidates`` compares against and belongs with
    CHAOS-3325's contract, not here.
    """

    ambiguous = await _run(
        _preflight(
            [
                (ORG_ID, ASK_DEV_PROJECT),
                (ORG_ID, ATLAS_PROJECT_ONE),
                (ORG_ID, ATLAS_PROJECT_TWO),
            ]
        ),
        request_for("Compare project Ask Dev and project Atlas"),
    )
    fallback = await _run(
        _preflight([(ORG_ID, issue) for issue in go_workers_issues(3)]),
        request_for(GO_WORKERS_QUESTION),
    )

    for result in (ambiguous, fallback):
        assert result.ledger is not None
        assert [entry.entry_ordinal for entry in result.ledger.entries] == [0, 1]
        assert result.terminating_resolution_entry is not None
        assert result.terminating_resolution_entry.entry_ordinal == 1

    assert ambiguous.diagnostic == "unresolved_ambiguous_candidates"
    assert fallback.diagnostic == "unresolved_close_matches"


@pytest.mark.asyncio
async def test_a_genuine_ambiguity_keeps_its_own_clarification_copy() -> None:
    """The close-matches copy must not swallow real same-name ambiguity."""

    result = await _run(
        _preflight([(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)]),
        request_for("What is the status of the Atlas project?"),
    )

    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.diagnostic == "unresolved_ambiguous_candidates"
    assert result.answer is not None
    assert result.answer.frame.direct_answer == CLARIFICATION_COPY["ambiguous"]


@pytest.mark.asyncio
async def test_an_unresolved_bare_name_is_never_re_searched_by_the_fallback() -> None:
    """The fallback is for *typed* mentions only, and this is what that buys.

    A bare name is already resolved across every searchable kind, so the
    fallback's search would be byte-identical to one that just returned
    nothing. Asserting the run still proceeds organization-wide does **not**
    prove the guard: an untyped mention is excluded from the terminate loop
    and ``AMBIGUOUS_CANDIDATES`` is an unresolved outcome like any other, so
    the *decision* is unchanged either way. What the guard actually prevents
    is the redundant round trip -- and a candidate-bearing ledger entry for a
    mention no clarification will ever act on -- so that is what is asserted.
    """

    catalog = LimitRecordingCatalog([(ORG_ID, issue) for issue in go_workers_issues(3)])
    result = await _run(
        _preflight(None, catalog=catalog),
        request_for("How is Nightfall doing?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.legacy_guard_required is True
    assert result.diagnostic == "proceeded_unresolved_bare_name"
    assert [limit for _kinds, limit in catalog.search_limits] == [MAX_CANDIDATES], (
        "the bare name's own all-kinds search is the only one this run may make"
    )
    assert result.ledger is not None
    assert [entry.outcome for entry in result.ledger.entries] == [
        ResolutionOutcome.NO_AUTHORIZED_MATCH
    ]


@pytest.mark.asyncio
async def test_a_failed_fallback_search_degrades_to_not_found() -> None:
    result = await _run(
        _preflight(
            [(ORG_ID, issue) for issue in go_workers_issues(3)],
            catalog_cls=MultiKindFailingCatalog,
        ),
        request_for(GO_WORKERS_QUESTION),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NOT_FOUND
    assert result.diagnostic == "unresolved_no_authorized_match"


def test_every_preflight_diagnostic_fits_the_persisted_column() -> None:
    """``dev_runs.preflight_outcome`` is a ``String(32)`` with no CHECK.

    The closed tuple's own docstring claims this test exists; it did not. A
    diagnostic over the bound is a real insert-time failure in production that
    every unit test which never persists a run would pass.
    """

    assert len(set(PREFLIGHT_DIAGNOSTICS)) == len(PREFLIGHT_DIAGNOSTICS)
    oversized = [name for name in PREFLIGHT_DIAGNOSTICS if len(name) > 32]
    assert oversized == []
