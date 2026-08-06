"""CHAOS-3422: a typed mention's kind must rank its own kind first.

The live repro (run ``acdc0a67-8312-43e0-8b59-0aafc0f10a68``, main @
``c255e4932``): *"What's the status of the ACR project…"* — CHAOS-3388's
acronym matcher correctly finds the one real project, and the clarification
then lists it **5th of 5**, behind four irrelevant issue substring hits.

Two kind-blind ranking seams produced that, and each has its own test here:

* ``ClickHouseAuthorizedEntityCatalog.search`` merges alias hits ahead of
  substring hits and truncates to ``limit`` — precision-ranked, kind-blind.
* ``ScopeResolutionService.search`` then re-sorts the whole page into one
  alphabetical order by label, which discards even that alias-first rank.
  ``[CHAOS-2911] Harden ACR runtime client boundary`` casefolds to a ``[``
  that sorts ahead of ``dev health agent context runtime…``.

The fix ranks, it never filters. Filtering to the mention's kind would gut
CHAOS-3366's own premise — its live case is "the Go workers project" where
the catalog holds only *issues*, so a kind filter returns zero candidates and
regresses that ticket back to a bare not-found. Both directions are pinned
below.
"""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2 import PublicOutcome, ResolutionOutcome
from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
from dev_health_ops.api.dev.scope_catalog import merge_search_candidates
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ScopeRequestCache,
    ScopeResolutionService,
    ScopeSearchRequest,
)
from dev_health_ops.api.dev.subject_preflight import (
    NOT_FOUND_FALLBACK_LIMIT,
    PreflightDecision,
    SubjectPreflight,
)
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    ORG_ID,
    SeededCatalog,
    fixed_now,
    request_for,
    sequential_ids,
    versions,
)

#: The production catalog row the live repro named, verbatim from the
#: CHAOS-3388 fixture that was read out of ClickHouse ``projects`` — not a
#: hand-authored stand-in.
ACR_PROJECT = AuthorizedEntity(
    kind=EntityKind.PROJECT,
    canonical_id="60997592-f9f4-462b-87c3-ef82671df270",
    label="Dev Health Agent Context Runtime (Context Fabric)",
    repository_id=None,
)

#: The literal repro question from the live incident report.
ACR_QUESTION = "What's the status of the ACR Project and what is blocking it?"

#: The four incidental substring hits the live clarification actually listed
#: ahead of the project. Every label casefolds to a leading ``[``, which sorts
#: ahead of any letter — this is the exact shape that buried the project.
ACR_SUBSTRING_ISSUES = tuple(
    AuthorizedEntity(
        kind=EntityKind.ISSUE,
        canonical_id=f"linear:CHAOS-{number}",
        label=label,
        repository_id=None,
    )
    for number, label in (
        (2911, "[CHAOS-2911] Harden ACR runtime client boundary"),
        (2912, "[CHAOS-2912] ACR runtime deploy stack"),
        (2913, "[CHAOS-2913] ACR context budget guard"),
        (2914, "[CHAOS-2914] ACR evidence adapter"),
    )
)


def go_workers_issues(count: int) -> list[AuthorizedEntity]:
    """CHAOS-3366's live case: work items titled ``Go workers…``, no project."""

    return [
        AuthorizedEntity(
            kind=EntityKind.ISSUE,
            canonical_id=f"linear:CHAOS-{3000 + index}",
            label=f"Go workers: stage {index:02d}",
            repository_id=None,
        )
        for index in range(count)
    ]


def _preflight(entities: Any) -> SubjectPreflight:
    mint = sequential_ids()
    return SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(
            SeededCatalog(entities), cache=ScopeRequestCache()
        ),
        versions=versions(),
        mint_id=mint,
        now=fixed_now,
    )


async def _run(preflight: SubjectPreflight, request: Any) -> Any:
    return await preflight.run(
        request=request,
        org_id=ORG_ID,
        permission_fingerprint="permissions_01",
        authorized_scope=request.scope,
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
    )


def _candidates(result: Any) -> tuple[Any, ...]:
    assert result.answer is not None
    return result.answer.frame.clarification_candidates


# ---------------------------------------------------------------------------
# RED -- the ticket's own repro
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_typed_project_mention_ranks_the_real_project_first() -> None:
    """The acceptance case: the only real project must lead, not trail.

    Asserts the *position*, not membership. CHAOS-3388 already pins that the
    acronym match survives the bound; surviving it in last place is what this
    ticket reports, so an ``in candidates`` assertion here could not fail.
    """

    result = await _run(
        _preflight(
            [(ORG_ID, ACR_PROJECT)]
            + [(ORG_ID, issue) for issue in ACR_SUBSTRING_ISSUES]
        ),
        request_for(ACR_QUESTION),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.diagnostic == "unresolved_close_matches"
    candidates = _candidates(result)
    assert [candidate.entity_ref.display_label for candidate in candidates] == [
        ACR_PROJECT.label,
        *(issue.label for issue in ACR_SUBSTRING_ISSUES),
    ]


@pytest.mark.asyncio
async def test_the_same_kind_candidate_is_ranked_before_the_bound_truncates() -> None:
    """Ranking after truncation is cosmetic: the project must not be dropped.

    Six alias hits for one five-slot page. Every team's acronym is ``acr`` and
    every team label sorts ahead of the project's, so a kind-blind page keeps
    five teams and loses the one project outright — the user asked for a
    project and is offered five things that are not one. Reordering whatever
    happens to survive cannot fix this; the preference has to be applied
    before the catalog's own ``[:limit]``.
    """

    teams = [
        AuthorizedEntity(
            kind=EntityKind.TEAM,
            canonical_id=f"team-acr-{index}",
            label=f"A{index} Core Runtime",
            repository_id=None,
        )
        for index in range(NOT_FOUND_FALLBACK_LIMIT)
    ]
    result = await _run(
        _preflight([(ORG_ID, ACR_PROJECT)] + [(ORG_ID, team) for team in teams]),
        request_for(ACR_QUESTION),
    )

    candidates = _candidates(result)
    assert len(candidates) == NOT_FOUND_FALLBACK_LIMIT, "the cap is unchanged"
    assert candidates[0].entity_ref.display_label == ACR_PROJECT.label
    assert candidates[0].entity_ref.entity_id == ACR_PROJECT.canonical_id


# ---------------------------------------------------------------------------
# The preference ranks; it must never filter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_typed_mention_still_gets_other_kind_close_matches() -> None:
    """CHAOS-3366's own live case, unchanged: typed project, only issues exist.

    A kind *filter* would return nothing here and regress this question to a
    bare ``not_found``. The whole offer, its count and its kinds, must be
    exactly what CHAOS-3366 landed.
    """

    result = await _run(
        _preflight(
            [(ORG_ID, ASK_DEV_PROJECT)]
            + [(ORG_ID, issue) for issue in go_workers_issues(23)]
        ),
        request_for('What is the status of the "Go workers" project?'),
    )

    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.diagnostic == "unresolved_close_matches"
    candidates = _candidates(result)
    assert len(candidates) == NOT_FOUND_FALLBACK_LIMIT
    assert {candidate.entity_ref.entity_kind.value for candidate in candidates} == {
        "issue"
    }
    assert [candidate.entity_ref.display_label for candidate in candidates] == [
        issue.label for issue in go_workers_issues(NOT_FOUND_FALLBACK_LIMIT)
    ]


@pytest.mark.asyncio
async def test_a_nonsense_typed_name_still_offers_nothing() -> None:
    """Preferring a kind must not invent a candidate of that kind."""

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, ACR_PROJECT)]),
        request_for("What is the status of the Zebra Project?"),
    )

    assert result.outcome is PublicOutcome.NOT_FOUND
    assert result.diagnostic == "unresolved_no_authorized_match"
    assert result.answer is not None
    assert not result.answer.frame.clarification_candidates


# ---------------------------------------------------------------------------
# Untyped mentions keep today's behaviour exactly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_untyped_bare_name_never_reaches_the_close_match_fallback() -> None:
    """An untyped bare name carries ``PROJECT`` as a *declared default*.

    ``question_interpreter._add_untyped_mentions`` stamps that kind on a span
    the user never typed a kind for, so preferring it would be inventing a
    user intent. The structural reason it cannot happen is that
    ``blocking_ids`` excludes every untyped mention, so the terminating
    mention — the only one the fallback ever searches for — is always typed.
    This pins the untyped run end to end, so a change that let an untyped
    mention terminate would surface here rather than silently acquiring a
    project preference.
    """

    result = await _run(
        _preflight(
            [(ORG_ID, ACR_PROJECT)]
            + [(ORG_ID, issue) for issue in ACR_SUBSTRING_ISSUES]
        ),
        request_for("How is ACR doing?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.answer is None


@pytest.mark.asyncio
async def test_an_untyped_span_beside_a_typed_one_ranks_only_the_typed_kind() -> None:
    """A typed mention terminates; the untyped span's default never ranks.

    "the ACR Project" types ``project``; "Nightfall" is a bare name typed only
    by the declared default. The offer must be ranked for the *typed*
    mention's kind — the same ordering as the single-mention repro above.
    """

    result = await _run(
        _preflight(
            [(ORG_ID, ACR_PROJECT)]
            + [(ORG_ID, issue) for issue in ACR_SUBSTRING_ISSUES]
        ),
        request_for("What is the status of the ACR Project, and how is Nightfall?"),
    )

    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.diagnostic == "unresolved_close_matches"
    candidates = _candidates(result)
    assert candidates[0].entity_ref.display_label == ACR_PROJECT.label


# ---------------------------------------------------------------------------
# The search seam itself
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_without_a_preference_is_byte_for_byte_todays_ordering() -> None:
    """Every existing caller passes no preference and must be untouched."""

    entities = [(ORG_ID, ACR_PROJECT)] + [
        (ORG_ID, issue) for issue in ACR_SUBSTRING_ISSUES
    ]
    service = ScopeResolutionService(SeededCatalog(entities), cache=ScopeRequestCache())

    result = await service.search(
        ORG_ID,
        "permissions_01",
        ScopeSearchRequest(
            query="acr",
            kinds=(EntityKind.ISSUE, EntityKind.PROJECT),
            limit=NOT_FOUND_FALLBACK_LIMIT,
            include_alias_matches=True,
        ),
    )

    assert [candidate.label for candidate in result.candidates] == [
        *(issue.label for issue in ACR_SUBSTRING_ISSUES),
        ACR_PROJECT.label,
    ]


@pytest.mark.asyncio
async def test_a_preference_and_no_preference_do_not_share_a_cache_entry() -> None:
    """Same query, same kinds, same limit — different answers.

    A shared cache key would let whichever request ran first silently answer
    the other's call, which is the defect CHAOS-3388 already documented for
    ``include_alias_matches``.
    """

    entities = [(ORG_ID, ACR_PROJECT)] + [
        (ORG_ID, issue) for issue in ACR_SUBSTRING_ISSUES
    ]
    service = ScopeResolutionService(SeededCatalog(entities), cache=ScopeRequestCache())
    kwargs: dict[str, Any] = {
        "query": "acr",
        "kinds": (EntityKind.ISSUE, EntityKind.PROJECT),
        "limit": NOT_FOUND_FALLBACK_LIMIT,
        "include_alias_matches": True,
    }

    unpreferred = await service.search(
        ORG_ID, "permissions_01", ScopeSearchRequest(**kwargs)
    )
    preferred = await service.search(
        ORG_ID,
        "permissions_01",
        ScopeSearchRequest(preferred_kinds=frozenset({EntityKind.PROJECT}), **kwargs),
    )

    assert unpreferred.candidates[0].label != preferred.candidates[0].label
    assert preferred.candidates[0].label == ACR_PROJECT.label


def test_a_preference_outside_the_searched_kinds_is_a_caller_defect() -> None:
    """The preference is a ranking over the searched page, not a widening."""

    with pytest.raises(ValueError, match="preferred"):
        ScopeSearchRequest(
            query="acr",
            kinds=(EntityKind.ISSUE,),
            preferred_kinds=frozenset({EntityKind.PROJECT}),
        )


def test_inside_the_preferred_kind_alias_precision_still_decides() -> None:
    """CHAOS-3388's second key survives underneath CHAOS-3422's first one.

    ``Aardvark ACR Rites`` sorts ahead of the real project by label and holds
    ``ACR`` only as an incidental substring; the real project matches by
    acronym. Both are projects, so the kind key cannot separate them and the
    precision key must. Asserted at ``merge_search_candidates`` because
    that is the only layer holding the alias/substring distinction: an
    ``AuthorizedEntity`` carries no match provenance downstream of it.
    """

    aardvark = AuthorizedEntity(
        kind=EntityKind.PROJECT,
        canonical_id="project-aardvark-rites",
        label="Aardvark ACR Rites",
        repository_id=None,
    )
    ranked = merge_search_candidates(
        alias_hits=[ACR_PROJECT],
        substring_hits=[aardvark, *ACR_SUBSTRING_ISSUES],
        preferred_kinds=frozenset({EntityKind.PROJECT}),
        limit=NOT_FOUND_FALLBACK_LIMIT,
    )

    assert [entity.label for entity in ranked] == [
        ACR_PROJECT.label,
        aardvark.label,
        *(issue.label for issue in ACR_SUBSTRING_ISSUES[:3]),
    ]


def test_merging_without_a_preference_keeps_the_chaos_3388_order() -> None:
    """No preference must be byte-for-byte alias-then-substring, by label."""

    aardvark = AuthorizedEntity(
        kind=EntityKind.PROJECT,
        canonical_id="project-aardvark-rites",
        label="Aardvark ACR Rites",
        repository_id=None,
    )
    ranked = merge_search_candidates(
        alias_hits=[ACR_PROJECT],
        substring_hits=[aardvark, *ACR_SUBSTRING_ISSUES],
        preferred_kinds=frozenset(),
        limit=NOT_FOUND_FALLBACK_LIMIT,
    )

    assert [entity.label for entity in ranked] == [
        ACR_PROJECT.label,
        *(issue.label for issue in ACR_SUBSTRING_ISSUES),
    ]


@pytest.mark.asyncio
async def test_the_close_match_entry_still_never_commits() -> None:
    """Ranking must not turn a sole leading candidate into a pick."""

    result = await _run(
        _preflight([(ORG_ID, ACR_PROJECT)]),
        request_for(ACR_QUESTION),
    )

    assert result.committed_resolution is None
    latest = result.ledger.latest_by_mention()
    outcomes = {entry.outcome for entry in latest.values()}
    assert ResolutionOutcome.EXACT_MATCH not in outcomes
    assert ResolutionOutcome.AMBIGUOUS_CANDIDATES in outcomes
