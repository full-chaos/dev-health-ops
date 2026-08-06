"""CHAOS-3422: a typed mention's kind must rank its own kind first.

The live repro (run ``acdc0a67-8312-43e0-8b59-0aafc0f10a68``, main @
``c255e4932``): *"What's the status of the ACR project…"* — CHAOS-3388's
acronym matcher correctly finds the one real project, and the clarification
then lists it **5th of 5**, behind four irrelevant issue substring hits.

Two kind-blind seams produced that, and each has its own test here:

* ``ClickHouseAuthorizedEntityCatalog.search`` merged alias hits ahead of
  substring hits and truncated to ``limit`` — precision-ranked, kind-blind.
* ``ScopeResolutionService.search`` then re-sorted the whole page into one
  alphabetical order by label, discarding even that alias-first rank.
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
from dev_health_ops.api.dev.scope_catalog import (
    ClickHouseAuthorizedEntityCatalog,
    merge_search_candidates,
)
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
async def test_search_without_a_preference_keeps_the_catalogs_own_rank() -> None:
    """No preference must leave the catalog's ranking exactly as it arrived.

    Codex review round 3, confirmed: the service used to re-sort every page by
    label, which discarded CHAOS-3388's alias precedence as well as this
    ticket's kind key — the acronym-matched project arrived first from the
    catalog and left the service last. Preserving the order is only visible
    for the one caller that enables alias matching; with substring hits alone
    the catalog's order already *is* label order.
    """

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
        ACR_PROJECT.label,
        *(issue.label for issue in ACR_SUBSTRING_ISSUES),
    ]


@pytest.mark.asyncio
async def test_a_preference_and_no_preference_do_not_share_a_cache_entry() -> None:
    """Same query, same kinds, same limit — different answers.

    A shared cache key would let whichever request ran first silently answer
    the other's call, which is the defect CHAOS-3388 already documented for
    ``include_alias_matches``. The seeded rows must be a case where the two
    genuinely differ, or the test would pass with the cache keys collided:
    ``Aardvark ACR Notes`` is a project matching only by substring, so it
    trails the acronym hit either way, while the *issues* lead the page
    without a preference and trail both projects with one.
    """

    aardvark = AuthorizedEntity(
        kind=EntityKind.PROJECT,
        canonical_id="project-aardvark",
        label="Aardvark ACR Notes",
    )
    entities = [(ORG_ID, ACR_PROJECT), (ORG_ID, aardvark)] + [
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

    assert [candidate.label for candidate in unpreferred.candidates] == [
        ACR_PROJECT.label,
        *(issue.label for issue in ACR_SUBSTRING_ISSUES),
    ]
    assert [candidate.label for candidate in preferred.candidates] == [
        ACR_PROJECT.label,
        aardvark.label,
        *(issue.label for issue in ACR_SUBSTRING_ISSUES[:3]),
    ]


#: Rows the oracle below drives through both search implementations. Chosen so
#: every tier boundary is crossed: alias-only, alias *and* substring, and
#: substring-only, with the substring-only label sorting first so a lost alias
#: rank is visible as a reordering rather than hidden by the alphabet.
_ORACLE_ROWS = (
    AuthorizedEntity(
        kind=EntityKind.PROJECT, canonical_id="project-zeta", label="Zeta Runtime (ACR)"
    ),
    AuthorizedEntity(
        kind=EntityKind.PROJECT,
        canonical_id="project-aardvark",
        label="Aardvark ACR Notes",
    ),
    AuthorizedEntity(
        kind=EntityKind.PROJECT, canonical_id="project-agile", label="Agile Core Runway"
    ),
    AuthorizedEntity(
        kind=EntityKind.TEAM, canonical_id="team-acr", label="Analytics Core Reporting"
    ),
    AuthorizedEntity(
        kind=EntityKind.TEAM, canonical_id="team-beta", label="Beta ACR Guild"
    ),
)


def _oracle_query_dicts(rows: tuple[AuthorizedEntity, ...]) -> Any:
    """``query_dicts`` over ``rows``, mirroring each production query's WHERE.

    The roster scan and the substring pass are told apart by their *parameters*
    -- only the substring pass binds ``query`` -- not by matching SQL text.
    Codex review round 3, confirmed: the first version keyed on the substring
    ``lowerUTF8(name)``, which the project substring SQL also contains
    (``scope_catalog`` line 402), so both production queries were handed the
    unfiltered roster and the two sides were never driven from equal inputs.
    It passed anyway, by luck, because the extra row was an alias hit too.
    """

    kind_by_table = {
        "FROM projects FINAL": EntityKind.PROJECT,
        "FROM teams FINAL": EntityKind.TEAM,
    }

    async def query_dicts(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        kind = next(
            (kind for table, kind in kind_by_table.items() if table in sql), None
        )
        if kind is None:
            return []
        matching = [row for row in rows if row.kind is kind]
        if "query" in params:  # the substring pass
            needle = params["query"].casefold()
            matching = [
                row
                for row in matching
                if needle == row.canonical_id.casefold()
                or needle in row.label.casefold()
            ]
        return [
            {"canonical_id": row.canonical_id, "label": row.label} for row in matching
        ]

    return query_dicts


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query", ["acr", "runtime", "project-zeta", "nothing-matches-this"]
)
@pytest.mark.parametrize(
    "preferred",
    [frozenset(), frozenset({EntityKind.PROJECT}), frozenset({EntityKind.TEAM})],
)
async def test_the_seeded_catalog_agrees_with_the_production_catalog(
    monkeypatch: pytest.MonkeyPatch, query: str, preferred: frozenset[EntityKind]
) -> None:
    """A differential oracle over the two search implementations.

    Every preflight scenario in this module reaches ``merge_search_candidates``
    through ``SeededCatalog``, so the fake is a second implementation of the
    production catalog's search and the only question that matters is whether
    they agree. Codex review, medium: they did not — the fake skipped the
    alias pass for any entity the substring pass had already matched, so
    ``Zeta Runtime (ACR)`` was an alias hit in production and a substring hit
    in the fake, one rank lower.

    Both sides are driven from the *same* rows, through the same WHERE
    semantics, so the comparison cannot be satisfied by two different inputs.
    Repository search is deliberately outside this oracle: production enriches
    repository labels through ``resolve_scope_display_names``, which the fake
    does not model at all — a pre-existing divergence this ticket does not
    close, and pretending otherwise here would be the false confidence the
    oracle exists to remove.
    """

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts",
        _oracle_query_dicts(_ORACLE_ROWS),
    )
    kinds = (EntityKind.PROJECT, EntityKind.TEAM)

    production = await ClickHouseAuthorizedEntityCatalog(object()).search(
        ORG_ID,
        query,
        kinds,
        limit=NOT_FOUND_FALLBACK_LIMIT,
        include_alias_matches=True,
        preferred_kinds=preferred,
    )
    seeded = await SeededCatalog([(ORG_ID, row) for row in _ORACLE_ROWS]).search(
        ORG_ID,
        query,
        kinds,
        limit=NOT_FOUND_FALLBACK_LIMIT,
        include_alias_matches=True,
        preferred_kinds=preferred,
    )

    assert seeded == production, f"the fake diverges for {query!r}/{preferred}"


@pytest.mark.asyncio
async def test_the_oracle_rows_rank_the_way_the_contract_says(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Agreeing on a *wrong* order must not satisfy the oracle above."""

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts",
        _oracle_query_dicts(_ORACLE_ROWS),
    )

    result = await ClickHouseAuthorizedEntityCatalog(object()).search(
        ORG_ID,
        "acr",
        (EntityKind.PROJECT, EntityKind.TEAM),
        limit=NOT_FOUND_FALLBACK_LIMIT,
        include_alias_matches=True,
        preferred_kinds=frozenset({EntityKind.PROJECT}),
    )

    assert [entity.canonical_id for entity in result] == [
        # Preferred kind first: alias hits by label, then the substring-only one.
        "project-agile",  # "Agile Core Runway" -> acronym "acr"
        "project-zeta",  # alias AND substring -- still ranked as an alias hit
        "project-aardvark",  # substring only
        # Then the other kind, same two keys.
        "team-acr",  # "Analytics Core Reporting" -> acronym "acr"
        "team-beta",  # substring only
    ]


@pytest.mark.asyncio
async def test_the_service_ranks_even_when_the_catalog_ignores_the_hint() -> None:
    """``preferred_kinds`` is a hint to the catalog and a guarantee at the service.

    ``AuthorizedEntityCatalog`` is a Protocol, and ``preferred_kinds`` reaches
    it with a default of ``frozenset()`` — so an implementation that simply
    ignores the argument is a valid one, and would silently return a
    kind-blind page. Depending on every implementation to honour a ranking
    hint is the kind of unstated assumption that only shows up in production,
    so the service re-applies the key over whatever it is handed. Without
    this, that re-application has no witness at all: the real catalog already
    ranks, so removing it changes nothing any other test can see.
    """

    class RankIgnoringCatalog(SeededCatalog):
        async def search(
            self,
            org_id: str,
            query: str,
            kinds: tuple[EntityKind, ...],
            *,
            limit: int,
            include_alias_matches: bool = False,
            preferred_kinds: frozenset[EntityKind] = frozenset(),
        ) -> list[AuthorizedEntity]:
            return await super().search(
                org_id,
                query,
                kinds,
                limit=limit,
                include_alias_matches=include_alias_matches,
            )

    # A plain substring match on both sides, so nothing but the kind key can
    # move the project: its label sorts last, and the catalog here ignores the
    # hint that would otherwise have raised it.
    project = AuthorizedEntity(
        kind=EntityKind.PROJECT,
        canonical_id="project-zulu",
        label="Zulu Runtime Program",
    )
    issues = [
        AuthorizedEntity(
            kind=EntityKind.ISSUE,
            canonical_id=f"linear:CHAOS-{2911 + index}",
            label=f"{index} runtime task",
        )
        for index in range(4)
    ]
    service = ScopeResolutionService(
        RankIgnoringCatalog([(ORG_ID, entity) for entity in [project, *issues]]),
        cache=ScopeRequestCache(),
    )

    result = await service.search(
        ORG_ID,
        "permissions_01",
        ScopeSearchRequest(
            query="runtime",
            kinds=(EntityKind.ISSUE, EntityKind.PROJECT),
            limit=NOT_FOUND_FALLBACK_LIMIT,
            preferred_kinds=frozenset({EntityKind.PROJECT}),
        ),
    )

    assert [candidate.canonical_id for candidate in result.candidates] == [
        project.canonical_id,
        *(issue.canonical_id for issue in issues),
    ]


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
