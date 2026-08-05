"""CHAOS-3393: subject preflight for ``status.portfolio.v1`` --

* A homogeneous PROJECT cohort named under the PORTFOLIO_STATUS intent
  PROCEEDs (a real committed ``dev_subject_set.v1``, no single
  ``committed_resolution``), instead of terminating UNSUPPORTED like every
  other cohort still does.
* A TEAM-only cohort under the SAME intent still terminates UNSUPPORTED --
  the gate is intent AND kind, not intent alone.
* An ORGANIZATION_WIDE portfolio question (no named subjects) commits a
  bounded, deterministic project enumeration as its subject set; zero
  authorized projects falls back to the ordinary organization-wide PROCEED;
  more authorized projects than the batch cap discloses truncation via the
  subject set's own warnings, never a silent sample.
"""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.api.dev.contracts_v2 import PublicOutcome
from dev_health_ops.api.dev.portfolio_status_service import MAX_PORTFOLIO_PROJECTS
from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ScopeRequestCache,
    ScopeResolutionService,
)
from dev_health_ops.api.dev.subject_preflight import PreflightDecision, SubjectPreflight
from tests._chaos_3292_preflight import (
    ASK_DEV_PROJECT,
    NIGHTFALL_PROJECT,
    ORG_ID,
    SeededCatalog,
    fixed_now,
    request_for,
    sequential_ids,
    versions,
)

_PLATFORM_TEAM = AuthorizedEntity(EntityKind.TEAM, "team-platform", "Platform")
_ROCKET_TEAM = AuthorizedEntity(EntityKind.TEAM, "team-rocket", "Rocket")


def _preflight(entities: Any, **catalog_kwargs: Any) -> SubjectPreflight:
    mint = sequential_ids()
    return SubjectPreflight(
        interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
        scope_service=ScopeResolutionService(
            SeededCatalog(entities, **catalog_kwargs), cache=ScopeRequestCache()
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
        authorized_scope=kwargs.pop("authorized_scope", request.scope),
        run_id="run_01",
        answer_id="answer_01",
        conversation_id="conversation_01",
        **kwargs,
    )


@pytest.mark.asyncio
async def test_a_named_project_cohort_proceeds_under_portfolio_status() -> None:
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, NIGHTFALL_PROJECT)]),
        request_for("What is the status of project Ask Dev and project Nightfall?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.outcome is None
    assert result.answer is None
    assert result.diagnostic == "committed_cohort_portfolio_v1"
    # A cohort, never a single-scope binding -- rendering it is
    # status.portfolio.v1's job, not a singular committed_resolution.
    assert result.committed_resolution is None
    assert result.subject_set is not None
    assert result.committed_subjects is not None
    assert result.committed_subjects.subject_set is result.subject_set
    assert result.committed_subjects.resolution is None
    committed_ids = {ref.entity_id for ref in result.subject_set.committed_entity_refs}
    assert committed_ids == {
        ASK_DEV_PROJECT.canonical_id,
        NIGHTFALL_PROJECT.canonical_id,
    }
    assert result.subject_set.cohort_complete is True


@pytest.mark.asyncio
async def test_a_team_only_cohort_still_terminates_unsupported() -> None:
    """Same PORTFOLIO_STATUS-recognized wording, but the named cohort is
    teams, not projects -- status.portfolio.v1 only ever supports PROJECT
    subjects, so this must keep today's UNSUPPORTED behavior exactly."""

    result = await _run(
        _preflight([(ORG_ID, _PLATFORM_TEAM), (ORG_ID, _ROCKET_TEAM)]),
        request_for("What is the status of team Platform and team Rocket?"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.UNSUPPORTED
    assert result.diagnostic == "committed_cohort_v1_only"
    assert result.committed_resolution is None
    assert result.subject_set is not None
    committed_ids = {ref.entity_id for ref in result.subject_set.committed_entity_refs}
    assert committed_ids == {_PLATFORM_TEAM.canonical_id, _ROCKET_TEAM.canonical_id}


@pytest.mark.asyncio
async def test_a_non_portfolio_cohort_still_terminates_unsupported() -> None:
    """The pre-existing D1 behavior, pinned as an explicit CHAOS-3393
    boundary control: a 2-project cohort whose wording does NOT recognize
    as PORTFOLIO_STATUS (no status anchor -- "compare", not "status of")
    keeps the published committed_cohort_v1_only outcome unchanged."""

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, NIGHTFALL_PROJECT)]),
        request_for("Compare project Ask Dev and project Nightfall"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.UNSUPPORTED
    assert result.diagnostic == "committed_cohort_v1_only"


@pytest.mark.asyncio
async def test_organization_wide_portfolio_commits_a_bounded_project_set() -> None:
    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, NIGHTFALL_PROJECT)]),
        request_for("What is the portfolio status?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.diagnostic == "committed_portfolio_org_wide"
    assert result.committed_resolution is None
    assert result.ledger is None
    assert result.subject_set is not None
    assert result.committed_subjects is not None
    assert result.committed_subjects.subject_set is result.subject_set
    committed_ids = {ref.entity_id for ref in result.subject_set.committed_entity_refs}
    assert committed_ids == {
        ASK_DEV_PROJECT.canonical_id,
        NIGHTFALL_PROJECT.canonical_id,
    }
    # Deterministic label-then-id order, never insertion order.
    assert [ref.entity_id for ref in result.subject_set.committed_entity_refs] == [
        ASK_DEV_PROJECT.canonical_id,
        NIGHTFALL_PROJECT.canonical_id,
    ]
    assert result.subject_set.cohort_complete is True
    assert result.subject_set.warnings == ()


@pytest.mark.asyncio
async def test_organization_wide_portfolio_with_no_projects_falls_back_cleanly() -> (
    None
):
    result = await _run(
        _preflight([]),
        request_for("What is the portfolio status?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.diagnostic == "proceeded_organization_wide"
    assert result.subject_set is None
    assert result.committed_resolution is None


@pytest.mark.asyncio
async def test_organization_wide_portfolio_discloses_truncation_past_the_cap() -> None:
    entities = [
        AuthorizedEntity(
            EntityKind.PROJECT, f"project-{index:02}", f"Project {index:02}"
        )
        for index in range(MAX_PORTFOLIO_PROJECTS + 5)
    ]
    result = await _run(
        _preflight([(ORG_ID, entity) for entity in entities]),
        request_for("What is the portfolio status?"),
    )

    assert result.decision is PreflightDecision.PROCEED
    assert result.diagnostic == "committed_portfolio_org_wide"
    assert result.subject_set is not None
    assert len(result.subject_set.committed_entity_refs) == MAX_PORTFOLIO_PROJECTS
    # Never a silent sample: the cap being hit is disclosed.
    assert any(
        "authorized projects" in warning for warning in result.subject_set.warnings
    )


@pytest.mark.asyncio
async def test_organization_wide_portfolio_fails_closed_on_catalog_outage() -> None:
    """CHAOS-3393 codex MED-3: a catalog OUTAGE during ORGANIZATION_WIDE
    project enumeration must never be conflated with an authoritative,
    confirmed-zero organization -- the prior behavior fell back to the
    ordinary organization-wide PROCEED (``allowed_tools=ALL_TOOLS``) on
    BOTH, silently granting unrestricted model-tool access during a
    transient catalog failure. This must instead fail closed: TERMINATE,
    TEMPORARILY_UNAVAILABLE, zero tools.
    """

    result = await _run(
        _preflight(
            [(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, NIGHTFALL_PROJECT)],
            fail_search=True,
        ),
        request_for("What is the portfolio status?"),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.TEMPORARILY_UNAVAILABLE
    assert result.allowed_tools == frozenset()
    assert result.subject_set is None
    assert result.committed_resolution is None
    # Never the ordinary org-wide fallback diagnostic -- that would prove
    # the outage was silently conflated with a confirmed-empty catalog.
    assert result.diagnostic != "proceeded_organization_wide"


@pytest.mark.asyncio
async def test_an_unresolved_non_project_mention_terminates_a_portfolio_cohort() -> (
    None
):
    """CHAOS-3393 codex MED-1: D2's partial-cohort relaxation (>=2 distinct
    resolved entities lets an unresolved mention be OMITTED rather than
    terminate) must NOT apply to a PORTFOLIO_STATUS question -- the
    omitted mention here is a TEAM, a kind the portfolio plan can never
    execute, and D2's own kind-blind >=2-count check would otherwise let
    it silently vanish while the run proceeds on the two resolved
    projects alone. Portfolio execution requires every typed mention to
    resolve exactly; any unresolved/ambiguous mention terminates the run,
    the same as a SINGULAR commit's own lowest-ordinal-unresolved rule.
    """

    result = await _run(
        _preflight([(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, NIGHTFALL_PROJECT)]),
        request_for(
            "What is the status of project Ask Dev, project Nightfall, and team Ghost?"
        ),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NOT_FOUND
    # Never proceeded on the two resolved projects alone.
    assert result.subject_set is None
    assert result.committed_resolution is None


@pytest.mark.asyncio
async def test_an_ambiguous_non_project_mention_terminates_a_portfolio_cohort() -> None:
    """Same MED-1 gap, the AMBIGUOUS_CANDIDATES half: a same-name team
    ambiguity must also terminate a portfolio cohort rather than being
    omitted as D2 would otherwise allow."""

    ambiguous_team_a = AuthorizedEntity(EntityKind.TEAM, "team-ghost-1", "Ghost")
    ambiguous_team_b = AuthorizedEntity(EntityKind.TEAM, "team-ghost-2", "Ghost")
    result = await _run(
        _preflight(
            [
                (ORG_ID, ASK_DEV_PROJECT),
                (ORG_ID, NIGHTFALL_PROJECT),
                (ORG_ID, ambiguous_team_a),
                (ORG_ID, ambiguous_team_b),
            ]
        ),
        request_for(
            "What is the status of project Ask Dev, project Nightfall, and team Ghost?"
        ),
    )

    assert result.decision is PreflightDecision.TERMINATE
    assert result.outcome is PublicOutcome.NEEDS_CLARIFICATION
    assert result.subject_set is None
    assert result.committed_resolution is None
