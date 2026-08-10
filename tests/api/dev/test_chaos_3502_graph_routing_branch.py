"""CHAOS-3502: the graph-assisted routing branch itself (CHAOS-3660 sign-off).

Covers the two binding additions from team-lead's design sign-off on
CHAOS-3660, both exercised through the real orchestrator seam
(``run_preflight_orchestrator`` -- a genuine ``DISCOVERED_COHORT``
``preflight_result`` only comes from the real interpreter/preflight
pipeline, never a hand-built one):

(a) every ``GraphQueryOutcome`` the seam can return emits a content-safe,
    outcome-tagged log record plus increments
    ``ASK_DEV_GRAPH_ROUTING_OUTCOME_TOTAL`` with that outcome as the label
    (beta gate 10 observability hook) -- and every outcome other than
    ``CANCELLED`` falls through to the legacy model-tool-choice loop and
    still produces a real answer, per the CHAOS-3660 fallback table;
    ``CANCELLED`` terminates the run directly instead.
(b) an explicit flag-off run is byte-identical (via
    ``RunOutput.outcome_tuple()``) to a run where the graph collaborators
    were never constructed at all, and the fake is never called -- proving
    the new branch is genuinely inert, not merely "usually skipped", when
    the runtime kill switch is off.

Sibling of ``test_chaos_3502_graph_routing_seam.py`` (that file covers the
flag scaffolding and construction-only collaborator wiring landed ahead of
this branch); this file covers the branch's own dispatch behavior.
"""

from __future__ import annotations

import logging

import pytest

from dev_health_ops.api.dev.evidence_service import (
    EvidenceReferenceSigner,
    EvidenceService,
)
from dev_health_ops.api.dev.graph_investigation_query import (
    CohortDiscoveryFamily,
    GraphQueryOutcome,
)
from dev_health_ops.api.dev.orchestrator import GRAPH_ROUTING_RUNTIME_FLAG
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.metrics.prometheus import ASK_DEV_GRAPH_ROUTING_OUTCOME_TOTAL
from tests._chaos_3292_preflight import ORG_ID, run_preflight_orchestrator
from tests._chaos_3502_graph_investigation_fake import FakeGraphInvestigationQuery

#: A real, zero-mention, cohort-discovery-shaped question --
#: ``question_interpreter``'s ``cohort.discovery`` recognizer (subject
#: anchor "teams" + judgment anchor "struggling", CHAOS-3652) resolves this
#: to ``QuestionIntentID.DISCOVERED_COHORT`` /
#: ``Cardinality.ORGANIZATION_WIDE`` through the real preflight pipeline --
#: never asserted by construction here, only by driving the real seam. Also
#: an exclusive (TEAM, PRESSURE) pairing (CHAOS-3689), so
#: ``classify_cohort_discovery_family`` resolves it to ``TEAM_PRESSURE``.
_DISCOVERED_COHORT_QUESTION = "Which teams are struggling right now?"

#: CHAOS-3689: still recognized as ``DISCOVERED_COHORT`` by the ``cohort.
#: discovery`` recognizer (its own subject anchors are an undifferentiated
#: union -- "team"/"teams" OR "project"/"projects" both satisfy it) but a
#: mixed subject for family classification -- both TEAM and PROJECT anchors
#: present, so neither exclusive pairing is satisfied. Verified directly
#: (not guessed): the real interpreter resolves this to
#: ``QuestionIntentID.DISCOVERED_COHORT`` with zero mentions, while
#: ``classify_cohort_discovery_family`` returns ``None``.
_UNCLASSIFIABLE_COHORT_QUESTION = "Which teams and projects are struggling?"

_EVIDENCE_SIGNING_SECRET = "chaos-3502-branch-test-secret-at-least-32-bytes"

#: Every non-terminal outcome: falls through to the legacy loop and still
#: answers. CANCELLED is covered separately below (it terminates instead).
_FALLTHROUGH_OUTCOMES = (
    GraphQueryOutcome.COMPLETED,
    GraphQueryOutcome.DISABLED,
    GraphQueryOutcome.UNAVAILABLE,
    GraphQueryOutcome.STALE,
    GraphQueryOutcome.DEADLINE_EXCEEDED,
    GraphQueryOutcome.PROVIDER_FAILURE,
)


class _Entitlement:
    async def require(self, _org_id: str) -> None:
        return None


class _Authorizer:
    async def resolve(self, _org_id: str, _permission_fingerprint: str, _request):
        raise NotImplementedError(
            "assembly is deferred (CHAOS-3502) -- this branch never reaches "
            "evidence admission yet, so this must never be called"
        )


def _evidence_service() -> EvidenceService:
    return EvidenceService(
        entitlement=_Entitlement(),
        authorizer=_Authorizer(),
        signer=EvidenceReferenceSigner(_EVIDENCE_SIGNING_SECRET),
        native_adapters=(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", _FALLTHROUGH_OUTCOMES)
async def test_every_fallthrough_outcome_is_observed_and_still_answers(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    outcome: GraphQueryOutcome,
) -> None:
    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    fake = FakeGraphInvestigationQuery(outcome=outcome)
    before = ASK_DEV_GRAPH_ROUTING_OUTCOME_TOTAL.labels(
        outcome=outcome.value
    )._value.get()

    with caplog.at_level(logging.INFO, logger="dev_health_ops.api.dev.orchestrator"):
        output = await run_preflight_orchestrator(
            question=_DISCOVERED_COHORT_QUESTION,
            entities=[],
            org_id=ORG_ID,
            script_id=f"chaos3502-{outcome.value}",
            graph_investigation_query=fake,
            evidence_service=_evidence_service(),
        )

    assert len(fake.received_requests) == 1, (
        "the graph seam must be called exactly once for a genuine "
        "DISCOVERED_COHORT run with both collaborators wired and the "
        "runtime flag on"
    )
    assert fake.received_requests[0].intent_id.value == "discovered_cohort"

    # Every fallthrough outcome must still complete as a real, non-error
    # answer via the legacy loop -- per the CHAOS-3660 fallback table, a
    # graph outage/lag/refusal-to-assemble must never regress an existing
    # working answer.
    assert output.result.state is not RunState.CANCELLED
    assert output.result.state is not RunState.FAILED
    assert output.result.answer is not None

    assert any(
        record.levelno == logging.INFO
        and record.message == "ask_dev.orchestrator.graph_routing_outcome"
        and getattr(record, "outcome", None) == outcome.value
        for record in caplog.records
    ), f"outcome {outcome.value!r} must emit the content-safe outcome-tagged log record"

    after = ASK_DEV_GRAPH_ROUTING_OUTCOME_TOTAL.labels(
        outcome=outcome.value
    )._value.get()
    assert after == before + 1, (
        f"outcome {outcome.value!r} must increment "
        "ASK_DEV_GRAPH_ROUTING_OUTCOME_TOTAL with that outcome as the label"
    )


@pytest.mark.asyncio
async def test_cancelled_outcome_terminates_the_run_directly(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """CANCELLED is the one outcome that does NOT fall through -- the RUN is
    cancelled, not just this one call (module docstring / CHAOS-3660
    fallback table). Distinct from the parametrized fallthrough cases
    above: the legacy loop must never even start, so ``calls`` stays empty.
    """

    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    fake = FakeGraphInvestigationQuery(outcome=GraphQueryOutcome.CANCELLED)
    before = ASK_DEV_GRAPH_ROUTING_OUTCOME_TOTAL.labels(
        outcome=GraphQueryOutcome.CANCELLED.value
    )._value.get()

    with caplog.at_level(logging.INFO, logger="dev_health_ops.api.dev.orchestrator"):
        output = await run_preflight_orchestrator(
            question=_DISCOVERED_COHORT_QUESTION,
            entities=[],
            org_id=ORG_ID,
            script_id="chaos3502-cancelled",
            graph_investigation_query=fake,
            evidence_service=_evidence_service(),
        )

    assert output.result.state is RunState.CANCELLED
    assert output.result.error is not None
    assert output.result.error.code == "cancelled"
    assert output.result.answer is None
    assert output.calls == [], (
        "a CANCELLED graph outcome must terminate the run before the "
        "legacy model-tool-choice loop ever executes a tool"
    )

    assert any(
        record.levelno == logging.INFO
        and record.message == "ask_dev.orchestrator.graph_routing_outcome"
        and getattr(record, "outcome", None) == "cancelled"
        for record in caplog.records
    ), "CANCELLED must still emit the content-safe outcome-tagged log record"

    after = ASK_DEV_GRAPH_ROUTING_OUTCOME_TOTAL.labels(
        outcome=GraphQueryOutcome.CANCELLED.value
    )._value.get()
    assert after == before + 1


@pytest.mark.asyncio
async def test_flag_off_is_byte_identical_to_collaborators_never_wired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3660 binding addition (b): with the runtime flag off, a run
    wired with real (non-``None``) ``graph_investigation_query``/
    ``evidence_service`` collaborators must be byte-identical
    (``RunOutput.outcome_tuple()``) to a run where those collaborators were
    never constructed at all -- and the fake must never be called. Proves
    the branch is genuinely inert when the kill switch is off, not merely
    "usually skipped".
    """

    monkeypatch.delenv(GRAPH_ROUTING_RUNTIME_FLAG, raising=False)

    fake = FakeGraphInvestigationQuery(outcome=GraphQueryOutcome.COMPLETED)
    with_collaborators = await run_preflight_orchestrator(
        question=_DISCOVERED_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3502-flag-off",
        graph_investigation_query=fake,
        evidence_service=_evidence_service(),
    )
    assert fake.received_requests == [], (
        "flag off must mean the graph seam is never called, even though "
        "both collaborators are wired"
    )

    without_collaborators = await run_preflight_orchestrator(
        question=_DISCOVERED_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3502-flag-off",
    )

    assert with_collaborators.outcome_tuple() == without_collaborators.outcome_tuple()


@pytest.mark.asyncio
async def test_the_request_carries_the_classified_cohort_discovery_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3689: the routing branch populates ``GraphInvestigationRequest.
    cohort_discovery_family`` from ``question_interpreter.
    classify_cohort_discovery_family`` -- supplied, not re-derived by the
    seam (same "supplied, never invented" posture as ``authorized_entity_ids``/
    ``window_start``/``window_end``).
    """

    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    fake = FakeGraphInvestigationQuery(outcome=GraphQueryOutcome.DISABLED)

    await run_preflight_orchestrator(
        question=_DISCOVERED_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3689-family",
        graph_investigation_query=fake,
        evidence_service=_evidence_service(),
    )

    assert len(fake.received_requests) == 1
    assert (
        fake.received_requests[0].cohort_discovery_family
        is CohortDiscoveryFamily.TEAM_PRESSURE
    )


@pytest.mark.asyncio
async def test_an_unclassifiable_cohort_question_never_calls_the_seam(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3689: an honest ``None`` classification gates the routing
    branch itself -- the graph seam is never invoked at all, exactly like
    every other gate failure (flag off, missing collaborator). The run
    still completes via the legacy loop, and is byte-identical
    (``RunOutput.outcome_tuple()``) to a run with the graph collaborators
    never wired at all, mirroring the flag-off inertness test above.
    """

    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    fake = FakeGraphInvestigationQuery(outcome=GraphQueryOutcome.COMPLETED)

    with_collaborators = await run_preflight_orchestrator(
        question=_UNCLASSIFIABLE_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3689-unclassifiable",
        graph_investigation_query=fake,
        evidence_service=_evidence_service(),
    )
    assert fake.received_requests == [], (
        "an unclassifiable cohort-discovery family must mean the graph seam "
        "is never called, even though both collaborators are wired and the "
        "flag is on"
    )
    assert with_collaborators.result.state is not RunState.FAILED
    assert with_collaborators.result.answer is not None

    without_collaborators = await run_preflight_orchestrator(
        question=_UNCLASSIFIABLE_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3689-unclassifiable",
    )

    assert with_collaborators.outcome_tuple() == without_collaborators.outcome_tuple()
