"""CHAOS-3619: the native leg, driven through the REAL orchestrator.

The leg seeds the production catalog from the corpus and runs
``run_preflight_orchestrator`` -- the real interpreter, the real subject
preflight, the real scope service, the real native producer and the real
shadow seam. Nothing about the native arm's interpretation is written by the
trial, which is the point: subject discovery and ambiguity are half this
corpus, and a hand-built ``NativeProjectionInput`` would be the trial author
baselining themselves.

**The distinction these tests exist to protect.** A native case that produces
no packet and a harness that never called the producer look identical from
the outside -- both leave zero packets. One is a measured capability result
(the arm ran and reported the run unprojectable, which is the number the
comparison turns on); the other is a broken trial reporting a capability
finding. So the producer's *invocation* is asserted separately from its
*output*, and every gap row is only meaningful because the invocation was
observed.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from dev_health_ops.api.dev import investigation_shadow as seam
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_corpus.cases import authored_cases
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.context_fabric.native_arm.producer import (
    NativeInvestigationPacketProducer,
)
from tests._chaos_3292_preflight import run_preflight_orchestrator
from tests._chaos_3295_plan_executor import (
    FakePlanExecutorRuntime,
    InvestigationRecorder,
    executor_for,
)
from trials.chaos_3619.native_leg import (
    NATIVE_CATALOG_KIND,
    catalog_entities,
    unrepresentable_corpus_kinds,
)

pytestmark = pytest.mark.asyncio

_RUN_NAMESPACE = uuid.UUID("3619b00b-0000-4000-8000-000000000002")

#: Measured and pinned. The production subject vocabulary has six members;
#: the corpus world models ten kinds. These four cannot enter the native
#: catalog at all, which is a vocabulary gap and not a resolution failure.
UNREPRESENTABLE_KINDS = frozenset({"dependency", "initiative", "portfolio", "service"})
#: Of the analyst's grant: how many entities the catalog can hold, and how
#: many it structurally cannot.
SEEDED_FOR_ANALYST = 38
SKIPPED_FOR_ANALYST = 9


class _CapturingProducer:
    """The REAL producer, plus what it was handed and what it returned.

    Delegates rather than reimplements: a double that *behaved* like the
    producer would prove nothing about the producer.
    """

    def __init__(self) -> None:
        self._inner = NativeInvestigationPacketProducer()
        self.contexts: list[Any] = []
        self.payloads: list[Any] = []

    def build_packet(self, run: Any) -> Any:
        self.contexts.append(run)
        payload = self._inner.build_packet(run)
        self.payloads.append(payload)
        return payload


async def _run_case(question: str, case_id: str, principal_id: str) -> Any:
    seeding = catalog_entities(principal_id)
    producer = _CapturingProducer()
    await run_preflight_orchestrator(
        question=question,
        entities=list(seeding.entities),
        org_id=world.PRINCIPALS[principal_id].tenant_id,
        run_id=str(uuid.uuid5(_RUN_NAMESPACE, case_id)),
        plan_registry=CORE_PLANS_BY_INTENT,
        plan_executor=executor_for(FakePlanExecutorRuntime()),
        recorder_factory=InvestigationRecorder,
        investigation_shadow=seam.InvestigationShadow(enabled=True),
        investigation_packet_producer=producer,
    )
    return producer


class TestTheCatalogSeedingIsHonestAboutWhatItCannotHold:
    async def test_the_unrepresentable_kinds_are_derived_not_listed(self) -> None:
        """A new corpus kind, or a new EntityKind, must move this by itself."""

        assert unrepresentable_corpus_kinds() == UNREPRESENTABLE_KINDS

    async def test_skipped_entities_are_reported_rather_than_dropped(self) -> None:
        """A silent drop turns a vocabulary gap into a resolution failure.

        The native arm would then appear to miss subjects it was never able
        to see, and the trial would report a capability difference that is
        really a type-system difference.
        """

        seeding = catalog_entities(world.PRINCIPAL_ANALYST)
        assert len(seeding.entities) == SEEDED_FOR_ANALYST
        assert len(seeding.skipped) == SKIPPED_FOR_ANALYST
        assert seeding.skipped_kinds == UNREPRESENTABLE_KINDS

    async def test_no_corpus_kind_is_mapped_onto_a_different_kind(self) -> None:
        """Every mapping is an exact counterpart, never an approximation.

        Mapping ``service`` onto ``project`` would let the native arm
        "resolve" a service by answering about something else -- the frozen
        registry's ``wrong_but_similar_subject_ranked_first`` fault mode,
        introduced by the trial's own adapter.
        """

        for corpus_kind, entity_kind in NATIVE_CATALOG_KIND.items():
            assert corpus_kind == entity_kind.value, (
                f"{corpus_kind!r} is mapped onto {entity_kind.value!r}; an "
                "approximate mapping manufactures wrong-but-confident subjects"
            )

    async def test_the_catalog_is_the_grant_not_the_tenant(self) -> None:
        """The corpus plants a restricted same-tenant project precisely so a
        tenant-scoped catalog looks correct and is not."""

        seeding = catalog_entities(world.PRINCIPAL_ANALYST)
        seeded_ids = {entity.canonical_id for _org, entity in seeding.entities}
        grant = world.authorized_entity_ids(world.PRINCIPAL_ANALYST)
        assert "proj_quarry" not in grant, "fixture drift: quarry is now granted"
        assert "proj_quarry" not in seeded_ids, (
            "the native catalog was seeded with an entity outside the principal's grant"
        )
        assert seeded_ids <= set(grant)


class TestTheLegReachesTheRealProducer:
    """The invocation, asserted apart from the output.

    Without this, every ``arm_declared_gap`` row in the trial would be
    indistinguishable from a harness that never called the arm at all.
    """

    async def test_the_producer_is_invoked_with_a_finished_run_context(
        self,
    ) -> None:
        case = next(c for c in authored_cases() if c.case_id.startswith("T01"))
        producer = await _run_case(case.question, case.case_id, case.principal_id)
        assert len(producer.contexts) == 1, (
            "the native producer was not invoked; any gap recorded for this "
            "case would be a harness artefact wearing a capability result's "
            "clothes"
        )
        context = producer.contexts[0]
        assert isinstance(context, seam.FinishedRunContext)
        assert context.organization_id == world.ORG_HELIO

    async def test_a_gap_is_the_arms_decision_not_an_absent_call(self) -> None:
        """The two facts together are what makes a gap row meaningful."""

        case = next(c for c in authored_cases() if c.case_id.startswith("T01"))
        producer = await _run_case(case.question, case.case_id, case.principal_id)
        assert producer.contexts, "producer never called"
        assert producer.payloads == [None], (
            "expected the arm to decline this run; if it now projects a "
            "packet that is a capability change, and the fairness table's "
            "native figures must be re-measured"
        )

    async def test_the_run_carries_a_bounded_window(self) -> None:
        """A window-less run is refused by the producer for a reason that has
        nothing to do with the corpus, so a leg that produced one would
        report a harness gap as an arm gap."""

        case = next(c for c in authored_cases() if c.case_id.startswith("T01"))
        producer = await _run_case(case.question, case.case_id, case.principal_id)
        context = producer.contexts[0]
        assert context.window_start is not None
        assert context.window_end is not None
        assert context.window_end > context.window_start
