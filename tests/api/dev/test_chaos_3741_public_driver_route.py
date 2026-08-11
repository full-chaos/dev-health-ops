"""CHAOS-3741: production-packet drivers reach the public Ask Dev route.

The primary oracle executes the real Context Fabric projection, driver
discovery, packet builder, candidate extraction, canonical admission, and
orchestrator route. Scenario variants start from that production packet and
change only typed packet fields needed to exercise W4 dispositions and
admission outcomes; they never hand-author a parallel packet shape.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts import (
    AnswerStatus,
    DevAnswerDriverExclusionReason,
    DevAnswerDriverStanding,
    DevAnswerDriverWithheldReason,
    FreshnessState,
    PacketLimitationKind,
    StreamEventType,
)
from dev_health_ops.api.dev.contracts_v2.base import QuestionIntentID
from dev_health_ops.api.dev.evidence_service import EvidenceRecord, EvidenceService
from dev_health_ops.api.dev.graph_evidence_admission import extract_evidence_candidates
from dev_health_ops.api.dev.investigation_contract import (
    AskDevInvestigationPacket,
    ComparisonShape,
    DriverStanding,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.orchestrator import (
    GRAPH_GROUNDED_WARNING_DRIVER_PROJECTION_TRUNCATED,
    GRAPH_GROUNDED_WARNING_EVIDENCE_GAP,
    GRAPH_ROUTING_RUNTIME_FLAG,
)
from dev_health_ops.api.dev.streaming import stream_orchestrator
from dev_health_ops.context_fabric.graph_arm import corpus_adapter
from dev_health_ops.context_fabric.graph_arm.drivers import discover_drivers
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    ProductionJobContext,
    build_production_packet,
)
from dev_health_ops.context_fabric.graph_arm.readback import ProjectionGraphReader
from dev_health_ops.metrics.prometheus import ASK_DEV_GRAPH_ASSEMBLY_OUTCOME_TOTAL
from tests._chaos_3292_preflight import ORG_ID, run_preflight_orchestrator
from tests._chaos_3502_graph_investigation_fake import FakeGraphInvestigationQuery
from tests.api.dev.test_chaos_3650_graph_grounded_assembler import (
    _DISCOVERED_COHORT_QUESTION,
    _canonical_enrichment,
    _Entitlement,
    _OrgAuthorizer,
)
from tests.context_fabric import chaos_3620_spine as spine

_SUBJECT = "proj_identity_rewrite"
_RUN_ID = "7c3e9a10-2222-4333-9444-555566667777"
_SIGNING_SECRET = "chaos-3741-production-route-test-signing-secret"


async def _production_packet() -> AskDevInvestigationPacket:
    """Emit a supported production packet from the real corpus projection."""

    projection = spine.helio_projection()
    readout = await ProjectionGraphReader(projection).neighbourhood(
        org_id=projection.org_id,
        seed_canonical_ids=[_SUBJECT],
        authorized_entity_ids=sorted(
            corpus_adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST)
        ),
        max_hops=1,
    )
    findings, truncated = discover_drivers(
        readout,
        _SUBJECT,
        as_of=spine.PRODUCED_AT,
    )
    from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner

    return build_production_packet(
        readout=readout,
        job=ProductionJobContext(
            job_id="chaos_3741_production_route",
            intent_id=QuestionIntentID.ENTITY_STATUS,
            run_id=_RUN_ID,
            job_statement="What is the current status and what is driving it?",
            comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
            window_start=world.WINDOW_START,
            window_end=world.WINDOW_END,
        ),
        watermark=spine.current_watermark(),
        signer=EvidenceReferenceSigner(_SIGNING_SECRET),
        produced_at=spine.PRODUCED_AT,
        drivers=findings,
        drivers_truncated=truncated,
    )


async def _route_packet() -> tuple[AskDevInvestigationPacket, tuple[str, ...]]:
    """Retenant the real packet without rewriting producer-owned locators."""

    packet = await _production_packet()
    asserted = tuple(
        candidate
        for candidate in packet.driver_analysis.candidates
        if candidate.standing
        in {DriverStanding.PRINCIPAL_DRIVER, DriverStanding.CONTRIBUTING_DRIVER}
    )
    assert asserted, "the production corpus packet must contain an asserted driver"
    required_handles = tuple(
        sorted(
            {
                str(handle)
                for candidate in asserted
                for handle in candidate.supporting_evidence_ids
            }
        )
    )

    payload = deepcopy(packet.model_dump(mode="json"))
    payload["organization_id"] = ORG_ID
    return AskDevInvestigationPacket.model_validate(payload), required_handles


class _ProductionResolver:
    source_system = "context_fabric_graph_arm"

    def __init__(
        self,
        *,
        stale_locators: frozenset[str] = frozenset(),
        unavailable_locators: frozenset[str] = frozenset(),
    ) -> None:
        self.stale_locators = stale_locators
        self.unavailable_locators = unavailable_locators

    async def resolve(self, *, org_id, scope, candidate):
        del org_id, scope
        if candidate.locator in self.unavailable_locators:
            raise RuntimeError("source unavailable")
        return EvidenceRecord(
            source_system=self.source_system,
            source_version="test.v1",
            entity_type="team",
            entity_id="team_platform",
            display_label="Platform driver evidence",
            observed_at=datetime(2026, 8, 9, tzinfo=UTC),
            freshness=(
                FreshnessState.STALE
                if candidate.locator in self.stale_locators
                else FreshnessState.FRESH
            ),
            provenance="native",
            confidence=1.0,
        )


def _production_evidence_service(
    *,
    stale_locators: frozenset[str] = frozenset(),
    unavailable_locators: frozenset[str] = frozenset(),
) -> EvidenceService:
    from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner

    return EvidenceService(
        entitlement=_Entitlement(),
        authorizer=_OrgAuthorizer(),
        signer=EvidenceReferenceSigner(_SIGNING_SECRET),
        native_adapters=(),
        candidate_resolvers=(
            _ProductionResolver(
                stale_locators=stale_locators,
                unavailable_locators=unavailable_locators,
            ),
        ),
    )


def _replace_driver_candidates(
    packet: AskDevInvestigationPacket, candidates: list[dict]
) -> AskDevInvestigationPacket:
    """Replace only W4's typed candidate slice and rebuild reverse support."""

    payload = deepcopy(packet.model_dump(mode="json"))
    payload["driver_analysis"]["candidates"] = candidates
    payload["driver_analysis"]["principal_driver_ids"] = [
        item["driver_id"]
        for item in candidates
        if item["standing"] == DriverStanding.PRINCIPAL_DRIVER.value
    ]
    support_by_handle: dict[str, list[str]] = {}
    for candidate in candidates:
        for handle in candidate["supporting_evidence_ids"]:
            support_by_handle.setdefault(handle, []).append(candidate["driver_id"])
    for entry in payload["evidence_coverage"]["evidence_index"]:
        entry["supports_driver_ids"] = sorted(
            support_by_handle.get(entry["evidence"]["evidence_ref_id"], [])
        )
    limitation_kinds = {
        item["kind"] for item in payload["evidence_coverage"]["limitations"]
    }
    if (
        any(
            (item.get("staffing_qualification") or {}).get("denominator_state")
            in {"partial_allocation_evidence", "denominator_absent"}
            for item in candidates
        )
        and "absent_staffing_denominator" not in limitation_kinds
    ):
        payload["evidence_coverage"]["limitations"].append(
            {
                "kind": "absent_staffing_denominator",
                "detail": "The allocation denominator is partial or absent.",
            }
        )
    if any(item["conflicting_evidence_ids"] for item in candidates) and (
        "conflicting_evidence" not in limitation_kinds
    ):
        payload["evidence_coverage"]["limitations"].append(
            {
                "kind": "conflicting_evidence",
                "detail": "Canonical evidence conflicts with one driver judgment.",
            }
        )
    return AskDevInvestigationPacket.model_validate(payload)


async def _run_packet(
    monkeypatch: pytest.MonkeyPatch,
    packet: AskDevInvestigationPacket,
    *,
    evidence_service: EvidenceService | None = None,
    script_id: str,
):
    monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, "1")
    return await run_preflight_orchestrator(
        question=_DISCOVERED_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id=script_id,
        graph_investigation_query=FakeGraphInvestigationQuery(packet=packet),
        evidence_service=evidence_service or _production_evidence_service(),
        graph_routing_entitlement=_Entitlement(),
        canonical_enrichment=_canonical_enrichment(),
    )


@pytest.mark.asyncio
async def test_real_production_packet_driver_is_not_dropped_at_public_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RED oracle: baseline has admitted driver evidence but no public driver."""

    monkeypatch.setenv("ASK_DEV_GRAPH_ROUTING_ENABLED", "1")
    packet, supporting_handles = await _route_packet()
    candidates = extract_evidence_candidates(packet)
    assert candidates
    assert all(candidate.locator for candidate in candidates), (
        "the production packet must carry the real source locators admission uses"
    )
    assert not any("chaos-3741-" in candidate.locator for candidate in candidates)
    assert packet.outcome.value == "supported_with_gaps"
    assert supporting_handles
    assert any(
        handle in supporting_handles
        for candidate in packet.driver_analysis.candidates
        for handle in candidate.supporting_evidence_ids
    )

    output = await run_preflight_orchestrator(
        question=_DISCOVERED_COHORT_QUESTION,
        entities=[],
        org_id=ORG_ID,
        script_id="chaos3741-public-driver-route",
        graph_investigation_query=FakeGraphInvestigationQuery(packet=packet),
        evidence_service=_production_evidence_service(),
        graph_routing_entitlement=_Entitlement(),
        canonical_enrichment=_canonical_enrichment(),
    )

    answer = output.result.answer
    assert answer is not None
    assert answer.graph_assisted is not None
    assert answer.evidence
    assert len(answer.evidence) == len(packet.evidence_coverage.evidence_index)
    assert (
        PacketLimitationKind.INTERPRETATION_UNCERTAINTY
        in answer.graph_assisted.limitations
    )
    assert answer.graph_assisted.ranked_drivers, (
        "an admitted production driver candidate must reach the public route"
    )
    driver = answer.graph_assisted.ranked_drivers[0]
    assert driver.contribution is None
    assert driver.standing is DevAnswerDriverStanding.PRINCIPAL_DRIVER
    assert driver.role is not None
    assert driver.category is not None
    assert driver.confidence is not None
    assert driver.relevance is not None
    assert driver.freshness is FreshnessState.FRESH
    assert set(driver.evidence_ref_ids) <= {
        evidence.evidence_ref_id for evidence in answer.evidence
    }
    assert "summary" not in driver.model_dump()
    assert "drv_block_wu_authcore_release" not in answer.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize("gap_kind", ["unconfigured", "unavailable"])
async def test_partial_driver_support_gap_is_disclosed_at_answer_level(
    monkeypatch: pytest.MonkeyPatch, gap_kind: str
) -> None:
    """One admitted support must not hide its unavailable sibling."""

    packet, _ = await _route_packet()
    payload = packet.model_dump(mode="json")
    entries = payload["evidence_coverage"]["evidence_index"]
    candidate = deepcopy(payload["driver_analysis"]["candidates"][0])
    support_handles = [
        entries[0]["evidence"]["evidence_ref_id"],
        entries[1]["evidence"]["evidence_ref_id"],
    ]
    candidate["supporting_evidence_ids"] = support_handles
    packet = _replace_driver_candidates(packet, [candidate])

    unavailable_locators: frozenset[str] = frozenset()
    if gap_kind == "unconfigured":
        changed = packet.model_dump(mode="json")
        for entry in changed["evidence_coverage"]["evidence_index"]:
            if entry["evidence"]["evidence_ref_id"] == support_handles[1]:
                entry["evidence"]["source_system"] = "unconfigured_graph_source"
        packet = AskDevInvestigationPacket.model_validate(changed)
    else:
        unavailable_locator = next(
            entry.evidence.record_locator
            for entry in packet.evidence_coverage.evidence_index
            if entry.evidence.evidence_ref_id == support_handles[1]
        )
        assert unavailable_locator is not None
        unavailable_locators = frozenset({unavailable_locator})

    output = await _run_packet(
        monkeypatch,
        packet,
        evidence_service=_production_evidence_service(
            unavailable_locators=unavailable_locators
        ),
        script_id=f"chaos3741-partial-{gap_kind}",
    )
    answer = output.result.answer
    assert answer is not None
    assert answer.status is AnswerStatus.DEGRADED
    assert GRAPH_GROUNDED_WARNING_EVIDENCE_GAP in answer.warnings
    assert answer.graph_assisted is not None
    assert PacketLimitationKind.MISSING_SOURCE in answer.graph_assisted.limitations
    assert len(answer.graph_assisted.ranked_drivers) == 1
    assert (
        answer.graph_assisted.ranked_drivers[0].withheld_reason
        is DevAnswerDriverWithheldReason.EVIDENCE_UNAVAILABLE
    )


@pytest.mark.asyncio
async def test_w4_order_dispositions_conflicts_and_staffing_survive_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The public route consumes W4 ordering; it does not rank again."""

    packet, _ = await _route_packet()
    payload = packet.model_dump(mode="json")
    entries = payload["evidence_coverage"]["evidence_index"]
    handles = [entry["evidence"]["evidence_ref_id"] for entry in entries[:4]]
    original = deepcopy(payload["driver_analysis"]["candidates"][0])

    excluded = deepcopy(original)
    excluded.update(
        driver_id="drv_private_excluded",
        standing=DriverStanding.EXCLUDED.value,
        relevance="historical_only",
        exclusion_reason="not_currently_relevant",
        supporting_evidence_ids=[handles[0]],
        conflicting_evidence_ids=[],
        conflict_note=None,
    )
    principal = deepcopy(original)
    principal.update(
        driver_id="drv_private_principal",
        supporting_evidence_ids=[handles[1]],
        conflicting_evidence_ids=[handles[2]],
        conflict_note="A canonical observation conflicts with this judgment.",
        confidence_qualifier="qualified",
    )
    contributing = deepcopy(original)
    contributing.update(
        driver_id="drv_private_staffing",
        standing=DriverStanding.CONTRIBUTING_DRIVER.value,
        category="capacity_or_staffing",
        supporting_evidence_ids=[handles[3]],
        conflicting_evidence_ids=[],
        conflict_note=None,
        staffing_qualification={
            "denominator_state": "partial_allocation_evidence",
            "denominator_source_classes": ["work_item"],
            "qualification_note": "Allocation evidence covers only part of the window.",
        },
        confidence_qualifier="qualified",
    )
    packet = _replace_driver_candidates(packet, [excluded, principal, contributing])

    output = await _run_packet(monkeypatch, packet, script_id="chaos3741-w4-order")
    answer = output.result.answer
    assert answer is not None and answer.graph_assisted is not None
    drivers = answer.graph_assisted.ranked_drivers
    assert [driver.rank for driver in drivers] == [1, 2, 3]
    assert [driver.standing for driver in drivers] == [
        DevAnswerDriverStanding.EXCLUDED,
        DevAnswerDriverStanding.PRINCIPAL_DRIVER,
        DevAnswerDriverStanding.CONTRIBUTING_DRIVER,
    ]
    assert (
        drivers[0].exclusion_reason
        is DevAnswerDriverExclusionReason.NOT_CURRENTLY_RELEVANT
    )
    assert drivers[1].conflicting_evidence_ref_ids
    assert drivers[2].staffing_qualification is not None
    assert drivers[2].staffing_qualification.denominator_state.value == (
        "partial_allocation_evidence"
    )
    assert all(driver.contribution is None for driver in drivers)
    serialized = answer.model_dump_json()
    assert "drv_private_" not in serialized
    assert "Allocation evidence covers only" not in serialized
    assert "A canonical observation conflicts" not in serialized


@pytest.mark.asyncio
async def test_withholding_an_earlier_candidate_does_not_rerank_w4_survivor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packet, _ = await _route_packet()
    payload = packet.model_dump(mode="json")
    entries = payload["evidence_coverage"]["evidence_index"]
    handles = [entry["evidence"]["evidence_ref_id"] for entry in entries[:2]]
    original = deepcopy(payload["driver_analysis"]["candidates"][0])
    withheld = deepcopy(original)
    withheld.update(
        driver_id="drv_private_withheld_first",
        standing=DriverStanding.CONTRIBUTING_DRIVER.value,
        supporting_evidence_ids=[handles[0]],
    )
    survivor = deepcopy(original)
    survivor.update(
        driver_id="drv_private_survivor_second",
        supporting_evidence_ids=[handles[1]],
    )
    packet = _replace_driver_candidates(packet, [withheld, survivor])
    changed = packet.model_dump(mode="json")
    for entry in changed["evidence_coverage"]["evidence_index"]:
        if entry["evidence"]["evidence_ref_id"] == handles[0]:
            entry["evidence"]["source_system"] = "unconfigured_graph_source"
    packet = AskDevInvestigationPacket.model_validate(changed)

    output = await _run_packet(monkeypatch, packet, script_id="chaos3741-w4-rank-gap")
    answer = output.result.answer
    assert answer is not None and answer.graph_assisted is not None
    assert len(answer.graph_assisted.ranked_drivers) == 1
    assert answer.graph_assisted.ranked_drivers[0].rank == 2
    assert "drv_private_" not in answer.model_dump_json()


@pytest.mark.asyncio
async def test_public_driver_cap_degrades_and_discloses_omitted_w4_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packet, _ = await _route_packet()
    payload = packet.model_dump(mode="json")
    handles = [
        entry["evidence"]["evidence_ref_id"]
        for entry in payload["evidence_coverage"]["evidence_index"]
    ]
    original = payload["driver_analysis"]["candidates"][0]
    candidates = []
    for index in range(26):
        candidate = deepcopy(original)
        candidate.update(
            driver_id=f"drv_private_cap_{index + 1}",
            standing=(
                DriverStanding.PRINCIPAL_DRIVER.value
                if index == 0
                else DriverStanding.CONTRIBUTING_DRIVER.value
            ),
            supporting_evidence_ids=[handles[index % len(handles)]],
        )
        candidates.append(candidate)
    packet = _replace_driver_candidates(packet, candidates)
    assert len(packet.driver_analysis.candidates) == 26
    assert packet.driver_analysis.candidates_truncated is False

    label = "assembled_degraded_driver_projection_truncated"
    before = ASK_DEV_GRAPH_ASSEMBLY_OUTCOME_TOTAL.labels(outcome=label)._value.get()
    output = await _run_packet(
        monkeypatch, packet, script_id="chaos3741-public-driver-cap"
    )
    after = ASK_DEV_GRAPH_ASSEMBLY_OUTCOME_TOTAL.labels(outcome=label)._value.get()

    answer = output.result.answer
    assert answer is not None and answer.graph_assisted is not None
    assert answer.status is AnswerStatus.DEGRADED
    assert answer.warnings == [GRAPH_GROUNDED_WARNING_DRIVER_PROJECTION_TRUNCATED]
    assert answer.coverage.degraded_required_sources == []
    assert answer.coverage.available_source_count == 2
    assert len(answer.graph_assisted.ranked_drivers) == 25
    assert [driver.rank for driver in answer.graph_assisted.ranked_drivers] == list(
        range(1, 26)
    )
    assert after == before + 1
    assert "drv_private_cap_26" not in answer.model_dump_json()


@pytest.mark.asyncio
async def test_stale_support_degrades_and_discloses_freshness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packet, supporting = await _route_packet()
    support_locator = next(
        entry.evidence.record_locator
        for entry in packet.evidence_coverage.evidence_index
        if entry.evidence.evidence_ref_id == supporting[0]
    )
    assert support_locator is not None
    output = await _run_packet(
        monkeypatch,
        packet,
        evidence_service=_production_evidence_service(
            stale_locators=frozenset({support_locator})
        ),
        script_id="chaos3741-stale-support",
    )
    answer = output.result.answer
    assert answer is not None and answer.graph_assisted is not None
    assert answer.status is AnswerStatus.DEGRADED
    assert GRAPH_GROUNDED_WARNING_EVIDENCE_GAP in answer.warnings
    assert PacketLimitationKind.STALE_SOURCE in answer.graph_assisted.limitations
    assert answer.graph_assisted.ranked_drivers[0].freshness is FreshnessState.STALE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("denominator_state", "source_classes"),
    [
        ("allocation_evidence_available", ["work_item"]),
        ("partial_allocation_evidence", ["work_item"]),
        ("denominator_absent", []),
    ],
)
async def test_all_staffing_denominator_states_survive_the_public_route(
    monkeypatch: pytest.MonkeyPatch,
    denominator_state: str,
    source_classes: list[str],
) -> None:
    packet, _ = await _route_packet()
    payload = packet.model_dump(mode="json")
    candidate = deepcopy(payload["driver_analysis"]["candidates"][0])
    candidate.update(
        category="capacity_or_staffing",
        confidence_qualifier="qualified",
        staffing_qualification={
            "denominator_state": denominator_state,
            "denominator_source_classes": source_classes,
            "qualification_note": "Private qualification prose.",
        },
    )
    packet = _replace_driver_candidates(packet, [candidate])

    output = await _run_packet(
        monkeypatch, packet, script_id=f"chaos3741-staffing-{denominator_state}"
    )
    answer = output.result.answer
    assert answer is not None and answer.graph_assisted is not None
    staffing = answer.graph_assisted.ranked_drivers[0].staffing_qualification
    assert staffing is not None
    assert staffing.denominator_state.value == denominator_state
    assert [source.value for source in staffing.denominator_source_classes] == (
        source_classes
    )
    assert "Private qualification prose" not in answer.model_dump_json()


@pytest.mark.asyncio
async def test_full_driver_evidence_withholding_falls_back_without_public_judgment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packet, _ = await _route_packet()
    payload = packet.model_dump(mode="json")
    for entry in payload["evidence_coverage"]["evidence_index"]:
        entry["evidence"]["source_system"] = "unconfigured_graph_source"
    packet = AskDevInvestigationPacket.model_validate(payload)

    output = await _run_packet(
        monkeypatch, packet, script_id="chaos3741-full-withholding"
    )
    answer = output.result.answer
    assert answer is not None and answer.graph_assisted is not None
    assert answer.graph_assisted.ranked_drivers == []
    assert output.calls, "no graph material must fall through to the native answer path"


@pytest.mark.asyncio
async def test_driver_projection_is_identical_in_graph_event_answer_and_sse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    packet, _ = await _route_packet()
    output = await _run_packet(monkeypatch, packet, script_id="chaos3741-sse")
    answer = output.result.answer
    assert answer is not None and answer.graph_assisted is not None
    internal_graph = next(
        event.graph_state
        for event in output.result.events
        if event.graph_state is not None
    )
    assert internal_graph.ranked_drivers == answer.graph_assisted.ranked_drivers

    async def run_with_events(sink):
        for event in output.result.events:
            await sink(event)
        return output.result

    streamed = [
        event
        async for event in stream_orchestrator(
            run_id=output.result.run_id,
            run_with_events=run_with_events,
            cancellation=asyncio.Event(),
        )
    ]
    graph_event = next(
        event for event in streamed if event.event is StreamEventType.GRAPH_STATE
    )
    completed = next(
        event for event in streamed if event.event is StreamEventType.ANSWER_COMPLETED
    )
    assert graph_event.graph_state is not None
    assert completed.answer is not None and completed.answer.graph_assisted is not None
    assert graph_event.graph_state.ranked_drivers == (
        completed.answer.graph_assisted.ranked_drivers
    )
