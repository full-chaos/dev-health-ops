"""CHAOS-3646: the arm run, unstubbed.

The real projection, the real reader, the real grant, the real emitter. This
mirrors ``tests/context_fabric/chaos_3620_spine.py`` -- deliberately, because
that composition is the one CHAOS-3620 proved safe -- but lives under
``trials/`` so the sweep does not import a test module.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from functools import cache

from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
from dev_health_ops.api.dev.investigation_contract import (
    AskDevInvestigationPacket,
    ComparisonShape,
    QuestionFamilyID,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.drivers import (
    DriverFinding,
    discover_drivers,
)
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    JobContext,
    TrialContext,
    build_packet,
)
from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection
from dev_health_ops.context_fabric.graph_arm.readback import (
    InvestigationReadout,
    ProjectionGraphReader,
)
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

#: The ARM's signer. Distinct from the canonical service's, and that is the
#: point: with admission on, nothing this key signs reaches the packet.
ARM_SIGNING_SECRET = "chaos-3646-arm-signing-secret-not-a-real-key"
PRODUCED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
RUN_ID = "3646a5af-0000-4000-8000-000000003646"


@cache
def projection() -> GraphProjection:
    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


@cache
def grant_for(principal_id: str = world.PRINCIPAL_ANALYST) -> frozenset[str]:
    return adapter.authorized_entity_ids_for(principal_id)


def readout(
    seeds: tuple[str, ...], principal_id: str = world.PRINCIPAL_ANALYST
) -> InvestigationReadout:
    graph = projection()
    return asyncio.run(
        ProjectionGraphReader(graph).neighbourhood(
            org_id=graph.org_id,
            seed_canonical_ids=list(seeds),
            authorized_entity_ids=sorted(grant_for(principal_id)),
            max_hops=2,
        )
    )


def packet(
    readout_: InvestigationReadout,
    case,
    seeds: tuple[str, ...],
    *,
    admitted_evidence=None,
    drivers: bool = False,
) -> AskDevInvestigationPacket:
    findings: tuple[DriverFinding, ...] = ()
    if drivers and seeds:
        discovered, _truncated = discover_drivers(
            readout_, seeds[0], as_of=world.TRIAL_NOW
        )
        findings = tuple(discovered)
    job = JobContext(
        job_id=f"job_3646_{case.case_id.lower()}",
        question_family=QuestionFamilyID(str(case.question_family)),
        job_statement=case.question,
        comparison_shape=ComparisonShape(str(case.comparison_shape)),
        window_start=world.WINDOW_START,
        window_end=world.WINDOW_END,
    )
    return build_packet(
        readout=readout_,
        job=job,
        watermark=IndexWatermark(
            indexed_through=world.WINDOW_END,
            projected_at=world.WINDOW_END,
            records_indexed=48,
        ),
        signer=EvidenceReferenceSigner(ARM_SIGNING_SECRET),
        trial=TrialContext(run_id=RUN_ID, corpus_version=adapter.CORPUS_VERSION),
        produced_at=PRODUCED_AT,
        drivers=findings,
        admitted_evidence=admitted_evidence,
    )
