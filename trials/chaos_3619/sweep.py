"""The sweep driver: 39 cases, two arms, two legs, one live graph store.

This is the module that actually *runs* the trial. Everything it composes --
the legs, the budget, the classification cascade, the records, the frozen
oracles -- is built and tested elsewhere; what lives here is the wiring, and
the wiring carries three decisions worth stating.

**The live store is required, never optional.** A sweep against a projection
reader would exercise the same call sites and produce a full artifact whose
graph column describes a reader nobody ships. :func:`_require_store` raises
rather than degrading, because a fallback here is indistinguishable from a
measurement in every downstream file.

**Leg B holds the analytical job constant by substituting the CLASSIFIER's
answer, and nothing else.** The native arm derives its question family inside
``native_arm/projection.py`` from ``classify_question_family`` and
``comparison_shape_for``. Leg B replaces exactly those two return values with
the corpus case's declared family and shape, which is what "the production
constrained-model fallback classifier operating perfectly" means as code. No
subject, no evidence, no expected answer crosses -- the substitution happens
downstream of interpretation and upstream of everything the trial scores.

**Leg A hands the graph arm what the production interpreter produced, and
when the interpreter produces nothing the graph arm is not invoked.** The
alternative -- letting the graph leg pick its own family in Leg A -- would
give the graph arm a classification capability the deployed product does not
route to it, and every Leg A graph figure would then be measuring the trial's
own generosity. A case where no family exists is recorded as
``NOT_RUN_PRECONDITION`` with the shared reason named, so it reads as the
as-deployed interpretation ceiling it is rather than as a harness omission.
"""

from __future__ import annotations

import argparse
import asyncio
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dev_health_ops.api.dev import investigation_shadow as seam
from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
from dev_health_ops.api.dev.investigation_contract import (
    ComparisonShape,
    QuestionFamilyID,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_corpus.cases import (
    CASE_REGISTRY,
    authored_cases,
)
from dev_health_ops.api.dev.investigation_plans.plan_documents import (
    CORE_PLANS_BY_INTENT,
)
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.backend import DeterministicEmbedder
from dev_health_ops.context_fabric.graph_arm.packet_builder import IndexWatermark
from dev_health_ops.context_fabric.graph_arm.projection import (
    GraphProjection,
    build_projection,
)
from dev_health_ops.context_fabric.graph_arm.readback import LiveGraphReader
from dev_health_ops.context_fabric.graph_arm.store import GraphArmStore
from dev_health_ops.context_fabric.native_arm.producer import (
    NativeInvestigationPacketProducer,
)

from . import graph_leg, native_leg
from .binding import RunClass, collect_binding
from .budget import DEFAULT_PER_CASE_TIMEOUT_SECONDS, BudgetOutcome
from .dispositions import CaseDisposition
from .legs import LegId, leg_b_channel
from .records import ArmResult, CaseRecord, TrialRecordSet, write_records
from .runner import (
    ArmAttempt,
    arm_result,
    deterministic_produced_at,
    interpretation_of,
    run_id_for,
)

__all__ = ["run_sweep"]

GRAPH_ARM_ID = "graph_assisted_shadow_arm"
NATIVE_ARM_ID = "native"

#: The signing secret for evidence references in this trial. A trial-scoped
#: constant, never a production key: the packets never leave the trial store.
SIGNING_SECRET = "chaos-3619-trial-signing-secret-not-a-real-key"

#: Hop budget for the graph readback. Three, because the H3 spike measured
#: that observations only come into range at three hops from a project seed;
#: a smaller budget would produce packets with no evidence and read as a
#: capability result.
MAX_HOPS = 3

_NATIVE_RUN_NAMESPACE = uuid.UUID("3619b00b-0000-4000-8000-000000000002")


# ---------------------------------------------------------------------------
# Live store
# ---------------------------------------------------------------------------


def _require_store_config() -> Any:
    """The live trial store's config, or a hard failure naming what is absent.

    Deliberately not the pytest gate: this is not a test and must not be able
    to *skip*. A sweep that quietly ran against a projection reader would
    produce a complete artifact describing a reader nobody ships.
    """

    from dev_health_ops.context_fabric.graph_arm.flags import trial_store_config

    config = trial_store_config()
    if config is None:
        raise RuntimeError(
            """no live graph store configured. Set
GRAPH_TRIAL_PROJECT="graph-trial-$(openssl rand -hex 6)", launch with
`docker compose --project-name "$GRAPH_TRIAL_PROJECT" --profile graph-trial up -d graph-trial-store`,
discover its OS-assigned host port with
`docker compose --project-name "$GRAPH_TRIAL_PROJECT" port graph-trial-store 6379`,
and export CONTEXT_FABRIC_GRAPH_STORE_URI for that port. The sweep will not
fall back to a projection reader, because a sweep against a reader nobody ships
is not this trial. After the run, tear it down with
`docker compose --project-name "$GRAPH_TRIAL_PROJECT" down --volumes --remove-orphans`"""
        )
    import socket

    try:
        with socket.create_connection((config.host, config.port), timeout=5):
            pass
    except OSError as exc:
        raise RuntimeError(
            f"the live graph store at {config.host}:{config.port} is not "
            f"reachable ({exc}). Refused rather than degraded"
        ) from exc
    return config


# ---------------------------------------------------------------------------
# Native arm
# ---------------------------------------------------------------------------


class _CapturingProducer:
    """The REAL native producer, plus what it was handed and what it returned.

    Delegates rather than reimplements. The captured context is what makes an
    ``ARM_DECLARED_GAP`` row meaningful: without observing the invocation
    separately from the output, a gap and a harness that never called the arm
    are the same zero.
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


@dataclass(frozen=True, slots=True)
class NativeOutcome:
    attempt: ArmAttempt
    context: Any
    shadow: dict[str, Any] | None


async def _run_native(
    *,
    question: str,
    case_id: str,
    principal_id: str,
    leg: LegId,
    declared_family: QuestionFamilyID,
    declared_shape: ComparisonShape,
) -> NativeOutcome:
    """One native attempt, through the real orchestrator.

    In Leg B the two classifier outputs are substituted for the duration of
    this one run. Patched on the ``native_arm.projection`` module rather than
    at the definition site so the substitution cannot leak into the graph leg
    or into anything else in the process.
    """

    from tests._chaos_3292_preflight import run_preflight_orchestrator
    from tests._chaos_3295_plan_executor import (
        FakePlanExecutorRuntime,
        InvestigationRecorder,
        executor_for,
    )

    seeding = native_leg.catalog_entities(principal_id)
    producer = _CapturingProducer()

    from dev_health_ops.context_fabric.native_arm import projection as native_projection

    original_family = native_projection.classify_question_family
    original_shape = native_projection.comparison_shape_for
    if leg is LegId.JOB_HELD_CONSTANT:
        channel = leg_b_channel(
            question_family=declared_family, comparison_shape=declared_shape
        )
        native_projection.classify_question_family = lambda **_kwargs: (
            channel.question_family
        )
        native_projection.comparison_shape_for = lambda **_kwargs: (
            channel.comparison_shape
        )
    try:
        await run_preflight_orchestrator(
            question=question,
            entities=list(seeding.entities),
            org_id=world.PRINCIPALS[principal_id].tenant_id,
            run_id=str(uuid.uuid5(_NATIVE_RUN_NAMESPACE, f"{leg.value}:{case_id}")),
            plan_registry=CORE_PLANS_BY_INTENT,
            plan_executor=executor_for(FakePlanExecutorRuntime()),
            recorder_factory=InvestigationRecorder,
            investigation_shadow=seam.InvestigationShadow(enabled=True),
            investigation_packet_producer=producer,
        )
    finally:
        native_projection.classify_question_family = original_family
        native_projection.comparison_shape_for = original_shape

    context = producer.contexts[0] if producer.contexts else None
    payload = producer.payloads[0] if producer.payloads else None
    shadow = None
    if payload is not None and context is not None:
        record = seam.InvestigationShadow(enabled=True).evaluate(
            payload=payload,
            run_id=context.run_id,
            organization_id=context.organization_id,
            canonical_evidence=context.canonical_evidence,
        )
        shadow = seam.shadow_record_payload(record)
    return NativeOutcome(
        attempt=ArmAttempt(invoked=context is not None, payload=payload),
        context=context,
        shadow=shadow,
    )


# ---------------------------------------------------------------------------
# Graph arm
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GraphOutcome:
    attempt: ArmAttempt | None
    shadow: dict[str, Any] | None
    seeds: tuple[str, ...]
    authorization_filtered_count: int


async def _run_graph(
    *,
    store: GraphArmStore,
    projection: GraphProjection,
    question: str,
    case_id: str,
    principal_id: str,
    leg: LegId,
    question_family: QuestionFamilyID | None,
    comparison_shape: ComparisonShape | None,
    produced_at: datetime,
) -> GraphOutcome:
    """One graph attempt: discover, read back live, assemble, submit.

    ``question_family``/``comparison_shape`` are ``None`` in Leg A whenever
    the production interpreter derived none. The arm is then NOT invoked: it
    is handed no analytical job in the deployed routing either, and inventing
    one here would score the trial's generosity rather than the product.
    """

    grant = frozenset(adapter.authorized_entity_ids_for(principal_id))
    if question_family is None or comparison_shape is None:
        # No analytical job, so no entry mode is selected and neither runs.
        # ``discover_subjects`` is not called either: its withheld count is a
        # disclosure about an investigation, and there was none.
        return GraphOutcome(
            attempt=None,
            shadow=None,
            seeds=(),
            authorization_filtered_count=0,
        )

    watermark = IndexWatermark(
        indexed_through=world.WINDOW_END,
        projected_at=world.WINDOW_END,
        records_indexed=len(projection.nodes),
    )
    reader = LiveGraphReader(store)

    if comparison_shape is ComparisonShape.DISCOVERED_COHORT:
        # CHAOS-3645, the second entry mode. The branch is chosen by the
        # analytical job's SHAPE, not by whether extraction happened to come
        # up empty. A mode that only ran as a fallback from failed extraction
        # would still be an extraction-dependent capability, and these cases
        # refuse precisely because no extractor can reach a question that
        # contains no subject.
        return await _run_graph_cohort(
            reader=reader,
            store=store,
            projection=projection,
            question=question,
            case_id=case_id,
            grant=grant,
            leg=leg,
            question_family=question_family,
            comparison_shape=comparison_shape,
            watermark=watermark,
            produced_at=produced_at,
        )

    discovery = graph_leg.discover_subjects(
        question=question,
        projection=projection,
        authorized_entity_ids=grant,
    )
    seeds = tuple(graph_leg.seeds_from(discovery))
    if not seeds:
        return GraphOutcome(
            attempt=ArmAttempt(
                invoked=True,
                payload=None,
                refusal="no authorized subject resolved from the question",
            ),
            shadow=None,
            seeds=(),
            authorization_filtered_count=discovery.authorization_filtered_count,
        )

    readout = await reader.neighbourhood(
        org_id=store.org_id,
        seed_canonical_ids=list(seeds),
        authorized_entity_ids=sorted(grant),
        max_hops=MAX_HOPS,
    )
    outcome = graph_leg.assemble_packet(
        readout=readout,
        projection=projection,
        discovery=discovery,
        question_family=question_family,
        comparison_shape=comparison_shape,
        job_statement=question,
        window_start=world.WINDOW_START,
        window_end=world.WINDOW_END,
        run_id=run_id_for(leg, case_id),
        watermark=watermark,
        signer=EvidenceReferenceSigner(SIGNING_SECRET),
        produced_at=produced_at,
        as_of=world.WINDOW_END,
        authorized_entity_ids=grant,
    )
    return _graph_outcome(
        outcome=outcome,
        store=store,
        leg=leg,
        case_id=case_id,
        seeds=seeds,
    )


async def _run_graph_cohort(
    *,
    reader: LiveGraphReader,
    store: GraphArmStore,
    projection: GraphProjection,
    question: str,
    case_id: str,
    grant: frozenset[str],
    leg: LegId,
    question_family: QuestionFamilyID,
    comparison_shape: ComparisonShape,
    watermark: IndexWatermark,
    produced_at: datetime,
) -> GraphOutcome:
    """One subjectless attempt: enumerate, read back live, assemble, submit.

    ``question`` reaches this function for ONE purpose -- it is the packet's
    ``job_statement``, the human sentence a reader needs in order to know what
    the packet was answering. It is not parsed, matched or extracted from
    here, and the cohort is already fully determined before it is used.
    """

    from dev_health_ops.context_fabric.graph_arm.cohort_discovery import (
        UnsupportedCohortFamilyError,
    )

    try:
        cohort = graph_leg.discover_cohort_for(
            question_family=question_family,
            projection=projection,
            authorized_entity_ids=grant,
            as_of=world.WINDOW_END,
        )
    except UnsupportedCohortFamilyError as raised:
        # A named capability boundary, recorded as a refusal rather than as a
        # gap: the arm decided, and it must be visible that it decided.
        return GraphOutcome(
            attempt=ArmAttempt(
                invoked=True,
                payload=None,
                refusal=f"UnsupportedCohortFamilyError: {raised}",
            ),
            shadow=None,
            seeds=(),
            authorization_filtered_count=0,
        )

    seeds = tuple(graph_leg.cohort_seeds_from(cohort))
    if not seeds:
        return GraphOutcome(
            attempt=ArmAttempt(
                invoked=True,
                payload=None,
                refusal=(
                    "no authorized cohort could be enumerated for this "
                    "question family; the candidate universe was "
                    f"{cohort.universe_size} entities and none of them shared "
                    "a basis the graph can state with another"
                ),
            ),
            shadow=None,
            seeds=(),
            authorization_filtered_count=cohort.authorization_filtered_count,
        )

    readout = await reader.neighbourhood(
        org_id=store.org_id,
        seed_canonical_ids=list(seeds),
        authorized_entity_ids=sorted(grant),
        max_hops=MAX_HOPS,
    )
    outcome = graph_leg.assemble_cohort_packet(
        readout=readout,
        cohort=cohort,
        question_family=question_family,
        comparison_shape=comparison_shape,
        job_statement=question,
        window_start=world.WINDOW_START,
        window_end=world.WINDOW_END,
        run_id=run_id_for(leg, case_id),
        watermark=watermark,
        signer=EvidenceReferenceSigner(SIGNING_SECRET),
        produced_at=produced_at,
    )
    return _graph_outcome(
        outcome=outcome,
        store=store,
        leg=leg,
        case_id=case_id,
        seeds=seeds,
    )


def _graph_outcome(
    *,
    outcome: graph_leg.GraphPacketOutcome,
    store: GraphArmStore,
    leg: LegId,
    case_id: str,
    seeds: tuple[str, ...],
) -> GraphOutcome:
    """Submit an emitted packet to the real seam and record the row.

    Shared by both entry modes deliberately. The seam call is the part the
    trial is measuring; a second copy of it beside the second entry mode is
    how one mode ends up quietly skipping a submission the other makes.
    """

    shadow = None
    if outcome.payload is not None:
        record = seam.InvestigationShadow(enabled=True).evaluate(
            payload=outcome.payload,
            run_id=run_id_for(leg, case_id),
            organization_id=store.org_id,
            canonical_evidence=(),
        )
        shadow = seam.shadow_record_payload(record)
    return GraphOutcome(
        attempt=ArmAttempt(
            invoked=True,
            payload=(dict(outcome.payload) if outcome.payload is not None else None),
            refusal=outcome.refusal,
            fault=outcome.fault,
        ),
        shadow=shadow,
        seeds=seeds,
        authorization_filtered_count=outcome.authorization_filtered_count,
    )


# ---------------------------------------------------------------------------
# The per-case bound
# ---------------------------------------------------------------------------


async def _bounded(
    coroutine: Any, *, limit_seconds: float = DEFAULT_PER_CASE_TIMEOUT_SECONDS
) -> BudgetOutcome:
    """Await one arm attempt under the per-case budget, in THIS loop.

    ``budget.hard_bound`` runs its call on a worker thread with its own event
    loop, which is the right shape for a wedged synchronous producer and the
    wrong shape here: the live ``GraphArmStore`` connection is created on --
    and bound to -- the sweep's loop, so a readback issued from a second
    loop fails on loop affinity rather than measuring anything. The bound
    that matters is preserved: the runner stops waiting at the deadline, the
    case is recorded NOT RUN, and it is never retried.

    The remaining difference from ``hard_bound`` is disclosed rather than
    papered over: cancellation here is cooperative, so a call wedged inside
    an uncancellable C-level read would still stall. CHAOS-3631 (FalkorDB has
    no socket timeout) owns that residual.
    """

    import time

    started = time.monotonic()
    try:
        value = await asyncio.wait_for(coroutine, timeout=limit_seconds)
    except TimeoutError:
        return BudgetOutcome(
            elapsed_seconds=time.monotonic() - started,
            limit_seconds=limit_seconds,
            exceeded=True,
        )
    except Exception as raised:  # noqa: BLE001 - recorded as a fault, not swallowed
        return BudgetOutcome(
            elapsed_seconds=time.monotonic() - started,
            limit_seconds=limit_seconds,
            exceeded=False,
            fault=raised,
        )
    return BudgetOutcome(
        elapsed_seconds=time.monotonic() - started,
        limit_seconds=limit_seconds,
        exceeded=False,
        value=value,
    )


# ---------------------------------------------------------------------------
# The sweep
# ---------------------------------------------------------------------------


def _no_job_result(*, arm_id: str, leg: LegId) -> ArmResult:
    """The as-deployed interpretation ceiling, recorded as itself.

    Not ``ARM_DECLARED_GAP``: the arm made no statement, because it was
    handed no analytical job. Not a harness artefact either, and the detail
    says which of the two it is so a reader cannot take it for the other.
    """

    return ArmResult(
        arm_id=arm_id,
        disposition=CaseDisposition.NOT_RUN_PRECONDITION.value,
        detail=(
            "the production interpreter derived no representable question "
            "family for this question, so no analytical job existed to hand "
            "either arm. This is the as-deployed interpretation ceiling that "
            "Leg B removes -- not an arm result and not a harness omission"
        ),
        latency_ms=0,
        packet_emitted=False,
    )


async def _sweep_leg(
    *,
    leg: LegId,
    store: GraphArmStore,
    projection: GraphProjection,
    produced_at: datetime,
    cases: Sequence[Any],
) -> list[CaseRecord]:
    records: list[CaseRecord] = []
    for case in cases:
        native_budget = await _bounded(
            _run_native(
                question=case.question,
                case_id=case.case_id,
                principal_id=case.principal_id,
                leg=leg,
                declared_family=case.question_family,
                declared_shape=case.comparison_shape,
            )
        )
        native = (
            native_budget.value
            if isinstance(native_budget.value, NativeOutcome)
            else None
        )
        interpretation = None
        derived_family: QuestionFamilyID | None = None
        derived_shape: ComparisonShape | None = None
        if native is not None and native.context is not None:
            from dev_health_ops.context_fabric.native_arm.capabilities import (
                classify_question_family,
            )
            from dev_health_ops.context_fabric.native_arm.projection import (
                comparison_shape_for,
            )

            interp = native.context.interpretation
            derived_shape = comparison_shape_for(
                cardinality=interp.intent.cardinality,
                has_unresolved_mentions=bool(
                    getattr(interp, "unresolved_mentions", ())
                ),
            )
            derived_family = classify_question_family(
                intent_id=interp.intent.intent_id, shape=derived_shape
            )
            interpretation = interpretation_of(interp, derived_family)

        if leg is LegId.JOB_HELD_CONSTANT:
            graph_family: QuestionFamilyID | None = case.question_family
            graph_shape: ComparisonShape | None = case.comparison_shape
        else:
            graph_family = derived_family
            graph_shape = derived_shape if derived_family is not None else None

        graph_budget = await _bounded(
            _run_graph(
                store=store,
                projection=projection,
                question=case.question,
                case_id=case.case_id,
                principal_id=case.principal_id,
                leg=leg,
                question_family=graph_family,
                comparison_shape=graph_shape,
                produced_at=produced_at,
            )
        )
        graph = (
            graph_budget.value if isinstance(graph_budget.value, GraphOutcome) else None
        )

        native_row = arm_result(
            arm_id=NATIVE_ARM_ID,
            leg=leg,
            case_id=case.case_id,
            attempt=native.attempt if native is not None else None,
            budget=native_budget,
            shadow=native.shadow if native is not None else None,
            interpretation=interpretation,
        )
        if graph is not None and graph.attempt is None:
            # The arm was deliberately not invoked: no analytical job existed.
            graph_row = _no_job_result(arm_id=GRAPH_ARM_ID, leg=leg)
        else:
            graph_row = arm_result(
                arm_id=GRAPH_ARM_ID,
                leg=leg,
                case_id=case.case_id,
                attempt=graph.attempt if graph is not None else None,
                budget=graph_budget,
                shadow=graph.shadow if graph is not None else None,
            )
        records.append(
            CaseRecord(
                case_id=case.case_id,
                question=case.question,
                question_family=case.question_family.value,
                corpus_family=case.corpus_family.value
                if hasattr(case.corpus_family, "value")
                else str(case.corpus_family),
                comparison_shape=case.comparison_shape.value,
                variant_kind=case.variant_kind.value
                if hasattr(case.variant_kind, "value")
                else str(case.variant_kind),
                expected_answer=case.expected_answer.value
                if hasattr(case.expected_answer, "value")
                else str(case.expected_answer),
                principal_id=case.principal_id,
                organization_id=world.PRINCIPALS[case.principal_id].tenant_id,
                declared_dimension_ids=tuple(
                    d.value if hasattr(d, "value") else str(d)
                    for d in case.scoring_dimension_ids
                ),
                leg=leg.value,
                arms=(native_row, graph_row),
            )
        )
    return records


async def run_sweep(
    out_path: Path, *, only_comparison_shape: ComparisonShape | None = None
) -> TrialRecordSet:
    """The whole sweep: ingest once, then both legs over all authored cases.

    ``only_comparison_shape`` re-runs one slice of the corpus -- CHAOS-3645
    re-runs the ``discovered_cohort`` families and nothing else. **A partial
    sweep is disclosed in the binding's notes, not left for a reader to
    notice**, because a records file holding fourteen cases is
    indistinguishable by inspection from a full sweep that lost twenty-five,
    and the notes travel with the file into the report. The cases that were
    NOT run are named individually rather than counted: "25 cases were
    skipped" tells a reader they are missing something and not what.

    The corpus, the oracles and both arms are untouched by the filter. It
    selects which authored cases run and changes nothing about how they run,
    which is what makes the resulting rows comparable with the full sweep's.

    **The partition is the corpus's real tenant, and it has to be.** The
    obvious hygiene move -- a throwaway partition id, so two lanes sharing
    the trial container cannot read each other's nodes -- was measured and
    rejected: the readout carries the partition id into the packet as its
    organization, the corpus principal belongs to ``org_helio``, and the
    authorization oracle then reports a tenant mismatch on every scored graph
    row. That is a MUST_BE_ZERO safety dimension failing 12 times for a
    reason that has nothing to do with the arm. Addressing hygiene is not
    worth an unsound safety column, so the sweep writes ``org_helio`` and
    purges the partition on both sides of the run instead.
    """

    config = _require_store_config()
    org_id = world.ORG_HELIO
    projection = build_projection(adapter.corpus_batch(world.ORG_HELIO))
    # Derived from EVERY authored case, filtered or not. The emission
    # timestamp is a property of the corpus rather than of the slice, and a
    # partial re-run whose packets carry a different clock would diff against
    # the full sweep in every row.
    produced_at = deterministic_produced_at(list(authored_cases()))

    selected = [
        case
        for case in authored_cases()
        if only_comparison_shape is None
        or case.comparison_shape is only_comparison_shape
    ]
    if not selected:
        raise RuntimeError(
            f"no authored case has comparison shape {only_comparison_shape}; "
            "refusing to write an empty artifact, which would read as a sweep "
            "that measured nothing and found nothing wrong"
        )
    skipped = tuple(
        sorted(
            case.case_id
            for case in authored_cases()
            if case not in selected  # noqa: PLR6201 - CorpusCase is unhashable
        )
    )

    store = GraphArmStore.for_org(org_id, config=config)
    cases: list[CaseRecord] = []
    try:
        # Purge BEFORE writing as well as after: a partition left behind by an
        # interrupted earlier run would otherwise be read as this run's world.
        await store.purge_org()
        await store.build_indices()
        await store.write_projection(projection)
        for leg in (LegId.AS_DEPLOYED, LegId.JOB_HELD_CONSTANT):
            cases.extend(
                await _sweep_leg(
                    leg=leg,
                    store=store,
                    projection=projection,
                    produced_at=produced_at,
                    cases=selected,
                )
            )
    finally:
        try:
            await store.purge_org()
        finally:
            await store.close()

    authored_ids = {case.case_id for case in authored_cases()}
    non_authored = tuple(
        {
            "case_id": case.case_id,
            "disposition": (
                case.disposition.value
                if hasattr(case.disposition, "value")
                else str(case.disposition)
            ),
            "reason": case.disposition_reason,
        }
        for case in CASE_REGISTRY.values()
        if case.case_id not in authored_ids
    )

    notes: tuple[str, ...] = ()
    if only_comparison_shape is not None:
        notes = (
            f"PARTIAL SWEEP. Only cases whose declared comparison shape is "
            f"{only_comparison_shape.value!r} were run: "
            f"{len(selected)} of {len(list(authored_cases()))} authored cases, "
            "both legs. This artifact is a re-run of one slice and is NOT a "
            "replacement for the full sweep; any figure quoted from it must "
            "name the slice.",
            "Cases NOT run in this artifact, named rather than counted so a "
            f"reader can tell absence from omission: {', '.join(skipped)}",
        )

    records = TrialRecordSet(
        schema_version="chaos_3619_trial_results.v1",
        binding=collect_binding(
            run_class=RunClass.MEASURED,
            per_case_timeout_seconds=DEFAULT_PER_CASE_TIMEOUT_SECONDS,
            trial_store_backend=f"falkordb ({config.host}:{config.port})",
            graph_embedder_model_id=DeterministicEmbedder().model_id,
            notes=notes,
        ),
        cases=tuple(cases),
        non_authored=non_authored,
    )
    write_records(records, out_path)
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the CHAOS-3619 sweep")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--only-comparison-shape",
        choices=[shape.value for shape in ComparisonShape],
        default=None,
        help=(
            "re-run only the authored cases declaring this comparison shape. "
            "The resulting artifact records the restriction in its binding "
            "notes and names every case it did not run"
        ),
    )
    args = parser.parse_args()
    only = (
        ComparisonShape(args.only_comparison_shape)
        if args.only_comparison_shape
        else None
    )
    records = asyncio.run(run_sweep(args.out, only_comparison_shape=only))
    print(f"wrote {len(records.cases)} case rows to {args.out}")


if __name__ == "__main__":
    main()
