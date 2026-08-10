"""CHAOS-3678: the production bounded graph query service.

Implements :class:`~dev_health_ops.api.dev.graph_investigation_query.
GraphInvestigationQuery` — Lane B's consumer-side Protocol
(CHAOS-3502/CHAOS-3660). This module owns job → execution-mechanism
selection only. The analytical job and comparison shape are classified by
production's interpreter and arrive on the wire in
``GraphInvestigationRequest.intent_id``/``.cardinality`` — the accepted
CHAOS-3660 determination is that this pair already carries both signals in
production's own vocabulary, so nothing here inspects ``question_text`` to
decide what kind of question was asked. What stays graph-side is
:func:`mechanism_for`: given an already-classified job and shape, which
internal traversal/synthesis mechanism actually answers it.

**Increment 1 scope.** The transport/outcome mapping — ``GraphQueryOutcome``'s
``DISABLED``/``UNAVAILABLE``/``STALE``/``DEADLINE_EXCEEDED``/``CANCELLED``
states — is real, live-tested, and composes the absolute request deadline
with CHAOS-3631's per-operation :class:`~.flags.GraphDeadlines` and
CHAOS-3679's persisted watermark.

**Increment 2 implemented one COMPLETED path:**
``GraphMechanism.SEEDED_SINGULAR_SUBJECT``, via
:func:`~.subject_resolution._resolve_exact_subjects` (EXACT canonical-id/
display-label match only — see that module's own docstring for why this is
narrower than the trial's ``discovery.search_candidates``),
:class:`~.readback.LiveGraphReader` traversal, and
:func:`~.packet_builder.build_production_packet` with ``drivers=None`` (no
driver synthesis — mirrors the trial's own PR1 scope, which never
synthesized drivers either).

**Increment 3 (this revision) adds ``SEEDED_EXPLICIT_COHORT``** (CHAOS-3688),
following the trial's own construction
(``trials/chaos_3619/graph_leg.py``'s ``assemble_packet``) exactly: every
mention resolves the same EXACT-only way, the first resolved candidate
becomes the anchor subject, and :func:`~.cohort.build_cohort` walks two hops
out from it over live edges (:func:`~.subject_resolution._live_cohort_edges`)
to discover peers — never a cohort built directly from "the other named
mentions". ``SUBJECTLESS_COHORT_DISCOVERY`` still returns
``PROVIDER_FAILURE`` with a diagnostic naming the mechanism — it needs
``cohort_discovery.discover_cohort`` wired in, tracked as CHAOS-3689. Every
path is an honest "not yet" where it applies, never a silent wrong answer,
never a crash a caller has to guard against separately.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, cast

from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    QuestionIntentID,
    SourceRequirementState,
)
from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
from dev_health_ops.api.dev.graph_investigation_query import (
    GraphInvestigationQuery,
    GraphInvestigationRequest,
    GraphQueryOutcome,
    GraphQueryResult,
)
from dev_health_ops.api.dev.investigation_contract import ComparisonShape

from .cohort import build_cohort
from .flags import graph_read_enabled
from .packet_builder import (
    ProductionJobContext,
    build_production_packet,
    signer_from_environment,
)
from .readback import GraphReader, LiveGraphReader
from .store import GraphArmStore, StoreUnavailableError
from .subject_resolution import (
    _live_cohort_edges,
    _live_entities,
    _live_entity_labels,
    _resolve_exact_subjects,
)
from .watermark import DEFAULT_STALENESS_TOLERANCE, IndexWatermark

logger = logging.getLogger(__name__)

__all__ = [
    "GraphMechanism",
    "ProductionGraphInvestigationQuery",
    "mechanism_for",
]


class GraphMechanism(StrEnum):
    """Which internal execution path a ``(intent_id, cardinality)`` pair
    selects.

    Job/shape classification stays production's (CHAOS-3660 determination);
    this is the one thing that stays graph-side: given an already-classified
    job and shape, which traversal/synthesis mechanism actually answers it.
    Trial metadata only — never reaches the wire.
    """

    #: One resolved subject, seeded neighbourhood traversal.
    SEEDED_SINGULAR_SUBJECT = "seeded_singular_subject"
    #: Two or more named subjects, seeded from each.
    SEEDED_EXPLICIT_COHORT = "seeded_explicit_cohort"
    #: Zero named subjects, a closed job-specific candidate universe
    #: (CHAOS-3645's second entry mode).
    SUBJECTLESS_COHORT_DISCOVERY = "subjectless_cohort_discovery"
    #: No graph mechanism answers this ``(intent_id, cardinality)`` pair.
    #: Distinct from every other member: selecting it means the caller asked
    #: for organization-wide scope on an intent that is not
    #: ``DISCOVERED_COHORT`` — the unrestricted sweep this arm must never
    #: construct (handoff §4: "unresolved named subjects never widen to
    #: organization scope").
    UNSUPPORTED = "unsupported"


def mechanism_for(
    intent_id: QuestionIntentID, cardinality: Cardinality
) -> GraphMechanism:
    """The fixed ``(intent_id, cardinality) -> mechanism`` table.

    Fixed and total — never question-text inspection, per the CHAOS-3660
    job/shape determination. ``DISCOVERED_COHORT`` requires
    ``Cardinality.ORGANIZATION_WIDE`` by production's own contract invariant
    (``DevQuestionIntent.validate_intent_invariants``), so a request that
    reached this function with that intent and a different cardinality would
    already be invalid upstream; this function does not re-derive that
    invariant; it only reads ``intent_id`` first because that is the more
    specific signal.
    """

    if intent_id is QuestionIntentID.DISCOVERED_COHORT:
        return GraphMechanism.SUBJECTLESS_COHORT_DISCOVERY
    if cardinality is Cardinality.SINGULAR:
        return GraphMechanism.SEEDED_SINGULAR_SUBJECT
    if cardinality is Cardinality.PLURAL_COHORT:
        return GraphMechanism.SEEDED_EXPLICIT_COHORT
    return GraphMechanism.UNSUPPORTED


def _diagnostic(operation: str, detail: str) -> str:
    """A content-safe diagnostic: a fixed template and an operation name.

    Mirrors CHAOS-3631/CHAOS-3676's content-safety bar — never the raw
    question text, never an entity label, never a caller-controlled string
    verbatim. ``detail`` is always a value THIS module chose (an exception
    type name, a fixed phrase), never something read off the request.
    """

    return f"graph query service: {operation}: {detail}"


class _WatermarkStore(Protocol):
    """The two methods ``investigate`` needs from a store.

    A ``Protocol`` rather than ``GraphArmStore`` itself, mirroring
    ``projector.ProjectingStore``: tests exercise the transport/outcome
    mapping against fakes that implement exactly these two methods, and
    structural typing keeps this module honest about how little of the
    store it actually depends on.
    """

    async def read_watermark(self) -> IndexWatermark: ...

    async def close(self) -> None: ...


class _TraversalStore(Protocol):
    """What live traversal and subject resolution need directly from a
    store: partition identity and driver access.

    Kept separate from ``_WatermarkStore`` rather than added to it (each
    Protocol still states exactly one capability), so none of increment 1's
    existing fakes -- which implement only ``read_watermark``/``close`` --
    need to widen to keep passing; this is only reached once a mechanism
    reaches ``SEEDED_SINGULAR_SUBJECT``'s COMPLETED path.

    Production's default ``store_factory`` (``GraphArmStore.for_org``)
    satisfies both this and ``_WatermarkStore`` from the very same real
    instance -- the ``cast`` at the one call site that needs this view says
    exactly that, rather than claiming a structural guarantee this module
    does not have.
    """

    partition: str
    _driver: Any


#: A fixed, closed-vocabulary description -- never ``request.question_text``
#: verbatim. The Protocol's own docstring bars echoing question text into a
#: packet field a consumer renders; keying on ``QuestionIntentID`` instead
#: means this can never emit caller-controlled text.
_JOB_STATEMENT_TEMPLATE = "Investigate {intent} for the resolved subject."


def _job_statement(intent_id: QuestionIntentID) -> str:
    return _JOB_STATEMENT_TEMPLATE.format(intent=intent_id.value)


class ProductionGraphInvestigationQuery:
    """The production implementation of ``GraphInvestigationQuery``.

    Structural conformance to the Protocol (duck typing — the Protocol is
    ``runtime_checkable``-free by design, so no inheritance is required or
    attempted). See the module docstring for what increment 1 does and does
    not do.
    """

    def __init__(
        self,
        *,
        staleness_tolerance: timedelta = DEFAULT_STALENESS_TOLERANCE,
        store_factory: Callable[[str], _WatermarkStore] = GraphArmStore.for_org,
        reader_factory: Callable[[Any], GraphReader] = LiveGraphReader,
        signer_factory: Callable[[], EvidenceReferenceSigner] = signer_from_environment,
    ) -> None:
        self._staleness_tolerance = staleness_tolerance
        # Injected rather than hardcoded to `GraphArmStore.for_org` inline,
        # so tests can exercise every `GraphQueryOutcome` -- DISABLED,
        # UNAVAILABLE, STALE, DEADLINE_EXCEEDED, CANCELLED -- against a fake
        # store without a live FalkorDB for every case; only the
        # live-store positive control needs the real default.
        self._store_factory = store_factory
        # Same rationale as store_factory: SEEDED_SINGULAR_SUBJECT's tests
        # supply a fake GraphReader (a canned neighbourhood() result)
        # instead of needing a live-store-compatible entity/edge fixture
        # for a test that only exercises transport/outcome mapping.
        self._reader_factory = reader_factory
        # Resolved lazily, per call -- see `_complete_seeded_singular_
        # subject` -- never at construction, so increment 1's existing
        # tests (none of which reach that far) are unaffected by whether
        # JWT_SECRET_KEY happens to be set in the test environment.
        self._signer_factory = signer_factory

    async def investigate(self, request: GraphInvestigationRequest) -> GraphQueryResult:
        """Bounded by ``request.deadline`` end to end, never past it.

        Order of checks is deliberate, each one a distinct outcome the
        Protocol requires stay distinct from every other:

        1. the read flag — an intentional, configured state, checked before
           anything that could look like a transport problem;
        2. the absolute deadline, already passed — checked before any I/O,
           so a caller that waited too long to even dispatch the call gets
           ``DEADLINE_EXCEEDED`` rather than a store construction it never
           had time for;
        3. store construction and the watermark read, both bounded by
           whatever remains of the absolute deadline (composed with
           CHAOS-3631's own per-operation ``GraphDeadlines`` bound — the
           tighter of the two wins, since ``asyncio.wait_for`` here is
           layered on top of, not instead of, the store's own bounded
           calls);
        4. freshness, from the CHAOS-3679 persisted watermark — checked
           before mechanism selection, so a stale/unavailable partition
           never reaches a traversal attempt against data that cannot
           answer the question;
        5. mechanism selection and (pending CHAOS-3660) execution.

        ``asyncio.CancelledError`` is caught and converted to
        ``GraphQueryOutcome.CANCELLED`` rather than left to propagate — the
        Protocol's own docstring requires this ("the caller's own
        cancellation token fired... before the call completed" is a
        ``GraphQueryOutcome`` value, not an exception a caller must guard
        against). This is a deliberate cancellation boundary: the seam
        absorbs its own cancellation into a result rather than the request
        task's; nothing above this call needs its own cancellation to be
        interrupted mid-await for this to be correct, because control only
        reaches here already inside this coroutine's own task.
        """

        if not graph_read_enabled():
            return GraphQueryResult(
                outcome=GraphQueryOutcome.DISABLED,
                diagnostic=_diagnostic("read_flag", "graph read is disabled"),
            )

        now = datetime.now(UTC)
        remaining = (request.deadline - now).total_seconds()
        if remaining <= 0:
            return GraphQueryResult(
                outcome=GraphQueryOutcome.DEADLINE_EXCEEDED,
                diagnostic=_diagnostic(
                    "deadline", "the absolute deadline had already passed"
                ),
            )

        store: _WatermarkStore | None = None
        try:
            try:
                store = self._store_factory(request.org_id)
            except StoreUnavailableError as exc:
                return GraphQueryResult(
                    outcome=GraphQueryOutcome.UNAVAILABLE,
                    diagnostic=_diagnostic("store_construction", type(exc).__name__),
                )

            try:
                watermark = await asyncio.wait_for(
                    store.read_watermark(), timeout=remaining
                )
            except TimeoutError:
                return GraphQueryResult(
                    outcome=GraphQueryOutcome.DEADLINE_EXCEEDED,
                    diagnostic=_diagnostic(
                        "read_watermark", "did not complete before the deadline"
                    ),
                )
            except StoreUnavailableError as exc:
                return GraphQueryResult(
                    outcome=GraphQueryOutcome.UNAVAILABLE,
                    diagnostic=_diagnostic("read_watermark", type(exc).__name__),
                )

            freshness = watermark.freshness_for(
                now, tolerance=self._staleness_tolerance
            )
            if freshness is SourceRequirementState.UNAVAILABLE:
                return GraphQueryResult(
                    outcome=GraphQueryOutcome.UNAVAILABLE,
                    diagnostic=_diagnostic(
                        "freshness", "partition has never been projected"
                    ),
                )
            if freshness is SourceRequirementState.AVAILABLE_STALE:
                return GraphQueryResult(
                    outcome=GraphQueryOutcome.STALE,
                    diagnostic=_diagnostic("freshness", watermark.detail_for(now)),
                )

            mechanism = mechanism_for(request.intent_id, request.cardinality)
            if mechanism is GraphMechanism.UNSUPPORTED:
                return GraphQueryResult(
                    outcome=GraphQueryOutcome.PROVIDER_FAILURE,
                    diagnostic=_diagnostic(
                        "mechanism_selection",
                        f"no graph mechanism for intent={request.intent_id.value} "
                        f"cardinality={request.cardinality.value}",
                    ),
                )
            if mechanism is GraphMechanism.SEEDED_SINGULAR_SUBJECT:
                return await self._complete_seeded_singular_subject(
                    request, store, watermark
                )
            if mechanism is GraphMechanism.SEEDED_EXPLICIT_COHORT:
                return await self._complete_seeded_explicit_cohort(
                    request, store, watermark
                )
            # SUBJECTLESS_COHORT_DISCOVERY (CHAOS-3689): still needs
            # cohort_discovery.discover_cohort wired in -- a separate,
            # tracked follow-up, not bundled here. Naming the mechanism
            # rather than a bare "not implemented" is what makes this
            # diagnostic actionable.
            return GraphQueryResult(
                outcome=GraphQueryOutcome.PROVIDER_FAILURE,
                diagnostic=_diagnostic(
                    "execution",
                    f"mechanism {mechanism.value} selected; packet "
                    "assembly for this mechanism is a tracked follow-up",
                ),
            )
        except asyncio.CancelledError:
            return GraphQueryResult(
                outcome=GraphQueryOutcome.CANCELLED,
                diagnostic=_diagnostic(
                    "cancellation", "the calling task was cancelled"
                ),
            )
        finally:
            if store is not None:
                try:
                    await asyncio.wait_for(store.close(), timeout=5.0)
                except Exception:  # noqa: BLE001
                    # Closing best-effort: a close failure must never mask
                    # whichever outcome the try block already decided, and
                    # must never itself propagate as an unhandled exception
                    # from a seam the Protocol requires not to raise for an
                    # ordinary runtime condition.
                    logger.warning(
                        "context-fabric query service: store.close() failed "
                        "for org %r after investigate() completed",
                        request.org_id,
                    )

    async def _complete_seeded_singular_subject(
        self,
        request: GraphInvestigationRequest,
        store: _WatermarkStore,
        watermark: IndexWatermark,
    ) -> GraphQueryResult:
        """The one COMPLETED path this increment implements.

        Resolves the request's first mention (SINGULAR cardinality) by
        EXACT canonical-id/display-label match only -- see
        :mod:`.subject_resolution`'s own module docstring for why this is
        narrower than the trial's ``discovery.search_candidates``. A
        mention that does not resolve still reaches ``COMPLETED``: the
        seam's own outcome axis distinguishes "the call completed" from
        "the investigation found the subject", and an empty seed list
        produces a packet whose own ``outcome`` honestly discloses no
        committed subject -- never a guess, and never reported as a
        transport failure it is not (mirrors the Protocol's own "a
        completed call may still carry a refusal shape" documentation).

        Any unexpected exception during resolution, traversal or packet
        assembly is caught and reported as ``PROVIDER_FAILURE`` rather than
        left to propagate -- the Protocol's docstring reserves an
        uncaught exception for "a bug in the caller", which this is not.
        """

        traversal_store = cast(_TraversalStore, store)
        queries = [mention.normalized_lookup_text for mention in request.mentions[:1]]
        try:
            candidates = await _resolve_exact_subjects(
                traversal_store._driver,
                traversal_store.partition,
                queries,
                request.authorized_entity_ids,
            )
            reader = self._reader_factory(traversal_store)
            readout = await reader.neighbourhood(
                org_id=request.org_id,
                seed_canonical_ids=[
                    candidate.canonical_id for candidate in candidates[:1]
                ],
                authorized_entity_ids=tuple(request.authorized_entity_ids),
            )
            packet = build_production_packet(
                readout=readout,
                job=ProductionJobContext(
                    job_id=f"graph_query_{request.run_id}",
                    intent_id=request.intent_id,
                    run_id=request.run_id,
                    job_statement=_job_statement(request.intent_id),
                    comparison_shape=ComparisonShape.SINGULAR_SUBJECT,
                    window_start=request.window_start,
                    window_end=request.window_end,
                ),
                watermark=watermark,
                signer=self._signer_factory(),
                produced_at=datetime.now(UTC),
                staleness_tolerance=self._staleness_tolerance,
            )
        except Exception as exc:  # noqa: BLE001
            return GraphQueryResult(
                outcome=GraphQueryOutcome.PROVIDER_FAILURE,
                diagnostic=_diagnostic("execution", type(exc).__name__),
            )
        return GraphQueryResult(outcome=GraphQueryOutcome.COMPLETED, packet=packet)

    async def _complete_seeded_explicit_cohort(
        self,
        request: GraphInvestigationRequest,
        store: _WatermarkStore,
        watermark: IndexWatermark,
    ) -> GraphQueryResult:
        """``SEEDED_EXPLICIT_COHORT``'s COMPLETED path (CHAOS-3688).

        Resolves every mention (PLURAL_COHORT cardinality: two or more,
        per ``DevQuestionIntent.validate_intent_invariants``) by the same
        EXACT-only match as the singular path, then follows the trial's
        own construction (``trials/chaos_3619/graph_leg.py``'s
        ``assemble_packet``) exactly: the FIRST resolved candidate becomes
        the anchor subject, and ``cohort.build_cohort`` walks two hops out
        from it -- peers are DISCOVERED via shared team/portfolio/
        initiative/dependency edges, not simply "the other named
        mentions". A caller who named two subjects sharing no anchor gets
        an honestly small or empty cohort from ``build_cohort`` itself,
        never one fabricated from the mentions.

        No mention resolving to an authorized anchor, or ``build_cohort``
        producing fewer than two members or no comparison dimension
        (``IncomparableCohortError``), both reach ``PROVIDER_FAILURE`` with
        a diagnostic naming the mechanism -- an honest, explicit
        degradation. Unlike ``SEEDED_SINGULAR_SUBJECT``, there is no
        "empty but valid" packet shape here: ``ComparisonShape.
        EXPLICIT_COHORT`` requires a real, comparable cohort by contract,
        so a caller must be told plainly rather than handed a packet that
        looks like a completed comparison but silently compares nothing.
        """

        traversal_store = cast(_TraversalStore, store)
        queries = [mention.normalized_lookup_text for mention in request.mentions]
        try:
            candidates = await _resolve_exact_subjects(
                traversal_store._driver,
                traversal_store.partition,
                queries,
                request.authorized_entity_ids,
            )
            if not candidates:
                return GraphQueryResult(
                    outcome=GraphQueryOutcome.PROVIDER_FAILURE,
                    diagnostic=_diagnostic(
                        "execution",
                        f"mechanism {GraphMechanism.SEEDED_EXPLICIT_COHORT.value}: "
                        "no mention resolved to an authorized anchor subject",
                    ),
                )
            subject_id = candidates[0].canonical_id
            edges = await _live_cohort_edges(
                traversal_store._driver, traversal_store.partition
            )
            entities = await _live_entities(
                traversal_store._driver, traversal_store.partition
            )
            cohort = build_cohort(
                subject_id,
                edges,
                _live_entity_labels(entities),
                request.authorized_entity_ids,
            )
            reader = self._reader_factory(traversal_store)
            readout = await reader.neighbourhood(
                org_id=request.org_id,
                seed_canonical_ids=[subject_id],
                authorized_entity_ids=tuple(request.authorized_entity_ids),
            )
            packet = build_production_packet(
                readout=readout,
                job=ProductionJobContext(
                    job_id=f"graph_query_{request.run_id}",
                    intent_id=request.intent_id,
                    run_id=request.run_id,
                    job_statement=_job_statement(request.intent_id),
                    comparison_shape=ComparisonShape.EXPLICIT_COHORT,
                    window_start=request.window_start,
                    window_end=request.window_end,
                ),
                cohort=cohort,
                watermark=watermark,
                signer=self._signer_factory(),
                produced_at=datetime.now(UTC),
                staleness_tolerance=self._staleness_tolerance,
            )
        except Exception as exc:  # noqa: BLE001
            return GraphQueryResult(
                outcome=GraphQueryOutcome.PROVIDER_FAILURE,
                diagnostic=_diagnostic(
                    "execution",
                    f"mechanism {GraphMechanism.SEEDED_EXPLICIT_COHORT.value}: "
                    f"{type(exc).__name__}",
                ),
            )
        return GraphQueryResult(outcome=GraphQueryOutcome.COMPLETED, packet=packet)


if TYPE_CHECKING:
    # Exists only so mypy verifies structural conformance to the Protocol.
    # Inert at runtime -- code under `if TYPE_CHECKING:` never executes, so
    # this never actually constructs an instance -- but mypy still type
    # checks the assignment, and would flag it if `investigate`'s signature
    # ever drifted from the Protocol's. Cheaper and earlier than a test
    # discovering the mismatch at a call site.
    _conforms_to_protocol: GraphInvestigationQuery = ProductionGraphInvestigationQuery()
