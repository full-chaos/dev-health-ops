"""CHAOS-3618: the one shadow seam both trial arms hand their packet to.

Sits beside :mod:`.qua_shadow` and copies its posture deliberately, because
that posture is already proven: injected rather than constructed, run
strictly after the live decision is complete, total exception containment,
and a return value nothing downstream reads. Flipping this seam on changes
no user-visible output, and the inertness test proves it rather than
asserting it.

**Arm-neutral by construction.** This module imports neither arm. It takes
an already-built ``ask_dev_investigation_packet.v1`` payload and reads arm
identity off ``versions.trial``, which is where CHAOS-3615 put it precisely
so arm identity stays evaluation metadata rather than product truth. There
is no code path here that can behave differently for the native arm than
for the graph arm — not by convention, but because the seam cannot tell
which one it is holding except by reading a field it only ever records.

**No live model call.** The seam builds its shadow frame deterministically.
The narrative provider is not a parameter of any function in this module,
so provider-authored prose is unreachable from here in the same
signature-level way ``orchestrator._server_grounded_answer`` makes rejected
model prose unreachable. A budgeted live synthesis is a separate, explicit
piece of work and is deliberately not scaffolded here.

**Canonical services keep their authority.** A packet may reference what
canonical services measured; it may not introduce measurements of its own.
:func:`canonical_bypass_offenders` is the enforcement point: every evidence
handle a packet cites must already appear in the run's own canonical
evidence set. A packet citing a handle no canonical service minted is
rejected outright, because that is the shape a graph arm's fabricated
measurement would arrive in.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime
from enum import Enum, StrEnum
from typing import Any, Protocol, Self

from pydantic import ValidationError

from .contracts import DevScopeResolution
from .contracts_v2.embedded import DevEvidenceRefV2
from .investigation_contract import (
    INVESTIGATION_CONTRACT_MODELS,
    AskDevInvestigationPacket,
)

logger = logging.getLogger(__name__)

__all__ = [
    "INVESTIGATION_SHADOW_FLAG",
    "INVESTIGATION_SHADOW_RECORD_SCHEMA_VERSION",
    "FinishedRunContext",
    "InvestigationPacketProducer",
    "InvestigationShadow",
    "InvestigationShadowRecord",
    "InvestigationShadowStatus",
    "RESERVED_LOG_RECORD_ATTRS",
    "canonical_bypass_offenders",
    "run_window",
    "shadow_record_payload",
    "shadow_enabled",
]

#: Matches the CHAOS-3617 convention (``CONTEXT_FABRIC_*``, ``== "1"``), so
#: "unset" is off and every other value is off too.
INVESTIGATION_SHADOW_FLAG = "CONTEXT_FABRIC_SHADOW_SYNTHESIS_ENABLED"

#: The version of the *record* shape, distinct from the packet's own schema
#: version. CHAOS-3618 lands the seam without a durable table, so the
#: comparison rows exist only as log lines until CHAOS-3619 either reads
#: this shape or lands the table -- and a log shape a consumer parses is a
#: contract whether or not anybody calls it one. Versioning it now means the
#: trial can assert which shape it read instead of guessing.
INVESTIGATION_SHADOW_RECORD_SCHEMA_VERSION = "investigation_shadow_record.v1"

_PACKET_MODEL = INVESTIGATION_CONTRACT_MODELS["ask_dev_investigation_packet.v1"]


def shadow_enabled(environ: Mapping[str, str]) -> bool:
    """Whether the shadow seam is switched on for this process."""

    return environ.get(INVESTIGATION_SHADOW_FLAG) == "1"


def run_window(
    resolution: DevScopeResolution | None,
) -> tuple[datetime, datetime] | None:
    """The bounded window the run actually executed under, or ``None``.

    Arm-neutral, like everything else in this module: it reads the run's own
    server-owned scope decision and nothing else. ``ScopeResolutionService``
    pins one ``ResolvedTimeRange`` per request and stamps it onto BOTH
    ``requested_scope`` and ``resolved_scope``, so either carries the same
    interval -- ``resolved_scope`` is preferred only because it is the scope
    the plan executor genuinely ran against.

    ``None`` when the run ended before scope resolution completed, and that
    is the whole reason this returns an option rather than a default. An arm
    handed a manufactured window would put a bounded time context on the
    packet that no query ever used, and every temporal claim in the packet
    rests on that context. A run with no window is a run no arm may project.
    """

    if resolution is None:
        return None
    scope = resolution.resolved_scope or resolution.requested_scope
    # ``DevTimeRange.validate_order`` already rejects a degenerate interval,
    # so a window that exists here is a real one by construction.
    return (scope.time_range.start, scope.time_range.end)


@dataclass(frozen=True, slots=True)
class FinishedRunContext:
    """What an arm is given to build a packet from: one completed run.

    Arm-neutral by shape as well as by name -- every field is a server-owned
    artefact of the run the orchestrator just finished, and there is nothing
    here that only one arm could use. The orchestrator hands this to
    whatever producer it was configured with and never imports an arm.

    ``canonical_evidence`` is the load-bearing field, and it is populated
    from the run's own frame -- which the server built from what the
    evidence service returned. **It must never be populated from the packet
    an arm produced.** ``canonical_bypass_offenders`` digests each cited
    record against this sequence, so feeding an arm's own evidence back in
    would compare a value to itself: a check that cannot fail, wearing the
    appearance of one. See ``test_canonical_evidence_comes_from_the_run_not
    _the_packet``.

    ``window_start``/``window_end`` are the run's own bounded window, from
    :func:`run_window`, and they are ``datetime | None`` rather than
    defaulted for the reason that function's docstring gives: an arm handed
    a manufactured window would stamp the packet with a time context no
    query used. ``None`` is a fact about the run, and an arm's only honest
    response to it is to refuse to project.
    """

    run_id: str
    organization_id: str
    frame: Any
    investigation_result: Any
    interpretation: Any
    ledger: Any
    subject_set: Any
    committed_subject: Any
    window_start: datetime | None
    window_end: datetime | None
    canonical_evidence: tuple[DevEvidenceRefV2, ...]


class InvestigationPacketProducer(Protocol):
    """One trial arm, seen from the orchestrator.

    Returns a packet payload, or ``None`` when the arm has nothing to emit
    for this run. Returning ``None`` is a normal outcome, not a failure --
    the native arm reports several kinds of run as unprojectable by design.
    """

    def build_packet(
        self, run: FinishedRunContext
    ) -> Mapping[str, Any] | None:  # pragma: no cover - protocol
        ...


class InvestigationShadowStatus(StrEnum):
    """How one shadow evaluation ended.

    Every member is a *recorded* outcome. There is no "raised" member,
    because the seam never propagates an exception to its caller — see
    :meth:`InvestigationShadow.evaluate`.
    """

    #: A valid packet was accepted and a shadow frame assembled.
    RECORDED = "recorded"
    #: The seam was asked to evaluate while switched off.
    #:
    #: **Unreachable from the production wiring, and that is not a defect
    #: to fix -- it is a claim to state accurately.** The orchestrator
    #: returns before calling :meth:`InvestigationShadow.evaluate` when the
    #: seam is disabled, and ``native_shadow_wiring`` only ever constructs
    #: ``InvestigationShadow(enabled=True)``, so "off" is the ABSENCE of a
    #: collaborator rather than a disabled one. This member is reachable
    #: only by calling ``evaluate`` directly on a disabled seam, which the
    #: seam's own tests do.
    #:
    #: An earlier version of this comment leaned on ``SKIPPED_DISABLED`` to
    #: justify ``PRODUCER_GAP`` by contrast -- "a disabled seam records, so
    #: an unprojectable run should too". The independent verifier caught
    #: that the contrast describes a state production never reaches. The
    #: justification below is the one that survives.
    SKIPPED_DISABLED = "skipped_disabled"
    #: The arm ran and reported this run as unprojectable.
    #:
    #: Added by the codex review. The defect was simple and did not need a
    #: contrast to be real: a producer returning ``None`` produced NO record
    #: at all, so a run the arm could not express was indistinguishable
    #: from a run the seam never saw -- and those are the runs the trial
    #: most needs to count, because "how often can the baseline express its
    #: own run" is one of the numbers the comparison turns on.
    #:
    #: The seam stays arm-neutral about WHY: the reason is the arm's own
    #: vocabulary, and it lives in the arm's log line, not in a field this
    #: seam would have to understand.
    PRODUCER_GAP = "producer_gap"
    #: The payload failed the canonical validator.
    PACKET_INVALID = "packet_invalid"
    #: The packet cited evidence no canonical service minted for this run.
    CANONICAL_BYPASS_REJECTED = "canonical_bypass_rejected"
    #: The seam itself faulted. The live run is unaffected.
    SEAM_FAULT = "seam_fault"


@dataclass(frozen=True, slots=True)
class InvestigationShadowRecord:
    """What the trial compares, and the lineage that makes it comparable.

    ``arm_id``, ``packet_schema_version`` and ``evidence_handles`` are the
    three things a later differential needs and cannot reconstruct: which
    arm produced the packet, which contract version it was written against,
    and exactly which evidence it rested on. All three are read off the
    packet rather than supplied by the caller, so a caller cannot mislabel
    another arm's packet as its own.
    """

    run_id: str
    status: InvestigationShadowStatus
    arm_id: str | None
    packet_schema_version: str | None
    projection_version: str | None
    packet_id: str | None
    outcome: str | None
    evidence_handles: tuple[str, ...]
    latency_ms: int
    detail: str | None = None
    #: Present only for :attr:`InvestigationShadowStatus.RECORDED`.
    frame_facts: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def producer_gap(cls, *, run_id: str, latency_ms: int) -> Self:
        """The record for a run its arm could not express.

        A constructor rather than a caller-assembled record, so the shape
        cannot vary between the two arms: every field a packet would have
        supplied is genuinely absent here, and nothing may invent one.
        """

        return cls(
            run_id=run_id,
            status=InvestigationShadowStatus.PRODUCER_GAP,
            arm_id=None,
            packet_schema_version=None,
            projection_version=None,
            packet_id=None,
            outcome=None,
            evidence_handles=(),
            latency_ms=latency_ms,
            detail="the arm reported this run as unprojectable",
        )

    def __post_init__(self) -> None:
        recorded = self.status is InvestigationShadowStatus.RECORDED
        if recorded and not self.arm_id:
            raise ValueError(
                "a recorded shadow evaluation must name the arm that produced "
                "the packet; an unattributed record is not comparable"
            )
        if recorded and not self.packet_schema_version:
            raise ValueError(
                "a recorded shadow evaluation must carry the packet's schema "
                "version, or a later differential cannot know what it compared"
            )


def shadow_record_payload(record: InvestigationShadowRecord) -> dict[str, Any]:
    """One comparison record as a versioned, machine-parseable mapping.

    CHAOS-3618 lands no durable table (see ``PersistenceRunRecorder``), so
    until CHAOS-3619 lands one this mapping IS the comparison artefact --
    which makes it a contract, and an unversioned contract read by a later
    consumer is a silent drift surface. Hence the schema version.

    Every field is derived by iterating the dataclass's own ``fields()``
    rather than listed by hand. A hand-written list would keep parsing
    cleanly while quietly dropping any field added later, and a trial that
    reads a record missing a field it needs cannot tell that from a record
    whose field was empty. Adding a field to the record therefore adds it
    here, and ``test_the_record_payload_carries_every_recorded_field``
    fails if this stops being true.
    """

    payload: dict[str, Any] = {
        "schema_version": INVESTIGATION_SHADOW_RECORD_SCHEMA_VERSION
    }
    for entry in fields(record):
        payload[entry.name] = _json_safe(getattr(record, entry.name))
    return payload


#: Attribute names ``logging`` reserves on a ``LogRecord``.
#:
#: Derived from a real ``LogRecord`` rather than typed out, because the
#: consequence of missing one is not a wrong value: ``Logger.makeRecord``
#: raises ``KeyError`` for a colliding ``extra`` key, inside a recorder whose
#: failures are contained -- so the whole trial stream would go quiet with
#: nothing to read but a contained-write log line. Found by the codex review
#: as a latent risk for a FUTURE field; pinned now, because the cost of
#: discovering it later is every record between then and now.
RESERVED_LOG_RECORD_ATTRS = frozenset(
    vars(
        logging.LogRecord(
            name="", level=0, pathname="", lineno=0, msg="", args=(), exc_info=None
        )
    )
) | {"message", "asctime", "taskName"}


def _json_safe(value: Any) -> Any:
    """Coerce one record field into something ``json.dumps`` accepts.

    Deliberately total and deliberately lossy-but-legible at the edges. The
    fields today are strings, ints, ``None`` and tuples of strings, so none
    of this fires; it exists because the field set is DERIVED and will grow,
    and the failure it prevents is silent: ``json.dumps`` raising inside a
    contained recorder write means the record simply never appears, and a
    trial reading that stream cannot tell an absent record from a run that
    never happened.
    """

    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if is_dataclass(value) and not isinstance(value, type):
        return {
            entry.name: _json_safe(getattr(value, entry.name))
            for entry in fields(value)
        }
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _evidence_digest(ref: DevEvidenceRefV2) -> str:
    """A stable digest of an evidence record's whole payload."""

    return hashlib.sha256(
        ref.model_dump_json(exclude_none=False).encode("utf-8")
    ).hexdigest()


def canonical_bypass_offenders(
    *,
    packet_evidence: Sequence[DevEvidenceRefV2],
    canonical_evidence: Sequence[DevEvidenceRefV2],
) -> tuple[str, ...]:
    """Cited evidence that does not match what canonical services minted.

    This is the whole "graph facts cannot bypass canonical validation"
    rule, expressed as something a test can plant a violation against.

    The first version compared **handles only**, and the adversarial review
    broke it in one line: mutate a record's ``display_label`` while keeping
    a genuine handle, and the forged payload was accepted as RECORDED. A
    handle is a pointer; the claim lives in the record. So the whole payload
    is digested and compared, and a packet whose copy of a record differs
    from the canonical one in ANY field is an offender -- there is no
    "cosmetic" field on an evidence record, because every field is something
    a reader would take as canonical.

    Returns the offenders rather than a boolean, so a record can name them
    and a reviewer can tell one altered field from a wholesale forgery.
    """

    minted = {ref.evidence_ref_id: _evidence_digest(ref) for ref in canonical_evidence}
    offenders: set[str] = set()
    for ref in packet_evidence:
        canonical = minted.get(ref.evidence_ref_id)
        if canonical is None or canonical != _evidence_digest(ref):
            offenders.add(ref.evidence_ref_id)
    return tuple(sorted(offenders))


class InvestigationShadow:
    """Accepts either arm's packet and produces one comparable record.

    Construct it with the flag already decided (``enabled``), the same way
    ``QuestionUnderstandingShadow`` is constructed only when its own flag is
    on: the orchestrator holds ``None`` when the seam is off, so the off
    state is the absence of an object rather than a branch inside one.
    """

    def __init__(self, *, enabled: bool) -> None:
        self._enabled = enabled

    @property
    def enabled(self) -> bool:
        return self._enabled

    def evaluate(
        self,
        *,
        payload: Mapping[str, Any],
        run_id: str,
        organization_id: str,
        canonical_evidence: Sequence[DevEvidenceRefV2],
    ) -> InvestigationShadowRecord:
        """Validate, check canonical authority, and record. Never raises.

        Every branch returns a record. A caller that wraps this in
        ``try/except`` is being belt-and-braces, not compensating for a
        contract this method might break — and the orchestrator does wrap
        it anyway, because a shadow-mode bug must never fail or roll back
        the run it shadows.
        """

        started = time.monotonic()

        def elapsed() -> int:
            return int((time.monotonic() - started) * 1000)

        if not self._enabled:
            # Recorded, not silent: "the seam ran and chose to do nothing"
            # and "the seam never ran" are different facts, and a trial that
            # cannot tell them apart cannot audit its own coverage. Mirrors
            # QUAShadowStatus.SKIPPED_DISABLED.
            return InvestigationShadowRecord(
                run_id=run_id,
                status=InvestigationShadowStatus.SKIPPED_DISABLED,
                arm_id=_arm_id_of(payload),
                packet_schema_version=None,
                projection_version=None,
                packet_id=None,
                outcome=None,
                evidence_handles=(),
                latency_ms=0,
                detail="the shadow seam is switched off for this process",
            )

        try:
            # Validated through the registry the frozen manifest names as
            # canonical, then narrowed: the isinstance is not ceremony, it
            # asserts the registry entry really is the packet model, so a
            # registry that started returning something else fails here
            # rather than downstream.
            validated = _PACKET_MODEL.model_validate(payload)
            if not isinstance(validated, AskDevInvestigationPacket):
                raise TypeError(
                    "the canonical registry returned "
                    f"{type(validated).__name__}, not AskDevInvestigationPacket"
                )
            packet = validated
        except ValidationError as invalid:
            return InvestigationShadowRecord(
                run_id=run_id,
                status=InvestigationShadowStatus.PACKET_INVALID,
                arm_id=_arm_id_of(payload),
                packet_schema_version=None,
                projection_version=None,
                packet_id=None,
                outcome=None,
                evidence_handles=(),
                latency_ms=elapsed(),
                detail=f"{invalid.error_count()} validation errors",
            )
        except Exception as fault:  # pragma: no cover - defensive
            logger.exception(
                "ask_dev.investigation_shadow.validation_fault",
                extra={"run_id": run_id, "exception_type": type(fault).__name__},
            )
            return _fault_record(run_id, fault, elapsed())

        try:
            cited = tuple(
                entry.evidence for entry in packet.evidence_coverage.evidence_index
            )
            handles = tuple(ref.evidence_ref_id for ref in cited)
            versions = packet.versions
            trial = versions.trial
            packet_run = trial.run_id if trial else None
            if packet_run is not None and packet_run != run_id:
                # Mirrors the organization branch. A packet produced for one
                # run, evaluated against another run's canonical evidence,
                # was previously RECORDED and filed under the evaluating run
                # -- so a stale or misrouted packet became a comparison row
                # attributed to work it never described.
                return InvestigationShadowRecord(
                    run_id=run_id,
                    status=InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED,
                    arm_id=trial.arm_id if trial else None,
                    packet_schema_version=versions.packet_schema_version,
                    projection_version=versions.projection_version,
                    packet_id=packet.packet_id,
                    outcome=packet.outcome.value,
                    evidence_handles=handles,
                    latency_ms=elapsed(),
                    detail=(
                        f"the packet was produced for run {packet_run}, not the "
                        f"run it is being evaluated against"
                    ),
                )
            if packet.organization_id != organization_id:
                # Canonical material is scoped to an organization. A packet
                # claiming a different one cannot be checked against this
                # run's evidence at all, and accepting it would compare a
                # tenant against another tenant's canonical records.
                return InvestigationShadowRecord(
                    run_id=run_id,
                    status=InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED,
                    arm_id=trial.arm_id if trial else None,
                    packet_schema_version=versions.packet_schema_version,
                    projection_version=versions.projection_version,
                    packet_id=packet.packet_id,
                    outcome=packet.outcome.value,
                    evidence_handles=handles,
                    latency_ms=elapsed(),
                    detail=(
                        "the packet declares a different organization than the "
                        "run whose canonical evidence it is checked against"
                    ),
                )
            offenders = canonical_bypass_offenders(
                packet_evidence=cited,
                canonical_evidence=canonical_evidence,
            )
            if offenders:
                return InvestigationShadowRecord(
                    run_id=run_id,
                    status=InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED,
                    arm_id=trial.arm_id if trial else None,
                    packet_schema_version=versions.packet_schema_version,
                    projection_version=versions.projection_version,
                    packet_id=packet.packet_id,
                    outcome=packet.outcome.value,
                    evidence_handles=handles,
                    latency_ms=elapsed(),
                    detail=(
                        f"{len(offenders)} cited evidence records were never minted "
                        f"by a canonical service, or differ from what was: "
                        f"{list(offenders[:3])}"
                    ),
                )
            if trial is None:
                # Arm identity is optional on the contract by design, but a
                # comparison record without it is not comparable, so the
                # seam refuses rather than inventing one.
                return InvestigationShadowRecord(
                    run_id=run_id,
                    status=InvestigationShadowStatus.PACKET_INVALID,
                    arm_id=None,
                    packet_schema_version=versions.packet_schema_version,
                    projection_version=versions.projection_version,
                    packet_id=packet.packet_id,
                    outcome=packet.outcome.value,
                    evidence_handles=handles,
                    latency_ms=elapsed(),
                    detail="the packet declares no trial metadata, so no arm owns it",
                )
            return InvestigationShadowRecord(
                run_id=run_id,
                status=InvestigationShadowStatus.RECORDED,
                arm_id=trial.arm_id,
                packet_schema_version=versions.packet_schema_version,
                projection_version=versions.projection_version,
                packet_id=packet.packet_id,
                outcome=packet.outcome.value,
                evidence_handles=handles,
                latency_ms=elapsed(),
                frame_facts=_shadow_frame_facts(packet),
            )
        except Exception as fault:
            logger.exception(
                "ask_dev.investigation_shadow.assembly_fault",
                extra={"run_id": run_id, "exception_type": type(fault).__name__},
            )
            return _fault_record(run_id, fault, elapsed())


def _fault_record(
    run_id: str, fault: Exception, latency_ms: int
) -> InvestigationShadowRecord:
    return InvestigationShadowRecord(
        run_id=run_id,
        status=InvestigationShadowStatus.SEAM_FAULT,
        arm_id=None,
        packet_schema_version=None,
        projection_version=None,
        packet_id=None,
        outcome=None,
        evidence_handles=(),
        latency_ms=latency_ms,
        detail=type(fault).__name__,
    )


def _arm_id_of(payload: Mapping[str, Any]) -> str | None:
    """Best-effort arm identity from an unvalidated payload.

    An invalid packet still belongs to an arm, and a trial that cannot
    attribute its rejections learns nothing from them. Read defensively:
    the payload failed validation, so nothing about its shape is known.
    """

    try:
        versions = payload.get("versions")
        if not isinstance(versions, Mapping):
            return None
        trial = versions.get("trial")
        if not isinstance(trial, Mapping):
            return None
        arm_id = trial.get("arm_id")
        return arm_id if isinstance(arm_id, str) else None
    except Exception:  # pragma: no cover - defensive
        return None


def _shadow_frame_facts(packet: AskDevInvestigationPacket) -> tuple[str, ...]:
    """The deterministic frame content this packet supports.

    Server-owned throughout: every string is assembled here from the
    packet's *structural* claims — an outcome, a family, a driver standing —
    and never from a text field a producer wrote. The packet contract has
    no prose field an assistant would speak, and this function is where
    that property is cashed in rather than quietly relied on.
    """

    facts = [
        f"outcome:{packet.outcome.value}",
        f"family:{packet.analytical_job.question_family.value}",
        f"shape:{packet.analytical_job.comparison_shape.value}",
        f"cohort_members:{len(packet.comparison_cohort.members)}",
        f"lineage_paths:{len(packet.related_context.paths)}",
        f"principal_drivers:{len(packet.driver_analysis.principal_driver_ids)}",
        f"missing_sources:{len(packet.evidence_coverage.missing_sources)}",
    ]
    return tuple(facts)
