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
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from pydantic import ValidationError

from .contracts_v2.embedded import DevEvidenceRefV2
from .investigation_contract import (
    INVESTIGATION_CONTRACT_MODELS,
    AskDevInvestigationPacket,
)

logger = logging.getLogger(__name__)

__all__ = [
    "INVESTIGATION_SHADOW_FLAG",
    "InvestigationShadow",
    "InvestigationShadowRecord",
    "InvestigationShadowStatus",
    "canonical_bypass_offenders",
    "shadow_enabled",
]

#: Matches the CHAOS-3617 convention (``CONTEXT_FABRIC_*``, ``== "1"``), so
#: "unset" is off and every other value is off too.
INVESTIGATION_SHADOW_FLAG = "CONTEXT_FABRIC_SHADOW_SYNTHESIS_ENABLED"

_PACKET_MODEL = INVESTIGATION_CONTRACT_MODELS["ask_dev_investigation_packet.v1"]


def shadow_enabled(environ: Mapping[str, str]) -> bool:
    """Whether the shadow seam is switched on for this process."""

    return environ.get(INVESTIGATION_SHADOW_FLAG) == "1"


class InvestigationShadowStatus(StrEnum):
    """How one shadow evaluation ended.

    Every member is a *recorded* outcome. There is no "raised" member,
    because the seam never propagates an exception to its caller — see
    :meth:`InvestigationShadow.evaluate`.
    """

    #: A valid packet was accepted and a shadow frame assembled.
    RECORDED = "recorded"
    #: The seam is switched off. Recorded rather than silent, so a run that
    #: chose to do nothing is distinguishable from a seam that never ran.
    SKIPPED_DISABLED = "skipped_disabled"
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
