"""CHAOS-3618 PR 2: the native arm, seen from the orchestrator.

The seam (``api.dev.investigation_shadow``) is arm-neutral machinery and
imports no arm. This module is the other half: it adapts one finished run
into :class:`~.projection.NativeProjectionInput`, calls the projection, and
hands back a packet payload. It is the ONLY place the native arm touches
the run path, and it touches it through a Protocol the graph arm implements
identically.

**Total, like the projection it wraps.** ``build_packet`` never raises. The
orchestrator does contain it -- a producer is third-party code from the run
path's point of view -- but relying on that containment would attribute
this module's own defects to the seam, and the trial reports "how often can
the baseline express its own run" as a statement about the PRODUCT. The
same argument produced ``NativeProjectionGapReason.PROJECTION_FAULT`` one
layer down, and it does not stop being true one layer up.

**Adapts, never enriches.** Every field below is copied from the finished
run. There is no service handle here, no clock-derived window, no lookup:
a producer that could call a service could quietly make the baseline look
better than the run it describes, which is the exact dishonesty
CHAOS-3618's brief forbids. The one clock reading is ``produced_at``, which
is metadata about the projection rather than about the run.

**Refuses rather than substitutes.** A run with no interpretation, or no
bounded window, cannot be projected -- and the refusal is a named,
logged gap, not a defaulted packet. Both are honest measurements of the
current product; a packet dated to a window nothing queried would not be.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from dev_health_ops.api.dev.investigation_shadow import (
    FinishedRunContext,
    InvestigationShadow,
    shadow_enabled,
)
from dev_health_ops.api.dev.question_interpreter import InterpretedQuestion

from .flags import native_projection_enabled
from .projection import (
    NATIVE_ARM_ID,
    NativeProjectionGap,
    NativeProjectionGapReason,
    NativeProjectionInput,
    NativeProjectionOutcome,
    project_native_investigation,
)

logger = logging.getLogger(__name__)

__all__ = ["NativeInvestigationPacketProducer", "native_shadow_wiring"]


def _utc_now() -> datetime:
    return datetime.now(UTC)


class NativeInvestigationPacketProducer:
    """Projects a finished native run into the shared packet contract.

    Satisfies ``investigation_shadow.InvestigationPacketProducer``
    structurally rather than by inheritance, the same way the graph arm's
    producer will: the orchestrator holds the Protocol and never imports
    either arm.
    """

    #: Exposed so a caller assembling trial metadata does not have to import
    #: the projection module to learn which arm it wired.
    arm_id = NATIVE_ARM_ID

    def __init__(self, *, now: Callable[[], datetime] = _utc_now) -> None:
        self._now = now

    def build_packet(self, run: FinishedRunContext) -> Mapping[str, Any] | None:
        """One finished run -> one packet payload, or ``None``.

        ``None`` means "this run is not projectable", which is a
        measurement the trial counts, not a failure. Every ``None`` is
        preceded by a logged gap naming the reason, so a run the baseline
        could not express is never silently indistinguishable from a run
        the seam never saw.
        """

        try:
            outcome = self._project(run)
        except Exception as fault:  # pragma: no cover - defensive
            # Unreachable by construction: ``_project`` builds the input
            # inside its own guard and ``project_native_investigation`` is
            # total. Kept because "unreachable" is a claim about today's
            # code, and the cost of being wrong is the run path catching an
            # arm's exception and filing it as a seam fault.
            logger.exception(
                "context_fabric.native_arm.producer_fault",
                extra={
                    "run_id": run.run_id,
                    "exception_type": type(fault).__name__,
                },
            )
            return None
        if outcome.packet is None:
            for gap in outcome.gaps:
                logger.info(
                    "context_fabric.native_arm.projection_gap",
                    extra={
                        "run_id": run.run_id,
                        "organization_id": run.organization_id,
                        "arm_id": NATIVE_ARM_ID,
                        "gap_reason": gap.reason.value,
                        "gap_detail": gap.detail,
                    },
                )
            return None
        return outcome.packet.model_dump(mode="json")

    def _project(self, run: FinishedRunContext) -> NativeProjectionOutcome:
        """Adapt and project. Raises only if the adaptation itself is
        impossible, which ``build_packet`` contains."""

        interpretation = run.interpretation
        if not isinstance(interpretation, InterpretedQuestion):
            # Not a type-safety flourish: ``FinishedRunContext`` carries
            # this as ``Any`` (the seam must not couple to any one arm's
            # view of a run), so the narrowing has to happen somewhere, and
            # here it becomes a NAMED gap rather than an AttributeError
            # inside the projection that would be filed as a crash.
            return _gap(
                NativeProjectionGapReason.NO_INTERPRETED_QUESTION,
                "the run terminated before subject preflight interpreted the "
                f"question (interpretation={type(interpretation).__name__})",
            )
        window_start = run.window_start
        window_end = run.window_end
        if window_start is None or window_end is None:
            return _gap(
                NativeProjectionGapReason.NO_BOUNDED_WINDOW,
                "the run ended before scope resolution completed, so it has no "
                "bounded window and no window may be substituted for it",
            )
        if window_end <= window_start:
            # ``DevTimeRange`` already rejects this upstream, so reaching it
            # means the window did not come from a scope decision. A gap
            # says so; ``NativeProjectionInput.__post_init__`` would raise
            # and the raise would read as an arm crash.
            return _gap(
                NativeProjectionGapReason.NO_BOUNDED_WINDOW,
                "the run's window is not a real interval "
                f"({window_start.isoformat()}..{window_end.isoformat()})",
            )
        return project_native_investigation(
            NativeProjectionInput(
                org_id=run.organization_id,
                run_id=run.run_id,
                produced_at=self._now(),
                interpretation=interpretation,
                ledger=run.ledger,
                subject_set=run.subject_set,
                committed_subject=run.committed_subject,
                investigation_result=run.investigation_result,
                window_start=window_start,
                window_end=window_end,
                # The frame's own evidence, which the server built from what
                # the evidence service returned -- the same sequence the
                # seam will digest each cited record against. Sourcing it
                # anywhere else is what the seam's canonical-bypass check
                # exists to catch.
                evidence=run.canonical_evidence,
            )
        )


def native_shadow_wiring(
    environ: Mapping[str, str] | None = None,
) -> tuple[InvestigationShadow | None, NativeInvestigationPacketProducer | None]:
    """The two values ``BoundedDevRuntime`` takes, decided by the flags.

    ``(None, None)`` when off, because off is the ABSENCE of an object here
    rather than a disabled one -- the orchestrator's own guard is
    ``shadow is None or producer is None``, so a flag-off process cannot
    reach the seam through any branch, only through a missing collaborator.

    The two flags are independent on purpose. The seam is machinery and the
    producer is one arm, so a trial can switch on the seam with the graph
    arm's producer, or run neither, without either flag implying the other.

    **Not called from ``production_runtime.py`` in this PR.** That is the
    declared gap, not an oversight: CHAOS-3618 lands inert wiring, and a
    process that constructed this would be one flag flip away from writing
    trial records nobody is reading yet. CHAOS-3619 makes the call.
    """

    if environ is None:
        environ = os.environ
    shadow = InvestigationShadow(enabled=True) if shadow_enabled(environ) else None
    producer = (
        NativeInvestigationPacketProducer()
        if native_projection_enabled(environ)
        else None
    )
    return shadow, producer


def _gap(reason: NativeProjectionGapReason, detail: str) -> NativeProjectionOutcome:
    return NativeProjectionOutcome(
        packet=None, gaps=(NativeProjectionGap(reason=reason, detail=detail),)
    )
