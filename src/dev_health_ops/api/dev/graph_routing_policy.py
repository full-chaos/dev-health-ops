"""CHAOS-3665 (Phase 1, ops-side leg): organization entitlement and the
web-facing composite availability view for graph-assisted Ask Dev.

Posted as a scoping proposal on CHAOS-3660 before this module was written;
see that comment thread for the full reasoning. Two independent pieces:

1. **Entitlement** (:class:`CanonicalGraphRoutingEntitlementAuthorizer`).
   ``ask_dev_graph_routing`` is already a registered explicit-purchase
   feature (``licensing/registry.py``) evaluated through the existing
   ``decide_feature``/``evaluate_org_feature_async`` machinery -- this
   mirrors ``entitlement.CanonicalAskDevEntitlementAuthorizer`` exactly, for
   the design-partner-beta graph route rather than base Ask Dev. This is the
   ORGANIZATION-level gate; ``orchestrator.graph_routing_runtime_enabled()``
   is the separate, same-process runtime kill switch documented at
   ``registry.ASK_DEV_GRAPH_ROUTING_FEATURE`` -- both must be true for a run
   to attempt the graph route. Neither substitutes for the other.

2. **Composite availability** (:func:`describe_availability`). CHAOS-3665's
   acceptance criteria name a web-facing vocabulary --
   enabled/unavailable/stale/lagging/truncated/fallback -- but that is
   explicitly NOT a new, independent source of truth: it is derived from
   the organization entitlement decision above, Lane B's transport-level
   ``GraphQueryOutcome`` (``graph_investigation_query.py``), and the
   packet's own already-validated truncation/staleness disclosure
   (``investigation_contract.packet``'s ``PacketLimitationKind``). Minting a
   fourth, parallel enum here was explicitly ruled out.

   ``fallback`` is declared in :class:`GraphAssistedAvailability` (CHAOS-3665
   names it) but :func:`describe_availability` never returns it: nothing in
   the codebase today names "graph route attempted, answer degraded to
   native investigation" as a fact. Per this project's verification
   discipline, a measurement that cannot happen must fail loudly rather than
   silently default -- so that state stays unreachable from this function
   until a real signal exists for it (flagged as a gap on CHAOS-3660, not
   guessed at here).
"""

from __future__ import annotations

import uuid
from enum import StrEnum
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.licensing import (
    FeatureDecisionReason,
    evaluate_org_feature_async,
)
from dev_health_ops.licensing.registry import ASK_DEV_GRAPH_ROUTING_FEATURE

from .graph_investigation_query import GraphQueryOutcome, GraphQueryResult
from .investigation_contract import AskDevInvestigationPacket, PacketLimitationKind

__all__ = [
    "GraphRoutingEntitlementAuthorizer",
    "GraphRoutingPolicyDeniedError",
    "CanonicalGraphRoutingEntitlementAuthorizer",
    "GraphAssistedAvailability",
    "limitation_kinds_of",
    "describe_availability",
]


# --------------------------------------------------------------------------
# Entitlement
# --------------------------------------------------------------------------


class GraphRoutingEntitlementAuthorizer(Protocol):
    async def require(self, org_id: str) -> None: ...


class GraphRoutingPolicyDeniedError(RuntimeError):
    """Fail-closed denial from the canonical feature-decision seam."""

    def __init__(self, reason: FeatureDecisionReason) -> None:
        self.reason = reason
        super().__init__("ask_dev_graph_routing_not_available")


class CanonicalGraphRoutingEntitlementAuthorizer:
    """Evaluate ``ask_dev_graph_routing`` without encoding plan names here.

    Mirrors ``entitlement.CanonicalAskDevEntitlementAuthorizer`` field for
    field -- same fail-closed shape, same feature-decision seam -- gating the
    design-partner-beta graph route instead of base Ask Dev.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def require(self, org_id: str) -> None:
        try:
            parsed_org_id = uuid.UUID(org_id)
        except ValueError as exc:
            raise GraphRoutingPolicyDeniedError(
                FeatureDecisionReason.INVALID_FEATURE_STATE
            ) from exc
        try:
            decision = await evaluate_org_feature_async(
                self._session,
                parsed_org_id,
                ASK_DEV_GRAPH_ROUTING_FEATURE,
            )
        except Exception as exc:
            raise GraphRoutingPolicyDeniedError(
                FeatureDecisionReason.STORAGE_ERROR
            ) from exc
        if not decision.allowed:
            raise GraphRoutingPolicyDeniedError(decision.reason)


# --------------------------------------------------------------------------
# Composite availability
# --------------------------------------------------------------------------


class GraphAssistedAvailability(StrEnum):
    """Web-facing degradation-state vocabulary for CHAOS-3665.

    Not an independent source of truth -- see the module docstring.
    """

    ENABLED = "enabled"
    UNAVAILABLE = "unavailable"
    STALE = "stale"
    LAGGING = "lagging"
    TRUNCATED = "truncated"
    FALLBACK = "fallback"


#: Every ``GraphQueryOutcome`` other than ``COMPLETED``/``STALE`` means "no
#: graph-assisted answer this run" from the web's point of view. The
#: distinct transport diagnostic (why, specifically) stays on
#: ``GraphQueryResult.diagnostic`` for logs/telemetry; the web only needs to
#: know there is nothing graph-derived to render.
_TRANSPORT_UNAVAILABLE_OUTCOMES: frozenset[GraphQueryOutcome] = frozenset(
    {
        GraphQueryOutcome.DISABLED,
        GraphQueryOutcome.UNAVAILABLE,
        GraphQueryOutcome.DEADLINE_EXCEEDED,
        GraphQueryOutcome.CANCELLED,
        GraphQueryOutcome.PROVIDER_FAILURE,
    }
)


def limitation_kinds_of(
    packet: AskDevInvestigationPacket,
) -> frozenset[PacketLimitationKind]:
    """The set of ``PacketLimitationKind``s a completed packet discloses.

    Reads ``evidence_coverage.limitations`` -- the packet's own
    cross-field validator (``validate_partial_results_are_disclosed``)
    already guarantees any section's truncation or per-source staleness is
    reflected here, so this is the one place to look rather than
    re-deriving truncation/staleness from each section's own flags.
    """

    return frozenset(entry.kind for entry in packet.evidence_coverage.limitations)


def describe_availability(
    *,
    entitled: bool,
    result: GraphQueryResult | None,
) -> GraphAssistedAvailability:
    """Compose the org entitlement decision and a graph-route attempt's
    outcome into the CHAOS-3665 web vocabulary.

    ``result`` is ``None`` when the graph route was never attempted this run
    (e.g. the runtime kill switch was off, or routing chose not to call the
    seam at all) -- treated the same as any other "no graph-assisted answer"
    case: ``unavailable``, never a fabricated ``enabled``.

    Precedence when a completed packet discloses more than one relevant
    limitation: ``truncated`` outranks ``lagging`` -- a bounded/cut-off
    result is a stronger degradation to surface than "some of what fed it
    was a little behind."
    """

    if not entitled:
        return GraphAssistedAvailability.UNAVAILABLE
    if result is None or result.outcome in _TRANSPORT_UNAVAILABLE_OUTCOMES:
        return GraphAssistedAvailability.UNAVAILABLE
    if result.outcome is GraphQueryOutcome.STALE:
        return GraphAssistedAvailability.STALE
    if result.outcome is not GraphQueryOutcome.COMPLETED:
        raise AssertionError(f"unhandled GraphQueryOutcome member: {result.outcome!r}")
    if result.packet is None:
        raise AssertionError(
            "GraphQueryResult declares COMPLETED but carries no packet; "
            "this violates GraphQueryResult's own invariant"
        )
    kinds = limitation_kinds_of(result.packet)
    if PacketLimitationKind.TRUNCATED_TRAVERSAL in kinds:
        return GraphAssistedAvailability.TRUNCATED
    if PacketLimitationKind.STALE_SOURCE in kinds:
        return GraphAssistedAvailability.LAGGING
    return GraphAssistedAvailability.ENABLED
