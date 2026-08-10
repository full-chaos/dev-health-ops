"""CHAOS-3665 (ops-side leg): organization entitlement + the web-facing
composite availability view for graph-assisted Ask Dev.

Mirrors ``tests/api/dev/test_entitlement.py``'s shape for the entitlement
half (same fail-closed feature-decision seam, now gating
``ask_dev_graph_routing`` instead of ``ask_dev``). The composite-availability
half is exercised as pure branching logic over ``GraphQueryOutcome`` plus the
packet's own disclosed ``PacketLimitationKind``s -- see the CHAOS-3660
proposal comment for why this derives from Lane B's existing
``GraphQueryOutcome`` and the packet's own truncation/staleness disclosure
rather than minting a fourth, independent vocabulary.
"""

from __future__ import annotations

import uuid

import pytest

from dev_health_ops.api.dev.graph_investigation_query import (
    GraphQueryOutcome,
    GraphQueryResult,
)
from dev_health_ops.api.dev.graph_routing_policy import (
    CanonicalGraphRoutingEntitlementAuthorizer,
    GraphAssistedAvailability,
    GraphRoutingPolicyDeniedError,
    describe_availability,
    limitation_kinds_of,
)
from dev_health_ops.api.dev.investigation_contract import (
    AskDevInvestigationPacket,
    PacketLimitation,
    PacketLimitationKind,
)
from dev_health_ops.licensing import FeatureDecision, FeatureDecisionReason
from dev_health_ops.licensing.registry import ASK_DEV_GRAPH_ROUTING_FEATURE

ORG_ID = "00000000-0000-0000-0000-000000000001"


# --------------------------------------------------------------------------
# Entitlement
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("allowed", "reason"),
    [
        (False, FeatureDecisionReason.EXPLICIT_PURCHASE_REQUIRED),
        (True, FeatureDecisionReason.ENABLED_BY_ORG_OVERRIDE),
        (True, FeatureDecisionReason.ENABLED_BY_LICENSE_OVERRIDE),
    ],
)
async def test_entitlement_uses_the_graph_routing_feature_not_base_ask_dev(
    monkeypatch: pytest.MonkeyPatch,
    allowed: bool,
    reason: FeatureDecisionReason,
) -> None:
    """This is the guard that matters most: a copy-paste from entitlement.py
    that forgot to change the feature key would silently gate the beta route
    on plain ``ask_dev`` instead of the design-partner-beta feature -- every
    org with base Ask Dev would get graph routing for free.
    """

    calls: list[tuple[object, uuid.UUID, str]] = []

    async def evaluate(
        session: object, org_id: uuid.UUID, feature_key: str
    ) -> FeatureDecision:
        calls.append((session, org_id, feature_key))
        return FeatureDecision(feature_key, allowed, reason)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.graph_routing_policy.evaluate_org_feature_async",
        evaluate,
    )
    session = object()
    authorizer = CanonicalGraphRoutingEntitlementAuthorizer(session)  # type: ignore[arg-type]

    if allowed:
        await authorizer.require(ORG_ID)
    else:
        with pytest.raises(GraphRoutingPolicyDeniedError) as exc_info:
            await authorizer.require(ORG_ID)
        assert exc_info.value.reason is reason

    assert calls == [(session, uuid.UUID(ORG_ID), ASK_DEV_GRAPH_ROUTING_FEATURE)]


@pytest.mark.asyncio
async def test_entitlement_storage_failure_and_invalid_org_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unavailable(*_args: object) -> FeatureDecision:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        "dev_health_ops.api.dev.graph_routing_policy.evaluate_org_feature_async",
        unavailable,
    )
    authorizer = CanonicalGraphRoutingEntitlementAuthorizer(object())  # type: ignore[arg-type]

    with pytest.raises(GraphRoutingPolicyDeniedError) as storage:
        await authorizer.require(ORG_ID)
    assert storage.value.reason is FeatureDecisionReason.STORAGE_ERROR

    with pytest.raises(GraphRoutingPolicyDeniedError) as invalid:
        await authorizer.require("not-an-org-id")
    assert invalid.value.reason is FeatureDecisionReason.INVALID_FEATURE_STATE


# --------------------------------------------------------------------------
# Composite availability
# --------------------------------------------------------------------------


def _completed_packet() -> AskDevInvestigationPacket:
    from tests._chaos_3502_graph_investigation_fake import default_investigation_packet

    return default_investigation_packet()


def _with_limitation_kinds(
    packet: AskDevInvestigationPacket, kinds: tuple[PacketLimitationKind, ...]
) -> AskDevInvestigationPacket:
    """Patch only ``evidence_coverage.limitations`` -- deliberately bypasses
    the packet's own cross-field disclosure validators (``model_copy``
    doesn't re-run them), because this helper exists solely to feed
    ``describe_availability`` a packet whose *limitation kinds* are
    controlled, not to construct a fully self-consistent packet. The
    plumbing from a real, validated packet is covered separately by
    ``test_limitation_kinds_of_reads_the_real_contract_shape`` below.
    """

    limitations = tuple(
        PacketLimitation(kind=kind, detail=f"test fixture: {kind.value}")
        for kind in kinds
    )
    coverage = packet.evidence_coverage.model_copy(update={"limitations": limitations})
    return packet.model_copy(update={"evidence_coverage": coverage})


def test_limitation_kinds_of_reads_the_real_contract_shape() -> None:
    """Confirms the extraction helper reads the packet's real,
    contract-validated ``evidence_coverage.limitations`` -- not a
    hand-rolled shape a unit test alone might get away with.
    """

    packet = _completed_packet()
    kinds = limitation_kinds_of(packet)
    assert kinds == frozenset(
        entry.kind for entry in packet.evidence_coverage.limitations
    )


@pytest.mark.parametrize(
    "outcome",
    [
        GraphQueryOutcome.DISABLED,
        GraphQueryOutcome.UNAVAILABLE,
        GraphQueryOutcome.DEADLINE_EXCEEDED,
        GraphQueryOutcome.CANCELLED,
        GraphQueryOutcome.PROVIDER_FAILURE,
    ],
)
def test_not_entitled_and_every_transport_failure_outcome_report_unavailable(
    outcome: GraphQueryOutcome,
) -> None:
    result = GraphQueryResult(outcome=outcome, packet=None, diagnostic="probe")
    assert describe_availability(entitled=True, result=result) is (
        GraphAssistedAvailability.UNAVAILABLE
    )
    # Entitlement is checked first: even a COMPLETED-shaped result must not
    # leak through for an org without the feature.
    completed = GraphQueryResult(
        outcome=GraphQueryOutcome.COMPLETED, packet=_completed_packet()
    )
    assert describe_availability(entitled=False, result=completed) is (
        GraphAssistedAvailability.UNAVAILABLE
    )


def test_no_attempt_yet_is_unavailable_not_a_fabricated_enabled() -> None:
    assert describe_availability(entitled=True, result=None) is (
        GraphAssistedAvailability.UNAVAILABLE
    )


def test_route_level_stale_reports_stale() -> None:
    result = GraphQueryResult(
        outcome=GraphQueryOutcome.STALE,
        packet=None,
        diagnostic="watermark beyond tolerance",
    )
    assert describe_availability(entitled=True, result=result) is (
        GraphAssistedAvailability.STALE
    )


def test_completed_with_truncated_traversal_reports_truncated() -> None:
    packet = _with_limitation_kinds(
        _completed_packet(), (PacketLimitationKind.TRUNCATED_TRAVERSAL,)
    )
    result = GraphQueryResult(outcome=GraphQueryOutcome.COMPLETED, packet=packet)
    assert describe_availability(entitled=True, result=result) is (
        GraphAssistedAvailability.TRUNCATED
    )


def test_completed_with_stale_source_reports_lagging() -> None:
    packet = _with_limitation_kinds(
        _completed_packet(), (PacketLimitationKind.STALE_SOURCE,)
    )
    result = GraphQueryResult(outcome=GraphQueryOutcome.COMPLETED, packet=packet)
    assert describe_availability(entitled=True, result=result) is (
        GraphAssistedAvailability.LAGGING
    )


def test_truncated_takes_precedence_over_lagging_when_both_present() -> None:
    packet = _with_limitation_kinds(
        _completed_packet(),
        (PacketLimitationKind.TRUNCATED_TRAVERSAL, PacketLimitationKind.STALE_SOURCE),
    )
    result = GraphQueryResult(outcome=GraphQueryOutcome.COMPLETED, packet=packet)
    assert describe_availability(entitled=True, result=result) is (
        GraphAssistedAvailability.TRUNCATED
    )


def test_clean_completed_reports_enabled() -> None:
    packet = _with_limitation_kinds(_completed_packet(), ())
    result = GraphQueryResult(outcome=GraphQueryOutcome.COMPLETED, packet=packet)
    assert describe_availability(entitled=True, result=result) is (
        GraphAssistedAvailability.ENABLED
    )
