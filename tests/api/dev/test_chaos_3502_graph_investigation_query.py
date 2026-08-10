"""CHAOS-3502/CHAOS-3660: smoke coverage for the proposed consumer-side
graph-investigation seam (``graph_investigation_query.py``) and its fake.

Not full orchestrator-routing coverage yet (that lands with the actual
routing branch) -- this proves the Protocol + fake are usable and that the
transport/outcome contract behaves as documented before anything binds to
it.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dev_health_ops.api.dev.contracts_v2.base import Cardinality, QuestionIntentID
from dev_health_ops.api.dev.graph_investigation_query import (
    GraphInvestigationQuery,
    GraphInvestigationRequest,
    GraphQueryOutcome,
    GraphQueryResult,
)
from dev_health_ops.api.dev.investigation_contract import AskDevInvestigationPacket
from tests._chaos_3502_graph_investigation_fake import (
    FakeGraphInvestigationQuery,
    default_investigation_packet,
)


def _request(
    *, authorized_entity_ids: frozenset[str] = frozenset()
) -> GraphInvestigationRequest:
    return GraphInvestigationRequest(
        org_id="org_1",
        run_id="run_1",
        intent_id=QuestionIntentID.DISCOVERED_COHORT,
        cardinality=Cardinality.ORGANIZATION_WIDE,
        mentions=(),
        question_text="Which teams are currently struggling?",
        authorized_entity_ids=authorized_entity_ids,
        deadline=datetime(2026, 8, 10, 0, 0, tzinfo=UTC),
    )


def test_fake_satisfies_the_protocol_structurally() -> None:
    """``GraphInvestigationQuery`` is a plain (non-runtime-checkable) Protocol
    -- matching this codebase's convention (``question_interpreter.
    IntentClassifier``) -- so conformance is a static/mypy property, not an
    ``isinstance`` check. Assigning to the Protocol-typed variable below is
    the actual check: this file fails ``mypy`` if the fake's ``investigate``
    signature ever drifts from the Protocol's.
    """

    fake: GraphInvestigationQuery = FakeGraphInvestigationQuery()
    assert fake is not None


@pytest.mark.asyncio
async def test_completed_outcome_carries_a_valid_packet() -> None:
    fake = FakeGraphInvestigationQuery()
    result = await fake.investigate(_request())
    assert result.outcome is GraphQueryOutcome.COMPLETED
    assert isinstance(result.packet, AskDevInvestigationPacket)


@pytest.mark.asyncio
async def test_non_completed_outcome_never_carries_a_packet() -> None:
    fake = FakeGraphInvestigationQuery(outcome=GraphQueryOutcome.UNAVAILABLE)
    result = await fake.investigate(_request())
    assert result.outcome is GraphQueryOutcome.UNAVAILABLE
    assert result.packet is None


def test_result_rejects_completed_without_a_packet() -> None:
    with pytest.raises(ValueError, match="if and only if"):
        GraphQueryResult(outcome=GraphQueryOutcome.COMPLETED, packet=None)


def test_result_rejects_a_packet_on_a_non_completed_outcome() -> None:
    with pytest.raises(ValueError, match="if and only if"):
        GraphQueryResult(
            outcome=GraphQueryOutcome.STALE, packet=default_investigation_packet()
        )


@pytest.mark.asyncio
async def test_fake_records_the_request_it_received() -> None:
    fake = FakeGraphInvestigationQuery()
    authorized = frozenset({"proj_a", "proj_b"})
    request = _request(authorized_entity_ids=authorized)
    await fake.investigate(request)
    assert fake.received_requests == [request]
    assert fake.received_requests[0].authorized_entity_ids == authorized


@pytest.mark.asyncio
async def test_fake_responder_overrides_defaults() -> None:
    fake = FakeGraphInvestigationQuery(
        responder=lambda req: GraphQueryResult(
            outcome=GraphQueryOutcome.DEADLINE_EXCEEDED,
            diagnostic=f"deadline was {req.deadline.isoformat()}",
        )
    )
    result = await fake.investigate(_request())
    assert result.outcome is GraphQueryOutcome.DEADLINE_EXCEEDED
    assert result.packet is None
    assert result.diagnostic is not None and "2026-08-10" in result.diagnostic
