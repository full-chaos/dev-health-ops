"""A Lane-B-owned fake for ``graph_investigation_query.GraphInvestigationQuery``.

Lives in the ``tests`` package (mirrors ``tests/_chaos_3295_plan_executor``,
``tests/_chaos_3292_preflight``) so both mypy and pytest resolve it the same
way. Per the CHAOS-3660 ratified plan: Lane B binds orchestrator routing to
the ``GraphInvestigationQuery`` Protocol and this fake ONLY -- this is not a
scaled-down copy of the trial/shadow graph arm's construction, and it must
never be imported from production code (``src/``). Its only job is to let
Lane B's own orchestrator-routing tests exercise every ``GraphQueryOutcome``
deterministically, without a live graph backend.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from dev_health_ops.api.dev.graph_investigation_query import (
    GraphInvestigationQuery,
    GraphInvestigationRequest,
    GraphQueryOutcome,
    GraphQueryResult,
)
from dev_health_ops.api.dev.investigation_contract import AskDevInvestigationPacket
from dev_health_ops.api.dev.investigation_contract.fixtures import positive_fixtures

__all__ = ["FakeGraphInvestigationQuery", "default_investigation_packet"]


def default_investigation_packet() -> AskDevInvestigationPacket:
    """A valid, contract-checked packet -- the same fixture the investigation
    contract's own test suite validates against, so a fake result is never
    accidentally malformed in a way production code would reject.
    """

    payload = positive_fixtures()["ask_dev_investigation_packet.v1"]
    return AskDevInvestigationPacket.model_validate(payload)


@dataclass
class FakeGraphInvestigationQuery(GraphInvestigationQuery):
    """Deterministic, in-memory. Configure ``outcome``/``packet`` for the
    "happy path" cases; use ``responder`` for a test that needs the result to
    depend on the request (e.g. asserting the request's ``authorized_entity_ids``
    reached the fake unchanged).
    """

    outcome: GraphQueryOutcome = GraphQueryOutcome.COMPLETED
    packet: AskDevInvestigationPacket | None = field(
        default_factory=default_investigation_packet
    )
    diagnostic: str | None = None
    #: When set, overrides ``outcome``/``packet``/``diagnostic`` entirely --
    #: the fake calls this with the request and returns whatever it returns.
    responder: Callable[[GraphInvestigationRequest], GraphQueryResult] | None = None
    #: Every request this fake received, in call order -- lets a test assert
    #: on what the orchestrator actually sent (deadline, authorized set,
    #: intent) without needing a real graph backend to introspect.
    received_requests: list[GraphInvestigationRequest] = field(default_factory=list)

    async def investigate(self, request: GraphInvestigationRequest) -> GraphQueryResult:
        self.received_requests.append(request)
        if self.responder is not None:
            return self.responder(request)
        packet = self.packet if self.outcome is GraphQueryOutcome.COMPLETED else None
        return GraphQueryResult(
            outcome=self.outcome, packet=packet, diagnostic=self.diagnostic
        )


def deadline_from_now(seconds: float, *, now: Callable[[], datetime]) -> datetime:
    """Small helper so a test's deadline is computed from its own injected
    clock rather than a bare ``datetime.now()`` call the repo's fixed-clock
    convention (``tests/_chaos_3292_preflight.fixed_now``) exists to avoid.
    """

    from datetime import timedelta

    return now() + timedelta(seconds=seconds)
