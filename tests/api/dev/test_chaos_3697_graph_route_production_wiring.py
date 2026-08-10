"""Regression coverage for the production graph-route composition root.

The defect was invisible to tests that hand-constructed ``DevOrchestrator``
with graph collaborators. These tests instead drive the real path from
``build_production_runtime()`` through ``BoundedDevRuntime.run()`` and inspect
what that call site supplies. An enabled organization must receive all three
shared collaborators; a policy-off organization must retain their ``None``
state, and the independent runtime kill switch must still default off.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from dev_health_ops.api.dev import production_runtime
from dev_health_ops.api.dev import runtime as runtime_module
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import DevMessageRequest
from dev_health_ops.api.dev.orchestrator import (
    GRAPH_ROUTING_RUNTIME_FLAG,
    DevOrchestrator,
    NullRunRecorder,
    graph_routing_runtime_enabled,
)
from dev_health_ops.api.dev.production_runtime import ProductionProviderResolution
from dev_health_ops.llm.agent.policy import AgentProviderSource


class _FakeProvider:
    """No test in this file ever reaches an actual provider call -- the
    capturing stand-in below raises the instant ``DevOrchestrator.__init__``
    returns, before ``run()`` does anything that would need one."""

    async def decide(self, **_values: Any) -> Any:
        raise AssertionError(
            "this test stops at DevOrchestrator construction; the provider "
            "must never be called"
        )

    async def aclose(self) -> None:
        return None


class _ConstructionObserved(Exception):
    """Raised by the capturing stand-in the instant construction finishes,
    so this test never has to drive a full agentic run just to observe what
    the real runtime handed the real constructor."""


async def _build_runtime(
    monkeypatch: pytest.MonkeyPatch, *, wave_3_1_enabled: bool = True
) -> Any:
    """The real production composition root
    (``production_runtime.build_production_runtime``), with only the
    provider resolution and the JWT secret stubbed -- the same seam
    ``test_production_runtime_wires_exactly_the_nine_registered_tools``
    (``test_production_runtime.py``) already stubs to drive this same
    builder without a live database or ClickHouse. Session and ClickHouse
    are opaque placeholders: construction never touches either eagerly,
    only tool execution would, and no test here executes a tool.
    """

    async def resolve_provider(
        _session: Any, *, org_id: str
    ) -> ProductionProviderResolution:
        del org_id
        return ProductionProviderResolution(
            provider=cast(Any, _FakeProvider()),
            source=AgentProviderSource.PLATFORM,
            family="openai",
            model="certified-model",
            provider_label="OpenAI compatible",
            model_label="certified-model",
        )

    monkeypatch.setattr(
        production_runtime, "resolve_production_provider", resolve_provider
    )

    async def resolve_wave_policy(_session: Any, _org_id: str) -> bool:
        return wave_3_1_enabled

    monkeypatch.setattr(production_runtime, "_wave_3_1_enabled", resolve_wave_policy)
    monkeypatch.setenv("JWT_SECRET_KEY", "test-evidence-signing-secret-32-bytes-long")
    return await production_runtime.build_production_runtime(
        cast(Any, object()),
        org_id="org_01",
        permission_fingerprint="permissions_01",
        clickhouse=cast(Any, object()),
    )


async def _capture_orchestrator_construction(
    monkeypatch: pytest.MonkeyPatch, bounded_runtime: Any
) -> dict[str, Any]:
    """Drive the REAL ``BoundedDevRuntime.run()`` call site (``runtime.py``)
    that constructs ``DevOrchestrator``, and capture exactly the keyword
    arguments it was constructed with. Nothing here hand-passes those
    kwargs -- that is precisely the trap every existing test falls into.
    """

    captured: dict[str, Any] = {}

    class _CapturingOrchestrator(DevOrchestrator):
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)
            super().__init__(**kwargs)
            # Stop right here: the thing under test is what runtime.py
            # PASSED, not what a full agentic run would do with it.
            raise _ConstructionObserved()

    monkeypatch.setattr(runtime_module, "DevOrchestrator", _CapturingOrchestrator)

    async def _null_event_sink(_event: Any) -> None:
        return None

    with pytest.raises(_ConstructionObserved):
        await bounded_runtime.run(
            request=DevMessageRequest.model_validate(
                positive_fixtures()["dev_message_request.v1"]
            ),
            org_id="org_01",
            user_id="user_01",
            permission_fingerprint="permissions_01",
            run_id="run_01",
            conversation_id="conversation_01",
            answer_id="answer_01",
            cancellation=asyncio.Event(),
            recorder=NullRunRecorder(),
            event_sink=_null_event_sink,
        )
    return captured


#: The 17 keyword arguments runtime.py's DevOrchestrator(...) call site
#: passes. Used by the positive control below to prove the capturing harness
#: genuinely observed the real construction seam.
_KWARGS_RUNTIME_PY_ACTUALLY_PASSES = frozenset(
    {
        "provider",
        "provider_source",
        "provider_family",
        "registry",
        "scope_resolver",
        "versions",
        "recorder",
        "preflight",
        "plan_registry",
        "plan_executor",
        "narrative_provider",
        "qua_shadow",
        "investigation_shadow",
        "investigation_packet_producer",
        "graph_investigation_query",
        "evidence_service",
        "canonical_enrichment",
    }
)


@pytest.mark.asyncio
async def test_the_capturing_harness_genuinely_observes_a_real_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Positive control for the wiring regression test below.

    If the interception in
    ``_capture_orchestrator_construction`` ever silently stops working --
    the ``DevOrchestrator(...)`` call site moves, gets wrapped in a
    factory, the import binding changes, or the probe exception fires
    before ``captured`` is populated -- this test fails loudly instead of
    letting the collaborator assertions claim coverage over an unobserved
    construction.

    This test asserts the harness actually captured a construction: the
    exact 17 keyword names ``runtime.py`` passes, populated
    with real values (not just present-but-empty) -- ``registry`` is a
    genuine ``AskDevToolRegistry``, and ``provider`` is the exact
    ``_FakeProvider`` instance this test's own stub handed to
    ``build_production_runtime``, not a stand-in constructed independently
    by the harness itself.
    """

    bounded_runtime = await _build_runtime(monkeypatch)
    try:
        captured = await _capture_orchestrator_construction(
            monkeypatch, bounded_runtime
        )
    finally:
        await bounded_runtime.aclose()

    assert captured, (
        "the capturing DevOrchestrator subclass never ran -- the "
        "interception itself is broken, which would make the wiring test "
        "below meaningless"
    )
    assert set(captured) == _KWARGS_RUNTIME_PY_ACTUALLY_PASSES, (
        "runtime.py's DevOrchestrator(...) call site no longer passes the "
        "keyword set this harness was written against -- update "
        "_KWARGS_RUNTIME_PY_ACTUALLY_PASSES to match the real call site "
        "before trusting the wiring claim again"
    )
    assert captured["registry"] is not None
    assert type(captured["registry"]).__name__ == "AskDevToolRegistry"
    assert captured["provider"] is bounded_runtime.provider, (
        "the captured provider must be the SAME instance this test's stub "
        "handed to build_production_runtime -- a different instance would "
        "mean the harness is observing a different construction than the "
        "one the real runtime performed"
    )


@pytest.mark.asyncio
async def test_production_runtime_wires_the_graph_route_collaborators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cohort-discovery routing gate
    requires ``graph_investigation_query``, ``evidence_service``, AND (once
    wired) ``canonical_enrichment`` to all be non-``None`` before it even
    asks ``graph_routing_runtime_enabled()``. This asserts on the ACTUAL
    ``DevOrchestrator`` the production composition root builds -- not one
    this test built by hand -- so it fails for the same reason the route
    fails to activate in production, not for an unrelated mistake in the
    test itself. The helper explicitly enables the organization policy so
    collaborator presence makes the runtime kill switch the deciding gate.
    """

    bounded_runtime = await _build_runtime(monkeypatch)
    try:
        captured = await _capture_orchestrator_construction(
            monkeypatch, bounded_runtime
        )
    finally:
        await bounded_runtime.aclose()

    assert captured.get("graph_investigation_query") is not None, (
        "production_runtime.py never constructs a GraphInvestigationQuery "
        "anywhere -- the routing gate's first identity check "
        "(self._graph_investigation_query is not None) can never pass, so "
        "the graph route is unreachable regardless of the runtime flag"
    )
    assert captured.get("evidence_service") is not None, (
        "_assemble_production_runtime builds a real EvidenceService but "
        "never threads it into BoundedDevRuntime (no field carries it "
        "through to the DevOrchestrator() call in runtime.py) -- the "
        "routing gate's second identity check can never pass either"
    )
    assert captured.get("canonical_enrichment") is not None, (
        "canonical_enrichment is not threaded from the production "
        "composition root into DevOrchestrator"
    )
    assert (
        captured["graph_investigation_query"]
        is bounded_runtime.graph_investigation_query
    )
    assert captured["evidence_service"] is bounded_runtime.evidence_service
    assert captured["canonical_enrichment"] is bounded_runtime.canonical_enrichment


@pytest.mark.asyncio
async def test_production_runtime_keeps_graph_collaborators_off_for_policy_off_org(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bounded_runtime = await _build_runtime(monkeypatch, wave_3_1_enabled=False)
    try:
        captured = await _capture_orchestrator_construction(
            monkeypatch, bounded_runtime
        )
    finally:
        await bounded_runtime.aclose()

    assert captured["graph_investigation_query"] is None
    assert captured["evidence_service"] is None
    assert captured["canonical_enrichment"] is None


def test_graph_routing_runtime_flag_defaults_off_in_a_production_shaped_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Companion to the wiring test above. Two independent reasons it
    passes: (1) the runtime kill switch
    (``GRAPH_ROUTING_RUNTIME_FLAG``) already defaults off on its own,
    unrelated to whether the collaborators get wired; and (2) the routing
    gate is an ``and`` of collaborator-presence and this flag, so even
    after the collaborators reach the constructor, an operator
    still has to explicitly opt in for the route to activate. This guards
    against a fix that makes the route always-on instead of genuinely
    flag-controlled.
    """

    monkeypatch.delenv(GRAPH_ROUTING_RUNTIME_FLAG, raising=False)
    assert graph_routing_runtime_enabled() is False
