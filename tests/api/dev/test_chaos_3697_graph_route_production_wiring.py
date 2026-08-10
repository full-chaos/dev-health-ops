"""Issue 3697: the graph-assisted Ask Dev route is unreachable in
production, and the runtime flag is never even the deciding factor.

``BoundedDevRuntime.run()`` (``runtime.py``) constructs the real
``DevOrchestrator`` production actually serves. That constructor call
passes none of ``graph_investigation_query``, ``evidence_service``, or
``canonical_enrichment`` -- confirmed by grep over the whole ``src/`` tree:
zero keyword-argument call sites for either of the first two anywhere, and
the third does not exist as a parameter at all yet on this branch. So all
three default to ``None`` on every real request, independent of the
organization feature-policy gate, the runtime kill switch
(``graph_routing_runtime_enabled()``), or anything an operator does.

The routing gate at ``orchestrator.py``'s cohort-discovery branch reads:

    and self._graph_investigation_query is not None
    and self._evidence_service is not None
    and graph_routing_runtime_enabled()

Both identity checks are ``False`` in production, so the ``and``-chain
short-circuits before ``graph_routing_runtime_enabled()`` is ever
evaluated. Turning the runtime flag on today changes nothing.

Concretely, in ``production_runtime.py``:

* ``_assemble_production_runtime`` builds a real ``EvidenceService`` (see
  its own ``evidence_service = EvidenceService(...)`` call) -- and then
  never threads it into the ``BoundedDevRuntime`` it returns.
  ``BoundedDevRuntime`` (``runtime.py``) has no field for it at all.
* No code path anywhere in ``production_runtime.py`` constructs a
  ``GraphInvestigationQuery`` in the first place.

Existing coverage (``test_chaos_3502_graph_routing_seam.py``,
``test_chaos_3502_graph_routing_branch.py``) never catches this: every one
of those tests hand-constructs
``DevOrchestrator(**kwargs, graph_investigation_query=..., evidence_service=...)``
directly. That proves the constructor *accepts* the collaborators; it
proves nothing about whether the real composition root ever *passes* one --
which is exactly why this defect shipped invisibly. This file drives the
real path instead: ``production_runtime.build_production_runtime()`` ->
``BoundedDevRuntime.run()`` -> the real ``DevOrchestrator(...)`` call site
in ``runtime.py`` -- and inspects what THAT call actually received.
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


async def _build_runtime(monkeypatch: pytest.MonkeyPatch) -> Any:
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


#: The 14 keyword arguments runtime.py's DevOrchestrator(...) call site
#: passes today (verified against the actual source at the top of this
#: file's module docstring investigation). Used ONLY by the positive
#: control below to prove the capturing harness genuinely observed a real
#: construction -- never asserted against directly in the RED test itself,
#: which only cares about the three that are ABSENT from this set.
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
    }
)


@pytest.mark.asyncio
async def test_the_capturing_harness_genuinely_observes_a_real_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Positive control for the RED test below -- deliberately NOT xfail,
    and MUST keep passing.

    ``xfail`` swallows any failure the marked test raises, not only the one
    it is meant to demonstrate. If the interception in
    ``_capture_orchestrator_construction`` ever silently stops working --
    the ``DevOrchestrator(...)`` call site moves, gets wrapped in a
    factory, the import binding changes, or the probe exception fires
    before ``captured`` is populated -- the RED test still reports
    ``XFAIL`` (an assertion failure inside ``pytest.raises`` also
    satisfies it), which reads as "expected failure, all fine" while the
    harness has quietly stopped observing anything about production wiring
    at all. That is the exact failure shape this whole issue is about, one
    layer up: a measurement that fails toward "fine" instead of loudly.

    This test asserts the harness actually captured a construction: the
    exact 14 keyword names ``runtime.py`` really passes today, populated
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
        "interception itself is broken, which would make the RED test "
        "below meaningless (an XFAIL that observes nothing still reports "
        "XFAIL)"
    )
    assert set(captured) == _KWARGS_RUNTIME_PY_ACTUALLY_PASSES, (
        "runtime.py's DevOrchestrator(...) call site no longer passes the "
        "keyword set this harness was written against -- update "
        "_KWARGS_RUNTIME_PY_ACTUALLY_PASSES to match the real call site "
        "before trusting the RED test's absence claim again"
    )
    assert captured["registry"] is not None
    assert type(captured["registry"]).__name__ == "AskDevToolRegistry"
    assert captured["provider"] is bounded_runtime.provider, (
        "the captured provider must be the SAME instance this test's stub "
        "handed to build_production_runtime -- a different instance would "
        "mean the harness is observing a different construction than the "
        "one the real runtime performed"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Issue 3697: the graph-assisted route is unreachable in production "
        "-- BoundedDevRuntime.run() never wires graph_investigation_query, "
        "evidence_service, or canonical_enrichment into DevOrchestrator. "
        "Landing this test RED (rather than deferring it) is deliberate: it "
        "must be visible on the feature branch as a known, tracked gap, not "
        "silently absent. strict=True is load-bearing, not decorative -- it "
        "flips this to a hard CI failure the instant any leg of the fix "
        "lands without this marker being removed in the SAME change, so the "
        "fix and the test that proves it can never drift apart silently."
    ),
)
@pytest.mark.asyncio
async def test_production_runtime_never_wires_the_graph_route_collaborators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue 3697, RED (xfail-strict; see the marker above for why it is
    landed failing rather than deferred). The cohort-discovery routing gate
    requires ``graph_investigation_query``, ``evidence_service``, AND (once
    wired) ``canonical_enrichment`` to all be non-``None`` before it even
    asks ``graph_routing_runtime_enabled()``. This asserts on the ACTUAL
    ``DevOrchestrator`` the production composition root builds -- not one
    this test built by hand -- so it fails for the same reason the route
    fails to activate in production, not for an unrelated mistake in the
    test itself.

    MUST currently fail: none of the three collaborators reach the
    constructor today. ``.get()`` is used deliberately for
    ``canonical_enrichment``, which is not even a ``DevOrchestrator``
    parameter yet on this branch (concurrent, unrelated lane work) -- a
    missing key reads as ``None`` here, same as an explicit ``None``, so
    this test does not crash ahead of that landing and starts holding it to
    the same bar the moment it does.
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


def test_graph_routing_runtime_flag_defaults_off_in_a_production_shaped_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Companion to the RED test above -- NOT equivalent evidence, and this
    one legitimately passes today. Two independent reasons it will keep
    passing once the wiring gap above is fixed: (1) the runtime kill switch
    (``GRAPH_ROUTING_RUNTIME_FLAG``) already defaults off on its own,
    unrelated to whether the collaborators get wired; and (2) the routing
    gate is an ``and`` of collaborator-presence and this flag, so even
    after the collaborators start reaching the constructor, an operator
    still has to explicitly opt in for the route to activate. This guards
    against a fix that makes the route always-on instead of genuinely
    flag-controlled.
    """

    monkeypatch.delenv(GRAPH_ROUTING_RUNTIME_FLAG, raising=False)
    assert graph_routing_runtime_enabled() is False
