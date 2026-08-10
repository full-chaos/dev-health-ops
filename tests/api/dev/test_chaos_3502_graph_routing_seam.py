"""CHAOS-3502: the graph-assisted routing seam's flag scaffolding.

Landed ahead of the actual routing branch (which gets its own design note
on CHAOS-3660 -- exact hook point in ``run()``, ``ScopeResolveRequest``
derivation, per-``GraphQueryOutcome`` fallback semantics, interaction with
the ``plan_executor`` branch -- for sign-off before implementation) --
mirrors CHAOS-3567's own "flag-off scaffolding first, consuming logic
later" pattern. Covers only what exists today:

* the runtime kill switch (``orchestrator.graph_routing_runtime_enabled``),
  independent of the organization feature-policy gate;
* the organization feature-policy gate itself
  (``licensing.registry.ASK_DEV_GRAPH_ROUTING_FEATURE``), denied by default
  on every tier;
* ``DevOrchestrator`` accepting ``graph_investigation_query`` and
  ``evidence_service`` (CHAOS-3502 increment 2c) without any behavior
  change -- ``run()`` does not read either attribute yet, so there is
  nothing for it to change.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevContractVersions,
    DevScopeResolution,
    DevToolResult,
    ToolID,
)
from dev_health_ops.api.dev.evidence_service import (
    EvidenceReferenceSigner,
    EvidenceService,
)
from dev_health_ops.api.dev.orchestrator import (
    GRAPH_ROUTING_RUNTIME_FLAG,
    DevOrchestrator,
    graph_routing_runtime_enabled,
)
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from dev_health_ops.licensing.registry import (
    ASK_DEV_GRAPH_ROUTING_FEATURE,
    EXPLICIT_PURCHASE_FEATURES,
    get_features_for_tier,
)
from dev_health_ops.licensing.types import LicenseTier
from tests._chaos_3502_graph_investigation_fake import FakeGraphInvestigationQuery

_EVIDENCE_SIGNING_SECRET = "chaos-3683-test-secret-at-least-thirty-two-bytes-long"


class _Entitlement:
    async def require(self, _org_id: str) -> None:
        return None


class _Authorizer:
    async def resolve(self, _org_id: str, _permission_fingerprint: str, _request):
        raise NotImplementedError("construction-only smoke test; never called")


def _minimal_evidence_service() -> EvidenceService:
    return EvidenceService(
        entitlement=_Entitlement(),
        authorizer=_Authorizer(),
        signer=EvidenceReferenceSigner(_EVIDENCE_SIGNING_SECRET),
        native_adapters=(),
    )


def _minimal_registry() -> AskDevToolRegistry:
    async def execute(_context, request):
        payload = dict(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        return DevToolResult.model_validate(payload)

    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


def _minimal_orchestrator_kwargs() -> dict:
    async def resolve(**_values) -> DevScopeResolution:
        return DevScopeResolution.model_validate(
            positive_fixtures()["dev_scope_resolution.v1"]
        )

    return {
        "provider": object(),  # AgentLLMProvider is structural; never called here
        "provider_source": "platform",
        "provider_family": "test",
        "registry": _minimal_registry(),
        "scope_resolver": resolve,
        "versions": DevContractVersions.model_validate(
            positive_fixtures()["dev_answer.v1"]["versions"]
        ),
    }


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (None, False),
        ("", False),
        ("0", False),
        ("false", False),
        ("1", True),
    ],
)
def test_graph_routing_runtime_flag_defaults_off(
    monkeypatch: pytest.MonkeyPatch, raw_value: str | None, expected: bool
) -> None:
    if raw_value is None:
        monkeypatch.delenv(GRAPH_ROUTING_RUNTIME_FLAG, raising=False)
    else:
        monkeypatch.setenv(GRAPH_ROUTING_RUNTIME_FLAG, raw_value)
    assert graph_routing_runtime_enabled() is expected


def test_graph_routing_is_an_explicit_purchase_feature_denied_by_default() -> None:
    assert ASK_DEV_GRAPH_ROUTING_FEATURE in EXPLICIT_PURCHASE_FEATURES
    for tier in LicenseTier.__members__.values():
        features = get_features_for_tier(tier)
        assert features[ASK_DEV_GRAPH_ROUTING_FEATURE] is False


def test_orchestrator_accepts_a_graph_investigation_query_collaborator() -> None:
    """Construction-only smoke test: the parameter exists, defaults to
    ``None`` (the flag-off path, mirroring every other optional collaborator
    on this constructor), and accepting a real value does not raise. No
    behavior assertion here -- ``run()`` does not read
    ``self._graph_investigation_query`` yet, so there is nothing to observe
    changing; that lands with the routing branch itself.
    """

    kwargs = _minimal_orchestrator_kwargs()
    default = DevOrchestrator(**kwargs)
    assert default._graph_investigation_query is None

    with_query = DevOrchestrator(
        **kwargs, graph_investigation_query=FakeGraphInvestigationQuery()
    )
    assert isinstance(
        with_query._graph_investigation_query, FakeGraphInvestigationQuery
    )


def test_orchestrator_accepts_an_evidence_service_collaborator() -> None:
    """Construction-only smoke test, same shape as the
    ``graph_investigation_query`` one above (CHAOS-3502 increment 2c):
    the parameter exists, defaults to ``None`` (flag-off), and accepting a
    real ``EvidenceService`` does not raise. ``run()`` does not read
    ``self._evidence_service`` yet -- that lands with the routing branch
    that calls ``evidence_service.admit()`` on packet-extracted candidates,
    which gets its own design note on CHAOS-3660 before implementation.
    """

    kwargs = _minimal_orchestrator_kwargs()
    default = DevOrchestrator(**kwargs)
    assert default._evidence_service is None

    service = _minimal_evidence_service()
    with_service = DevOrchestrator(**kwargs, evidence_service=service)
    assert with_service._evidence_service is service
