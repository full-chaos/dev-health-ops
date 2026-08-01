from __future__ import annotations

import asyncio
from copy import deepcopy
from typing import cast

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevScope,
    DevToolRequest,
    DevToolResult,
    ToolID,
)
from dev_health_ops.api.dev.orchestrator import DevOrchestrator
from dev_health_ops.api.dev.tool_registry import (
    AskDevToolRegistry,
    ToolExecutionCancelled,
    ToolExecutionContext,
    ToolExecutionTimedOut,
    ToolRequestRejected,
    UnknownToolError,
)
from dev_health_ops.llm.providers.openai_capabilities import build_wire_tool_name_map


def _request(**updates: object) -> DevToolRequest:
    payload = deepcopy(positive_fixtures()["dev_tool_request.v1"])
    payload.update(updates)
    return DevToolRequest.model_validate(payload)


def _context(*, remaining_seconds: float = 1.0) -> ToolExecutionContext:
    return ToolExecutionContext(
        org_id="org_fullchaos",
        user_id="user_01",
        permission_fingerprint="permissions_01",
        authorized_scope=DevScope.model_validate(positive_fixtures()["dev_scope.v1"]),
        cancellation=asyncio.Event(),
        remaining_seconds=remaining_seconds,
    )


def _registry(executor=None) -> AskDevToolRegistry:
    async def success(_context, request: DevToolRequest) -> DevToolResult:
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        return DevToolResult.model_validate(payload)

    selected = executor or success
    return AskDevToolRegistry({tool_id: selected for tool_id in ToolID})


def test_manifest_is_the_exact_nine_tool_server_allowlist() -> None:
    manifest = _registry().manifest()
    tools = cast(list[dict[str, object]], manifest["tools"])
    assert manifest["version"] == "ask_dev_tools.v2"
    assert [item["tool_id"] for item in tools] == sorted(item.value for item in ToolID)
    assert len(tools) == 9
    assert all(item["required_permission"] == "ask_dev:read" for item in tools)
    assert all(
        item["scope_policy"] == "exact_server_authorized_scope" for item in tools
    )


def test_registry_construction_asserts_wire_tool_name_collision_freedom() -> None:
    """CHAOS-3286: constructing the real V1 registry proves, at build time,
    that none of its nine dotted tool_ids sanitize to the same OpenAI wire
    function name. If a future tool_id addition ever collided, registry
    construction itself would raise -- not just a defensive request-time
    check inside the wire adapter.
    """
    assert build_wire_tool_name_map(item.value for item in ToolID) == {
        "resolve_scope_v1": "resolve_scope.v1",
        "list_metrics_v1": "list_metrics.v1",
        "query_metric_v1": "query_metric.v1",
        "status_snapshot_v1": "status_snapshot.v1",
        "change_summary_v1": "change_summary.v1",
        "work_graph_neighbors_v1": "work_graph_neighbors.v1",
        "search_evidence_v1": "search_evidence.v1",
        "get_evidence_v1": "get_evidence.v1",
        "data_health_v1": "data_health.v1",
    }
    # The registry constructor itself performs this same assertion; proving
    # it doesn't raise is the build-time guarantee this test exists for.
    _registry()


def test_unknown_and_cross_tenant_tools_are_rejected_before_execution() -> None:
    registry = _registry()
    with pytest.raises(UnknownToolError, match="not registered"):
        registry.definition("shell.v1")

    request = _request()
    cross_tenant = _context()
    object.__setattr__(cross_tenant, "org_id", "another_org")
    with pytest.raises(ToolRequestRejected, match="authenticated tenant"):
        registry.validate_request(request, cross_tenant)


def test_only_the_committed_scope_is_authorized_not_an_uncommitted_sibling() -> None:
    """CHAOS-3301 manifest property: "tool authorization admits only
    committed member scopes."

    D1 means no cohort ever reaches tool execution in this issue, so there
    is no e2e multi-scope authorization path to exercise the property
    against. This is the unit-level form instead: the registry's equality
    gate (``tool_registry.py:268``) admits exactly the one scope the server
    actually committed for this run, and rejects a structurally valid
    sibling scope (e.g. another repository that could equally have been
    named) that was never committed — never "close enough."
    """
    committed = DevScope.model_validate(positive_fixtures()["dev_scope.v1"])
    sibling = committed.model_copy(
        update={
            "repositories": ["repo_sibling_never_committed"],
            "surface_context": None,
        }
    )
    context = ToolExecutionContext(
        org_id="org_fullchaos",
        user_id="user_01",
        permission_fingerprint="permissions_01",
        authorized_scope=committed,
        cancellation=asyncio.Event(),
        remaining_seconds=1.0,
    )
    registry = _registry()

    # The committed scope itself is authorized.
    registry.validate_request(
        _request(scope=committed.model_dump(mode="json")), context
    )

    # A structurally valid, never-committed sibling is rejected.
    with pytest.raises(ToolRequestRejected, match="server-authorized scope"):
        registry.validate_request(
            _request(scope=sibling.model_dump(mode="json")), context
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"tool_id": "query_metric.v1", "metric_id": None},
        {"tool_id": "search_evidence.v1", "metric_id": None, "query": None},
        {
            "tool_id": "get_evidence.v1",
            "metric_id": None,
            "query": None,
            "evidence_ref_ids": [],
        },
        {"tool_id": "status_snapshot.v1", "metric_id": None, "query": "SQL"},
        # CHAOS-3262: list_metrics.v1 is a pure catalog read and must reject
        # every optional argument, including ones the generic five-key
        # allowlist otherwise lets through for other tools.
        {
            "tool_id": "list_metrics.v1",
            "metric_id": None,
            "query": "which metrics",
        },
        {"tool_id": "list_metrics.v1", "metric_id": "items_completed"},
        {
            "tool_id": "list_metrics.v1",
            "metric_id": None,
            "evidence_ref_ids": ["evidence_01"],
        },
        {"tool_id": "list_metrics.v1", "metric_id": None},  # include_comparison=True
        {
            "tool_id": "work_graph_neighbors.v1",
            "metric_id": None,
            "query": "meridian/web-app",
        },
        {"tool_id": "work_graph_neighbors.v1", "metric_id": None},
        {
            "tool_id": "data_health.v1",
            "metric_id": None,
            "evidence_ref_ids": ["evidence_01"],
        },
        {"tool_id": "resolve_scope.v1", "metric_id": None},  # include_comparison=True
    ],
)
def test_tool_specific_fields_fail_closed(updates: dict[str, object]) -> None:
    with pytest.raises(ToolRequestRejected):
        _registry().validate_request(_request(**updates), _context())


@pytest.mark.asyncio
async def test_list_metrics_accepts_a_bare_limit_request() -> None:
    request = _request(
        tool_id="list_metrics.v1",
        metric_id=None,
        query=None,
        evidence_ref_ids=[],
        include_comparison=False,
        limit=8,
    )
    execution = await _registry().execute(request, _context())
    assert execution.result.tool_id is ToolID.LIST_METRICS


def test_every_advertised_tool_accepts_its_own_schema_valid_request() -> None:
    """CHAOS-3262 drift guard: every advertised tool must have a
    ``validate_request`` branch that accepts exactly the arguments its own
    provider-facing schema (``DevOrchestrator._provider_tool_input_schema``)
    tells the model it may send. If a tool's schema and its server-side
    validation ever disagree again, this test fails instead of a live model
    silently hitting ``tool_unavailable`` before any tool executes.
    """
    registry = _registry()
    for tool_id in ToolID:
        definition = registry.definition(tool_id)
        schema = DevOrchestrator._provider_tool_input_schema(
            tool_id, definition.max_items
        )
        accepted = set(schema["properties"]) - {"limit"}
        # The model can only ever choose from the advertised enum, which may
        # be tighter than the tool's own max_items (e.g. status_snapshot.v1
        # advertises up to dev_tool_request.v1's wire-level limit ceiling,
        # not its registered max_items=100).
        max_advertised_limit = max(schema["properties"]["limit"]["enum"])
        values: dict[str, object] = {
            "tool_id": tool_id.value,
            "limit": max_advertised_limit,
            "query": "meridian/web-app" if "query" in accepted else None,
            "metric_id": "items_completed" if "metric_id" in accepted else None,
            "evidence_ref_ids": (
                ["evidence_01"] if "evidence_ref_ids" in accepted else []
            ),
            "include_comparison": "include_comparison" in accepted,
        }
        request = _request(**values)
        registry.validate_request(request, _context())


@pytest.mark.asyncio
async def test_execution_recomputes_bytes_and_preserves_canonical_ids() -> None:
    execution = await _registry().execute(_request(), _context())
    assert execution.result.run_id == "run_01"
    assert execution.result.tool_id is ToolID.QUERY_METRIC
    assert 0 < execution.serialized_bytes <= 65_536
    assert execution.latency_ms >= 0


@pytest.mark.asyncio
async def test_cancellation_prevents_executor_entry() -> None:
    entered = False

    async def executor(_context, _request):
        nonlocal entered
        entered = True
        raise AssertionError("cancelled request executed")

    context = _context()
    context.cancellation.set()
    with pytest.raises(ToolExecutionCancelled):
        await _registry(executor).execute(_request(), context)
    assert entered is False


@pytest.mark.asyncio
async def test_timeout_cancels_in_flight_executor() -> None:
    cancelled = asyncio.Event()

    async def executor(_context, _request):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    with pytest.raises(ToolExecutionTimedOut):
        await _registry(executor).execute(_request(), _context(remaining_seconds=0.01))
    assert cancelled.is_set()
