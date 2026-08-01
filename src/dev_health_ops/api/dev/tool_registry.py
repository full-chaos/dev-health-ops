"""Server-owned registry for the nine bounded Ask Dev V1 read tools."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass
from enum import StrEnum

from dev_health_ops.llm.providers.openai_capabilities import build_wire_tool_name_map

from .contracts import DevScope, DevToolRequest, DevToolResult, ToolID

TOOL_CONTRACT_VERSION = "ask_dev_tools.v2"
DEFAULT_TOOL_TIMEOUT_SECONDS = 15.0
MAX_TOOL_RESULT_BYTES = 65_536


class ToolRegistryError(RuntimeError):
    """Base class for safe, deterministic registry failures."""


class UnknownToolError(ToolRegistryError):
    pass


class ToolRequestRejected(ToolRegistryError):
    pass


class ToolResultRejected(ToolRegistryError):
    pass


class ToolExecutionTimedOut(ToolRegistryError):
    pass


class ToolExecutionCancelled(ToolRegistryError):
    pass


class ToolCachePolicy(StrEnum):
    REQUEST_LOCAL = "request_local"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    tool_id: ToolID
    version: str
    description: str
    required_permission: str
    scope_policy: str
    timeout_seconds: float
    max_items: int
    max_result_bytes: int
    cache_policy: ToolCachePolicy
    freshness_semantics: str
    audit_class: str

    def manifest(self) -> dict[str, object]:
        return {
            "tool_id": self.tool_id.value,
            "version": self.version,
            "description": self.description,
            "required_permission": self.required_permission,
            "scope_policy": self.scope_policy,
            "timeout_seconds": self.timeout_seconds,
            "max_items": self.max_items,
            "max_result_bytes": self.max_result_bytes,
            "cache_policy": self.cache_policy.value,
            "freshness_semantics": self.freshness_semantics,
            "audit_class": self.audit_class,
            "input_schema_ref": "dev_tool_request.v1",
            "output_schema_ref": "dev_tool_result.v1",
        }


@dataclass(frozen=True, slots=True)
class ToolExecutionContext:
    org_id: str
    user_id: str
    permission_fingerprint: str
    authorized_scope: DevScope
    cancellation: asyncio.Event
    remaining_seconds: float

    def __post_init__(self) -> None:
        if not self.org_id or not self.user_id or not self.permission_fingerprint:
            raise ValueError(
                "authenticated tenant, user, and permission fingerprint required"
            )
        if self.remaining_seconds <= 0:
            raise ValueError("remaining tool budget must be positive")


@dataclass(frozen=True, slots=True)
class ToolExecution:
    result: DevToolResult
    serialized_bytes: int
    latency_ms: int


ToolExecutor = Callable[
    [ToolExecutionContext, DevToolRequest], Awaitable[DevToolResult]
]


def _definition(
    tool_id: ToolID,
    description: str,
    *,
    max_items: int,
    cache_policy: ToolCachePolicy = ToolCachePolicy.REQUEST_LOCAL,
    freshness_semantics: str = "source_specific",
    audit_class: str = "authorized_query",
) -> ToolDefinition:
    return ToolDefinition(
        tool_id=tool_id,
        version=tool_id.value,
        description=description,
        required_permission="ask_dev:read",
        scope_policy="exact_server_authorized_scope",
        timeout_seconds=DEFAULT_TOOL_TIMEOUT_SECONDS,
        max_items=max_items,
        max_result_bytes=MAX_TOOL_RESULT_BYTES,
        cache_policy=cache_policy,
        freshness_semantics=freshness_semantics,
        audit_class=audit_class,
    )


TOOL_DEFINITIONS: tuple[ToolDefinition, ...] = (
    _definition(
        ToolID.RESOLVE_SCOPE,
        "Resolve visible user scope to authorized canonical identifiers.",
        max_items=25,
        freshness_semantics="tenant_catalog_watermark",
        audit_class="scope_resolution",
    ),
    _definition(
        ToolID.LIST_METRICS,
        "List the exact registered Ask Dev metrics eligible for the scope.",
        max_items=8,
        freshness_semantics="code_owned_registry",
        audit_class="metric_catalog",
    ),
    _definition(
        ToolID.QUERY_METRIC,
        "Query one registered metric through its bounded canonical service.",
        max_items=12,
        audit_class="metric_query",
    ),
    _definition(
        ToolID.STATUS_SNAPSHOT,
        "Build a deterministic evidence-backed status snapshot.",
        max_items=100,
        audit_class="status_query",
    ),
    _definition(
        ToolID.CHANGE_SUMMARY,
        "Return observed changes between the resolved comparison windows.",
        max_items=100,
        audit_class="change_query",
    ),
    _definition(
        ToolID.WORK_GRAPH_NEIGHBORS,
        "Return depth-one canonical work-graph relationships.",
        max_items=100,
        audit_class="graph_query",
    ),
    _definition(
        ToolID.SEARCH_EVIDENCE,
        "Search bounded authorized evidence through existing structured indexes.",
        max_items=25,
        audit_class="evidence_search",
    ),
    _definition(
        ToolID.GET_EVIDENCE,
        "Expand up to ten opaque authorized evidence references.",
        max_items=10,
        cache_policy=ToolCachePolicy.NONE,
        audit_class="evidence_expansion",
    ),
    _definition(
        ToolID.DATA_HEALTH,
        "Report source-specific coverage, freshness, and availability.",
        max_items=25,
        freshness_semantics="source_policy_and_watermark",
        audit_class="data_health_query",
    ),
)


class AskDevToolRegistry:
    """Exact allowlist and execution choke point for model-requested tools."""

    def __init__(self, executors: Mapping[ToolID, ToolExecutor]) -> None:
        definitions = {item.tool_id: item for item in TOOL_DEFINITIONS}
        expected = set(ToolID)
        if set(definitions) != expected:
            raise RuntimeError("tool metadata must cover the exact V1 registry")
        # Explicitly rejected at registry build time (CHAOS-3286): the
        # OpenAI-compatible adapter sanitizes each dotted tool_id into a
        # wire-legal function name; this asserts the real V1 registry can
        # never collide under that mapping, rather than only ever being
        # caught defensively at request time.
        build_wire_tool_name_map(item.value for item in ToolID)
        if set(executors) != expected:
            missing = sorted(item.value for item in expected - set(executors))
            extra = sorted(str(item) for item in set(executors) - expected)
            raise ValueError(
                f"tool executors must match V1 registry; missing={missing}, extra={extra}"
            )
        self._definitions = definitions
        self._executors = dict(executors)

    def definition(self, tool_id: ToolID | str) -> ToolDefinition:
        try:
            canonical = tool_id if isinstance(tool_id, ToolID) else ToolID(tool_id)
        except ValueError as exc:
            raise UnknownToolError("tool is not registered") from exc
        return self._definitions[canonical]

    def manifest(
        self, *, allowed_tool_ids: Collection[ToolID] | None = None
    ) -> dict[str, object]:
        """The registry manifest, optionally narrowed to a per-run allowlist.

        Until CHAOS-3292 there was no per-run tool-availability seam at all:
        every registered tool was advertised on every round, to every question.
        ``allowed_tool_ids`` is that seam. ``None`` means "advertise everything",
        which is both the default and the behaviour when no preflight ran.
        """

        allowed = set(ToolID) if allowed_tool_ids is None else set(allowed_tool_ids)
        unknown = sorted(str(item) for item in allowed - set(ToolID))
        if unknown:
            raise ToolRegistryError(
                f"tool allowlist names unregistered tools {unknown}"
            )
        return {
            "version": TOOL_CONTRACT_VERSION,
            "schemas": {
                "dev_tool_request.v1": DevToolRequest.model_json_schema(
                    mode="validation"
                ),
                "dev_tool_result.v1": DevToolResult.model_json_schema(
                    mode="validation"
                ),
            },
            "tools": [
                self._definitions[tool_id].manifest()
                for tool_id in sorted(ToolID, key=lambda item: item.value)
                if tool_id in allowed
            ],
        }

    def validate_request(
        self, request: DevToolRequest, context: ToolExecutionContext
    ) -> ToolDefinition:
        definition = self.definition(request.tool_id)
        if request.scope.organization_id != context.org_id:
            raise ToolRequestRejected("tool scope does not match authenticated tenant")
        if request.scope != context.authorized_scope:
            raise ToolRequestRejected(
                "tool scope does not match server-authorized scope"
            )
        if request.limit > definition.max_items:
            raise ToolRequestRejected("tool request exceeds registered item limit")

        if request.tool_id is ToolID.QUERY_METRIC:
            if request.metric_id is None or request.query or request.evidence_ref_ids:
                raise ToolRequestRejected("metric query fields are invalid")
        elif request.tool_id is ToolID.SEARCH_EVIDENCE:
            if (
                not request.query
                or request.metric_id
                or request.evidence_ref_ids
                or request.include_comparison
            ):
                raise ToolRequestRejected("evidence search fields are invalid")
        elif request.tool_id is ToolID.GET_EVIDENCE:
            if (
                not request.evidence_ref_ids
                or len(request.evidence_ref_ids) > definition.max_items
                or request.query
                or request.metric_id
                or request.include_comparison
            ):
                raise ToolRequestRejected("evidence expansion fields are invalid")
        elif request.tool_id is ToolID.RESOLVE_SCOPE:
            if (
                request.metric_id
                or request.evidence_ref_ids
                or request.include_comparison
            ):
                raise ToolRequestRejected("scope resolution fields are invalid")
        elif request.tool_id is ToolID.LIST_METRICS:
            # TRD 8.2: list_metrics.v1 takes no arguments beyond the bounded
            # item limit already checked above; it is a pure catalog read.
            if (
                request.query
                or request.metric_id
                or request.evidence_ref_ids
                or request.include_comparison
            ):
                raise ToolRequestRejected("metric catalog fields are invalid")
        elif request.tool_id in {ToolID.STATUS_SNAPSHOT, ToolID.CHANGE_SUMMARY}:
            if request.query or request.metric_id or request.evidence_ref_ids:
                raise ToolRequestRejected(
                    "tool request contains fields outside its contract"
                )
        elif request.tool_id in {ToolID.WORK_GRAPH_NEIGHBORS, ToolID.DATA_HEALTH}:
            if (
                request.query
                or request.metric_id
                or request.evidence_ref_ids
                or request.include_comparison
            ):
                raise ToolRequestRejected(
                    "tool request contains fields outside its contract"
                )
        else:  # pragma: no cover - exhaustive over the nine registered ToolID members
            raise ToolRequestRejected(
                "tool request contains fields outside its contract"
            )
        return definition

    async def execute(
        self, request: DevToolRequest, context: ToolExecutionContext
    ) -> ToolExecution:
        definition = self.validate_request(request, context)
        if context.cancellation.is_set():
            raise ToolExecutionCancelled("tool execution cancelled")

        timeout_seconds = min(definition.timeout_seconds, context.remaining_seconds)
        started = time.monotonic()

        async def run_executor() -> DevToolResult:
            return await self._executors[request.tool_id](context, request)

        async def wait_for_cancellation() -> DevToolResult:
            await context.cancellation.wait()
            raise ToolExecutionCancelled("tool execution cancelled")

        execution_task = asyncio.create_task(run_executor())
        cancellation_task = asyncio.create_task(wait_for_cancellation())
        try:
            done, _ = await asyncio.wait(
                {execution_task, cancellation_task},
                timeout=timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if context.cancellation.is_set():
                execution_task.cancel()
                await asyncio.gather(execution_task, return_exceptions=True)
                raise ToolExecutionCancelled("tool execution cancelled")
            if execution_task not in done:
                execution_task.cancel()
                await asyncio.gather(execution_task, return_exceptions=True)
                raise ToolExecutionTimedOut("tool execution timed out")
            result = next(iter(done)).result()
        finally:
            execution_task.cancel()
            cancellation_task.cancel()
            await asyncio.gather(
                execution_task, cancellation_task, return_exceptions=True
            )

        if (
            result.run_id != request.run_id
            or result.tool_call_id != request.tool_call_id
        ):
            raise ToolResultRejected("tool result identifiers do not match request")
        if result.tool_id is not request.tool_id:
            raise ToolResultRejected("tool result type does not match request")
        serialized = json.dumps(
            result.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if len(serialized) > definition.max_result_bytes:
            raise ToolResultRejected("tool result exceeds registered byte limit")
        if result.serialized_bytes > definition.max_result_bytes:
            raise ToolResultRejected("tool result reports an invalid byte count")
        return ToolExecution(
            result=result,
            serialized_bytes=len(serialized),
            latency_ms=max(0, round((time.monotonic() - started) * 1000)),
        )


__all__ = [
    "AskDevToolRegistry",
    "TOOL_CONTRACT_VERSION",
    "TOOL_DEFINITIONS",
    "ToolDefinition",
    "ToolExecution",
    "ToolExecutionCancelled",
    "ToolExecutionContext",
    "ToolExecutionTimedOut",
    "ToolRegistryError",
    "ToolRequestRejected",
    "ToolResultRejected",
    "UnknownToolError",
]
