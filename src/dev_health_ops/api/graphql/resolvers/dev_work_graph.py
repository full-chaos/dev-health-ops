"""GraphQL adapter for the shared bounded Ask Dev work-graph service."""

from __future__ import annotations

from typing import cast

import strawberry

from dev_health_ops.api.dev.entitlement import (
    AskDevEntitlementDeniedError,
    CanonicalAskDevEntitlementAuthorizer,
)
from dev_health_ops.api.dev.work_graph_neighbors_service import (
    ClickHouseWorkGraphNeighborSource,
    GraphDirection,
    WorkGraphNeighborsRequest,
    WorkGraphNeighborsResult,
    WorkGraphNeighborsService,
    WorkGraphRootRef,
)

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.dev_work_graph import (
    DevWorkGraphDirection,
    DevWorkGraphNeighborEdge,
    DevWorkGraphNeighborNode,
    DevWorkGraphNeighborsInput,
    DevWorkGraphNeighborsResult,
    DevWorkGraphSourceRef,
)
from .dev_evidence import (
    _entitlement_denied,
    _postgres_session,
    _scope_request,
    _scope_service,
)
from .dev_scope import permission_fingerprint


async def resolve_dev_work_graph_neighbors(
    context: GraphQLContext, input: DevWorkGraphNeighborsInput
) -> DevWorkGraphNeighborsResult:
    org_id = require_org_id(context)
    fingerprint = permission_fingerprint(context)
    if context.client is None:
        raise RuntimeError("Database client not available for Ask Dev work graph")
    request = WorkGraphNeighborsRequest(
        scope_request=_scope_request(context, input.scope),
        root_refs=tuple(
            WorkGraphRootRef(item.node_type, str(item.node_id))
            for item in input.root_refs
        ),
        relationship_types=tuple(input.relationship_types),
        direction=GraphDirection(input.direction.value),
        limit=input.limit,
        depth=input.depth,
    )
    try:
        injected = getattr(context, "dev_work_graph_service", None)
        if injected is not None:
            service = cast(WorkGraphNeighborsService, injected)
            result = await service.neighbors(
                org_id=org_id,
                permission_fingerprint=fingerprint,
                request=request,
            )
        else:
            async with _postgres_session(context) as session:
                service = WorkGraphNeighborsService(
                    ClickHouseWorkGraphNeighborSource(context.client),
                    CanonicalAskDevEntitlementAuthorizer(session),
                    _scope_service(context),
                )
                result = await service.neighbors(
                    org_id=org_id,
                    permission_fingerprint=fingerprint,
                    request=request,
                )
    except AskDevEntitlementDeniedError as exc:
        raise _entitlement_denied(exc) from exc
    return _result(result)


def _result(result: WorkGraphNeighborsResult) -> DevWorkGraphNeighborsResult:
    return DevWorkGraphNeighborsResult(
        schema_version=result.schema_version,
        state=result.state.value,
        nodes=[
            DevWorkGraphNeighborNode(
                node_type=item.node_type,
                node_id=strawberry.ID(item.node_id),
                display_label=item.display_label,
                resolution_state=item.resolution_state,
                repository_id=strawberry.ID(item.repository_id)
                if item.repository_id
                else None,
            )
            for item in result.nodes
        ],
        edges=[
            DevWorkGraphNeighborEdge(
                edge_id=strawberry.ID(item.edge_id),
                source_type=item.source_type,
                source_id=strawberry.ID(item.source_id),
                target_type=item.target_type,
                target_id=strawberry.ID(item.target_id),
                relationship_type=item.relationship_type,
                direction=DevWorkGraphDirection(item.direction.value),
                provenance=item.provenance,
                confidence=item.confidence,
                source_ref_id=strawberry.ID(item.source_ref_id),
                evidence_ref_ids=[],
                observed_at=item.observed_at,
                freshness="source_watermark",
            )
            for item in result.edges
        ],
        source_refs=[
            DevWorkGraphSourceRef(
                ref_id=strawberry.ID(item.ref_id),
                source_table=item.source_table,
                source_version=item.source_version,
                watermark=item.watermark,
                query_version=item.query_version,
            )
            for item in result.source_refs
        ],
        warnings=list(result.warnings),
        total_count=result.total_count,
        returned_count=result.returned_count,
        truncated=result.truncated,
        depth=result.depth,
        query_version=result.query_version,
        watermark=result.watermark,
    )
