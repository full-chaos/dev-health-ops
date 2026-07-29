"""Agent-safe facade over the shared bounded work-graph service."""

from __future__ import annotations

from dataclasses import dataclass

from ..work_graph_neighbors_service import (
    WorkGraphNeighborsRequest,
    WorkGraphNeighborsResult,
    WorkGraphNeighborsService,
)


@dataclass(frozen=True, slots=True)
class AskDevWorkGraphTools:
    service: WorkGraphNeighborsService

    async def work_graph_neighbors_v1(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        request: WorkGraphNeighborsRequest,
    ) -> WorkGraphNeighborsResult:
        return await self.service.neighbors(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            request=request,
        )
