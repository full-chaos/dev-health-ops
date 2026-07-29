"""Named V1 tool facade over shared evidence and data-health services.

Wave 2's registry may expose these methods to the bounded orchestrator. The
facade contains no model, GraphQL, HTTP, MCP, SQL, or URL-fetch authority.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ..contracts import DevEvidenceRef
from ..data_health_service import DataHealthResult, DataHealthService
from ..evidence_service import (
    EvidenceExpansionResult,
    EvidenceSearchResult,
    EvidenceService,
)
from ..scope_service import ScopeResolveRequest


@dataclass(frozen=True, slots=True)
class AskDevEvidenceTools:
    evidence_service: EvidenceService
    data_health_service: DataHealthService

    async def search_evidence_v1(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope_request: ScopeResolveRequest,
        query: str,
        limit: int = 25,
    ) -> EvidenceSearchResult:
        return await self.evidence_service.search(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            scope_request=scope_request,
            query=query,
            limit=limit,
        )

    async def get_evidence_v1(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope_request: ScopeResolveRequest,
        evidence: Sequence[DevEvidenceRef],
    ) -> EvidenceExpansionResult:
        return await self.evidence_service.expand(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            scope_request=scope_request,
            evidence=evidence,
        )

    async def data_health_v1(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        scope_request: ScopeResolveRequest,
        required_sources: Sequence[str],
    ) -> DataHealthResult:
        return await self.data_health_service.inspect(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            scope_request=scope_request,
            required_sources=required_sources,
        )
