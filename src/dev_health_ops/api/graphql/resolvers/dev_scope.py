"""GraphQL adapter for the shared Ask Dev scope-resolution service."""

from __future__ import annotations

import hashlib
import json

import strawberry

from dev_health_ops.api.dev.scope_catalog import ClickHouseAuthorizedEntityCatalog
from dev_health_ops.api.dev.scope_service import (
    EntityKind,
    ScopeRequestCache,
    ScopeResolutionService,
    ScopeSearchRequest,
)
from dev_health_ops.api.services.permissions import get_user_permissions

from ..authz import require_org_id
from ..context import GraphQLContext
from ..errors import AuthorizationError
from ..types.dev_scope import (
    DevScopeCandidate,
    DevScopeEntityKind,
    DevScopeSearchInput,
    DevScopeSearchResult,
)
from . import dev_entitlement


def permission_fingerprint(context: GraphQLContext) -> str:
    """Hash the effective server-side permission state for cache isolation."""
    user = context.user
    if user is None:
        raise AuthorizationError("Authentication required")
    permissions = sorted(get_user_permissions(user))
    if "metrics:read" not in permissions:
        raise AuthorizationError("Metrics read permission required")
    payload = json.dumps(
        {
            "org_id": context.org_id,
            "user_id": user.user_id,
            "role": user.role,
            "token_version": user.token_version,
            "impersonated_by": user.impersonated_by,
            "permissions": permissions,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _scope_service(context: GraphQLContext) -> ScopeResolutionService:
    if context.client is None:
        raise RuntimeError("Database client not available for scope search")
    if context.dev_scope_cache is None:
        context.dev_scope_cache = ScopeRequestCache()
    return ScopeResolutionService(
        ClickHouseAuthorizedEntityCatalog(context.client),
        cache=context.dev_scope_cache,
    )


async def resolve_dev_scope_search(
    context: GraphQLContext, input: DevScopeSearchInput
) -> DevScopeSearchResult:
    org_id = require_org_id(context)
    fingerprint = permission_fingerprint(context)
    await dev_entitlement.require_ask_dev_entitlement(org_id)
    result = await _scope_service(context).search(
        org_id,
        fingerprint,
        ScopeSearchRequest(
            query=input.query,
            kinds=tuple(EntityKind(kind.value) for kind in input.kinds),
            limit=input.limit,
        ),
    )
    return DevScopeSearchResult(
        candidates=[
            DevScopeCandidate(
                kind=DevScopeEntityKind(candidate.kind.value),
                canonical_id=strawberry.ID(candidate.canonical_id),
                label=candidate.label,
                repository_id=strawberry.ID(candidate.repository_id)
                if candidate.repository_id
                else None,
            )
            for candidate in result.candidates
        ],
        query_version=result.query_version,
        catalog_watermark=result.catalog_watermark,
    )
