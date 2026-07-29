"""GraphQL adapters over the shared Ask Dev evidence application services."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import strawberry

from dev_health_ops.api.dev.data_health_service import (
    NATIVE_EVIDENCE_SOURCES,
    DataHealthService,
    NativeDataHealthReader,
)
from dev_health_ops.api.dev.entitlement import (
    AskDevEntitlementAuthorizer,
    AskDevEntitlementDeniedError,
    CanonicalAskDevEntitlementAuthorizer,
)
from dev_health_ops.api.dev.evidence_service import (
    EvidenceReferenceSigner,
    EvidenceService,
)
from dev_health_ops.api.dev.native_evidence import native_evidence_adapters
from dev_health_ops.api.dev.scope_catalog import ClickHouseAuthorizedEntityCatalog
from dev_health_ops.api.dev.scope_service import (
    EntityKind,
    ScopeRef,
    ScopeRequestCache,
    ScopeResolutionService,
    ScopeResolveRequest,
    TimeRangeRequest,
)

from ..authz import require_org_id
from ..context import GraphQLContext
from ..errors import AuthorizationError
from ..types.dev_evidence import (
    DevDataHealthInput,
    DevDataHealthResult,
    DevDataHealthSource,
    DevEvidence,
    DevEvidenceFlags,
    DevEvidenceLink,
    DevEvidenceScopeInput,
    DevEvidenceSearchInput,
    DevEvidenceSearchResult,
    DevEvidenceSourceState,
)
from .dev_scope import permission_fingerprint


def _scope_service(context: GraphQLContext) -> ScopeResolutionService:
    if context.client is None:
        raise RuntimeError("Database client not available for Ask Dev evidence")
    if context.dev_scope_cache is None:
        context.dev_scope_cache = ScopeRequestCache()
    return ScopeResolutionService(
        ClickHouseAuthorizedEntityCatalog(context.client),
        cache=context.dev_scope_cache,
    )


def _scope_request(
    context: GraphQLContext, input: DevEvidenceScopeInput
) -> ScopeResolveRequest:
    direct = EntityKind(input.direct_scope.value)
    refs = tuple(
        ScopeRef(EntityKind(item.kind.value), item.value) for item in input.refs
    )
    if direct is EntityKind.ORGANIZATION:
        if refs:
            raise ValueError("Organization evidence scope does not accept refs")
        refs = (ScopeRef(EntityKind.ORGANIZATION, require_org_id(context)),)
    elif not refs:
        raise ValueError("Non-organization evidence scope requires refs")
    elif any(ref.kind is not direct for ref in refs):
        raise ValueError("Evidence scope refs must match direct_scope")
    time_range = TimeRangeRequest(
        preset_days=input.preset_days,
        start_date=input.start_date,
        end_date=input.end_date,
        timezone=input.timezone,
    )
    return ScopeResolveRequest(
        explicit_refs=refs,
        team_filter_refs=tuple(
            ScopeRef(EntityKind.TEAM, value) for value in input.team_ids
        ),
        time_range=time_range,
        allow_organization_fallback=False,
    )


def _signer() -> EvidenceReferenceSigner:
    secret = os.getenv("JWT_SECRET_KEY")
    if not secret:
        raise RuntimeError("Ask Dev evidence signing is unavailable")
    return EvidenceReferenceSigner(secret)


def _evidence_service(
    context: GraphQLContext,
    entitlement: AskDevEntitlementAuthorizer,
) -> EvidenceService:
    if context.client is None:
        raise RuntimeError("Database client not available for Ask Dev evidence")
    scope_service = _scope_service(context)
    return EvidenceService(
        entitlement=entitlement,
        authorizer=scope_service,
        signer=_signer(),
        native_adapters=native_evidence_adapters(context.client),
        # ACR is optional and has no Ops-side evidence transport today. An
        # existing-contract adapter may be injected here when configured; no
        # MCP path or fallback source is created by this resolver.
        acr_adapter=None,
    )


def _require_reader(context: GraphQLContext) -> None:
    if context.user is None:
        raise AuthorizationError("Authentication required")
    permission_fingerprint(context)


@asynccontextmanager
async def _postgres_session(context: GraphQLContext) -> AsyncIterator[Any]:
    existing = getattr(context, "db_session", None) or getattr(context, "session", None)
    if existing is not None:
        yield existing
        return
    from dev_health_ops.db import get_postgres_session

    async with get_postgres_session() as session:
        yield session


def _entitlement_denied(exc: AskDevEntitlementDeniedError) -> AuthorizationError:
    return AuthorizationError("Ask Dev is not available for this organization")


async def resolve_dev_evidence_search(
    context: GraphQLContext, input: DevEvidenceSearchInput
) -> DevEvidenceSearchResult:
    _require_reader(context)
    try:
        async with _postgres_session(context) as session:
            result = await _evidence_service(
                context, CanonicalAskDevEntitlementAuthorizer(session)
            ).search(
                org_id=require_org_id(context),
                permission_fingerprint=permission_fingerprint(context),
                scope_request=_scope_request(context, input.scope),
                query=input.query,
                limit=input.limit,
            )
    except AskDevEntitlementDeniedError as exc:
        raise _entitlement_denied(exc) from exc
    return DevEvidenceSearchResult(
        evidence=[_evidence(item) for item in result.evidence],
        sources=[
            DevEvidenceSourceState(
                source_system=item.source_system,
                state=item.state.value,
                watermark=item.watermark,
                warning=item.warning,
            )
            for item in result.source_states
        ],
        query_version=result.query_version,
        ranking_version=result.ranking_version,
    )


async def resolve_dev_data_health(
    context: GraphQLContext, input: DevDataHealthInput
) -> DevDataHealthResult:
    _require_reader(context)
    required_sources = tuple(input.required_sources or NATIVE_EVIDENCE_SOURCES)
    unknown = sorted(set(required_sources) - set((*NATIVE_EVIDENCE_SOURCES, "acr")))
    if unknown:
        raise ValueError("Unsupported Ask Dev data-health source")
    try:
        async with _postgres_session(context) as session:
            service = DataHealthService(
                entitlement=CanonicalAskDevEntitlementAuthorizer(session),
                authorizer=_scope_service(context),
                reader=NativeDataHealthReader(context.client, session),
            )
            result = await service.inspect(
                org_id=require_org_id(context),
                permission_fingerprint=permission_fingerprint(context),
                scope_request=_scope_request(context, input.scope),
                required_sources=required_sources,
            )
    except AskDevEntitlementDeniedError as exc:
        raise _entitlement_denied(exc) from exc
    return DevDataHealthResult(
        sources=[
            DevDataHealthSource(
                source_system=item.source_system,
                state=item.state.value,
                required=item.required,
                last_successful_at=item.last_successful_at,
                watermark=item.watermark,
                missing_repository_ids=[
                    strawberry.ID(value) for value in item.missing_repository_ids
                ],
                missing_entity_ids=[
                    strawberry.ID(value) for value in item.missing_entity_ids
                ],
                coverage=item.coverage,
                confidence_impact=item.confidence_impact,
                freshness_policy_version=item.freshness_policy_version,
                warning=item.warning,
            )
            for item in result.sources
        ],
        complete_eligible=result.complete_eligible,
        query_version=result.query_version,
    )


def _evidence(item: object) -> DevEvidence:
    # Runtime object is the canonical Pydantic DevEvidenceRef. Keeping the
    # adapter field-by-field makes GraphQL/schema drift explicit.
    link = getattr(item, "link")
    flags = getattr(item, "flags")
    return DevEvidence(
        evidence_ref_id=strawberry.ID(getattr(item, "evidence_ref_id")),
        source_system=getattr(item, "source_system"),
        source_version=getattr(item, "source_version"),
        entity_type=getattr(item, "entity_type"),
        entity_id=strawberry.ID(getattr(item, "entity_id")),
        display_label=getattr(item, "display_label"),
        link=DevEvidenceLink(
            internal_path=link.internal_path,
            source_url=link.source_url,
        )
        if link
        else None,
        observed_at=getattr(item, "observed_at"),
        freshness=getattr(item, "freshness").value,
        provenance=getattr(item, "provenance"),
        confidence=getattr(item, "confidence"),
        citation_text=getattr(item, "citation_text"),
        repository_ids=[
            strawberry.ID(value) for value in getattr(item, "repository_ids")
        ],
        valid_entity_ids=[
            strawberry.ID(value) for value in getattr(item, "valid_entity_ids")
        ],
        flags=DevEvidenceFlags(
            stale=flags.stale,
            unavailable=flags.unavailable,
            redacted=flags.redacted,
            deleted=flags.deleted,
            uncertain=flags.uncertain,
            conflicting=flags.conflicting,
            untrusted_content=flags.untrusted_content,
        ),
    )
