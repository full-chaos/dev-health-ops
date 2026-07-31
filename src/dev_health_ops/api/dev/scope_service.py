"""Server-owned authorization and entity resolution for Ask Dev.

This module is the single application-service seam used by both
``resolve_scope.v1`` and the typed GraphQL search field.  It deliberately does
not define the canonical ``dev_scope.v1`` or ``dev_scope_resolution.v1`` wire
models; CHAOS-3223 owns those contracts and adapters convert this module's
domain result at the boundary.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from enum import StrEnum
from typing import Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .contracts import (
    DevDisambiguationCandidate,
    DevEntityRef,
    DevScope,
    DevScopeResolution,
    DevSurfaceContext,
    DevTimeRange,
    DirectScope,
    EntityType,
    ScopeResolutionOutcome,
)

QUERY_VERSION = "resolve-scope.v1"
MAX_REPOSITORIES = 20
MAX_ENTITY_REFS = 20
MAX_CANDIDATES = 25
MAX_REQUEST_CACHE_ENTRIES = 128


class EntityKind(StrEnum):
    ORGANIZATION = "organization"
    REPOSITORY = "repository"
    PROJECT = "project"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"
    TEAM = "team"


DIRECT_SCOPE_KINDS = frozenset(
    {
        EntityKind.ORGANIZATION,
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
    }
)


@dataclass(frozen=True, slots=True)
class ScopeRef:
    kind: EntityKind
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, EntityKind):
            raise ValueError("Unsupported Ask Dev scope kind")
        value = self.value.strip()
        if not value or len(value) > 512:
            raise ValueError("Scope references must contain 1 to 512 characters")
        object.__setattr__(self, "value", value)


@dataclass(frozen=True, slots=True)
class AuthorizedEntity:
    kind: EntityKind
    canonical_id: str
    label: str
    repository_id: str | None = None

    def __post_init__(self) -> None:
        if not self.canonical_id or not self.label:
            raise ValueError("Authorized entities require a canonical ID and label")


@dataclass(frozen=True, slots=True)
class TimeRangeRequest:
    preset_days: int | None = 30
    start_date: date | None = None
    end_date: date | None = None
    timezone: str = "UTC"

    def __post_init__(self) -> None:
        is_absolute = self.start_date is not None or self.end_date is not None
        if is_absolute:
            if self.start_date is None or self.end_date is None:
                raise ValueError("Absolute ranges require both start_date and end_date")
            if self.start_date > self.end_date:
                raise ValueError("start_date must not be after end_date")
            # A caller constructing an absolute range may omit the preset and
            # receives the default value from this dataclass. Treat dates as
            # authoritative rather than making callers explicitly clear it.
        elif self.preset_days not in {7, 30, 90}:
            raise ValueError("Time preset must be 7, 30, or 90 days")


@dataclass(frozen=True, slots=True)
class ResolvedTimeRange:
    timezone: str
    utc_start: datetime
    utc_end: datetime
    local_start: str
    local_end: str
    comparison_utc_start: datetime
    comparison_utc_end: datetime
    comparison_local_start: str
    comparison_local_end: str


@dataclass(frozen=True, slots=True)
class ScopeResolveRequest:
    explicit_refs: tuple[ScopeRef, ...] = ()
    page_context_refs: tuple[ScopeRef, ...] = ()
    conversation_context_refs: tuple[ScopeRef, ...] = ()
    team_filter_refs: tuple[ScopeRef, ...] = ()
    time_range: TimeRangeRequest = field(default_factory=TimeRangeRequest)
    # This is an application-policy decision, never a model/browser flag.  The
    # GraphQL search adapter does not expose it.
    allow_organization_fallback: bool = False
    organization_label: str = "Organization"
    surface_route_id: str | None = None
    surface_filter_fingerprint: str | None = None

    def __post_init__(self) -> None:
        for refs in (
            self.explicit_refs,
            self.page_context_refs,
            self.conversation_context_refs,
        ):
            unsupported = [
                ref.kind for ref in refs if ref.kind not in DIRECT_SCOPE_KINDS
            ]
            if unsupported:
                raise ValueError(
                    "Team and supporting-evidence entities are not direct scopes"
                )
            repository_count = sum(ref.kind is EntityKind.REPOSITORY for ref in refs)
            if repository_count > MAX_REPOSITORIES:
                raise ValueError(f"At most {MAX_REPOSITORIES} repositories are allowed")
            if len(refs) > MAX_ENTITY_REFS:
                raise ValueError(f"At most {MAX_ENTITY_REFS} entity refs are allowed")
            if repository_count and repository_count != len(refs):
                raise ValueError("Repository sets cannot mix direct scope kinds")
            if not repository_count and len(refs) > 1:
                raise ValueError(
                    "Project, WorkUnit, issue, PR, and organization scopes are singular"
                )
        if any(ref.kind is not EntityKind.TEAM for ref in self.team_filter_refs):
            raise ValueError("team_filter_refs accepts only team references")
        if len(self.team_filter_refs) > MAX_ENTITY_REFS:
            raise ValueError(f"At most {MAX_ENTITY_REFS} team filters are allowed")
        for controlled_id in (
            self.surface_route_id,
            self.surface_filter_fingerprint,
        ):
            if controlled_id is not None and (
                not controlled_id.strip() or len(controlled_id) > 128
            ):
                raise ValueError("Surface context IDs must contain 1 to 128 characters")


@dataclass(frozen=True, slots=True)
class ScopeSearchRequest:
    query: str
    kinds: tuple[EntityKind, ...]
    limit: int = MAX_CANDIDATES

    def __post_init__(self) -> None:
        query = self.query.strip()
        if not query or len(query) > 256:
            raise ValueError("Search query must contain 1 to 256 characters")
        if not self.kinds:
            raise ValueError("At least one entity kind is required")
        if any(
            kind not in DIRECT_SCOPE_KINDS - {EntityKind.ORGANIZATION}
            for kind in self.kinds
        ):
            raise ValueError("Only approved searchable V1 direct scopes are allowed")
        if self.limit < 1 or self.limit > MAX_CANDIDATES:
            raise ValueError(f"Search limit must be between 1 and {MAX_CANDIDATES}")
        object.__setattr__(self, "query", query)


@dataclass(frozen=True, slots=True)
class ScopeSearchResult:
    candidates: tuple[AuthorizedEntity, ...]
    query_version: str
    catalog_watermark: str


@dataclass(frozen=True, slots=True)
class ScopeResolution:
    outcome: ScopeResolutionOutcome
    entities: tuple[AuthorizedEntity, ...]
    team_filters: tuple[AuthorizedEntity, ...]
    candidates: tuple[AuthorizedEntity, ...]
    time_range: ResolvedTimeRange
    fallbacks: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


class AuthorizedEntityCatalog(Protocol):
    """Tenant-scoped catalog used by the resolution service."""

    async def watermark(self, org_id: str, kinds: tuple[EntityKind, ...]) -> str: ...

    async def exact(
        self, org_id: str, ref: ScopeRef, *, limit: int
    ) -> list[AuthorizedEntity]: ...

    async def search(
        self,
        org_id: str,
        query: str,
        kinds: tuple[EntityKind, ...],
        *,
        limit: int,
    ) -> list[AuthorizedEntity]: ...

    async def organization_repository_ids(
        self, org_id: str, *, limit: int
    ) -> tuple[list[str], int]:
        """Return up to ``limit`` authorized repository IDs, and the true total.

        The total must reflect every repository authorized for ``org_id``,
        not just the returned page, so callers can detect when the org
        exceeds the public ``authorized_repository_ids`` contract cap.
        """
        ...


class ScopeRequestCache:
    """Small request-local LRU; never share this object across requests."""

    def __init__(self, max_entries: int = MAX_REQUEST_CACHE_ENTRIES) -> None:
        if max_entries < 1 or max_entries > MAX_REQUEST_CACHE_ENTRIES:
            raise ValueError(
                f"Request cache size must be between 1 and {MAX_REQUEST_CACHE_ENTRIES}"
            )
        self._max_entries = max_entries
        self._values: OrderedDict[str, object] = OrderedDict()

    def get(self, key: str) -> object | None:
        value = self._values.get(key)
        if value is not None:
            self._values.move_to_end(key)
        return value

    def put(self, key: str, value: object) -> None:
        self._values[key] = value
        self._values.move_to_end(key)
        while len(self._values) > self._max_entries:
            self._values.popitem(last=False)


_OPAQUE_ID_RE = re.compile(
    r"^(?:[0-9a-f]{8}-[0-9a-f-]{27}|[0-9a-f]{24,}|[a-z][a-z0-9_-]*:[^\s]+)$",
    re.IGNORECASE,
)


def _is_opaque_reference(value: str) -> bool:
    return bool(_OPAQUE_ID_RE.match(value))


def _entity_order(entity: AuthorizedEntity) -> tuple[str, str, str]:
    return (entity.label.casefold(), entity.kind.value, entity.canonical_id)


def _dedupe_entities(
    entities: Sequence[AuthorizedEntity],
) -> tuple[AuthorizedEntity, ...]:
    unique = {(entity.kind, entity.canonical_id): entity for entity in entities}
    return tuple(sorted(unique.values(), key=_entity_order))


def _request_payload(request: ScopeResolveRequest) -> dict[str, object]:
    def refs(values: tuple[ScopeRef, ...]) -> list[tuple[str, str]]:
        return [(ref.kind.value, ref.value) for ref in values]

    return {
        "explicit": refs(request.explicit_refs),
        "page": refs(request.page_context_refs),
        "conversation": refs(request.conversation_context_refs),
        "team_filters": refs(request.team_filter_refs),
        "time": {
            "preset_days": request.time_range.preset_days,
            "start_date": request.time_range.start_date.isoformat()
            if request.time_range.start_date
            else None,
            "end_date": request.time_range.end_date.isoformat()
            if request.time_range.end_date
            else None,
            "timezone": request.time_range.timezone,
        },
        "allow_org_fallback": request.allow_organization_fallback,
        "surface_route_id": request.surface_route_id,
        "surface_filter_fingerprint": request.surface_filter_fingerprint,
    }


class ScopeResolutionService:
    """Resolve only authorized V1 scope entities with no implicit widening."""

    def __init__(
        self,
        catalog: AuthorizedEntityCatalog,
        *,
        cache: ScopeRequestCache | None = None,
    ) -> None:
        self._catalog = catalog
        self._cache = cache or ScopeRequestCache()

    def resolve_time_range(
        self,
        request: TimeRangeRequest,
        *,
        now: datetime | None = None,
    ) -> ResolvedTimeRange:
        try:
            zone = ZoneInfo(request.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("Unknown IANA timezone") from exc

        if request.start_date is not None and request.end_date is not None:
            start_local = datetime.combine(request.start_date, time.min, tzinfo=zone)
            # Absolute end dates are inclusive in the UI and represented as a
            # half-open boundary at the following local midnight.
            end_local = datetime.combine(
                request.end_date + timedelta(days=1), time.min, tzinfo=zone
            )
        else:
            if request.preset_days not in {7, 30, 90}:
                raise ValueError("Time preset must be 7, 30, or 90 days")
            current = now or datetime.now(timezone.utc)
            if current.tzinfo is None:
                raise ValueError("now must be timezone-aware")
            local_today = current.astimezone(zone).date()
            end_local = datetime.combine(
                local_today + timedelta(days=1), time.min, tzinfo=zone
            )
            start_local = datetime.combine(
                end_local.date() - timedelta(days=request.preset_days),
                time.min,
                tzinfo=zone,
            )

        utc_start = start_local.astimezone(timezone.utc)
        utc_end = end_local.astimezone(timezone.utc)
        duration = utc_end - utc_start
        comparison_utc_end = utc_start
        comparison_utc_start = comparison_utc_end - duration
        return ResolvedTimeRange(
            timezone=request.timezone,
            utc_start=utc_start,
            utc_end=utc_end,
            local_start=start_local.isoformat(),
            local_end=end_local.isoformat(),
            comparison_utc_start=comparison_utc_start,
            comparison_utc_end=comparison_utc_end,
            comparison_local_start=comparison_utc_start.astimezone(zone).isoformat(),
            comparison_local_end=comparison_utc_end.astimezone(zone).isoformat(),
        )

    async def resolve(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: ScopeResolveRequest,
    ) -> ScopeResolution:
        if not org_id or not permission_fingerprint:
            raise ValueError("Tenant and permission fingerprint are required")

        active_refs, inherited = self._active_refs(request)
        used_org_fallback = False
        if not active_refs and request.allow_organization_fallback:
            active_refs = (ScopeRef(EntityKind.ORGANIZATION, org_id),)
            used_org_fallback = True
        relevant_kinds = tuple(
            sorted(
                {ref.kind for ref in active_refs + request.team_filter_refs},
                key=lambda kind: kind.value,
            )
        )
        try:
            watermark = await self._catalog.watermark(org_id, relevant_kinds)
        except Exception:
            return ScopeResolution(
                outcome=ScopeResolutionOutcome.UNRESOLVED,
                entities=(),
                team_filters=(),
                candidates=(),
                time_range=self.resolve_time_range(request.time_range),
                warnings=("catalog_unavailable",),
            )
        cache_key = self._cache_key(
            "resolve",
            org_id,
            permission_fingerprint,
            _request_payload(request),
            watermark,
        )
        cached = self._cache.get(cache_key)
        if isinstance(cached, ScopeResolution):
            return cached

        time_range = self.resolve_time_range(request.time_range)
        if not active_refs:
            result = ScopeResolution(
                outcome=ScopeResolutionOutcome.UNRESOLVED,
                entities=(),
                team_filters=(),
                candidates=(),
                time_range=time_range,
            )
            self._cache.put(cache_key, result)
            return result

        try:
            entity_result = await self._resolve_refs(org_id, active_refs)
            team_result = await self._resolve_refs(org_id, request.team_filter_refs)
        except Exception:
            result = ScopeResolution(
                outcome=ScopeResolutionOutcome.UNRESOLVED,
                entities=(),
                team_filters=(),
                candidates=(),
                time_range=time_range,
                warnings=("catalog_unavailable",),
            )
            self._cache.put(cache_key, result)
            return result

        entities, candidates, missing = entity_result
        teams, team_candidates, missing_teams = team_result
        all_candidates = _dedupe_entities((*candidates, *team_candidates))[
            :MAX_CANDIDATES
        ]
        if all_candidates:
            outcome = ScopeResolutionOutcome.AMBIGUOUS
            entities = ()
            teams = ()
        elif missing or missing_teams:
            missing_refs = (*missing, *missing_teams)
            outcome = (
                ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND
                if any(
                    ref.kind is EntityKind.ORGANIZATION
                    or _is_opaque_reference(ref.value)
                    for ref in missing_refs
                )
                else ScopeResolutionOutcome.UNRESOLVED
            )
            entities = ()
            teams = ()
        elif inherited:
            outcome = ScopeResolutionOutcome.INHERITED
        elif used_org_fallback and teams:
            outcome = ScopeResolutionOutcome.FILTERED
        elif used_org_fallback:
            outcome = ScopeResolutionOutcome.ORGANIZATION_FALLBACK
        elif teams:
            outcome = ScopeResolutionOutcome.FILTERED
        else:
            outcome = ScopeResolutionOutcome.EXACT

        if used_org_fallback and entities:
            entities = (
                AuthorizedEntity(
                    EntityKind.ORGANIZATION, org_id, request.organization_label
                ),
            )
        result = ScopeResolution(
            outcome=outcome,
            entities=entities,
            team_filters=teams,
            candidates=all_candidates,
            time_range=time_range,
            fallbacks=("organization",) if used_org_fallback else (),
        )
        self._cache.put(cache_key, result)
        return result

    async def search(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: ScopeSearchRequest,
    ) -> ScopeSearchResult:
        if not org_id or not permission_fingerprint:
            raise ValueError("Tenant and permission fingerprint are required")
        kinds = tuple(sorted(set(request.kinds), key=lambda kind: kind.value))
        watermark = await self._catalog.watermark(org_id, kinds)
        payload = {
            "query": request.query,
            "kinds": [kind.value for kind in kinds],
            "limit": request.limit,
        }
        cache_key = self._cache_key(
            "search", org_id, permission_fingerprint, payload, watermark
        )
        cached = self._cache.get(cache_key)
        if isinstance(cached, ScopeSearchResult):
            return cached
        entities = await self._catalog.search(
            org_id, request.query, kinds, limit=request.limit
        )
        result = ScopeSearchResult(
            candidates=_dedupe_entities(entities)[: request.limit],
            query_version=QUERY_VERSION,
            catalog_watermark=watermark,
        )
        self._cache.put(cache_key, result)
        return result

    async def resolve_contract(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: ScopeResolveRequest,
        *,
        resolved_at: datetime | None = None,
    ) -> DevScopeResolution:
        """Resolve ``resolve_scope.v1`` into the canonical CHAOS-3223 contract."""
        domain = await self.resolve(org_id, permission_fingerprint, request)
        requested = self._requested_scope(org_id, request, domain.time_range)
        resolved = self._resolved_scope(org_id, request, domain)
        outcome = domain.outcome
        warnings = list(domain.warnings)
        repository_ids = sorted(
            {
                entity.canonical_id
                for entity in domain.entities
                if entity.kind is EntityKind.REPOSITORY
            }
            | {
                entity.repository_id
                for entity in domain.entities
                if entity.repository_id is not None
            }
        )
        if (
            resolved is not None
            and resolved.direct_scope is DirectScope.ORGANIZATION
            and not domain.team_filters
        ):
            # A team-filtered organization scope is deliberately excluded:
            # no native status/change query applies a team filter, so the
            # native execution layer always fails closed for it regardless
            # of repository count (CHAOS-3255). Enumerating the full org
            # repository set here would report a warning/authorized set
            # that describes execution behavior that will not actually
            # happen for this request.
            (
                repository_ids,
                outcome,
                resolved,
                warnings,
            ) = await self._organization_scope_repositories(
                org_id, permission_fingerprint, outcome, resolved, warnings
            )
        entity_ids = sorted(
            {
                entity.canonical_id
                for entity in domain.entities
                if entity.kind not in {EntityKind.ORGANIZATION, EntityKind.REPOSITORY}
            }
        )
        candidates = [
            DevDisambiguationCandidate(
                entity_ref=self._contract_entity_ref(candidate),
                repository_id=candidate.repository_id,
                reason="Multiple authorized entities match the requested scope.",
            )
            for candidate in domain.candidates
        ]
        return DevScopeResolution(
            schema_version="dev_scope_resolution.v1",
            requested_scope=requested,
            resolved_scope=resolved,
            outcome=outcome,
            authorized_repository_ids=repository_ids,
            authorized_entity_ids=entity_ids,
            candidates=candidates,
            fallbacks=list(domain.fallbacks),
            warnings=warnings,
            resolved_at=resolved_at or datetime.now(timezone.utc),
        )

    async def _organization_scope_repositories(
        self,
        org_id: str,
        permission_fingerprint: str,
        outcome: ScopeResolutionOutcome,
        resolved: DevScope,
        warnings: list[str],
    ) -> tuple[list[str], ScopeResolutionOutcome, DevScope | None, list[str]]:
        """Enumerate the complete server-owned repository set for organization scope.

        ``DevScopeResolution.authorized_repository_ids`` is capped at
        ``MAX_REPOSITORIES`` entries by the ask-dev/v1 contract. An
        organization at or under that cap gets the complete set inline. An
        organization above the cap must never have a truncated, misleadingly
        "complete" list serialized onto the wire: status and change reads
        instead re-derive the authorized set themselves, organization-natively,
        bound only by ``org_id`` — the same boundary every other native query
        already enforces (CHAOS-3255). Zero authorized repositories is explicit
        insufficient evidence, never an exact scope with accidental partial
        coverage.
        """
        try:
            watermark = await self._catalog.watermark(org_id, (EntityKind.REPOSITORY,))
            cache_key = self._cache_key(
                "organization_repositories",
                org_id,
                permission_fingerprint,
                {},
                watermark,
            )
            cached = self._cache.get(cache_key)
            if isinstance(cached, tuple) and len(cached) == 2:
                repo_ids, total = cached
            else:
                repo_ids, total = await self._catalog.organization_repository_ids(
                    org_id, limit=MAX_REPOSITORIES
                )
                self._cache.put(cache_key, (repo_ids, total))
        except Exception:
            return (
                [],
                ScopeResolutionOutcome.UNRESOLVED,
                None,
                [*warnings, "catalog_unavailable"],
            )
        if total == 0:
            return (
                [],
                ScopeResolutionOutcome.UNRESOLVED,
                None,
                [*warnings, "organization_has_no_authorized_repositories"],
            )
        if total > MAX_REPOSITORIES:
            return (
                [],
                outcome,
                resolved,
                [
                    *warnings,
                    (
                        f"organization repository set ({total}) exceeds the "
                        f"{MAX_REPOSITORIES}-repository contract limit; status "
                        "and change tools resolve the authorized set "
                        "organization-natively"
                    ),
                ],
            )
        return sorted(repo_ids), outcome, resolved, warnings

    def _requested_scope(
        self,
        org_id: str,
        request: ScopeResolveRequest,
        time_range: ResolvedTimeRange,
    ) -> DevScope:
        refs, _inherited = self._active_refs(request)
        direct_scope = self._direct_scope(refs)
        if not refs:
            direct_scope = DirectScope.ORGANIZATION
        repositories = (
            [self._safe_requested_id(ref.value) for ref in refs]
            if direct_scope is DirectScope.REPOSITORY
            else []
        )
        entity_refs = (
            [self._requested_entity_ref(refs[0])]
            if refs
            and direct_scope not in {DirectScope.ORGANIZATION, DirectScope.REPOSITORY}
            else []
        )
        surface_context = None
        if request.surface_route_id is not None:
            surface_context = DevSurfaceContext(
                route_id=self._safe_requested_id(request.surface_route_id),
                entity_refs=[
                    self._requested_entity_ref(ref)
                    for ref in request.page_context_refs
                    if ref.kind is not EntityKind.ORGANIZATION
                ],
                filter_fingerprint=self._safe_requested_id(
                    request.surface_filter_fingerprint
                )
                if request.surface_filter_fingerprint
                else None,
            )
        return DevScope(
            schema_version="dev_scope.v1",
            organization_id=org_id,
            direct_scope=direct_scope,
            repositories=repositories,
            entity_refs=entity_refs,
            team_ids=[
                self._safe_requested_id(ref.value) for ref in request.team_filter_refs
            ],
            time_range=self._contract_time_range(time_range),
            comparison_range=self._contract_comparison_range(time_range),
            surface_context=surface_context,
        )

    def _resolved_scope(
        self,
        org_id: str,
        request: ScopeResolveRequest,
        resolution: ScopeResolution,
    ) -> DevScope | None:
        if resolution.outcome not in {
            ScopeResolutionOutcome.EXACT,
            ScopeResolutionOutcome.FILTERED,
            ScopeResolutionOutcome.INHERITED,
            ScopeResolutionOutcome.ORGANIZATION_FALLBACK,
        }:
            return None
        direct_scope = self._direct_scope_from_entities(resolution.entities)
        return DevScope(
            schema_version="dev_scope.v1",
            organization_id=org_id,
            direct_scope=direct_scope,
            repositories=[
                entity.canonical_id
                for entity in resolution.entities
                if entity.kind is EntityKind.REPOSITORY
            ],
            entity_refs=[
                self._contract_entity_ref(entity)
                for entity in resolution.entities
                if entity.kind not in {EntityKind.ORGANIZATION, EntityKind.REPOSITORY}
            ],
            team_ids=[team.canonical_id for team in resolution.team_filters],
            time_range=self._contract_time_range(resolution.time_range),
            comparison_range=self._contract_comparison_range(resolution.time_range),
            surface_context=self._requested_scope(
                org_id, request, resolution.time_range
            ).surface_context,
        )

    @staticmethod
    def _direct_scope(refs: tuple[ScopeRef, ...]) -> DirectScope:
        if not refs:
            return DirectScope.ORGANIZATION
        return DirectScope(refs[0].kind.value)

    @staticmethod
    def _direct_scope_from_entities(
        entities: tuple[AuthorizedEntity, ...],
    ) -> DirectScope:
        if not entities:
            raise ValueError("Resolved scope requires an authorized direct entity")
        return DirectScope(entities[0].kind.value)

    @staticmethod
    def _contract_time_range(value: ResolvedTimeRange) -> DevTimeRange:
        return DevTimeRange(
            start=value.utc_start,
            end=value.utc_end,
            timezone=value.timezone,
        )

    @staticmethod
    def _contract_comparison_range(value: ResolvedTimeRange) -> DevTimeRange:
        return DevTimeRange(
            start=value.comparison_utc_start,
            end=value.comparison_utc_end,
            timezone=value.timezone,
        )

    @staticmethod
    def _contract_entity_ref(entity: AuthorizedEntity) -> DevEntityRef:
        return DevEntityRef(
            entity_type=EntityType(entity.kind.value),
            entity_id=entity.canonical_id,
            display_label=entity.label,
            repository_id=entity.repository_id,
        )

    @classmethod
    def _requested_entity_ref(cls, ref: ScopeRef) -> DevEntityRef:
        return DevEntityRef(
            entity_type=EntityType(ref.kind.value),
            entity_id=cls._safe_requested_id(ref.value),
            display_label=ref.value[:256],
        )

    @staticmethod
    def _safe_requested_id(value: str) -> str:
        if len(value) <= 128 and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/#-]*", value):
            return value
        return "request:" + hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def _active_refs(
        request: ScopeResolveRequest,
    ) -> tuple[tuple[ScopeRef, ...], bool]:
        if request.explicit_refs:
            return request.explicit_refs, False
        if request.page_context_refs:
            return request.page_context_refs, True
        if request.conversation_context_refs:
            return request.conversation_context_refs, True
        return (), False

    async def _resolve_refs(
        self, org_id: str, refs: tuple[ScopeRef, ...]
    ) -> tuple[
        tuple[AuthorizedEntity, ...],
        tuple[AuthorizedEntity, ...],
        tuple[ScopeRef, ...],
    ]:
        resolved: list[AuthorizedEntity] = []
        candidates: list[AuthorizedEntity] = []
        missing: list[ScopeRef] = []
        seen_refs: set[tuple[EntityKind, str]] = set()
        for ref in refs:
            ref_key = (ref.kind, ref.value.casefold())
            if ref_key in seen_refs:
                continue
            seen_refs.add(ref_key)
            matches: tuple[AuthorizedEntity, ...]
            if ref.kind is EntityKind.ORGANIZATION:
                matches = (
                    (AuthorizedEntity(EntityKind.ORGANIZATION, org_id, "Organization"),)
                    if ref.value == org_id
                    else ()
                )
            else:
                matches = _dedupe_entities(
                    await self._catalog.exact(org_id, ref, limit=MAX_CANDIDATES)
                )
            if not matches:
                missing.append(ref)
            elif len(matches) > 1:
                candidates.extend(matches)
            else:
                resolved.append(matches[0])
        return _dedupe_entities(resolved), _dedupe_entities(candidates), tuple(missing)

    @staticmethod
    def _cache_key(
        operation: str,
        org_id: str,
        permission_fingerprint: str,
        payload: dict[str, object],
        watermark: str,
    ) -> str:
        raw = json.dumps(
            {
                "operation": operation,
                "org_id": org_id,
                "permission": permission_fingerprint,
                "input": payload,
                "query_version": QUERY_VERSION,
                "watermark": watermark,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(raw.encode()).hexdigest()
