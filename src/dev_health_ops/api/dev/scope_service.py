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
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from enum import StrEnum
from typing import Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .alias_matching import SpanMatch
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
from .contracts_v2 import DevEntityRefV2, DevSubjectSet, ResolutionOutcome
from .contracts_v2 import EntityKind as ContractEntityKind

QUERY_VERSION = "resolve-scope.v1"
MAX_REPOSITORIES = 20
MAX_ENTITY_REFS = 20
MAX_CANDIDATES = 25
MAX_REQUEST_CACHE_ENTRIES = 128
#: ``DevResolutionEntry.query_version`` is a non-empty ``Version``; a failed
#: catalog read has no watermark to carry, and a content-free placeholder is
#: honest where an empty string would be unrepresentable.
_UNAVAILABLE_WATERMARK = "catalog-watermark-unavailable"


class EntityKind(StrEnum):
    ORGANIZATION = "organization"
    REPOSITORY = "repository"
    PROJECT = "project"
    WORK_UNIT = "work_unit"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"
    TEAM = "team"


def subject_set_fingerprint(kind: EntityKind, canonical_ids: Sequence[str]) -> str:
    """Mint-derived, opaque set fingerprint (structural pattern 5).

    ``"set1_" + sha256(kind, sorted(canonical_ids)).hexdigest()[:40]``.
    Hex cannot spell a subject name, and the value is stable across runs
    for the same (kind, member set), which is what makes it usable as a
    batch cache key.

    CHAOS-3393 codex HIGH-1: a public, module-level function (not a
    ``ScopeResolutionService`` method) specifically so
    ``investigation_plans.executor`` can recompute it from the SAME
    canonical formula ``committed_subject_set_for`` used to mint the
    caller's authorization receipt (``DevSubjectSet.fingerprint`` /
    ``DevInvestigationResult.subject_set_fingerprint``), and cross-check a
    step's actual batch against it before executing -- imported by
    reference, never a second, divergent copy of the hash formula.
    """

    digest = hashlib.sha256(
        "\0".join((kind.value, *sorted(canonical_ids))).encode()
    ).hexdigest()[:40]
    return f"set1_{digest}"


#: Kinds with a real v1 ``DevScope.direct_scope`` representation. CHAOS-3301
#: adds ``TEAM`` here: a client (or the subject preflight) can now commit and
#: re-validate a team-direct ``DevScope`` through ``resolve_contract`` /
#: ``committed_resolution_for``, exactly like any other direct scope kind.
DIRECT_SCOPE_KINDS = frozenset(
    {
        EntityKind.ORGANIZATION,
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
        EntityKind.TEAM,
    }
)

#: CHAOS-3301 structural pattern: explicit, independently-pinned frozensets
#: per surface, never derived from ``DIRECT_SCOPE_KINDS`` or from each other.
#: Widening ``DIRECT_SCOPE_KINDS`` (as this issue just did, for ``TEAM``) must
#: never silently widen what a caller may *search by free-text query* — that
#: is a deliberate, separate decision per surface, made by editing that
#: surface's own constant. All three currently name the same five kinds
#: (organization is excluded because named-entity resolution never searches
#: organization scope itself, CHAOS-3256; team is excluded because CHAOS-3301
#: gives team a v1 *subject* — resolved from question text via the preflight
#: — not a v1 *free-text search* kind on these surfaces).
#:
#: The kinds ``resolve_scope.v1``'s query-search path may search.
V1_SEARCHABLE_ENTITY_KINDS = frozenset(
    {
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
    }
)
#: The kinds the model-facing ``resolve_scope.v1`` tool may search.
MODEL_SEARCHABLE_ENTITY_KINDS = frozenset(
    {
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
    }
)
#: The kinds the typed GraphQL scope-search field may search.
GRAPHQL_SEARCHABLE_ENTITY_KINDS = frozenset(
    {
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
    }
)

#: The **ceiling** on what any caller may search — not a default. CHAOS-3292
#: adds ``TEAM`` here so the subject preflight can resolve a named team against
#: the catalog (which has supported teams since ``scope_catalog``'s team
#: queries landed) and record an honest ``exact_match`` for it. CHAOS-3301
#: gives team real v1 ``DirectScope``/``DevScope`` semantics once *committed*
#: by the preflight, but every pre-existing free-text-search caller keeps
#: ``V1_SEARCHABLE_ENTITY_KINDS`` (excluding team) — only the preflight's own
#: typed-mention resolution (``resolve_mention``) is allowed to reach this
#: wider ceiling.
SEARCHABLE_ENTITY_KINDS = V1_SEARCHABLE_ENTITY_KINDS | {EntityKind.TEAM}


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
    #: CHAOS-3553: how the SEARCH SPAN that produced this row relates to the
    #: label -- stamped by ``scope_catalog.merge_search_candidates``, the one
    #: place that sees both the query and the ranked page.
    #:
    #: ``None`` on every entity that has no span to speak of: the catalog's
    #: ``exact()`` path, the organization roster, the organization-fallback
    #: entity, and anything a caller builds by hand. ``None`` means "not
    #: classified", never "no match" -- every consumer must fail closed on it
    #: rather than assume a class.
    #:
    #: Excluded from equality and hashing (``compare=False``) on purpose. This
    #: is metadata about a QUERY, not about the entity: the same authorized
    #: entity found by "MWA" and by "Web Application" is the same entity, and
    #: letting the span decide identity would make ``==`` answer a different
    #: question than every id-keyed dedupe in this module already answers.
    #: Assert on ``.span_match`` directly rather than through entity equality.
    span_match: SpanMatch | None = field(default=None, compare=False)

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


def scope_request_from_scope(scope: DevScope) -> ScopeResolveRequest:
    """Derive a :class:`ScopeResolveRequest` from a client-supplied ``DevScope``.

    CHAOS-3502: promoted from ``production_runtime._scope_request`` (a
    behavior-preserving move, not a rewrite -- every existing call site there
    now imports this instead of a private copy) so ``orchestrator.py`` can
    build the same request shape ``EvidenceService.admit()`` needs without a
    circular import: ``production_runtime.py`` already imports from
    ``orchestrator.py`` (the DI/wiring layer depends on the orchestrator, not
    the reverse), so the orchestrator can never import from
    ``production_runtime``. ``scope_service.py`` -- where ``ScopeResolveRequest``
    itself is already defined -- is the shared leaf both sides can import.
    """

    refs: tuple[ScopeRef, ...]
    if scope.direct_scope is DirectScope.ORGANIZATION:
        refs = (ScopeRef(EntityKind.ORGANIZATION, scope.organization_id),)
    elif scope.direct_scope is DirectScope.REPOSITORY:
        refs = tuple(
            ScopeRef(EntityKind.REPOSITORY, value) for value in scope.repositories
        )
    else:
        refs = tuple(
            ScopeRef(EntityKind(item.entity_type.value), item.entity_id)
            for item in scope.entity_refs
        )
    return ScopeResolveRequest(
        explicit_refs=refs,
        # A team *direct* scope already carries its team as an explicit_ref
        # (via the entity_refs branch above); team_ids there is required by
        # DevScope.validate_direct_scope to name that same team, not a second
        # independent dimension. Also passing it as a team_filter_ref would
        # make `resolve()` treat the run as team-*filtered* (outcome=FILTERED)
        # rather than an exact single-entity commit (CHAOS-3301).
        team_filter_refs=(
            ()
            if scope.direct_scope is DirectScope.TEAM
            else tuple(ScopeRef(EntityKind.TEAM, value) for value in scope.team_ids)
        ),
        time_range=TimeRangeRequest(
            preset_days=None,
            start_date=date.fromisoformat(scope.time_range.start.date().isoformat()),
            end_date=date.fromisoformat(scope.time_range.end.date().isoformat()),
            timezone=scope.time_range.timezone,
        ),
        allow_organization_fallback=False,
    )


@dataclass(frozen=True, slots=True)
class ScopeSearchRequest:
    query: str
    kinds: tuple[EntityKind, ...]
    limit: int = MAX_CANDIDATES
    #: Each caller declares the kinds *it* is allowed to search;
    #: ``SEARCHABLE_ENTITY_KINDS`` is the ceiling, never the default. Widening
    #: the ceiling for the subject preflight must not implicitly expose team
    #: search on the GraphQL field or the model-facing ``resolve_scope.v1``
    #: tool, both of which CHAOS-3301 owns.
    allowed_kinds: frozenset[EntityKind] = V1_SEARCHABLE_ENTITY_KINDS
    #: CHAOS-3388. Never set by an ordinary caller -- only
    #: ``subject_preflight._close_matches`` (the CHAOS-3366 closest-matches
    #: fallback for an already-unresolved named mention) opts in. Acronym
    #: matching is a derived, candidate-only signal (see ``alias_matching``'s
    #: module docstring): folding it into the *primary* resolution search
    #: would let a query like "ACR" land in the ordinary ``AMBIGUOUS_
    #: CANDIDATES``/"more than one entity matches this name" outcome, which
    #: is the wrong copy for "nothing matched literally, but here is
    #: something close" -- that is exactly the outcome and copy the
    #: closest-matches fallback already owns.
    include_alias_matches: bool = False
    #: CHAOS-3422. The kind a *typed* mention actually named. Purely a ranking
    #: key -- candidates of this kind sort ahead of every other kind, before
    #: each of the two truncations on this path (the catalog's page bound and
    #: this service's own), and nothing is ever excluded for being another
    #: kind. Excluding would regress CHAOS-3366, whose whole premise is that
    #: the thing the user meant may sit one kind over. Empty for every caller
    #: that has no user-stated kind to honour, which is today's behaviour
    #: exactly; an untyped bare name is one of those, because its
    #: ``requested_entity_kind`` is a declared default, not something the user
    #: typed (``question_interpreter._add_untyped_mentions``).
    preferred_kinds: frozenset[EntityKind] = frozenset()

    def __post_init__(self) -> None:
        query = self.query.strip()
        if not query or len(query) > 256:
            raise ValueError("Search query must contain 1 to 256 characters")
        if not self.kinds:
            raise ValueError("At least one entity kind is required")
        if not self.allowed_kinds <= SEARCHABLE_ENTITY_KINDS:
            raise ValueError("Caller allowlist exceeds the searchable entity ceiling")
        if any(kind not in self.allowed_kinds for kind in self.kinds):
            raise ValueError("Only approved searchable V1 direct scopes are allowed")
        if not self.preferred_kinds <= set(self.kinds):
            raise ValueError("A preferred kind must be one of the searched kinds")
        if self.limit < 1 or self.limit > MAX_CANDIDATES:
            raise ValueError(f"Search limit must be between 1 and {MAX_CANDIDATES}")
        object.__setattr__(self, "query", query)


@dataclass(frozen=True, slots=True)
class MentionResolution:
    """One subject mention's resolution outcome, in the v2 vocabulary.

    Distinct from ``resolve_query_contract``'s ``DevScopeResolution``, whose
    ``FORBIDDEN_OR_NOT_FOUND`` conflates not-exists, cross-tenant, and (until
    this module started catching it) a catalog error. Nothing on this path ever
    constructs that combined outcome — the preflight has no code path that
    produces it, which is what makes "the internal combined outcome never
    leaves the server" structural rather than a filter.
    """

    outcome: ResolutionOutcome
    entity: AuthorizedEntity | None
    candidates: tuple[AuthorizedEntity, ...]
    catalog_watermark: str
    query_version: str


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
        include_alias_matches: bool = False,
        preferred_kinds: frozenset[EntityKind] = frozenset(),
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

    async def organization_project_entities(
        self, org_id: str, *, limit: int
    ) -> tuple[list[AuthorizedEntity], int]:
        """CHAOS-3393: up to ``limit`` authorized projects (labeled,
        deterministically ordered by display label then id), and the true
        total authorized for ``org_id`` -- so a caller enumerating an
        ORGANIZATION_WIDE portfolio can disclose truncation rather than
        silently sampling.
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


def _dedupe_preserving_rank(
    entities: Sequence[AuthorizedEntity],
) -> tuple[AuthorizedEntity, ...]:
    """Deduplicate a search page **without** re-ordering it.

    Search results arrive already ranked by ``merge_search_candidates`` — the
    named kind, then alias-equality over incidental substring containment,
    then label. Passing them through ``_dedupe_entities`` re-sorted the whole
    page by label and destroyed both of the first two keys, because an
    ``AuthorizedEntity`` carries no match provenance for a later layer to
    recover. That is what listed the one real project last in the CHAOS-3422
    repro even though the catalog had ranked it first, and it silently
    undid CHAOS-3388's alias precedence for every same-kind pair
    (codex review round 3, confirmed: alias ``Zeta Runtime (ACR)`` lost its
    lead to substring-only ``Aardvark ACR Notes``).

    Not a behaviour change for the alias-unaware callers: with
    ``include_alias_matches=False`` the catalog's own order *is* label order,
    the identical sequence ``_dedupe_entities`` produced.

    Exactly **two** callers enable alias matching and therefore see a
    different page — counted, because an earlier draft of this docstring
    claimed there was only one and a reviewer had to find the other:

    * ``subject_preflight._close_matches`` — the closest-matches fallback,
      whose ranking is what this ticket restores.
    * ``qua_shadow.QUAShadowEvaluator._shortlist`` — the CHAOS-3389 shadow
      seam. Its shortlist is reordered the same way, which is safe because
      the model is shown the very list its returned indices are resolved
      against, so there is no index/list skew, and because nothing downstream
      reads the record it produces. It needs no ``preferred_kinds`` of its
      own: it already searches a single kind for a typed mention, so a
      preference would be vacuous, and for an untyped one the kind is a
      declared default that must not be preferred at all.

    ``resolve()``'s exact path still uses ``_dedupe_entities``: those entities
    come from ``exact()``, which has no rank of its own to preserve.
    """

    unique: dict[tuple[EntityKind, str], AuthorizedEntity] = {}
    for entity in entities:
        unique.setdefault((entity.kind, entity.canonical_id), entity)
    return tuple(unique.values())


def _rank_preferred_kinds(
    entities: Sequence[AuthorizedEntity], preferred_kinds: frozenset[EntityKind]
) -> tuple[AuthorizedEntity, ...]:
    """CHAOS-3422: the kind the user named leads, everything else follows.

    A **stable** partition, so with no preference the result is the identical
    tuple and with one the catalog's rank still decides the order inside each
    side. Applied here as well as in the catalog because the two truncate
    separately: the catalog bounds its page, this bounds what a caller asked
    for, and a rank applied at only one of them is a rank applied after a cut.
    """

    if not preferred_kinds:
        return tuple(entities)
    preferred = [entity for entity in entities if entity.kind in preferred_kinds]
    rest = [entity for entity in entities if entity.kind not in preferred_kinds]
    return tuple(preferred + rest)


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
        *,
        now: datetime | None = None,
    ) -> ScopeResolution:
        """Resolve ``request`` as of ``now`` (defaults to the real current
        instant -- production callers never pass this explicitly).

        ``now`` governs the *whole* resolution, not just part of it: a
        preset window must be anchored on the SAME instant the caller
        believes it pinned. CHAOS-3392: ``now`` used to be silently dropped
        here -- every preset-relative ``time_range`` (the common case;
        explicit ``start_date``/``end_date`` was unaffected) was always
        computed against the REAL wall clock even when a caller resolved an
        otherwise fully deterministic, watermark-cached result via
        ``resolve_contract(resolved_at=...)`` (below), so a resolution that
        took its window from the wall clock while its caller believed it
        had pinned the instant was reproducible only until the next local
        midnight. Threading ``now`` through closes that leak; the default
        keeps today's production behavior (resolve against the actual
        current moment) byte-for-byte unchanged.

        (An independently-diagnosed fix for this same defect landed on
        main as #1366 while this branch's own codex-hardened fix was in
        flight; reconciled here onto this branch's semantics -- see the
        cache-key comment below for why, and its ``resolved_at``-anchors-
        the-window / two-instants-do-not-collide tests are kept alongside
        this branch's own three.)
        """
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
        # Resolved once, up front: the catalog-failure branch below, the
        # cache key, and the eventual ``ScopeResolution.time_range`` all
        # share this SAME value, so a transient catalog failure on a
        # SECOND, microseconds-later call in the same request can never
        # observe a different ``time_range`` than a first call that already
        # succeeded and was cached (see the cache-key comment below).
        time_range = self.resolve_time_range(request.time_range, now=now)
        try:
            watermark = await self._catalog.watermark(org_id, relevant_kinds)
        except Exception:
            return ScopeResolution(
                outcome=ScopeResolutionOutcome.UNRESOLVED,
                entities=(),
                team_filters=(),
                candidates=(),
                time_range=time_range,
                warnings=("catalog_unavailable",),
            )
        payload = _request_payload(request)
        if request.time_range.start_date is None or request.time_range.end_date is None:
            # Preset-relative window: ``now`` DOES affect the result, but
            # the raw instant must never be the cache key material itself.
            # CHAOS-3392 codex MEDIUM: keying on ``now.isoformat()`` (as
            # main's independent #1366 fix for this same defect did too --
            # reconciled here onto this branch's semantics, not main's) gave
            # every call within one request its own microsecond-distinct
            # key (``resolve_contract`` always resolves a concrete instant,
            # even for callers who never pin one), which defeated the
            # request-local cache entirely for the common, unpinned
            # production path -- repeated calls in one request each re-hit
            # the catalog instead of reusing the first result, and a
            # transient catalog failure on that SECOND call could flip an
            # already-resolved, already-cacheable request from exact to
            # unresolved. Keyed instead by the EFFECTIVE resolved boundary
            # (``time_range.utc_start``/``utc_end``): two calls landing on
            # the same local day -- the only thing a preset window is
            # actually sensitive to -- collapse onto the SAME cache entry,
            # exactly like two calls with no ``now`` divergence at all did
            # before CHAOS-3392 threaded ``now`` through in the first place.
            payload = {
                **payload,
                "resolved_utc_start": time_range.utc_start.isoformat(),
                "resolved_utc_end": time_range.utc_end.isoformat(),
            }
        # Absolute ``start_date``/``end_date`` ranges are left untouched:
        # ``now`` cannot affect their result (``resolve_time_range`` never
        # reads it in that branch), so the cache key stays byte-for-byte
        # identical to pre-CHAOS-3392 -- no needless cache-busting for the
        # case this ticket was never about.
        cache_key = self._cache_key(
            "resolve",
            org_id,
            permission_fingerprint,
            payload,
            watermark,
        )
        cached = self._cache.get(cache_key)
        if isinstance(cached, ScopeResolution):
            return cached

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
            # CHAOS-3388: distinct cache entry from an alias-unaware search of
            # the same query/kinds/limit -- the two can return different
            # candidate sets, so sharing a cache key would let whichever
            # request happened to run first silently answer the other's call.
            "include_alias_matches": request.include_alias_matches,
            # CHAOS-3422, for the same reason: a preference changes both which
            # candidates survive the catalog's page bound and the order they
            # arrive in, so a preferred and an unpreferred search of the same
            # query/kinds/limit must never share a cache entry.
            "preferred_kinds": sorted(kind.value for kind in request.preferred_kinds),
        }
        cache_key = self._cache_key(
            "search", org_id, permission_fingerprint, payload, watermark
        )
        cached = self._cache.get(cache_key)
        if isinstance(cached, ScopeSearchResult):
            return cached
        entities = await self._catalog.search(
            org_id,
            request.query,
            kinds,
            limit=request.limit,
            include_alias_matches=request.include_alias_matches,
            preferred_kinds=request.preferred_kinds,
        )
        result = ScopeSearchResult(
            candidates=_rank_preferred_kinds(
                _dedupe_preserving_rank(entities), request.preferred_kinds
            )[: request.limit],
            query_version=QUERY_VERSION,
            catalog_watermark=watermark,
        )
        self._cache.put(cache_key, result)
        return result

    async def resolve_mention(
        self,
        org_id: str,
        permission_fingerprint: str,
        *,
        lookup_text: str,
        kinds: tuple[EntityKind, ...],
        exact: bool = False,
    ) -> MentionResolution:
        """Resolve one named subject mention to a per-mention typed outcome.

        The existing service cannot express this: ``resolve()`` collapses every
        ref to one combined outcome and ``resolve_query_contract`` returns the
        internal ``FORBIDDEN_OR_NOT_FOUND`` code. Ambiguity is probed against
        the full ``MAX_CANDIDATES`` page for the same reason
        ``resolve_query_contract`` documents: truncating first would let an
        arbitrary one of several real matches be reported as exact.

        **The catalog call is wrapped here deliberately.** ``search()`` has no
        exception handling of its own — unlike ``resolve()``, which types a
        catalog failure as ``UNRESOLVED``/``catalog_unavailable`` — so without
        this wrapper a ClickHouse outage would escape as an exception, surface
        as ``internal_error``, and make ``CATALOG_UNAVAILABLE`` unreachable.
        """

        if not org_id or not permission_fingerprint:
            raise ValueError("Tenant and permission fingerprint are required")
        if any(kind not in SEARCHABLE_ENTITY_KINDS for kind in kinds) or not kinds:
            return MentionResolution(
                outcome=ResolutionOutcome.UNSUPPORTED_KIND,
                entity=None,
                candidates=(),
                catalog_watermark=_UNAVAILABLE_WATERMARK,
                query_version=QUERY_VERSION,
            )
        ordered_kinds = tuple(sorted(set(kinds), key=lambda kind: kind.value))
        # Built outside the try: a malformed request is a caller defect, not a
        # catalog outage, and must not be mistyped as one.
        search_request = (
            None
            if exact
            else ScopeSearchRequest(
                query=lookup_text[:256],
                kinds=ordered_kinds,
                limit=MAX_CANDIDATES,
                allowed_kinds=SEARCHABLE_ENTITY_KINDS,
            )
        )
        refs = tuple(ScopeRef(kind, lookup_text[:512]) for kind in ordered_kinds)
        try:
            watermark = await self._catalog.watermark(org_id, ordered_kinds)
            # Exact first, always. The catalog's fuzzy query orders by label and
            # applies its LIMIT in SQL, so with more than MAX_CANDIDATES
            # substring matches sorted ahead of it the exactly-named entity
            # never reaches this code at all — and a legitimate question died
            # ambiguous with the one thing the user actually named missing from
            # the candidate list. ``exact()`` matches id or label equality and
            # is not subject to that page boundary.
            matches: list[AuthorizedEntity] = []
            for ref in refs:
                matches.extend(
                    await self._catalog.exact(org_id, ref, limit=MAX_CANDIDATES)
                )
            candidates = _dedupe_entities(matches)[:MAX_CANDIDATES]
            if not candidates and search_request is not None:
                result = await self.search(
                    org_id, permission_fingerprint, search_request
                )
                candidates = result.candidates
                watermark = result.catalog_watermark
        except Exception:
            return MentionResolution(
                outcome=ResolutionOutcome.CATALOG_UNAVAILABLE,
                entity=None,
                candidates=(),
                catalog_watermark=_UNAVAILABLE_WATERMARK,
                query_version=QUERY_VERSION,
            )
        watermark = watermark or _UNAVAILABLE_WATERMARK
        if not candidates:
            return MentionResolution(
                outcome=ResolutionOutcome.NO_AUTHORIZED_MATCH,
                entity=None,
                candidates=(),
                catalog_watermark=watermark,
                query_version=QUERY_VERSION,
            )
        # The catalog's text search is a ``%query%`` LIKE, so a *sole* result is
        # not the same thing as an exact match: "the Dev project" returns only
        # "Ask Dev" and would otherwise commit it, answering about an entity
        # the user did not name. Committing therefore requires the name to
        # equal a label or canonical id outright; a partial name that matched
        # something real is offered back as candidates instead.
        #
        # CHAOS-3388 codex re-review (HIGH, confirmed): a candidate's own
        # parenthetical segment ("Dev Health Agent Context Runtime (Context
        # Fabric)" -> "Context Fabric") is NOT eligible for the same
        # outright-equality commit as its primary label, however real the
        # alternate name reads in ordinary language. The catalog carries no
        # explicit alias field -- ``alias_forms`` *derives* every
        # parenthetical the same mechanical way, by splitting one label
        # string on its parentheses, with no way to distinguish a genuine
        # alternate name ("Context Fabric") from a qualifier appended to the
        # primary label ("Payments (Legacy)", "Reports (Archived)"). The
        # first commit of this check treated both identically and
        # auto-committed "the Legacy project" onto ``payments-legacy`` --
        # answering about an entity the user never actually named. So a
        # parenthetical match is held to exactly the acronym rule: candidate
        # list only, never a pick, however unique the match turns out to be
        # (see ``alias_matching``'s module docstring). Should the catalog
        # ever grow a real, explicit alias field -- distinct from splitting
        # the display label -- that field's values would be the eligible
        # input here instead; a derived parenthetical never is.
        wanted = lookup_text.strip().casefold()
        exact_matches = tuple(
            candidate
            for candidate in candidates
            if wanted in {candidate.canonical_id.casefold(), candidate.label.casefold()}
        )
        if len(exact_matches) == 1:
            return MentionResolution(
                outcome=ResolutionOutcome.EXACT_MATCH,
                entity=exact_matches[0],
                candidates=(),
                catalog_watermark=watermark,
                query_version=QUERY_VERSION,
            )
        return MentionResolution(
            outcome=ResolutionOutcome.AMBIGUOUS_CANDIDATES,
            entity=None,
            candidates=exact_matches or candidates,
            catalog_watermark=watermark,
            query_version=QUERY_VERSION,
        )

    def committed_resolution_for(
        self,
        entity: AuthorizedEntity,
        *,
        org_id: str,
        base_scope: DevScope,
        resolved_at: datetime,
    ) -> DevScopeResolution:
        """The one construction of an exact-match committed scope.

        Shared by ``resolve_query_contract`` (the legacy ``resolve_scope.v1``
        path) and the subject preflight, so there is one notion of "committed
        scope" rather than two that can drift.

        Raises ``ValueError`` for a kind with no v1 direct-scope
        representation at all (currently just ``organization``, which is
        never a *single-entity* commit), rather than letting
        ``DirectScope(...)``/``EntityType(...)`` raise a bare enum error from
        deep inside scope construction.

        CHAOS-3301: team is now a real v1 direct scope. Its repository
        attribution is deliberately **not** carried here —
        ``authorized_repository_ids``/``authorized_entity_ids`` stay empty for
        a team commit, since ``AuthorizedEntity.repository_id`` is always
        ``None`` for a team (``scope_catalog`` resolves teams
        organization-scoped with no repository dimension). Team→repository
        attribution is re-derived at query time by the status/change source,
        not carried on the wire (Amendment: addendum item 1) — CHAOS-3303's
        job, not this function's.
        """

        if (
            entity.kind is EntityKind.ORGANIZATION
            or entity.kind not in DIRECT_SCOPE_KINDS
        ):
            raise ValueError(
                f"{entity.kind.value} has no v1 direct-scope representation"
            )
        is_repository = entity.kind is EntityKind.REPOSITORY
        is_team = entity.kind is EntityKind.TEAM
        resolved_scope = DevScope(
            schema_version="dev_scope.v1",
            organization_id=org_id,
            direct_scope=DirectScope(entity.kind.value),
            repositories=[entity.canonical_id] if is_repository else [],
            entity_refs=[] if is_repository else [self._contract_entity_ref(entity)],
            team_ids=[entity.canonical_id] if is_team else [],
            time_range=base_scope.time_range,
            comparison_range=base_scope.comparison_range,
            surface_context=None,
        )
        repository_ids = sorted(
            ({entity.canonical_id} if is_repository else set())
            | ({entity.repository_id} if entity.repository_id else set())
        )
        entity_ids = [] if is_repository else [entity.canonical_id]
        return DevScopeResolution(
            schema_version="dev_scope_resolution.v1",
            requested_scope=base_scope,
            resolved_scope=resolved_scope,
            outcome=ScopeResolutionOutcome.EXACT,
            authorized_repository_ids=repository_ids,
            authorized_entity_ids=entity_ids,
            candidates=[],
            fallbacks=[],
            warnings=[],
            resolved_at=resolved_at,
        )

    @classmethod
    def committed_cohort_resolution_for(
        cls,
        subject_set: DevSubjectSet,
        *,
        org_id: str,
        base_scope: DevScope,
        resolved_at: datetime,
    ) -> DevScopeResolution:
        """The committed-scope construction for a COHORT (CHAOS-3534).

        Deliberately a sibling of ``committed_resolution_for`` in this same
        module rather than a second notion built elsewhere: that function's
        docstring exists to keep exactly one idea of "committed scope", and a
        cohort is the same idea over more than one entity. The conventions are
        inherited rather than reinvented -- most importantly that a REPOSITORY
        commit carries its ids in ``authorized_repository_ids`` and leaves
        ``authorized_entity_ids`` empty, which is exactly the detail a second
        producer written from scratch gets backwards.

        WHY IT EXISTS: a question naming several subjects that ALL resolve
        exactly commits a real ``dev_subject_set.v1`` and then terminates,
        because the v1 surface cannot render a cohort (D1). The resolution
        published on that terminal used to be re-derived from the answer
        FRAME, which structurally cannot carry a cohort -- so it fell through
        to ``UNRESOLVED`` and told an auditor the run resolved nothing, for a
        run that resolved everything it was asked about.

        ``cohort_complete`` is the CALLER's gate, not this function's
        business: an incomplete cohort genuinely did not resolve everything
        the question named, and calling that ``exact`` would be the same false
        statement pointing the other way.
        """

        kind = EntityKind(subject_set.entity_kind.value)
        if kind is not EntityKind.REPOSITORY:
            # v1 CANNOT express a multi-entity direct scope for any other
            # kind, and that is a contract fact rather than a policy choice:
            # DevScope.validate_direct_scope requires EXACTLY ONE matching
            # entity_ref for PROJECT/WORK_UNIT/ISSUE/PULL_REQUEST/TEAM
            # ("direct entity scope requires one matching entity"), while
            # `repositories` is a list with a >= 1 bound. So a repository
            # cohort has a faithful v1 representation and no other kind does.
            #
            # Refused loudly HERE rather than left to fail deep inside
            # DevScope construction: the first cut of CHAOS-3534 did exactly
            # that, and turned a two-project cohort's honest
            # `feature_not_enabled` into an opaque `internal_error` -- a
            # strictly worse outcome than the disclosure defect being fixed.
            # The caller is responsible for not asking; this is the backstop
            # that makes a future caller's mistake loud rather than silent.
            raise ValueError(
                f"a {kind.value} cohort has no multi-entity v1 scope "
                "representation; only repository cohorts can be published as "
                "an exact committed scope"
            )
        is_repository = True
        is_team = False
        entities = [
            AuthorizedEntity(
                kind=EntityKind(ref.entity_kind.value),
                canonical_id=ref.entity_id,
                label=ref.display_label,
                repository_id=ref.repository_id,
            )
            for ref in subject_set.committed_entity_refs
        ]
        canonical_ids = [entity.canonical_id for entity in entities]
        resolved_scope = DevScope(
            schema_version="dev_scope.v1",
            organization_id=org_id,
            direct_scope=DirectScope(kind.value),
            repositories=list(canonical_ids) if is_repository else [],
            entity_refs=(
                []
                if is_repository
                else [cls._contract_entity_ref(entity) for entity in entities]
            ),
            team_ids=list(canonical_ids) if is_team else [],
            time_range=base_scope.time_range,
            comparison_range=base_scope.comparison_range,
            surface_context=None,
        )
        repository_ids = sorted(
            (set(canonical_ids) if is_repository else set())
            | {entity.repository_id for entity in entities if entity.repository_id}
        )
        return DevScopeResolution(
            schema_version="dev_scope_resolution.v1",
            requested_scope=base_scope,
            resolved_scope=resolved_scope,
            outcome=ScopeResolutionOutcome.EXACT,
            authorized_repository_ids=repository_ids,
            authorized_entity_ids=[] if is_repository else list(canonical_ids),
            candidates=[],
            fallbacks=[],
            warnings=[],
            resolved_at=resolved_at,
        )

    def committed_subject_set_for(
        self,
        entities: Sequence[AuthorizedEntity],
        *,
        set_id: str,
        original_mention_count: int,
        unresolved_mention_ids: tuple[str, ...] = (),
        ambiguous_mention_ids: tuple[str, ...] = (),
        warnings: tuple[str, ...] = (),
    ) -> DevSubjectSet:
        """Build one ``dev_subject_set.v1`` from already-deduplicated entities.

        ``entities`` must already be deduplicated by canonical id and
        homogeneous in kind — the subject preflight owns those decisions
        (bounds are rejections, never truncations: a >25-member or
        heterogeneous set must never reach here at all). This is pure
        construction plus the fingerprint grammar; the pydantic model's own
        ``validate_set_invariants`` is the final check on omission/
        completeness/warning-disclosure consistency.
        """

        if not entities:
            raise ValueError("a subject set requires at least one committed entity")
        kind = entities[0].kind
        if any(entity.kind is not kind for entity in entities):
            raise ValueError("a subject set must be homogeneous in entity kind")
        omitted = len(unresolved_mention_ids) + len(ambiguous_mention_ids)
        return DevSubjectSet(
            schema_version="dev_subject_set.v1",
            set_id=set_id,
            entity_kind=ContractEntityKind(kind.value),
            committed_entity_refs=tuple(
                DevEntityRefV2(
                    entity_kind=ContractEntityKind(entity.kind.value),
                    entity_id=entity.canonical_id,
                    display_label=entity.label,
                    repository_id=entity.repository_id,
                    team_id=(
                        entity.canonical_id if entity.kind is EntityKind.TEAM else None
                    ),
                )
                for entity in entities
            ),
            original_mention_count=original_mention_count,
            unresolved_mention_ids=unresolved_mention_ids,
            ambiguous_mention_ids=ambiguous_mention_ids,
            cohort_complete=omitted == 0,
            warnings=warnings,
            fingerprint=subject_set_fingerprint(
                kind, [entity.canonical_id for entity in entities]
            ),
        )

    async def resolve_query_contract(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: ScopeSearchRequest,
        *,
        base_scope: DevScope,
        resolved_at: datetime | None = None,
    ) -> DevScopeResolution:
        """Resolve a model-owned free-text query into an authorized direct scope.

        This is the canonical seam ``resolve_scope.v1`` uses when the model
        names an entity: it searches only the authenticated organization's
        authorized catalog (never free-form SQL/GraphQL, never a
        model-authored ID) and commits an exact match, returns typed
        candidates when ambiguous, or reports not-found/forbidden. An
        unresolved or ambiguous query never silently falls back to
        organization scope (CHAOS-3256).

        Ambiguity is always probed against the full ``MAX_CANDIDATES`` page,
        never the model-owned ``request.limit``: truncating the search
        *before* checking for a second match would let a caller-supplied
        ``limit=1`` silently pick an arbitrary one of several real matches
        and report it as an exact commit. ``request.limit`` only bounds how
        many typed candidates are returned once the outcome is ambiguous.
        """
        # A typed guard, not an assumption: ``ScopeSearchRequest``'s allowlist
        # is now per-caller, so a caller that widened its own allowlist must
        # not be able to reach ``DirectScope(...)``/``EntityType(...)`` below
        # with a kind v1 cannot represent and produce a bare enum ValueError
        # from inside scope construction.
        unrepresentable = sorted(
            kind.value
            for kind in request.kinds
            if kind not in V1_SEARCHABLE_ENTITY_KINDS
        )
        if unrepresentable:
            raise ValueError(
                "resolve_scope.v1 cannot represent entity kinds "
                f"{unrepresentable}; use resolve_mention for those"
            )
        probe_request = (
            request
            if request.limit == MAX_CANDIDATES
            else replace(request, limit=MAX_CANDIDATES)
        )
        result = await self.search(org_id, permission_fingerprint, probe_request)
        resolved_at = resolved_at or datetime.now(timezone.utc)
        if not result.candidates:
            return DevScopeResolution(
                schema_version="dev_scope_resolution.v1",
                requested_scope=base_scope,
                resolved_scope=None,
                outcome=ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND,
                authorized_repository_ids=[],
                authorized_entity_ids=[],
                candidates=[],
                fallbacks=[],
                warnings=["No authorized entity matched the requested query."],
                resolved_at=resolved_at,
            )
        if len(result.candidates) > 1:
            return DevScopeResolution(
                schema_version="dev_scope_resolution.v1",
                requested_scope=base_scope,
                resolved_scope=None,
                outcome=ScopeResolutionOutcome.AMBIGUOUS,
                authorized_repository_ids=[],
                authorized_entity_ids=[],
                candidates=[
                    DevDisambiguationCandidate(
                        entity_ref=self._contract_entity_ref(candidate),
                        repository_id=candidate.repository_id,
                        reason="Multiple authorized entities match the requested query.",
                    )
                    for candidate in result.candidates[: request.limit]
                ],
                fallbacks=[],
                warnings=[],
                resolved_at=resolved_at,
            )
        return self.committed_resolution_for(
            result.candidates[0],
            org_id=org_id,
            base_scope=base_scope,
            resolved_at=resolved_at,
        )

    async def resolve_contract(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: ScopeResolveRequest,
        *,
        resolved_at: datetime | None = None,
    ) -> DevScopeResolution:
        """Resolve ``resolve_scope.v1`` into the canonical CHAOS-3223 contract.

        ``resolved_at`` is the instant the resolution is *made at*, so it
        also anchors the resolved preset window -- it is not merely a
        label applied to a window taken from a second, unrelated clock.
        Resolved early (was previously computed only for the OUTPUT
        ``resolved_at`` field, at the very end of this method) so the SAME
        instant also pins ``domain.time_range`` via ``self.resolve`` below
        -- see that method's docstring. Production callers pass ``None``
        and get the wall clock for both, unchanged.
        """
        resolved_at = resolved_at or datetime.now(timezone.utc)
        domain = await self.resolve(
            org_id, permission_fingerprint, request, now=resolved_at
        )
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
            # Already resolved above (and threaded into ``self.resolve`` as
            # ``now``) -- always non-None by this point.
            resolved_at=resolved_at,
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

    async def organization_committed_projects(
        self,
        org_id: str,
        permission_fingerprint: str,
        *,
        limit: int,
    ) -> tuple[list[AuthorizedEntity], int, bool]:
        """CHAOS-3393: bounded, deterministic project enumeration for an
        ORGANIZATION_WIDE ``status.portfolio.v1`` run (no named subjects).

        Unlike :meth:`_organization_scope_repositories`, which DROPS the
        whole set once ``total`` exceeds its cap (an authorized-repository
        list is either complete or entirely re-derived native-side, never
        partial), a portfolio batch is legitimate as a bounded, DISCLOSED
        subset of a larger organization -- ``PortfolioStatusService`` itself
        already caps at ``MAX_PORTFOLIO_PROJECTS`` and expects the caller
        to say so, not omit the whole answer. ``limit`` is therefore always
        honored as a page bound, never a completeness gate; the true
        ``total`` is returned alongside so the caller can disclose
        truncation. Watermark-cached exactly like the repository path
        above.

        CHAOS-3393 codex MED-3: the third element, ``catalog_available``,
        distinguishes a genuine catalog OUTAGE from an authoritative,
        confirmed-zero enumeration -- both used to collapse to the
        identical ``([], 0)`` shape, and the caller
        (``subject_preflight._organization_wide_portfolio_result``) could
        not tell "the organization truly has no authorized projects" from
        "the catalog could not be queried at all", so it silently widened
        BOTH into an ordinary organization-wide PROCEED with unrestricted
        ``ALL_TOOLS`` access -- exactly the wrong failure mode for an
        outage. Only the ``except`` branch below returns ``False``; a
        real, successful zero-row catalog read (including a cache hit,
        which by definition came from a prior successful read) returns
        ``True``.
        """

        try:
            watermark = await self._catalog.watermark(org_id, (EntityKind.PROJECT,))
            cache_key = self._cache_key(
                "organization_portfolio_projects",
                org_id,
                permission_fingerprint,
                {"limit": limit},
                watermark,
            )
            cached = self._cache.get(cache_key)
            if isinstance(cached, tuple) and len(cached) == 2:
                entities, total = cached
                return entities, total, True
            entities, total = await self._catalog.organization_project_entities(
                org_id, limit=limit
            )
            self._cache.put(cache_key, (entities, total))
            return entities, total, True
        except Exception:
            return [], 0, False

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
        # CHAOS-3392 codex MEDIUM: a caller-pinned instant (``resolve``'s
        # ``now``) must NOT be folded into this key as a raw timestamp --
        # main's independent #1366 fix for this same defect did exactly
        # that (a ``resolved_as_of`` parameter here, keyed on
        # ``now.isoformat()``), reintroducing the round-1 flaw this
        # branch's own codex review round refuted: ``resolve_contract``
        # always resolves a concrete instant, even for callers who never
        # pin one, so every "unpinned" production call got its own
        # microsecond-distinct key and the request-local cache never hit.
        # ``resolve``'s caller instead folds the EFFECTIVE resolved
        # boundary into ``payload`` itself when it matters (preset-relative
        # windows only) -- see that method's cache-key comment.
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
