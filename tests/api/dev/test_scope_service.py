from __future__ import annotations

from datetime import date, datetime, timezone, tzinfo
from typing import Self

import pytest

from dev_health_ops.api.dev import scope_service as scope_service_module
from dev_health_ops.api.dev.contracts import DevScope, DevTimeRange, DirectScope
from dev_health_ops.api.dev.scope_catalog import merge_search_candidates
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ScopeRef,
    ScopeResolutionOutcome,
    ScopeResolutionService,
    ScopeResolveRequest,
    ScopeSearchRequest,
    TimeRangeRequest,
)


class _ScriptedNow(datetime):
    """A ``datetime`` subclass whose ``.now()`` pops from a scripted queue
    instead of reading the real system clock.

    CHAOS-3392 codex MEDIUM: ``ScopeResolutionService.resolve_contract``'s
    "caller omitted ``resolved_at``" fallback is
    ``resolved_at or datetime.now(timezone.utc)`` -- the REAL wall clock,
    called fresh on every invocation. A test that wants to deterministically
    prove two such "unpinned" calls collide (or don't) on the request-local
    cache can't rely on real elapsed microseconds between two lines of test
    code; monkeypatching ``scope_service.datetime`` to this subclass lets a
    test script exactly which instant each ``datetime.now()`` call inside
    the module returns while every OTHER ``datetime`` usage in that module
    (``datetime.combine``, the ``datetime(...)`` constructor, etc.) keeps
    working unchanged, since this class inherits the real implementation of
    everything except ``now`` itself.
    """

    _queue: list[datetime] = []

    @classmethod
    def now(cls, tz: tzinfo | None = None) -> Self:
        value = cls._queue.pop(0)
        result = value if tz is None else value.astimezone(tz)
        return cls(
            result.year,
            result.month,
            result.day,
            result.hour,
            result.minute,
            result.second,
            result.microsecond,
            tzinfo=result.tzinfo,
        )


class FakeCatalog:
    def __init__(
        self,
        entities: list[AuthorizedEntity],
        *,
        organization_repositories: list[str] | None = None,
    ) -> None:
        self.entities = entities
        self.organization_repositories = organization_repositories or []
        self.exact_calls = 0
        self.search_calls = 0
        self.watermark_calls = 0
        self.organization_repository_ids_calls = 0
        self.organization_project_entities_calls = 0
        self.fail_exact = False
        self.fail_watermark = False
        self.fail_organization_repository_ids = False
        self.fail_organization_project_entities = False

    async def watermark(self, org_id: str, kinds: tuple[EntityKind, ...]) -> str:
        self.watermark_calls += 1
        if self.fail_watermark:
            raise RuntimeError("watermark unavailable")
        return f"{org_id}:watermark-1"

    async def exact(
        self, org_id: str, ref: ScopeRef, *, limit: int
    ) -> list[AuthorizedEntity]:
        self.exact_calls += 1
        if self.fail_exact:
            raise RuntimeError("stale catalog")
        return [
            entity
            for entity in self.entities
            if entity.kind is ref.kind
            and (
                entity.canonical_id == ref.value
                or entity.label.casefold() == ref.value.casefold()
            )
        ][:limit]

    async def search(
        self,
        org_id: str,
        query: str,
        kinds: tuple[EntityKind, ...],
        *,
        limit: int,
        include_alias_matches: bool = False,
        preferred_kinds: frozenset[EntityKind] = frozenset(),
    ) -> list[AuthorizedEntity]:
        self.search_calls += 1
        # Ranked and truncated by the production helper: every catalog query
        # applies its ORDER BY in SQL, and since CHAOS-3422 the service
        # preserves the order it is handed rather than re-sorting it, so a
        # double returning rows in seeded order would pin an order the real
        # catalog never emits.
        return merge_search_candidates(
            alias_hits=(),
            substring_hits=[
                entity
                for entity in self.entities
                if entity.kind in kinds and query.casefold() in entity.label.casefold()
            ],
            preferred_kinds=preferred_kinds,
            limit=limit,
        )

    async def organization_repository_ids(
        self, org_id: str, *, limit: int
    ) -> tuple[list[str], int]:
        self.organization_repository_ids_calls += 1
        if self.fail_organization_repository_ids:
            raise RuntimeError("organization repository catalog unavailable")
        ids = sorted(self.organization_repositories)
        return ids[:limit], len(ids)

    async def organization_project_entities(
        self, org_id: str, *, limit: int
    ) -> tuple[list[AuthorizedEntity], int]:
        self.organization_project_entities_calls += 1
        if self.fail_organization_project_entities:
            raise RuntimeError("organization project catalog unavailable")
        projects = sorted(
            (entity for entity in self.entities if entity.kind is EntityKind.PROJECT),
            key=lambda entity: (entity.label.casefold(), entity.canonical_id),
        )
        return projects[:limit], len(projects)


def _entity(
    kind: EntityKind,
    canonical_id: str,
    label: str,
    *,
    repository_id: str | None = None,
) -> AuthorizedEntity:
    return AuthorizedEntity(
        kind=kind,
        canonical_id=canonical_id,
        label=label,
        repository_id=repository_id,
    )


def _org_scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.ORGANIZATION,
        repositories=[],
        entity_refs=[],
        team_ids=[],
        time_range=DevTimeRange(
            start=datetime(2026, 6, 28, tzinfo=timezone.utc),
            end=datetime(2026, 7, 28, tzinfo=timezone.utc),
            timezone="UTC",
        ),
        comparison_range=None,
        surface_context=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
    ],
)
async def test_every_catalog_backed_direct_scope_resolves_exactly(
    kind: EntityKind,
) -> None:
    entity = _entity(kind, f"{kind.value}:id", f"{kind.value} label")
    service = ScopeResolutionService(FakeCatalog([entity]))

    result = await service.resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(explicit_refs=(ScopeRef(kind, entity.canonical_id),)),
    )

    assert result.outcome is ScopeResolutionOutcome.EXACT
    assert result.entities == (entity,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
    ],
)
async def test_every_catalog_backed_direct_scope_handles_ambiguity(
    kind: EntityKind,
) -> None:
    service = ScopeResolutionService(
        FakeCatalog(
            [
                _entity(kind, f"{kind.value}:z", "Duplicate"),
                _entity(kind, f"{kind.value}:a", "duplicate"),
            ]
        )
    )

    result = await service.resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(explicit_refs=(ScopeRef(kind, "Duplicate"),)),
    )

    assert result.outcome is ScopeResolutionOutcome.AMBIGUOUS
    assert [candidate.canonical_id for candidate in result.candidates] == [
        f"{kind.value}:a",
        f"{kind.value}:z",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
    ],
)
async def test_every_catalog_backed_direct_scope_handles_missing_name(
    kind: EntityKind,
) -> None:
    result = await ScopeResolutionService(FakeCatalog([])).resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(explicit_refs=(ScopeRef(kind, "Missing name"),)),
    )

    assert result.outcome is ScopeResolutionOutcome.UNRESOLVED
    assert result.entities == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
    ],
)
async def test_every_catalog_backed_direct_scope_has_existence_neutral_id_failure(
    kind: EntityKind,
) -> None:
    result = await ScopeResolutionService(FakeCatalog([])).resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(kind, f"{kind.value}:cross-tenant-id"),)
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND
    assert result.candidates == ()


@pytest.mark.asyncio
async def test_authenticated_organization_is_an_exact_direct_scope_without_catalog_query() -> (
    None
):
    catalog = FakeCatalog([])
    service = ScopeResolutionService(catalog)

    result = await service.resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-a"),)
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.EXACT
    assert result.entities[0].canonical_id == "org-a"
    assert catalog.exact_calls == 0


@pytest.mark.asyncio
async def test_explicit_missing_reference_never_falls_back_to_organization() -> None:
    service = ScopeResolutionService(FakeCatalog([]))

    result = await service.resolve(
        org_id="org-a",
        permission_fingerprint="perm-a",
        request=ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.REPOSITORY, "missing/repo"),),
            allow_organization_fallback=True,
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.UNRESOLVED
    assert result.entities == ()
    assert "organization" not in result.fallbacks


@pytest.mark.asyncio
async def test_cross_tenant_shaped_id_is_existence_neutral() -> None:
    service = ScopeResolutionService(FakeCatalog([]))

    result = await service.resolve(
        org_id="org-a",
        permission_fingerprint="perm-a",
        request=ScopeResolveRequest(
            explicit_refs=(
                ScopeRef(EntityKind.REPOSITORY, "11111111-1111-1111-1111-111111111111"),
            )
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND
    assert result.candidates == ()


@pytest.mark.asyncio
async def test_explicit_other_organization_is_existence_neutral() -> None:
    service = ScopeResolutionService(FakeCatalog([]))

    result = await service.resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-b"),)
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND
    assert result.entities == ()


@pytest.mark.asyncio
async def test_ambiguous_name_returns_sorted_authorized_candidates_without_picking() -> (
    None
):
    catalog = FakeCatalog(
        [
            _entity(EntityKind.PROJECT, "project-z", "Platform"),
            _entity(EntityKind.PROJECT, "project-a", "platform"),
        ]
    )
    service = ScopeResolutionService(catalog)

    result = await service.resolve(
        org_id="org-a",
        permission_fingerprint="perm-a",
        request=ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.PROJECT, "Platform"),)
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.AMBIGUOUS
    assert result.entities == ()
    assert [candidate.canonical_id for candidate in result.candidates] == [
        "project-a",
        "project-z",
    ]


@pytest.mark.asyncio
async def test_page_context_is_revalidated_and_marked_inherited() -> None:
    repository = _entity(
        EntityKind.REPOSITORY,
        "11111111-1111-1111-1111-111111111111",
        "full-chaos/dev-health",
    )
    catalog = FakeCatalog([repository])
    service = ScopeResolutionService(catalog)

    result = await service.resolve(
        org_id="org-a",
        permission_fingerprint="perm-a",
        request=ScopeResolveRequest(
            page_context_refs=(
                ScopeRef(EntityKind.REPOSITORY, repository.canonical_id),
            )
        ),
    )

    assert catalog.exact_calls == 1
    assert result.outcome is ScopeResolutionOutcome.INHERITED
    assert result.entities == (repository,)


@pytest.mark.asyncio
async def test_team_is_only_a_filter_on_an_authorized_direct_scope() -> None:
    team = _entity(EntityKind.TEAM, "team-a", "Platform")
    service = ScopeResolutionService(FakeCatalog([team]))

    result = await service.resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            team_filter_refs=(ScopeRef(EntityKind.TEAM, team.canonical_id),),
            allow_organization_fallback=True,
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.FILTERED
    assert result.entities[0].kind is EntityKind.ORGANIZATION
    assert result.team_filters == (team,)
    assert result.fallbacks == ("organization",)


@pytest.mark.asyncio
async def test_catalog_failure_is_typed_as_unavailable_not_as_empty_org_scope() -> None:
    catalog = FakeCatalog([])
    catalog.fail_exact = True
    service = ScopeResolutionService(catalog)

    result = await service.resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ISSUE, "jira:CHAOS-3204"),),
            allow_organization_fallback=True,
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.UNRESOLVED
    assert result.warnings == ("catalog_unavailable",)
    assert result.fallbacks == ()


@pytest.mark.asyncio
async def test_watermark_failure_is_typed_as_catalog_unavailable() -> None:
    catalog = FakeCatalog([])
    catalog.fail_watermark = True
    service = ScopeResolutionService(catalog)

    result = await service.resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(explicit_refs=(ScopeRef(EntityKind.PROJECT, "project-a"),)),
    )

    assert result.outcome is ScopeResolutionOutcome.UNRESOLVED
    assert result.warnings == ("catalog_unavailable",)
    assert catalog.exact_calls == 0


@pytest.mark.asyncio
async def test_resolve_scope_v1_returns_canonical_pr_contract() -> None:
    pr_id = "11111111-1111-1111-1111-111111111111#pr42"
    pr = _entity(
        EntityKind.PULL_REQUEST,
        pr_id,
        "Add authorized scope resolution",
        repository_id="11111111-1111-1111-1111-111111111111",
    )
    service = ScopeResolutionService(FakeCatalog([pr]))

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.PULL_REQUEST, pr_id),),
            time_range=TimeRangeRequest(preset_days=30, timezone="UTC"),
        ),
        resolved_at=datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc),
    )

    assert result.schema_version == "dev_scope_resolution.v1"
    assert result.outcome is ScopeResolutionOutcome.EXACT
    assert result.resolved_scope is not None
    assert result.resolved_scope.direct_scope is DirectScope.PULL_REQUEST
    assert result.resolved_scope.entity_refs[0].entity_id == pr_id
    assert result.authorized_repository_ids == [pr.repository_id]
    assert result.authorized_entity_ids == [pr_id]
    assert result.resolved_scope.comparison_range is not None
    assert (
        result.resolved_scope.time_range.end - result.resolved_scope.time_range.start
        == result.resolved_scope.comparison_range.end
        - result.resolved_scope.comparison_range.start
    )


@pytest.mark.asyncio
async def test_resolve_scope_v1_represents_ambiguous_repository_candidates() -> None:
    service = ScopeResolutionService(
        FakeCatalog(
            [
                _entity(EntityKind.REPOSITORY, "repo-z", "platform/api"),
                _entity(EntityKind.REPOSITORY, "repo-a", "Platform/API"),
            ]
        )
    )

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.REPOSITORY, "platform/api"),)
        ),
        resolved_at=datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc),
    )

    assert result.outcome is ScopeResolutionOutcome.AMBIGUOUS
    assert result.resolved_scope is None
    assert [candidate.entity_ref.entity_id for candidate in result.candidates] == [
        "repo-a",
        "repo-z",
    ]
    assert all(
        candidate.entity_ref.entity_type.value == "repository"
        for candidate in result.candidates
    )


@pytest.mark.asyncio
async def test_resolve_scope_v1_returns_canonical_repository_set() -> None:
    repositories = [
        _entity(EntityKind.REPOSITORY, "repo-b", "Org/B"),
        _entity(EntityKind.REPOSITORY, "repo-a", "Org/A"),
    ]
    service = ScopeResolutionService(FakeCatalog(repositories))

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=tuple(
                ScopeRef(EntityKind.REPOSITORY, repository.canonical_id)
                for repository in repositories
            )
        ),
        resolved_at=datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc),
    )

    assert result.resolved_scope is not None
    assert result.resolved_scope.direct_scope is DirectScope.REPOSITORY
    assert result.resolved_scope.repositories == ["repo-a", "repo-b"]
    assert result.authorized_repository_ids == ["repo-a", "repo-b"]


@pytest.mark.asyncio
async def test_disambiguation_candidates_are_capped_at_twenty_five() -> None:
    service = ScopeResolutionService(
        FakeCatalog(
            [
                _entity(EntityKind.PROJECT, f"project:{index:02}", "Duplicate")
                for index in range(30)
            ]
        )
    )

    result = await service.resolve(
        "org-a",
        "perm-a",
        ScopeResolveRequest(explicit_refs=(ScopeRef(EntityKind.PROJECT, "Duplicate"),)),
    )

    assert result.outcome is ScopeResolutionOutcome.AMBIGUOUS
    assert len(result.candidates) == 25


@pytest.mark.asyncio
async def test_explicit_scope_precedes_and_does_not_merge_page_or_conversation_context() -> (
    None
):
    explicit = _entity(EntityKind.ISSUE, "jira:CHAOS-3204", "CHAOS-3204")
    page = _entity(EntityKind.PROJECT, "project-a", "Ask Dev")
    conversation = _entity(EntityKind.REPOSITORY, "repo-a", "full-chaos/other")
    service = ScopeResolutionService(FakeCatalog([explicit, page, conversation]))

    result = await service.resolve(
        org_id="org-a",
        permission_fingerprint="perm-a",
        request=ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ISSUE, explicit.canonical_id),),
            page_context_refs=(ScopeRef(EntityKind.PROJECT, page.canonical_id),),
            conversation_context_refs=(
                ScopeRef(EntityKind.REPOSITORY, conversation.canonical_id),
            ),
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.EXACT
    assert result.entities == (explicit,)


@pytest.mark.asyncio
async def test_request_local_cache_is_partitioned_by_permission_and_watermark() -> None:
    repository = _entity(EntityKind.REPOSITORY, "repo-a", "full-chaos/dev-health")
    catalog = FakeCatalog([repository])
    service = ScopeResolutionService(catalog)
    request = ScopeResolveRequest(
        explicit_refs=(ScopeRef(EntityKind.REPOSITORY, repository.canonical_id),)
    )

    first = await service.resolve("org-a", "perm-a", request)
    second = await service.resolve("org-a", "perm-a", request)
    third = await service.resolve("org-a", "perm-b", request)

    assert first == second == third
    assert catalog.exact_calls == 2
    assert catalog.watermark_calls == 3


@pytest.mark.asyncio
async def test_resolve_contract_unpinned_repeated_calls_share_one_cache_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3392 codex MEDIUM regression (RED on commit 0e029a122):
    production callers of ``resolve_contract`` never pass ``resolved_at``
    -- it falls back to ``datetime.now(timezone.utc)`` INSIDE
    ``resolve_contract`` itself, so two calls in the SAME request each got
    their own fresh, microsecond-distinct instant even though neither
    caller asked to pin anything. The fixed commit folded that raw instant
    directly into ``resolve()``'s request-local cache key, so two such
    "unpinned" calls never shared a cache entry -- defeating the whole
    point of a request-local cache for the overwhelming majority
    (unpinned) production callers. Scripted instants a few hundred
    microseconds apart, same calendar day -- exactly what two real,
    back-to-back ``datetime.now()`` calls in one request would produce.
    """

    repository = _entity(EntityKind.REPOSITORY, "repo-a", "full-chaos/dev-health")
    catalog = FakeCatalog([repository])
    service = ScopeResolutionService(catalog)
    request = ScopeResolveRequest(
        explicit_refs=(ScopeRef(EntityKind.REPOSITORY, repository.canonical_id),)
    )
    _ScriptedNow._queue = [
        datetime(2026, 7, 28, 10, 0, 0, 111111, tzinfo=timezone.utc),
        datetime(2026, 7, 28, 10, 0, 0, 999999, tzinfo=timezone.utc),
    ]
    monkeypatch.setattr(scope_service_module, "datetime", _ScriptedNow)

    first = await service.resolve_contract("org-a", "perm-a", request)
    second = await service.resolve_contract("org-a", "perm-a", request)

    assert first.outcome is ScopeResolutionOutcome.EXACT
    assert second.outcome is ScopeResolutionOutcome.EXACT
    assert catalog.exact_calls == 1, (
        "two unpinned calls in one request must share one cache entry, "
        f"not each hit the catalog (exact_calls={catalog.exact_calls})"
    )


@pytest.mark.asyncio
async def test_resolve_contract_different_effective_days_do_not_collide(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cache-key fix must not COLLAPSE genuinely different preset
    windows onto one entry -- two calls whose effective local day differs
    (24h apart here) must each resolve, and cache, independently."""

    repository = _entity(EntityKind.REPOSITORY, "repo-a", "full-chaos/dev-health")
    catalog = FakeCatalog([repository])
    service = ScopeResolutionService(catalog)
    request = ScopeResolveRequest(
        explicit_refs=(ScopeRef(EntityKind.REPOSITORY, repository.canonical_id),)
    )
    _ScriptedNow._queue = [
        datetime(2026, 7, 28, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 7, 29, 10, 0, tzinfo=timezone.utc),
    ]
    monkeypatch.setattr(scope_service_module, "datetime", _ScriptedNow)

    first = await service.resolve_contract("org-a", "perm-a", request)
    second = await service.resolve_contract("org-a", "perm-a", request)

    assert catalog.exact_calls == 2, (
        "two calls landing on DIFFERENT effective days must not share a "
        f"cache entry (exact_calls={catalog.exact_calls})"
    )
    assert first.resolved_scope is not None
    assert second.resolved_scope is not None
    assert (
        first.resolved_scope.time_range.end != second.resolved_scope.time_range.end
    ), "the two resolutions must carry genuinely different time windows"


@pytest.mark.asyncio
async def test_transient_catalog_failure_on_second_call_cannot_flip_a_cached_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3392 codex MEDIUM behavioral repro (RED on commit 0e029a122):
    before the cache-key fix, two unpinned calls in one request never
    shared a cache entry, so a TRANSIENT catalog failure on the SECOND
    call re-ran full entity resolution (``exact()``, which only ever
    fires on a cache MISS -- ``watermark()`` itself is intentionally
    re-queried on every call regardless of caching, precisely so the
    cache invalidates the instant the catalog's data changes; a
    transient failure THERE can never be masked by any cache-key fix,
    which is why this repro targets ``exact()`` instead) and flipped an
    already-successfully-resolved, already-cacheable request from EXACT
    to UNRESOLVED/``catalog_unavailable`` -- even though the first call,
    microseconds earlier, had already proven the catalog reachable and
    the resolution EXACT. A request-local cache exists precisely to make
    this impossible: a cache HIT must never re-touch ``exact()`` at all.
    """

    repository = _entity(EntityKind.REPOSITORY, "repo-a", "full-chaos/dev-health")
    catalog = FakeCatalog([repository])
    service = ScopeResolutionService(catalog)
    request = ScopeResolveRequest(
        explicit_refs=(ScopeRef(EntityKind.REPOSITORY, repository.canonical_id),)
    )
    _ScriptedNow._queue = [
        datetime(2026, 7, 28, 10, 0, 0, 111111, tzinfo=timezone.utc),
        datetime(2026, 7, 28, 10, 0, 0, 999999, tzinfo=timezone.utc),
    ]
    monkeypatch.setattr(scope_service_module, "datetime", _ScriptedNow)

    first = await service.resolve_contract("org-a", "perm-a", request)
    assert first.outcome is ScopeResolutionOutcome.EXACT

    catalog.fail_exact = True
    second = await service.resolve_contract("org-a", "perm-a", request)

    assert second.outcome is ScopeResolutionOutcome.EXACT, (
        "a transient catalog failure on a second, cache-hit call must "
        f"never surface -- got outcome={second.outcome!r} "
        f"warnings={second.warnings!r}"
    )
    assert "catalog_unavailable" not in second.warnings
    assert catalog.exact_calls == 1, (
        "the second call must be a cache HIT and never re-invoke exact() "
        f"at all (exact_calls={catalog.exact_calls})"
    )


@pytest.mark.asyncio
async def test_scope_search_is_bounded_and_deterministically_ordered() -> None:
    catalog = FakeCatalog(
        [
            _entity(EntityKind.ISSUE, "z", "Same"),
            _entity(EntityKind.ISSUE, "a", "same"),
        ]
    )
    service = ScopeResolutionService(catalog)

    result = await service.search(
        "org-a",
        "perm-a",
        ScopeSearchRequest(query="same", kinds=(EntityKind.ISSUE,), limit=25),
    )

    assert [candidate.canonical_id for candidate in result.candidates] == ["a", "z"]
    assert result.query_version == "resolve-scope.v1"
    assert result.catalog_watermark == "org-a:watermark-1"


@pytest.mark.asyncio
async def test_resolved_at_anchors_the_window_it_stamps() -> None:
    """``resolved_at`` must govern the window, not merely label it.

    ``resolve_contract`` stamped the caller's instant on the contract while
    ``resolve`` took the preset window from ``datetime.now``. Callers that
    pinned the instant therefore got a scope that still moved with the wall
    clock, and every assertion written against it held only until the next
    local midnight -- a whole test module went red overnight on exactly that
    (``tests/api/dev/test_project_scope_status_snapshot_repositories.py``),
    with nothing in the diff that broke it.

    The boundaries below are absolute, so this test cannot itself expire: a
    30-day preset resolved at 2026-08-04T01:44Z is the half-open UTC window
    [2026-07-06, 2026-08-05), whatever day it is read on.
    """

    service = ScopeResolutionService(FakeCatalog([]))
    resolved_at = datetime(2026, 8, 4, 1, 44, tzinfo=timezone.utc)

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-a"),),
            time_range=TimeRangeRequest(preset_days=30, timezone="UTC"),
        ),
        resolved_at=resolved_at,
    )

    assert result.resolved_at == resolved_at
    assert result.requested_scope.time_range.end == datetime(
        2026, 8, 5, tzinfo=timezone.utc
    )
    assert result.requested_scope.time_range.start == datetime(
        2026, 7, 6, tzinfo=timezone.utc
    )


@pytest.mark.asyncio
async def test_two_instants_do_not_share_one_cached_resolution() -> None:
    """The request cache must not serve a window resolved at another instant.

    The window is derived from the instant but was absent from the cache key,
    so the second of two resolutions differing only in ``resolved_at`` would
    silently inherit the first one's window -- the same stale-window bug as
    above, reintroduced through the cache.
    """

    service = ScopeResolutionService(FakeCatalog([]))
    request = ScopeResolveRequest(
        explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-a"),),
        time_range=TimeRangeRequest(preset_days=7, timezone="UTC"),
    )

    first = await service.resolve_contract(
        "org-a",
        "perm-a",
        request,
        resolved_at=datetime(2026, 8, 4, 1, 44, tzinfo=timezone.utc),
    )
    second = await service.resolve_contract(
        "org-a",
        "perm-a",
        request,
        resolved_at=datetime(2026, 8, 5, 1, 44, tzinfo=timezone.utc),
    )

    assert first.requested_scope.time_range.end == datetime(
        2026, 8, 5, tzinfo=timezone.utc
    )
    assert second.requested_scope.time_range.end == datetime(
        2026, 8, 6, tzinfo=timezone.utc
    )


def test_relative_time_range_uses_local_day_boundaries_across_dst() -> None:
    service = ScopeResolutionService(FakeCatalog([]))

    resolved = service.resolve_time_range(
        TimeRangeRequest(preset_days=7, timezone="America/Los_Angeles"),
        now=datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert resolved.local_start == "2026-03-04T00:00:00-08:00"
    assert resolved.local_end == "2026-03-11T00:00:00-07:00"
    assert resolved.utc_start == datetime(2026, 3, 4, 8, 0, tzinfo=timezone.utc)
    assert resolved.utc_end == datetime(2026, 3, 11, 7, 0, tzinfo=timezone.utc)
    assert resolved.comparison_utc_end == resolved.utc_start
    assert resolved.utc_end - resolved.utc_start == (
        resolved.comparison_utc_end - resolved.comparison_utc_start
    )


def test_absolute_date_range_is_inclusive_in_local_timezone() -> None:
    service = ScopeResolutionService(FakeCatalog([]))

    resolved = service.resolve_time_range(
        TimeRangeRequest(
            start_date=date(2026, 11, 1),
            end_date=date(2026, 11, 1),
            timezone="America/Los_Angeles",
        )
    )

    assert resolved.local_start == "2026-11-01T00:00:00-07:00"
    assert resolved.local_end == "2026-11-02T00:00:00-08:00"
    assert (resolved.utc_end - resolved.utc_start).total_seconds() == 25 * 60 * 60


@pytest.mark.parametrize("preset", [0, 1, 8, 29, 31, 89, 91])
def test_only_approved_time_presets_are_accepted(preset: int) -> None:
    service = ScopeResolutionService(FakeCatalog([]))

    with pytest.raises(ValueError, match="7, 30, or 90"):
        service.resolve_time_range(TimeRangeRequest(preset_days=preset))


def test_supporting_evidence_entity_cannot_be_constructed_as_direct_scope() -> None:
    with pytest.raises(ValueError):
        ScopeRef("deployment", "deploy-1")  # type: ignore[arg-type]


def test_repository_set_is_bounded_to_twenty() -> None:
    with pytest.raises(ValueError, match="At most 20 repositories"):
        ScopeResolveRequest(
            explicit_refs=tuple(
                ScopeRef(EntityKind.REPOSITORY, f"org/repo-{index}")
                for index in range(21)
            )
        )


def test_non_repository_direct_scope_is_singular() -> None:
    with pytest.raises(ValueError, match="scopes are singular"):
        ScopeResolveRequest(
            explicit_refs=(
                ScopeRef(EntityKind.ISSUE, "jira:A-1"),
                ScopeRef(EntityKind.ISSUE, "jira:A-2"),
            )
        )


def test_repository_set_cannot_mix_direct_scope_kinds() -> None:
    with pytest.raises(ValueError, match="cannot mix"):
        ScopeResolveRequest(
            explicit_refs=(
                ScopeRef(EntityKind.REPOSITORY, "org/repo"),
                ScopeRef(EntityKind.ISSUE, "jira:A-1"),
            )
        )


# --- CHAOS-3255: organization scope must be executable for status/change reads ---


@pytest.mark.asyncio
async def test_organization_scope_enumerates_complete_authorized_repository_set() -> (
    None
):
    catalog = FakeCatalog([], organization_repositories=["repo-b", "repo-a", "repo-c"])
    service = ScopeResolutionService(catalog)

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-a"),)
        ),
        resolved_at=datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc),
    )

    assert result.outcome is ScopeResolutionOutcome.EXACT
    assert result.resolved_scope is not None
    assert result.resolved_scope.direct_scope is DirectScope.ORGANIZATION
    assert result.authorized_repository_ids == ["repo-a", "repo-b", "repo-c"]
    assert catalog.organization_repository_ids_calls == 1


@pytest.mark.asyncio
async def test_organization_scope_with_zero_repositories_is_insufficient_evidence() -> (
    None
):
    catalog = FakeCatalog([], organization_repositories=[])
    service = ScopeResolutionService(catalog)

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-a"),)
        ),
        resolved_at=datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc),
    )

    assert result.outcome is ScopeResolutionOutcome.UNRESOLVED
    assert result.resolved_scope is None
    assert result.authorized_repository_ids == []
    assert "organization_has_no_authorized_repositories" in result.warnings


@pytest.mark.asyncio
async def test_organization_scope_above_contract_limit_is_not_truncated() -> None:
    repositories = [f"repo-{index:02}" for index in range(25)]
    catalog = FakeCatalog([], organization_repositories=repositories)
    service = ScopeResolutionService(catalog)

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-a"),)
        ),
        resolved_at=datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc),
    )

    assert result.outcome is ScopeResolutionOutcome.EXACT
    assert result.resolved_scope is not None
    assert result.resolved_scope.direct_scope is DirectScope.ORGANIZATION
    # Never serialize a truncated, misleadingly "complete" 20-entry list.
    assert result.authorized_repository_ids == []
    assert any("exceeds" in warning for warning in result.warnings)


@pytest.mark.asyncio
async def test_organization_scope_repository_catalog_failure_is_unresolved() -> None:
    catalog = FakeCatalog([], organization_repositories=["repo-a"])
    catalog.fail_organization_repository_ids = True
    service = ScopeResolutionService(catalog)

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-a"),)
        ),
        resolved_at=datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc),
    )

    assert result.outcome is ScopeResolutionOutcome.UNRESOLVED
    assert result.resolved_scope is None
    assert "catalog_unavailable" in result.warnings


@pytest.mark.asyncio
async def test_organization_scope_repository_enumeration_is_request_cached() -> None:
    catalog = FakeCatalog([], organization_repositories=["repo-a", "repo-b"])
    service = ScopeResolutionService(catalog)
    request = ScopeResolveRequest(
        explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-a"),)
    )

    first = await service.resolve_contract("org-a", "perm-a", request)
    second = await service.resolve_contract("org-a", "perm-a", request)

    assert first.authorized_repository_ids == second.authorized_repository_ids
    assert catalog.organization_repository_ids_calls == 1


@pytest.mark.asyncio
async def test_non_organization_scope_never_queries_organization_repositories() -> None:
    repository = _entity(EntityKind.REPOSITORY, "repo-a", "full-chaos/dev-health")
    catalog = FakeCatalog([repository], organization_repositories=["repo-a"])
    service = ScopeResolutionService(catalog)

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.REPOSITORY, repository.canonical_id),)
        ),
    )

    assert result.resolved_scope is not None
    assert result.resolved_scope.direct_scope is DirectScope.REPOSITORY
    assert catalog.organization_repository_ids_calls == 0


@pytest.mark.asyncio
async def test_team_filtered_organization_scope_skips_repository_enumeration() -> None:
    """A team filter narrows organization scope, but no native status/change
    query applies team filters, so the native execution layer always fails
    closed for this case regardless of repository count (CHAOS-3255). The
    contract layer must not report a repository set or a warning describing
    organization-native execution that will not actually happen."""
    team = _entity(EntityKind.TEAM, "team-a", "Platform")
    catalog = FakeCatalog([team], organization_repositories=["repo-a", "repo-b"])
    service = ScopeResolutionService(catalog)

    result = await service.resolve_contract(
        "org-a",
        "perm-a",
        ScopeResolveRequest(
            team_filter_refs=(ScopeRef(EntityKind.TEAM, team.canonical_id),),
            allow_organization_fallback=True,
        ),
    )

    assert result.outcome is ScopeResolutionOutcome.FILTERED
    assert result.resolved_scope is not None
    assert result.resolved_scope.direct_scope is DirectScope.ORGANIZATION
    assert result.authorized_repository_ids == []
    assert catalog.organization_repository_ids_calls == 0
    assert not any("organization-natively" in warning for warning in result.warnings)


# --- CHAOS-3393: bounded org-wide project enumeration for status.portfolio.v1 ---


@pytest.mark.asyncio
async def test_organization_committed_projects_returns_labeled_bounded_page() -> None:
    entities = [
        _entity(EntityKind.PROJECT, "project-z", "Zeta"),
        _entity(EntityKind.PROJECT, "project-a", "Alpha"),
        _entity(EntityKind.TEAM, "team-a", "Platform"),
    ]
    catalog = FakeCatalog(entities)
    service = ScopeResolutionService(catalog)

    projects, total, catalog_available = await service.organization_committed_projects(
        "org-a", "perm-a", limit=25
    )

    # Deterministic label-then-id order, never insertion order, and never
    # any non-PROJECT entity from the catalog.
    assert [entity.canonical_id for entity in projects] == ["project-a", "project-z"]
    assert total == 2
    assert catalog_available is True
    assert catalog.organization_project_entities_calls == 1


@pytest.mark.asyncio
async def test_organization_committed_projects_discloses_true_total_when_capped() -> (
    None
):
    entities = [
        _entity(EntityKind.PROJECT, f"project-{index:02}", f"Project {index:02}")
        for index in range(30)
    ]
    catalog = FakeCatalog(entities)
    service = ScopeResolutionService(catalog)

    projects, total, catalog_available = await service.organization_committed_projects(
        "org-a", "perm-a", limit=25
    )

    # The page is bounded at the caller's cap, but the TRUE total is
    # returned alongside so the caller can disclose truncation -- never a
    # silently sampled "complete" page.
    assert len(projects) == 25
    assert total == 30
    assert catalog_available is True


@pytest.mark.asyncio
async def test_organization_committed_projects_catalog_failure_is_flagged_unavailable() -> (
    None
):
    """CHAOS-3393 codex MED-3: a catalog OUTAGE must be distinguishable
    from an authoritative, confirmed-zero enumeration -- ``([], 0)`` alone
    is exactly the same shape either way, and the caller
    (``subject_preflight._organization_wide_portfolio_result``) used to
    treat both identically, silently widening a transient catalog failure
    into an ordinary organization-wide PROCEED with unrestricted
    ``ALL_TOOLS`` access. The third element makes the two cases
    structurally distinct at the return-type level, not just in a comment.
    """

    catalog = FakeCatalog([_entity(EntityKind.PROJECT, "project-a", "Alpha")])
    catalog.fail_organization_project_entities = True
    service = ScopeResolutionService(catalog)

    projects, total, catalog_available = await service.organization_committed_projects(
        "org-a", "perm-a", limit=25
    )

    assert projects == []
    assert total == 0
    assert catalog_available is False


@pytest.mark.asyncio
async def test_organization_committed_projects_confirmed_zero_is_flagged_available() -> (
    None
):
    """The other half of MED-3: an authoritative, confirmed-empty catalog
    (the organization genuinely has zero authorized projects) is NOT
    ``catalog_available=False`` -- only a real exception is."""

    catalog = FakeCatalog([])
    service = ScopeResolutionService(catalog)

    projects, total, catalog_available = await service.organization_committed_projects(
        "org-a", "perm-a", limit=25
    )

    assert projects == []
    assert total == 0
    assert catalog_available is True


@pytest.mark.asyncio
async def test_organization_committed_projects_is_request_cached() -> None:
    catalog = FakeCatalog([_entity(EntityKind.PROJECT, "project-a", "Alpha")])
    service = ScopeResolutionService(catalog)

    first = await service.organization_committed_projects("org-a", "perm-a", limit=25)
    second = await service.organization_committed_projects("org-a", "perm-a", limit=25)

    assert first == second
    assert catalog.organization_project_entities_calls == 1


# --- CHAOS-3256: resolve named entities before executing status tools ---


@pytest.mark.asyncio
async def test_resolve_query_contract_commits_exact_named_project_scope() -> None:
    project = _entity(EntityKind.PROJECT, "project-ask-dev", "Ask Dev")
    catalog = FakeCatalog([project])
    service = ScopeResolutionService(catalog)
    base_scope = _org_scope()

    result = await service.resolve_query_contract(
        "org-a",
        "perm-a",
        ScopeSearchRequest(
            query="ask dev",
            kinds=(EntityKind.PROJECT, EntityKind.REPOSITORY),
            limit=25,
        ),
        base_scope=base_scope,
        resolved_at=datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc),
    )

    assert result.outcome is ScopeResolutionOutcome.EXACT
    assert result.resolved_scope is not None
    assert result.resolved_scope.direct_scope is DirectScope.PROJECT
    assert result.resolved_scope.entity_refs[0].entity_id == "project-ask-dev"
    assert result.authorized_entity_ids == ["project-ask-dev"]
    assert result.requested_scope == base_scope


@pytest.mark.asyncio
async def test_resolve_query_contract_commits_exact_named_repository_scope() -> None:
    repository = _entity(EntityKind.REPOSITORY, "repo-a", "full-chaos/dev-health")
    catalog = FakeCatalog([repository])
    service = ScopeResolutionService(catalog)

    result = await service.resolve_query_contract(
        "org-a",
        "perm-a",
        ScopeSearchRequest(
            query="dev-health", kinds=(EntityKind.REPOSITORY,), limit=25
        ),
        base_scope=_org_scope(),
    )

    assert result.outcome is ScopeResolutionOutcome.EXACT
    assert result.resolved_scope is not None
    assert result.resolved_scope.direct_scope is DirectScope.REPOSITORY
    assert result.resolved_scope.repositories == ["repo-a"]
    assert result.authorized_repository_ids == ["repo-a"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        EntityKind.REPOSITORY,
        EntityKind.PROJECT,
        EntityKind.WORK_UNIT,
        EntityKind.ISSUE,
        EntityKind.PULL_REQUEST,
    ],
)
async def test_resolve_query_contract_covers_every_supported_direct_scope(
    kind: EntityKind,
) -> None:
    entity = _entity(
        kind,
        f"{kind.value}:id",
        f"{kind.value} label",
        repository_id="repo-a" if kind is not EntityKind.REPOSITORY else None,
    )
    catalog = FakeCatalog([entity])
    service = ScopeResolutionService(catalog)

    result = await service.resolve_query_contract(
        "org-a",
        "perm-a",
        ScopeSearchRequest(query=f"{kind.value} label", kinds=(kind,), limit=25),
        base_scope=_org_scope(),
    )

    assert result.outcome is ScopeResolutionOutcome.EXACT
    assert result.resolved_scope is not None
    assert result.resolved_scope.direct_scope is DirectScope(kind.value)
    if kind is EntityKind.REPOSITORY:
        assert result.resolved_scope.repositories == [entity.canonical_id]
        assert result.authorized_repository_ids == [entity.canonical_id]
    else:
        assert result.resolved_scope.entity_refs[0].entity_id == entity.canonical_id
        assert result.authorized_repository_ids == ["repo-a"]
        assert result.authorized_entity_ids == [entity.canonical_id]


@pytest.mark.asyncio
async def test_resolve_query_contract_returns_typed_ambiguous_candidates() -> None:
    catalog = FakeCatalog(
        [
            _entity(EntityKind.PROJECT, "project-z", "Platform"),
            _entity(EntityKind.PROJECT, "project-a", "platform"),
        ]
    )
    service = ScopeResolutionService(catalog)

    result = await service.resolve_query_contract(
        "org-a",
        "perm-a",
        ScopeSearchRequest(query="platform", kinds=(EntityKind.PROJECT,), limit=25),
        base_scope=_org_scope(),
    )

    assert result.outcome is ScopeResolutionOutcome.AMBIGUOUS
    assert result.resolved_scope is None
    assert [candidate.entity_ref.entity_id for candidate in result.candidates] == [
        "project-a",
        "project-z",
    ]


@pytest.mark.asyncio
async def test_resolve_query_contract_detects_ambiguity_despite_a_narrow_limit() -> (
    None
):
    """A caller-supplied ``limit=1`` must never truncate the search before
    ambiguity is checked -- that would let it silently commit an arbitrary
    one of several real matches as if it were the unique exact answer."""
    catalog = FakeCatalog(
        [
            _entity(EntityKind.PROJECT, "project-z", "Platform"),
            _entity(EntityKind.PROJECT, "project-a", "platform"),
        ]
    )
    service = ScopeResolutionService(catalog)

    result = await service.resolve_query_contract(
        "org-a",
        "perm-a",
        ScopeSearchRequest(query="platform", kinds=(EntityKind.PROJECT,), limit=1),
        base_scope=_org_scope(),
    )

    assert result.outcome is ScopeResolutionOutcome.AMBIGUOUS
    assert result.resolved_scope is None
    # The typed candidate list itself still honors the caller's requested
    # limit; only the exact-vs-ambiguous decision must not be truncated.
    assert len(result.candidates) == 1


@pytest.mark.asyncio
async def test_resolve_query_contract_not_found_never_falls_back_to_organization() -> (
    None
):
    catalog = FakeCatalog([])
    service = ScopeResolutionService(catalog)

    result = await service.resolve_query_contract(
        "org-a",
        "perm-a",
        ScopeSearchRequest(
            query="does not exist", kinds=(EntityKind.PROJECT,), limit=25
        ),
        base_scope=_org_scope(),
    )

    assert result.outcome is ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND
    assert result.resolved_scope is None
    assert result.candidates == []


@pytest.mark.asyncio
async def test_resolve_query_contract_cross_tenant_entity_is_not_found() -> None:
    # The production catalog is tenant-scoped in every SQL query (org_id is
    # part of every WHERE clause); a fake catalog that never returns another
    # tenant's rows for this query models that authorization boundary.
    catalog = FakeCatalog([])
    service = ScopeResolutionService(catalog)

    result = await service.resolve_query_contract(
        "org-a",
        "perm-a",
        ScopeSearchRequest(
            query="other-tenant-project", kinds=(EntityKind.PROJECT,), limit=25
        ),
        base_scope=_org_scope(),
    )

    assert result.outcome is ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND
    assert result.resolved_scope is None
