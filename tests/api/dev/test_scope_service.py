from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from dev_health_ops.api.dev.contracts import DirectScope
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


class FakeCatalog:
    def __init__(self, entities: list[AuthorizedEntity]) -> None:
        self.entities = entities
        self.exact_calls = 0
        self.search_calls = 0
        self.watermark_calls = 0
        self.fail_exact = False
        self.fail_watermark = False

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
    ) -> list[AuthorizedEntity]:
        self.search_calls += 1
        return [
            entity
            for entity in self.entities
            if entity.kind in kinds and query.casefold() in entity.label.casefold()
        ][:limit]


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
