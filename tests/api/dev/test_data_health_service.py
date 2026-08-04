from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dev_health_ops.api.dev.data_health_service import (
    DataHealthService,
    DataHealthState,
    SourceHealthObservation,
)
from dev_health_ops.api.dev.native_evidence import SourceFreshnessPolicy
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ResolvedTimeRange,
    ScopeRef,
    ScopeResolution,
    ScopeResolutionOutcome,
    ScopeResolveRequest,
    TimeRangeRequest,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


class Entitlement:
    async def require(self, _org_id: str) -> None:
        return None


def _resolution() -> ScopeResolution:
    return ScopeResolution(
        outcome=ScopeResolutionOutcome.EXACT,
        entities=(AuthorizedEntity(EntityKind.REPOSITORY, "repo-a", "Repo A"),),
        team_filters=(),
        candidates=(),
        time_range=ResolvedTimeRange(
            "UTC",
            NOW - timedelta(days=30),
            NOW,
            (NOW - timedelta(days=30)).isoformat(),
            NOW.isoformat(),
            NOW - timedelta(days=60),
            NOW - timedelta(days=30),
            (NOW - timedelta(days=60)).isoformat(),
            (NOW - timedelta(days=30)).isoformat(),
        ),
    )


class Authorizer:
    def __init__(self, allowed: bool = True) -> None:
        self.allowed = allowed

    async def resolve(self, *_args: object) -> ScopeResolution:
        if self.allowed:
            return _resolution()
        return ScopeResolution(
            outcome=ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND,
            entities=(),
            team_filters=(),
            candidates=(),
            time_range=_resolution().time_range,
        )


class Reader:
    def __init__(self, values: list[SourceHealthObservation]) -> None:
        self.values = values

    async def read(self, **_kwargs: object) -> tuple[SourceHealthObservation, ...]:
        return tuple(self.values)


def _request() -> ScopeResolveRequest:
    return ScopeResolveRequest(
        explicit_refs=(ScopeRef(EntityKind.REPOSITORY, "repo-a"),),
        time_range=TimeRangeRequest(preset_days=30),
    )


@pytest.mark.asyncio
async def test_data_health_distinguishes_complete_stale_unavailable_unconfigured_and_no_data() -> (
    None
):
    observations = [
        SourceHealthObservation("complete", True, True, NOW, NOW, False, ("repo-a",)),
        SourceHealthObservation(
            "stale",
            True,
            True,
            NOW - timedelta(days=3),
            NOW - timedelta(days=3),
            False,
            ("repo-a",),
        ),
        SourceHealthObservation("unavailable", True, True, NOW, NOW, True, ("repo-a",)),
        SourceHealthObservation("unconfigured", False, True),
        SourceHealthObservation("no_data", True, True),
    ]
    policies = {
        source: SourceFreshnessPolicy(source, f"{source}.v1", timedelta(hours=48))
        for source in ("complete", "stale", "unavailable", "unconfigured", "no_data")
    }
    result = await DataHealthService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        reader=Reader(observations),
        policies=policies,
        now=NOW,
    ).inspect(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        required_sources=list(policies),
    )

    assert {item.source_system: item.state for item in result.sources} == {
        "complete": DataHealthState.COMPLETE,
        "stale": DataHealthState.STALE,
        "unavailable": DataHealthState.UNAVAILABLE,
        "unconfigured": DataHealthState.UNCONFIGURED,
        "no_data": DataHealthState.NO_DATA,
    }
    assert result.complete_eligible is False
    assert all(
        item.confidence_impact is not None
        for item in result.sources
        if item.state is not DataHealthState.COMPLETE
    )


@pytest.mark.asyncio
async def test_required_unavailable_prevents_complete_but_optional_acr_does_not() -> (
    None
):
    reader = Reader(
        [
            SourceHealthObservation(
                "work_items", True, True, NOW, NOW, False, ("repo-a",)
            ),
            SourceHealthObservation(
                "acr", None, False, warning="optional_acr_unavailable"
            ),
        ]
    )
    policies = {
        "work_items": SourceFreshnessPolicy(
            "work_items", "work-items.v1", timedelta(hours=48)
        )
    }
    result = await DataHealthService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        reader=reader,
        policies=policies,
        now=NOW,
    ).inspect(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        required_sources=["work_items", "acr"],
    )
    assert result.complete_eligible is True
    assert result.sources[1].state is DataHealthState.UNAVAILABLE


@pytest.mark.asyncio
async def test_data_health_cross_tenant_scope_is_existence_neutral() -> None:
    result = await DataHealthService(
        entitlement=Entitlement(),
        authorizer=Authorizer(False),
        reader=Reader([]),
        policies={},
        now=NOW,
    ).inspect(
        org_id="org-b",
        permission_fingerprint="allowed",
        scope_request=_request(),
        required_sources=["work_items"],
    )
    assert result.sources[0].state is DataHealthState.UNAUTHORIZED
    assert result.sources[0].warning == "not_found"
    assert result.complete_eligible is False


@pytest.mark.asyncio
async def test_schedule_derived_source_policy_overrides_fallback_threshold() -> None:
    observation = SourceHealthObservation(
        "work_items",
        True,
        True,
        NOW - timedelta(hours=12),
        NOW - timedelta(hours=12),
        False,
        ("repo-a",),
        (),
        timedelta(hours=6),
        "work_items-sync-schedule.v1",
    )
    fallback = SourceFreshnessPolicy(
        "work_items", "work_items-sync-fallback.v1", timedelta(hours=48)
    )
    result = await DataHealthService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        reader=Reader([observation]),
        policies={"work_items": fallback},
        now=NOW,
    ).inspect(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        required_sources=["work_items"],
    )
    assert result.sources[0].state is DataHealthState.STALE
    assert result.sources[0].freshness_policy_version == "work_items-sync-schedule.v1"


@pytest.mark.asyncio
async def test_measured_empty_subject_is_no_data_distinct_from_unavailable() -> None:
    """CHAOS-3375: a genuinely empty, *measured* PROJECT/TEAM repository

    derivation (``NativeDataHealthReader``'s ``project_repository_scope_
    empty`` / ``team_repository_scope_empty`` warnings) must read as
    ``DataHealthState.NO_DATA`` -- "queried, genuinely nothing in scope" --
    never the same ``DataHealthState.UNAVAILABLE`` a *failed* derivation
    (``..._unavailable``) reports. Collapsing the two was the residual gap
    CHAOS-3375 closed on top of #1453's PROJECT-only widening fix.
    """

    observations = [
        SourceHealthObservation(
            "measured_empty",
            True,
            True,
            watermark=None,
            warning="project_repository_scope_empty",
        ),
        SourceHealthObservation(
            "unavailable",
            None,
            False,
            warning="project_repository_scope_unavailable",
        ),
    ]
    policies = {
        source: SourceFreshnessPolicy(source, f"{source}.v1", timedelta(hours=48))
        for source in ("measured_empty", "unavailable")
    }
    result = await DataHealthService(
        entitlement=Entitlement(),
        authorizer=Authorizer(),
        reader=Reader(observations),
        policies=policies,
        now=NOW,
    ).inspect(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=_request(),
        required_sources=list(policies),
    )

    by_source = {item.source_system: item for item in result.sources}
    assert by_source["measured_empty"].state is DataHealthState.NO_DATA
    assert by_source["unavailable"].state is DataHealthState.UNAVAILABLE
    assert by_source["measured_empty"].state is not by_source["unavailable"].state
    assert result.complete_eligible is False


@pytest.mark.asyncio
async def test_relevant_missing_repository_is_not_reported_as_complete() -> None:
    organization_scope = ScopeResolution(
        outcome=ScopeResolutionOutcome.EXACT,
        entities=(AuthorizedEntity(EntityKind.ORGANIZATION, "org-a", "Org A"),),
        team_filters=(),
        candidates=(),
        time_range=_resolution().time_range,
    )

    class OrganizationAuthorizer:
        async def resolve(self, *_args: object) -> ScopeResolution:
            return organization_scope

    observation = SourceHealthObservation(
        source_system="work_items",
        configured=True,
        required=True,
        last_successful_at=NOW,
        watermark=NOW,
        covered_repository_ids=("repo-a",),
        relevant_repository_ids=("repo-a", "repo-b"),
    )
    result = await DataHealthService(
        entitlement=Entitlement(),
        authorizer=OrganizationAuthorizer(),
        reader=Reader([observation]),
        policies={
            "work_items": SourceFreshnessPolicy(
                "work_items", "work-items.v1", timedelta(hours=48)
            )
        },
        now=NOW,
    ).inspect(
        org_id="org-a",
        permission_fingerprint="allowed",
        scope_request=ScopeResolveRequest(
            explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, "org-a"),),
            time_range=TimeRangeRequest(preset_days=30),
        ),
        required_sources=["work_items"],
    )
    assert result.sources[0].state is DataHealthState.NO_DATA
    assert result.sources[0].missing_repository_ids == ("repo-b",)
    assert result.sources[0].coverage == 0.5
    assert result.complete_eligible is False
