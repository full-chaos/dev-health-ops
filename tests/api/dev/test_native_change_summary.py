from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev.contracts import (
    DevEntityRef,
    DevScope,
    DevTimeRange,
    DirectScope,
    EntityType,
    FreshnessState,
)
from dev_health_ops.api.dev.native_status_change import (
    _CI_CHANGES_SQL,
    _DEPLOYMENT_CHANGES_SQL,
    _INCIDENT_CHANGES_SQL,
    _PULL_REQUEST_CHANGES_SQL,
    _REVIEW_CHANGES_SQL,
    ClickHouseStatusChangeSource,
)
from dev_health_ops.api.dev.status_change_service import (
    ChangeCategory,
    ChangeSummaryRequest,
    StatusChangeService,
    StatusResultState,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


def _scope() -> DevScope:
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id="org-a",
        direct_scope=DirectScope.PROJECT,
        repositories=["repo-a"],
        entity_refs=[
            DevEntityRef(
                entity_type=EntityType.PROJECT,
                entity_id="project-a",
                display_label="Project A",
                repository_id="repo-a",
            )
        ],
        time_range=DevTimeRange(
            start=NOW - timedelta(days=14), end=NOW, timezone="UTC"
        ),
    )


def _request(*, max_items: int = 100) -> ChangeSummaryRequest:
    return ChangeSummaryRequest(
        scope=_scope(),
        current_start=NOW - timedelta(days=7),
        current_end=NOW,
        comparison_start=NOW - timedelta(days=14),
        comparison_end=NOW - timedelta(days=7),
        max_items=max_items,
    )


_CLASS_FIXTURES = (
    (
        "FROM git_pull_requests AS pr",
        "pull_requests",
        ChangeCategory.PULL_REQUEST,
        {
            "change_id": "repo-a#pr7#state#merged",
            "entity_id": "repo-a#pr7",
            "display_label": "PR 7",
            "before_value": "open",
            "after_value": "merged",
            "observed_at": NOW - timedelta(hours=5),
            "last_synced": NOW,
        },
    ),
    (
        "FROM git_pull_request_reviews AS review",
        "reviews",
        ChangeCategory.REVIEW,
        {
            "change_id": "repo-a#pr7#review#review-1",
            "entity_id": "repo-a#pr7#review#review-1",
            "display_label": "Review by reviewer-a",
            "before_value": None,
            "after_value": "approved",
            "observed_at": NOW - timedelta(hours=4),
            "last_synced": NOW,
        },
    ),
    (
        "FROM ci_pipeline_runs AS run",
        "ci_runs",
        ChangeCategory.CI,
        {
            "change_id": "repo-a#ci#run-1",
            "entity_id": "repo-a#ci#run-1",
            "display_label": "CI run run-1",
            "before_value": None,
            "after_value": "success",
            "observed_at": NOW - timedelta(hours=3),
            "last_synced": NOW,
        },
    ),
    (
        "FROM deployments AS deployment",
        "deployments",
        ChangeCategory.DEPLOYMENT,
        {
            "change_id": "repo-a#deployment#deployment-1",
            "entity_id": "repo-a#deployment#deployment-1",
            "display_label": "Deployment deployment-1",
            "before_value": None,
            "after_value": "success",
            "observed_at": NOW - timedelta(hours=2),
            "last_synced": NOW,
        },
    ),
    (
        "FROM operational_incidents AS incident",
        "incidents",
        ChangeCategory.INCIDENT,
        {
            "change_id": "incident-1#state#resolved",
            "entity_id": "incident-1",
            "display_label": "Incident 1",
            "before_value": None,
            "after_value": "resolved",
            "observed_at": NOW - timedelta(hours=1),
            "last_synced": NOW,
        },
    ),
)


def _fixture_for(sql: str) -> list[dict[str, Any]]:
    for marker, _, _, row in _CLASS_FIXTURES:
        if marker in sql:
            return [row]
    return []


@pytest.mark.asyncio
async def test_native_change_summary_emits_every_delivery_change_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_params: list[dict[str, Any]] = []

    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed_params.append(params)
        return _fixture_for(sql)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", _request())

    expected_categories = {fixture[2] for fixture in _CLASS_FIXTURES}
    assert {change.category for change in result.changes} == expected_categories
    assert all(change.claim_kind.value == "observed" for change in result.changes)
    refs_by_source = {ref.source_system: ref for ref in result.source_refs}
    for _, source_system, _, _ in _CLASS_FIXTURES:
        assert refs_by_source[source_system].freshness is FreshnessState.FRESH
    assert observed_params
    assert all(params["org_id"] == "org-a" for params in observed_params)
    assert all(params["repository_ids"] == ["repo-a"] for params in observed_params)
    assert all(params["scope_type"] == "project" for params in observed_params)
    assert all(params["entity_id"] == "project-a" for params in observed_params)
    assert all(params["start"] == NOW - timedelta(days=7) for params in observed_params)
    assert all(params["end"] == NOW for params in observed_params)
    assert all(params["limit"] == 100 for params in observed_params)


@pytest.mark.parametrize(
    ("failed_marker", "source_system"),
    [(fixture[0], fixture[1]) for fixture in _CLASS_FIXTURES],
)
@pytest.mark.asyncio
async def test_native_change_summary_degrades_when_delivery_source_is_unavailable(
    monkeypatch: pytest.MonkeyPatch, failed_marker: str, source_system: str
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if failed_marker in sql:
            raise RuntimeError("source unavailable")
        return _fixture_for(sql)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", _request())

    assert result.state is StatusResultState.DEGRADED
    failed_ref = next(
        ref for ref in result.source_refs if ref.source_system == source_system
    )
    assert failed_ref.freshness is FreshnessState.UNAVAILABLE
    assert f"{source_system} source unavailable" in result.warnings


@pytest.mark.parametrize(
    ("missing_marker", "source_system"),
    [(fixture[0], fixture[1]) for fixture in _CLASS_FIXTURES],
)
@pytest.mark.asyncio
async def test_native_change_summary_marks_empty_delivery_source_freshness_unknown(
    monkeypatch: pytest.MonkeyPatch, missing_marker: str, source_system: str
) -> None:
    async def fake_query(
        _client: object, sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if missing_marker in sql:
            return []
        return _fixture_for(sql)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    result = await StatusChangeService(
        ClickHouseStatusChangeSource(object(), now=NOW)
    ).change_summary("org-a", "permission-v1", _request())

    assert result.state is StatusResultState.PARTIAL
    missing_ref = next(
        ref for ref in result.source_refs if ref.source_system == source_system
    )
    assert missing_ref.freshness is FreshnessState.UNKNOWN
    assert all(source_system not in warning for warning in result.warnings)


@pytest.mark.asyncio
async def test_native_change_summary_applies_one_global_deterministic_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        assert params["limit"] == 3
        return _fixture_for(sql)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_status_change.query_dicts", fake_query
    )
    service = StatusChangeService(ClickHouseStatusChangeSource(object(), now=NOW))

    first = await service.change_summary(
        "org-a", "permission-v1", _request(max_items=3)
    )
    second = await service.change_summary(
        "org-a", "permission-v1", _request(max_items=3)
    )

    assert first == second
    assert [change.change_id for change in first.changes] == [
        "repo-a#pr7#state#merged",
        "repo-a#pr7#review#review-1",
        "repo-a#ci#run-1",
    ]


@pytest.mark.parametrize(
    "sql",
    (
        _PULL_REQUEST_CHANGES_SQL,
        _REVIEW_CHANGES_SQL,
        _CI_CHANGES_SQL,
        _DEPLOYMENT_CHANGES_SQL,
        _INCIDENT_CHANGES_SQL,
    ),
)
def test_delivery_change_queries_pin_every_authorization_and_resource_bound(
    sql: str,
) -> None:
    for required in (
        "{org_id:String}",
        "{repository_ids:Array(String)}",
        "{scope_type:String}",
        "{entity_id:String}",
        "{start:DateTime64(3, 'UTC')}",
        "{end:DateTime64(3, 'UTC')}",
        "{limit:UInt32}",
    ):
        assert required in sql
