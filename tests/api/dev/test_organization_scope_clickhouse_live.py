"""Live-ClickHouse integration coverage for CHAOS-3255.

Connects a real multi-repository organization against a migrated ClickHouse
database and proves the complete server-authorized repository set reaches
resolve_scope.v1's contract *and* every downstream status/change query --
against the actual production SQL, not a mock. Also proves a second
tenant's repositories can never enter the first tenant's authorized set.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev.native_status_change import ClickHouseStatusChangeSource
from dev_health_ops.api.dev.scope_catalog import ClickHouseAuthorizedEntityCatalog
from dev_health_ops.api.dev.scope_service import (
    MAX_REPOSITORIES,
    EntityKind,
    ScopeRef,
    ScopeResolutionService,
    ScopeResolveRequest,
)
from dev_health_ops.api.dev.status_change_service import (
    ChangeSummaryRequest,
    StatusChangeService,
    StatusSnapshotRequest,
)

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
NO_REPOSITORY_SET_WARNING = (
    "Status reads require the complete authorized repository set; "
    "scope was not widened."
)
NO_CHANGE_SET_WARNING = "Observed-change scope was not widened."

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(not CLICKHOUSE_URI, reason="Requires migrated CLICKHOUSE_URI"),
]


@pytest.fixture
def raw_client() -> Any:
    import clickhouse_connect

    assert CLICKHOUSE_URI is not None
    client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def ch_sink() -> Any:
    from dev_health_ops.metrics.sinks.factory import create_sink

    assert CLICKHOUSE_URI is not None
    sink = create_sink(CLICKHOUSE_URI)
    try:
        yield sink
    finally:
        sink.close()


def _insert_repo(client: Any, *, org_id: str, repo_id: str, name: str) -> None:
    client.command(
        "INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES "
        "({repo_id:UUID}, {name:String}, now64(3), now64(3), {org_id:String})",
        parameters={"repo_id": repo_id, "name": name, "org_id": org_id},
    )


@pytest.mark.asyncio
async def test_organization_scope_authorized_repos_reach_status_and_change_reads(
    raw_client: Any, ch_sink: Any
) -> None:
    org_id = f"chaos-3255-{uuid.uuid4().hex[:16]}"
    other_org_id = f"chaos-3255-other-{uuid.uuid4().hex[:16]}"
    repo_ids = [str(uuid.uuid4()) for _ in range(3)]
    other_repo_id = str(uuid.uuid4())
    try:
        for index, repo_id in enumerate(repo_ids):
            _insert_repo(
                raw_client, org_id=org_id, repo_id=repo_id, name=f"org/repo-{index}"
            )
        # A second tenant's repository must never enter org_id's authorized set.
        _insert_repo(
            raw_client, org_id=other_org_id, repo_id=other_repo_id, name="other/repo"
        )

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(ch_sink))
        resolution = await service.resolve_contract(
            org_id,
            "permission-live",
            ScopeResolveRequest(
                explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, org_id),)
            ),
        )

        assert resolution.outcome.value == "exact"
        assert resolution.resolved_scope is not None
        assert resolution.resolved_scope.direct_scope.value == "organization"
        assert sorted(resolution.authorized_repository_ids) == sorted(repo_ids)
        assert other_repo_id not in resolution.authorized_repository_ids
        assert len(repo_ids) <= MAX_REPOSITORIES

        status_service = StatusChangeService(
            ClickHouseStatusChangeSource(ch_sink, now=NOW)
        )
        scope = resolution.resolved_scope
        assert scope.comparison_range is not None
        comparison_range = scope.comparison_range

        snapshot = await status_service.status_snapshot(
            org_id, "permission-live", StatusSnapshotRequest(scope)
        )
        assert NO_REPOSITORY_SET_WARNING not in snapshot.warnings

        change = await status_service.change_summary(
            org_id,
            "permission-live",
            ChangeSummaryRequest(
                scope=scope,
                current_start=scope.time_range.start,
                current_end=scope.time_range.end,
                comparison_start=comparison_range.start,
                comparison_end=comparison_range.end,
            ),
        )
        assert NO_CHANGE_SET_WARNING not in change.warnings
    finally:
        raw_client.command(
            "ALTER TABLE repos DELETE WHERE org_id IN ({org_id:String}, {other_org_id:String})",
            parameters={"org_id": org_id, "other_org_id": other_org_id},
        )


@pytest.mark.asyncio
async def test_organization_scope_above_contract_limit_stays_organization_native(
    raw_client: Any, ch_sink: Any
) -> None:
    """An org above the 20-repository wire cap must resolve exact + non-empty
    status reads without ever serializing a truncated repository list."""
    org_id = f"chaos-3255-wide-{uuid.uuid4().hex[:16]}"
    repo_ids = [str(uuid.uuid4()) for _ in range(MAX_REPOSITORIES + 5)]
    try:
        for index, repo_id in enumerate(repo_ids):
            _insert_repo(
                raw_client, org_id=org_id, repo_id=repo_id, name=f"org/repo-{index}"
            )

        service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(ch_sink))
        resolution = await service.resolve_contract(
            org_id,
            "permission-live",
            ScopeResolveRequest(
                explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, org_id),)
            ),
        )

        assert resolution.outcome.value == "exact"
        assert resolution.resolved_scope is not None
        # Never a truncated, misleadingly "complete" 20-entry list.
        assert resolution.authorized_repository_ids == []
        assert any("exceeds" in warning for warning in resolution.warnings)

        status_service = StatusChangeService(
            ClickHouseStatusChangeSource(ch_sink, now=NOW)
        )
        snapshot = await status_service.status_snapshot(
            org_id, "permission-live", StatusSnapshotRequest(resolution.resolved_scope)
        )
        # The native source re-derives the full >20 repository set itself;
        # it must not fall back to the empty-repositories unavailable path.
        assert NO_REPOSITORY_SET_WARNING not in snapshot.warnings
    finally:
        raw_client.command(
            "ALTER TABLE repos DELETE WHERE org_id = {org_id:String}",
            parameters={"org_id": org_id},
        )


@pytest.mark.asyncio
async def test_organization_scope_with_no_connected_repos_is_explicit(
    raw_client: Any, ch_sink: Any
) -> None:
    org_id = f"chaos-3255-empty-{uuid.uuid4().hex[:16]}"

    service = ScopeResolutionService(ClickHouseAuthorizedEntityCatalog(ch_sink))
    resolution = await service.resolve_contract(
        org_id,
        "permission-live",
        ScopeResolveRequest(explicit_refs=(ScopeRef(EntityKind.ORGANIZATION, org_id),)),
    )

    assert resolution.outcome.value == "unresolved"
    assert resolution.resolved_scope is None
    assert resolution.authorized_repository_ids == []
