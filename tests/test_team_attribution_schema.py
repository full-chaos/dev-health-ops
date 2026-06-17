from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.metrics.schemas import (
    MemberRecord,
    ProjectRecord,
    TeamMembershipRecord,
    TeamProjectOwnershipRecord,
    TeamRepoOwnershipRecord,
    WorkItemTeamAttributionRecord,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.storage.clickhouse import ClickHouseStore


def test_migration_051_creates_attribution_dimensions() -> None:
    migration = Path(
        "src/dev_health_ops/migrations/clickhouse/051_team_attribution_dimensions.sql"
    ).read_text()

    for table in (
        "projects",
        "members",
        "team_memberships",
        "team_project_ownership",
        "team_repo_ownership",
        "work_item_team_attributions",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in migration

    for column in ("provider", "native_team_key", "parent_team_id"):
        assert f"ALTER TABLE teams ADD COLUMN IF NOT EXISTS {column}" in migration

    assert "ORDER BY (org_id, provider, id)" in migration
    assert (
        "ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source)"
        in migration
    )


def test_metrics_sink_writes_dimension_and_attribution_columns() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    repo_id = uuid.uuid4()

    with patch.object(ClickHouseMetricsSink, "__init__", lambda self, dsn: None):
        sink = ClickHouseMetricsSink("clickhouse://dummy")
        sink.client = MagicMock()

        sink.write_projects(
            [
                ProjectRecord(
                    id="proj-1",
                    org_id="org-1",
                    provider="linear",
                    project_key="PLAT",
                    name="Platform",
                    is_active=1,
                    updated_at=now,
                    last_synced=now,
                )
            ]
        )
        assert sink.client.insert.call_args.args[0] == "projects"
        assert "project_key" in sink.client.insert.call_args.kwargs["column_names"]

        sink.write_members(
            [
                MemberRecord(
                    org_id="org-1",
                    member_id="user-1",
                    name="Ada",
                    email="ada@example.com",
                    provider_identities='{"linear":"ada"}',
                    is_active=1,
                    updated_at=now,
                )
            ]
        )
        assert sink.client.insert.call_args.args[0] == "members"

        sink.write_team_memberships(
            [
                TeamMembershipRecord(
                    org_id="org-1",
                    provider="linear",
                    team_id="CHAOS",
                    member_id="user-1",
                    source="native",
                    is_primary=1,
                    specificity=10,
                    priority=0,
                    valid_from=now,
                    updated_at=now,
                )
            ]
        )
        assert sink.client.insert.call_args.args[0] == "team_memberships"

        sink.write_team_project_ownership(
            [
                TeamProjectOwnershipRecord(
                    org_id="org-1",
                    provider="linear",
                    team_id="CHAOS",
                    project_id="proj-1",
                    project_key="PLAT",
                    source="native",
                    is_primary=1,
                    specificity=100,
                    priority=0,
                    valid_from=now,
                    updated_at=now,
                )
            ]
        )
        assert sink.client.insert.call_args.args[0] == "team_project_ownership"

        sink.write_team_repo_ownership(
            [
                TeamRepoOwnershipRecord(
                    org_id="org-1",
                    provider="github",
                    team_id="CHAOS",
                    repo_id=repo_id,
                    repo_full_name="full-chaos/dev-health",
                    match_type="exact",
                    source="provider_access",
                    is_primary=0,
                    specificity=50,
                    priority=10,
                    valid_from=now,
                    updated_at=now,
                )
            ]
        )
        assert sink.client.insert.call_args.args[0] == "team_repo_ownership"

        sink.write_work_item_team_attributions(
            [
                WorkItemTeamAttributionRecord(
                    org_id="org-1",
                    repo_id=repo_id,
                    work_item_id="linear:CHAOS-1",
                    provider="linear",
                    team_id="CHAOS",
                    team_name="Fullchaos",
                    source="native_team",
                    is_primary=1,
                    confidence="high",
                    evidence="native_team_key=CHAOS",
                    computed_at=now,
                )
            ]
        )
        assert sink.client.insert.call_args.args[0] == "work_item_team_attributions"

        sink.write_work_item_team_attributions(
            [
                WorkItemTeamAttributionRecord(
                    org_id="org-1",
                    work_item_id="linear:CHAOS-2",
                    provider="linear",
                    team_id="CHAOS",
                    team_name="Fullchaos",
                    source="native_team",
                    is_primary=1,
                    confidence="high",
                    evidence="native_team_key=CHAOS",
                    computed_at=now,
                )
            ]
        )
        columns = sink.client.insert.call_args.kwargs["column_names"]
        row = sink.client.insert.call_args.args[1][0]
        assert row[columns.index("repo_id")] == uuid.UUID(int=0)


@pytest.mark.asyncio
async def test_async_store_writes_team_attribution_tables() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    repo_id = uuid.uuid4()
    store = ClickHouseStore("clickhouse://localhost:8123/stats")
    captured: list[tuple[str, list[str], list[dict[str, Any]]]] = []

    async def _capture(
        table: str, columns: list[str], rows: list[dict[str, Any]]
    ) -> None:
        captured.append((table, columns, rows))

    setattr(store, "_insert_rows", AsyncMock(side_effect=_capture))

    await store.insert_projects(
        [
            {
                "id": "proj-1",
                "org_id": "org-1",
                "provider": "linear",
                "project_key": "PLAT",
                "name": "Platform",
                "updated_at": now,
                "last_synced": now,
            }
        ]
    )
    await store.insert_members(
        [
            {
                "org_id": "org-1",
                "member_id": "user-1",
                "name": "Ada",
                "provider_identities": '{"linear":"ada"}',
                "updated_at": now,
            }
        ]
    )
    await store.insert_team_repo_ownership(
        [
            {
                "org_id": "org-1",
                "provider": "github",
                "team_id": "CHAOS",
                "repo_id": repo_id,
                "repo_full_name": "full-chaos/dev-health",
                "match_type": "exact",
                "source": "provider_access",
                "valid_from": now,
                "updated_at": now,
            }
        ]
    )
    await store.insert_work_item_team_attributions(
        [
            {
                "org_id": "org-1",
                "work_item_id": "linear:CHAOS-1",
                "provider": "linear",
                "team_id": "CHAOS",
                "team_name": "Fullchaos",
                "source": "native_team",
                "is_primary": 1,
                "confidence": "high",
                "evidence": "native_team_key=CHAOS",
                "computed_at": now,
            }
        ]
    )

    assert [entry[0] for entry in captured] == [
        "projects",
        "members",
        "team_repo_ownership",
        "work_item_team_attributions",
    ]
    assert "repo_id" in captured[2][1]
    assert captured[2][2][0]["repo_id"] == repo_id
    assert captured[3][2][0]["source"] == "native_team"
    assert captured[3][2][0]["repo_id"] == uuid.UUID(int=0)
