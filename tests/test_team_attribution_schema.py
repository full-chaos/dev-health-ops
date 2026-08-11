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


def test_migration_054_creates_identities() -> None:
    # CHAOS-2600 CS5: ClickHouse-native identity records (canonical_id ->
    # provider identities + team membership). The CH table is named `identities`
    # (not `identity_mappings`) so it does not collide with the Postgres
    # `identity_mappings` table during org-deletion purge. ReplacingMergeTree
    # keyed on the logical identity so FINAL reads are well-formed.
    migration = Path(
        "src/dev_health_ops/migrations/clickhouse/054_identities.sql"
    ).read_text()

    assert "CREATE TABLE IF NOT EXISTS identities" in migration
    # Must NOT define a CH table literally named identity_mappings (PG owns it).
    assert "CREATE TABLE IF NOT EXISTS identity_mappings" not in migration
    assert "ENGINE = ReplacingMergeTree(updated_at)" in migration
    assert "ORDER BY (org_id, canonical_id)" in migration
    for column in (
        "canonical_id",
        "identity_uuid",
        "provider_identities",
        "team_ids",
    ):
        assert column in migration
    # The migration splitter strips comments before splitting on ';', but the
    # committed-migration guard also forbids ';' inside comment text — assert it
    # holds for this file too.
    for line in migration.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            assert ";" not in stripped


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
        assert sink.client.insert.call_args.args[0] == "project_declared_state_history"
        assert "project_key" in sink.client.insert.call_args.kwargs["column_names"]

        tables_written = [call.args[0] for call in sink.client.insert.call_args_list]
        assert tables_written == ["projects", "project_declared_state_history"], (
            "CHAOS-3563: every project sync must append to the additive history "
            "table IN ADDITION TO (never instead of) the existing `projects` "
            "write. PR #1602 review F5 (PLAUSIBLE): `projects` writes FIRST -- "
            "if the history table is missing on a stale worker image, that "
            "failure must not abort `projects`' own sync too and cascade "
            "staleness org-wide; the history insert stays fail-loud (its "
            "exception still propagates, just AFTER `projects` already "
            "committed)."
        )
        assert (
            sink.client.insert.call_args_list[0].args[1]
            == sink.client.insert.call_args_list[1].args[1]
        ), "both writes must carry the identical row matrix -- no drift"

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
        "project_declared_state_history",
        "members",
        "team_repo_ownership",
        "work_item_team_attributions",
    ], (
        "PR #1602 review F2 (CONFIRMED): ClickHouseStore.insert_projects must "
        "mirror into the additive history table too, IN ADDITION TO (never "
        "instead of) `projects` -- otherwise this writer's declared state "
        "silently vanishes from _PROJECT_DECLARED_FACTS_SQL's history-backed "
        "read the moment `projects`' own ReplacingMergeTree collapses it. "
        "`projects` first (PR #1602 review F5's ordering, applied here too)."
    )
    assert captured[0][2][0]["updated_at"] is not None
    assert captured[0][2][0]["last_synced"] is not None
    assert captured[1][2][0]["id"] == "proj-1"
    assert "repo_id" in captured[3][1]
    assert captured[3][2][0]["repo_id"] == repo_id
    assert captured[4][2][0]["source"] == "native_team"
    assert captured[4][2][0]["repo_id"] == uuid.UUID(int=0)
