"""Live ClickHouse guarantees for canonical operational source-version ordering."""

from __future__ import annotations

import asyncio
import os
from dataclasses import asdict, fields, replace
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from uuid import uuid4

import pytest

from dev_health_ops.models.operational import OperationalIncident, operational_columns
from dev_health_ops.storage.operational_current import current_operational_rows_sql

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    clickhouse_uri = CLICKHOUSE_URI
    assert clickhouse_uri is not None
    database = (urlparse(clickhouse_uri).path or "").lstrip("/")
    if database in ("", "default"):
        pytest.skip("refusing to run ClickHouse schema setup against default")
    result = ClickHouseMetricsSink(clickhouse_uri)
    result.ensure_schema(force=True)
    yield result
    result.close()


def test_operational_incident_uses_source_version_for_tombstone_ordering(sink) -> None:
    # Given: a current incident, newer tombstone, duplicate, and delayed older source event.
    org_id = f"test-chaos-2955-{uuid4()}"
    source_version = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
    incident = OperationalIncident(
        org_id=org_id,
        provider="pagerduty",
        provider_instance_id="pd-test",
        source_entity_type="incident",
        external_id="incident-1",
        source_version_at=source_version,
        last_synced=source_version,
        title="Payments incident",
        started_at=source_version,
    )
    tombstone = replace(
        incident,
        source_version_at=source_version + timedelta(minutes=2),
        last_synced=source_version + timedelta(minutes=3),
        is_deleted=True,
        deleted_at=source_version + timedelta(minutes=2),
    )
    duplicate = replace(tombstone, last_synced=source_version + timedelta(minutes=4))
    delayed_old_event = replace(
        incident,
        source_version_at=source_version - timedelta(minutes=1),
        last_synced=source_version + timedelta(minutes=5),
    )
    columns = list(operational_columns(OperationalIncident))

    try:
        # When: source-order-conflicting rows are written and merged.
        sink.client.insert(
            "operational_incidents",
            [
                [asdict(row)[column] for column in columns]
                for row in (incident, tombstone, duplicate, delayed_old_event)
            ],
            column_names=columns,
        )
        sink.client.command("OPTIMIZE TABLE operational_incidents FINAL")
        result = sink.client.query(
            "SELECT is_deleted, source_version_at FROM "
            + current_operational_rows_sql(
                "operational_incidents", ("id = {id:String}",)
            ),
            parameters={"org_id": org_id, "id": incident.id},
        )
        visible = sink.client.query(
            "SELECT id FROM "
            + current_operational_rows_sql(
                "operational_incidents", ("id = {id:String}", "is_deleted = 0")
            ),
            parameters={"org_id": org_id, "id": incident.id},
        )
        types = sink.client.query(
            "DESCRIBE TABLE operational_service_repository_mappings"
        )

        # Then: the newer source tombstone wins despite a later ingestion timestamp.
        assert result.result_rows == [
            (1, tombstone.source_version_at.replace(tzinfo=None))
        ]
        assert visible.result_rows == []
        column_types = {row[0]: row[1] for row in types.result_rows}
        assert column_types["repo_id"] == "Nullable(UUID)"
    finally:
        sink.client.command(
            "ALTER TABLE operational_incidents DELETE "
            "WHERE org_id = {org_id:String} SETTINGS mutations_sync=2",
            parameters={"org_id": org_id},
        )


def test_operational_incident_columns_match_the_live_schema(sink) -> None:
    # Given: the canonical incident dataclass and migrated table.
    columns = {
        row[0]
        for row in sink.client.query("DESCRIBE TABLE operational_incidents").result_rows
    }

    # When: their declared columns are compared.
    expected = {field.name for field in fields(OperationalIncident)}

    # Then: live writes use the complete current contract.
    assert columns == expected


def test_operational_native_write_and_legacy_backfill_are_idempotent(sink) -> None:
    # Given: live and legacy incident sources scoped to an isolated organization.
    from dev_health_ops.backfill.operational_clickhouse import (
        run_canonical_operational_backfill,
    )
    from dev_health_ops.metrics.sinks.ingestion import IngestionSink
    from dev_health_ops.models.atlassian_ops import (
        AtlassianOpsAlert,
        AtlassianOpsIncident,
        AtlassianOpsSchedule,
    )
    from dev_health_ops.models.git import Incident, Repo
    from dev_health_ops.providers.operational_migration import (
        AtlassianOpsRows,
        AtlassianOpsSource,
        IssueIncidentSource,
        map_atlassian_ops_batch,
        map_issue_incidents,
    )
    from dev_health_ops.storage.clickhouse import ClickHouseStore

    clickhouse_uri = CLICKHOUSE_URI
    assert clickhouse_uri is not None
    org_id = f"test-chaos-2963-{uuid4()}"
    repo_id = uuid4()
    source_version = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)

    async def seed_native_and_legacy_rows() -> None:
        async with ClickHouseStore(clickhouse_uri) as store:
            store.org_id = org_id
            ingestion_sink = IngestionSink(store)
            repo = Repo(
                id=repo_id,
                repo="acme/api",
                provider="github",
                settings={"source": "github", "github_instance_url": "github.com"},
                tags=["github"],
            )
            legacy_incident = Incident(
                repo_id=repo_id,
                incident_id="17",
                status="closed",
                started_at=source_version,
                resolved_at=source_version,
            )
            atlassian_incident = AtlassianOpsIncident(
                id="atlassian-incident-1",
                url="https://acme.atlassian.net/ops/incident-1",
                summary="Pager incident",
                description=None,
                status="closed",
                severity="high",
                created_at=source_version,
                provider_id="acme-atlassian",
                last_synced=source_version,
            )
            atlassian_alert = AtlassianOpsAlert(
                id="atlassian-alert-1",
                status="closed",
                priority="high",
                created_at=source_version,
                closed_at=source_version,
                last_synced=source_version,
            )
            atlassian_schedule = AtlassianOpsSchedule(
                id="atlassian-schedule-1",
                name="Primary response",
                timezone="UTC",
                last_synced=source_version,
            )
            await store.insert_repo(repo)
            await store._insert_rows(
                "incidents",
                [
                    "repo_id",
                    "incident_id",
                    "status",
                    "started_at",
                    "resolved_at",
                    "last_synced",
                ],
                [
                    {
                        "repo_id": legacy_incident.repo_id,
                        "incident_id": legacy_incident.incident_id,
                        "status": legacy_incident.status,
                        "started_at": legacy_incident.started_at,
                        "resolved_at": legacy_incident.resolved_at,
                        "last_synced": source_version,
                    }
                ],
            )
            await store.insert_atlassian_ops_incidents([atlassian_incident])
            await store.insert_atlassian_ops_alerts([atlassian_alert])
            await store.insert_atlassian_ops_schedules([atlassian_schedule])
            await ingestion_sink.insert_operational_batch(
                map_issue_incidents(
                    (
                        IssueIncidentSource(
                            org_id=org_id,
                            provider="github",
                            provider_instance_id="github.com",
                            repo_id=repo_id,
                            repo_full_name="acme/api",
                            external_id="17",
                            issue_number="17",
                            source_url="https://github.com/acme/api/issues/17",
                            labels=("incident",),
                            raw_status="closed",
                            title="Database unavailable",
                            description=None,
                            created_at=source_version,
                            resolved_at=source_version,
                            source_version_at=source_version,
                        ),
                    )
                )
            )
            await ingestion_sink.insert_operational_batch(
                map_atlassian_ops_batch(
                    AtlassianOpsSource(
                        org_id=org_id,
                        provider_instance_id="atlassian-ops",
                        rows=AtlassianOpsRows(
                            incidents=(atlassian_incident,),
                            alerts=(atlassian_alert,),
                            schedules=(atlassian_schedule,),
                        ),
                    )
                )
            )

    try:
        # When: native canonical writes and repeated legacy backfills coexist.
        asyncio.run(seed_native_and_legacy_rows())
        first_result = asyncio.run(
            run_canonical_operational_backfill(
                clickhouse_uri=clickhouse_uri,
                org_id=org_id,
            )
        )
        second_result = asyncio.run(
            run_canonical_operational_backfill(
                clickhouse_uri=clickhouse_uri,
                org_id=org_id,
            )
        )
        assert first_result.parity_verified is True
        assert second_result.parity_verified is True
        assert first_result.expected_incidents == 2
        assert first_result.verified_incidents == 2
        assert first_result.expected_service_repository_mappings == 1
        assert first_result.verified_service_repository_mappings == 1
        for table in (
            "operational_services",
            "operational_incidents",
            "operational_alerts",
            "operational_on_call_schedules",
            "operational_service_repository_mappings",
        ):
            sink.client.command(f"OPTIMIZE TABLE {table} FINAL")

        # Then: native content wins even when the legacy backfill has the same source time.
        counts = {
            table: sink.client.query(
                f"SELECT count() FROM {current_operational_rows_sql(table)}",
                parameters={"org_id": org_id},
            ).result_rows[0][0]
            for table in (
                "operational_services",
                "operational_incidents",
                "operational_alerts",
                "operational_on_call_schedules",
                "operational_service_repository_mappings",
            )
        }
        assert counts == {
            "operational_services": 1,
            "operational_incidents": 2,
            "operational_alerts": 1,
            "operational_on_call_schedules": 1,
            "operational_service_repository_mappings": 1,
        }
        incident = sink.client.query(
            "SELECT title FROM "
            + current_operational_rows_sql(
                "operational_incidents", ("provider = 'github'",)
            ),
            parameters={"org_id": org_id},
        ).result_rows
        assert incident == [("Database unavailable",)]
    finally:
        for table in (
            "operational_services",
            "operational_incidents",
            "operational_alerts",
            "operational_on_call_schedules",
            "operational_service_repository_mappings",
            "atlassian_ops_incidents",
            "atlassian_ops_alerts",
            "atlassian_ops_schedules",
            "incidents",
            "repos",
        ):
            sink.client.command(
                f"ALTER TABLE {table} DELETE WHERE org_id = {{org_id:String}} "
                "SETTINGS mutations_sync=2",
                parameters={"org_id": org_id},
            )
