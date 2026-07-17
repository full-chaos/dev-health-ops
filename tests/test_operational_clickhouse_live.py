"""Live ClickHouse guarantees for canonical operational source-version ordering."""

from __future__ import annotations

import os
from dataclasses import asdict, fields, replace
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from uuid import uuid4

import pytest

from dev_health_ops.models.operational import OperationalIncident, operational_columns

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

    assert CLICKHOUSE_URI is not None
    database = (urlparse(CLICKHOUSE_URI).path or "").lstrip("/")
    if database in ("", "default"):
        pytest.skip("refusing to run ClickHouse schema setup against default")
    result = ClickHouseMetricsSink(CLICKHOUSE_URI)
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
            "SELECT is_deleted, source_version_at FROM operational_incidents FINAL "
            "WHERE org_id = {org_id:String} AND id = {id:String}",
            parameters={"org_id": org_id, "id": incident.id},
        )
        visible = sink.client.query(
            "SELECT id FROM operational_incidents FINAL "
            "WHERE org_id = {org_id:String} AND id = {id:String} AND is_deleted = 0",
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
