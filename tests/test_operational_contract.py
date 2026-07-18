from __future__ import annotations

import asyncio
import re
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.migrations.clickhouse import split_sql_statements
from dev_health_ops.models.operational import (
    OPERATIONAL_ENTITY_TABLES,
    OperationalIncident,
    OperationalService,
    ServiceRepositoryMapping,
    canonical_operational_id,
)
from dev_health_ops.storage.clickhouse import ClickHouseStore
from tests.fixtures.operational_lifecycles import equivalent_operational_lifecycles

_MIGRATION = (
    Path(__file__).parents[1]
    / "src/dev_health_ops/migrations/clickhouse/066_operational_canonical.sql"
)


def _table_columns(statement: str) -> tuple[str, ...]:
    """Extract top-level column names from a migration CREATE TABLE statement."""
    definitions = statement.split("(", maxsplit=1)[1].rsplit(") ENGINE", maxsplit=1)[0]
    columns: list[str] = []
    definition = ""
    depth = 0
    for character in definitions:
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        if character == "," and depth == 0:
            columns.append(definition.strip().split(maxsplit=1)[0])
            definition = ""
        else:
            definition += character
    columns.append(definition.strip().split(maxsplit=1)[0])
    return tuple(columns)


def _create_statements() -> dict[str, str]:
    """Load every canonical operational table statement from migration 066."""
    statements = split_sql_statements(_MIGRATION.read_text(encoding="utf-8"))
    result: dict[str, str] = {}
    for statement in statements:
        match = re.search(r"CREATE TABLE IF NOT EXISTS (\w+)", statement)
        if match is not None:
            result[match.group(1)] = statement
    return result


def test_identity_is_deterministic_for_the_fixed_operational_seed() -> None:
    # Given: a canonical source identity tuple.
    seed = ("org-a", "github", "github-org-a", "operational_incident", "issue-42")

    # When: the tuple is resolved repeatedly and with a changed external id.
    first = canonical_operational_id(*seed)
    second = canonical_operational_id(*seed)
    changed = canonical_operational_id(*seed[:-1], "issue-43")

    # Then: the stable tuple maps to one durable id and changes do not collide.
    assert first == second
    assert first != changed


def test_equivalent_source_lifecycles_keep_source_identity_and_optional_repo_linkage() -> (
    None
):
    # Given: equivalent native and issue-derived operational lifecycle fixtures.
    lifecycles = equivalent_operational_lifecycles()

    # When: their canonical relationships are inspected.
    incident_service_ids = {fixture.incident.service_id for fixture in lifecycles}
    alert_incident_ids = {fixture.alert.incident_id for fixture in lifecycles}

    # Then: every source keeps its own identity and incidents have no repo_id field.
    assert {fixture.provider for fixture in lifecycles} == {
        "atlassian_jsm",
        "github",
        "gitlab",
        "pagerduty",
    }
    assert all(fixture.incident.id != fixture.alert.id for fixture in lifecycles)
    assert incident_service_ids == {fixture.service.id for fixture in lifecycles}
    assert alert_incident_ids == {fixture.incident.id for fixture in lifecycles}
    assert "repo_id" not in {field.name for field in fields(OperationalIncident)}


def test_clean_install_migration_creates_each_canonical_operational_table() -> None:
    # Given: the clean-install migration statements.
    statements = _create_statements()

    # When: the table catalog is compared to the public entity catalog.
    created_tables = set(statements)
    expected_tables = set(OPERATIONAL_ENTITY_TABLES.values())

    # Then: the migration can establish all twelve canonical entity tables.
    assert created_tables == expected_tables
    assert len(created_tables) == 12


def test_clean_install_runner_applies_migration_066() -> None:
    # Given: a fresh non-default ClickHouse database with no recorded migrations.
    client = MagicMock()
    client.query.return_value.result_rows = []
    store = ClickHouseStore("clickhouse://localhost:8123/operational_contract_test")

    # When: the standard migration runner initializes the store.
    with patch("clickhouse_connect.get_client", return_value=client):
        asyncio.run(store.__aenter__())

    # Then: migration 066 creates its tables and records its application.
    commands = [call.args[0] for call in client.command.call_args_list]
    versions = [
        call.kwargs["parameters"]["version"]
        for call in client.command.call_args_list
        if "INSERT INTO schema_migrations" in call.args[0]
    ]
    assert any(
        "CREATE TABLE IF NOT EXISTS operational_incidents" in command
        for command in commands
    )
    assert "066_operational_canonical.sql" in versions


def test_dataclass_columns_match_the_canonical_clickhouse_schema() -> None:
    # Given: the public dataclass and table catalog.
    statements = _create_statements()

    # When: field order is compared to the ClickHouse column order.
    actual = {
        table: _table_columns(statements[table])
        for entity, table in OPERATIONAL_ENTITY_TABLES.items()
    }
    expected = {
        table: tuple(field.name for field in fields(entity))
        for entity, table in OPERATIONAL_ENTITY_TABLES.items()
    }

    # Then: writers can insert every dataclass field without a schema adapter.
    assert actual == expected


def test_operational_sorting_keys_are_org_scoped_and_versioned() -> None:
    # Given: the clean-install DDL.
    statements = _create_statements()

    # When: each entity table engine and sorting key are inspected.
    compliant = [
        "ENGINE = ReplacingMergeTree(source_version_at)" in statement
        and "ORDER BY (org_id, id)" in statement
        for statement in statements.values()
    ]

    # Then: every deduplication key begins with the tenant boundary.
    assert all(compliant)


def test_store_inserts_a_canonical_service_with_org_id_parity() -> None:
    # Given: a store scoped to an organization and an explicitly bound service.
    class RecordingClient:
        def __init__(self) -> None:
            self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

        def insert(
            self,
            table: str,
            matrix: list[list[object]],
            *,
            column_names: list[str],
            settings: dict[str, object],
        ) -> None:
            self.inserts.append((table, matrix, column_names))

    service = OperationalService(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="service",
        external_id="payments-api",
        source_version_at=datetime(2026, 7, 17, tzinfo=timezone.utc),
        observed_at=datetime(2026, 7, 17, tzinfo=timezone.utc),
        last_synced=datetime(2026, 7, 17, tzinfo=timezone.utc),
        name="Payments API",
    )
    store = ClickHouseStore("clickhouse://unused")
    store.client = RecordingClient()
    store.org_id = "org-example"

    # When: the canonical entity is written.
    asyncio.run(store.insert_operational_services([service]))

    # Then: _insert_rows receives parity columns and preserves the identity org id.
    table, matrix, columns = store.client.inserts[0]
    row = matrix[0]
    assert table == "operational_services"
    assert tuple(columns) == tuple(field.name for field in fields(OperationalService))
    assert row[columns.index("org_id")] == "org-example"


def test_store_rejects_mixed_canonical_entity_batches() -> None:
    # Given: two distinct entity types from the same canonical lifecycle.
    lifecycle = equivalent_operational_lifecycles()[0]
    store = ClickHouseStore("clickhouse://unused")

    # When: an internal batch mixes table schemas.
    with pytest.raises(ValueError, match="one entity type"):
        asyncio.run(
            store._insert_operational_rows(
                "operational_services", [lifecycle.service, lifecycle.incident]
            )
        )


def test_store_loads_latest_non_deleted_operational_incidents_for_a_window() -> None:
    # Given: a ClickHouse result for a current incident lifecycle row.
    incident = equivalent_operational_lifecycles()[0].incident
    row = tuple(getattr(incident, field.name) for field in fields(OperationalIncident))

    class QueryResult:
        def __init__(self) -> None:
            self.result_rows = [row]

    class RecordingClient:
        def __init__(self) -> None:
            self.query_call: tuple[str, dict[str, datetime | str]] | None = None

        def query(
            self, query: str, parameters: dict[str, datetime | str]
        ) -> QueryResult:
            self.query_call = (query, parameters)
            return QueryResult()

    store = ClickHouseStore("clickhouse://unused")
    store.client = RecordingClient()
    start = incident.observed_at - timedelta(hours=1)
    end = incident.observed_at + timedelta(hours=1)

    # When: the compatibility reader loads the tenant and time window.
    actual = asyncio.run(store.load_operational_incidents("org-example", start, end))

    # Then: the reader uses FINAL and returns the canonical incident contract.
    assert store.client.query_call is not None
    query, parameters = store.client.query_call
    assert "operational_incidents FINAL" in query
    assert "is_deleted = 0" in query
    assert parameters["org_id"] == "org-example"
    assert actual == [incident]


def test_store_hides_the_latest_incident_tombstone() -> None:
    # Given: ClickHouse FINAL has selected a newer deleted incident version.
    class QueryResult:
        result_rows: list[tuple[()]] = []

    class TombstoneClient:
        def query(
            self, query: str, parameters: dict[str, datetime | str]
        ) -> QueryResult:
            assert "FINAL" in query
            assert "is_deleted = 0" in query
            return QueryResult()

    incident = equivalent_operational_lifecycles()[0].incident
    store = ClickHouseStore("clickhouse://unused")
    store.client = TombstoneClient()

    # When: the compatibility reader loads the tombstoned incident window.
    actual = asyncio.run(
        store.load_operational_incidents(
            incident.org_id,
            incident.observed_at - timedelta(hours=1),
            incident.observed_at + timedelta(hours=1),
        )
    )

    # Then: the current tombstone is absent from the canonical read result.
    assert actual == []


def test_store_loads_active_mapping_by_org_service_and_repository() -> None:
    service = equivalent_operational_lifecycles()[0].service
    mapping = ServiceRepositoryMapping(
        org_id=service.org_id,
        provider=service.provider,
        provider_instance_id=service.provider_instance_id,
        source_entity_type="admin_configuration",
        external_id="svc:github:full-chaos/payments:admin.v1",
        source_version_at=service.observed_at,
        service_id=service.id,
        repo_full_name="full-chaos/payments",
        repo_provider="github",
        mapping_kind="admin_configuration_exact",
        rule_id="service_repository_mapping.admin.v1",
        relationship_provenance="admin_configuration",
        relationship_confidence=1.0,
    )
    row = tuple(
        getattr(mapping, field.name) for field in fields(ServiceRepositoryMapping)
    )

    class QueryResult:
        result_rows = [row]

    class RecordingClient:
        def __init__(self) -> None:
            self.parameters: dict[str, str] | None = None

        def query(self, query: str, parameters: dict[str, str]) -> QueryResult:
            assert "operational_service_repository_mappings FINAL" in query
            self.parameters = parameters
            return QueryResult()

    store = ClickHouseStore("clickhouse://unused")
    store.client = RecordingClient()

    actual = asyncio.run(
        store.load_operational_service_repository_mappings(
            service.org_id,
            service_id=service.id,
        )
    )

    assert actual == [mapping]
    assert store.client.parameters == {
        "org_id": service.org_id,
        "service_id": service.id,
    }


def test_store_load_active_mappings_filters_by_is_active_not_is_deleted() -> None:
    # Given: an active mapping row; the mapping table has is_active, not is_deleted.
    mapping = ServiceRepositoryMapping(
        org_id="org-a",
        provider="pagerduty",
        provider_instance_id="pd-a",
        source_entity_type="pagerduty_service_metadata",
        external_id="svc:github:full-chaos/api:metadata.v1",
        source_version_at=datetime(2026, 7, 17, tzinfo=timezone.utc),
        service_id="svc-1",
        repo_full_name="full-chaos/api",
        repo_provider="github",
        relationship_provenance="pagerduty_service_metadata",
        relationship_confidence=0.95,
    )
    row = tuple(
        getattr(mapping, field.name) for field in fields(ServiceRepositoryMapping)
    )

    class QueryResult:
        result_rows = [row]

    class RecordingClient:
        def __init__(self) -> None:
            self.query_text: str | None = None

        def query(self, query: str, parameters: dict[str, str]) -> QueryResult:
            self.query_text = query
            return QueryResult()

    store = ClickHouseStore("clickhouse://unused")
    store.client = RecordingClient()

    # When: reconciliation loads active mapping evidence via the generic reader.
    actual = asyncio.run(
        store.load_active_operational_entities(
            ServiceRepositoryMapping,
            org_id="org-a",
            provider="pagerduty",
            provider_instance_id="pd-a",
            source_entity_type="pagerduty_service_metadata",
        )
    )

    # Then: the SQL filters on the real is_active column, never is_deleted.
    assert actual == [mapping]
    assert store.client.query_text is not None
    assert "is_active = 1" in store.client.query_text
    assert "is_deleted" not in store.client.query_text
