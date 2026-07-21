from __future__ import annotations

import asyncio
import importlib
import importlib.util
import os
from dataclasses import asdict, fields
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.models.operational import OperationalService
from dev_health_ops.models.operational_ordering_types import ORDERING_FIELD_NAMES
from dev_health_ops.storage.clickhouse import ClickHouseStore

_MIGRATION = (
    Path(__file__).parents[1]
    / "src/dev_health_ops/migrations/clickhouse/067_operational_ordering_contract.py"
)


def _guard() -> ModuleType:
    return importlib.import_module("dev_health_ops.storage.operational_ordering_guard")


def _migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "operational_ordering_guard_migration", _MIGRATION
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("raw", ["", "0", "3", "v2", " 2 "])
def test_ordering_contract_rejects_malformed_configuration(raw: str) -> None:
    # Given: an explicitly malformed rollout contract value.
    guard = _guard()

    # When: the startup configuration boundary parses it.
    # Then: startup fails closed with a typed configuration error.
    with pytest.raises(guard.OperationalOrderingConfigurationError):
        guard.parse_operational_ordering_contract(raw)


def test_ordering_contract_supports_legacy_bridge_and_current_modes() -> None:
    # Given: the only two supported rollout contract values.
    guard = _guard()

    # When: both are parsed at the configuration boundary.
    parsed = (
        guard.parse_operational_ordering_contract("1"),
        guard.parse_operational_ordering_contract("2"),
        guard.parse_operational_ordering_contract(None),
    )

    # Then: omitted configuration remains rollout-safe legacy until explicit cutover.
    assert parsed == (
        guard.OperationalOrderingContract.LEGACY,
        guard.OperationalOrderingContract.CURRENT,
        guard.OperationalOrderingContract.LEGACY,
    )


def test_old_writer_rejection_logs_only_bounded_rollout_dimensions(caplog) -> None:
    # Given: a legacy bridge trying to enter a migrated v2 table.
    guard = _guard()
    identity = guard.OperationalWriterIdentity(
        table="operational_incidents", service="worker", version="sha-123"
    )

    # When: startup evaluates writer admission.
    with pytest.raises(guard.OperationalOldWriterRejectedError):
        guard.ensure_operational_writer_admission(
            identity,
            guard.OperationalOrderingContract.LEGACY,
            guard.OperationalOrderingContract.CURRENT,
        )

    # Then: the rejection event contains only table, service, and version dimensions.
    record = next(
        item
        for item in caplog.records
        if item.message == "operational_old_writer_rejected"
    )
    assert (record.table, record.service, record.version) == (
        "operational_incidents",
        "worker",
        "sha-123",
    )
    assert not hasattr(record, "org_id")
    assert not hasattr(record, "source")


def test_current_writer_rejects_legacy_schema_as_stale_state() -> None:
    # Given: a v2 writer configured against a legacy table.
    guard = _guard()
    identity = guard.OperationalWriterIdentity(
        table="operational_incidents", service="api", version="sha-456"
    )

    # When: writer admission compares the configured and stored contracts.
    # Then: it fails with a typed stale-state error rather than attempting a write.
    with pytest.raises(guard.OperationalOrderingStaleStateError):
        guard.ensure_operational_writer_admission(
            identity,
            guard.OperationalOrderingContract.CURRENT,
            guard.OperationalOrderingContract.LEGACY,
        )


def test_legacy_bridge_defers_shadow_migration_without_recording_work() -> None:
    # Given: a bridge replica explicitly running contract 1.
    guard = _guard()
    migration = _migration()
    client = MagicMock()

    # When: its ambient migration loop reaches the v2 migration.
    # Then: a typed deferral prevents both table work and migration admission.
    with (
        patch.dict("os.environ", {guard.OPERATIONAL_ORDERING_CONTRACT_ENV: "1"}),
        pytest.raises(
            importlib.import_module(
                "dev_health_ops.migrations.clickhouse"
            ).MigrationDeferred
        ),
    ):
        migration.upgrade(client)
    assert client.method_calls == []


def test_omitted_contract_defers_shadow_migration_without_recording_work() -> None:
    # Given: a pre-cutover deployment with no ordering-contract environment value.
    migration = _migration()
    client = MagicMock()

    # When: the ambient migration loop reaches the candidate-preserving migration.
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("OPERATIONAL_ORDERING_CONTRACT", None)
        with pytest.raises(
            importlib.import_module(
                "dev_health_ops.migrations.clickhouse"
            ).MigrationDeferred
        ):
            migration.upgrade(client)

    # Then: omitted configuration neither mutates tables nor records migration work.
    assert client.method_calls == []


def test_omitted_contract_current_row_read_uses_legacy_final_selection() -> None:
    # Given: an old reader deployment with no ordering-contract configuration.
    current = importlib.import_module("dev_health_ops.storage.operational_current")

    # When: it renders the canonical current-row table expression.
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("OPERATIONAL_ORDERING_CONTRACT", None)
        query = current.current_operational_rows_sql("operational_incidents")

    # Then: the query remains compatible with migration-066 columns and FINAL readers.
    assert " FINAL" in query
    assert "source_revision" not in query


def test_explicit_current_store_startup_rejects_legacy_schema() -> None:
    # Given: an explicit contract-2 process presented with migration-066 tables.
    guard = _guard()
    client = MagicMock()
    client.query.return_value.result_rows = [
        [
            "CREATE TABLE operational_services "
            "(org_id String, id String, source_version_at DateTime64(6, 'UTC')) "
            "ENGINE = ReplacingMergeTree(source_version_at) "
            "ORDER BY (org_id, id)"
        ]
    ]

    async def enter_store() -> None:
        with (
            patch("clickhouse_connect.get_client", return_value=client),
            patch.object(ClickHouseStore, "_ensure_tables", new=AsyncMock()),
        ):
            await ClickHouseStore("clickhouse://unused").__aenter__()

    # When: startup performs writer admission after schema setup.
    # Then: stale legacy state is rejected before any operational write.
    with (
        patch.dict(os.environ, {"OPERATIONAL_ORDERING_CONTRACT": "2"}),
        pytest.raises(guard.OperationalOrderingStaleStateError),
    ):
        asyncio.run(enter_store())


def test_typed_writer_omits_ordering_fields_only_in_explicit_legacy_mode() -> None:
    # Given: the same derived v2 service written by bridge and current modes.
    class RecordingClient:
        def __init__(self) -> None:
            self.columns: list[str] = []

        def insert(
            self,
            _table: str,
            _matrix: list[list[object]],
            *,
            column_names: list[str],
            settings: dict[str, object],
        ) -> None:
            self.columns = column_names

    service = OperationalService(
        org_id="org-a",
        provider="pagerduty",
        provider_instance_id="pd-a",
        source_entity_type="service",
        external_id="service-1",
        source_version_at=datetime.now(timezone.utc),
        name="Service",
    )
    expected = {item.name for item in fields(service)}

    # When: each configured writer serializes through the generic typed seam.
    captured: dict[str, set[str]] = {}
    for contract in (None, "1", "2"):
        with patch.dict(os.environ, {}, clear=False):
            if contract is None:
                os.environ.pop("OPERATIONAL_ORDERING_CONTRACT", None)
                label = "unset"
            else:
                os.environ["OPERATIONAL_ORDERING_CONTRACT"] = contract
                label = contract
            store = ClickHouseStore("clickhouse://unused")
            store.client = RecordingClient()
            asyncio.run(store.insert_operational_services([service]))
            captured[label] = set(store.client.columns)

    # Then: only bridge mode uses legacy columns and v2 always requires all four fields.
    ordering_fields = {
        "source_revision",
        "source_conflict_key",
        "ingest_revision",
        "ordering_contract",
    }
    assert captured["unset"] == expected - ordering_fields
    assert captured["1"] == expected - ordering_fields
    assert captured["2"] == expected


def test_legacy_bridge_reads_legacy_columns_and_hydrates_current_entity() -> None:
    # Given: a contract-1 table row that predates the four ordering columns.
    service = OperationalService(
        org_id="org-a",
        provider="pagerduty",
        provider_instance_id="pd-a",
        source_entity_type="service",
        external_id="service-1",
        source_version_at=datetime.now(timezone.utc),
        name="Service",
    )
    legacy_columns = tuple(
        item.name for item in fields(service) if item.name not in ORDERING_FIELD_NAMES
    )
    values = asdict(service)

    class QueryResult:
        result_rows = [tuple(values[name] for name in legacy_columns)]

    class RecordingClient:
        query_text = ""

        def query(self, query: str, *, parameters: dict[str, str]) -> QueryResult:
            assert parameters["org_id"] == "org-a"
            self.query_text = query
            return QueryResult()

    # When: the bridge reads through the typed current-row seam.
    with patch.dict("os.environ", {"OPERATIONAL_ORDERING_CONTRACT": "1"}):
        store = ClickHouseStore("clickhouse://unused")
        client = RecordingClient()
        store.client = client
        loaded = asyncio.run(
            store.load_active_operational_entities(
                OperationalService,
                org_id="org-a",
                provider="pagerduty",
                provider_instance_id="pd-a",
                source_entity_type="service",
            )
        )

    # Then: selection uses legacy FINAL semantics and reconstructs derived v2 values.
    assert loaded == [service]
    assert " FINAL" in client.query_text
    assert "source_revision" not in client.query_text
