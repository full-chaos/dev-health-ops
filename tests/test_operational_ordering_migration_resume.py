from __future__ import annotations

import importlib.util
from dataclasses import asdict, replace
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, call, patch

import pytest

from dev_health_ops.models.operational import OperationalIncident, operational_columns
from dev_health_ops.models.operational_ordering_types import ORDERING_FIELD_NAMES
from tests.fixtures.operational_entities import all_operational_entities

_MIGRATION = (
    Path(__file__).parents[1]
    / "src/dev_health_ops/migrations/clickhouse/067_operational_ordering_contract.py"
)


def _load_migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "operational_ordering_resume", _MIGRATION
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _BlockStream:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows

    def __enter__(self):
        return iter((self.rows,))

    def __exit__(self, *_args: object) -> None:
        return None


class _CopyClient:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.query_text = ""
        self.inserts: list[tuple[str, list[list[object]], tuple[str, ...]]] = []

    def query_row_block_stream(self, query: str, *, settings: dict[str, int]):
        self.query_text = query
        assert settings == {"max_block_size": 1_000}
        return _BlockStream(self.rows)

    def insert(
        self, table: str, matrix: list[list[object]], *, column_names: tuple[str, ...]
    ) -> None:
        self.inserts.append((table, matrix, column_names))


class _DiscardingCopyClient(_CopyClient):
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        super().__init__(rows)
        self.inserted_rows = 0

    def insert(
        self, table: str, matrix: list[list[object]], *, column_names: tuple[str, ...]
    ) -> None:
        del table, column_names
        self.inserted_rows += len(matrix)


def test_raw_copy_batches_every_distinct_equal_time_candidate_through_builder() -> None:
    # Given: two legacy rows with one identity, timestamp, and different payloads.
    migration = _load_migration()
    first = next(
        entity
        for entity in all_operational_entities()
        if isinstance(entity, OperationalIncident)
    )
    second = replace(first, title="A distinct equal-time candidate")
    legacy_columns = tuple(
        item
        for item in operational_columns(OperationalIncident)
        if item not in ORDERING_FIELD_NAMES
    )
    rows = [
        tuple(asdict(entity)[column] for column in legacy_columns)
        for entity in (first, second)
    ]
    client = _CopyClient(rows)

    # When: the migration streams the raw rows into a v2 shadow.
    stats = migration._copy_raw_rows(
        client,
        "operational_incidents",
        "operational_incidents__ordering_v2",
        OperationalIncident,
    )

    # Then: both full conflict keys survive without a source-side collapsing read.
    assert "FINAL" not in client.query_text
    assert stats.raw_rows == 2
    assert stats.candidate_count == 2
    assert stats.logical_count == 1
    assert len(client.inserts) == 1
    assert client.inserts[0][2] == operational_columns(OperationalIncident)


def test_raw_copy_reports_bounded_counts_at_scale_without_retaining_keys() -> None:
    # Given: thousands of distinct candidates for one logical incident identity.
    migration = _load_migration()
    base = next(
        entity
        for entity in all_operational_entities()
        if isinstance(entity, OperationalIncident)
    )
    legacy_columns = tuple(
        item
        for item in operational_columns(OperationalIncident)
        if item not in ORDERING_FIELD_NAMES
    )
    rows = [
        tuple(
            asdict(replace(base, title=f"candidate-{index:05d}"))[column]
            for column in legacy_columns
        )
        for index in range(2_000)
    ]
    client = _DiscardingCopyClient(rows)

    # When: the raw migration streams the populated table into its shadow.
    stats = migration._copy_raw_rows(
        client,
        "operational_incidents",
        "operational_incidents__ordering_v2",
        OperationalIncident,
    )

    # Then: only scalar counts and one maximum survive beyond each bounded block.
    assert stats.raw_rows == 2_000
    assert stats.candidate_count == 2_000
    assert stats.logical_count == 1
    assert client.inserted_rows == 2_000
    assert all(not isinstance(value, (set, frozenset)) for value in stats)


def test_rebuild_creates_verified_shadow_exchanges_then_runs_resume() -> None:
    # Given: one legacy main table with no leftover shadow.
    migration = _load_migration()
    client = MagicMock()
    stats = migration._CopyStats(0, 0, 0, None)

    # When: the table rebuild reaches the swap boundary.
    with (
        patch.object(migration, "_table_exists", side_effect=(True, False)),
        patch.object(migration, "_show_create", side_effect=("legacy", "v2")),
        patch.object(migration, "_schema_contract", side_effect=(1, 2)),
        patch.object(migration, "_rewrite_ddl", return_value="CREATE SHADOW"),
        patch.object(migration, "_copy_raw_rows", return_value=stats),
        patch.object(migration, "_verify_copy") as verify,
        patch.object(migration, "_resume_exchanged_shadow") as resume,
    ):
        migration._rebuild_table(client, "operational_incidents")

    # Then: verification precedes atomic exchange and post-swap convergence runs.
    verify.assert_called_once()
    assert client.command.call_args_list == [
        call("CREATE SHADOW"),
        call(
            "EXCHANGE TABLES `operational_incidents` "
            "AND `operational_incidents__ordering_v2`"
        ),
    ]
    resume.assert_called_once_with(
        client, "operational_incidents", "operational_incidents__ordering_v2"
    )


def test_rerun_detects_exchanged_shadow_and_converges_without_second_swap() -> None:
    # Given: a v2 main table and the legacy shadow left by an interrupted catch-up.
    migration = _load_migration()
    client = MagicMock()

    # When: the migration reruns after the exchange interruption.
    with (
        patch.object(migration, "_table_exists", side_effect=(True, True)),
        patch.object(migration, "_show_create", return_value="v2"),
        patch.object(migration, "_schema_contract", return_value=2),
        patch.object(migration, "_resume_exchanged_shadow") as resume,
    ):
        migration._rebuild_table(client, "operational_incidents")

    # Then: it resumes the old shadow exactly once and never exchanges again.
    resume.assert_called_once_with(
        client, "operational_incidents", "operational_incidents__ordering_v2"
    )
    client.command.assert_not_called()


def test_failed_resume_keeps_old_shadow_for_the_next_attempt() -> None:
    # Given: an exchanged legacy shadow whose first copy verification is interrupted.
    migration = _load_migration()
    client = MagicMock()
    stats = migration._CopyStats(1, 1, 1, None)
    error = migration.OperationalOrderingMigrationStateError(
        "operational_incidents", "interrupted"
    )

    # When: one resume fails and a later retry succeeds.
    with (
        patch.object(migration, "_show_create", return_value="legacy"),
        patch.object(migration, "_schema_contract", return_value=1),
        patch.object(migration, "_copy_raw_rows", return_value=stats),
        patch.object(migration, "_verify_copy", side_effect=error),
        pytest.raises(migration.OperationalOrderingMigrationStateError),
    ):
        migration._resume_exchanged_shadow(
            client, "operational_incidents", "operational_incidents__ordering_v2"
        )
    client.command.assert_not_called()
    with (
        patch.object(migration, "_show_create", return_value="legacy"),
        patch.object(migration, "_schema_contract", return_value=1),
        patch.object(migration, "_copy_raw_rows", return_value=stats),
        patch.object(migration, "_verify_copy"),
    ):
        migration._resume_exchanged_shadow(
            client, "operational_incidents", "operational_incidents__ordering_v2"
        )

    # Then: only the successful retry drops the recoverable legacy shadow.
    client.command.assert_called_once_with(
        "DROP TABLE `operational_incidents__ordering_v2`"
    )
