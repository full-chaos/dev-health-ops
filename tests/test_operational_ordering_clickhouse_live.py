from __future__ import annotations

import asyncio
import importlib.util
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch
from urllib.parse import urlparse
from uuid import uuid4

import clickhouse_connect
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError

from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.models.operational import OperationalIncident, operational_columns
from dev_health_ops.models.operational_ordering_types import ORDERING_FIELD_NAMES
from dev_health_ops.storage.clickhouse import ClickHouseStore
from dev_health_ops.storage.operational_current import current_operational_rows_sql

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI pointed at an isolated scratch database",
    ),
]
_AT = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
_MIGRATION = (
    Path(__file__).parents[1]
    / "src/dev_health_ops/migrations/clickhouse/067_operational_ordering_contract.py"
)


def _migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "operational_ordering_clickhouse_live_migration", _MIGRATION
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@contextmanager
def _legacy_sink() -> Iterator[tuple[ClickHouseMetricsSink, str]]:
    clickhouse_uri = CLICKHOUSE_URI
    assert clickhouse_uri is not None
    parsed = urlparse(clickhouse_uri)
    database = f"ordering_migration_{uuid4().hex}"
    isolated_uri = parsed._replace(path=f"/{database}").geturl()
    admin = clickhouse_connect.get_client(dsn=clickhouse_uri)
    admin.command(f"CREATE DATABASE `{database}`")
    result = ClickHouseMetricsSink(isolated_uri)
    try:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OPERATIONAL_ORDERING_CONTRACT", None)
            result.ensure_schema(force=True)
        yield result, isolated_uri
    finally:
        result.close()
        admin.command(f"DROP DATABASE IF EXISTS `{database}`")
        admin.close()


class _InterruptingClient:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.interrupted = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def command(self, command: str, *args: object, **kwargs: object) -> Any:
        if command.startswith("DROP TABLE `operational_incidents__ordering_v2`"):
            self.interrupted = True
            raise RuntimeError("injected interruption after exchange")
        return self.client.command(command, *args, **kwargs)


@pytest.fixture(scope="module")
def sink():
    clickhouse_uri = CLICKHOUSE_URI
    assert clickhouse_uri is not None
    database = (urlparse(clickhouse_uri).path or "").lstrip("/")
    if database in ("", "default"):
        pytest.skip("refusing to run ClickHouse schema setup against default")
    with patch.dict(os.environ, {"OPERATIONAL_ORDERING_CONTRACT": "2"}):
        result = ClickHouseMetricsSink(clickhouse_uri)
        result.ensure_schema(force=True)
        yield result
        result.close()


def _incident(
    org_id: str, title: str, source_time: datetime = _AT
) -> OperationalIncident:
    return OperationalIncident(
        org_id=org_id,
        provider="pagerduty",
        provider_instance_id="pd-ordering-test",
        source_entity_type="incident",
        external_id="incident-1",
        source_version_at=source_time,
        observed_at=source_time,
        last_synced=source_time,
        title=title,
        started_at=source_time,
    )


def _matrix(
    entities: tuple[OperationalIncident, ...], forced_revision: int | None = None
) -> list[list[object]]:
    columns = operational_columns(OperationalIncident)
    rows: list[list[object]] = []
    for entity in entities:
        values = asdict(entity)
        if forced_revision is not None:
            values["source_revision"] = forced_revision
        rows.append([values[column] for column in columns])
    return rows


def _winner(client, entity: OperationalIncident) -> tuple[str, str]:
    result = client.query(
        "SELECT title, source_conflict_key FROM "
        + current_operational_rows_sql("operational_incidents", ("id = {id:String}",)),
        parameters={"org_id": entity.org_id, "id": entity.id},
    )
    return result.result_rows[0]


def _delete_org(client, org_id: str) -> None:
    client.command(
        "ALTER TABLE operational_incidents DELETE "
        "WHERE org_id = {org_id:String} SETTINGS mutations_sync=2",
        parameters={"org_id": org_id},
    )


def _legacy_matrix(
    entities: tuple[OperationalIncident, ...],
) -> tuple[tuple[str, ...], list[list[object]]]:
    columns = tuple(
        name
        for name in operational_columns(OperationalIncident)
        if name not in ORDERING_FIELD_NAMES
    )
    rows = []
    for entity in entities:
        values = asdict(entity)
        rows.append([values[column] for column in columns])
    return columns, rows


def test_populated_legacy_migration_preserves_candidates_and_rejects_omitted_writer() -> (
    None
):
    # Given: omitted configuration and a real migration-066 table with equal-time facts.
    migration = _migration()
    with _legacy_sink() as (legacy, isolated_uri):
        org_id = f"test-populated-migration-{uuid4()}"
        first = _incident(org_id, "alpha")
        second = _incident(org_id, "omega")
        legacy_columns, legacy_rows = _legacy_matrix((first, second))
        legacy.client.command("SYSTEM STOP MERGES operational_incidents")
        for row in legacy_rows:
            legacy.client.insert(
                "operational_incidents", [row], column_names=legacy_columns
            )
        assert migration._count(legacy.client, "count()", "operational_incidents") == 2
        assert (
            migration._schema_contract(
                migration._show_create(legacy.client, "operational_incidents"),
                "operational_incidents",
            )
            == 1
        )

        # When: maintenance explicitly selects contract 2 and migrates populated tables.
        with patch.dict(os.environ, {"OPERATIONAL_ORDERING_CONTRACT": "2"}):
            migration.upgrade(legacy.client)
            winner = _winner(legacy.client, first)

        # Then: both candidates survive, the deterministic winner is visible, and an
        # omitted-contract writer cannot satisfy the v2 table constraint.
        expected = max((first, second), key=lambda item: item.source_conflict_key)
        assert winner == (expected.title, expected.source_conflict_key)
        assert migration._candidate_count(legacy.client, "operational_incidents") == 2
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OPERATIONAL_ORDERING_CONTRACT", None)
            old_writer = ClickHouseStore(isolated_uri)
            old_writer.client = legacy.client
            with pytest.raises(DatabaseError):
                asyncio.run(old_writer.insert_operational_incidents([first]))


def test_populated_migration_resumes_real_interruption_after_exchange() -> None:
    # Given: a populated legacy table and an interruption at post-exchange cleanup.
    migration = _migration()
    with _legacy_sink() as (legacy, _isolated_uri):
        incident = _incident(f"test-resume-{uuid4()}", "resume")
        legacy_columns, legacy_rows = _legacy_matrix((incident,))
        legacy.client.insert(
            "operational_incidents", legacy_rows, column_names=legacy_columns
        )
        interrupted = _InterruptingClient(legacy.client)

        # When: the first rebuild stops after exchange and the same migration reruns.
        with (
            patch.dict(os.environ, {"OPERATIONAL_ORDERING_CONTRACT": "2"}),
            pytest.raises(RuntimeError, match="injected interruption"),
        ):
            migration._rebuild_table(interrupted, "operational_incidents")
        assert interrupted.interrupted
        assert migration._table_exists(
            legacy.client, "operational_incidents__ordering_v2"
        )
        migration._rebuild_table(legacy.client, "operational_incidents")

        # Then: the retry converges the populated table and removes the old shadow.
        assert not migration._table_exists(
            legacy.client, "operational_incidents__ordering_v2"
        )
        assert migration._candidate_count(legacy.client, "operational_incidents") == 1


def test_equal_revision_distinct_keys_choose_one_winner_for_every_arrival_mode(
    sink,
) -> None:
    # Given: three isolated identity pairs with forced equal UInt128 revisions.
    orgs = [f"test-ordering-{uuid4()}" for _ in range(3)]
    pairs = [(_incident(org, "alpha"), _incident(org, "omega")) for org in orgs]
    forced_revision = int(pairs[0][0].source_revision)
    columns = operational_columns(OperationalIncident)
    extra_clients = []

    try:
        # When: candidates arrive forward, reverse, and concurrently from two clients.
        sink.client.insert(
            "operational_incidents",
            _matrix(pairs[0], forced_revision),
            column_names=columns,
        )
        sink.client.insert(
            "operational_incidents",
            _matrix(tuple(reversed(pairs[1])), forced_revision),
            column_names=columns,
        )
        clickhouse_uri = CLICKHOUSE_URI
        assert clickhouse_uri is not None
        extra_clients = [
            clickhouse_connect.get_client(dsn=clickhouse_uri) for _ in range(2)
        ]
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    client.insert,
                    "operational_incidents",
                    _matrix((entity,), forced_revision),
                    column_names=columns,
                )
                for client, entity in zip(extra_clients, pairs[2], strict=True)
            ]
            for future in futures:
                future.result()
        before = [_winner(sink.client, pair[0]) for pair in pairs]
        sink.client.command("OPTIMIZE TABLE operational_incidents FINAL")
        after = [_winner(sink.client, pair[0]) for pair in pairs]

        # Then: the lexicographically greater full key wins and both candidates survive.
        expected = [
            max(pair, key=lambda item: item.source_conflict_key) for pair in pairs
        ]
        expected_rows = [(item.title, item.source_conflict_key) for item in expected]
        assert before == expected_rows
        assert after == expected_rows
        for pair in pairs:
            count = sink.client.query(
                "SELECT count() FROM operational_incidents "
                "WHERE org_id = {org_id:String} AND id = {id:String}",
                parameters={"org_id": pair[0].org_id, "id": pair[0].id},
            ).result_rows[0][0]
            assert count == 2
    finally:
        for client in extra_clients:
            client.close()
        for org_id in orgs:
            _delete_org(sink.client, org_id)


def test_tombstone_is_filtered_after_selection_and_later_active_recovers(sink) -> None:
    # Given: an active row, a newer tombstone, and a still-later recovery.
    org_id = f"test-recovery-{uuid4()}"
    active = _incident(org_id, "active")
    tombstone = replace(
        active,
        source_version_at=_AT + timedelta(seconds=1),
        is_deleted=True,
        deleted_at=_AT + timedelta(seconds=1),
    )
    recovery = replace(
        tombstone,
        source_version_at=_AT + timedelta(seconds=2),
        is_deleted=False,
        deleted_at=None,
        title="recovered",
    )
    columns = operational_columns(OperationalIncident)
    visible_query = "SELECT title FROM " + current_operational_rows_sql(
        "operational_incidents", ("id = {id:String}", "is_deleted = 0")
    )

    try:
        # When: the tombstone lands first, followed by a later active recovery.
        sink.client.insert(
            "operational_incidents", _matrix((active, tombstone)), column_names=columns
        )
        hidden = sink.client.query(
            visible_query, parameters={"org_id": org_id, "id": active.id}
        ).result_rows
        sink.client.insert(
            "operational_incidents", _matrix((recovery,)), column_names=columns
        )
        sink.client.command("OPTIMIZE TABLE operational_incidents FINAL")
        recovered = sink.client.query(
            visible_query, parameters={"org_id": org_id, "id": active.id}
        ).result_rows

        # Then: the earlier active row never resurfaces, while the later recovery does.
        assert hidden == []
        assert recovered == [("recovered",)]
    finally:
        _delete_org(sink.client, org_id)


def test_identical_replay_compacts_by_latest_ingest_revision(sink) -> None:
    # Given: two source-identical receives with increasing ingest revisions.
    org_id = f"test-replay-{uuid4()}"
    original = _incident(org_id, "replay")
    replay = replace(
        original,
        observed_at=_AT + timedelta(seconds=1),
        last_synced=_AT + timedelta(seconds=2),
    )
    columns = operational_columns(OperationalIncident)

    try:
        # When: both receives are inserted and the table is fully merged.
        sink.client.insert(
            "operational_incidents", _matrix((replay, original)), column_names=columns
        )
        sink.client.command("OPTIMIZE TABLE operational_incidents FINAL")
        result = sink.client.query(
            "SELECT count(), max(ingest_revision) FROM operational_incidents "
            "WHERE org_id = {org_id:String} AND id = {id:String} "
            "AND source_revision = {source_revision:UInt128} "
            "AND source_conflict_key = {source_conflict_key:String}",
            parameters={
                "org_id": org_id,
                "id": original.id,
                "source_revision": int(original.source_revision),
                "source_conflict_key": original.source_conflict_key,
            },
        ).result_rows

        # Then: one replay candidate remains and it carries the newest ingestion version.
        assert result == [(1, int(replay.ingest_revision))]
    finally:
        _delete_org(sink.client, org_id)
