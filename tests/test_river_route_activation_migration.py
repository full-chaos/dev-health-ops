from __future__ import annotations

import importlib
from types import ModuleType

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

_MODULE = "dev_health_ops.alembic.versions.0066_activate_river_worker_job_routes"


def _migration() -> ModuleType:
    return importlib.import_module(_MODULE)


def _create_schema(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            CREATE TABLE worker_job_routes (
                job_kind TEXT PRIMARY KEY,
                transport TEXT NOT NULL,
                paused BOOLEAN NOT NULL,
                generation BIGINT NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        )
    )


def _seed(connection: sa.Connection, kinds, transport="celery", paused=0, generation=1):
    for kind in kinds:
        connection.execute(
            sa.text(
                """
                INSERT INTO worker_job_routes
                    (job_kind, transport, paused, generation, updated_at)
                VALUES (:kind, :transport, :paused, :generation, '2026-07-25 00:00:00')
                """
            ),
            {
                "kind": kind,
                "transport": transport,
                "paused": paused,
                "generation": generation,
            },
        )


def _run(migration: ModuleType, connection: sa.Connection, direction: str) -> None:
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        getattr(migration, direction)()


def _routes(connection: sa.Connection) -> dict[str, tuple[str, int, int]]:
    return {
        row[0]: (row[1], row[2], row[3])
        for row in connection.execute(
            sa.text(
                "SELECT job_kind, transport, paused, generation FROM worker_job_routes"
            )
        )
    }


def test_upgrade_moves_every_checked_in_kind_to_river_and_is_idempotent() -> None:
    migration = _migration()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_schema(connection)
            # sync.provider_unit is the one live canary (seeded by 0061/0064)
            # and is excluded from this migration; every migrated kind sits on
            # the Celery rollback route beforehand.
            canary = "sync.provider_unit"
            assert canary not in migration._KINDS
            _seed(connection, migration._KINDS)
            _seed(connection, [canary], transport="river_canary", generation=2)

            _run(migration, connection, "upgrade")
            routes = _routes(connection)

            assert len(routes) == 24
            migrated = {k: v for k, v in routes.items() if k != canary}
            assert len(migrated) == 23
            assert {transport for transport, _, _ in migrated.values()} == {"river"}
            assert all(paused == 0 for _, paused, _ in migrated.values())
            # Every migrated row advanced its generation exactly once.
            assert routes["workgraph.build"][2] == 2
            # The canary must be left completely untouched -- not promoted, and
            # not generation-bumped, since no route decision was made about it.
            assert routes[canary] == ("river_canary", 0, 2)

            # A second run must not bump the generation again: an operator reads
            # generation as the count of real route decisions.
            _run(migration, connection, "upgrade")
            assert _routes(connection) == routes
    finally:
        engine.dispose()


def test_downgrade_restores_the_declared_celery_rollback_route() -> None:
    migration = _migration()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_schema(connection)
            _seed(connection, migration._KINDS)

            _run(migration, connection, "upgrade")
            _run(migration, connection, "downgrade")

            routes = _routes(connection)
            assert {transport for transport, _, _ in routes.values()} == {"celery"}
            # Forward and back are two decisions, not zero.
            assert routes["system.heartbeat"][2] == 3
    finally:
        engine.dispose()


def test_upgrade_refuses_to_run_when_a_route_row_is_missing() -> None:
    migration = _migration()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_schema(connection)
            _seed(connection, [k for k in migration._KINDS if k != "system.heartbeat"])

            with pytest.raises(RuntimeError, match="not seeded"):
                _run(migration, connection, "upgrade")

            # Nothing may be written when validation fails: a half-migrated
            # table means two execution owners across different kinds.
            assert {t for t, _, _ in _routes(connection).values()} == {"celery"}
    finally:
        engine.dispose()


def test_upgrade_refuses_an_unexpected_transport_without_writing() -> None:
    migration = _migration()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_schema(connection)
            _seed(connection, [k for k in migration._KINDS if k != "workgraph.build"])
            _seed(connection, ["workgraph.build"], transport="shadow")

            with pytest.raises(RuntimeError, match="not safe to retarget"):
                _run(migration, connection, "upgrade")

            routes = _routes(connection)
            assert routes["workgraph.build"][0] == "shadow"
            assert routes["system.heartbeat"][0] == "celery"
    finally:
        engine.dispose()


def test_pinned_kinds_match_the_checked_in_migration_state() -> None:
    """The migration pins its kind list; it must match the contract tree today.

    The list is deliberately not derived at runtime so a later contract edit
    cannot change what this revision did. This test is what catches the pin
    going stale while it is still the newest revision.
    """

    import json
    from pathlib import Path

    state = json.loads(
        (
            Path(__file__).parents[1] / "contracts/jobs/v1/migration-state.json"
        ).read_text(encoding="utf-8")
    )
    by_kind = {job["kind"]: job for job in state["jobs"]}
    pinned = set(_migration()._KINDS)

    # Each pinned kind must be routed to River by checked-in policy, otherwise
    # this migration would drive a row outside what the producer accepts.
    for kind in pinned:
        assert by_kind[kind]["route"] == "river", kind
        assert by_kind[kind]["rollback_route"] == "celery", kind

    # Exactly one checked-in kind is deliberately excluded. Asserting the
    # identity of the exclusion, not merely its count, is what stops a future
    # kind from being dropped from the migration unnoticed.
    assert set(by_kind) - pinned == {"sync.provider_unit"}
    assert by_kind["sync.provider_unit"]["state"] == "canary"
    assert by_kind["sync.provider_unit"]["route"] == "river_canary"
