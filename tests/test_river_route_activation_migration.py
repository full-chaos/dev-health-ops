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


@pytest.fixture
def _cutover_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER", "1")


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


def test_upgrade_moves_every_checked_in_kind_to_river_and_is_idempotent(
    _cutover_opt_in: None,
) -> None:
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


def test_downgrade_restores_the_declared_celery_rollback_route(
    _cutover_opt_in: None,
) -> None:
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


def test_upgrade_refuses_to_run_when_a_route_row_is_missing(
    _cutover_opt_in: None,
) -> None:
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


def test_upgrade_refuses_an_unexpected_transport_without_writing(
    _cutover_opt_in: None,
) -> None:
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

    # CHAOS-4243: metrics.remaining.extra_metrics/team_metrics were registered
    # handlers with zero producer anywhere, retired (removed from the
    # registry and migration-state entirely) by a later revision (0110). 0066
    # still pins them historically -- that is correct, it recorded what THAT
    # revision did -- but they no longer exist in migration-state.json, so
    # they cannot be checked against it like every other still-live pinned
    # kind below.
    #
    # CHAOS-4438: investment.dispatch/chunk/finalize are the same class --
    # registered Go handlers with zero producer anywhere, now retired
    # (removed from the registry and migration-state entirely). 0066 still
    # pins them historically, correctly; they cannot be checked against the
    # current migration-state.json either.
    retired_since_0066 = {
        "metrics.remaining.extra_metrics",
        "metrics.remaining.team_metrics",
        "investment.dispatch",
        "investment.chunk",
        "investment.finalize",
    }
    assert retired_since_0066 <= pinned
    assert retired_since_0066.isdisjoint(by_kind)

    # CHAOS-5320: the Celery dispatch plane is gone fleet-wide (prod Celery
    # stopped 2026-08-19), so resolve_worker_job_route now rejects a
    # celery-transport row outright and these 18 kinds -- all pinned by 0066
    # at the time, still rollback_route:celery then -- moved to
    # celery_removed/river/none. 0066's own historical pin is unchanged
    # (it correctly recorded what THAT revision did); this test's checked-in
    # comparison must recognize both shapes.
    celery_removed_since_0066 = {
        "investment.materialize",
        "metrics.daily_dispatch",
        "metrics.daily_finalize",
        "metrics.daily_partition",
        "metrics.remaining.capacity",
        "metrics.remaining.complexity",
        "metrics.remaining.dora",
        "metrics.remaining.membership_backfill",
        "metrics.remaining.recommendations",
        "metrics.remaining.release_impact",
        "operational.billing_notification",
        "operational.webhook_delivery",
        "report.execute_on_demand",
        "report.execute_scheduled",
        "sync.team_autoimport",
        "system.heartbeat",
        "system.retention_cleanup",
        "workgraph.build",
    }
    assert celery_removed_since_0066 <= (pinned - retired_since_0066)

    # Each pinned kind must be routed to River by checked-in policy, otherwise
    # this migration would drive a row outside what the producer accepts.
    for kind in pinned - retired_since_0066:
        assert by_kind[kind]["route"] == "river", kind
        if kind in celery_removed_since_0066:
            assert by_kind[kind]["rollback_route"] == "none", kind
        else:
            assert by_kind[kind]["rollback_route"] == "celery", kind

    # The canary and the post-0066 Go-native-only kinds are deliberately
    # excluded. Asserting their identities stops a future kind from being
    # dropped from the historical migration unnoticed.
    assert set(by_kind) - pinned == {
        "sync.provider_unit",
        "system.sync_coverage_refresh",
        "sync.team_repo_ownership_derivation",
        "metrics.remaining.work_item_attribution",
    }
    assert by_kind["sync.provider_unit"]["state"] == "canary"
    assert by_kind["sync.provider_unit"]["route"] == "river_canary"
    assert by_kind["system.sync_coverage_refresh"]["state"] == "celery_removed"
    assert by_kind["system.sync_coverage_refresh"]["route"] == "river"
    assert by_kind["system.sync_coverage_refresh"]["rollback_route"] == "none"
    assert by_kind["sync.team_repo_ownership_derivation"]["state"] == "celery_removed"
    assert by_kind["sync.team_repo_ownership_derivation"]["route"] == "river"
    assert by_kind["sync.team_repo_ownership_derivation"]["rollback_route"] == "none"
    # CHAOS-3092 PR-B: another Go-native-only kind, post-0066, same
    # celery_removed/river/none shape as the two above.
    assert (
        by_kind["metrics.remaining.work_item_attribution"]["state"] == "celery_removed"
    )
    assert by_kind["metrics.remaining.work_item_attribution"]["route"] == "river"
    assert (
        by_kind["metrics.remaining.work_item_attribution"]["rollback_route"] == "none"
    )
