"""0107 moves the fresh-install ``sync.provider_unit`` route off Celery.

The gap this covers was invisible to every other suite by construction. Tests
seed ``worker_job_routes`` through ``tests/_helpers.seed_sync_dispatch_transport_routes``,
which now seeds ``river_canary`` to match the production dump taken for
CHAOS-4082 -- so no test that uses the helper can observe what a FRESHLY
MIGRATED database actually starts with. This file deliberately does not use
the helper. It derives the fresh-install state by running the real 0061
migration (CHAOS-4041: fixtures come from the producer, never hand-authored),
and then asserts what the dispatcher would do with it.
"""

from __future__ import annotations

import importlib

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from dev_health_ops.workers.job_routes import PROVIDER_UNIT_OUTBOX_ROUTES

_KIND = "sync.provider_unit"


def _create_pre_0061_schema(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            CREATE TABLE worker_operator_audits (
                id INTEGER PRIMARY KEY,
                action TEXT NOT NULL,
                CONSTRAINT ck_worker_operator_audits_action CHECK (
                    action IN (
                        'jobs.cancel', 'jobs.retry', 'queues.pause',
                        'queues.resume', 'workers.drain'
                    )
                )
            )
            """
        )
    )
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


def _run(migration, connection: sa.Connection, direction: str = "upgrade") -> None:
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        getattr(migration, direction)()


def _load(name: str):
    return importlib.import_module(f"dev_health_ops.alembic.versions.{name}")


def _route(connection: sa.Connection) -> tuple[str, int]:
    transport, generation = connection.execute(
        sa.text(
            "SELECT transport, generation FROM worker_job_routes WHERE job_kind = :kind"
        ),
        {"kind": _KIND},
    ).one()
    return str(transport), int(generation)


def test_fresh_install_starts_on_a_route_the_dispatcher_refuses() -> None:
    """The defect, stated as a test before the fix is applied.

    This is the assertion that makes 0107 necessary rather than cosmetic: the
    value a fresh database really starts with is one ``dispatch_sync_run``
    fails closed on, so such an environment dispatches no provider unit at all
    until an operator intervenes.
    """

    seed = _load("0061_seed_sync_provider_canary_route")
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0061_schema(connection)
            _run(seed, connection)

            transport, _generation = _route(connection)
            assert transport == "celery"
            assert transport not in PROVIDER_UNIT_OUTBOX_ROUTES
    finally:
        engine.dispose()


def test_0107_lands_a_fresh_install_where_production_already_is() -> None:
    seed = _load("0061_seed_sync_provider_canary_route")
    promote = _load("0107_promote_sync_provider_unit_route_to_river_canary")
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0061_schema(connection)
            _run(seed, connection)
            _, before = _route(connection)

            _run(promote, connection)

            transport, generation = _route(connection)
            # river_canary, NOT plain river: graduating this kind to full River
            # ownership is an operator decision with its own evidence bar.
            assert transport == "river_canary"
            # The link that keeps migration and producer from drifting: the
            # value this migration lands on must be one the dispatcher accepts.
            assert transport in PROVIDER_UNIT_OUTBOX_ROUTES
            # Mirrors what `workerctl job-routes apply` does, so the operator
            # fence stays monotonic and a stale CAS still loses.
            assert generation == before + 1
    finally:
        engine.dispose()


def test_0107_is_idempotent_and_never_overwrites_an_operator_decision() -> None:
    """Re-running must not bump the fence, and a promoted row must not move.

    The negative control on the predicate. A migration that matched on kind
    alone would pass the test above and still clobber an environment an
    operator had deliberately moved -- including one sitting in a rollback at
    the moment the migration runs.
    """

    seed = _load("0061_seed_sync_provider_canary_route")
    promote = _load("0107_promote_sync_provider_unit_route_to_river_canary")
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0061_schema(connection)
            _run(seed, connection)
            _run(promote, connection)
            settled = _route(connection)

            _run(promote, connection)
            assert _route(connection) == settled

            # An operator who has since promoted to full River ownership.
            connection.execute(
                sa.text(
                    "UPDATE worker_job_routes SET transport = 'river' "
                    "WHERE job_kind = :kind"
                ),
                {"kind": _KIND},
            )
            promoted = _route(connection)

            _run(promote, connection)
            assert _route(connection) == promoted
    finally:
        engine.dispose()


def test_0107_downgrade_reverses_only_what_it_moved() -> None:
    seed = _load("0061_seed_sync_provider_canary_route")
    promote = _load("0107_promote_sync_provider_unit_route_to_river_canary")
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0061_schema(connection)
            _run(seed, connection)
            _run(promote, connection)

            _run(promote, connection, "downgrade")
            assert _route(connection)[0] == "celery"

            # A row an operator moved to full River ownership is NOT dragged
            # back to a transport this downgrade did not put it on.
            _run(promote, connection)
            connection.execute(
                sa.text(
                    "UPDATE worker_job_routes SET transport = 'river' "
                    "WHERE job_kind = :kind"
                ),
                {"kind": _KIND},
            )
            _run(promote, connection, "downgrade")
            assert _route(connection)[0] == "river"
    finally:
        engine.dispose()
