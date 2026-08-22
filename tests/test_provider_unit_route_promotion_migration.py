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

import pytest
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


def _seed_route(
    connection: sa.Connection, transport: str, generation: int, paused: bool
) -> None:
    connection.execute(
        sa.text("DELETE FROM worker_job_routes WHERE job_kind = :kind"),
        {"kind": _KIND},
    )
    connection.execute(
        sa.text(
            "INSERT INTO worker_job_routes "
            "(job_kind, transport, paused, generation, updated_at) "
            "VALUES (:kind, :transport, :paused, :generation, '2026-01-01')"
        ),
        {
            "kind": _KIND,
            "transport": transport,
            "paused": paused,
            "generation": generation,
        },
    )


#: Every row shape this migration must NOT touch, and why. Generation is the
#: provenance test: an operator mutation always bumps it
#: (``jobroute.Controller.ApplyCheckedIn``/``.Rollback``), so anything at a
#: generation above the 0061 seed carries somebody's decision.
_OPERATOR_OWNED = (
    # Production's own shape, set by an operator long before this migration.
    ("river_canary", 2, False),
    # Promoted further, to full River ownership.
    ("river", 3, False),
    # A deliberate rollback. Legal for the row; not this migration's to undo.
    ("celery", 3, False),
    # Paused: the control-plane stop. Promoting would silently override it.
    ("celery", 2, True),
    # Paused at the seed generation -- pause alone is an operator act.
    ("celery", 1, True),
)


@pytest.mark.parametrize(("transport", "generation", "paused"), _OPERATOR_OWNED)
def test_0107_never_overwrites_an_operator_decision(
    transport: str, generation: int, paused: bool
) -> None:
    """The predicate is exactly as narrow as the docstring's claim.

    An earlier cut of this migration matched on ``transport`` alone and claimed
    this property without testing it. It was false in three of these five
    shapes -- which is why every one of them is enumerated here rather than
    represented by a single example.
    """

    promote = _load("0107_promote_sync_provider_unit_route_to_river_canary")
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0061_schema(connection)
            _seed_route(connection, transport, generation, paused)
            before = _route(connection)

            _run(promote, connection)

            assert _route(connection) == before
    finally:
        engine.dispose()


def test_0107_is_idempotent() -> None:
    """Re-running must not bump the operator fence a second time.

    After the first run the row sits at generation 2, which no longer matches
    the seed fingerprint -- so idempotency falls out of the predicate rather
    than being a separate guard.
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
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("transport", "generation", "paused"),
    (("river_canary", 2, False), *_OPERATOR_OWNED),
)
def test_0107_downgrade_is_a_no_op(
    transport: str, generation: int, paused: bool
) -> None:
    """A deliberate exception to the reversible-downgrade rule, pinned here.

    No predicate can tell a row this migration promoted from one an operator
    promoted -- both are ``river_canary`` at the same generation, and
    production is the operator case. The only place a downgrade could reverse
    to is ``celery``, which has no executor. So it does nothing, and this test
    exists so that "does nothing" stays a decision rather than decaying into a
    forgotten stub someone later "fixes".
    """

    promote = _load("0107_promote_sync_provider_unit_route_to_river_canary")
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_pre_0061_schema(connection)
            _seed_route(connection, transport, generation, paused)
            before = _route(connection)

            _run(promote, connection, "downgrade")

            assert _route(connection) == before
    finally:
        engine.dispose()
