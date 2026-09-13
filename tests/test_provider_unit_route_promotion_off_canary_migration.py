"""0131 promotes ``sync.provider_unit`` off both legacy transports.

Companion to ``test_provider_unit_route_promotion_migration.py`` (0107),
which landed this kind on ``river_canary`` in the first place -- but only
for the untouched pre-cutover seed (transport ``celery``, generation 1,
unpaused). That migration's own docstring says graduating past canary was
an operator decision with its own evidence bar; 0131 is the migration that
clears it, once ``contracts/jobs/v1/migration-state.json`` pins the
checked-in policy route to plain ``river`` (rollback_route ``none``).

0131 promotes a row from EITHER legacy transport straight to ``river``:
``river_canary`` (0107's own successful promotion), or ``celery`` (a row
0107's narrow generation==1 guard never touched -- e.g. an operator paused
it before 0107 ran). Unlike 0107, it is unconditional on generation for
both, matching the "no longer a legitimate end state for anybody" reasoning
0125 used for its own celery->river promotion of twelve other kinds,
applied here to the one kind 0125 explicitly left alone.
"""

from __future__ import annotations

import importlib

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from tests._alembic_heads import application_schema_head

_KIND = "sync.provider_unit"
_MODULE = "0131_promote_sync_provider_unit_route_off_canary_and_celery"


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


def _run(migration, connection: sa.Connection, direction: str = "upgrade") -> None:
    context = MigrationContext.configure(connection)
    with Operations.context(context):
        getattr(migration, direction)()


def _load():
    return importlib.import_module(f"dev_health_ops.alembic.versions.{_MODULE}")


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


def _route(connection: sa.Connection) -> tuple[str, int, bool]:
    transport, generation, paused = connection.execute(
        sa.text(
            "SELECT transport, generation, paused FROM worker_job_routes "
            "WHERE job_kind = :kind"
        ),
        {"kind": _KIND},
    ).one()
    return str(transport), int(generation), bool(paused)


def test_0131_is_the_application_schema_head_and_chains_after_0130() -> None:
    """Derived, not typed. See ``tests/_alembic_heads.py``'s own docstring:
    the next migration author renumbers THIS check (or supersedes it, the
    same way this one supersedes 0130's) rather than leaving a stale pin.
    """
    migration = _load()
    assert migration.revision == application_schema_head(), (
        "0131 must be the application_schema head; if another migration "
        "landed first, renumber this one and re-run"
    )
    assert migration.down_revision == "0130"


@pytest.mark.parametrize("legacy_transport", ("river_canary", "celery"))
@pytest.mark.parametrize("generation", (1, 2, 3, 47))
def test_promotes_either_legacy_transport_to_river_at_any_generation(
    legacy_transport: str, generation: int
) -> None:
    """Unconditional on generation: production's river_canary sits at
    generation 2, but nothing about either legacy meaning for this kind was
    ever generation-dependent, so a row at any generation carries the same,
    now-obsolete, meaning.
    """

    promote = _load()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_schema(connection)
            _seed_route(connection, legacy_transport, generation, paused=False)

            _run(promote, connection)

            transport, new_generation, paused = _route(connection)
            assert transport == "river"
            assert new_generation == generation + 1
            assert paused is False
    finally:
        engine.dispose()


@pytest.mark.parametrize("legacy_transport", ("river_canary", "celery"))
def test_preserves_a_paused_row(legacy_transport: str) -> None:
    """Promoting transport must not silently unpause a paused kind."""

    promote = _load()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_schema(connection)
            _seed_route(connection, legacy_transport, 2, paused=True)

            _run(promote, connection)

            transport, generation, paused = _route(connection)
            assert transport == "river"
            assert generation == 3
            assert paused is True
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("transport", "generation", "paused"),
    (
        # Already graduated -- nothing to do, and re-bumping generation here
        # would be an unearned operator-fence advance.
        ("river", 3, False),
        # Some other transport value entirely; the predicate is exact-match
        # on the two legacy transports, not "anything that isn't river".
        ("shadow", 2, False),
    ),
)
def test_never_touches_a_row_already_on_river_or_an_unrelated_transport(
    transport: str, generation: int, paused: bool
) -> None:
    promote = _load()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_schema(connection)
            _seed_route(connection, transport, generation, paused)
            before = _route(connection)

            _run(promote, connection)

            assert _route(connection) == before
    finally:
        engine.dispose()


@pytest.mark.parametrize("legacy_transport", ("river_canary", "celery"))
def test_is_idempotent(legacy_transport: str) -> None:
    """A second run must not bump the generation again: after the first run
    the row reads ``river``, which no longer matches either predicate.
    """

    promote = _load()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_schema(connection)
            _seed_route(connection, legacy_transport, 2, paused=False)

            _run(promote, connection)
            settled = _route(connection)

            _run(promote, connection)
            assert _route(connection) == settled
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("transport", "generation", "paused"),
    (
        ("river_canary", 2, False),
        ("celery", 4, False),
        ("river", 3, False),
        ("river_canary", 2, True),
        ("celery", 1, True),
    ),
)
def test_downgrade_is_a_no_op(transport: str, generation: int, paused: bool) -> None:
    """A deliberate exception to the reversible-downgrade rule, pinned here.

    No predicate can tell a row this migration promoted from one that already
    read ``river`` for an unrelated reason, and the only places a downgrade
    could reverse to (``river_canary`` or ``celery``) are transports the
    checked-in policy no longer accepts. So it does nothing, and this test
    exists so that "does nothing" stays a decision rather than decaying into
    a forgotten stub someone later "fixes".
    """

    promote = _load()
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            _create_schema(connection)
            _seed_route(connection, transport, generation, paused)
            before = _route(connection)

            _run(promote, connection, "downgrade")

            assert _route(connection) == before
    finally:
        engine.dispose()
