"""Shared test helpers.

Centralizes patterns that mypy flags repeatedly across the suite, so individual
tests stay focused on their assertions instead of working around library
quirks.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from typing import Any, cast

from sqlalchemy import Table
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import Session

from dev_health_ops.db import normalize_sync_postgres_uri


def sync_postgres_test_url() -> URL:
    """Return ``DEV_HEALTH_POSTGRES_TEST_URI`` coerced to the blocking driver.

    ``DEV_HEALTH_POSTGRES_TEST_URI`` names the ASYNC driver (``+asyncpg``),
    which a blocking ``create_engine``/``Session`` cannot drive -- it raises
    ``MissingGreenlet`` the moment it touches IO. Every Postgres-gated test
    that builds a synchronous engine must coerce the driver first, the same
    way ``test_ask_dev_v2_persistence_startup_gate.py`` and
    ``test_canonical_incident_feature_flag_postgres_migration.py`` already do.

    This is centralized because the mismatch was invisible for a long time:
    the variable is set only in CI steps that did not collect these files, so
    the tests always skipped and nobody noticed that each one had open-coded
    ``create_engine(os.environ["DEV_HEALTH_POSTGRES_TEST_URI"])``. Reviving the
    coverage job (CHAOS-3450) ran them for the first time and all of them
    failed identically. One helper means the next such test cannot re-introduce
    the bug by copying a neighbour.

    Returns the URL OBJECT, never ``str(url)``: SQLAlchemy's ``__str__`` masks
    the password as ``***``, so stringifying here swaps a ``MissingGreenlet``
    for a "password authentication failed" that reads like broken CI
    credentials rather than a broken test helper.

    THE DRIVER IS NOT THE ONLY THING THAT DIFFERS between the async URI and a
    blocking one, which is why this delegates to the PRODUCTION normalizer
    rather than swapping ``drivername`` itself. ``asyncpg`` accepts
    ``?ssl=`` and ``?channel_binding=``; psycopg2 accepts neither, and raises
    ``invalid connection option "ssl"`` on a managed/TLS URI of the form
    ``postgresql+asyncpg://...?ssl=require&channel_binding=require``.
    ``normalize_sync_postgres_uri`` already owns that translation for
    production (``ssl`` -> ``sslmode``, ``channel_binding`` dropped), so
    reimplementing the driver half here left two normalizers that agreed on
    CI's bare URI and disagreed on every TLS one -- a divergence found by
    review after this helper replaced call sites that had been using the
    production normalizer directly. One normalizer, not two.
    """
    return make_url(
        normalize_sync_postgres_uri(os.environ["DEV_HEALTH_POSTGRES_TEST_URI"])
    ).set(drivername="postgresql+psycopg2")


def tables_of(*models: Any) -> list[Table]:
    """Return ``[Model.__table__, ...]`` typed as ``list[Table]``.

    SQLAlchemy 2's ``DeclarativeBase`` declares ``__table__`` as
    ``FromClause`` (broader, to accommodate inheritance), which makes mypy
    reject the value when callers pass it to APIs typed as
    ``Sequence[Table]`` (e.g. :meth:`MetaData.create_all`).

    Concrete mapped classes always expose a real :class:`Table` at runtime,
    so this helper performs the narrowing once instead of forcing every
    test fixture to repeat ``cast(Table, Model.__table__)``.

    The ``Any`` parameter type is intentional: SQLAlchemy mapped classes use
    ``DeclarativeAttributeIntercept`` as their metaclass, which doesn't
    surface ``__table__`` to a plain ``type[...]`` view.
    """
    return [cast(Table, model.__table__) for model in models]


def closing_coroutine_runner(
    return_value: Any = None, *, raises: BaseException | None = None
) -> Callable[..., Any]:
    """Build a ``side_effect`` for a mocked coroutine runner that closes the coroutine.

    Mocking a coroutine runner (``asyncio.run``, ``workers.async_runner.run_async``,
    etc.) leaves the coroutine argument un-awaited, which emits a
    ``RuntimeWarning: coroutine '...' was never awaited`` at garbage-collection
    time. This side_effect closes the coroutine so it is consumed cleanly, while
    the mock still records the call and honours the requested return/raise
    behaviour (CHAOS-2586).
    """

    def _run(coro: Any = None, *args: Any, **kwargs: Any) -> Any:
        if asyncio.iscoroutine(coro):
            coro.close()
        if raises is not None:
            raise raises
        return return_value

    return _run


def seed_sync_dispatch_transport_routes(session: Session) -> None:
    """Seed the durable transport routes for isolated outbox tests.

    The ``sync_dispatch_transport_routes`` rows keep the migration-default
    ``celery`` value that alembic 0049 seeds, because the Python outbox claim
    path still selects on it.

    ``worker_job_routes['sync.provider_unit']`` deliberately does NOT. Alembic
    0061 seeds it ``celery`` on a fresh database and 0066 excludes it, but a
    production dump taken for CHAOS-4082 shows the deployed row is
    ``river_canary`` -- an operator applied it, which is the state every
    provider unit has actually dispatched under for months. Seeding ``celery``
    here made the fixture disagree with production in the one field the
    dispatcher fails closed on, so every dispatch test had to override it by
    hand and a test that forgot got a fail-closed refusal that looked like a
    routing bug.

    A test that wants a route FAULT sets the row itself; see
    ``test_dispatch_sync_run_route_faults_fail_closed``.
    """
    from dev_health_ops.models import SyncDispatchTransportRoute, WorkerJobRoute

    session.add_all(
        [
            SyncDispatchTransportRoute(
                kind=kind,
                transport="celery",
                generation=1,
                paused=False,
                paused_at=None,
                rollback_transport="celery",
            )
            for kind in (
                "dispatch_sync_run",
                "finalize_sync_run",
                "post_sync",
                "reference_discovery",
            )
        ]
    )
    session.add(
        WorkerJobRoute(
            job_kind="sync.provider_unit",
            transport="river_canary",
            paused=False,
            generation=1,
        )
    )
    session.flush()


def pin_provider_unit_routability(monkeypatch: Any) -> None:
    """Treat every ``(provider, dataset)`` as matrix-routable for one test.

    CHAOS-4054 step 4 left the provider-unit dispatcher with exactly one
    question: does the checked-in capability matrix route this pair? A pair it
    does not route is terminalized as ``feature_disabled`` rather than
    published, because there is no second runtime left to publish to.

    The suites that call this helper are about something else entirely -- the
    DispatchGuard concurrency cap, budget-surplus admission, the CHAOS-2581
    invariants, the orchestration baseline -- and their dataset keys are
    deliberately SYNTHETIC bucket labels (``commits-active``, ``commits-0``,
    ``commits-stale``), chosen so they never collide with a real matrix
    identity and never pick up a real budget estimate. They reached a
    dispatcher before step 4 only because the Celery fallthrough accepted any
    pair. With that gone, an unpinned synthetic key terminalizes and the
    guard's decision -- the actual subject -- is never observed.

    Pinning routability changes exactly one thing. The alternative, renaming
    the synthetic keys to real matrix datasets, would ALSO change each unit's
    budget estimate and therefore which units fit the cap, silently altering
    what these tests prove.

    Routability itself is covered directly by
    ``tests/workers/test_provider_unit_route.py`` and end to end by
    ``tests/test_provider_unit_planner_dispatcher_parity.py``; this helper
    must never be used to paper over a routability assertion.
    """

    from dev_health_ops.workers import sync_units

    monkeypatch.setattr(sync_units, "routes_to_river", lambda *_args, **_kwargs: True)


def provider_unit_outbox_keys(session: Session) -> set[str]:
    """Every provider-unit dedupe key staged in the durable outbox.

    The post-step-4 replacement for spying on ``run_sync_unit.s(...)``: the
    outbox row IS the dispatch now, so "which units were dispatched" is a
    database question rather than a mock-call question.
    """

    from dev_health_ops.models import WorkerJobOutbox

    return {
        row.dedupe_key
        for row in session.query(WorkerJobOutbox).all()
        if row.job_kind == "sync.provider_unit"
    }
