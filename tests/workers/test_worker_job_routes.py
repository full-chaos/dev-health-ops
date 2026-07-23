from __future__ import annotations

from typing import cast
from unittest.mock import patch

import pytest
from sqlalchemy import Table, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from dev_health_ops.models import WorkerJobRoute
from dev_health_ops.workers.job_contracts import MigrationJob
from dev_health_ops.workers.job_routes import (
    WorkerJobRouteError,
    _locked_route_statement,
    resolve_worker_job_route,
    route_requires_celery,
    route_requires_outbox,
)

KIND = "operational.billing_notification"


@pytest.fixture
def engine():
    engine = create_engine("sqlite:///:memory:")
    cast(Table, WorkerJobRoute.__table__).create(engine)
    return engine


def _policy(route: str = "celery") -> tuple[MigrationJob, ...]:
    return (
        MigrationJob(
            kind=KIND,
            producer_version=1,
            required_profiles=("ops",),
            route=route,
        ),
    )


def test_default_route_is_celery_and_does_not_stage_river(engine) -> None:
    with Session(engine) as session, session.begin():
        session.add(
            WorkerJobRoute(
                job_kind=KIND, transport="celery", paused=False, generation=1
            )
        )
    with (
        Session(engine) as session,
        patch(
            "dev_health_ops.workers.job_routes.load_migration_jobs",
            return_value=_policy(),
        ),
    ):
        route = resolve_worker_job_route(session, KIND)
    assert route_requires_celery(route)
    assert not route_requires_outbox(route)


@pytest.mark.parametrize("transport", ("shadow", "river_canary", "river"))
def test_executable_route_selects_outbox_without_implicit_celery(
    engine, transport: str
) -> None:
    with Session(engine) as session, session.begin():
        session.add(
            WorkerJobRoute(
                job_kind=KIND, transport=transport, paused=False, generation=2
            )
        )
    with (
        Session(engine) as session,
        patch(
            "dev_health_ops.workers.job_routes.load_migration_jobs",
            return_value=_policy(transport),
        ),
    ):
        route = resolve_worker_job_route(session, KIND)
    assert route_requires_outbox(route)
    assert route_requires_celery(route) is (transport == "shadow")


def test_missing_paused_and_drifted_routes_fail_closed(engine) -> None:
    with (
        Session(engine) as session,
        patch(
            "dev_health_ops.workers.job_routes.load_migration_jobs",
            return_value=_policy(),
        ),
    ):
        with pytest.raises(WorkerJobRouteError):
            resolve_worker_job_route(session, KIND)

    with Session(engine) as session, session.begin():
        session.add(
            WorkerJobRoute(job_kind=KIND, transport="river", paused=True, generation=1)
        )
    with (
        Session(engine) as session,
        patch(
            "dev_health_ops.workers.job_routes.load_migration_jobs",
            return_value=_policy("river"),
        ),
    ):
        with pytest.raises(WorkerJobRouteError):
            resolve_worker_job_route(session, KIND)


def test_producer_route_read_holds_shared_lock_until_outbox_commit() -> None:
    statement = _locked_route_statement(KIND)
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert sql.endswith("FOR SHARE")
