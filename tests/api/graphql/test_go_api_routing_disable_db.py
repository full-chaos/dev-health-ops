"""The round trip: enable -> disable -> unreachable, against real Postgres.

THE test of this PR. `enable` shipped without an off-ramp, and the gap was
only visible when an operator needed to turn four diverging operations off
and the tool could not. This asserts the full cycle works, so the capability
cannot be lost again without a red test.

Additional coverage, never sole coverage: everything asserted here that can
be asserted without a database also lives in
`test_go_api_routing_disable.py`, which runs in CI. This file skips without
`DEV_HEALTH_POSTGRES_TEST_URI`.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import cast

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.graphql.go_api_routing_admin import (
    apply_disable,
    enable_operation,
    plan_disable,
    routing_status_rows,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.go_api_registry import (
    CandidateBuild,
    ProofRun,
    RoutingState,
)

POSTGRES_TEST_URI = os.environ.get("DEV_HEALTH_POSTGRES_TEST_URI")
pytestmark = pytest.mark.skipif(
    not POSTGRES_TEST_URI,
    reason="Requires DEV_HEALTH_POSTGRES_TEST_URI (admin creds able to CREATE DATABASE)",
)

LIVE = "sha256:live-digest"
BUILD = "b18e56fa79cfe20ce0f75df148144b832d92be36"
CATALOG = (("featureFlags", "doc-ff"), ("hotspots", "doc-hs"), ("flowMatrix", "doc-fm"))


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    assert POSTGRES_TEST_URI is not None
    db = f"lane_disable_{uuid.uuid4().hex}"

    def admin() -> sa.Engine:
        return sa.create_engine(
            make_url(POSTGRES_TEST_URI).set(drivername="postgresql+psycopg2"),
            isolation_level="AUTOCOMMIT",
        )

    eng = admin()
    try:
        with eng.connect() as c:
            c.exec_driver_sql(f'CREATE DATABASE "{db}"')
    finally:
        eng.dispose()

    url = make_url(POSTGRES_TEST_URI).set(database=db)
    sync = sa.create_engine(url.set(drivername="postgresql+psycopg2"))
    try:
        Base.metadata.create_all(
            sync,
            tables=cast(
                list[sa.Table],
                [CandidateBuild.__table__, RoutingState.__table__, ProofRun.__table__],
            ),
        )
    finally:
        sync.dispose()

    engine = create_async_engine(
        url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as s:
            yield s
    finally:
        await engine.dispose()
        eng = admin()
        try:
            with eng.connect() as c:
                c.exec_driver_sql(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
        finally:
            eng.dispose()


async def _enable_all(session: AsyncSession) -> None:
    for op, doc in CATALOG:
        await enable_operation(
            session,
            schema_digest=LIVE,
            document_digest=doc,
            selected_operation=op,
            candidate_build=BUILD,
            mode="canary",
            review_evidence="enabled for the round-trip test",
            recorded_by="tester",
        )
    await session.commit()


async def _reachable(session: AsyncSession) -> set[str]:
    rows = await routing_status_rows(session, live_schema_digest=LIVE, catalog=CATALOG)
    return {r.operation for r in rows if r.reachable}


@pytest.mark.asyncio
async def test_the_round_trip_enable_then_disable_makes_it_unreachable(
    session: AsyncSession,
) -> None:
    """The capability whose absence made hand-SQL necessary, twice."""
    await _enable_all(session)
    assert await _reachable(session) == {op for op, _ in CATALOG}

    changes, problems = await plan_disable(
        session,
        schema_digest=LIVE,
        operations=dict(CATALOG),
        new_mode="python",
    )
    assert problems == []
    await apply_disable(
        session,
        schema_digest=LIVE,
        changes=changes,
        review_evidence="diverged from Python",
        recorded_by="tester",
    )
    await session.commit()

    assert await _reachable(session) == set(), "disable did not make them unreachable"


@pytest.mark.asyncio
async def test_a_dry_run_plan_changes_nothing(session: AsyncSession) -> None:
    """`plan_disable` must be safe to run on any stack at any time."""
    await _enable_all(session)
    await plan_disable(
        session,
        schema_digest=LIVE,
        operations=dict(CATALOG),
        new_mode="disabled",
    )
    await session.commit()
    assert await _reachable(session) == {op for op, _ in CATALOG}


@pytest.mark.asyncio
async def test_disable_touches_only_the_named_operations(
    session: AsyncSession,
) -> None:
    """The surgical property the shadow intervention needed."""
    await _enable_all(session)
    only = {"hotspots": "doc-hs"}
    changes, _ = await plan_disable(
        session, schema_digest=LIVE, operations=only, new_mode="shadow"
    )
    await apply_disable(
        session,
        schema_digest=LIVE,
        changes=changes,
        review_evidence="diverges",
        recorded_by="tester",
    )
    await session.commit()

    assert await _reachable(session) == {"featureFlags", "flowMatrix"}


@pytest.mark.asyncio
async def test_disable_never_creates_a_row(session: AsyncSession) -> None:
    """An operation that was never enabled stays absent."""
    changes, problems = await plan_disable(
        session,
        schema_digest=LIVE,
        operations=dict(CATALOG),
        new_mode="python",
    )
    assert problems == []
    assert all(c.current_mode is None for c in changes)
    applied = await apply_disable(
        session,
        schema_digest=LIVE,
        changes=changes,
        review_evidence="nothing to do",
        recorded_by="tester",
    )
    await session.commit()

    assert applied == []
    count = await session.scalar(sa.select(sa.func.count()).select_from(RoutingState))
    assert count == 0, "disable inserted a row for an operation that was never enabled"


@pytest.mark.asyncio
async def test_the_candidate_build_guard_refuses_a_repointed_row(
    session: AsyncSession,
) -> None:
    """Someone repointed the row since the operator last looked."""
    await _enable_all(session)
    changes, problems = await plan_disable(
        session,
        schema_digest=LIVE,
        operations=dict(CATALOG),
        new_mode="python",
        expected_candidate_build="a-different-build",
    )
    assert changes == []
    assert len(problems) == len(CATALOG)
    assert "repointed it since you looked" in problems[0]


@pytest.mark.asyncio
async def test_provenance_is_written_to_the_row(session: AsyncSession) -> None:
    """alembic 0127's whole purpose: who and why, durably."""
    await _enable_all(session)
    changes, _ = await plan_disable(
        session,
        schema_digest=LIVE,
        operations={"hotspots": "doc-hs"},
        new_mode="shadow",
    )
    await apply_disable(
        session,
        schema_digest=LIVE,
        changes=changes,
        review_evidence="CHAOS-5447: churn 1 vs 5",
        recorded_by="alice",
    )
    await session.commit()

    row = (
        await session.execute(
            sa.select(RoutingState).where(RoutingState.selected_operation == "hotspots")
        )
    ).scalar_one()
    assert row.mode == "shadow"
    assert row.review_evidence == "CHAOS-5447: churn 1 vs 5"
    assert row.recorded_by == "alice"
