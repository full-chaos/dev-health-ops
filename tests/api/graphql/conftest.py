"""Shared scratch-Postgres fixture for the go_api registry tests.

Extracted from ``test_go_api_routing_admin.py`` when CHAOS-5484 added a
second module needing the same thing
(``test_enablement_proof_admission_cases.py``). One definition rather than
two copies: a fixture that creates a scratch database, builds exactly the
three registry tables in it and drops it afterwards is precisely the kind
of thing that rots when it is duplicated, because only one copy gets
fixed.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import cast

import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.git import Base
from dev_health_ops.models.go_api_registry import (
    CandidateBuild,
    ProofRun,
    RoutingState,
)

POSTGRES_TEST_URI = os.environ.get("DEV_HEALTH_POSTGRES_TEST_URI")


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    """A scratch database with only the three go_api registry tables."""
    assert POSTGRES_TEST_URI is not None
    db_name = f"lane_go_api_registry_{uuid.uuid4().hex}"
    admin = sa.create_engine(
        make_url(POSTGRES_TEST_URI).set(drivername="postgresql+psycopg2"),
        isolation_level="AUTOCOMMIT",
    )
    try:
        with admin.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{db_name}"')
    finally:
        admin.dispose()

    scratch_url = make_url(POSTGRES_TEST_URI).set(database=db_name)
    sync_engine = sa.create_engine(scratch_url.set(drivername="postgresql+psycopg2"))
    try:
        # cast: SQLAlchemy types ``__table__`` as FromClause, but
        # create_all wants Table. Same cast test_go_api_livelocal.py's
        # scratch-DB fixture already uses for these exact three tables.
        registry_tables = cast(
            list[sa.Table],
            [CandidateBuild.__table__, RoutingState.__table__, ProofRun.__table__],
        )
        Base.metadata.create_all(sync_engine, tables=registry_tables)
    finally:
        sync_engine.dispose()

    engine = create_async_engine(
        scratch_url.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as async_session:
            yield async_session
    finally:
        await engine.dispose()
        admin = sa.create_engine(
            make_url(POSTGRES_TEST_URI).set(drivername="postgresql+psycopg2"),
            isolation_level="AUTOCOMMIT",
        )
        try:
            with admin.connect() as connection:
                connection.exec_driver_sql(
                    f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'
                )
        finally:
            admin.dispose()
