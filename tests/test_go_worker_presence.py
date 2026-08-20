"""Live-PostgreSQL-gated proof for the CHAOS-3942 Go worker-presence check.

``_check_go_worker_presence``'s SQL is meant to match
``internal/jobruntime/worker_presence.go``'s ``ReadWorkerPresence`` query
(raw ``worker_instances`` read, no shared code with the Go side) -- a
column-name typo or a wrong ``expires_at`` comparison would still pass every
mocked-level test in ``tests/api/test_main_app_integration.py``. This test
only exercises the Python side against a real Postgres, proving that SQL is
right: a live heartbeat row is "ok" (whether ``accepting`` or ``draining``),
an expired one is "absent", and a completely unrepresented worker group is
"absent" too. It does NOT invoke the Go binary or its grouping/queue-decoding
logic -- a genuine Go-side regression needs a Go-side test.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy.engine import make_url

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"

pytestmark = pytest.mark.skipif(
    not os.getenv(_POSTGRES_URI_ENV), reason=f"requires {_POSTGRES_URI_ENV}"
)


@pytest.fixture
def scratch_database():
    """A throwaway PostgreSQL database (own alembic_version), auto-dropped."""

    configured = make_url(os.environ[_POSTGRES_URI_ENV])
    if configured.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")
    admin_url = configured.set(drivername="postgresql+psycopg2")
    dbname = f"go_worker_presence_gate_{uuid.uuid4().hex[:16]}"

    admin_engine = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as connection:
        connection.execute(sa.text(f'CREATE DATABASE "{dbname}"'))
    host = configured.host or "localhost"
    port = configured.port or 5432
    user = configured.username or ""
    password = configured.password or ""
    try:
        sync_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        async_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"
        yield sync_url, async_url
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                sa.text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    f"WHERE datname = '{dbname}'"
                )
            )
            connection.execute(sa.text(f'DROP DATABASE IF EXISTS "{dbname}"'))
        admin_engine.dispose()


def _upgrade_to_head(sync_url: str) -> None:
    from dev_health_ops.migrate import _make_alembic_config

    async_url = sync_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    cfg = _make_alembic_config(async_url)
    previous = os.environ.get("POSTGRES_URI")
    os.environ["POSTGRES_URI"] = async_url
    try:
        # "head" alone is ambiguous: 0066 is its own permanent, intentionally
        # unmerged "river_cutover" branch. worker_instances (0104) descends
        # from 0067's "application_schema" branch instead -- same disambiguation
        # tests/test_ask_dev_v2_persistence_startup_gate.py's _upgrade_to() uses.
        command.upgrade(cfg, "application_schema@head")
    finally:
        if previous is None:
            os.environ.pop("POSTGRES_URI", None)
        else:
            os.environ["POSTGRES_URI"] = previous


@pytest.mark.asyncio
async def test_check_go_worker_presence_reflects_real_worker_instances_rows(
    scratch_database,
) -> None:
    sync_url, async_url = scratch_database
    # alembic's env.py calls asyncio.run() internally, which raises if invoked
    # from inside this test's own running event loop -- run it in a thread so
    # it gets a fresh loop, matching the established pattern in
    # tests/test_ask_dev_v2_persistence_startup_gate.py's _upgrade_to().
    await asyncio.to_thread(_upgrade_to_head, sync_url)

    engine = sa.create_engine(sync_url)
    now = datetime.now(timezone.utc)
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text(
                    "INSERT INTO public.worker_instances "
                    "(instance_id, worker_group, queues, state, started_at, "
                    "heartbeat_at, expires_at) VALUES "
                    "(:instance_id, :worker_group, :queues, :state, :now, "
                    ":now, :expires_at)"
                ),
                [
                    {
                        "instance_id": str(uuid.uuid4()),
                        "worker_group": "heavy",
                        "queues": '["heavy"]',
                        "state": "accepting",
                        "now": now,
                        "expires_at": now + timedelta(seconds=30),
                    },
                    {
                        "instance_id": str(uuid.uuid4()),
                        "worker_group": "ops",
                        "queues": '["ops"]',
                        "state": "accepting",
                        "now": now,
                        "expires_at": now - timedelta(seconds=1),
                    },
                    {
                        "instance_id": str(uuid.uuid4()),
                        "worker_group": "sync",
                        "queues": '["sync"]',
                        # Draining is a live worker finishing in-flight work
                        # during a rolling deploy, not an absent one -- it
                        # must still count as "ok".
                        "state": "draining",
                        "now": now,
                        "expires_at": now + timedelta(seconds=30),
                    },
                ],
            )
    finally:
        engine.dispose()

    from dev_health_ops.api import _health

    previous_postgres_uri = os.environ.get("POSTGRES_URI")
    os.environ["POSTGRES_URI"] = async_url
    try:
        statuses = await _health._check_go_worker_presence(
            ["heavy", "ops", "sync", "sync-provider"]
        )
    finally:
        if previous_postgres_uri is None:
            os.environ.pop("POSTGRES_URI", None)
        else:
            os.environ["POSTGRES_URI"] = previous_postgres_uri

    assert statuses == {
        "heavy": "ok",
        "ops": "absent",
        "sync": "ok",
        "sync-provider": "absent",
    }
