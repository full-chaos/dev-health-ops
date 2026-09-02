"""CHAOS-3952: two concurrent attempts must never both win the completion
fence claim for the same billing_notifications row.

`_claim_billing_notification_completion` is a single
``UPDATE ... WHERE completed_at IS NULL`` so the DECISION (who gets to send)
is the same atomic statement as the WRITE, rather than a separate read
racing a later write. Codex round 1 (P2, executed against a simulated
model) showed the pre-fix read-then-write ordering let two attempts both
observe `completed_at IS NULL` and both send. This proves the fix against a
REAL PostgreSQL server, where the two attempts are two genuinely concurrent
connections/transactions, not two sequential calls in one process.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"


@dataclass(frozen=True, slots=True)
class ScratchDatabase:
    engine: Engine


def _migration_config() -> Config:
    from pathlib import Path

    config = Config()
    config.set_main_option(
        "script_location",
        str(Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"),
    )
    return config


@pytest.fixture
def scratch_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[ScratchDatabase]:
    configured_uri = os.environ.get(_POSTGRES_URI_ENV)
    if configured_uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(
                f"{_POSTGRES_URI_ENV} must be configured for PostgreSQL migration tests"
            )
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")

    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")

    database_name = f"test_billing_fence_claim_{uuid.uuid4().hex}"
    admin_engine = sa.create_engine(
        configured_url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    database_created = False
    engine: Engine | None = None
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            database_created = True

        sync_uri = configured_url.set(
            drivername="postgresql+psycopg2", database=database_name
        ).render_as_string(hide_password=False)
        monkeypatch.setenv(
            "POSTGRES_URI",
            configured_url.set(
                drivername="postgresql+asyncpg", database=database_name
            ).render_as_string(hide_password=False),
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "application_schema@head")

        from dev_health_ops.db import reset_sync_engine

        reset_sync_engine()
        engine = sa.create_engine(sync_uri)
        yield ScratchDatabase(engine=engine)
    finally:
        from dev_health_ops.db import reset_sync_engine

        reset_sync_engine()
        if engine is not None:
            engine.dispose()
        if database_created:
            with admin_engine.connect() as connection:
                connection.execute(
                    sa.text(
                        """
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = :database_name
                          AND pid <> pg_backend_pid()
                        """
                    ),
                    {"database_name": database_name},
                )
                connection.exec_driver_sql(f'DROP DATABASE "{database_name}"')
        admin_engine.dispose()


def test_two_concurrent_claims_on_the_same_row_only_one_wins(
    scratch_db: ScratchDatabase,
) -> None:
    from dev_health_ops.workers.system_ops import (
        _claim_billing_notification_completion,
    )

    notification_id = uuid.uuid4()
    with scratch_db.engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO billing_notifications
                    (id, org_id, notification_type, idempotency_key, attributes, created_at)
                VALUES
                    (:id, :org_id, 'invoice_receipt', :key, '{}'::json, now())
                """
            ),
            {
                "id": notification_id,
                "org_id": uuid.uuid4(),
                "key": f"billing:{notification_id}",
            },
        )

    # Two real threads, two real connections/transactions racing the same
    # atomic UPDATE. ThreadPoolExecutor's submit ordering does not guarantee
    # simultaneity, but the claim function opens its own session per call
    # (get_postgres_session_sync), so both requests are in flight against
    # the server concurrently far more often than not; the assertion below
    # (exactly one True) is what the atomic UPDATE guarantees regardless of
    # timing -- a non-atomic read-then-write could still race and win both.
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(_claim_billing_notification_completion, str(notification_id))
            for _ in range(2)
        ]
        results = [future.result() for future in futures]

    claimed_flags = sorted(result.claimed for result in results)
    assert claimed_flags == [False, True], (
        f"expected exactly one winner across two concurrent claims, got {results}"
    )
    # The loser's ClaimResult must report the winner's claimed_at, not an
    # empty/absent read -- otherwise the caller (send_billing_notification)
    # cannot classify recent-vs-stale and would silently mistreat every loss
    # as a permanent claim.
    loser = next(result for result in results if not result.claimed)
    assert loser.claimed_at is not None
    assert loser.completed_at is None

    with scratch_db.engine.connect() as connection:
        claimed_at = connection.execute(
            sa.text("SELECT claimed_at FROM billing_notifications WHERE id = :id"),
            {"id": notification_id},
        ).scalar_one()
    assert claimed_at is not None
