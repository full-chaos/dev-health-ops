"""PostgreSQL proof for alembic 0109's executed-proof ledger (CHAOS-4114).

The migration has two jobs and both can fail silently.

Creating the table is the easy half. The BACKFILL is the one that matters:
until it runs, the ledger is empty, and an empty ledger reads as "no pair has
ever been attempted" -- which bootstraps every pair through un-gated. A
backfill that quietly produced nothing would look exactly like a healthy fresh
install and would disable the CHAOS-4060 gate outright on the deploy that was
supposed to make it durable.

So this seeds the real shapes into ``sync_run_units`` BEFORE upgrading and
requires the ledger to come out carrying them, including the two the gate
exists to tell apart: a pair that succeeded with rows, and a pair that
"succeeded" repeatedly while persisting nothing.
"""

from __future__ import annotations

import importlib
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Integration,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
    SyncRunUnit,
)

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"
_TABLE = "sync_executed_proof_ledger"


@dataclass(frozen=True, slots=True)
class PostgresMigrationHarness:
    engine: Engine


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture
def migrated_to_0108(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[PostgresMigrationHarness]:
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

    database_name = f"test_chaos_4114_{uuid.uuid4().hex}"
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

        monkeypatch.setenv(
            "POSTGRES_URI",
            configured_url.set(
                drivername="postgresql+asyncpg", database=database_name
            ).render_as_string(hide_password=False),
        )
        monkeypatch.delenv("MIGRATION_DATABASE_URI", raising=False)
        monkeypatch.delenv("MIGRATION_DATABASE_URI_FILE", raising=False)
        command.upgrade(_migration_config(), "0108")

        engine = sa.create_engine(
            configured_url.set(drivername="postgresql+psycopg2", database=database_name)
        )
        yield PostgresMigrationHarness(engine=engine)
    finally:
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


def _revisions(engine: Engine) -> set[str]:
    with engine.connect() as connection:
        return {
            str(row[0])
            for row in connection.execute(
                sa.text("SELECT version_num FROM alembic_version")
            )
        }


def _ledger(engine: Engine) -> dict[str, bool]:
    """``provider/dataset -> proven``, the shape the gate reads."""

    with engine.connect() as connection:
        return {
            f"{row[0]}/{row[1]}": row[2] is not None
            for row in connection.execute(
                sa.text(f"SELECT provider, dataset_key, proven_at FROM public.{_TABLE}")
            )
        }


def _seed_history(engine: Engine) -> None:
    """Seed the pre-migration ``sync_run_units`` history through the ORM.

    Producer paths, not hand-authored SQL: the same mapped classes the planner
    and the workers write through. A fixture that invented its own column list
    would be checking the backfill against a schema production does not have --
    the exact class of gap CHAOS-4041 shipped green on.
    """

    now = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)
    with Session(engine) as session:
        integration = Integration(
            org_id="org-acme",
            provider="github",
            name="acme",
            config={},
            is_active=True,
        )
        session.add(integration)
        session.flush()
        source = IntegrationSource(
            org_id="org-acme",
            integration_id=integration.id,
            provider="github",
            source_type="repo",
            external_id="acme/api",
            name="api",
            full_name="acme/api",
            metadata_={},
            is_enabled=True,
            discovered_at=now,
            last_seen_at=now,
        )
        session.add(source)
        sync_run = SyncRun(
            org_id="org-acme",
            integration_id=integration.id,
            triggered_by="schedule",
            mode=SyncRunMode.INCREMENTAL.value,
            status=SyncRunStatus.SUCCESS.value,
            total_units=0,
            completed_units=0,
            failed_units=0,
        )
        session.add(sync_run)
        session.flush()
        seeds = (
            # Proven: a Go route that landed real rows.
            ("github", "prs", "success", {"go_provider_route": {"records": 3}}),
            # Proven via the legacy Python key.
            ("jira", "incidents", "success", {"persisted": 5}),
            # The CHAOS-4049 counterexample: repeatedly "successful", never
            # persisted a row. Attempted, never proven -- the whole reason the
            # gate exists, and it must survive the projection.
            ("pagerduty", "teams", "success", {"persisted": 0}),
            ("pagerduty", "teams", "success", {"persisted": 0}),
            # Attempted only.
            ("gitlab", "security", "failed", {"error": "boom"}),
            # Malformed and out-of-range values must not fail the backfill --
            # one bad historical blob cannot abort the migration for every
            # other pair.
            ("launchdarkly", "feature-flags", "success", {"persisted": "NaN"}),
            (
                "gitlab",
                "blame",
                "success",
                {"go_provider_route": {"records": 99999999999999999999}},
            ),
            # Mixed case in stored history must fold onto one normalized row.
            ("GitHub", "PRs", "success", {"go_provider_route": {"records": 1}}),
        )
        for provider, dataset, status, result in seeds:
            session.add(
                SyncRunUnit(
                    org_id="org-acme",
                    sync_run_id=sync_run.id,
                    integration_id=integration.id,
                    source_id=source.id,
                    provider=provider,
                    dataset_key=dataset,
                    cost_class="medium",
                    mode=SyncRunMode.INCREMENTAL.value,
                    status=status,
                    attempts=1,
                    processor_flags={},
                    result=result,
                )
            )
        session.commit()


def test_0109_backfills_the_ledger_from_existing_history(
    migrated_to_0108: PostgresMigrationHarness,
) -> None:
    engine = migrated_to_0108.engine
    assert not sa.inspect(engine).has_table(_TABLE)
    _seed_history(engine)

    command.upgrade(_migration_config(), "0109")

    assert _revisions(engine) == {"0109"}
    ledger = _ledger(engine)
    assert ledger == {
        "github/prs": True,
        "jira/incidents": True,
        "pagerduty/teams": False,
        "gitlab/security": False,
        "launchdarkly/feature-flags": False,
        "gitlab/blame": False,
    }, (
        "the backfill did not reproduce the pre-migration evidence. An empty or "
        "wrong ledger reads as 'nothing was ever attempted', which bootstraps "
        "every pair through and disables the CHAOS-4060 gate."
    )

    # attempted_at is derived from the units, not stamped with now(): a
    # backfilled ledger must read as the history it is.
    with engine.connect() as connection:
        derived = connection.execute(
            sa.text(
                f"""
                SELECT count(*)
                FROM public.{_TABLE} AS ledger
                JOIN (
                  SELECT btrim(lower(provider)) AS provider,
                         btrim(lower(dataset_key)) AS dataset_key,
                         min(created_at) AS first_attempt
                  FROM public.sync_run_units GROUP BY 1, 2
                ) AS history
                  ON history.provider = ledger.provider
                 AND history.dataset_key = ledger.dataset_key
                WHERE ledger.attempted_at = history.first_attempt
                """
            )
        ).scalar_one()
    assert derived == len(ledger)


def test_0109_backfill_is_idempotent_and_downgrade_is_clean(
    migrated_to_0108: PostgresMigrationHarness,
) -> None:
    engine = migrated_to_0108.engine
    _seed_history(engine)
    command.upgrade(_migration_config(), "0109")
    first = _ledger(engine)
    with engine.connect() as connection:
        first_stamps = connection.execute(
            sa.text(
                f"SELECT provider, dataset_key, attempted_at, proven_at "
                f"FROM public.{_TABLE} ORDER BY 1, 2"
            )
        ).all()

    # The backfill statement is the recovery tool an operator reaches for when
    # a ledger looks wrong. Re-running it must be a no-op, not a second
    # opinion -- and in particular must never move an existing proven instant.
    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0109_add_sync_executed_proof_ledger"
    )
    with engine.begin() as connection:
        connection.execute(sa.text(migration._BACKFILL_SQL))
    with engine.connect() as connection:
        second_stamps = connection.execute(
            sa.text(
                f"SELECT provider, dataset_key, attempted_at, proven_at "
                f"FROM public.{_TABLE} ORDER BY 1, 2"
            )
        ).all()
    assert second_stamps == first_stamps
    assert _ledger(engine) == first

    command.downgrade(_migration_config(), "0108")
    assert not sa.inspect(engine).has_table(_TABLE)
    assert _revisions(engine) == {"0108"}

    command.upgrade(_migration_config(), "application_schema@head")
    assert _revisions(engine) == {"0116"}
    assert _ledger(engine) == first


def test_0109_backfill_repairs_a_ledger_that_missed_rows(
    migrated_to_0108: PostgresMigrationHarness,
) -> None:
    """The reconciliation tool for the two cutover windows, proven to work.

    Two windows can leave a ``sync_run_units`` row without a ledger write: a
    row committed after the backfill's snapshot, and a row committed by a
    pre-deploy worker mid-rollout. Both leave the ledger BEHIND reality, which
    is the fail-closed direction -- a pair missing its proof is blocked, and it
    was already blocked before this migration existed.

    The remedy is the backfill statement itself. This simulates both shapes of
    loss against a live ledger -- a pair whose row vanished entirely (missing
    attempted), and a proven pair demoted to unproven (missing proof) -- then
    re-runs the backfill and requires the ledger to come back correct WITHOUT
    disturbing a proving instant that was already recorded elsewhere.
    """

    engine = migrated_to_0108.engine
    _seed_history(engine)
    command.upgrade(_migration_config(), "0109")
    intact = _ledger(engine)
    with engine.connect() as connection:
        untouched_before = connection.execute(
            sa.text(
                f"SELECT proven_at FROM public.{_TABLE} "
                "WHERE provider = 'jira' AND dataset_key = 'incidents'"
            )
        ).scalar_one()

    with engine.begin() as connection:
        # The pair a missed INSERT would leave absent entirely.
        connection.execute(
            sa.text(
                f"DELETE FROM public.{_TABLE} "
                "WHERE provider = 'github' AND dataset_key = 'prs'"
            )
        )
        # The pair a missed successful COMPLETION would leave unproven.
        connection.execute(
            sa.text(
                f"UPDATE public.{_TABLE} SET proven_at = NULL "
                "WHERE provider = 'gitlab' AND dataset_key = 'blame'"
            )
        )
        connection.execute(
            sa.text(
                f"UPDATE public.{_TABLE} SET proven_at = NULL "
                "WHERE provider = 'github' AND dataset_key = 'cicd'"
            )
        )
    damaged = _ledger(engine)
    assert damaged != intact, (
        "the damage fixture changed nothing, so the repair proves nothing"
    )

    migration = importlib.import_module(
        "dev_health_ops.alembic.versions.0109_add_sync_executed_proof_ledger"
    )
    with engine.begin() as connection:
        connection.execute(sa.text(migration._BACKFILL_SQL))

    assert _ledger(engine) == intact, (
        "re-running the backfill did not reconcile the ledger with sync_run_units"
    )
    with engine.connect() as connection:
        untouched_after = connection.execute(
            sa.text(
                f"SELECT proven_at FROM public.{_TABLE} "
                "WHERE provider = 'jira' AND dataset_key = 'incidents'"
            )
        ).scalar_one()
    assert untouched_after == untouched_before, (
        "the repair moved a proving instant it had no business touching -- proof "
        "is permanent, and a reconciliation that re-dates it is not monotone"
    )
