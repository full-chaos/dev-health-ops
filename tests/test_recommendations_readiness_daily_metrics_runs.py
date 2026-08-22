"""Live-PostgreSQL proof for the recommendations readiness gate (CHAOS-4066).

``_daily_metrics_ready`` used to read a ``metric_checkpoints`` row with
``metric_type='daily_finalize'``. Its only writer, ``run_daily_metrics_finalize_task``,
lost its trigger when Celery Beat stopped on 2026-08-19 and was deleted by
CHAOS-4026 (#1846), so the gate had zero writers and "no checkpoint -> proceed"
fired on every call: recommendations could evaluate against a half-written
daily-metrics table and persist misleading fired/tombstone rows (CHAOS-2373).

The gate now reads Go's durable state, ``daily_metrics_runs.finalization_status``.
These tests run the real query against a real PostgreSQL database whose schema
is produced by the alembic migrations themselves (``command.upgrade`` to 0095),
never by hand-written DDL -- the CHAOS-3997/#1836 lesson: an invented test
schema let a query referencing a nonexistent column pass the gate and crash
production.

What matters most here is WHICH run the gate treats as authoritative.
``generation`` is an opaque ``varchar(64)`` written by two disjoint producers
that both land on the same ``target_day``:

* ``fixed-schedule:daily_metrics_fanout:<RFC3339>`` -- the nightly ORG-WIDE run
  (internal/scheduler/fixed/producers.go:433; repositories discovered for the
  whole org at internal/jobs/metrics/daily/clickhouse.go:28-40);
* ``post-sync:<sync run id>`` -- a SUBSET run over only the repositories one
  sync touched (cmd/dev-health-worker/sync_dispatch.go:36-41,
  internal/syncdispatchruntime/native_post_sync.go:403-410).

So the tests below pin all three ways this can be got wrong:
always-passing (a finished subset post-sync masking an unfinished org-wide
fan-out, or an unfinished run ignored outright -- the bug being fixed),
always-blocking (a superseded strand or a terminally canceled run wedging a
live org), and picking the wrong row when several generations exist.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine, make_url

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"
_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"

# The last migration that touches daily_metrics_runs: 0057 creates the table,
# 0095 widens ck_daily_metrics_run_status with the 'no_repositories' outcome.
_TARGET_REVISION = "0095"

_ORG = "00000000-0000-4000-8000-00000000a001"
_OTHER_ORG = "00000000-0000-4000-8000-00000000a002"
_DAY = date(2026, 8, 21)

_FANOUT_GENERATION = "fixed-schedule:daily_metrics_fanout:2026-08-21T01:00:00Z"
# A second nightly generation for the same day, as a schedule replay produces.
_LATER_FANOUT_GENERATION = "fixed-schedule:daily_metrics_fanout:2026-08-21T01:30:00Z"
_POST_SYNC_GENERATION = "post-sync:2f1c9b0e-6a4d-4f2a-9c3b-7e5d8a1b2c3d"

_BASE_TIME = datetime(2026, 8, 21, 1, 0, tzinfo=UTC)


def _migration_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    return config


@pytest.fixture(scope="module")
def migrated_database_url() -> Iterator[str]:
    """A throwaway database migrated to 0095 by alembic itself."""
    configured_uri = os.environ.get(_POSTGRES_URI_ENV)
    if configured_uri is None:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(f"{_POSTGRES_URI_ENV} must be configured for PostgreSQL tests")
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")

    configured_url = make_url(configured_uri)
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")

    database_name = f"test_chaos_4066_{uuid.uuid4().hex}"
    admin_engine = sa.create_engine(
        configured_url.set(drivername="postgresql+psycopg2", database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    database_created = False
    async_url = configured_url.set(
        drivername="postgresql+asyncpg", database=database_name
    ).render_as_string(hide_password=False)
    previous = {
        name: os.environ.get(name)
        for name in (
            "POSTGRES_URI",
            "MIGRATION_DATABASE_URI",
            "MIGRATION_DATABASE_URI_FILE",
        )
    }
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')
            database_created = True

        os.environ["POSTGRES_URI"] = async_url
        os.environ.pop("MIGRATION_DATABASE_URI", None)
        os.environ.pop("MIGRATION_DATABASE_URI_FILE", None)
        command.upgrade(_migration_config(), _TARGET_REVISION)

        yield async_url
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
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


@pytest.fixture
def daily_metrics_runs(
    migrated_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Engine]:
    """Point the production session factory at the migrated database."""
    from dev_health_ops import db as db_module

    monkeypatch.setenv("POSTGRES_URI", migrated_database_url)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.setattr(db_module, "_postgres_sync_engine", None, raising=False)

    engine = sa.create_engine(
        make_url(migrated_database_url).set(drivername="postgresql+psycopg2")
    )
    try:
        with engine.begin() as connection:
            connection.execute(sa.text("TRUNCATE TABLE daily_metrics_runs CASCADE"))
        yield engine
    finally:
        engine.dispose()
        db_module._postgres_sync_engine = None


def _insert_run(
    engine: Engine,
    *,
    generation: str,
    finalization_status: str,
    status: str = "running",
    org_id: str = _ORG,
    target_day: date = _DAY,
    created_at: datetime = _BASE_TIME,
) -> None:
    """Seed one daily_metrics_runs row, honouring the migration's constraints.

    ``ck_daily_metrics_finalize_lease`` (alembic 0057) requires a claim token
    and a lease expiry for -- and only for -- ``finalization_status='running'``,
    so the fixture supplies exactly what production supplies
    (internal/jobs/metrics/daily/postgres.go:770-775).
    """
    claiming = finalization_status == "running"
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                INSERT INTO daily_metrics_runs (
                    id, org_id, target_day, generation, status,
                    finalization_status, finalization_claim_token,
                    finalization_lease_expires_at, finalized_at,
                    created_at, updated_at
                ) VALUES (
                    :id, CAST(:org_id AS uuid), CAST(:target_day AS date),
                    :generation, :status, :finalization_status,
                    CAST(:claim_token AS uuid), :lease_expires_at, :finalized_at,
                    :created_at, :created_at
                )
                """
            ),
            {
                "id": uuid.uuid4(),
                "org_id": org_id,
                "target_day": target_day.isoformat(),
                "generation": generation,
                "status": status,
                "finalization_status": finalization_status,
                "claim_token": str(uuid.uuid4()) if claiming else None,
                "lease_expires_at": created_at + timedelta(minutes=10)
                if claiming
                else None,
                "finalized_at": created_at
                if finalization_status == "succeeded"
                else None,
                "created_at": created_at,
            },
        )


def _ready(org_id: str = _ORG, day: date = _DAY) -> bool:
    from dev_health_ops.workers.recommendations_tasks import _daily_metrics_ready

    return _daily_metrics_ready(org_id, day)


# ---------------------------------------------------------------------------
# The single-run truth table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "finalization_status", "expected_ready"),
    [
        # Inserted 'pending'/'pending' by both writers (postgres.go:109-114,
        # :176-181): the run exists but nothing has computed yet.
        ("pending", "pending", False),
        # Partitions in flight: finalize is not even claimable until every
        # partition succeeded (postgres.go:735-742).
        ("running", "pending", False),
        # A finalizer holds the lease (postgres.go:770-775).
        ("running", "running", False),
        # A claimed finalize was released (postgres.go:866-875). River retries
        # it; until it succeeds the metric tables are demonstrably incomplete.
        ("running", "failed", False),
        # The only success: CompleteFinalize committed (postgres.go:830-836).
        ("succeeded", "succeeded", True),
        # An organization with no repositories is a real no-work outcome, not
        # an unfinished run -- Go sets both columns together (postgres.go:452-454,
        # status value added by alembic 0095).
        ("no_repositories", "succeeded", True),
    ],
)
def test_gate_follows_finalization_status_of_the_days_run(
    daily_metrics_runs: Engine,
    status: str,
    finalization_status: str,
    expected_ready: bool,
) -> None:
    _insert_run(
        daily_metrics_runs,
        generation=_FANOUT_GENERATION,
        status=status,
        finalization_status=finalization_status,
    )

    assert _ready() is expected_ready


def test_absent_run_proceeds(daily_metrics_runs: Engine) -> None:
    """No row for the day -> proceed.

    Deliberate, and the one branch that matches the dead checkpoint's
    behaviour: Go recorded no run for this org/day, so there is no positive
    evidence of partial data. Blocking on absence would stop recommendations
    for every org whose day Go never dispatched.
    """
    assert _ready() is True


# ---------------------------------------------------------------------------
# Which generation is authoritative
# ---------------------------------------------------------------------------


def test_finished_subset_post_sync_cannot_mask_an_unfinished_fanout(
    daily_metrics_runs: Engine,
) -> None:
    """The always-passes failure mode, and the subtle one.

    The 01:00 org-wide fan-out is still pending at evaluation time. A post-sync
    run created at 01:20 -- covering only the repositories one sync touched --
    has already finalized. Ranking the two families together by recency would
    pick the post-sync row, read ``succeeded``, and wave through a day whose
    other repositories are still being written. Only a fan-out generation can
    certify the day.
    """
    _insert_run(
        daily_metrics_runs,
        generation=_FANOUT_GENERATION,
        status="pending",
        finalization_status="pending",
        created_at=_BASE_TIME,
    )
    _insert_run(
        daily_metrics_runs,
        generation=_POST_SYNC_GENERATION,
        status="succeeded",
        finalization_status="succeeded",
        created_at=_BASE_TIME + timedelta(minutes=20),
    )

    assert _ready() is False


def test_post_sync_run_alone_does_not_certify_the_day(
    daily_metrics_runs: Engine,
) -> None:
    """A post-sync run is authoritative in neither direction.

    With no fan-out generation at all the gate is in its documented
    absence branch and proceeds -- a subset run neither certifies the day nor
    blocks it, exactly as the dead checkpoint (which no post-sync path ever
    wrote) behaved.
    """
    _insert_run(
        daily_metrics_runs,
        generation=_POST_SYNC_GENERATION,
        status="running",
        finalization_status="pending",
    )

    assert _ready() is True


def test_superseded_fanout_strand_does_not_wedge_a_finalized_day(
    daily_metrics_runs: Engine,
) -> None:
    """The always-blocks failure mode.

    The first fan-out generation stranded in 'running' with no live job (the
    class CHAOS-3997 exists to reclaim); a later fan-out generation for the
    same day finalized. The day IS complete, so the gate must proceed -- a rule
    of "any unfinished run blocks" would stop recommendations for this org
    until the strand were repaired by hand.
    """
    _insert_run(
        daily_metrics_runs,
        generation=_FANOUT_GENERATION,
        status="running",
        finalization_status="pending",
        created_at=_BASE_TIME,
    )
    _insert_run(
        daily_metrics_runs,
        generation=_LATER_FANOUT_GENERATION,
        status="succeeded",
        finalization_status="succeeded",
        created_at=_BASE_TIME + timedelta(minutes=30),
    )

    assert _ready() is True


def test_canceled_run_is_abandoned_rather_than_in_flight(
    daily_metrics_runs: Engine,
) -> None:
    """A canceled run can never reach 'succeeded', so it must not block.

    Go's store refuses to claim dispatch or finalize unless the run status is
    'running' (internal/jobs/metrics/daily/postgres.go:377-379, :750-752), so a
    canceled row's finalization_status is frozen wherever it stopped. Treating
    it as in-flight would block its target day forever. It is excluded, and the
    gate falls back to the previous eligible generation.
    """
    _insert_run(
        daily_metrics_runs,
        generation=_FANOUT_GENERATION,
        status="succeeded",
        finalization_status="succeeded",
        created_at=_BASE_TIME,
    )
    _insert_run(
        daily_metrics_runs,
        generation=_LATER_FANOUT_GENERATION,
        status="canceled",
        finalization_status="pending",
        created_at=_BASE_TIME + timedelta(minutes=30),
    )

    assert _ready() is True


# ---------------------------------------------------------------------------
# Scoping
# ---------------------------------------------------------------------------


def test_gate_is_scoped_to_the_evaluated_day(daily_metrics_runs: Engine) -> None:
    """A pending run for another day must not block, and vice versa."""
    _insert_run(
        daily_metrics_runs,
        generation=_FANOUT_GENERATION,
        status="succeeded",
        finalization_status="succeeded",
        target_day=_DAY,
    )
    _insert_run(
        daily_metrics_runs,
        generation=_FANOUT_GENERATION,
        status="pending",
        finalization_status="pending",
        target_day=_DAY + timedelta(days=1),
    )

    assert _ready(day=_DAY) is True
    assert _ready(day=_DAY + timedelta(days=1)) is False


def test_gate_is_scoped_to_the_evaluated_organization(
    daily_metrics_runs: Engine,
) -> None:
    """Another tenant's unfinished run must not block this one."""
    _insert_run(
        daily_metrics_runs,
        generation=_FANOUT_GENERATION,
        status="pending",
        finalization_status="pending",
        org_id=_OTHER_ORG,
    )

    assert _ready(org_id=_ORG) is True
    assert _ready(org_id=_OTHER_ORG) is False


def test_default_sentinel_proceeds_without_a_query(
    daily_metrics_runs: Engine,
) -> None:
    """``daily_metrics_runs.org_id`` is a uuid; 'default' cannot exist there.

    Casting it would raise and be swallowed by the fail-open handler, which
    would hide a genuine database outage behind a routine sentinel, so the
    sentinel short-circuits before the query.
    """
    _insert_run(
        daily_metrics_runs,
        generation=_FANOUT_GENERATION,
        status="pending",
        finalization_status="pending",
    )

    assert _ready(org_id="default") is True


def test_unreadable_database_fails_open(
    daily_metrics_runs: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A read error must never permanently wedge the pipeline."""
    from dev_health_ops import db as db_module

    monkeypatch.setenv(
        "POSTGRES_URI",
        "postgresql+asyncpg://postgres:postgres@127.0.0.1:1/chaos_4066_absent",
    )
    monkeypatch.setattr(db_module, "_postgres_sync_engine", None, raising=False)

    assert _ready() is True


# ---------------------------------------------------------------------------
# Cross-language drift guard (no database required -- runs in every tier)
# ---------------------------------------------------------------------------


_REPO_ROOT = Path(__file__).parents[1]


def test_fanout_generation_prefix_matches_the_go_writer() -> None:
    """The gate is only as live as the string Go actually writes.

    This whole ticket exists because a Python gate read a key nothing wrote any
    more, and nothing failed. The gate is now keyed on a literal generation
    prefix owned by Go, so a rename there would silently make it vacuous again
    -- every fan-out row would stop matching and the gate would fall into its
    "no run recorded -> proceed" branch. Pin both ends: the constant Go
    declares, and the expression the scheduler composes it into.
    """
    from dev_health_ops.workers.recommendations_tasks import (
        _SCHEDULED_FANOUT_GENERATION_PREFIX,
    )

    store = (_REPO_ROOT / "internal/jobs/metrics/daily/postgres.go").read_text()
    assert (
        f'scheduledFanoutGenerationPrefix = "{_SCHEDULED_FANOUT_GENERATION_PREFIX}"'
        in store
    ), (
        "internal/jobs/metrics/daily/postgres.go no longer declares "
        f"{_SCHEDULED_FANOUT_GENERATION_PREFIX!r}; _daily_metrics_ready would "
        "match no fan-out run and silently stop gating (CHAOS-4066)"
    )

    # producers.go builds the generation as
    #   "fixed-schedule:" + schedule.ID + ":" + <RFC3339>
    # so the prefix is only correct while the schedule keeps this ID.
    producers = (_REPO_ROOT / "internal/scheduler/fixed/producers.go").read_text()
    schedule_id = _SCHEDULED_FANOUT_GENERATION_PREFIX.removeprefix(
        "fixed-schedule:"
    ).removesuffix(":")
    assert 'generation := "fixed-schedule:" + schedule.ID + ":"' in producers
    assert f'schedule.ID != "{schedule_id}"' in producers, (
        f"the daily fan-out schedule is no longer {schedule_id!r}; the "
        "readiness gate's generation prefix is stale (CHAOS-4066)"
    )
