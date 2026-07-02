"""Tests for the durable rate-limit observation store (CHAOS-2758).

Covers:
  * the observation row commits atomically with the unit's RETRYING stamp --
    same session/transaction, never an orphan when the deferral CAS loses.
  * integration_id / route_family / dimension enrichment from the unit row +
    the budget estimate already computed for dispatch (never re-estimated).
  * only normalized fields are persisted -- never raw headers/secrets.
  * the ws-a exception-unification fix (CHAOS-2753) means
    ``connectors.base.RateLimitException`` (no signal) also produces a row.
  * the beat-scheduled retention prune task.
  * migration 0031 is a guarded, retry-safe (idempotent) upgrade/downgrade.
"""

from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    ProviderRateLimitObservation,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.budget_types import BudgetDimension
from dev_health_ops.sync.rate_limit_signal import RateLimitSignal
from tests.test_sync_units import (
    _mark_dispatching,
    _patch_db_session,
    _patch_finalize_apply,
    _patch_runtime,
    _seed_run,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _clean_env(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URI", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)


def _aware(value: datetime) -> datetime:
    """Normalize a datetime read back from SQLite (which drops tzinfo on
    round-trip) to UTC-aware, for comparison against the tz-aware value that
    was written."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Atomic persistence with the RETRYING stamp
# ---------------------------------------------------------------------------


def test_observation_persisted_atomically_with_deferral(db_session, monkeypatch):
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)  # provider=github, dataset_key=commits
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    reset_at = datetime(2026, 7, 2, 0, 0, 0, tzinfo=timezone.utc)

    def rate_limited(ctx, runtime):
        raise RateLimitException(
            "GitHub primary rate limit exceeded",
            retry_after_seconds=45.0,
            signal=RateLimitSignal(
                provider="github",
                host="api.github.com",
                dimension=BudgetDimension.REST_CORE,
                retry_after_seconds=45.0,
                reset_at=reset_at,
                reason="primary",
                request_id="req-123",
            ),
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    db_session.refresh(unit)
    assert result["status"] == "rate_limited_deferred"
    assert unit.status == SyncRunUnitStatus.RETRYING.value

    observations = db_session.query(ProviderRateLimitObservation).all()
    assert len(observations) == 1
    observation = observations[0]
    assert observation.org_id == unit.org_id
    assert observation.provider == "github"
    assert observation.host == "api.github.com"
    assert observation.integration_id == unit.integration_id
    assert observation.sync_run_id == run.id
    assert observation.sync_run_unit_id == unit.id
    assert observation.route_family == "git"  # github "commits" estimator family
    assert observation.dimension == "rest_core"
    assert observation.retry_after_seconds == 45.0
    assert _aware(observation.reset_at) == reset_at
    assert observation.reason == "primary"
    assert observation.request_id == "req-123"
    assert observation.observed_at is not None


def test_observation_not_orphaned_when_deferral_cas_loses_lease(
    db_session, monkeypatch
):
    """A CAS loss on the RETRYING stamp must leave no orphan observation row.

    The observation insert is deliberately placed AFTER the deferral CAS
    rowcount check in ``run_sync_unit`` so the two either commit together or
    neither does. Mirrors the established
    ``test_run_sync_unit_lost_lease_before_work_item_sink_aborts_without_finalize``
    pattern: steal the lease via the SAME shared session mid-flight (rather
    than a second engine/session), so the CAS predicate's ``lease_owner``
    match fails without detaching the ``unit`` ORM object the deferral branch
    still reads (``rate_limit_deferrals`` / ``rate_limit_first_seen_at``).
    """
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    def rate_limited(ctx, runtime):
        # Simulate another worker stealing the lease while this task is
        # mid-flight, so the deferral CAS below (which matches on the
        # original lease_owner) affects zero rows.
        db_session.refresh(unit)
        unit.lease_owner = "other-worker"
        db_session.flush()
        raise RateLimitException(
            "GitHub primary rate limit exceeded", retry_after_seconds=30.0
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result == {
        "status": "skipped",
        "unit_id": str(unit.id),
        "reason": "lease_lost",
    }
    assert db_session.query(ProviderRateLimitObservation).count() == 0


# ---------------------------------------------------------------------------
# Enrichment: integration_id from the unit row, route_family/dimension from
# the pre-computed budget audit (never re-estimated).
# ---------------------------------------------------------------------------


def test_observation_enriched_with_integration_and_route_family(
    db_session, monkeypatch
):
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.sync_units import run_sync_unit

    # "prs" emits three estimates: prs/rest_core, pr_social/graphql_cost,
    # pr_social/secondary_abuse_risk -- a real multi-route-family unit.
    run, unit = _seed_run(db_session, dataset_key="prs")
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    estimate_calls = []
    real_estimate = sync_units.estimate_provider_budget

    def counting_estimate(ctx):
        estimate_calls.append(ctx)
        return real_estimate(ctx)

    monkeypatch.setattr(sync_units, "estimate_provider_budget", counting_estimate)

    def rate_limited(ctx, runtime):
        raise RateLimitException(
            "GitHub GraphQL secondary rate limit",
            retry_after_seconds=60.0,
            signal=RateLimitSignal(
                provider="github",
                dimension=BudgetDimension.GRAPHQL_COST,
                retry_after_seconds=60.0,
                reason="secondary",
            ),
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "rate_limited_deferred"
    # The estimate was computed exactly once (at dispatch, for the started_at
    # audit) -- the exception branch reuses it, it never re-estimates
    # (estimators require credential decryption).
    assert len(estimate_calls) == 1

    observation = db_session.query(ProviderRateLimitObservation).one()
    assert observation.integration_id == unit.integration_id
    assert observation.route_family == "pr_social"
    assert observation.dimension == "graphql_cost"


def test_route_family_resolution_falls_back_to_primary_estimate():
    """Unit test of the pick-one-primary-family decision (CHAOS-2758).

    When the signal's dimension is absent or matches nothing in the budget
    audit, the first-emitted estimate (each provider estimator emits its
    dataset-primary family first) is used as the fallback "primary".
    """
    from dev_health_ops.workers.sync_units import _route_family_for_rate_limit

    budget_audit = [
        {"bucket": {"dimension": "rest_core"}, "route_family": "prs"},
        {"bucket": {"dimension": "graphql_cost"}, "route_family": "pr_social"},
        {"bucket": {"dimension": "secondary_abuse_risk"}, "route_family": "pr_social"},
    ]

    assert (
        _route_family_for_rate_limit(budget_audit, "secondary_abuse_risk")
        == "pr_social"
    )
    assert _route_family_for_rate_limit(budget_audit, "rest_core") == "prs"
    # No match / no signal dimension -> first-emitted (documented "primary").
    assert _route_family_for_rate_limit(budget_audit, None) == "prs"
    assert _route_family_for_rate_limit(budget_audit, "search") == "prs"
    assert _route_family_for_rate_limit(None, "rest_core") is None
    assert _route_family_for_rate_limit([], "rest_core") is None


# ---------------------------------------------------------------------------
# No raw headers / secrets ever persisted
# ---------------------------------------------------------------------------


def test_observation_contains_no_raw_headers_or_secrets(db_session, monkeypatch):
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    class _ExceptionWithRawHeaders(RateLimitException):
        """Simulates a provider client that stashed raw headers on the
        exception for its own in-place retry logic -- the observation writer
        must never read or persist them."""

        def __init__(self) -> None:
            super().__init__(
                "rate limited",
                retry_after_seconds=15.0,
                signal=RateLimitSignal(
                    provider="github",
                    dimension=BudgetDimension.REST_CORE,
                    retry_after_seconds=15.0,
                    reason="primary",
                ),
            )
            self.raw_headers = {
                "Authorization": "Bearer super-secret-token",
                "X-RateLimit-Remaining": "0",
            }

    def rate_limited(ctx, runtime):
        raise _ExceptionWithRawHeaders()

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    getattr(run_sync_unit, "run")(str(unit.id))

    observation = db_session.query(ProviderRateLimitObservation).one()
    mapper = sa.inspect(ProviderRateLimitObservation)
    persisted_columns = {col.key for col in mapper.columns}
    # Only the normalized schema fields exist on the row -- no headers column,
    # no raw dict anywhere in the mapped state.
    assert "raw_headers" not in persisted_columns
    assert "headers" not in persisted_columns
    for column_name in persisted_columns:
        value = getattr(observation, column_name)
        if isinstance(value, str):
            assert "super-secret-token" not in value
            assert "Authorization" not in value


# ---------------------------------------------------------------------------
# Legacy connector exceptions (post CHAOS-2753 unification) also persist
# ---------------------------------------------------------------------------


def test_legacy_connector_exception_also_persists_observation(db_session, monkeypatch):
    from dev_health_ops.connectors.base import (
        RateLimitException as LegacyRateLimitException,
    )
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit

    run, unit = _seed_run(db_session)
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    _clean_env(monkeypatch)

    def rate_limited(ctx, runtime):
        # No `signal=` -- matches the real legacy connectors (connectors/
        # github.py, connectors/gitlab.py) at call sites that predate ws-a
        # signal population, and any that still omit one.
        raise LegacyRateLimitException(
            "GitLab rate limited (HTTP 429)", retry_after_seconds=30.0
        )

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)

    result = getattr(run_sync_unit, "run")(str(unit.id))

    assert result["status"] == "rate_limited_deferred"
    observation = db_session.query(ProviderRateLimitObservation).one()
    assert observation.org_id == unit.org_id
    assert observation.provider == "github"  # falls back to ctx.provider
    assert observation.integration_id == unit.integration_id
    assert observation.sync_run_unit_id == unit.id
    assert observation.retry_after_seconds == 30.0
    assert observation.dimension is None  # no signal -> no client-known dimension
    assert observation.route_family == "git"  # falls back to first estimate
    assert observation.reason == "GitLab rate limited (HTTP 429)"
    assert observation.host is None
    assert observation.request_id is None


# ---------------------------------------------------------------------------
# Retention prune task
# ---------------------------------------------------------------------------


def _make_observation(**overrides):
    defaults = dict(
        org_id=str(uuid.uuid4()),
        provider="github",
        host="api.github.com",
        integration_id=uuid.uuid4(),
        sync_run_id=uuid.uuid4(),
        sync_run_unit_id=uuid.uuid4(),
        route_family="git",
        dimension="rest_core",
        retry_after_seconds=30.0,
        reason="primary",
        observed_at=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    return ProviderRateLimitObservation(**defaults)


def test_prune_task_deletes_expired_rows_only(db_session, monkeypatch):
    from dev_health_ops.workers.sync_reconciler import prune_rate_limit_observations

    now = datetime.now(timezone.utc)
    expired = _make_observation(observed_at=now - timedelta(days=20))
    borderline_fresh = _make_observation(observed_at=now - timedelta(days=1))
    fresh = _make_observation(observed_at=now - timedelta(hours=1))
    db_session.add_all([expired, borderline_fresh, fresh])
    db_session.commit()

    _patch_db_session(monkeypatch, db_session)
    monkeypatch.setenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", "14")

    result = getattr(prune_rate_limit_observations, "run")()

    assert result["status"] == "completed"
    assert result["deleted"] == 1
    assert result["retention_days"] == 14
    remaining_ids = {
        row.id for row in db_session.query(ProviderRateLimitObservation).all()
    }
    assert remaining_ids == {borderline_fresh.id, fresh.id}


def test_prune_task_honors_explicit_retention_days_override(db_session, monkeypatch):
    from dev_health_ops.workers.sync_reconciler import prune_rate_limit_observations

    now = datetime.now(timezone.utc)
    three_days_old = _make_observation(observed_at=now - timedelta(days=3))
    one_hour_old = _make_observation(observed_at=now - timedelta(hours=1))
    db_session.add_all([three_days_old, one_hour_old])
    db_session.commit()

    _patch_db_session(monkeypatch, db_session)
    # Env default (14) would keep both; an explicit override prunes tighter.
    monkeypatch.delenv("SYNC_RATE_LIMIT_OBSERVATION_RETENTION_DAYS", raising=False)

    result = getattr(prune_rate_limit_observations, "run")(retention_days=1)

    assert result["deleted"] == 1
    remaining_ids = {
        row.id for row in db_session.query(ProviderRateLimitObservation).all()
    }
    assert remaining_ids == {one_hour_old.id}


# ---------------------------------------------------------------------------
# Migration 0031: guarded, retry-safe (idempotent) upgrade
# ---------------------------------------------------------------------------


def _load_migration_0031():
    return importlib.import_module(
        "dev_health_ops.alembic.versions.0031_add_provider_rate_limit_observations"
    )


def test_migration_0031_idempotent_upgrade():
    migration = _load_migration_0031()
    assert migration.revision == "0031"
    assert migration.down_revision == "0030"

    engine = create_engine("sqlite:///:memory:")
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            # Operations.context() installs the module-global `alembic.op`
            # proxy (the same `from alembic import op` the migration file
            # imports) for the duration of the block -- the documented way to
            # unit-test an individual migration's upgrade()/downgrade().
            with Operations.context(ctx):
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "provider_rate_limit_observations" in inspector.get_table_names()
                index_names = {
                    ix["name"]
                    for ix in inspector.get_indexes("provider_rate_limit_observations")
                }
                assert "ix_provider_rate_limit_observations_cooldown" in index_names
                assert "ix_provider_rate_limit_observations_org_id" in index_names

                # Re-running upgrade() must be a no-op, not an error (guarded
                # create-if-missing, per the 0020/0025 convention).
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert (
                    inspector.get_table_names().count(
                        "provider_rate_limit_observations"
                    )
                    == 1
                )

                migration.downgrade()
                inspector = sa.inspect(conn)
                assert (
                    "provider_rate_limit_observations"
                    not in inspector.get_table_names()
                )

                # downgrade() on an already-absent table is also a no-op.
                migration.downgrade()

                # And upgrade() works again from a clean slate.
                migration.upgrade()
                inspector = sa.inspect(conn)
                assert "provider_rate_limit_observations" in inspector.get_table_names()
    finally:
        engine.dispose()
