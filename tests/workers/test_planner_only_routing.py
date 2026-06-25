"""Planner-only routing invariants (CHAOS-2647).

Asserts that manual "Sync now", scheduler, and backfill ALL route through
plan_sync_run + dispatch_sync_run, and that an unmigrated config (no
migrated_integration_id) causes:
  - manual trigger  → planner_request_for_config_if_routed returns None
                       (the HTTP layer converts this to HTTP 400)
  - scheduler       → _maybe_dispatch_config returns False (skip)
  - backfill        → planner_request_for_config_if_routed returns None

These tests exercise the real routing helpers against an in-memory SQLite DB
using the same fixture style as tests/test_sync_units.py.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRunMode,
)
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.models.users import Organization


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _fake_session_ctx(session):
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


def _patch_db_session(monkeypatch, session):
    import dev_health_ops.db as db

    session.commit()
    monkeypatch.setattr(
        db, "get_postgres_session_sync", lambda: _fake_session_ctx(session)
    )


def _seed_org(session) -> str:
    org_id = str(uuid.uuid4())
    org = Organization(
        id=uuid.UUID(org_id),
        name="test-org",
        slug="test-org",
    )
    session.add(org)
    session.flush()
    return org_id


def _seed_integration(session, org_id: str) -> Integration:
    integration = Integration(
        org_id=org_id,
        provider="github",
        name="test-integration",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    return integration


def _seed_source(session, org_id: str, integration: Integration) -> IntegrationSource:
    source = IntegrationSource(
        org_id=org_id,
        integration_id=integration.id,
        provider="github",
        source_type="repo",
        external_id="org/repo",
        name="repo",
        full_name="org/repo",
        metadata_={},
        is_enabled=True,
    )
    session.add(source)
    session.flush()
    return source


def _seed_dataset(session, org_id: str, integration: Integration) -> IntegrationDataset:
    dataset = IntegrationDataset(
        org_id=org_id,
        integration_id=integration.id,
        dataset_key="commits",
        is_enabled=True,
        options={},
    )
    session.add(dataset)
    session.flush()
    return dataset


def _seed_config(
    session,
    org_id: str,
    integration: Integration,
    *,
    migrated: bool = True,
    is_active: bool = True,
    schedule_cron: str | None = None,
) -> SyncConfiguration:
    """Seed a SyncConfiguration, optionally linked to a migrated integration."""
    config = SyncConfiguration(
        org_id=org_id,
        name="test-config",
        provider="github",
        sync_targets=["git"],
        sync_options={"schedule_cron": schedule_cron} if schedule_cron else {},
        is_active=is_active,
        migrated_integration_id=integration.id if migrated else None,
    )
    session.add(config)
    session.flush()
    return config


# ---------------------------------------------------------------------------
# Test 5a: unmigrated config → planner_request_for_config_if_routed returns None
# ---------------------------------------------------------------------------


def test_unmigrated_config_returns_none_from_planner_request(db_session):
    """An unmigrated config (no migrated_integration_id) must return None.

    The HTTP layer converts None → HTTP 400.  This test exercises the real
    routing helper without going through the HTTP stack.
    """
    from dev_health_ops.sync.trigger_routing import planner_request_for_config_if_routed

    org_id = str(uuid.uuid4())
    integration = _seed_integration(db_session, org_id)
    config = _seed_config(db_session, org_id, integration, migrated=False)

    result = planner_request_for_config_if_routed(
        db_session, config, triggered_by="manual", mode="incremental"
    )

    assert result is None, (
        "planner_request_for_config_if_routed must return None for an unmigrated "
        "config (no migrated_integration_id) — the HTTP layer maps this to HTTP 400"
    )


# ---------------------------------------------------------------------------
# Test 5b: migrated config → planner_request_for_config_if_routed returns a request
# ---------------------------------------------------------------------------


def test_migrated_config_returns_plan_request(db_session):
    """A migrated config must produce a non-None SyncPlanRequest."""
    from dev_health_ops.sync.trigger_routing import planner_request_for_config_if_routed

    org_id = str(uuid.uuid4())
    integration = _seed_integration(db_session, org_id)
    _seed_source(db_session, org_id, integration)
    _seed_dataset(db_session, org_id, integration)
    config = _seed_config(db_session, org_id, integration, migrated=True)

    result = planner_request_for_config_if_routed(
        db_session, config, triggered_by="manual", mode="incremental"
    )

    assert result is not None, (
        "planner_request_for_config_if_routed must return a SyncPlanRequest for a "
        "migrated config"
    )
    assert result.integration_id == str(integration.id)
    assert result.org_id == org_id
    assert result.triggered_by == "manual"
    assert result.mode == SyncRunMode.INCREMENTAL.value


# ---------------------------------------------------------------------------
# Test 5c: scheduler skips unmigrated config
# ---------------------------------------------------------------------------


def test_scheduler_skips_unmigrated_config(db_session, monkeypatch):
    """_maybe_dispatch_config must return False for an unmigrated config.

    The scheduler calls planner_request_for_config_if_routed; when it returns
    None the scheduler logs a warning and returns False (skip).
    """
    from dev_health_ops.workers.sync_scheduler import _maybe_dispatch_config

    org_id = str(uuid.uuid4())
    integration = _seed_integration(db_session, org_id)
    config = _seed_config(
        db_session,
        org_id,
        integration,
        migrated=False,
        schedule_cron="0 * * * *",
    )

    # Patch org existence check so the scheduler doesn't bail early.
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_scheduler.organization_exists_sync",
        lambda session, org_id_arg: True,
    )

    now = datetime.now(timezone.utc)
    result = _maybe_dispatch_config(db_session, config, now)

    assert result is False, (
        "_maybe_dispatch_config must return False (skip) for an unmigrated config"
    )


# ---------------------------------------------------------------------------
# Test 5d: migrated config with schedule → scheduler calls plan_sync_run + dispatch
# ---------------------------------------------------------------------------


def test_scheduler_routes_migrated_config_through_planner(db_session, monkeypatch):
    """A migrated, due config must be routed through plan_sync_run + dispatch_sync_run."""
    from dev_health_ops.workers.sync_scheduler import _maybe_dispatch_config

    org_id = str(uuid.uuid4())
    integration = _seed_integration(db_session, org_id)
    _seed_source(db_session, org_id, integration)
    _seed_dataset(db_session, org_id, integration)
    config = _seed_config(
        db_session,
        org_id,
        integration,
        migrated=True,
        schedule_cron="0 * * * *",
    )
    # Force last_sync_at far in the past so the config is due.
    # Use a naive datetime: croniter.get_next(datetime) returns naive, so
    # _maybe_dispatch_config's comparison requires both sides to be naive.
    config.last_sync_at = datetime(2000, 1, 1)  # naive UTC
    db_session.flush()

    plan_calls: list[object] = []
    dispatch_calls: list[object] = []

    from dev_health_ops.sync import planner as planner_mod
    from dev_health_ops.workers import sync_units as sync_units_mod

    original_plan = planner_mod.plan_sync_run

    def fake_plan(session, request):
        plan_calls.append(request)
        return original_plan(session, request)

    monkeypatch.setattr(planner_mod, "plan_sync_run", fake_plan)
    monkeypatch.setattr(
        sync_units_mod.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None, **kw: dispatch_calls.append((args, queue)),
    )
    monkeypatch.setattr(
        "dev_health_ops.workers.sync_scheduler.organization_exists_sync",
        lambda session, org_id_arg: True,
    )
    _patch_db_session(monkeypatch, db_session)

    # Pass a naive now to match croniter's naive output.
    now = datetime.utcnow()
    result = _maybe_dispatch_config(db_session, config, now)

    assert result is True, (
        "_maybe_dispatch_config must return True for a due, migrated config"
    )
    assert len(plan_calls) == 1, "plan_sync_run must be called exactly once"
    assert len(dispatch_calls) == 1, "dispatch_sync_run.apply_async must be called once"


# ---------------------------------------------------------------------------
# Test 5e: backfill unmigrated config → planner_request_for_config_if_routed returns None
# ---------------------------------------------------------------------------


def test_backfill_unmigrated_config_returns_none(db_session):
    """Backfill of an unmigrated config must return None (HTTP layer → HTTP 400)."""
    from dev_health_ops.sync.trigger_routing import planner_request_for_config_if_routed

    org_id = str(uuid.uuid4())
    integration = _seed_integration(db_session, org_id)
    config = _seed_config(db_session, org_id, integration, migrated=False)

    result = planner_request_for_config_if_routed(
        db_session, config, triggered_by="backfill", mode="backfill"
    )

    assert result is None, (
        "planner_request_for_config_if_routed must return None for an unmigrated "
        "config on backfill — the HTTP layer maps this to HTTP 400"
    )
