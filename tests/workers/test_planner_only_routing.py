"""Planner-only routing invariants (CHAOS-2647).

Asserts that manual "Sync now", scheduler, and backfill ALL route through
plan_sync_run + the transactional dispatch outbox, and that an unmigrated config (no
integration_id) causes:
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
        integration_id=integration.id if migrated else None,
    )
    session.add(config)
    session.flush()
    return config


# ---------------------------------------------------------------------------
# Test 5a: unmigrated config → planner_request_for_config_if_routed returns None
# ---------------------------------------------------------------------------


def test_unmigrated_config_returns_none_from_planner_request(db_session):
    """An unmigrated config (no integration_id) must return None.

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
        "config (no integration_id) — the HTTP layer maps this to HTTP 400"
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


# CHAOS-3093: Tests 5c/5d (test_scheduler_skips_unmigrated_config,
# test_scheduler_routes_migrated_config_through_planner) tested
# workers.sync_scheduler._maybe_dispatch_config, a thin wrapper around
# planner_request_for_config_if_routed (tested directly above/below) plus a
# durable-outbox plan call -- deleted with that module (dispatch_scheduled_syncs
# had zero real callers since CHAOS-4054 removed the dispatch plane; the
# scheduling cadence itself is internal/scheduler/sync, already Go-native).


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
