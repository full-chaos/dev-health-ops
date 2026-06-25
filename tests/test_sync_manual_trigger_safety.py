"""Manual "Sync now" trigger safety (CHAOS-2647).

The legacy ``sync_runtime``/``sync_tasks`` worker path was removed; manual
triggers route through the fan-out planner (``plan_sync_run`` +
``dispatch_sync_run``). These tests exercise the surviving safety guards on
``trigger_sync_config`` at the function level:

* cross-org service results are rejected (404);
* a config not linked to a migrated integration is rejected (400, planner-only);
* a dispatch-enqueue failure flips the committed-but-undispatched SyncRun to
  FAILED via ``mark_sync_run_failed`` (best-effort fallback) and surfaces 503.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from dev_health_ops.api.admin.routers import sync as sync_router
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunStatus,
)
from dev_health_ops.models.settings import SyncConfiguration


class _FakeAsyncSession:
    """Minimal async-session shim over a sync SQLAlchemy Session.

    Exposes only the ``run_sync`` / ``commit`` surface that
    ``trigger_sync_config`` uses, running the supplied callables against a real
    sync session so the planner helpers persist as they do in production.
    """

    def __init__(self, sync_session: Any):
        self._s = sync_session

    async def run_sync(self, fn, *args, **kwargs):
        return fn(self._s, *args, **kwargs)

    async def commit(self) -> None:
        commit = getattr(self._s, "commit", None)
        if commit is not None:
            commit()


def _service_factory(config: Any):
    class _Svc:
        def __init__(self, session, org_id):
            pass

        async def get_by_id(self, config_id):
            return config

    return _Svc


def _seed_migrated_config(session: Session, org_id: str = "org-a") -> SyncConfiguration:
    integration = Integration(
        org_id=org_id,
        provider="github",
        name=f"integration-{uuid.uuid4()}",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    session.add_all(
        [
            IntegrationSource(
                org_id=org_id,
                integration_id=integration.id,
                provider="github",
                source_type="repository",
                external_id="acme/repo",
                name="repo",
                full_name="acme/repo",
                metadata_={},
                is_enabled=True,
            ),
            IntegrationDataset(
                org_id=org_id,
                integration_id=integration.id,
                dataset_key="commits",
                is_enabled=True,
                options={},
            ),
        ]
    )
    config = SyncConfiguration(
        org_id=org_id,
        name="manual-trigger",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        is_active=True,
        migrated_integration_id=integration.id,
    )
    session.add(config)
    session.flush()
    return config


@pytest.mark.asyncio
async def test_trigger_sync_config_rejects_cross_org_service_result(monkeypatch):
    """A config whose org_id does not match the caller's org is treated as not
    found (404) — the trigger must never act on another org's config."""

    config = SimpleNamespace(
        id=uuid.uuid4(),
        org_id="org-b",
        provider="github",
        sync_targets=["git"],
        sync_options={"owner": "full-chaos"},
        is_active=True,
    )
    monkeypatch.setattr(
        sync_router, "SyncConfigurationService", _service_factory(config)
    )

    with pytest.raises(HTTPException) as exc_info:
        await sync_router.trigger_sync_config(
            str(uuid.uuid4()), session=cast(Any, object()), org_id="org-a"
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_trigger_unmigrated_config_returns_400(monkeypatch):
    """A config not linked to a migrated integration cannot be planner-routed,
    so the manual trigger returns HTTP 400 instead of enqueuing legacy work."""

    config = SimpleNamespace(
        id=uuid.uuid4(),
        org_id="org-a",
        provider="github",
        sync_targets=["git"],
        sync_options={},
        is_active=True,
        migrated_integration_id=None,
        migrated_source_id=None,
        planner_managed=False,
    )
    monkeypatch.setattr(
        sync_router, "SyncConfigurationService", _service_factory(config)
    )

    with pytest.raises(HTTPException) as exc_info:
        await sync_router.trigger_sync_config(
            str(config.id),
            session=cast(Any, _FakeAsyncSession(object())),
            org_id="org-a",
        )

    assert exc_info.value.status_code == 400
    assert "not linked to a migrated integration" in exc_info.value.detail


@pytest.mark.asyncio
async def test_trigger_dispatch_enqueue_failure_marks_sync_run_failed(monkeypatch):
    """When the fan-out dispatch enqueue fails after the planner SyncRun is
    committed, the manual trigger surfaces 503 and the best-effort
    ``mark_sync_run_failed`` fallback flips the stranded PLANNED run to FAILED."""

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as sync_session:
            config = _seed_migrated_config(sync_session, org_id="org-a")
            sync_session.commit()

            monkeypatch.setattr(
                sync_router, "SyncConfigurationService", _service_factory(config)
            )

            class _FailingDispatch:
                @staticmethod
                def apply_async(*args, **kwargs):
                    raise RuntimeError("broker down")

            monkeypatch.setattr(sync_router, "dispatch_sync_run", _FailingDispatch)

            with pytest.raises(HTTPException) as exc_info:
                await sync_router.trigger_sync_config(
                    str(config.id),
                    session=cast(Any, _FakeAsyncSession(sync_session)),
                    org_id="org-a",
                )

            assert exc_info.value.status_code == 503
            assert "broker down" in exc_info.value.detail

            runs = list(sync_session.execute(select(SyncRun)).scalars().all())
            assert len(runs) == 1
            assert runs[0].status == SyncRunStatus.FAILED.value
            assert runs[0].error == "dispatch enqueue failed"
    finally:
        engine.dispose()
