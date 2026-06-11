from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import HTTPException

from dev_health_ops.api.admin.routers import sync as sync_router
from dev_health_ops.workers import sync_runtime, sync_tasks


class _NoConfigQuery:
    def filter(self, *args):
        return self

    def one_or_none(self):
        return None


class _NoConfigSession:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def query(self, model):
        return _NoConfigQuery()


def test_run_sync_config_unknown_config_is_terminal_without_retry(monkeypatch):
    retry_called = False

    def fail_retry(*args, **kwargs):
        nonlocal retry_called
        retry_called = True
        raise AssertionError("retry should not be called")

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        lambda: _NoConfigSession(),
    )
    monkeypatch.setattr(sync_runtime, "organization_exists_sync", lambda *args: True)
    monkeypatch.setattr(sync_runtime.run_sync_config, "retry", fail_retry)

    with pytest.raises(sync_runtime._TerminalSyncError):
        cast(Any, sync_runtime.run_sync_config).run(str(uuid.uuid4()), "org-a")

    assert retry_called is False


@pytest.mark.skip(
    reason="Flaky in full-suite runs only (passes in isolation and local sqlite suite): "
    "cross-test global-state pollution under CI service tier corrupts dispatch "
    "capture. Test-infra bug, not a product regression. Tracked in CHAOS-2265."
)
@pytest.mark.asyncio
async def test_trigger_sync_config_dispatches_resolved_config_id_and_org(monkeypatch):
    request_id = uuid.uuid4()
    resolved_id = uuid.uuid4()
    calls = []

    class FakeService:
        def __init__(self, session, org_id):
            self.org_id = org_id

        async def get_by_id(self, config_id):
            assert config_id == str(request_id)
            return SimpleNamespace(
                id=resolved_id,
                org_id=self.org_id,
                provider="github",
                sync_targets=["commits"],
                sync_options={"owner": "full-chaos"},
            )

    class FakeTask:
        def delay(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(id="task-1")

    fake_batch_task = FakeTask()
    monkeypatch.setattr(sync_router, "SyncConfigurationService", FakeService)
    monkeypatch.setattr(sync_router, "_is_batch_eligible", lambda config: True)
    monkeypatch.setattr(sync_tasks, "dispatch_batch_sync", fake_batch_task)

    result = await sync_router.trigger_sync_config(
        str(request_id), session=cast(Any, object()), org_id="org-a"
    )

    assert result == {
        "status": "triggered",
        "config_id": str(resolved_id),
        "task_id": "task-1",
    }
    assert calls == [
        {
            "config_id": str(resolved_id),
            "org_id": "org-a",
            "triggered_by": "manual",
        }
    ]


@pytest.mark.skip(
    reason="Flaky in full-suite runs only (shares the global-state pollution "
    "vulnerability of the sibling dispatch test). Test-infra bug, not a product "
    "regression. Tracked in CHAOS-2265."
)
@pytest.mark.asyncio
async def test_trigger_sync_config_rejects_cross_org_service_result(monkeypatch):
    class FakeService:
        def __init__(self, session, org_id):
            pass

        async def get_by_id(self, config_id):
            return SimpleNamespace(
                id=uuid.uuid4(),
                org_id="org-b",
                provider="github",
                sync_targets=["commits"],
                sync_options={"owner": "full-chaos"},
            )

    monkeypatch.setattr(sync_router, "SyncConfigurationService", FakeService)

    with pytest.raises(HTTPException) as exc_info:
        await sync_router.trigger_sync_config(
            str(uuid.uuid4()), session=cast(Any, object()), org_id="org-a"
        )

    assert exc_info.value.status_code == 404
