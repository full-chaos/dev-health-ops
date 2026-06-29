from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.backfill import runner as backfill_runner
from dev_health_ops.models import (
    Base,
    Integration,
    SyncRun,
    SyncRunMode,
    SyncRunStatus,
)
from dev_health_ops.models.settings import IntegrationCredential, SyncConfiguration
from dev_health_ops.workers import team_autoimport
from dev_health_ops.workers.post_sync_dispatch import _dispatch_post_sync_tasks

_TEAM_AUTOIMPORT_TASK = "dev_health_ops.workers.tasks.run_post_sync_team_autoimport"
_ORG = "team-autoimport-sync-org"


def test_backfill_autoimport_false_or_absent_does_not_call(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        backfill_runner,
        "run_team_autoimport_strict",
        lambda **kwargs: calls.append(kwargs),
    )

    assert (
        backfill_runner._run_strict_reference_discovery_for_backfill(
            provider="jira",
            org_id="org-1",
            credentials={},
            sync_options={},
            sync_config_id="cfg-1",
            since=date(2026, 1, 1),
            before=date(2026, 1, 7),
            window_count=1,
            analytics_db_url=None,
        )
        is None
    )
    assert (
        backfill_runner._run_strict_reference_discovery_for_backfill(
            provider="jira",
            org_id="org-1",
            credentials={},
            sync_options={"auto_import_teams": False},
            sync_config_id="cfg-1",
            since=date(2026, 1, 1),
            before=date(2026, 1, 7),
            window_count=1,
            analytics_db_url=None,
        )
        is None
    )
    assert calls == []


def test_backfill_autoimport_true_calls_once(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run_team_autoimport(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(
        backfill_runner, "run_team_autoimport_strict", fake_run_team_autoimport
    )
    monkeypatch.setattr(backfill_runner, "_verify_reference_readback", lambda **_: None)

    result = backfill_runner._run_strict_reference_discovery_for_backfill(
        provider="jira",
        org_id="org-1",
        credentials={"email": "dev@example.com"},
        sync_options={"auto_import_teams": True, "project_keys": ["OPS"]},
        sync_config_id="cfg-1",
        since=date(2026, 1, 1),
        before=date(2026, 1, 7),
        window_count=1,
        analytics_db_url="clickhouse://backfill-dsn",
    )

    assert result == {"status": "success"}
    assert len(calls) == 1
    assert calls[0] == {
        "provider": "jira",
        "org_id": "org-1",
        "credentials": {"email": "dev@example.com"},
        "analytics_db_url": "clickhouse://backfill-dsn",
        "scope": {
            "mode": "backfill",
            "sync_config_id": "cfg-1",
            "sync_options": {"auto_import_teams": True, "project_keys": ["OPS"]},
            "window_count": 1,
            "since": "2026-01-01",
            "before": "2026-01-07",
        },
    }


# ---------------------------------------------------------------------------
# Regular-sync surface (CHAOS-2647): the unitized post-sync path must restore
# the per-config-run team auto-import that the deleted legacy worker performed.
# ---------------------------------------------------------------------------


@contextmanager
def _patched_post_sync_dispatch():
    """Patch the celery factories so _dispatch_post_sync_tasks never hits a broker.

    Mirrors tests/test_post_sync_dora_dispatch.py: signature/chain are stubbed so
    the investment chain does not dispatch, leaving send_task observable.
    """
    with (
        patch(
            "dev_health_ops.workers.post_sync_dispatch.celery_app.signature"
        ) as mock_signature,
        patch("dev_health_ops.workers.post_sync_dispatch.chain") as mock_chain,
        patch(
            "dev_health_ops.workers.post_sync_dispatch.celery_app.send_task"
        ) as mock_send_task,
    ):
        mock_signature.side_effect = lambda name, **kwargs: MagicMock()
        mock_chain.return_value = MagicMock()
        yield mock_send_task


def _team_autoimport_calls(mock_send_task):
    return [
        call
        for call in mock_send_task.call_args_list
        if call.args and call.args[0] == _TEAM_AUTOIMPORT_TASK
    ]


def test_post_sync_dispatch_enqueues_team_autoimport_when_enabled() -> None:
    with _patched_post_sync_dispatch() as mock_send_task:
        _dispatch_post_sync_tasks(
            provider="github",
            sync_targets=["work-items"],
            org_id="org-1",
            auto_import_teams=True,
            sync_run_id="run-1",
        )
    calls = _team_autoimport_calls(mock_send_task)
    assert len(calls) == 1
    assert calls[0].kwargs["kwargs"] == {"sync_run_id": "run-1"}
    assert calls[0].kwargs["queue"] == "sync"


def test_post_sync_dispatch_skips_team_autoimport_when_disabled() -> None:
    with _patched_post_sync_dispatch() as mock_send_task:
        _dispatch_post_sync_tasks(
            provider="github",
            sync_targets=["work-items"],
            org_id="org-1",
            auto_import_teams=False,
            sync_run_id="run-1",
        )
    assert _team_autoimport_calls(mock_send_task) == []


def test_post_sync_dispatch_skips_team_autoimport_without_sync_run_id() -> None:
    with _patched_post_sync_dispatch() as mock_send_task:
        _dispatch_post_sync_tasks(
            provider="github",
            sync_targets=["work-items"],
            org_id="org-1",
            auto_import_teams=True,
            sync_run_id=None,
        )
    assert _team_autoimport_calls(mock_send_task) == []


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


@contextmanager
def _session_ctx(session):
    yield session
    session.commit()


def _patch_session(monkeypatch, session) -> None:
    import dev_health_ops.db as db

    monkeypatch.setattr(db, "get_postgres_session_sync", lambda: _session_ctx(session))


def _seed_run_with_config(
    session,
    *,
    provider: str = "github",
    status: str = SyncRunStatus.SUCCESS.value,
    sync_options: dict[str, Any] | None = None,
    credential_id: uuid.UUID | None = None,
):
    integration = Integration(
        org_id=_ORG,
        provider=provider,
        name=f"{provider} integration",
        config={},
        is_active=True,
    )
    session.add(integration)
    session.flush()
    integration.credential_id = credential_id
    config = SyncConfiguration(
        name="canonical-config",
        provider=provider,
        org_id=_ORG,
        sync_targets=["work-items"],
        sync_options=sync_options if sync_options is not None else {},
        integration_id=integration.id,
        parent_id=None,
        planner_managed=True,
    )
    session.add(config)
    run = SyncRun(
        org_id=_ORG,
        integration_id=integration.id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=status,
    )
    session.add(run)
    session.flush()
    return run, integration, config


def test_post_sync_team_autoimport_calls_run_team_autoimport_on_success(
    db_session, monkeypatch
) -> None:
    run, _integration, config = _seed_run_with_config(
        db_session,
        status=SyncRunStatus.SUCCESS.value,
        sync_options={"auto_import_teams": True},
    )
    _patch_session(monkeypatch, db_session)
    captured: list[dict[str, Any]] = []

    def _run_autoimport(**kwargs: Any) -> dict[str, Any]:
        captured.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(team_autoimport, "run_team_autoimport", _run_autoimport)

    result = team_autoimport.run_post_sync_team_autoimport(str(run.id))

    assert result["status"] == "dispatched"
    assert len(captured) == 1
    assert captured[0]["provider"] == "github"
    assert captured[0]["org_id"] == _ORG
    scope = captured[0]["scope"]
    assert scope["mode"] == "sync_config"
    assert scope["sync_config_id"] == str(config.id)
    assert scope["sync_targets"] == ["work-items"]
    assert scope["triggered_by"] == "manual"


def test_post_sync_team_autoimport_skips_non_success_run(
    db_session, monkeypatch
) -> None:
    run, _integration, _config = _seed_run_with_config(
        db_session,
        status=SyncRunStatus.FAILED.value,
        sync_options={"auto_import_teams": True},
    )
    _patch_session(monkeypatch, db_session)
    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(
        team_autoimport,
        "run_team_autoimport",
        lambda **kwargs: captured.append(kwargs),
    )

    result = team_autoimport.run_post_sync_team_autoimport(str(run.id))

    assert result["status"] == "skipped"
    assert result["reason"] == "run_not_successful"
    assert captured == []


def test_post_sync_team_autoimport_skips_when_auto_import_disabled(
    db_session, monkeypatch
) -> None:
    run, _integration, _config = _seed_run_with_config(
        db_session,
        status=SyncRunStatus.SUCCESS.value,
        sync_options={},
    )
    _patch_session(monkeypatch, db_session)
    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(
        team_autoimport,
        "run_team_autoimport",
        lambda **kwargs: captured.append(kwargs),
    )

    result = team_autoimport.run_post_sync_team_autoimport(str(run.id))

    assert result["status"] == "skipped"
    assert result["reason"] == "auto_import_disabled"
    assert captured == []


def test_post_sync_team_autoimport_resolves_credentials_from_integration(
    db_session, monkeypatch
) -> None:
    """Credentials come from the run's Integration.credential_id (CHAOS-2647).

    The unit workers authenticate via ``integration.credential_id``; auto-import
    must use the SAME source, never the legacy ``SyncConfiguration.credential_id``
    row, so it does not split-brain against the sync that just ran.
    """
    credential = IntegrationCredential(
        org_id=_ORG,
        provider="github",
        name="default",
        credentials_encrypted=None,
        config={},
    )
    db_session.add(credential)
    db_session.flush()
    run, _integration, _config = _seed_run_with_config(
        db_session,
        status=SyncRunStatus.SUCCESS.value,
        sync_options={"auto_import_teams": True},
        credential_id=credential.id,
    )
    _patch_session(monkeypatch, db_session)
    import dev_health_ops.workers.task_utils as task_utils

    monkeypatch.setattr(
        task_utils,
        "_credential_mapping",
        lambda cred: {"token": "from-integration-cred"},
    )
    captured: list[dict[str, Any]] = []

    def _run_autoimport(**kwargs: Any) -> dict[str, Any]:
        captured.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(team_autoimport, "run_team_autoimport", _run_autoimport)

    team_autoimport.run_post_sync_team_autoimport(str(run.id))

    assert len(captured) == 1
    assert captured[0]["credentials"] == {"token": "from-integration-cred"}


def test_post_sync_team_autoimport_skips_when_integration_missing(
    db_session, monkeypatch
) -> None:
    """If the run's Integration row is gone, skip rather than authenticate with
    env credentials that may not match the synced integration (CHAOS-2647)."""
    orphan_integration_id = uuid.uuid4()
    config = SyncConfiguration(
        name="canonical-config",
        provider="github",
        org_id=_ORG,
        sync_targets=["work-items"],
        sync_options={"auto_import_teams": True},
        integration_id=orphan_integration_id,
        parent_id=None,
        planner_managed=True,
    )
    db_session.add(config)
    run = SyncRun(
        org_id=_ORG,
        integration_id=orphan_integration_id,
        triggered_by="manual",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunStatus.SUCCESS.value,
    )
    db_session.add(run)
    db_session.flush()
    _patch_session(monkeypatch, db_session)
    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(
        team_autoimport,
        "run_team_autoimport",
        lambda **kwargs: captured.append(kwargs),
    )

    result = team_autoimport.run_post_sync_team_autoimport(str(run.id))

    assert result["status"] == "skipped"
    assert result["reason"] == "integration_not_found"
    assert captured == []
