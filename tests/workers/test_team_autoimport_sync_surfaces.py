from __future__ import annotations

import uuid
from contextlib import contextmanager
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


def test_backfill_reference_discovery_arms_ledger_and_returns_ledger_result(
    monkeypatch,
) -> None:
    """CHAOS-4498: _run_strict_reference_discovery_for_backfill arms the
    shared sync_run_reference_discoveries ledger (via
    sync.planner.seed_reference_discovery_run) and waits for it
    (await_reference_discovery_terminal) instead of calling
    run_team_autoimport_strict directly -- for jira exactly like every
    other provider, since the native-vs-bridge choice is made entirely by
    TeamCatalogDiscoveryExecutor on the Go side, not by this function."""
    seed_calls: list[dict[str, Any]] = []

    def fake_seed(session: Any, **kwargs: Any) -> str:
        seed_calls.append(kwargs)
        return "22222222-2222-2222-2222-222222222222"

    monkeypatch.setattr(
        "dev_health_ops.sync.planner.seed_reference_discovery_run", fake_seed
    )
    monkeypatch.setattr(
        backfill_runner,
        "get_postgres_session_sync",
        lambda: _session_ctx_noop(),
    )
    monkeypatch.setattr(
        backfill_runner,
        "await_reference_discovery_terminal",
        lambda sync_run_id, **kwargs: {
            "outcome": "success",
            "sync_run_id": sync_run_id,
            "result": {"status": "success", "teams_imported": 1},
        },
    )

    result = backfill_runner._run_strict_reference_discovery_for_backfill(
        provider="jira",
        org_id="org-1",
        integration_id="integration-1",
        sync_config_id="cfg-1",
        triggered_by="operator_backfill",
    )

    assert result == {"status": "success", "teams_imported": 1}
    assert len(seed_calls) == 1
    assert seed_calls[0] == {
        "integration_id": "integration-1",
        "org_id": "org-1",
        "triggered_by": "operator_backfill",
    }


@contextmanager
def _session_ctx_noop():
    class _Session:
        def commit(self) -> None:
            pass

    yield _Session()


def test_backfill_reference_discovery_raises_on_failed_ledger_outcome(
    monkeypatch,
) -> None:
    """CHAOS-4498: a failed ledger outcome raises with the row's reason --
    never falls back to calling the Python populator directly."""
    monkeypatch.setattr(
        "dev_health_ops.sync.planner.seed_reference_discovery_run",
        lambda session, **kwargs: "22222222-2222-2222-2222-222222222222",
    )
    monkeypatch.setattr(
        backfill_runner, "get_postgres_session_sync", lambda: _session_ctx_noop()
    )
    monkeypatch.setattr(
        backfill_runner,
        "await_reference_discovery_terminal",
        lambda sync_run_id, **kwargs: {
            "outcome": "failed",
            "sync_run_id": sync_run_id,
            "reason": "missing Jira credentials",
        },
    )

    with pytest.raises(ValueError, match="missing Jira credentials"):
        backfill_runner._run_strict_reference_discovery_for_backfill(
            provider="jira",
            org_id="org-1",
            integration_id="integration-1",
            sync_config_id="cfg-1",
            triggered_by="operator_backfill",
        )


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


def test_post_sync_team_autoimport_logs_warning_on_roster_preservation_failure(
    db_session, monkeypatch, caplog
) -> None:
    """CHAOS-4323 (team-lead 08-26): a roster-preservation-read failure must
    be visible in the post-sync log at WARNING, not just as a field in the
    returned summary dict nobody reads -- a degraded run must not look
    identical to a clean one in the logs any more than in the metrics."""
    import logging

    run, _integration, _config = _seed_run_with_config(
        db_session,
        status=SyncRunStatus.SUCCESS.value,
        sync_options={"auto_import_teams": True},
    )
    _patch_session(monkeypatch, db_session)
    monkeypatch.setattr(
        team_autoimport,
        "run_team_autoimport",
        lambda **kwargs: {"status": "success", "roster_preservation_failed": True},
    )

    with caplog.at_level(
        logging.WARNING, logger="dev_health_ops.workers.team_autoimport"
    ):
        result = team_autoimport.run_post_sync_team_autoimport(str(run.id))

    assert result["status"] == "dispatched"
    assert any(
        "could not confirm" in record.message and str(run.id) in record.message
        for record in caplog.records
    )


def test_post_sync_team_autoimport_no_warning_on_clean_success(
    db_session, monkeypatch, caplog
) -> None:
    import logging

    run, _integration, _config = _seed_run_with_config(
        db_session,
        status=SyncRunStatus.SUCCESS.value,
        sync_options={"auto_import_teams": True},
    )
    _patch_session(monkeypatch, db_session)
    monkeypatch.setattr(
        team_autoimport,
        "run_team_autoimport",
        lambda **kwargs: {"status": "success"},
    )

    with caplog.at_level(
        logging.WARNING, logger="dev_health_ops.workers.team_autoimport"
    ):
        team_autoimport.run_post_sync_team_autoimport(str(run.id))

    assert not any("could not confirm" in record.message for record in caplog.records)


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


@pytest.mark.parametrize(
    "sync_options",
    [
        {"auto_import_projects": True},
        {"auto_import_members": True},
        {
            "auto_import_teams": True,
            "auto_import_projects": False,
            "auto_import_members": False,
        },
    ],
)
def test_post_sync_team_autoimport_dispatches_when_any_single_category_selected(
    db_session, monkeypatch, sync_options
) -> None:
    """CHAOS-4323: the task-level gate is an OR across all three -- selecting
    ONLY one category (not necessarily teams) must still run
    run_team_autoimport, which is where each flag is honoured independently."""
    run, _integration, config = _seed_run_with_config(
        db_session,
        status=SyncRunStatus.SUCCESS.value,
        sync_options=sync_options,
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
    assert captured[0]["scope"]["sync_options"] == sync_options


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


def test_post_sync_team_autoimport_uses_run_stamped_credential_after_repoint(
    db_session, monkeypatch
) -> None:
    """CHAOS-2755 freeze reaches post-sync attribution: a mid-run repoint of
    ``Integration.credential_id`` must not switch auto-import onto a different
    credential than the units that produced the synced data (PR #1109 review M1)."""
    from dev_health_ops.credentials.fingerprint import (
        AUTH_SOURCE_INTEGRATION_CREDENTIAL,
    )

    cred_a = IntegrationCredential(
        org_id=_ORG,
        provider="github",
        name="stamped",
        credentials_encrypted=None,
        config={},
    )
    cred_b = IntegrationCredential(
        org_id=_ORG,
        provider="github",
        name="repointed",
        credentials_encrypted=None,
        config={},
    )
    db_session.add_all([cred_a, cred_b])
    db_session.flush()
    run, integration, _config = _seed_run_with_config(
        db_session,
        status=SyncRunStatus.SUCCESS.value,
        sync_options={"auto_import_teams": True},
        credential_id=cred_a.id,
    )
    # Mimic the planner stamp (fingerprint None -> verification no-op), then
    # repoint the mutable integration pointer mid-run.
    run.credential_id = cred_a.id
    run.auth_source = AUTH_SOURCE_INTEGRATION_CREDENTIAL
    run.credential_fingerprint = None
    integration.credential_id = cred_b.id
    db_session.flush()

    _patch_session(monkeypatch, db_session)
    import dev_health_ops.workers.task_utils as task_utils

    monkeypatch.setattr(
        task_utils,
        "_credential_mapping",
        lambda cred: {"token": f"tok-{cred.name}"},
    )
    captured: list[dict[str, Any]] = []

    def _run_autoimport(**kwargs: Any) -> dict[str, Any]:
        captured.append(kwargs)
        return {"status": "success"}

    monkeypatch.setattr(team_autoimport, "run_team_autoimport", _run_autoimport)

    team_autoimport.run_post_sync_team_autoimport(str(run.id))

    assert len(captured) == 1
    # Frozen on the stamped credential, NOT the repointed one.
    assert captured[0]["credentials"] == {"token": "tok-stamped"}


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
