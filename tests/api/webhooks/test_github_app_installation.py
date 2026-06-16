from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import GithubAppInstallation, IntegrationCredential
from dev_health_ops.workers import system_webhooks
from dev_health_ops.workers.system_webhooks import _process_github_event
from tests._helpers import tables_of


@pytest.fixture
def session_maker(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "github-installation-webhook.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(
        engine, tables=tables_of(GithubAppInstallation, IntegrationCredential)
    )
    maker = sessionmaker(bind=engine, expire_on_commit=False)

    @contextmanager
    def session_override():
        session = maker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        session_override,
    )
    try:
        yield maker
    finally:
        engine.dispose()


def _payload(action: str, installation_id: int = 123) -> dict:
    return {
        "action": action,
        "installation": {
            "id": installation_id,
            "account": {"login": "full-chaos", "type": "Organization"},
        },
    }


def _installation(session_maker) -> GithubAppInstallation:
    with session_maker() as session:
        return session.execute(select(GithubAppInstallation)).scalar_one()


class FakeNoRowResult:
    def scalar_one_or_none(self):
        return None


def test_installation_webhook_upserts_and_tracks_transitions(session_maker):
    created = _process_github_event("installation", _payload("created"), None, None)
    assert created["processed"] is True
    installation = _installation(session_maker)
    assert installation.installation_id == 123
    assert installation.account_login == "full-chaos"
    assert installation.account_type == "Organization"
    assert installation.suspended_at is None

    suspended = _process_github_event("installation", _payload("suspend"), None, None)
    assert suspended["processed"] is True
    assert _installation(session_maker).suspended_at is not None

    unsuspended = _process_github_event(
        "installation", _payload("unsuspend"), None, None
    )
    assert unsuspended["processed"] is True
    assert _installation(session_maker).suspended_at is None


def test_installation_webhook_recovers_when_callback_created_row_concurrently(
    session_maker,
    monkeypatch,
):
    with session_maker() as session:
        installation = GithubAppInstallation()
        installation.installation_id = 123
        session.add(installation)
        session.commit()

    original_execute = Session.execute
    stale_select_consumed = False

    def stale_first_execute(self, *args, **kwargs):
        nonlocal stale_select_consumed
        if not stale_select_consumed:
            stale_select_consumed = True
            return FakeNoRowResult()
        return original_execute(self, *args, **kwargs)

    monkeypatch.setattr(Session, "execute", stale_first_execute)

    result = _process_github_event("installation", _payload("created"), None, None)

    assert result["processed"] is True
    installation = _installation(session_maker)
    assert installation.account_login == "full-chaos"
    assert installation.suspended_at is None


def test_installation_deleted_deactivates_matching_app_credential(session_maker):
    with session_maker() as session:
        installation = GithubAppInstallation()
        installation.installation_id = 999
        installation.org_id = "org-1"
        credential = IntegrationCredential(
            provider="github",
            name="github-app",
            org_id="org-1",
            credentials_encrypted="encrypted",
            is_active=True,
        )
        session.add_all([installation, credential])
        session.commit()

    deleted = _process_github_event(
        "installation", _payload("deleted", 999), None, None
    )
    assert deleted["processed"] is True

    with session_maker() as session:
        installation = session.execute(select(GithubAppInstallation)).scalar_one()
        credential = session.execute(select(IntegrationCredential)).scalar_one()
        assert installation.org_id == "org-1"
        assert installation.suspended_at is not None
        assert credential.is_active is False


def test_installation_db_failure_raises_for_retry(monkeypatch):
    @contextmanager
    def broken_session():
        raise RuntimeError("db unavailable")
        yield

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session_sync",
        broken_session,
    )

    with pytest.raises(RuntimeError, match="db unavailable"):
        _process_github_event("installation", _payload("suspend"), None, None)


def test_failed_delivery_is_not_recorded(monkeypatch):
    recorded: list[str] = []

    def failing_process(event_type, payload, org_id, repo_name):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(system_webhooks, "_is_duplicate_delivery", lambda p, d: False)
    monkeypatch.setattr(
        system_webhooks, "_record_delivery", lambda p, d: recorded.append(d)
    )
    monkeypatch.setattr(system_webhooks, "_process_github_event", failing_process)
    monkeypatch.setattr(
        system_webhooks.process_webhook_event,
        "retry",
        lambda exc, countdown: (_ for _ in ()).throw(exc),
    )

    with pytest.raises(RuntimeError, match="db unavailable"):
        getattr(system_webhooks.process_webhook_event, "run")(
            provider="github",
            event_type="installation",
            delivery_id="delivery-1",
            payload=_payload("deleted"),
            org_id=None,
            repo_name=None,
        )

    assert recorded == []
