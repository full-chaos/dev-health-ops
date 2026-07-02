from __future__ import annotations

import json
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.core.encryption import encrypt_value
from dev_health_ops.models import Base, Integration
from dev_health_ops.models.settings import IntegrationCredential, SyncConfiguration
from dev_health_ops.workers.team_drift_sync import _provider_scan_complete

_ORG = "team-drift-sync-org"


def test_provider_scan_complete_accepts_plain_success() -> None:
    assert _provider_scan_complete({"status": "success"})


def test_provider_scan_complete_rejects_truncated_or_warning_results() -> None:
    assert not _provider_scan_complete({"status": "success", "complete": False})
    assert not _provider_scan_complete({"status": "success", "warnings": ["bounded"]})
    assert not _provider_scan_complete({"status": "skipped"})


@contextmanager
def _session_ctx(session):
    yield session
    session.commit()


def test_configured_provider_syncs_resolves_credential_via_linked_integration(
    monkeypatch,
) -> None:
    """CHAOS-2762 regression: drift sync must authenticate off the linked
    ``Integration``'s credential (the sanctioned surface reached via
    ``SyncConfiguration.integration_id``) -- ``SyncConfiguration`` carries no
    credential column of its own to read instead.
    """
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-team-drift-sync-secret")
    import dev_health_ops.db as db
    from dev_health_ops.workers import team_drift_sync

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            credential = IntegrationCredential(
                provider="github",
                name="drift-cred",
                org_id=_ORG,
                credentials_encrypted=encrypt_value(json.dumps({"token": "tok-drift"})),
                config={},
                is_active=True,
            )
            session.add(credential)
            session.flush()

            integration = Integration(
                org_id=_ORG,
                provider="github",
                name="gh-integration",
                config={},
                is_active=True,
            )
            integration.credential_id = credential.id
            session.add(integration)
            session.flush()

            config = SyncConfiguration(
                name="drift-config",
                provider="github",
                org_id=_ORG,
                sync_targets=["git"],
                sync_options={"owner": "acme"},
                integration_id=integration.id,
            )
            session.add(config)
            session.commit()

            monkeypatch.setattr(
                db, "get_postgres_session_sync", lambda: _session_ctx(session)
            )

            configs = team_drift_sync._configured_provider_syncs(_ORG)
    finally:
        engine.dispose()

    assert len(configs) == 1
    assert configs[0].provider == "github"
    assert configs[0].credentials.get("token") == "tok-drift"


def test_configured_provider_syncs_falls_back_to_env_without_linked_integration(
    monkeypatch,
) -> None:
    """A config with no linked integration (legacy, pre-planner row) falls
    back to env credentials rather than erroring -- same behavior as before
    CHAOS-2762, just resolved through Integration instead of a stale column.
    """
    monkeypatch.setenv("GITHUB_TOKEN", "tok-env-fallback")
    import dev_health_ops.db as db
    from dev_health_ops.workers import team_drift_sync

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            config = SyncConfiguration(
                name="legacy-config",
                provider="github",
                org_id=_ORG,
                sync_targets=["git"],
                sync_options={"owner": "acme"},
            )
            session.add(config)
            session.commit()

            monkeypatch.setattr(
                db, "get_postgres_session_sync", lambda: _session_ctx(session)
            )

            configs = team_drift_sync._configured_provider_syncs(_ORG)
    finally:
        engine.dispose()

    assert len(configs) == 1
    assert configs[0].credentials.get("token") == "tok-env-fallback"
