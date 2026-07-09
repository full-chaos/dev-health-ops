from __future__ import annotations

import asyncio
import json
import uuid
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

            result = team_drift_sync._configured_provider_syncs(_ORG)
    finally:
        engine.dispose()

    assert result.skipped == []
    assert len(result.configs) == 1
    assert result.configs[0].provider == "github"
    assert result.configs[0].credentials.get("token") == "tok-drift"


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

            result = team_drift_sync._configured_provider_syncs(_ORG)
    finally:
        engine.dispose()

    assert result.skipped == []
    assert len(result.configs) == 1
    assert result.configs[0].credentials.get("token") == "tok-env-fallback"


def test_configured_provider_syncs_fails_closed_when_credential_inactive(
    monkeypatch,
) -> None:
    """CHAOS-2762 planner-parity regression (codex finding #1): an inactive
    credential must fail closed (the config is dropped), never silently fall
    back to env auth. ``sync/planner.py``'s ``_resolve_credential_stamp``
    raises on an inactive credential rather than treating it as absent --
    falling back here would let this worker authenticate where the planner
    would reject outright.
    """
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-team-drift-sync-secret")
    monkeypatch.setenv("GITHUB_TOKEN", "tok-env-must-not-be-used")
    import dev_health_ops.db as db
    from dev_health_ops.workers import team_drift_sync

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            credential = IntegrationCredential(
                provider="github",
                name="inactive-cred",
                org_id=_ORG,
                credentials_encrypted=encrypt_value(
                    json.dumps({"token": "tok-inactive"})
                ),
                config={},
                is_active=False,
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

            result = team_drift_sync._configured_provider_syncs(_ORG)
    finally:
        engine.dispose()

    assert result.configs == []
    assert len(result.skipped) == 1
    assert result.skipped[0].provider == "github"
    assert result.skipped[0].reason == team_drift_sync._SKIP_REASON_CREDENTIAL_INACTIVE


def test_configured_provider_syncs_fails_closed_when_credential_missing(
    monkeypatch,
) -> None:
    """CHAOS-2762 planner-parity regression (codex finding #1): a stamped
    ``credential_id`` with no matching row must fail closed, matching the
    planner's "Credential not found" fail-fast rather than falling back to
    env auth.
    """
    monkeypatch.setenv("GITHUB_TOKEN", "tok-env-must-not-be-used")
    import dev_health_ops.db as db
    from dev_health_ops.workers import team_drift_sync

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            integration = Integration(
                org_id=_ORG,
                provider="github",
                name="gh-integration",
                config={},
                is_active=True,
            )
            # Points at a credential that was never created (deleted, or a
            # stale/corrupt reference) -- the FK is deliberately unenforced.
            missing_credential_id = uuid.uuid4()
            integration.credential_id = missing_credential_id
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

            result = team_drift_sync._configured_provider_syncs(_ORG)
    finally:
        engine.dispose()

    assert result.configs == []
    assert len(result.skipped) == 1
    assert result.skipped[0].reason == team_drift_sync._SKIP_REASON_CREDENTIAL_NOT_FOUND


def test_configured_provider_syncs_fails_closed_when_credential_cross_org(
    monkeypatch,
) -> None:
    """CHAOS-2762 planner-parity regression (codex finding #1, cross-org
    case): a credential that exists but belongs to a DIFFERENT org must never
    be used, and must not silently fall back to env auth either -- mirrors
    the planner's org-scoped credential lookup
    (``IntegrationCredential.org_id == integration.org_id``).
    """
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-team-drift-sync-secret")
    monkeypatch.setenv("GITHUB_TOKEN", "tok-env-must-not-be-used")
    import dev_health_ops.db as db
    from dev_health_ops.workers import team_drift_sync

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            other_org_credential = IntegrationCredential(
                provider="github",
                name="other-org-cred",
                org_id="other-org",
                credentials_encrypted=encrypt_value(
                    json.dumps({"token": "tok-other-org"})
                ),
                config={},
                is_active=True,
            )
            session.add(other_org_credential)
            session.flush()

            integration = Integration(
                org_id=_ORG,
                provider="github",
                name="gh-integration",
                config={},
                is_active=True,
            )
            integration.credential_id = other_org_credential.id
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

            result = team_drift_sync._configured_provider_syncs(_ORG)
    finally:
        engine.dispose()

    assert result.configs == []
    assert len(result.skipped) == 1
    assert result.skipped[0].reason == team_drift_sync._SKIP_REASON_CREDENTIAL_NOT_FOUND


def test_configured_provider_syncs_fails_closed_when_integration_cross_org(
    monkeypatch,
) -> None:
    """CHAOS-2762 planner-parity regression (codex finding #1, cross-org
    case): a ``SyncConfiguration.integration_id`` pointing at an ``Integration``
    belonging to a DIFFERENT org (corrupt data / manual tampering) must never
    resolve a credential -- it fails closed rather than falling back to env
    auth, matching the planner's org-scoped ``_load_integration``.
    """
    monkeypatch.setenv("GITHUB_TOKEN", "tok-env-must-not-be-used")
    import dev_health_ops.db as db
    from dev_health_ops.workers import team_drift_sync

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            other_org_integration = Integration(
                org_id="other-org",
                provider="github",
                name="other-org-integration",
                config={},
                is_active=True,
            )
            session.add(other_org_integration)
            session.flush()

            config = SyncConfiguration(
                name="drift-config",
                provider="github",
                org_id=_ORG,
                sync_targets=["git"],
                sync_options={"owner": "acme"},
                integration_id=other_org_integration.id,
            )
            session.add(config)
            session.commit()

            monkeypatch.setattr(
                db, "get_postgres_session_sync", lambda: _session_ctx(session)
            )

            result = team_drift_sync._configured_provider_syncs(_ORG)
    finally:
        engine.dispose()

    assert result.configs == []
    assert len(result.skipped) == 1
    assert (
        result.skipped[0].reason == team_drift_sync._SKIP_REASON_INTEGRATION_NOT_FOUND
    )


def test_configured_provider_syncs_fails_closed_when_credential_provider_mismatch(
    monkeypatch,
) -> None:
    """CHAOS-2762 planner-parity regression (codex finding #1): a credential
    that exists, is active, and belongs to the SAME org but was provisioned
    for a DIFFERENT provider must never be used for this config's provider --
    it fails closed with a distinct, specific reason rather than silently
    substituting env auth.
    """
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-team-drift-sync-secret")
    monkeypatch.setenv("GITHUB_TOKEN", "tok-env-must-not-be-used")
    import dev_health_ops.db as db
    from dev_health_ops.workers import team_drift_sync

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            gitlab_credential = IntegrationCredential(
                provider="gitlab",
                name="gitlab-cred",
                org_id=_ORG,
                credentials_encrypted=encrypt_value(
                    json.dumps({"token": "tok-gitlab"})
                ),
                config={},
                is_active=True,
            )
            session.add(gitlab_credential)
            session.flush()

            # An Integration whose provider is "github" but whose stamped
            # credential_id points at a "gitlab" credential -- a data
            # inconsistency that should never actually arise, but the check
            # must catch it rather than trust the linkage blindly.
            integration = Integration(
                org_id=_ORG,
                provider="github",
                name="gh-integration",
                config={},
                is_active=True,
            )
            integration.credential_id = gitlab_credential.id
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

            result = team_drift_sync._configured_provider_syncs(_ORG)
    finally:
        engine.dispose()

    assert result.configs == []
    assert len(result.skipped) == 1
    assert (
        result.skipped[0].reason
        == team_drift_sync._SKIP_REASON_CREDENTIAL_PROVIDER_MISMATCH
    )


def test_sync_team_drift_async_surfaces_all_skipped_configs_in_result(
    monkeypatch,
) -> None:
    """Codex re-pass regression: when EVERY configured provider is skipped by
    the fail-closed auth check, the task result must show it.

    Before this fix, ``_sync_team_drift_async`` returned
    ``status: "success"`` with ``providers_attempted: 0`` and nothing else --
    a fleet-wide credential outage (every config's linked credential
    deactivated/deleted) would read as a clean, complete success unless
    someone thought to grep worker logs. ``configs_skipped`` /
    ``configs_skipped_count`` now make that outage visible in the result
    itself, and a WARNING aggregate line is logged.

    Isolates the aggregation/surfacing logic in ``_sync_team_drift_async``
    from the DB-query logic in ``_configured_provider_syncs`` (covered by the
    tests above) by monkeypatching the latter directly, and fakes
    ``ClickHouseStore`` so no real ClickHouse connection is needed -- with
    zero configs, ``project_team_rows_with_store`` is never called anyway.
    """
    import dev_health_ops.storage.clickhouse as clickhouse_module
    from dev_health_ops.workers import team_drift_sync

    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://test:9000/test")

    skipped = [
        team_drift_sync._SkippedConfig(
            config_id="config-1",
            provider="github",
            reason=team_drift_sync._SKIP_REASON_CREDENTIAL_INACTIVE,
        ),
        team_drift_sync._SkippedConfig(
            config_id="config-2",
            provider="gitlab",
            reason=team_drift_sync._SKIP_REASON_CREDENTIAL_NOT_FOUND,
        ),
    ]
    monkeypatch.setattr(
        team_drift_sync,
        "_configured_provider_syncs",
        lambda org_id: team_drift_sync._ConfiguredProviderSyncs(
            configs=[], skipped=skipped
        ),
    )

    class _FakeClickHouseStore:
        def __init__(self, conn_string, settings=None):
            self.conn_string = conn_string
            self.org_id: str | None = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(clickhouse_module, "ClickHouseStore", _FakeClickHouseStore)

    result = asyncio.run(team_drift_sync._sync_team_drift_async(org_id="org-x"))

    assert result["status"] == "success"
    assert result["providers_attempted"] == 0
    assert result["configs_skipped_count"] == 2
    assert result["configs_skipped"] == [
        {
            "config_id": "config-1",
            "provider": "github",
            "reason": "credential_inactive",
        },
        {
            "config_id": "config-2",
            "provider": "gitlab",
            "reason": "credential_not_found_or_cross_org",
        },
    ]


def test_sync_team_drift_async_configs_skipped_empty_when_nothing_skipped(
    monkeypatch,
) -> None:
    """``configs_skipped`` is always present (an empty list, not a missing
    key) when nothing was skipped, so callers can rely on the key existing
    unconditionally.
    """
    import dev_health_ops.storage.clickhouse as clickhouse_module
    from dev_health_ops.workers import team_drift_sync

    monkeypatch.setenv("CLICKHOUSE_URI", "clickhouse://test:9000/test")

    monkeypatch.setattr(
        team_drift_sync,
        "_configured_provider_syncs",
        lambda org_id: team_drift_sync._ConfiguredProviderSyncs(configs=[], skipped=[]),
    )

    class _FakeClickHouseStore:
        def __init__(self, conn_string, settings=None):
            self.org_id: str | None = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(clickhouse_module, "ClickHouseStore", _FakeClickHouseStore)

    result = asyncio.run(team_drift_sync._sync_team_drift_async(org_id="org-x"))

    assert result["status"] == "success"
    assert result["configs_skipped"] == []
    assert result["configs_skipped_count"] == 0
