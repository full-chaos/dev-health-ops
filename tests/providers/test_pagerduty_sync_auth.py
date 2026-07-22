from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from dev_health_ops.core.encryption import decrypt_value, encrypt_value
from dev_health_ops.credentials.fingerprint import credential_fingerprint
from dev_health_ops.models.integrations import Integration
from dev_health_ops.models.settings import (
    IntegrationCredential,
    ProviderOAuthCredential,
)
from dev_health_ops.providers.pagerduty.oauth import (
    READ_SCOPES,
    OAuthTokens,
    PagerDutyOAuthConfig,
)
from dev_health_ops.providers.pagerduty.oauth_lifecycle import (
    ClientCredentialsTokenCacheRegistry,
)
from dev_health_ops.providers.pagerduty.oauth_storage import OAuthRotationConflictError
from dev_health_ops.providers.pagerduty.sync_auth import hydrate_pagerduty_credentials
from dev_health_ops.workers.sync_bootstrap import resolve_run_auth


@pytest.fixture(autouse=True)
def encryption_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine("sqlite://")
    ProviderOAuthCredential.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_factory() as database_session:
        yield database_session
    engine.dispose()


def tokens(
    access_token: str,
    *,
    refresh_token: str | None = "refresh-token",
    expires_at: datetime | None = None,
    granted_scopes: frozenset[str] = READ_SCOPES,
) -> OAuthTokens:
    return OAuthTokens(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at or datetime.now(UTC) + timedelta(hours=1),
        granted_scopes=granted_scopes,
    )


def seed_credential(
    session: Session,
    oauth_tokens: OAuthTokens,
    *,
    binding_id: str = "binding-id",
    credential_name: str = "primary",
) -> None:
    now = datetime.now(UTC)
    session.add(
        ProviderOAuthCredential(
            org_id="oauth-org",
            provider="pagerduty",
            credential_name=credential_name,
            token_encrypted=encrypt_value(oauth_tokens.model_dump_json()),
            version=1,
            created_at=now,
            updated_at=now,
            binding_id=binding_id,
            expires_at=oauth_tokens.expires_at,
            granted_scopes=sorted(oauth_tokens.granted_scopes),
            has_refresh_token=oauth_tokens.refresh_token is not None,
        )
    )
    session.commit()


def patch_sync_session(monkeypatch: pytest.MonkeyPatch, session: Session) -> None:
    @contextmanager
    def postgres_session() -> Iterator[Session]:
        try:
            yield session
            session.commit()
        except BaseException:
            session.rollback()
            raise

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.sync_auth.get_postgres_session_sync",
        postgres_session,
    )


def test_hydrate_oauth_uses_stored_non_expired_token_without_mutating_mapping(
    monkeypatch: pytest.MonkeyPatch, session: Session
) -> None:
    seed_credential(session, tokens("stored-token"))
    patch_sync_session(monkeypatch, session)
    monkeypatch.setenv("PAGER_DUTY_CLIENT_ID", "client-id")
    mapping = {
        "auth_mode": "oauth",
        "oauth_credential_name": "primary",
        "oauth_binding_id": "binding-id",
        "subdomain": "acme",
        "region": "us",
    }

    hydrated = hydrate_pagerduty_credentials(mapping, org_id="oauth-org")

    assert hydrated == {**mapping, "access_token": "stored-token"}
    assert hydrated is not mapping
    assert mapping == {
        "auth_mode": "oauth",
        "oauth_credential_name": "primary",
        "oauth_binding_id": "binding-id",
        "subdomain": "acme",
        "region": "us",
    }


def test_hydrate_oauth_refreshes_due_token_and_rotates_bound_credential(
    monkeypatch: pytest.MonkeyPatch, session: Session
) -> None:
    seed_credential(
        session,
        tokens("stale-token", expires_at=datetime.now(UTC) - timedelta(seconds=1)),
    )
    patch_sync_session(monkeypatch, session)
    monkeypatch.setenv("PAGER_DUTY_CLIENT_ID", "client-id")

    async def refresh(_: PagerDutyOAuthConfig, __: str) -> OAuthTokens:
        return tokens("refreshed-token")

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.refresh_tokens", refresh
    )
    mapping = {
        "auth_mode": "oauth",
        "oauth_credential_name": "primary",
        "oauth_binding_id": "binding-id",
        "subdomain": "acme",
        "region": "us",
    }

    hydrated = hydrate_pagerduty_credentials(mapping, org_id="oauth-org")

    session.expire_all()
    stored = session.get(ProviderOAuthCredential, ("oauth-org", "pagerduty", "primary"))
    assert hydrated["access_token"] == "refreshed-token"
    assert stored is not None
    assert stored.version == 2
    assert stored.binding_id == "binding-id"
    assert OAuthTokens.model_validate_json(
        decrypt_value(stored.token_encrypted)
    ).access_token == ("refreshed-token")


def test_hydrate_oauth_rejects_a_reconnected_credential_binding(
    monkeypatch: pytest.MonkeyPatch, session: Session
) -> None:
    seed_credential(session, tokens("stored-token"))
    patch_sync_session(monkeypatch, session)
    monkeypatch.setenv("PAGER_DUTY_CLIENT_ID", "client-id")
    mapping = {
        "auth_mode": "oauth",
        "oauth_credential_name": "primary",
        "oauth_binding_id": "stale-binding-id",
        "subdomain": "acme",
        "region": "us",
    }

    with pytest.raises(OAuthRotationConflictError, match="binding mismatch"):
        hydrate_pagerduty_credentials(mapping, org_id="oauth-org")


def test_hydrate_oauth_rejects_a_token_missing_operational_scopes(
    monkeypatch: pytest.MonkeyPatch, session: Session
) -> None:
    # Given: a persisted OAuth token that lacks one required operational scope.
    seed_credential(
        session, tokens("stored-token", granted_scopes=frozenset({"Users.read"}))
    )
    patch_sync_session(monkeypatch, session)
    monkeypatch.setenv("PAGER_DUTY_CLIENT_ID", "client-id")

    # When: a sync worker hydrates its OAuth descriptor.
    with pytest.raises(ValueError, match="missing required read scopes"):
        hydrate_pagerduty_credentials(
            {
                "auth_mode": "oauth",
                "oauth_credential_name": "primary",
                "oauth_binding_id": "binding-id",
                "subdomain": "acme",
                "region": "us",
            },
            org_id="oauth-org",
        )

    # Then: the worker fails before any provider import can begin.


def test_hydrate_client_credentials_mints_machine_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def client_credentials(
        config: PagerDutyOAuthConfig,
        *,
        scopes: set[str],
        subdomain: str,
        region: str,
    ) -> OAuthTokens:
        assert config.client_id == "self-hosted-id"
        assert config.client_secret == "self-hosted-secret"
        assert subdomain == "acme"
        assert region == "eu"
        return tokens("machine-token", refresh_token=None).model_copy(
            update={"granted_scopes": frozenset(scopes)}
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.client_credentials",
        client_credentials,
    )
    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.sync_auth._REGISTRY",
        ClientCredentialsTokenCacheRegistry(),
    )
    mapping = {
        "auth_mode": "client_credentials",
        "client_id": "self-hosted-id",
        "client_secret": "self-hosted-secret",
        "subdomain": "acme",
        "region": "eu",
    }

    hydrated = hydrate_pagerduty_credentials(mapping, org_id="client-org")

    assert hydrated == {**mapping, "access_token": "machine-token"}
    assert hydrated is not mapping


def test_hydrate_client_credentials_rejects_a_partial_scope_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: PagerDuty grants a machine token without the requested operational scopes.
    async def client_credentials(
        _: PagerDutyOAuthConfig,
        *,
        scopes: set[str],
        subdomain: str,
        region: str,
    ) -> OAuthTokens:
        assert scopes == set(READ_SCOPES)
        assert subdomain == "acme"
        assert region == "us"
        return tokens(
            "partial-machine-token",
            refresh_token=None,
            granted_scopes=frozenset({"Users.read"}),
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.client_credentials",
        client_credentials,
    )
    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.sync_auth._REGISTRY",
        ClientCredentialsTokenCacheRegistry(),
    )

    # When: worker hydration asks for a client-credentials token.
    with pytest.raises(
        OAuthRotationConflictError, match="missing required read scopes"
    ):
        hydrate_pagerduty_credentials(
            {
                "auth_mode": "client_credentials",
                "client_id": "self-hosted-id",
                "client_secret": "self-hosted-secret",
                "subdomain": "acme",
                "region": "us",
            },
            org_id="client-org",
        )

    # Then: a partial grant cannot hydrate a worker credential.


def test_hydrate_api_token_returns_a_copied_mapping_without_database_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @contextmanager
    def fail_if_opened() -> Iterator[Session]:
        raise AssertionError("api-token credentials must not open an OAuth session")
        yield

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.sync_auth.get_postgres_session_sync",
        fail_if_opened,
    )
    mapping = {
        "auth_mode": "api_token",
        "api_token": "api-token",
        "subdomain": "acme",
        "region": "us",
    }

    hydrated = hydrate_pagerduty_credentials(mapping, org_id="api-token-org")

    assert hydrated == mapping
    assert hydrated is not mapping


def test_resolve_run_auth_hydrates_pagerduty_after_strict_fingerprint_verification(
    monkeypatch: pytest.MonkeyPatch, session: Session
) -> None:
    descriptor = {
        "auth_mode": "oauth",
        "oauth_credential_name": "primary",
        "oauth_binding_id": "binding-id",
        "subdomain": "acme",
        "region": "us",
    }
    credential = IntegrationCredential(
        provider="pagerduty",
        name="primary",
        org_id="oauth-org",
        credentials_encrypted=encrypt_value(json.dumps(descriptor)),
        config={},
        is_active=True,
    )
    session.add(credential)
    session.flush()
    integration = SimpleNamespace(
        id=uuid4(), org_id="oauth-org", credential_id=credential.id
    )
    run = SimpleNamespace(
        auth_source="integration_credential",
        credential_id=credential.id,
        credential_fingerprint=credential_fingerprint(
            descriptor,
            credential_id=str(credential.id),
            integration_id=str(integration.id),
        ),
    )

    def hydrate(mapping: dict[str, object], *, org_id: str) -> dict[str, object]:
        assert mapping == descriptor
        assert org_id == "oauth-org"
        return {**mapping, "access_token": "hydrated-access-token"}

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.sync_auth.hydrate_pagerduty_credentials",
        hydrate,
    )
    monkeypatch.setenv("SYNC_RUN_AUTH_STRICT", "1")

    credential_id, resolved = resolve_run_auth(
        session,
        run=run,
        integration=integration,
        provider="pagerduty",
        error_label="PagerDuty strict hydration",
    )

    assert credential_id == credential.id
    assert resolved == {**descriptor, "access_token": "hydrated-access-token"}
    assert run.credential_fingerprint == credential_fingerprint(
        descriptor,
        credential_id=str(credential.id),
        integration_id=str(integration.id),
    )


def test_resolve_run_auth_keeps_non_pagerduty_credentials_unhydrated(
    session: Session,
) -> None:
    descriptor = {"token": "github-token"}
    credential = IntegrationCredential(
        provider="github",
        name="default",
        org_id="github-org",
        credentials_encrypted=encrypt_value(json.dumps(descriptor)),
        config={},
        is_active=True,
    )
    session.add(credential)
    session.flush()
    integration = SimpleNamespace(
        id=uuid4(), org_id="github-org", credential_id=credential.id
    )
    run = SimpleNamespace(
        auth_source="integration_credential",
        credential_id=credential.id,
        credential_fingerprint=credential_fingerprint(
            descriptor,
            credential_id=str(credential.id),
            integration_id=str(integration.id),
        ),
    )

    credential_id, resolved = resolve_run_auth(
        session,
        run=run,
        integration=integration,
        provider="github",
        error_label="GitHub regression",
    )

    assert credential_id == credential.id
    assert resolved == descriptor


def test_resolve_run_auth_rejects_pagerduty_environment_fallback(
    monkeypatch: pytest.MonkeyPatch, session: Session
) -> None:
    # Given: a PagerDuty integration that has no persisted credential.
    integration = SimpleNamespace(
        id=uuid4(), org_id="pagerduty-org", credential_id=None
    )
    run = SimpleNamespace(auth_source=None)
    monkeypatch.setattr(
        "dev_health_ops.workers.task_utils._resolve_env_credentials",
        lambda _: (_ for _ in ()).throw(
            AssertionError("must not read deployment auth")
        ),
    )

    # When: the worker resolves the legacy mutable auth path.
    with pytest.raises(ValueError, match="active organization-scoped credential"):
        resolve_run_auth(
            session,
            run=run,
            integration=integration,
            provider="pagerduty",
            error_label="PagerDuty missing credential",
        )

    # Then: deployment environment credentials cannot supply tenant sync auth.


def test_resolve_run_auth_rejects_an_inactive_pagerduty_credential(
    session: Session,
) -> None:
    # Given: an integration references an inactive credential from its own org.
    credential = IntegrationCredential(
        provider="pagerduty",
        name="inactive",
        org_id="pagerduty-org",
        credentials_encrypted=encrypt_value(
            json.dumps(
                {
                    "auth_mode": "api_token",
                    "api_token": "inactive-token",
                    "subdomain": "acme",
                    "region": "us",
                }
            )
        ),
        config={},
        is_active=False,
    )
    session.add(credential)
    session.flush()
    integration = SimpleNamespace(
        id=uuid4(), org_id="pagerduty-org", credential_id=credential.id
    )
    run = SimpleNamespace(auth_source=None)

    # When: the worker resolves credentials for the PagerDuty unit.
    with pytest.raises(ValueError, match="active organization-scoped credential"):
        resolve_run_auth(
            session,
            run=run,
            integration=integration,
            provider="pagerduty",
            error_label="PagerDuty inactive credential",
        )

    # Then: no inactive credential can hydrate a sync unit.


def test_planner_rejects_pagerduty_sync_without_a_persisted_credential(
    session: Session,
) -> None:
    # Given: a PagerDuty integration whose target configuration is malformed.
    integration = Integration(
        org_id="pagerduty-org",
        provider="pagerduty",
        name="PagerDuty operational",
        config={},
        credential_id=None,
    )
    session.add(integration)
    session.flush()
    from dev_health_ops.sync.planner import _resolve_credential_stamp

    # When: the planner tries to freeze the run credential.
    with pytest.raises(ValueError, match="active organization-scoped credential"):
        _resolve_credential_stamp(session, integration)

    # Then: a legacy config is disabled instead of silently using deployment auth.


def test_hydrate_client_credentials_isolates_cache_by_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    minted_secrets: list[str | None] = []

    async def client_credentials(
        config: PagerDutyOAuthConfig,
        *,
        scopes: set[str],
        subdomain: str,
        region: str,
    ) -> OAuthTokens:
        minted_secrets.append(config.client_secret)
        return tokens(f"machine-{config.client_secret}", refresh_token=None).model_copy(
            update={"granted_scopes": frozenset(scopes)}
        )

    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.oauth_lifecycle.client_credentials",
        client_credentials,
    )
    monkeypatch.setattr(
        "dev_health_ops.providers.pagerduty.sync_auth._REGISTRY",
        ClientCredentialsTokenCacheRegistry(),
    )
    base = {
        "auth_mode": "client_credentials",
        "client_id": "self-hosted-id",
        "subdomain": "acme",
        "region": "eu",
    }

    first = hydrate_pagerduty_credentials(
        {**base, "client_secret": "secret-a"}, org_id="client-org"
    )
    second = hydrate_pagerduty_credentials(
        {**base, "client_secret": "secret-b"}, org_id="client-org"
    )
    first_again = hydrate_pagerduty_credentials(
        {**base, "client_secret": "secret-a"}, org_id="client-org"
    )

    # Different secrets never share a cache entry; a repeated secret is cached.
    assert first["access_token"] == "machine-secret-a"
    assert second["access_token"] == "machine-secret-b"
    assert first_again["access_token"] == "machine-secret-a"
    assert minted_secrets == ["secret-a", "secret-b"]
