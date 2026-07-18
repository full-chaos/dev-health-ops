"""Synchronous-session OAuth hydration for PagerDuty worker requests."""

import hashlib
from datetime import UTC, datetime
from typing import Any

import anyio
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from dev_health_ops.core.encryption import decrypt_value, encrypt_value
from dev_health_ops.db import get_postgres_session_sync
from dev_health_ops.models.settings import ProviderOAuthCredential
from dev_health_ops.providers.pagerduty.oauth import (
    READ_SCOPES,
    OAuthTokens,
    PagerDutyOAuthConfig,
)
from dev_health_ops.providers.pagerduty.oauth_lifecycle import (
    ClientCredentialsRequest,
    ClientCredentialsTokenCacheRegistry,
    get_client_credentials_access_token_keyed,
    get_valid_access_token,
)
from dev_health_ops.providers.pagerduty.oauth_storage import (
    OAuthRotationConflictError,
    VersionedOAuthTokens,
)

_REGISTRY = ClientCredentialsTokenCacheRegistry()


class _SyncSessionOAuthStore:
    """Adapt a sync SQLAlchemy session to the async OAuth renewal protocol."""

    def __init__(
        self,
        session: Session,
        org_id: str,
        credential_name: str,
        binding_id: str | None = None,
    ) -> None:
        self._session = session
        self._org_id = org_id
        self._credential_name = credential_name
        self._binding_id = binding_id

    async def get(self) -> VersionedOAuthTokens | None:
        """Return the current encrypted token payload without acquiring a lock."""
        credential = self._session.get(
            ProviderOAuthCredential,
            (self._org_id, "pagerduty", self._credential_name),
            populate_existing=True,
        )
        if credential is None:
            return None
        return self._versioned_tokens(credential)

    async def get_for_update(self) -> VersionedOAuthTokens | None:
        """Return and lock the current payload while a refresh is in progress."""
        statement = (
            select(ProviderOAuthCredential)
            .where(
                ProviderOAuthCredential.org_id == self._org_id,
                ProviderOAuthCredential.provider == "pagerduty",
                ProviderOAuthCredential.credential_name == self._credential_name,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        credential = self._session.execute(statement).scalar_one_or_none()
        if credential is None:
            return None
        return self._versioned_tokens(credential)

    async def rotate(
        self,
        current_version: int,
        tokens: OAuthTokens,
        *,
        expected_binding_id: str,
    ) -> int:
        """Persist a refresh only when its version and OAuth binding still match."""
        statement = (
            update(ProviderOAuthCredential)
            .where(
                ProviderOAuthCredential.org_id == self._org_id,
                ProviderOAuthCredential.provider == "pagerduty",
                ProviderOAuthCredential.credential_name == self._credential_name,
                ProviderOAuthCredential.version == current_version,
                ProviderOAuthCredential.binding_id == expected_binding_id,
            )
            .values(
                token_encrypted=encrypt_value(tokens.model_dump_json()),
                version=current_version + 1,
                expires_at=tokens.expires_at,
                granted_scopes=sorted(tokens.granted_scopes),
                has_refresh_token=bool(tokens.refresh_token),
                updated_at=datetime.now(UTC),
            )
            .returning(ProviderOAuthCredential.version)
        )
        version = self._session.execute(statement).scalar_one_or_none()
        if version is None:
            raise OAuthRotationConflictError("PagerDuty OAuth token rotation conflict")
        self._session.flush()
        return version

    async def delete(self) -> None:
        """Delete this encrypted credential without reading or logging its token."""
        statement = delete(ProviderOAuthCredential).where(
            ProviderOAuthCredential.org_id == self._org_id,
            ProviderOAuthCredential.provider == "pagerduty",
            ProviderOAuthCredential.credential_name == self._credential_name,
        )
        self._session.execute(statement)
        self._session.flush()

    def _versioned_tokens(
        self, credential: ProviderOAuthCredential
    ) -> VersionedOAuthTokens:
        if self._binding_id is not None and credential.binding_id != self._binding_id:
            raise OAuthRotationConflictError(
                "PagerDuty OAuth credential binding mismatch"
            )
        return VersionedOAuthTokens(
            OAuthTokens.model_validate_json(decrypt_value(credential.token_encrypted)),
            credential.version,
            credential.binding_id,
        )


def hydrate_pagerduty_credentials(
    mapping: dict[str, Any], *, org_id: str
) -> dict[str, Any]:
    """Copy a PagerDuty credential descriptor and attach its ephemeral access token."""
    result = mapping.copy()
    auth_mode = mapping.get("auth_mode")
    if auth_mode == "oauth":
        config = PagerDutyOAuthConfig.from_env()
        if config is None:
            raise ValueError("PagerDuty OAuth app is not configured")
        credential_name = mapping["oauth_credential_name"]
        with get_postgres_session_sync() as session:
            result["access_token"] = anyio.run(
                get_valid_access_token,
                _SyncSessionOAuthStore(
                    session,
                    org_id,
                    credential_name,
                    mapping["oauth_binding_id"],
                ),
                config,
            )
        return result
    if auth_mode == "client_credentials":
        config = PagerDutyOAuthConfig(
            client_id=mapping["client_id"],
            client_secret=mapping["client_secret"],
            redirect_uri="",
        )
        request = ClientCredentialsRequest(
            READ_SCOPES,
            mapping["subdomain"],
            mapping["region"],
        )
        key = (
            org_id,
            "client_credentials",
            READ_SCOPES,
            request.subdomain,
            request.region,
            hashlib.sha256(
                f"{config.client_id}:{config.client_secret}".encode()
            ).hexdigest(),
        )
        result["access_token"] = anyio.run(
            get_client_credentials_access_token_keyed,
            _REGISTRY,
            key,
            config,
            request,
        )
    return result
