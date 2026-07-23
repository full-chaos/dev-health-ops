"""Integration credentials service.

Stores per-provider credentials encrypted at rest. Decryption happens on
read; the test-connection result is tracked alongside the credential.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.core.encryption import decrypt_value, encrypt_value
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.sync.error_sanitize import sanitize_error_text

from ._helpers import _normalize_credential_keys

logger = logging.getLogger(__name__)

_PAGERDUTY_FORBIDDEN_TOKEN_KEYS: Final = frozenset(
    {"access_token", "refresh_token", "expires_at"}
)
_PAGERDUTY_DESCRIPTOR_KEYS: Final = {
    "oauth": frozenset(
        {
            "auth_mode",
            "oauth_credential_name",
            "oauth_binding_id",
            "subdomain",
            "region",
            "account_id",
        }
    ),
    "client_credentials": frozenset(
        {"auth_mode", "client_id", "client_secret", "subdomain", "region"}
    ),
    "api_token": frozenset({"auth_mode", "api_token", "subdomain", "region"}),
}
_PAGERDUTY_CONFIG_KEYS: Final = {
    "oauth": frozenset(
        {
            "auth_mode",
            "region",
            "subdomain",
            "account_id",
            "account_display",
            "granted_scopes",
        }
    ),
    "client_credentials": frozenset(
        {"auth_mode", "region", "subdomain", "account_id", "account_display"}
    ),
    "api_token": frozenset(
        {"auth_mode", "region", "subdomain", "account_id", "account_display"}
    ),
}
_PAGERDUTY_FORBIDDEN_CONFIG_KEYS: Final = _PAGERDUTY_FORBIDDEN_TOKEN_KEYS.union(
    {"client_secret", "client_id", "api_token"}
)


def _validate_pagerduty_descriptor(
    credentials: dict[str, Any], config: dict[str, Any] | None
) -> None:
    config_values = config or {}
    if _PAGERDUTY_FORBIDDEN_TOKEN_KEYS.intersection(credentials) or (
        _PAGERDUTY_FORBIDDEN_CONFIG_KEYS.intersection(config_values)
    ):
        raise ValueError("PagerDuty OAuth tokens must not be stored in descriptors")

    auth_mode = credentials.get("auth_mode")
    if not isinstance(auth_mode, str) or auth_mode not in _PAGERDUTY_DESCRIPTOR_KEYS:
        raise ValueError("PagerDuty credentials require a recognized auth_mode")
    if frozenset(credentials) != _PAGERDUTY_DESCRIPTOR_KEYS[auth_mode]:
        raise ValueError("PagerDuty credentials do not match the auth_mode descriptor")
    if not frozenset(config_values).issubset(_PAGERDUTY_CONFIG_KEYS[auth_mode]):
        raise ValueError("PagerDuty config does not match the auth_mode descriptor")
    if config_values.get("auth_mode") != auth_mode:
        raise ValueError("PagerDuty config auth_mode must match credentials")


class AmbiguousCredentialError(ValueError):
    """Raised when a provider has multiple active credentials and no
    explicit name/id was given to disambiguate."""

    def __init__(self, provider: str, names: list[str]):
        self.provider = provider
        self.names = sorted(names)
        super().__init__(
            f"Multiple active credentials exist for provider '{provider}' "
            f"({', '.join(self.names)}); specify credential_name or credential_id"
        )


class IntegrationCredentialsService:
    """Service for managing integration credentials with encryption."""

    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    async def get(
        self,
        provider: str,
        name: str = "default",
    ) -> IntegrationCredential | None:
        """Get an integration credential."""
        stmt = select(IntegrationCredential).where(
            IntegrationCredential.org_id == self.org_id,
            IntegrationCredential.provider == provider,
            IntegrationCredential.name == name,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, credential_id: str) -> IntegrationCredential | None:
        """Get an integration credential by its UUID primary key."""
        import uuid as uuid_module

        try:
            cred_uuid = uuid_module.UUID(credential_id)
        except (ValueError, AttributeError):
            return None
        stmt = select(IntegrationCredential).where(
            IntegrationCredential.org_id == self.org_id,
            IntegrationCredential.id == cred_uuid,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_decrypted_credentials_by_id(
        self,
        credential_id: str,
    ) -> tuple[dict[str, Any] | None, IntegrationCredential | None]:
        """Get credentials as a decrypted dictionary, looked up by ID.

        Returns (decrypted_dict, credential_record) tuple.
        """
        cred: Any | None = await self.get_by_id(credential_id)
        if cred is None or not cred.credentials_encrypted:
            return None, cred

        try:
            decrypted = decrypt_value(cred.credentials_encrypted)
            return json.loads(decrypted), cred
        except (ValueError, json.JSONDecodeError):
            logger.error(
                "Failed to decrypt/parse integration config for id=%s",
                sanitize_for_log(str(credential_id)),
            )
            return None, cred

    async def get_decrypted_credentials(
        self,
        provider: str,
        name: str = "default",
    ) -> dict[str, Any] | None:
        """Get credentials as a decrypted dictionary."""
        cred: Any | None = await self.get(provider, name)
        if cred is None or not cred.credentials_encrypted:
            return None

        try:
            decrypted = decrypt_value(cred.credentials_encrypted)
            return json.loads(decrypted)
        except (ValueError, json.JSONDecodeError):
            logger.error(
                "Failed to decode integration record for %s/%s",
                sanitize_for_log(provider),
                sanitize_for_log(name),
            )
            return None

    async def resolve_with_fallback(
        self,
        provider: str,
        name: str | None = None,
        credential_id: str | None = None,
    ) -> tuple[IntegrationCredential | None, dict[str, Any] | None]:
        """Resolve a credential for a provider, falling back when unnamed.

        Resolution order:
        1. ``credential_id`` (must belong to ``provider``)
        2. explicit ``name``
        3. the ``"default"`` name
        4. the single active credential for the provider

        Returns ``(credential, decrypted_dict)``; both ``None`` when nothing
        matches. Raises :class:`AmbiguousCredentialError` if step 4 finds more
        than one active credential.
        """
        if credential_id:
            decrypted, cred = await self.get_decrypted_credentials_by_id(credential_id)
            if cred is not None and str(getattr(cred, "provider")) != provider:
                return None, None
            return cred, decrypted

        if name:
            cred = await self.get(provider, name)
            if cred is None:
                return None, None
            return cred, await self.get_decrypted_credentials(provider, name)

        cred = await self.get(provider, "default")
        if cred is not None:
            return cred, await self.get_decrypted_credentials(provider, "default")

        candidates = [
            c
            for c in await self.list_by_provider(provider)
            if bool(getattr(c, "is_active"))
        ]
        if not candidates:
            return None, None
        if len(candidates) > 1:
            raise AmbiguousCredentialError(
                provider, [str(getattr(c, "name")) for c in candidates]
            )
        only = candidates[0]
        only_name = str(getattr(only, "name"))
        return only, await self.get_decrypted_credentials(provider, only_name)

    async def set(
        self,
        provider: str,
        credentials: dict[str, Any],
        name: str = "default",
        config: dict[str, Any] | None = None,
        is_active: bool = True,
    ) -> IntegrationCredential:
        """Set integration credentials (always encrypted)."""
        if provider == "pagerduty":
            _validate_pagerduty_descriptor(credentials, config)
        stmt = select(IntegrationCredential).where(
            IntegrationCredential.org_id == self.org_id,
            IntegrationCredential.provider == provider,
            IntegrationCredential.name == name,
        )
        result = await self.session.execute(stmt)
        cred: Any | None = result.scalar_one_or_none()

        credentials = _normalize_credential_keys(provider, credentials)

        encrypted_creds = encrypt_value(json.dumps(credentials))

        if cred is None:
            cred = IntegrationCredential(
                provider=provider,
                name=name,
                org_id=self.org_id,
                credentials_encrypted=encrypted_creds,
                config=config or {},
                is_active=is_active,
            )
            self.session.add(cred)
        else:
            cred.credentials_encrypted = encrypted_creds
            if config is not None:
                cred.config = config
            cred.is_active = is_active

        await self.session.flush()
        return cred

    async def update_test_result(
        self,
        provider: str,
        success: bool,
        error: str | None = None,
        name: str = "default",
    ) -> None:
        """Update the test connection result."""
        from datetime import datetime, timezone

        cred: Any | None = await self.get(provider, name)
        if cred:
            cred.last_test_at = datetime.now(timezone.utc)
            cred.last_test_success = success
            # CHAOS-2780: this stores errors from testing the credential
            # itself -- the most likely text to embed the secret -- and is
            # republished verbatim via admin API responses and sync-preflight
            # HTTP details, so it must go through the same redaction as the
            # sync-run error columns (CHAOS-2766).
            cred.last_test_error = sanitize_error_text(error)
            await self.session.flush()

    async def list_by_provider(self, provider: str) -> list[IntegrationCredential]:
        """List all credentials for a provider."""
        stmt = select(IntegrationCredential).where(
            IntegrationCredential.org_id == self.org_id,
            IntegrationCredential.provider == provider,
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def list_all(self, active_only: bool = False) -> list[IntegrationCredential]:
        """List all credentials."""
        stmt = select(IntegrationCredential).where(
            IntegrationCredential.org_id == self.org_id,
        )
        if active_only:
            stmt = stmt.where(IntegrationCredential.is_active == True)  # noqa: E712
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def delete(self, provider: str, name: str = "default") -> bool | None:
        """Delete a credential. Returns True if deleted."""
        if provider == "pagerduty":
            return await self.disconnect_pagerduty(name)
        cred = await self.get(provider, name)
        if cred is None:
            return False

        await self.session.delete(cred)
        await self.session.flush()
        return True

    async def disconnect_pagerduty(
        self,
        name: str = "default",
    ) -> bool | None:
        """Disconnect locally and retain remote revoke failures for retry."""

        from dev_health_ops.providers.pagerduty.oauth import (
            PagerDutyOAuthConfig,
        )
        from dev_health_ops.providers.pagerduty.oauth_revocations import (
            PagerDutyOAuthRevocationRepository,
        )
        from dev_health_ops.providers.pagerduty.oauth_storage import (
            PagerDutyOAuthCredentialRepository,
        )
        from dev_health_ops.providers.pagerduty.webhook_bindings import (
            PagerDutyWebhookBindingService,
        )

        credential = await self.get("pagerduty", name)
        repository = PagerDutyOAuthCredentialRepository(self.session, self.org_id, name)
        revocations = PagerDutyOAuthRevocationRepository(
            self.session, self.org_id, name
        )
        config = PagerDutyOAuthConfig.from_env()
        # Lock and revoke/detach webhook binding rows for this credential FIRST,
        # before touching the OAuth token row or the credential row itself.
        # This fixes the binding->source->integration->credential lock order:
        # cleanup paths must never lock the credential/OAuth row ahead of the
        # binding row, or a concurrent binding mutation could deadlock or
        # observe a half-torn-down credential.
        if credential is not None:
            await PagerDutyWebhookBindingService(
                self.session
            ).revoke_and_detach_for_credential(credential.id)
        revoke_candidate: str | None = None
        try:
            versioned = await repository.get()
            if versioned is not None:
                revoke_candidate = (
                    versioned.tokens.refresh_token or versioned.tokens.access_token
                )
        except ValueError:
            revoke_candidate = None
        if config is not None and revoke_candidate is not None:
            await revocations.enqueue(revoke_candidate, purpose="disconnect")
        await repository.delete()
        if credential is None:
            await self.session.commit()
            return None
        credential.is_active = False
        credential.credentials_encrypted = None
        await self.session.flush()
        await self.session.commit()
        if config is None:
            return revoke_candidate is None
        is_revoked = await revocations.retry_pending(config)
        await self.session.commit()
        return is_revoked
