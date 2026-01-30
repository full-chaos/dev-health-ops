"""Settings service for managing application configuration.

Provides CRUD operations for settings with encryption support for sensitive values.
Uses Fernet symmetric encryption with a key derived from SETTINGS_ENCRYPTION_KEY.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from typing import Any, Optional

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.settings import (
    IdentityMapping,
    IntegrationCredential,
    Setting,
    SettingCategory,
    SyncConfiguration,
    TeamMapping,
)

logger = logging.getLogger(__name__)


def _derive_key(secret: str) -> bytes:
    """Derive a Fernet-compatible key from a secret string using SHA-256."""
    digest = hashlib.sha256(secret.encode()).digest()
    return base64.urlsafe_b64encode(digest)


def _get_encryption_key() -> bytes:
    """Get the encryption key from environment, deriving it if needed."""
    secret = os.getenv("SETTINGS_ENCRYPTION_KEY")
    if not secret:
        raise RuntimeError(
            "SETTINGS_ENCRYPTION_KEY environment variable is required for encryption"
        )
    return _derive_key(secret)


def encrypt_value(plaintext: str) -> str:
    """Encrypt a plaintext string and return base64-encoded ciphertext."""
    key = _get_encryption_key()
    f = Fernet(key)
    return f.encrypt(plaintext.encode()).decode()


def decrypt_value(ciphertext: str) -> str:
    """Decrypt a base64-encoded ciphertext and return plaintext."""
    key = _get_encryption_key()
    f = Fernet(key)
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except InvalidToken:
        logger.error("Failed to decrypt value - invalid token or wrong key")
        raise ValueError("Decryption failed - check SETTINGS_ENCRYPTION_KEY")


class SettingsService:
    """Service for managing settings with encryption support."""

    def __init__(self, session: AsyncSession, org_id: str = "default"):
        self.session = session
        self.org_id = org_id

    async def get(
        self,
        key: str,
        category: str = SettingCategory.GENERAL.value,
        default: Optional[str] = None,
    ) -> Optional[str]:
        """Get a setting value, decrypting if necessary."""
        stmt = select(Setting).where(
            Setting.org_id == self.org_id,
            Setting.category == category,
            Setting.key == key,
        )
        result = await self.session.execute(stmt)
        setting = result.scalar_one_or_none()

        if setting is None:
            return default

        if setting.is_encrypted and setting.value:
            return decrypt_value(setting.value)
        return setting.value

    async def set(
        self,
        key: str,
        value: Optional[str],
        category: str = SettingCategory.GENERAL.value,
        encrypt: bool = False,
        description: Optional[str] = None,
    ) -> Setting:
        """Set a setting value, encrypting if requested."""
        stmt = select(Setting).where(
            Setting.org_id == self.org_id,
            Setting.category == category,
            Setting.key == key,
        )
        result = await self.session.execute(stmt)
        setting = result.scalar_one_or_none()

        stored_value = value
        if encrypt and value:
            stored_value = encrypt_value(value)

        if setting is None:
            setting = Setting(
                key=key,
                category=category,
                value=stored_value,
                org_id=self.org_id,
                is_encrypted=encrypt,
                description=description,
            )
            self.session.add(setting)
        else:
            setting.value = stored_value
            setting.is_encrypted = encrypt
            if description is not None:
                setting.description = description

        await self.session.flush()
        return setting

    async def delete(
        self, key: str, category: str = SettingCategory.GENERAL.value
    ) -> bool:
        """Delete a setting. Returns True if deleted, False if not found."""
        stmt = select(Setting).where(
            Setting.org_id == self.org_id,
            Setting.category == category,
            Setting.key == key,
        )
        result = await self.session.execute(stmt)
        setting = result.scalar_one_or_none()

        if setting is None:
            return False

        await self.session.delete(setting)
        await self.session.flush()
        return True

    async def list_by_category(self, category: str) -> list[dict[str, Any]]:
        """List all settings in a category (values decrypted)."""
        stmt = select(Setting).where(
            Setting.org_id == self.org_id,
            Setting.category == category,
        )
        result = await self.session.execute(stmt)
        settings = result.scalars().all()

        items = []
        for s in settings:
            value = s.value
            if s.is_encrypted and value:
                try:
                    value = decrypt_value(value)
                except ValueError:
                    value = "[DECRYPTION_FAILED]"
            items.append(
                {
                    "key": s.key,
                    "value": value,
                    "is_encrypted": s.is_encrypted,
                    "description": s.description,
                }
            )
        return items

    async def get_all_categories(self) -> list[str]:
        """Get all categories that have settings."""
        stmt = select(Setting.category).where(Setting.org_id == self.org_id).distinct()
        result = await self.session.execute(stmt)
        return [row[0] for row in result.all()]


class IntegrationCredentialsService:
    """Service for managing integration credentials with encryption."""

    def __init__(self, session: AsyncSession, org_id: str = "default"):
        self.session = session
        self.org_id = org_id

    async def get(
        self,
        provider: str,
        name: str = "default",
    ) -> Optional[IntegrationCredential]:
        """Get an integration credential."""
        stmt = select(IntegrationCredential).where(
            IntegrationCredential.org_id == self.org_id,
            IntegrationCredential.provider == provider,
            IntegrationCredential.name == name,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_decrypted_credentials(
        self,
        provider: str,
        name: str = "default",
    ) -> Optional[dict[str, Any]]:
        """Get credentials as a decrypted dictionary."""
        cred = await self.get(provider, name)
        if cred is None or not cred.credentials_encrypted:
            return None

        try:
            decrypted = decrypt_value(cred.credentials_encrypted)
            return json.loads(decrypted)
        except (ValueError, json.JSONDecodeError):
            logger.error(
                "Failed to decrypt/parse credentials for %s/%s", provider, name
            )
            return None

    async def set(
        self,
        provider: str,
        credentials: dict[str, Any],
        name: str = "default",
        config: Optional[dict[str, Any]] = None,
        is_active: bool = True,
    ) -> IntegrationCredential:
        """Set integration credentials (always encrypted)."""
        stmt = select(IntegrationCredential).where(
            IntegrationCredential.org_id == self.org_id,
            IntegrationCredential.provider == provider,
            IntegrationCredential.name == name,
        )
        result = await self.session.execute(stmt)
        cred = result.scalar_one_or_none()

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
        error: Optional[str] = None,
        name: str = "default",
    ) -> None:
        """Update the test connection result."""
        from datetime import datetime, timezone

        cred = await self.get(provider, name)
        if cred:
            cred.last_test_at = datetime.now(timezone.utc)
            cred.last_test_success = success
            cred.last_test_error = error
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

    async def delete(self, provider: str, name: str = "default") -> bool:
        """Delete a credential. Returns True if deleted."""
        cred = await self.get(provider, name)
        if cred is None:
            return False

        await self.session.delete(cred)
        await self.session.flush()
        return True


class SyncConfigurationService:
    """Service for managing sync configurations."""

    def __init__(self, session: AsyncSession, org_id: str = "default"):
        self.session = session
        self.org_id = org_id

    async def get(self, name: str) -> Optional[SyncConfiguration]:
        """Get a sync configuration by name."""
        stmt = select(SyncConfiguration).where(
            SyncConfiguration.org_id == self.org_id,
            SyncConfiguration.name == name,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def create(
        self,
        name: str,
        provider: str,
        sync_targets: list[str],
        sync_options: Optional[dict[str, Any]] = None,
        credential_id: Optional[str] = None,
    ) -> SyncConfiguration:
        """Create a new sync configuration."""
        import uuid as uuid_module

        config = SyncConfiguration(
            name=name,
            provider=provider,
            org_id=self.org_id,
            credential_id=uuid_module.UUID(credential_id) if credential_id else None,
            sync_targets=sync_targets,
            sync_options=sync_options or {},
        )
        self.session.add(config)
        await self.session.flush()
        return config

    async def update(
        self,
        name: str,
        sync_targets: Optional[list[str]] = None,
        sync_options: Optional[dict[str, Any]] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[SyncConfiguration]:
        """Update a sync configuration."""
        config = await self.get(name)
        if config is None:
            return None

        if sync_targets is not None:
            config.sync_targets = sync_targets
        if sync_options is not None:
            config.sync_options = sync_options
        if is_active is not None:
            config.is_active = is_active

        await self.session.flush()
        return config

    async def list_all(self, active_only: bool = False) -> list[SyncConfiguration]:
        """List all sync configurations."""
        stmt = select(SyncConfiguration).where(
            SyncConfiguration.org_id == self.org_id,
        )
        if active_only:
            stmt = stmt.where(SyncConfiguration.is_active == True)  # noqa: E712
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def delete(self, name: str) -> bool:
        """Delete a sync configuration."""
        config = await self.get(name)
        if config is None:
            return False

        await self.session.delete(config)
        await self.session.flush()
        return True


class IdentityMappingService:
    """Service for managing identity mappings."""

    def __init__(self, session: AsyncSession, org_id: str = "default"):
        self.session = session
        self.org_id = org_id

    async def get(self, canonical_id: str) -> Optional[IdentityMapping]:
        """Get an identity mapping by canonical ID."""
        stmt = select(IdentityMapping).where(
            IdentityMapping.org_id == self.org_id,
            IdentityMapping.canonical_id == canonical_id,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def find_by_provider_identity(
        self,
        provider: str,
        identity: str,
    ) -> Optional[IdentityMapping]:
        """Find an identity mapping by provider-specific identity."""
        stmt = select(IdentityMapping).where(
            IdentityMapping.org_id == self.org_id,
        )
        result = await self.session.execute(stmt)
        mappings = result.scalars().all()

        for mapping in mappings:
            identities = mapping.provider_identities.get(provider, [])
            if identity in identities:
                return mapping
        return None

    async def create_or_update(
        self,
        canonical_id: str,
        display_name: Optional[str] = None,
        email: Optional[str] = None,
        provider_identities: Optional[dict[str, list[str]]] = None,
        team_ids: Optional[list[str]] = None,
    ) -> IdentityMapping:
        """Create or update an identity mapping."""
        mapping = await self.get(canonical_id)

        if mapping is None:
            mapping = IdentityMapping(
                canonical_id=canonical_id,
                org_id=self.org_id,
                display_name=display_name,
                email=email,
                provider_identities=provider_identities or {},
                team_ids=team_ids or [],
            )
            self.session.add(mapping)
        else:
            if display_name is not None:
                mapping.display_name = display_name
            if email is not None:
                mapping.email = email
            if provider_identities is not None:
                mapping.provider_identities = provider_identities
            if team_ids is not None:
                mapping.team_ids = team_ids

        await self.session.flush()
        return mapping

    async def add_provider_identity(
        self,
        canonical_id: str,
        provider: str,
        identity: str,
    ) -> Optional[IdentityMapping]:
        """Add a provider identity to an existing mapping."""
        mapping = await self.get(canonical_id)
        if mapping is None:
            return None

        identities = mapping.provider_identities.get(provider, [])
        if identity not in identities:
            identities.append(identity)
            mapping.provider_identities = {
                **mapping.provider_identities,
                provider: identities,
            }
            await self.session.flush()

        return mapping

    async def list_all(self, active_only: bool = True) -> list[IdentityMapping]:
        """List all identity mappings."""
        stmt = select(IdentityMapping).where(
            IdentityMapping.org_id == self.org_id,
        )
        if active_only:
            stmt = stmt.where(IdentityMapping.is_active == True)  # noqa: E712
        result = await self.session.execute(stmt)
        return list(result.scalars().all())


class TeamMappingService:
    """Service for managing team mappings."""

    def __init__(self, session: AsyncSession, org_id: str = "default"):
        self.session = session
        self.org_id = org_id

    async def get(self, team_id: str) -> Optional[TeamMapping]:
        """Get a team mapping by team ID."""
        stmt = select(TeamMapping).where(
            TeamMapping.org_id == self.org_id,
            TeamMapping.team_id == team_id,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def create_or_update(
        self,
        team_id: str,
        name: str,
        description: Optional[str] = None,
        repo_patterns: Optional[list[str]] = None,
        project_keys: Optional[list[str]] = None,
        extra_data: Optional[dict[str, Any]] = None,
    ) -> TeamMapping:
        """Create or update a team mapping."""
        mapping = await self.get(team_id)

        if mapping is None:
            mapping = TeamMapping(
                team_id=team_id,
                name=name,
                org_id=self.org_id,
                description=description,
                repo_patterns=repo_patterns or [],
                project_keys=project_keys or [],
                extra_data=extra_data or {},
            )
            self.session.add(mapping)
        else:
            mapping.name = name
            if description is not None:
                mapping.description = description
            if repo_patterns is not None:
                mapping.repo_patterns = repo_patterns
            if project_keys is not None:
                mapping.project_keys = project_keys
            if extra_data is not None:
                mapping.extra_data = extra_data

        await self.session.flush()
        return mapping

    async def list_all(self, active_only: bool = True) -> list[TeamMapping]:
        """List all team mappings."""
        stmt = select(TeamMapping).where(
            TeamMapping.org_id == self.org_id,
        )
        if active_only:
            stmt = stmt.where(TeamMapping.is_active == True)  # noqa: E712
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def delete(self, team_id: str) -> bool:
        """Delete a team mapping."""
        mapping = await self.get(team_id)
        if mapping is None:
            return False

        await self.session.delete(mapping)
        await self.session.flush()
        return True
