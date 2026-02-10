"""Settings service for managing application configuration.

Provides CRUD operations for settings with encryption support for sensitive values.
Uses Fernet symmetric encryption with a key derived from SETTINGS_ENCRYPTION_KEY.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.schemas import DiscoveredTeam
from dev_health_ops.models.settings import (
    IdentityMapping,
    IntegrationCredential,
    Setting,
    SettingCategory,
    SyncConfiguration,
    TeamMapping,
)
from dev_health_ops.api.utils.logging import sanitize_for_log

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
                "Failed to decrypt/parse credentials for %s/%s",
                sanitize_for_log(provider),
                sanitize_for_log(name),
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

    async def get_by_id(self, config_id: str) -> Optional[SyncConfiguration]:
        """Get a sync configuration by ID."""
        import uuid as uuid_module

        try:
            uid = uuid_module.UUID(config_id)
        except ValueError:
            return None
        stmt = select(SyncConfiguration).where(
            SyncConfiguration.org_id == self.org_id,
            SyncConfiguration.id == uid,
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


class TeamDiscoveryService:
    """Service for discovering teams from external providers."""

    def __init__(self, session: AsyncSession, org_id: str = "default"):
        self.session = session
        self.org_id = org_id

    async def discover_github(self, token: str, org_name: str) -> list[DiscoveredTeam]:
        """Discover teams from GitHub organization."""

        def _discover() -> list[DiscoveredTeam]:
            from github import Auth, Github

            auth = Auth.Token(token)
            gh = Github(auth=auth, per_page=100)
            try:
                org = gh.get_organization(org_name)
                teams: list[DiscoveredTeam] = []
                for gh_team in org.get_teams():
                    repos = [f"{org_name}/{repo.name}" for repo in gh_team.get_repos()]
                    teams.append(
                        DiscoveredTeam(
                            provider_type="github",
                            provider_team_id=gh_team.slug,
                            name=gh_team.name,
                            description=gh_team.description,
                            member_count=getattr(gh_team, "members_count", None),
                            associations={
                                "repo_patterns": repos,
                                "provider_org": org_name,
                            },
                        )
                    )
                return teams
            finally:
                gh.close()

        return await asyncio.to_thread(_discover)

    async def discover_gitlab(
        self,
        token: str,
        group_path: str,
        url: str = "https://gitlab.com",
    ) -> list[DiscoveredTeam]:
        """Discover groups/subgroups from GitLab."""

        def _discover() -> list[DiscoveredTeam]:
            import gitlab as gl_lib

            gl = gl_lib.Gitlab(url=url, private_token=token)
            root_group = gl.groups.get(group_path)
            groups = [root_group]
            for subgroup in root_group.subgroups.list(per_page=100, get_all=True):
                groups.append(gl.groups.get(subgroup.id))

            teams: list[DiscoveredTeam] = []
            for group in groups:
                projects = group.projects.list(per_page=100, get_all=True)
                repo_patterns = [p.path_with_namespace for p in projects]
                teams.append(
                    DiscoveredTeam(
                        provider_type="gitlab",
                        provider_team_id=group.full_path,
                        name=group.name,
                        description=group.description,
                        associations={
                            "repo_patterns": repo_patterns,
                            "provider_org": root_group.full_path,
                        },
                    )
                )

            return teams

        return await asyncio.to_thread(_discover)

    async def discover_jira(
        self,
        email: str,
        api_token: str,
        url: str,
    ) -> list[DiscoveredTeam]:
        """Discover projects from Jira (as team units)."""

        def _discover() -> list[DiscoveredTeam]:
            response = requests.get(
                f"{url.rstrip('/')}/rest/api/3/project/search",
                auth=(email, api_token),
                params={"maxResults": 100},
                headers={"Accept": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()

            teams: list[DiscoveredTeam] = []
            for project in payload.get("values", []):
                project_key = project.get("key")
                project_name = project.get("name") or project_key
                if not project_key or not project_name:
                    continue
                teams.append(
                    DiscoveredTeam(
                        provider_type="jira",
                        provider_team_id=project_key,
                        name=project_name,
                        description=project.get("description"),
                        associations={
                            "project_keys": [project_key],
                            "provider_org": url,
                        },
                    )
                )

            return teams

        return await asyncio.to_thread(_discover)

    async def import_teams(
        self,
        teams: list[DiscoveredTeam],
        on_conflict: str = "skip",
    ) -> dict[str, Any]:
        """Import discovered teams into TeamMapping."""
        team_mapping_svc = TeamMappingService(self.session, self.org_id)
        imported = 0
        skipped = 0
        merged = 0
        details: list[dict[str, Any]] = []

        for team in teams:
            if team.provider_type == "github":
                team_id = f"gh:{team.provider_team_id}"
            elif team.provider_type == "gitlab":
                team_id = f"gl:{team.provider_team_id}"
            else:
                team_id = team.provider_team_id

            existing = await team_mapping_svc.get(team_id)
            if existing is not None and on_conflict == "skip":
                skipped += 1
                details.append(
                    {
                        "team_id": team_id,
                        "provider_team_id": team.provider_team_id,
                        "action": "skipped",
                    }
                )
                continue

            associations = team.associations or {}
            provider_linkage = {
                "provider_type": team.provider_type,
                "provider_team_id": team.provider_team_id,
                "provider_org": associations.get("provider_org"),
                "last_discovered_at": datetime.now(timezone.utc).isoformat(),
                "sync_source": "imported",
            }

            extra_data = dict(existing.extra_data or {}) if existing else {}
            extra_data.update(
                {k: v for k, v in provider_linkage.items() if v is not None}
            )

            await team_mapping_svc.create_or_update(
                team_id=team_id,
                name=team.name,
                description=team.description,
                repo_patterns=associations.get("repo_patterns", []),
                project_keys=associations.get("project_keys", []),
                extra_data=extra_data,
            )

            if existing is None:
                imported += 1
                details.append(
                    {
                        "team_id": team_id,
                        "provider_team_id": team.provider_team_id,
                        "action": "imported",
                    }
                )
            else:
                merged += 1
                details.append(
                    {
                        "team_id": team_id,
                        "provider_team_id": team.provider_team_id,
                        "action": "merged",
                    }
                )

        return {
            "imported": imported,
            "skipped": skipped,
            "merged": merged,
            "details": details,
        }
