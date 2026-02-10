"""Settings service for managing application configuration.

Provides CRUD operations for settings with encryption support for sensitive values.
Uses Fernet symmetric encryption with a key derived from SETTINGS_ENCRYPTION_KEY.
"""

from __future__ import annotations

import asyncio
import base64
import difflib
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

import requests
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.models.settings import (
    IdentityMapping,
    IntegrationCredential,
    Setting,
    SettingCategory,
    SyncConfiguration,
    TeamMapping,
)

if TYPE_CHECKING:
    from dev_health_ops.api.admin.schemas import (
        ConfirmInferredMemberAction,
        ConfirmMemberLink,
        DiscoveredMember,
        DiscoveredTeam,
        InferredMember,
        MemberMatchResult,
    )


def _get_discovered_team_cls() -> type:
    """Lazy import to avoid circular dependency with admin.schemas."""
    from dev_health_ops.api.admin.schemas import DiscoveredTeam as _DT

    return _DT


def _get_discovered_member_cls() -> type:
    from dev_health_ops.api.admin.schemas import DiscoveredMember as _DM

    return _DM


def _get_member_match_result_cls() -> type:
    from dev_health_ops.api.admin.schemas import MemberMatchResult as _MMR

    return _MMR


def _get_identity_mapping_response_cls() -> type:
    from dev_health_ops.api.admin.schemas import IdentityMappingResponse as _IMR

    return _IMR


def _get_jira_activity_schema_classes() -> tuple[type, type]:
    """Lazy import to avoid circular dependency with admin.schemas."""
    from dev_health_ops.api.admin.schemas import (
        ConfirmInferredMemberAction as _CIMA,
        InferredMember as _IM,
    )

    return _IM, _CIMA


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

            DiscoveredTeam = _get_discovered_team_cls()
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

            DiscoveredTeam = _get_discovered_team_cls()
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
            DiscoveredTeam = _get_discovered_team_cls()
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


class TeamDriftSyncService:
    """Compares discovered teams against stored TeamMappings and flags/merges changes."""

    def __init__(self, session: AsyncSession, org_id: str = "default"):
        self.session = session
        self.org_id = org_id

    async def run_drift_sync(
        self,
        provider: str,
        discovered_teams: list[DiscoveredTeam],
    ) -> dict[str, Any]:
        team_svc = TeamMappingService(self.session, self.org_id)
        existing_teams = await team_svc.list_all(active_only=True)

        provider_lookup: dict[str, TeamMapping] = {}
        for team in existing_teams:
            ed = team.extra_data or {}
            if ed.get("provider_type") == provider:
                provider_lookup[ed.get("provider_team_id", "")] = team

        discovered_lookup: dict[str, DiscoveredTeam] = {
            t.provider_team_id: t for t in discovered_teams
        }

        now = datetime.now(timezone.utc)
        auto_applied = 0
        flagged = 0
        new_available = 0
        provider_removed = 0

        for disc_team in discovered_teams:
            existing = provider_lookup.get(disc_team.provider_team_id)
            if existing is None:
                new_available += 1
                continue

            changes = self._compute_field_diffs(existing, disc_team)
            if not changes:
                existing.last_drift_sync_at = now
                continue

            if existing.sync_policy == 0:
                self._apply_changes(existing, changes, now)
                auto_applied += 1
            elif existing.sync_policy == 1:
                current_flagged = dict(existing.flagged_changes or {})
                current_flagged["pending"] = current_flagged.get("pending", [])
                for change in changes:
                    change["discovered_at"] = now.isoformat()
                    current_flagged["pending"].append(change)
                existing.flagged_changes = current_flagged
                flagged += 1

            existing.last_drift_sync_at = now

        for provider_team_id, existing in provider_lookup.items():
            if provider_team_id not in discovered_lookup:
                current_flagged = dict(existing.flagged_changes or {})
                current_flagged["pending"] = current_flagged.get("pending", [])
                current_flagged["pending"].append(
                    {
                        "change_type": "provider_removed",
                        "discovered_at": now.isoformat(),
                    }
                )
                existing.flagged_changes = current_flagged
                existing.last_drift_sync_at = now
                provider_removed += 1

        await self.session.flush()

        return {
            "provider": provider,
            "auto_applied": auto_applied,
            "flagged": flagged,
            "new_available": new_available,
            "provider_removed": provider_removed,
        }

    def _compute_field_diffs(
        self,
        existing: TeamMapping,
        discovered: DiscoveredTeam,
    ) -> list[dict[str, Any]]:
        changes: list[dict[str, Any]] = []
        managed = existing.managed_fields or []
        associations = discovered.associations or {}

        field_map = {
            "name": discovered.name,
            "description": discovered.description,
            "repo_patterns": associations.get("repo_patterns", []),
            "project_keys": associations.get("project_keys", []),
        }

        for field_name in managed:
            if field_name not in field_map:
                continue
            new_val = field_map[field_name]
            old_val = getattr(existing, field_name, None)

            if isinstance(old_val, list) and isinstance(new_val, list):
                if sorted(old_val) == sorted(new_val):
                    continue
            elif old_val == new_val:
                continue

            changes.append(
                {
                    "change_type": "field_changed",
                    "field": field_name,
                    "old_value": old_val,
                    "new_value": new_val,
                }
            )

        return changes

    def _apply_changes(
        self,
        existing: TeamMapping,
        changes: list[dict[str, Any]],
        now: datetime,
    ) -> None:
        for change in changes:
            field = change.get("field")
            if field and hasattr(existing, field):
                setattr(existing, field, change["new_value"])
        existing.last_drift_sync_at = now
        ed = dict(existing.extra_data or {})
        ed["last_discovered_at"] = now.isoformat()
        existing.extra_data = ed

    async def approve_changes(
        self,
        team_id: str,
        change_indices: list[int] | None = None,
    ) -> dict[str, Any]:
        team_svc = TeamMappingService(self.session, self.org_id)
        team = await team_svc.get(team_id)
        if team is None:
            return {"error": "Team not found"}

        flagged = dict(team.flagged_changes or {})
        pending = flagged.get("pending", [])

        if not pending:
            return {"approved": 0}

        now = datetime.now(timezone.utc)
        to_approve = (
            pending
            if change_indices is None
            else [pending[i] for i in change_indices if i < len(pending)]
        )

        applied = 0
        for change in to_approve:
            ct = change.get("change_type")
            if ct == "field_changed":
                field = change.get("field")
                if field and hasattr(team, field):
                    setattr(team, field, change["new_value"])
                    applied += 1
            elif ct == "new_team_available":
                pass
            elif ct == "provider_removed":
                applied += 1

        if change_indices is None:
            flagged["pending"] = []
        else:
            flagged["pending"] = [
                p for i, p in enumerate(pending) if i not in change_indices
            ]

        team.flagged_changes = flagged if flagged.get("pending") else None
        team.last_drift_sync_at = now
        await self.session.flush()

        return {"approved": applied}

    async def dismiss_changes(
        self,
        team_id: str,
        change_indices: list[int] | None = None,
    ) -> dict[str, Any]:
        team_svc = TeamMappingService(self.session, self.org_id)
        team = await team_svc.get(team_id)
        if team is None:
            return {"error": "Team not found"}

        flagged = dict(team.flagged_changes or {})
        pending = flagged.get("pending", [])

        if not pending:
            return {"dismissed": 0}

        count = (
            len(pending)
            if change_indices is None
            else len([i for i in change_indices if i < len(pending)])
        )

        if change_indices is None:
            flagged["pending"] = []
        else:
            flagged["pending"] = [
                p for i, p in enumerate(pending) if i not in change_indices
            ]

        team.flagged_changes = flagged if flagged.get("pending") else None
        await self.session.flush()

        return {"dismissed": count}

    async def get_all_pending_changes(self) -> list[dict[str, Any]]:
        team_svc = TeamMappingService(self.session, self.org_id)
        teams = await team_svc.list_all(active_only=True)

        all_changes: list[dict[str, Any]] = []
        for team in teams:
            flagged = team.flagged_changes or {}
            pending = flagged.get("pending", [])
            for change in pending:
                all_changes.append(
                    {
                        "team_id": team.team_id,
                        "team_name": team.name,
                        **change,
                    }
                )

        return all_changes


class TeamMembershipService:
    def __init__(self, session: AsyncSession, org_id: str = "default"):
        self.session = session
        self.org_id = org_id

    async def discover_members_github(
        self,
        token: str,
        org_name: str,
        team_slug: str,
    ) -> list[DiscoveredMember]:
        def _discover() -> list[DiscoveredMember]:
            from github import Auth, Github

            DiscoveredMember = _get_discovered_member_cls()
            auth = Auth.Token(token)
            gh = Github(auth=auth, per_page=100)
            try:
                org = gh.get_organization(org_name)
                team = org.get_team_by_slug(team_slug)
                members: list[DiscoveredMember] = []
                for member in team.get_members():
                    members.append(
                        DiscoveredMember(
                            provider_type="github",
                            provider_identity=member.login,
                            display_name=getattr(member, "name", None),
                            email=getattr(member, "email", None),
                            role=None,
                        )
                    )
                return members
            finally:
                gh.close()

        return await asyncio.to_thread(_discover)

    async def discover_members_gitlab(
        self,
        token: str,
        group_path: str,
        url: str,
    ) -> list[DiscoveredMember]:
        def _discover() -> list[DiscoveredMember]:
            import gitlab as gl_lib

            DiscoveredMember = _get_discovered_member_cls()
            gl = gl_lib.Gitlab(url=url, private_token=token)
            group = gl.groups.get(group_path)
            members: list[DiscoveredMember] = []
            for member in group.members.list(per_page=100, get_all=True):
                members.append(
                    DiscoveredMember(
                        provider_type="gitlab",
                        provider_identity=str(getattr(member, "username", "")),
                        display_name=getattr(member, "name", None),
                        email=getattr(member, "email", None),
                        role=str(getattr(member, "access_level", "")) or None,
                    )
                )
            return [m for m in members if m.provider_identity]

        return await asyncio.to_thread(_discover)

    async def discover_members_jira(
        self,
        email: str,
        api_token: str,
        url: str,
        project_key: str,
    ) -> list[DiscoveredMember]:
        def _discover() -> list[DiscoveredMember]:
            DiscoveredMember = _get_discovered_member_cls()
            response = requests.get(
                f"{url.rstrip('/')}/rest/api/3/project/{project_key}",
                auth=(email, api_token),
                headers={"Accept": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            lead = payload.get("lead") or {}
            provider_identity = (
                lead.get("accountId")
                or lead.get("emailAddress")
                or lead.get("displayName")
                or ""
            )
            if not provider_identity:
                return []
            return [
                DiscoveredMember(
                    provider_type="jira",
                    provider_identity=provider_identity,
                    display_name=lead.get("displayName"),
                    email=lead.get("emailAddress"),
                    role="lead",
                )
            ]

        return await asyncio.to_thread(_discover)

    async def match_members(
        self,
        members: list[DiscoveredMember],
    ) -> list[MemberMatchResult]:
        IdentityMappingResponse = _get_identity_mapping_response_cls()
        MemberMatchResult = _get_member_match_result_cls()
        identity_svc = IdentityMappingService(self.session, self.org_id)
        matched: list[MemberMatchResult] = []

        for member in members:
            mapping = await identity_svc.find_by_provider_identity(
                member.provider_type,
                member.provider_identity,
            )
            if mapping is not None:
                matched.append(
                    MemberMatchResult(
                        discovered=member,
                        match_status="matched",
                        matched_identity=IdentityMappingResponse.model_validate(
                            mapping
                        ),
                        confidence=1.0,
                    )
                )
                continue

            if member.email:
                stmt = select(IdentityMapping).where(
                    IdentityMapping.org_id == self.org_id,
                    IdentityMapping.email == member.email,
                    IdentityMapping.is_active == True,  # noqa: E712
                )
                email_result = await self.session.execute(stmt)
                email_match = email_result.scalar_one_or_none()
                if email_match is not None:
                    matched.append(
                        MemberMatchResult(
                            discovered=member,
                            match_status="suggested",
                            matched_identity=IdentityMappingResponse.model_validate(
                                email_match
                            ),
                            confidence=0.95,
                            suggestion_reason="email_match",
                        )
                    )
                    continue

            if member.display_name:
                name_stmt = select(IdentityMapping).where(
                    IdentityMapping.org_id == self.org_id,
                    IdentityMapping.display_name.isnot(None),
                    IdentityMapping.is_active == True,  # noqa: E712
                )
                name_result = await self.session.execute(name_stmt)
                best_match: Optional[IdentityMapping] = None
                best_score = 0.0
                for candidate in name_result.scalars().all():
                    if not candidate.display_name:
                        continue
                    score = difflib.SequenceMatcher(
                        a=member.display_name.lower(),
                        b=candidate.display_name.lower(),
                    ).ratio()
                    if score > best_score:
                        best_score = score
                        best_match = candidate

                if best_match is not None and best_score >= 0.8:
                    matched.append(
                        MemberMatchResult(
                            discovered=member,
                            match_status="suggested",
                            matched_identity=IdentityMappingResponse.model_validate(
                                best_match
                            ),
                            confidence=round(best_score, 2),
                            suggestion_reason="display_name_similarity",
                        )
                    )
                    continue

            matched.append(
                MemberMatchResult(
                    discovered=member,
                    match_status="unmatched",
                    matched_identity=None,
                    confidence=None,
                    suggestion_reason=None,
                )
            )

        return matched

    async def confirm_links(
        self,
        team_id: str,
        links: list[ConfirmMemberLink],
    ) -> dict[str, int]:
        identity_svc = IdentityMappingService(self.session, self.org_id)
        linked = 0
        created = 0
        skipped = 0

        for link in links:
            if link.action == "skip":
                skipped += 1
                continue

            if link.action == "link":
                mapping = await identity_svc.get(link.canonical_id)
                if mapping is None:
                    skipped += 1
                    continue

                team_ids = list(mapping.team_ids or [])
                if team_id not in team_ids:
                    team_ids.append(team_id)
                    mapping.team_ids = team_ids
                await identity_svc.add_provider_identity(
                    canonical_id=link.canonical_id,
                    provider=link.provider,
                    identity=link.provider_identity,
                )
                linked += 1
                continue

            if link.action == "create":
                await identity_svc.create_or_update(
                    canonical_id=link.canonical_id,
                    provider_identities={link.provider: [link.provider_identity]},
                    team_ids=[team_id],
                )
                created += 1
                continue

            skipped += 1

        await self.session.flush()
        return {
            "linked": linked,
            "created": created,
            "skipped": skipped,
        }


class JiraActivityInferenceService:
    def __init__(self, session: AsyncSession, org_id: str = "default"):
        self.session = session
        self.org_id = org_id

    def _parse_jira_datetime(self, value: Any) -> Optional[datetime]:
        if not isinstance(value, str) or not value:
            return None
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _confidence_for_count(self, count: int) -> str:
        if count >= 5:
            return "core"
        if count >= 2:
            return "active"
        return "peripheral"

    async def infer_members(
        self,
        email: str,
        api_token: str,
        jira_url: str,
        project_key: str,
        window_days: int = 90,
    ) -> list[InferredMember]:
        InferredMember, _ = _get_jira_activity_schema_classes()

        jql = f"project = '{project_key}' AND updated >= '-{int(window_days)}d'"
        from dev_health_ops.providers.jira.client import JiraAuth, JiraClient

        client = JiraClient(
            auth=JiraAuth(base_url=jira_url, email=email, api_token=api_token)
        )

        def _fetch_issues() -> list[dict[str, Any]]:
            try:
                return list(
                    client.iter_issues(
                        jql=jql,
                        fields=["assignee", "reporter", "creator", "comment"],
                        expand_changelog=False,
                        limit=500,
                    )
                )
            finally:
                client.close()

        issues = await asyncio.to_thread(_fetch_issues)

        activity_map: dict[str, dict[str, Any]] = {}

        def _touch(
            actor: Any,
            role: str,
            issue_updated_at: Optional[datetime],
        ) -> None:
            if not isinstance(actor, dict):
                return
            account_id = actor.get("accountId")
            if not account_id:
                return

            current = activity_map.get(account_id)
            if current is None:
                current = {
                    "account_id": account_id,
                    "display_name": actor.get("displayName"),
                    "email": actor.get("emailAddress"),
                    "activity_count": 0,
                    "roles": set(),
                    "last_active": None,
                }
                activity_map[account_id] = current

            current["activity_count"] += 1
            current["roles"].add(role)
            if not current.get("display_name") and actor.get("displayName"):
                current["display_name"] = actor.get("displayName")
            if not current.get("email") and actor.get("emailAddress"):
                current["email"] = actor.get("emailAddress")

            existing_last_active = current.get("last_active")
            if issue_updated_at and (
                existing_last_active is None or issue_updated_at > existing_last_active
            ):
                current["last_active"] = issue_updated_at

        for issue in issues:
            fields = issue.get("fields") or {}
            issue_updated_at = self._parse_jira_datetime(fields.get("updated"))
            _touch(fields.get("assignee"), "assignee", issue_updated_at)
            _touch(fields.get("reporter"), "reporter", issue_updated_at)
            _touch(fields.get("creator"), "commenter", issue_updated_at)

        inferred_members = [
            InferredMember(
                account_id=str(data["account_id"]),
                display_name=data.get("display_name"),
                email=data.get("email"),
                activity_count=int(data.get("activity_count", 0)),
                confidence=self._confidence_for_count(
                    int(data.get("activity_count", 0))
                ),
                roles=sorted(list(data.get("roles", set()))),
                last_active=data.get("last_active"),
            )
            for data in activity_map.values()
        ]

        return sorted(
            inferred_members,
            key=lambda member: (-member.activity_count, member.account_id),
        )

    async def match_and_confirm(
        self,
        team_id: str,
        members: list[ConfirmInferredMemberAction],
    ) -> dict[str, int]:
        identity_svc = IdentityMappingService(self.session, self.org_id)

        linked = 0
        created = 0
        skipped = 0

        for member in members:
            action = getattr(member, "action", None)
            account_id = str(getattr(member, "account_id", ""))
            if action == "skip":
                skipped += 1
                continue
            if action != "add" or not account_id:
                skipped += 1
                continue

            existing_by_provider = await identity_svc.find_by_provider_identity(
                "jira", account_id
            )

            canonical_id = getattr(member, "canonical_id", None)
            if existing_by_provider is not None and not canonical_id:
                raise ValueError(
                    f"canonical_id is required for existing Jira identity '{account_id}'"
                )

            if canonical_id:
                mapping = await identity_svc.get(canonical_id)
                if mapping is None:
                    raise ValueError(f"Identity '{canonical_id}' not found")
                if (
                    existing_by_provider is not None
                    and existing_by_provider.canonical_id != canonical_id
                ):
                    raise ValueError(
                        f"Jira identity '{account_id}' is linked to a different canonical identity"
                    )

                provider_identities = dict(mapping.provider_identities or {})
                jira_identities = list(provider_identities.get("jira", []))
                if account_id not in jira_identities:
                    jira_identities.append(account_id)
                provider_identities["jira"] = jira_identities

                team_ids = list(mapping.team_ids or [])
                if team_id not in team_ids:
                    team_ids.append(team_id)

                await identity_svc.create_or_update(
                    canonical_id=canonical_id,
                    display_name=getattr(member, "display_name", None)
                    or mapping.display_name,
                    email=getattr(member, "email", None) or mapping.email,
                    provider_identities=provider_identities,
                    team_ids=team_ids,
                )
                linked += 1
                continue

            await identity_svc.create_or_update(
                canonical_id=f"jira:{account_id}",
                display_name=getattr(member, "display_name", None),
                email=getattr(member, "email", None),
                provider_identities={"jira": [account_id]},
                team_ids=[team_id],
            )
            created += 1

        return {
            "linked": linked,
            "created": created,
            "skipped": skipped,
        }
