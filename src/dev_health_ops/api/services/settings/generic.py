"""Generic key/value settings service backed by the ``Setting`` model.

Provides CRUD operations for org-scoped settings with optional Fernet
symmetric encryption for sensitive values. Encryption uses the key derived
from ``SETTINGS_ENCRYPTION_KEY``.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.core.encryption import decrypt_value, encrypt_value
from dev_health_ops.models.settings import Setting, SettingCategory


class SettingsService:
    """Service for managing settings with encryption support."""

    def __init__(self, session: AsyncSession, org_id: str):
        self.session = session
        self.org_id = org_id

    async def get(
        self,
        key: str,
        category: str = SettingCategory.GENERAL.value,
        default: str | None = None,
    ) -> str | None:
        """Get a setting value, decrypting if necessary."""
        stmt = select(Setting).where(
            Setting.org_id == self.org_id,
            Setting.category == category,
            Setting.key == key,
        )
        result = await self.session.execute(stmt)
        setting: Any | None = result.scalar_one_or_none()

        if setting is None:
            return default

        if setting.is_encrypted and setting.value:
            return decrypt_value(setting.value)
        return setting.value

    async def set(
        self,
        key: str,
        value: str | None,
        category: str = SettingCategory.GENERAL.value,
        encrypt: bool = False,
        description: str | None = None,
    ) -> Setting:
        """Set a setting value, encrypting if requested."""
        stmt = select(Setting).where(
            Setting.org_id == self.org_id,
            Setting.category == category,
            Setting.key == key,
        )
        result = await self.session.execute(stmt)
        setting: Any | None = result.scalar_one_or_none()

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
        setting: Any | None = result.scalar_one_or_none()

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
        settings: list[Any] = list(result.scalars().all())

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
