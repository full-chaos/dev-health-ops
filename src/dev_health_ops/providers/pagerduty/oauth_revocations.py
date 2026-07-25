"""Durable, encrypted PagerDuty OAuth revocation retries."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.core.encryption import (
    decrypt_value,
    encrypt_value,
    is_v1_ciphertext,
)
from dev_health_ops.models.settings import ProviderOAuthRevocation
from dev_health_ops.providers.pagerduty.oauth import PagerDutyOAuthConfig, revoke_token


@dataclass(frozen=True, slots=True)
class PendingOAuthRevocation:
    """A non-secret summary of one pending remote revoke operation."""

    id: uuid.UUID
    attempts: int


class PagerDutyOAuthRevocationRepository:
    """Persists retryable revocations separately from replaceable OAuth grants."""

    def __init__(
        self, session: AsyncSession, org_id: str, credential_name: str = "default"
    ) -> None:
        self._session = session
        self._org_id = org_id
        self._credential_name = credential_name

    async def enqueue(self, token: str, *, purpose: str) -> PendingOAuthRevocation:
        """Durably retain a token before any local grant mutation can lose it."""
        now = datetime.now(UTC)
        ciphertext = encrypt_value(token)
        record = ProviderOAuthRevocation(
            id=uuid.uuid4(),
            org_id=self._org_id,
            provider="pagerduty",
            credential_name=self._credential_name,
            purpose=purpose,
            token_encrypted=ciphertext,
            token_key_version="v1" if is_v1_ciphertext(ciphertext) else "v0",
            status="pending",
            attempts=0,
            created_at=now,
            updated_at=now,
        )
        self._session.add(record)
        await self._session.flush()
        return PendingOAuthRevocation(id=record.id, attempts=record.attempts)

    async def retry_pending(self, config: PagerDutyOAuthConfig) -> bool:
        """Retry all stored revocations and retain failures for a later attempt."""
        rows = list(
            (
                await self._session.execute(
                    select(ProviderOAuthRevocation)
                    .where(
                        ProviderOAuthRevocation.org_id == self._org_id,
                        ProviderOAuthRevocation.provider == "pagerduty",
                        ProviderOAuthRevocation.credential_name
                        == self._credential_name,
                        ProviderOAuthRevocation.status == "pending",
                    )
                    .order_by(ProviderOAuthRevocation.created_at)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        is_complete = True
        for row in rows:
            try:
                await revoke_token(config, decrypt_value(row.token_encrypted))
            except (ValueError, httpx.HTTPError):
                row.attempts += 1
                row.last_error = "remote_revoke_failed"
                row.updated_at = datetime.now(UTC)
                is_complete = False
            else:
                await self._session.delete(row)
        await self._session.flush()
        return is_complete
