"""Server-side, one-time PKCE authorization-request storage for PagerDuty."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.core.encryption import decrypt_value, encrypt_value
from dev_health_ops.models.settings import PagerDutyOAuthAuthorizationRequest


def _utc_time(now: datetime | None) -> datetime:
    timestamp = now or datetime.now(UTC)
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class ConsumedAuthorizationRequest:
    """Authorization context available only after a successful one-time consume."""

    credential_name: str
    code_verifier: str
    enabled_datasets: list[str]
    region: str
    subdomain: str | None


class PagerDutyAuthorizationRequestStore:
    """Persist encrypted PKCE context keyed only by an opaque state's SHA-256 hash."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        org_id: str,
        state: str,
        credential_name: str,
        code_verifier: str,
        enabled_datasets: list[str],
        region: str,
        subdomain: str | None = None,
        initiated_by: str | None = None,
        ttl: timedelta = timedelta(minutes=15),
        now: datetime | None = None,
    ) -> None:
        """Store encrypted PKCE context for a short-lived OAuth authorization request."""
        created_at = _utc_time(now)
        await self._session.execute(
            delete(PagerDutyOAuthAuthorizationRequest).where(
                PagerDutyOAuthAuthorizationRequest.org_id == org_id,
                PagerDutyOAuthAuthorizationRequest.expires_at <= created_at,
            )
        )
        self._session.add(
            PagerDutyOAuthAuthorizationRequest(
                state_hash=hashlib.sha256(state.encode()).hexdigest(),
                org_id=org_id,
                credential_name=credential_name,
                code_verifier_encrypted=encrypt_value(code_verifier),
                enabled_datasets=enabled_datasets,
                region=region,
                subdomain=subdomain,
                initiated_by=initiated_by,
                created_at=created_at,
                expires_at=created_at + ttl,
            )
        )
        await self._session.flush()

    async def consume(
        self,
        *,
        org_id: str,
        state: str,
        now: datetime | None = None,
    ) -> ConsumedAuthorizationRequest | None:
        """Atomically consume an unexpired request scoped to its organization."""
        consumed_at = _utc_time(now)
        result = await self._session.execute(
            delete(PagerDutyOAuthAuthorizationRequest)
            .where(
                PagerDutyOAuthAuthorizationRequest.state_hash
                == hashlib.sha256(state.encode()).hexdigest(),
                PagerDutyOAuthAuthorizationRequest.org_id == org_id,
            )
            .returning(PagerDutyOAuthAuthorizationRequest)
        )
        authorization_request = result.scalar_one_or_none()
        await self._session.flush()

        if authorization_request is None:
            return None
        if _utc_time(authorization_request.expires_at) <= consumed_at:
            return None

        return ConsumedAuthorizationRequest(
            credential_name=authorization_request.credential_name,
            code_verifier=decrypt_value(authorization_request.code_verifier_encrypted),
            enabled_datasets=authorization_request.enabled_datasets,
            region=authorization_request.region,
            subdomain=authorization_request.subdomain,
        )

    async def purge_expired(self, *, now: datetime | None = None) -> int:
        """Delete expired authorization requests and return the number removed."""
        expired_at = _utc_time(now)
        result = await self._session.execute(
            delete(PagerDutyOAuthAuthorizationRequest)
            .where(PagerDutyOAuthAuthorizationRequest.expires_at <= expired_at)
            .returning(PagerDutyOAuthAuthorizationRequest.state_hash)
        )
        await self._session.flush()
        return len(result.scalars().all())
