"""Encrypted, optimistic PagerDuty OAuth token persistence."""

from dataclasses import dataclass

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.core.encryption import decrypt_value, encrypt_value
from dev_health_ops.models.settings import ProviderOAuthCredential
from dev_health_ops.providers.pagerduty.oauth import OAuthTokens


class OAuthRotationConflictError(RuntimeError):
    """Raised when another refresh has already rotated the credential."""


@dataclass(frozen=True, slots=True)
class VersionedOAuthTokens:
    tokens: OAuthTokens
    version: int


class PagerDutyOAuthCredentialRepository:
    """Persists one encrypted PagerDuty OAuth payload per named organization credential."""

    def __init__(
        self, session: AsyncSession, org_id: str, credential_name: str = "default"
    ) -> None:
        self._session = session
        self._org_id = org_id
        self._credential_name = credential_name

    async def rotate(self, current_version: int, tokens: OAuthTokens) -> int:
        payload = encrypt_value(tokens.model_dump_json())
        statement = (
            update(ProviderOAuthCredential)
            .where(
                ProviderOAuthCredential.org_id == self._org_id,
                ProviderOAuthCredential.provider == "pagerduty",
                ProviderOAuthCredential.credential_name == self._credential_name,
                ProviderOAuthCredential.version == current_version,
            )
            .values(token_encrypted=payload, version=current_version + 1)
            .returning(ProviderOAuthCredential.version)
        )
        result = await self._session.execute(statement)
        version = result.scalar_one_or_none()
        if version is None:
            raise OAuthRotationConflictError("PagerDuty OAuth token rotation conflict")
        await self._session.flush()
        return version

    async def get(self) -> VersionedOAuthTokens | None:
        credential = await self._session.get(
            ProviderOAuthCredential, (self._org_id, "pagerduty", self._credential_name)
        )
        if credential is None:
            return None
        return VersionedOAuthTokens(
            OAuthTokens.model_validate_json(decrypt_value(credential.token_encrypted)),
            credential.version,
        )
