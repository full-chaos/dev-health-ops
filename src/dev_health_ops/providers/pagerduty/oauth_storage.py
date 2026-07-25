"""Encrypted, optimistic PagerDuty OAuth token persistence."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import delete, select, update
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
    binding_id: str | None = None


@dataclass(frozen=True, slots=True)
class OAuthStatusMetadata:
    """Non-secret PagerDuty OAuth state for integration status surfaces."""

    binding_id: str | None
    expires_at: datetime | None
    granted_scopes: frozenset[str]
    has_refresh_token: bool
    account_id: str | None
    account_display: str | None
    version: int


@dataclass(frozen=True, slots=True)
class OAuthBindingReplacement:
    """The committed new binding and encrypted predecessor captured under one lock."""

    version: int
    replaced_tokens: OAuthTokens | None


class PagerDutyOAuthCredentialRepository:
    """Persists one encrypted PagerDuty OAuth payload per named organization credential."""

    def __init__(
        self,
        session: AsyncSession,
        org_id: str,
        credential_name: str = "default",
        *,
        expected_binding_id: str | None = None,
    ) -> None:
        self._session = session
        self._org_id = org_id
        self._credential_name = credential_name
        self._expected_binding_id = expected_binding_id

    async def rotate(
        self,
        current_version: int,
        tokens: OAuthTokens,
        *,
        expected_binding_id: str,
    ) -> int:
        """Atomically persist a refreshed token for its original OAuth binding."""
        payload = encrypt_value(tokens.model_dump_json())
        now = datetime.now(UTC)
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
                token_encrypted=payload,
                version=current_version + 1,
                expires_at=tokens.expires_at,
                granted_scopes=sorted(tokens.granted_scopes),
                has_refresh_token=bool(tokens.refresh_token),
                updated_at=now,
            )
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
            ProviderOAuthCredential,
            (self._org_id, "pagerduty", self._credential_name),
            populate_existing=True,
        )
        if credential is None:
            return None
        return self._versioned_tokens(credential)

    async def get_for_update(self) -> VersionedOAuthTokens | None:
        """Load and lock this credential for a refresh transaction."""
        credential = await self._locked_credential()
        if credential is None:
            return None
        return self._versioned_tokens(credential)

    def _versioned_tokens(
        self, credential: ProviderOAuthCredential
    ) -> VersionedOAuthTokens:
        if (
            self._expected_binding_id is not None
            and credential.binding_id != self._expected_binding_id
        ):
            raise OAuthRotationConflictError(
                "PagerDuty OAuth credential binding mismatch"
            )
        return VersionedOAuthTokens(
            OAuthTokens.model_validate_json(decrypt_value(credential.token_encrypted)),
            credential.version,
            credential.binding_id,
        )

    async def create_or_replace(
        self,
        tokens: OAuthTokens,
        *,
        binding_id: str,
        account_id: str | None = None,
        account_display: str | None = None,
        now: datetime | None = None,
    ) -> int:
        """Persist a newly connected OAuth binding, replacing any prior binding."""
        timestamp = now or datetime.now(UTC)
        credential = await self._locked_credential()
        payload = encrypt_value(tokens.model_dump_json())
        if credential is None:
            credential = ProviderOAuthCredential(
                org_id=self._org_id,
                provider="pagerduty",
                credential_name=self._credential_name,
                token_encrypted=payload,
                version=1,
                created_at=timestamp,
                updated_at=timestamp,
                binding_id=binding_id,
                expires_at=tokens.expires_at,
                granted_scopes=sorted(tokens.granted_scopes),
                has_refresh_token=bool(tokens.refresh_token),
                account_id=account_id,
                account_display=account_display,
            )
            self._session.add(credential)
        else:
            credential.token_encrypted = payload
            credential.version += 1
            credential.binding_id = binding_id
            credential.updated_at = timestamp
            credential.expires_at = tokens.expires_at
            credential.granted_scopes = sorted(tokens.granted_scopes)
            credential.has_refresh_token = bool(tokens.refresh_token)
            credential.account_id = account_id
            credential.account_display = account_display
        await self._session.flush()
        return credential.version

    async def replace_and_capture(
        self,
        tokens: OAuthTokens,
        *,
        binding_id: str,
        account_id: str | None = None,
        account_display: str | None = None,
        now: datetime | None = None,
    ) -> OAuthBindingReplacement:
        """Replace a grant while retaining its predecessor for durable revocation."""
        timestamp = now or datetime.now(UTC)
        credential = await self._locked_credential()
        previous = self._versioned_tokens(credential).tokens if credential else None
        payload = encrypt_value(tokens.model_dump_json())
        if credential is None:
            credential = ProviderOAuthCredential(
                org_id=self._org_id,
                provider="pagerduty",
                credential_name=self._credential_name,
                token_encrypted=payload,
                version=1,
                created_at=timestamp,
                updated_at=timestamp,
                binding_id=binding_id,
                expires_at=tokens.expires_at,
                granted_scopes=sorted(tokens.granted_scopes),
                has_refresh_token=bool(tokens.refresh_token),
                account_id=account_id,
                account_display=account_display,
            )
            self._session.add(credential)
        else:
            credential.token_encrypted = payload
            credential.version += 1
            credential.binding_id = binding_id
            credential.updated_at = timestamp
            credential.expires_at = tokens.expires_at
            credential.granted_scopes = sorted(tokens.granted_scopes)
            credential.has_refresh_token = bool(tokens.refresh_token)
            credential.account_id = account_id
            credential.account_display = account_display
        await self._session.flush()
        return OAuthBindingReplacement(credential.version, previous)

    async def get_status_metadata(self) -> OAuthStatusMetadata | None:
        """Read non-secret credential metadata without decrypting the token payload."""
        statement = select(
            ProviderOAuthCredential.binding_id,
            ProviderOAuthCredential.expires_at,
            ProviderOAuthCredential.granted_scopes,
            ProviderOAuthCredential.has_refresh_token,
            ProviderOAuthCredential.account_id,
            ProviderOAuthCredential.account_display,
            ProviderOAuthCredential.version,
        ).where(
            ProviderOAuthCredential.org_id == self._org_id,
            ProviderOAuthCredential.provider == "pagerduty",
            ProviderOAuthCredential.credential_name == self._credential_name,
        )
        result = await self._session.execute(statement)
        row = result.one_or_none()
        if row is None:
            return None
        return OAuthStatusMetadata(
            binding_id=row.binding_id,
            expires_at=row.expires_at,
            granted_scopes=frozenset(row.granted_scopes or ()),
            has_refresh_token=row.has_refresh_token,
            account_id=row.account_id,
            account_display=row.account_display,
            version=row.version,
        )

    async def delete(self) -> None:
        """Delete this credential without reading or logging its encrypted token."""
        statement = delete(ProviderOAuthCredential).where(
            ProviderOAuthCredential.org_id == self._org_id,
            ProviderOAuthCredential.provider == "pagerduty",
            ProviderOAuthCredential.credential_name == self._credential_name,
        )
        await self._session.execute(statement)
        await self._session.flush()

    async def _locked_credential(self) -> ProviderOAuthCredential | None:
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
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()
