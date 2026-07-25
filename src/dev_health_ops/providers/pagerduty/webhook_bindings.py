from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from dev_health_ops.core.encryption import (
    KEY_VERSION_PREFIX,
    decrypt_value,
    encrypt_value,
)
from dev_health_ops.models.integrations import Integration, IntegrationSource
from dev_health_ops.models.pagerduty_webhook_binding import PagerDutyWebhookBinding
from dev_health_ops.models.settings import IntegrationCredential

_ACTIVE_STATUS = "active"
_CANDIDATE_STATUS = "candidate"
_INACTIVE_STATUS = "inactive"
_READY_STATUS = "ready"
_CURRENT_KEY_VERSION = KEY_VERSION_PREFIX.removesuffix(":")


@dataclass(frozen=True, slots=True)
class CreatePagerDutyWebhookBinding:
    org_id: UUID
    integration_source_id: UUID
    credential_id: UUID
    provider_subscription_id: str
    signing_secret: str


@dataclass(frozen=True, slots=True)
class ResolvedPagerDutyWebhookBinding:
    binding: PagerDutyWebhookBinding
    signing_secret: str


class PagerDutyWebhookBindingInputError(ValueError):
    pass


class PagerDutyWebhookBindingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        input: CreatePagerDutyWebhookBinding,
        encrypted_secret: str,
        status: str,
    ) -> PagerDutyWebhookBinding:
        binding = PagerDutyWebhookBinding(
            org_id=input.org_id,
            integration_source_id=input.integration_source_id,
            credential_id=input.credential_id,
            provider_subscription_id=input.provider_subscription_id,
            signing_secret_encrypted=encrypted_secret,
            signing_secret_key_version=_CURRENT_KEY_VERSION,
            status=status,
        )
        self._session.add(binding)
        await self._session.flush()
        return binding

    async def active_by_id(self, binding_id: UUID) -> PagerDutyWebhookBinding | None:
        return await self._active_by(PagerDutyWebhookBinding.id == binding_id)

    async def by_id_for_org(
        self,
        binding_id: UUID,
        org_id: UUID,
    ) -> PagerDutyWebhookBinding | None:
        statement = select(PagerDutyWebhookBinding).where(
            PagerDutyWebhookBinding.id == binding_id,
            PagerDutyWebhookBinding.org_id == org_id,
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def by_id(self, binding_id: UUID) -> PagerDutyWebhookBinding | None:
        statement = select(PagerDutyWebhookBinding).where(
            PagerDutyWebhookBinding.id == binding_id
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def active_by_integration_source_id(
        self,
        integration_source_id: UUID,
    ) -> PagerDutyWebhookBinding | None:
        return await self._active_by(
            PagerDutyWebhookBinding.integration_source_id == integration_source_id
        )

    async def active_by_provider_subscription_id(
        self,
        provider_subscription_id: str,
    ) -> PagerDutyWebhookBinding | None:
        return await self._active_by(
            PagerDutyWebhookBinding.provider_subscription_id == provider_subscription_id
        )

    async def receivable_by_id(
        self,
        binding_id: UUID,
    ) -> PagerDutyWebhookBinding | None:
        statement = select(PagerDutyWebhookBinding).where(
            PagerDutyWebhookBinding.id == binding_id,
            PagerDutyWebhookBinding.status.in_(
                {_ACTIVE_STATUS, _CANDIDATE_STATUS, _READY_STATUS}
            ),
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def receivable_by_source_for_update(
        self,
        integration_source_id: UUID,
    ) -> list[PagerDutyWebhookBinding]:
        """Lock lifecycle rows in UUID order for all source transitions."""
        statement = (
            select(PagerDutyWebhookBinding)
            .where(
                PagerDutyWebhookBinding.integration_source_id == integration_source_id,
                PagerDutyWebhookBinding.status.in_(
                    {_ACTIVE_STATUS, _CANDIDATE_STATUS, _READY_STATUS}
                ),
            )
            .order_by(PagerDutyWebhookBinding.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        result = await self._session.execute(statement)
        return list(result.scalars().all())

    async def receivable_by_id_for_update(
        self,
        binding_id: UUID,
    ) -> PagerDutyWebhookBinding | None:
        statement = (
            select(PagerDutyWebhookBinding)
            .where(
                PagerDutyWebhookBinding.id == binding_id,
                PagerDutyWebhookBinding.status.in_(
                    {_ACTIVE_STATUS, _CANDIDATE_STATUS, _READY_STATUS}
                ),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def active_by_id_for_update(
        self,
        binding_id: UUID,
    ) -> PagerDutyWebhookBinding | None:
        statement = (
            select(PagerDutyWebhookBinding)
            .where(
                PagerDutyWebhookBinding.id == binding_id,
                PagerDutyWebhookBinding.status == _ACTIVE_STATUS,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def candidate_by_id_for_update(
        self,
        binding_id: UUID,
        org_id: UUID | None = None,
    ) -> PagerDutyWebhookBinding | None:
        predicates: list[ColumnElement[bool]] = [
            PagerDutyWebhookBinding.id == binding_id,
            PagerDutyWebhookBinding.status.in_({_CANDIDATE_STATUS, _READY_STATUS}),
        ]
        if org_id is not None:
            predicates.append(PagerDutyWebhookBinding.org_id == org_id)
        statement = (
            select(PagerDutyWebhookBinding)
            .where(*predicates)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def active_by_source_for_update(
        self,
        integration_source_id: UUID,
    ) -> PagerDutyWebhookBinding | None:
        statement = (
            select(PagerDutyWebhookBinding)
            .where(
                PagerDutyWebhookBinding.integration_source_id == integration_source_id,
                PagerDutyWebhookBinding.status == _ACTIVE_STATUS,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def candidate_by_source_for_update(
        self,
        integration_source_id: UUID,
    ) -> PagerDutyWebhookBinding | None:
        statement = (
            select(PagerDutyWebhookBinding)
            .where(
                PagerDutyWebhookBinding.integration_source_id == integration_source_id,
                PagerDutyWebhookBinding.status.in_({_CANDIDATE_STATUS, _READY_STATUS}),
            )
            .with_for_update()
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()

    async def by_credential_id_for_update(
        self,
        credential_id: UUID,
    ) -> list[PagerDutyWebhookBinding]:
        statement = (
            select(PagerDutyWebhookBinding)
            .where(PagerDutyWebhookBinding.credential_id == credential_id)
            .order_by(PagerDutyWebhookBinding.id)
            .with_for_update()
        )
        result = await self._session.execute(statement)
        return list(result.scalars().all())

    async def delete(self, binding: PagerDutyWebhookBinding) -> None:
        await self._session.delete(binding)

    async def save(self) -> None:
        await self._session.flush()

    async def _active_by(
        self,
        predicate: ColumnElement[bool],
    ) -> PagerDutyWebhookBinding | None:
        statement = select(PagerDutyWebhookBinding).where(
            predicate,
            PagerDutyWebhookBinding.status == _ACTIVE_STATUS,
        )
        result = await self._session.execute(statement)
        return result.scalar_one_or_none()


class PagerDutyWebhookBindingService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._repository = PagerDutyWebhookBindingRepository(session)

    async def create(
        self,
        input: CreatePagerDutyWebhookBinding,
    ) -> PagerDutyWebhookBinding:
        self._require_secret(input.signing_secret)
        self._require_subscription_id(input.provider_subscription_id)
        await self._require_active_pagerduty_graph(input)
        return await self._repository.create(
            input,
            encrypt_value(input.signing_secret),
            _CANDIDATE_STATUS,
        )

    async def load_active_by_id(
        self,
        binding_id: UUID,
    ) -> ResolvedPagerDutyWebhookBinding | None:
        binding = await self._repository.active_by_id(binding_id)
        return self._resolve(binding)

    async def load_by_id_for_org(
        self,
        binding_id: UUID,
        org_id: UUID,
    ) -> PagerDutyWebhookBinding | None:
        return await self._repository.by_id_for_org(binding_id, org_id)

    async def load_active_by_integration_source_id(
        self,
        integration_source_id: UUID,
    ) -> ResolvedPagerDutyWebhookBinding | None:
        binding = await self._repository.active_by_integration_source_id(
            integration_source_id
        )
        return self._resolve(binding)

    async def load_active_by_subscription_id(
        self,
        provider_subscription_id: str,
    ) -> ResolvedPagerDutyWebhookBinding | None:
        binding = await self._repository.active_by_provider_subscription_id(
            provider_subscription_id
        )
        return self._resolve(binding)

    async def load_receivable_by_id(
        self,
        binding_id: UUID,
    ) -> ResolvedPagerDutyWebhookBinding | None:
        """Resolve the route-addressed binding before trusting request headers."""
        binding = await self._repository.receivable_by_id(binding_id)
        return self._resolve(binding)

    async def create_rotation_candidate(
        self,
        active_binding_id: UUID,
        input: CreatePagerDutyWebhookBinding,
    ) -> PagerDutyWebhookBinding | None:
        """Create a separately encrypted candidate without mutating the active secret."""
        self._require_secret(input.signing_secret)
        self._require_subscription_id(input.provider_subscription_id)
        await self._require_active_pagerduty_graph(input)
        existing_active = await self._repository.active_by_id(active_binding_id)
        if existing_active is None:
            return None
        source_bindings = await self._repository.receivable_by_source_for_update(
            existing_active.integration_source_id
        )
        active = next(
            (
                binding
                for binding in source_bindings
                if binding.id == active_binding_id and binding.status == _ACTIVE_STATUS
            ),
            None,
        )
        if active is None:
            return None
        if (
            active.org_id != input.org_id
            or active.integration_source_id != input.integration_source_id
        ):
            raise PagerDutyWebhookBindingInputError(
                "rotation candidate must belong to the active binding source"
            )
        existing_candidate = next(
            (
                binding
                for binding in source_bindings
                if binding.status in {_CANDIDATE_STATUS, _READY_STATUS}
            ),
            None,
        )
        if existing_candidate is not None:
            raise PagerDutyWebhookBindingInputError(
                "a rotation candidate already exists for this source"
            )
        return await self._repository.create(
            input,
            encrypt_value(input.signing_secret),
            _CANDIDATE_STATUS,
        )

    async def mark_candidate_ready_from_verified_ping(
        self,
        candidate_binding_id: UUID,
        org_id: UUID | None = None,
    ) -> PagerDutyWebhookBinding | None:
        """Advance a candidate only after its receiver verifies a subscription ping."""
        candidate = await self._repository.candidate_by_id_for_update(
            candidate_binding_id, org_id
        )
        if candidate is None:
            return None
        if candidate.status == _CANDIDATE_STATUS:
            candidate.status = _READY_STATUS
            candidate.updated_at = datetime.now(UTC)
            await self._repository.save()
        return candidate

    async def cutover_ready_candidate(
        self,
        candidate_binding_id: UUID,
        org_id: UUID | None = None,
    ) -> PagerDutyWebhookBinding | None:
        """Atomically revoke the old binding and activate a ready replacement."""
        if org_id is None:
            candidate = await self._repository.by_id(candidate_binding_id)
        else:
            candidate = await self._repository.by_id_for_org(
                candidate_binding_id,
                org_id,
            )
        if candidate is None:
            return None
        source_bindings = await self._repository.receivable_by_source_for_update(
            candidate.integration_source_id
        )
        candidate = next(
            (
                binding
                for binding in source_bindings
                if binding.id == candidate_binding_id
            ),
            None,
        )
        if candidate is None or candidate.status != _READY_STATUS:
            return None
        timestamp = datetime.now(UTC)
        active = next(
            (
                binding
                for binding in source_bindings
                if binding.status == _ACTIVE_STATUS
            ),
            None,
        )
        if active is not None:
            active.status = _INACTIVE_STATUS
            active.revoked_at = timestamp
            active.rotated_at = timestamp
            active.updated_at = timestamp
            await self._repository.save()
        candidate.status = _ACTIVE_STATUS
        candidate.updated_at = timestamp
        await self._repository.save()
        return candidate

    async def revoke(
        self,
        binding_id: UUID,
        org_id: UUID | None = None,
    ) -> PagerDutyWebhookBinding | None:
        binding = await self._repository.receivable_by_id_for_update(binding_id)
        if binding is None or (org_id is not None and binding.org_id != org_id):
            return None
        timestamp = datetime.now(UTC)
        binding.status = _INACTIVE_STATUS
        if binding.revoked_at is None:
            binding.revoked_at = timestamp
        binding.updated_at = timestamp
        await self._repository.save()
        return binding

    async def revoke_and_detach_for_credential(
        self,
        credential_id: UUID,
    ) -> None:
        """Retain revoked binding history while detaching a deleted credential."""
        bindings = await self._repository.by_credential_id_for_update(credential_id)
        timestamp = datetime.now(UTC)
        for binding in bindings:
            binding.status = _INACTIVE_STATUS
            if binding.revoked_at is None:
                binding.revoked_at = timestamp
            binding.updated_at = timestamp
            binding.credential_id = None
        await self._repository.save()

    @staticmethod
    def _require_secret(signing_secret: str) -> None:
        if not signing_secret:
            raise PagerDutyWebhookBindingInputError("signing secret must not be empty")

    @staticmethod
    def _require_subscription_id(provider_subscription_id: str) -> None:
        if not provider_subscription_id.strip():
            raise PagerDutyWebhookBindingInputError(
                "provider subscription id must not be blank"
            )

    async def _require_active_pagerduty_graph(
        self,
        input: CreatePagerDutyWebhookBinding,
    ) -> None:
        """Trust only an active same-org PagerDuty integration/source/credential graph."""
        statement = (
            select(IntegrationSource, Integration, IntegrationCredential)
            .join(Integration, IntegrationSource.integration_id == Integration.id)
            .join(
                IntegrationCredential, IntegrationCredential.id == input.credential_id
            )
            .where(IntegrationSource.id == input.integration_source_id)
        )
        result = await self._session.execute(statement)
        row = result.one_or_none()
        if row is None:
            raise PagerDutyWebhookBindingInputError(
                "PagerDuty binding graph was not found"
            )
        source, integration, credential = row
        expected_org_id = str(input.org_id)
        if (
            source.org_id != expected_org_id
            or integration.org_id != expected_org_id
            or credential.org_id != expected_org_id
            or source.provider != "pagerduty"
            or integration.provider != "pagerduty"
            or credential.provider != "pagerduty"
            or integration.credential_id != credential.id
            or not source.is_enabled
            or not integration.is_active
            or not credential.is_active
        ):
            raise PagerDutyWebhookBindingInputError(
                "PagerDuty binding graph is not active and same-org"
            )

    @staticmethod
    def _resolve(
        binding: PagerDutyWebhookBinding | None,
    ) -> ResolvedPagerDutyWebhookBinding | None:
        if binding is None:
            return None
        return ResolvedPagerDutyWebhookBinding(
            binding=binding,
            signing_secret=decrypt_value(binding.signing_secret_encrypted),
        )
