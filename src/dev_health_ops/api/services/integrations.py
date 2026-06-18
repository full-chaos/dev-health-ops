"""Service layer for the integration admin API."""

from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunUnit,
)


class IntegrationService:
    """CRUD operations for Integration rows, scoped to an org."""

    def __init__(self, session: AsyncSession, org_id: str) -> None:
        self._session = session
        self._org_id = org_id

    async def list_all(self) -> list[Integration]:
        result = await self._session.execute(
            select(Integration)
            .where(Integration.org_id == self._org_id)
            .order_by(Integration.created_at.desc())
        )
        return list(result.scalars().all())

    async def get_by_id(self, integration_id: str) -> Integration | None:
        try:
            uid = uuid.UUID(integration_id)
        except ValueError:
            return None
        result = await self._session.execute(
            select(Integration).where(
                Integration.id == uid,
                Integration.org_id == self._org_id,
            )
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        *,
        name: str,
        provider: str,
        credential_id: str | None,
        config: dict[str, Any],
        is_active: bool,
        schedule_cron: str | None,
        timezone: str | None,
    ) -> Integration:
        integration = Integration(
            org_id=self._org_id,
            name=name,
            provider=provider,
            credential_id=uuid.UUID(credential_id) if credential_id else None,
            config=config,
            is_active=is_active,
            schedule_cron=schedule_cron,
            timezone=timezone,
        )
        self._session.add(integration)
        await self._session.flush()
        return integration

    async def update(
        self,
        integration: Integration,
        *,
        name: str | None = None,
        credential_id: str | None = None,
        config: dict[str, Any] | None = None,
        is_active: bool | None = None,
        schedule_cron: str | None = None,
        timezone: str | None = None,
    ) -> Integration:
        if name is not None:
            integration.name = name  # type: ignore[assignment]
        if credential_id is not None:
            integration.credential_id = uuid.UUID(credential_id)  # type: ignore[assignment]
        if config is not None:
            integration.config = config  # type: ignore[assignment]
        if is_active is not None:
            integration.is_active = is_active  # type: ignore[assignment]
        if schedule_cron is not None:
            integration.schedule_cron = schedule_cron  # type: ignore[assignment]
        if timezone is not None:
            integration.timezone = timezone  # type: ignore[assignment]
        await self._session.flush()
        return integration


class IntegrationSourceService:
    """Operations on IntegrationSource rows, scoped to an org."""

    def __init__(self, session: AsyncSession, org_id: str) -> None:
        self._session = session
        self._org_id = org_id

    async def list_for_integration(
        self, integration_id: str
    ) -> list[IntegrationSource]:
        try:
            uid = uuid.UUID(integration_id)
        except ValueError:
            return []
        result = await self._session.execute(
            select(IntegrationSource)
            .where(
                IntegrationSource.org_id == self._org_id,
                IntegrationSource.integration_id == uid,
            )
            .order_by(IntegrationSource.full_name, IntegrationSource.id)
        )
        return list(result.scalars().all())

    async def get_by_id(
        self, integration_id: str, source_id: str
    ) -> IntegrationSource | None:
        try:
            int_uid = uuid.UUID(integration_id)
            src_uid = uuid.UUID(source_id)
        except ValueError:
            return None
        result = await self._session.execute(
            select(IntegrationSource).where(
                IntegrationSource.id == src_uid,
                IntegrationSource.integration_id == int_uid,
                IntegrationSource.org_id == self._org_id,
            )
        )
        return result.scalar_one_or_none()

    async def set_enabled(
        self, source: IntegrationSource, enabled: bool
    ) -> IntegrationSource:
        source.is_enabled = enabled  # type: ignore[assignment]
        await self._session.flush()
        return source


class IntegrationDatasetService:
    """Operations on IntegrationDataset rows, scoped to an org."""

    def __init__(self, session: AsyncSession, org_id: str) -> None:
        self._session = session
        self._org_id = org_id

    async def list_for_integration(
        self, integration_id: str
    ) -> list[IntegrationDataset]:
        try:
            uid = uuid.UUID(integration_id)
        except ValueError:
            return []
        result = await self._session.execute(
            select(IntegrationDataset)
            .where(
                IntegrationDataset.org_id == self._org_id,
                IntegrationDataset.integration_id == uid,
            )
            .order_by(IntegrationDataset.dataset_key)
        )
        return list(result.scalars().all())

    async def set_enabled(
        self, dataset: IntegrationDataset, enabled: bool
    ) -> IntegrationDataset:
        dataset.is_enabled = enabled  # type: ignore[assignment]
        await self._session.flush()
        return dataset

    async def get_by_key(
        self, integration_id: str, dataset_key: str
    ) -> IntegrationDataset | None:
        try:
            uid = uuid.UUID(integration_id)
        except ValueError:
            return None
        result = await self._session.execute(
            select(IntegrationDataset).where(
                IntegrationDataset.org_id == self._org_id,
                IntegrationDataset.integration_id == uid,
                IntegrationDataset.dataset_key == dataset_key,
            )
        )
        return result.scalar_one_or_none()


class SyncRunService:
    """Read operations on SyncRun / SyncRunUnit rows, scoped to an org."""

    def __init__(self, session: AsyncSession, org_id: str) -> None:
        self._session = session
        self._org_id = org_id

    async def get_run(self, run_id: str) -> SyncRun | None:
        try:
            uid = uuid.UUID(run_id)
        except ValueError:
            return None
        result = await self._session.execute(
            select(SyncRun).where(
                SyncRun.id == uid,
                SyncRun.org_id == self._org_id,
            )
        )
        return result.scalar_one_or_none()

    async def list_units(self, run_id: str) -> list[SyncRunUnit]:
        try:
            uid = uuid.UUID(run_id)
        except ValueError:
            return []
        result = await self._session.execute(
            select(SyncRunUnit)
            .where(
                SyncRunUnit.sync_run_id == uid,
                SyncRunUnit.org_id == self._org_id,
            )
            .order_by(SyncRunUnit.id)
        )
        return list(result.scalars().all())

    @staticmethod
    def build_unit_rollups(
        units: list[SyncRunUnit],
    ) -> dict[str, Any]:
        """Build rollup dicts for the run-status UI."""
        by_status: dict[str, int] = defaultdict(int)
        by_source: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        by_dataset: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        by_cost_class: dict[str, int] = defaultdict(int)

        for unit in units:
            status = str(unit.status)
            source = str(unit.source_id)
            dataset = str(unit.dataset_key)
            cost = str(unit.cost_class)

            by_status[status] += 1
            by_source[source][status] += 1
            by_dataset[dataset][status] += 1
            by_cost_class[cost] += 1

        return {
            "by_status": dict(by_status),
            "by_source": {k: dict(v) for k, v in by_source.items()},
            "by_dataset": {k: dict(v) for k, v in by_dataset.items()},
            "by_cost_class": dict(by_cost_class),
        }
