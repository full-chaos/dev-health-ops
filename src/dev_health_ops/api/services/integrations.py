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
from dev_health_ops.models.settings import IntegrationCredential


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

    async def _resolve_credential_id(
        self, credential_id: str | None, provider: str
    ) -> uuid.UUID | None:
        """Validate a credential belongs to this org + provider.

        Prevents persisting a cross-org or provider-mismatched credential FK
        (a tenant must not reference another tenant's credential UUID).
        """
        if credential_id is None:
            return None
        try:
            uid = uuid.UUID(credential_id)
        except ValueError as exc:
            raise ValueError(f"Invalid credential_id: {credential_id}") from exc
        result = await self._session.execute(
            select(IntegrationCredential.id).where(
                IntegrationCredential.id == uid,
                IntegrationCredential.org_id == self._org_id,
                IntegrationCredential.provider == provider,
            )
        )
        if result.scalar_one_or_none() is None:
            raise ValueError(
                "credential_id does not reference a credential for this "
                "organization and provider"
            )
        return uid

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
            credential_id=await self._resolve_credential_id(credential_id, provider),
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
            integration.name = name
        if credential_id is not None:
            integration.credential_id = await self._resolve_credential_id(
                credential_id, str(integration.provider)
            )
        if config is not None:
            integration.config = config
        if is_active is not None:
            integration.is_active = is_active
        if schedule_cron is not None:
            integration.schedule_cron = schedule_cron
        if timezone is not None:
            integration.timezone = timezone
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
        source.is_enabled = enabled
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
        dataset.is_enabled = enabled
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
        slowest_limit: int = 5,
        failed_id_cap: int = 100,
    ) -> dict[str, Any]:
        """Build rollup dicts for the run-status UI (CHAOS-2519).

        Returns:
            by_status       – {status: count}
            by_source       – {source_id: {status: count}}
            by_dataset      – {dataset_key: {status: count}}
            by_cost_class   – {cost_class: count}
            slowest_units   – up to ``slowest_limit`` unit IDs sorted by
                              duration_seconds descending (terminal only)
            failed_unit_ids – IDs of all failed units
            partial_failure_summary – None when all succeeded/all failed;
                              dict with failed_sources/failed_datasets/
                              error_categories when the run is partial.
        """
        by_status: dict[str, int] = defaultdict(int)
        by_source: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        by_dataset: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        by_cost_class: dict[str, int] = defaultdict(int)

        failed_unit_ids: list[str] = []
        failed_unit_count = 0
        timed_units: list[tuple[int, str]] = []  # (duration_seconds, unit_id)
        failed_sources: set[str] = set()
        failed_datasets: set[str] = set()
        error_categories: dict[str, int] = defaultdict(int)

        for unit in units:
            status = str(unit.status)
            source = str(unit.source_id)
            dataset = str(unit.dataset_key)
            cost = str(unit.cost_class)
            unit_id = str(unit.id)

            by_status[status] += 1
            by_source[source][status] += 1
            by_dataset[dataset][status] += 1
            by_cost_class[cost] += 1

            if status == "failed":
                failed_unit_count += 1
                if len(failed_unit_ids) < failed_id_cap:
                    failed_unit_ids.append(unit_id)
                failed_sources.add(source)
                failed_datasets.add(dataset)
                # Extract error_category from result JSON if present
                result_data = unit.result or {}
                cat = result_data.get("error_category", "unknown")
                error_categories[str(cat)] += 1

            if unit.duration_seconds is not None:
                timed_units.append((int(unit.duration_seconds), unit_id))

        timed_units.sort(key=lambda x: x[0], reverse=True)
        slowest_unit_ids = [uid for _, uid in timed_units[:slowest_limit]]

        success_count = by_status.get("success", 0)
        failed_count = by_status.get("failed", 0)
        is_partial = success_count > 0 and failed_count > 0
        partial_failure_summary: dict[str, Any] | None = None
        if is_partial:
            partial_failure_summary = {
                "failed_sources": sorted(failed_sources),
                "failed_datasets": sorted(failed_datasets),
                "error_categories": dict(error_categories),
            }

        return {
            "by_status": dict(by_status),
            "by_source": {k: dict(v) for k, v in by_source.items()},
            "by_dataset": {k: dict(v) for k, v in by_dataset.items()},
            "by_cost_class": dict(by_cost_class),
            "slowest_unit_ids": slowest_unit_ids,
            "failed_unit_ids": failed_unit_ids,
            "failed_unit_count": failed_unit_count,
            "partial_failure_summary": partial_failure_summary,
        }
