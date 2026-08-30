"""Service layer for the integration admin API."""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunUnit,
)
from dev_health_ops.models.settings import IntegrationCredential, SyncWatermark
from dev_health_ops.sync.datasets import get_dataset_spec


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
        if (source.metadata_ or {}).get("capped_by_repo_limit"):
            # ANY explicit operator enable/disable (codex review, CHAOS-4584
            # round 4 P1) supersedes the automatic repo-limit bookkeeping --
            # from this point it's an operator decision, not something
            # discovery's own recovery pass should ever touch again. If the
            # operator disabled it, discovery must not "helpfully" re-enable
            # it once headroom appears; if they enabled it, same rule.
            # Mirrors sync/discovery.py::set_source_enabled, the other
            # enable/disable entry point for the same rows.
            source.metadata_ = {
                k: v
                for k, v in (source.metadata_ or {}).items()
                if k != "capped_by_repo_limit"
            }
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

    async def create(
        self,
        integration_id: uuid.UUID,
        dataset_key: str,
        is_enabled: bool,
    ) -> IntegrationDataset:
        dataset = IntegrationDataset(
            org_id=self._org_id,
            integration_id=integration_id,
            dataset_key=dataset_key,
            is_enabled=is_enabled,
            options={},
        )
        try:
            async with self._session.begin_nested():
                self._session.add(dataset)
                await self._session.flush()
        except IntegrityError:
            existing = await self.get_by_key(str(integration_id), dataset_key)
            if existing is None:
                raise
            return await self.set_enabled(existing, is_enabled)
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


def _dataset_cost_class(provider: str, dataset_key: str, unit_cost_class: str) -> str:
    """Cost class for ONE dataset key, resolved per dataset rather than per unit.

    CHAOS-3430: a collapsed work-item-family composite is a single row carrying a
    single ``cost_class`` (``work-items`` is medium), but its children are
    registered independently -- ``work-item-labels`` is light. Attributing the
    composite's class to every child misreports the child, and would silently
    SUPPRESS a heavy child's catch-up verdict, because the flag keys on
    ``cost_class == "heavy"``.

    Resolves from the same provider-aware registry the rest of the sync stack
    uses, so a provider-specific cost class is honoured rather than guessed.
    Falls back to the unit's own class when the registry has no entry for the
    pair -- an unknown dataset is better reported with the run's class than
    dropped or crashed on.
    """
    spec = get_dataset_spec(provider, dataset_key)
    if spec is None:
        return unit_cost_class
    return str(spec.default_cost_class.value)


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
            .options(selectinload(SyncRunUnit.source))
            .where(
                SyncRunUnit.sync_run_id == uid,
                SyncRunUnit.org_id == self._org_id,
            )
            .order_by(SyncRunUnit.id)
        )
        return list(result.scalars().all())

    async def build_dataset_freshness(
        self,
        units: list[SyncRunUnit],
        *,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Watermark-vs-now lag per (source, dataset) for this run (CHAOS-3430).

        A HEAVY dataset syncing under the incremental window ratchet finalizes
        every capped tick as an ordinary SUCCESS while its watermark can still
        trail ``now`` by weeks, so run status alone reads "complete" mid
        catch-up.  This reads the existing ``sync_watermarks`` rows — no schema
        change, no new persisted state — and reports the lag alongside the run.

        One query loads every watermark row that could serve any of the run's
        (source, dataset) pairs; precedence is then applied in memory by
        :func:`~dev_health_ops.sync.watermark_lag.resolve_watermark`, which is
        held to ``get_watermark`` by a differential test.

        Entries are emitted only for datasets that actually carry a watermark
        (``WatermarkBehavior.INCREMENTAL``); a dataset that never stamps one
        has no lag to report and would read as permanently unknown.  Order is
        deterministic: source label, then source id, then dataset key.
        """
        from dev_health_ops.api.services.sync_coverage import _effective_dataset_keys
        from dev_health_ops.sync.datasets import WatermarkBehavior, _watermark_behavior
        from dev_health_ops.sync.watermark_lag import (
            build_watermark_index,
            compute_watermark_lag,
            heavy_max_window_days,
            heavy_net_advance_seconds,
        )

        # (source_uuid, dataset_key) -> (watermark source key, source, cost class)
        pairs: dict[tuple[str, str], tuple[str, Any, str]] = {}
        for unit in units:
            source = unit.source
            if source is None:
                # No source row: the watermark key (external_id) is unknown, so
                # there is nothing honest to report for this pair.
                continue
            # CHAOS-2721/CHAOS-3430: the planner collapses the work-item family
            # into ONE composite unit (dataset_key="work-items") carrying
            # family_dataset_* flags, while the worker advances a SEPARATE
            # watermark per enabled child. Reading the composite's raw key would
            # report a dataset nothing stamps and hide a stale child entirely,
            # so expand to the same effective keys the worker stamps and
            # sync_coverage measures — reusing that expansion rather than
            # restating it, so the three cannot drift.
            for dataset_key in _effective_dataset_keys(
                str(unit.dataset_key), unit.processor_flags
            ):
                if (
                    _watermark_behavior(dataset_key)
                    is not WatermarkBehavior.INCREMENTAL
                ):
                    continue
                pairs.setdefault(
                    (str(unit.source_id), dataset_key),
                    (
                        str(source.external_id),
                        source,
                        _dataset_cost_class(
                            str(unit.provider), dataset_key, str(unit.cost_class)
                        ),
                    ),
                )

        if not pairs:
            return []

        source_keys = {watermark_key for watermark_key, _, _ in pairs.values()}
        dataset_keys = {dataset_key for _, dataset_key in pairs}
        # Narrow the SELECT to values that could satisfy some tier: a row whose
        # dataset_key AND target are both outside this set can never win.
        lookup_values = build_watermark_index(()).relevant_lookup_values(dataset_keys)
        result = await self._session.execute(
            select(SyncWatermark).where(
                SyncWatermark.org_id == self._org_id,
                or_(
                    SyncWatermark.source_id.in_(source_keys),
                    SyncWatermark.repo_id.in_(source_keys),
                ),
                or_(
                    SyncWatermark.dataset_key.in_(lookup_values),
                    SyncWatermark.target.in_(lookup_values),
                ),
            )
        )
        # Index once: resolution is then O(1) per pair instead of a full scan
        # per pair per precedence tier.
        index = build_watermark_index(result.scalars().all())

        # Resolve the cap and the per-tick advance once, so every entry in one
        # response is judged against the same configuration.
        cap_days = heavy_max_window_days()
        net_advance = heavy_net_advance_seconds()
        current = now or datetime.now(timezone.utc)

        entries: list[dict[str, Any]] = []
        for (source_uuid, dataset_key), (
            watermark_key,
            source,
            cost_class,
        ) in pairs.items():
            lag = compute_watermark_lag(
                cost_class=cost_class,
                watermark_at=index.resolve(watermark_key, dataset_key),
                now=current,
                window_cap_days=cap_days,
                net_advance_seconds=net_advance,
            )
            entries.append(
                {
                    "source_id": source_uuid,
                    "source_name": source.full_name or source.name,
                    "dataset_key": dataset_key,
                    "cost_class": cost_class,
                    "watermark_at": lag.watermark_at,
                    "lag_seconds": lag.lag_seconds,
                    "catching_up": lag.catching_up,
                    "ticks_behind": lag.ticks_behind,
                    "window_cap_days": lag.window_cap_days,
                }
            )

        entries.sort(
            key=lambda e: (
                str(e["source_name"] or ""),
                str(e["source_id"]),
                str(e["dataset_key"]),
            )
        )
        return entries

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
                result_data = unit.result
                cat = (
                    result_data.get("error_category", "unknown")
                    if isinstance(result_data, dict)
                    else "unknown"
                )
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
