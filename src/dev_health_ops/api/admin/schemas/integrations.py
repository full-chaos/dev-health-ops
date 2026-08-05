"""Pydantic schemas for the integration admin API."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------


class IntegrationCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    provider: str = Field(..., min_length=1)
    credential_id: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    is_active: bool = True
    schedule_cron: str | None = None
    timezone: str | None = None


class IntegrationUpdate(BaseModel):
    name: str | None = None
    credential_id: str | None = None
    config: dict[str, Any] | None = None
    is_active: bool | None = None
    schedule_cron: str | None = None
    timezone: str | None = None


class IntegrationResponse(BaseModel):
    id: str
    org_id: str
    provider: str
    credential_id: str | None
    name: str
    config: dict[str, Any]
    is_active: bool
    schedule_cron: str | None
    timezone: str | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


class IntegrationSourceResponse(BaseModel):
    id: str
    org_id: str
    integration_id: str
    provider: str
    source_type: str
    external_id: str
    name: str
    full_name: str
    metadata_: dict[str, Any] = Field(alias="metadata")
    is_enabled: bool
    discovered_at: datetime
    last_seen_at: datetime
    last_sync_at: datetime | None
    last_sync_success: bool | None
    last_sync_error: str | None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class IntegrationSourceUpdate(BaseModel):
    is_enabled: bool


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class IntegrationDatasetResponse(BaseModel):
    id: str
    org_id: str
    integration_id: str
    dataset_key: str
    is_enabled: bool
    options: dict[str, Any]

    model_config = ConfigDict(from_attributes=True)


class IntegrationDatasetUpdate(BaseModel):
    dataset_key: str
    is_enabled: bool


class IntegrationDatasetBatchUpdate(BaseModel):
    datasets: list[IntegrationDatasetUpdate]


# ---------------------------------------------------------------------------
# Discover
# ---------------------------------------------------------------------------


class DiscoverResponse(BaseModel):
    integration_id: str
    discovered: int
    sources: list[IntegrationSourceResponse]


# ---------------------------------------------------------------------------
# Sync / Backfill trigger
# ---------------------------------------------------------------------------


class SyncTriggerRequest(BaseModel):
    source_ids: list[str] | None = None
    dataset_keys: list[str] | None = None
    full_resync: bool = False


class BackfillTriggerRequest(BaseModel):
    since: datetime
    before: datetime
    source_ids: list[str] | None = None
    dataset_keys: list[str] | None = None


class SyncTriggerResponse(BaseModel):
    status: str
    integration_id: str
    sync_run_id: str
    total_units: int


# ---------------------------------------------------------------------------
# Sync run status
# ---------------------------------------------------------------------------


class SyncRunResponse(BaseModel):
    id: str
    org_id: str
    integration_id: str
    triggered_by: str
    mode: str
    status: str
    total_units: int
    completed_units: int
    failed_units: int
    started_at: datetime | None
    completed_at: datetime | None
    result: dict[str, Any] | None
    error: str | None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class SyncRunDatasetFreshness(BaseModel):
    """Watermark-vs-now lag for one (source, dataset) pair (CHAOS-3430).

    Read from the existing ``sync_watermarks`` rows at response time — the
    incremental window ratchet finalizes each capped HEAVY tick as an ordinary
    successful run, so run status alone cannot say how far behind the dataset
    still is.  ``catching_up`` is set only for a HEAVY dataset trailing by
    strictly more than ``window_cap_days``; every other dataset still reports
    an honest ``lag_seconds`` without the verdict.
    """

    #: IntegrationSource UUID, matching ``SyncRunUnitResponse.source_id`` so a
    #: caller can reuse an already-resolved source label.
    source_id: str
    source_name: str | None = None
    dataset_key: str
    cost_class: str
    #: Stored watermark in UTC; ``None`` when the dataset has never stamped one.
    watermark_at: datetime | None = None
    #: ``now - watermark_at`` in whole seconds; ``None`` without a watermark.
    lag_seconds: int | None = None
    catching_up: bool = False
    #: Scheduled ticks still needed to reach ``now`` at one capped window per
    #: tick; ``None`` unless ``catching_up``.
    ticks_behind: int | None = None
    window_cap_days: int


class SyncRunUnitSummary(BaseModel):
    """Rollup of SyncRunUnit rows for the run-status UI (CHAOS-2519)."""

    by_status: dict[str, int]
    by_source: dict[str, dict[str, int]]
    by_dataset: dict[str, dict[str, int]]
    by_cost_class: dict[str, int]
    slowest_unit_ids: list[str] = Field(default_factory=list)
    failed_unit_ids: list[str] = Field(default_factory=list)
    failed_unit_count: int = 0
    unit_count: int = 0
    partial_failure_summary: dict[str, Any] | None = None
    next_retry_at: datetime | None = None
    retry_exhausted_unit_count: int = 0
    # CHAOS-3412: units the sync budget guard is currently holding back
    # (status 'retrying' AND error_category 'budget_deferred'). Distinct
    # from failed_unit_count: these have not failed, they are BLOCKED --
    # the state that used to be invisible because nothing counted it.
    budget_blocked_unit_count: int = 0
    # CHAOS-3430: watermark lag per (source, dataset), and how many of those
    # pairs are a HEAVY dataset still ratcheting toward the current time.
    dataset_freshness: list[SyncRunDatasetFreshness] = Field(default_factory=list)
    catching_up_dataset_count: int = 0
    # The scope those two describe. "run" means they cover ONLY the (source,
    # dataset) pairs THIS run planned -- so a manually filtered or single-source
    # run reporting zero datasets catching up is not a statement about the
    # workspace, and must not be rendered as one. An org-wide freshness surface
    # is tracked separately (CHAOS-3438); this field exists so a consumer can
    # tell which question it is holding the answer to.
    dataset_freshness_scope: str = "run"
    units: list[SyncRunUnitResponse]


class SyncRunUnitResponse(BaseModel):
    id: str
    org_id: str
    sync_run_id: str
    integration_id: str
    source_id: str
    source_name: str | None
    source_full_name: str | None
    provider: str
    dataset_key: str
    cost_class: str
    mode: str
    since_at: datetime | None
    before_at: datetime | None
    status: str
    attempts: int
    available_at: datetime | None
    rate_limit_deferrals: int
    budget_deferrals: int = 0
    duration_seconds: int | None
    error: str | None
    error_category: str | None
    last_heartbeat_at: datetime | None
    result: Any | None
    retry_count: int | None = None
    retry_reason: str | None = None
    last_lease_expired_at: datetime | None = None
    next_retry_at: datetime | None = None
    retry_exhausted: bool | None = None
    retry_surfaces: list[str] | None = None
    linear_page_count: int | None = None
    linear_batch_count: int | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
