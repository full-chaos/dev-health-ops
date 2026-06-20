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
    units: list[SyncRunUnitResponse]


class SyncRunUnitResponse(BaseModel):
    id: str
    org_id: str
    sync_run_id: str
    integration_id: str
    source_id: str
    provider: str
    dataset_key: str
    cost_class: str
    mode: str
    since_at: datetime | None
    before_at: datetime | None
    status: str
    attempts: int
    duration_seconds: int | None
    error: str | None
    result: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
