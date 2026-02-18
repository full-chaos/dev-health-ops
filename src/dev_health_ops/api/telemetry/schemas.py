from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class TelemetryStatus(BaseModel):
    opted_in: bool
    last_report_at: datetime | None = None


class TelemetryReport(BaseModel):
    total_organizations: int
    active_organizations: int
    total_users: int
    active_users: int
    total_repos: int
    total_sync_configs: int
    active_syncs_24h: int
    tier_distribution: dict[str, int]
    feature_usage: dict[str, int]
    version: str
    collected_at: datetime


class HeartbeatPayload(BaseModel):
    license_hash: str | None = None
    instance_id: str
    version: str
    org_count: int
    user_count: int
    tier: str
    uptime_seconds: float
    timestamp: datetime
