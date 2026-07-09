from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class BackfillMetricsDiagnosticsBucket(BaseModel):
    """Row-count/missing-input summary for one day or an aggregate window.

    Shared shape for the CHAOS-2888 backfill diagnostics contract: the same
    fields appear per-day (``BackfillMetricsDiagnosticsDay``) and rolled up
    (``BackfillMetricsDiagnostics.aggregate``). ``reason_counts`` keys are
    the fixed vocabulary from ``metrics.compounding_risk``
    (``missing_rework_churn``, ``missing_complexity_delta``,
    ``missing_review_latency``, ``missing_ownership_signal``).
    """

    repo_metrics_rows: int
    repo_complexity_rows: int
    compounding_risk_rows: int
    compounding_risk_non_null_rows: int
    compounding_risk_unknown_rows: int
    reason_counts: dict[str, int]


class BackfillMetricsDiagnosticsDay(BackfillMetricsDiagnosticsBucket):
    day: date


class BackfillMetricsDiagnostics(BaseModel):
    """Metrics observability for one backfill job's date window.

    Populated for the detail endpoint (``GET /backfill-jobs/{job_id}``)
    from ClickHouse analytics reads only. List responses leave this
    ``None`` to stay cheap.
    """

    range_start: date
    range_end: date
    aggregate: BackfillMetricsDiagnosticsBucket
    per_day: list[BackfillMetricsDiagnosticsDay]


class BackfillJobResponse(BaseModel):
    id: str
    sync_config_id: str
    status: str
    since_date: date
    before_date: date
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    progress_pct: float
    error_message: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime
    metrics_diagnostics: BackfillMetricsDiagnostics | None = None


class BackfillJobListResponse(BaseModel):
    items: list[BackfillJobResponse]
    total: int
    limit: int
    offset: int
