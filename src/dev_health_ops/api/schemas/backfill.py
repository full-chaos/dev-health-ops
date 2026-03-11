from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


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


class BackfillJobListResponse(BaseModel):
    items: list[BackfillJobResponse]
    total: int
    limit: int
    offset: int
