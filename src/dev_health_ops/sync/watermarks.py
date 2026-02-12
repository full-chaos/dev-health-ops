from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from dev_health_ops.models.settings import SyncWatermark


def get_watermark(
    session: Session, org_id: str, repo_id: str, target: str
) -> datetime | None:
    row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.repo_id == repo_id,
            SyncWatermark.target == target,
        )
        .one_or_none()
    )
    if row is None:
        return None
    return row.last_synced_at


def set_watermark(
    session: Session,
    org_id: str,
    repo_id: str,
    target: str,
    timestamp: datetime,
) -> None:
    row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.repo_id == repo_id,
            SyncWatermark.target == target,
        )
        .one_or_none()
    )
    if row is None:
        row = SyncWatermark(
            repo_id=repo_id,
            target=target,
            org_id=org_id,
            last_synced_at=timestamp,
        )
        session.add(row)
    else:
        row.last_synced_at = timestamp
        row.updated_at = datetime.now(timezone.utc)
    session.flush()
