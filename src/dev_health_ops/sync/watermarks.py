from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from dev_health_ops.models.settings import SyncWatermark


def get_watermark(
    session: Session, org_id: str, source_id: str, dataset_key: str
) -> datetime | None:
    row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.source_id == source_id,
            SyncWatermark.dataset_key == dataset_key,
        )
        .one_or_none()
    )
    if row is None:
        return None
    return row.last_synced_at


def set_watermark(
    session: Session,
    org_id: str,
    source_id: str,
    dataset_key: str,
    timestamp: datetime,
) -> None:
    row = (
        session.query(SyncWatermark)
        .filter(
            SyncWatermark.org_id == org_id,
            SyncWatermark.source_id == source_id,
            SyncWatermark.dataset_key == dataset_key,
        )
        .one_or_none()
    )
    if row is None:
        row = SyncWatermark(
            repo_id=source_id,
            target=dataset_key,
            org_id=org_id,
            source_id=source_id,
            dataset_key=dataset_key,
            last_synced_at=timestamp,
        )
        session.add(row)
    else:
        row.last_synced_at = timestamp
        row.updated_at = datetime.now(timezone.utc)
    session.flush()


def get_legacy_repo_watermark(
    session: Session, org_id: str, repo_id: str, target: str
) -> datetime | None:
    return get_watermark(session, org_id, repo_id, target)


def set_legacy_repo_watermark(
    session: Session,
    org_id: str,
    repo_id: str,
    target: str,
    timestamp: datetime,
) -> None:
    set_watermark(session, org_id, repo_id, target, timestamp)
