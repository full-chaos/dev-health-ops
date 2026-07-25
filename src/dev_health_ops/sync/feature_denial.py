from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final

from sqlalchemy import update
from sqlalchemy.orm import Session

from dev_health_ops.models import SyncRun, SyncRunStatus, SyncRunUnit, SyncRunUnitStatus
from dev_health_ops.sync.canonical_incident_gate import (
    FEATURE_DISABLED_ERROR_CATEGORY,
    CanonicalIncidentFeatureDisabledError,
)

_UNCLAIMED_DENIAL_STATUSES: Final = (
    SyncRunUnitStatus.PLANNED.value,
    SyncRunUnitStatus.RETRYING.value,
    SyncRunUnitStatus.DISPATCHING.value,
)


@dataclass(frozen=True, slots=True)
class FeatureDisabledRunTransition:
    failed_units: int
    running_units: int
    run_terminal: bool


@dataclass(frozen=True, slots=True)
class _RunningLease:
    unit_id: uuid.UUID
    owner: str | None


def terminalize_feature_disabled_run(
    session: Session,
    run: SyncRun,
    error: CanonicalIncidentFeatureDisabledError,
) -> FeatureDisabledRunTransition:
    now = datetime.now(timezone.utc)
    error_text = str(error)
    result_payload = {"error_category": FEATURE_DISABLED_ERROR_CATEGORY}
    running_leases = [
        _RunningLease(unit_id=unit_id, owner=lease_owner)
        for unit_id, lease_owner in session.query(
            SyncRunUnit.id,
            SyncRunUnit.lease_owner,
        )
        .filter(
            SyncRunUnit.sync_run_id == run.id,
            SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
        )
        .all()
    ]
    session.execute(
        update(SyncRunUnit)
        .where(
            SyncRunUnit.sync_run_id == run.id,
            SyncRunUnit.status.in_(_UNCLAIMED_DENIAL_STATUSES),
        )
        .values(
            status=SyncRunUnitStatus.FAILED.value,
            available_at=None,
            error=error_text,
            result=result_payload,
            lease_owner=None,
            lease_expires_at=None,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    for lease in running_leases:
        lease_owner_matches = (
            SyncRunUnit.lease_owner.is_(None)
            if lease.owner is None
            else SyncRunUnit.lease_owner == lease.owner
        )
        session.execute(
            update(SyncRunUnit)
            .where(
                SyncRunUnit.id == lease.unit_id,
                SyncRunUnit.sync_run_id == run.id,
                SyncRunUnit.status == SyncRunUnitStatus.RUNNING.value,
                lease_owner_matches,
            )
            .values(
                status=SyncRunUnitStatus.FAILED.value,
                available_at=None,
                error=error_text,
                result=result_payload,
                lease_owner=None,
                lease_expires_at=None,
                updated_at=now,
            )
            .execution_options(synchronize_session=False)
        )
    statuses = [
        status
        for (status,) in session.query(SyncRunUnit.status)
        .filter(SyncRunUnit.sync_run_id == run.id)
        .all()
    ]
    failed_units = statuses.count(SyncRunUnitStatus.FAILED.value)
    running_units = statuses.count(SyncRunUnitStatus.RUNNING.value)
    run.failed_units = failed_units
    run.completed_units = statuses.count(SyncRunUnitStatus.SUCCESS.value)
    run.error = error_text
    run.result = result_payload
    run_terminal = all(
        status
        in {
            SyncRunUnitStatus.SUCCESS.value,
            SyncRunUnitStatus.FAILED.value,
        }
        for status in statuses
    )
    if run_terminal:
        run.status = SyncRunStatus.FAILED.value
        run.completed_at = run.completed_at or now
    session.flush()
    return FeatureDisabledRunTransition(
        failed_units=failed_units,
        running_units=running_units,
        run_terminal=run_terminal,
    )
