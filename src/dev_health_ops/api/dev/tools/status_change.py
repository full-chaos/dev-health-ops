"""Named V1 tool facade over the shared status/change application service."""

from __future__ import annotations

from dataclasses import dataclass

from ..status_change_service import (
    ChangeSummaryRequest,
    ChangeSummaryResult,
    StatusChangeService,
    StatusSnapshotRequest,
    StatusSnapshotResult,
)


@dataclass(frozen=True, slots=True)
class AskDevStatusChangeTools:
    service: StatusChangeService

    async def status_snapshot_v1(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        request: StatusSnapshotRequest,
    ) -> StatusSnapshotResult:
        return await self.service.status_snapshot(
            org_id, permission_fingerprint, request
        )

    async def change_summary_v1(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        request: ChangeSummaryRequest,
    ) -> ChangeSummaryResult:
        return await self.service.change_summary(
            org_id, permission_fingerprint, request
        )
