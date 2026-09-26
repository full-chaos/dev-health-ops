from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import TypedDict

import pytest
import pytest_asyncio
from sqlalchemy import func, select

from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.integrations import SyncRun
from dev_health_ops.models.settings import JobRun
from tests.api.admin.canonical_incident_sync_support import (
    ApiState,
    api_client,
    canonical_api_state_context,
    seed_operational_config,
)


@pytest_asyncio.fixture
async def canonical_api_state(tmp_path: Path) -> AsyncIterator[ApiState]:
    async with canonical_api_state_context(tmp_path) as state:
        yield state


class _SyncConfigPayload(TypedDict):
    name: str
    provider: str
    sync_targets: list[str]
    sync_options: dict[str, str]


def _operational_payload(name: str) -> _SyncConfigPayload:
    return {
        "name": name,
        "provider": "pagerduty",
        "sync_targets": ["operational"],
        "sync_options": {},
    }


@pytest.mark.asyncio
async def test_sync_config_trigger_is_denied_before_work_creation(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    config_id = await seed_operational_config(state, state.disabled)
    # When
    async with api_client(state, state.disabled) as client:
        response = await client.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    # Then
    assert response.status_code == 403
    async with state.session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(SyncRun)) == 0
        assert await session.scalar(select(func.count()).select_from(JobRun)) == 0


@pytest.mark.asyncio
async def test_sync_config_backfill_is_denied_before_work_creation(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    config_id = await seed_operational_config(state, state.disabled)
    # When
    async with api_client(state, state.disabled) as client:
        response = await client.post(
            f"/api/v1/admin/sync-configs/{config_id}/backfill",
            json={"since": "2026-07-01", "before": "2026-07-02"},
        )

    # Then
    assert response.status_code == 403
    async with state.session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(SyncRun)) == 0
        assert await session.scalar(select(func.count()).select_from(BackfillJob)) == 0
