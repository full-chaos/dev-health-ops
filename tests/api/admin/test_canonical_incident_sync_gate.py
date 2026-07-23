from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import TypedDict

import pytest
import pytest_asyncio
from sqlalchemy import delete, func, select, text

from dev_health_ops.api.admin.routers import sync as sync_router
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.integrations import SyncRun
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride
from dev_health_ops.models.settings import JobRun, SyncConfiguration
from tests.api.admin.canonical_incident_sync_support import (
    FEATURE_KEY,
    ApiState,
    api_client,
    canonical_api_state_context,
    seed_operational_config,
    seed_repository_config,
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
async def test_sync_target_discovery_is_org_scoped(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state

    # When
    async with api_client(state, state.enabled) as enabled_client:
        enabled = await enabled_client.get("/api/v1/admin/sync-targets")
    async with api_client(state, state.disabled) as disabled_client:
        disabled = await disabled_client.get("/api/v1/admin/sync-targets")

    # Then
    assert enabled.status_code == 200
    assert disabled.status_code == 200
    assert "incidents" in enabled.json()["gitlab"]
    assert "incidents" not in disabled.json()["gitlab"]
    assert disabled.json()["gitlab"]


@pytest.mark.asyncio
async def test_sync_config_create_succeeds_by_default_without_override(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state

    # When
    async with api_client(state, state.enabled) as client:
        response = await client.post(
            "/api/v1/admin/sync-configs",
            json=_operational_payload("enabled-create"),
        )

    # Then
    assert response.status_code == 201, response.text


@pytest.mark.asyncio
async def test_sync_config_create_succeeds_without_override(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    async with state.session_maker() as session:
        await session.execute(
            delete(OrgFeatureOverride).where(
                OrgFeatureOverride.org_id == state.disabled.org_id,
                OrgFeatureOverride.feature_id == state.feature_id,
            )
        )
        await session.commit()

    # When
    async with api_client(state, state.disabled) as client:
        response = await client.post(
            "/api/v1/admin/sync-configs",
            json=_operational_payload("default-without-override"),
        )

    # Then
    assert response.status_code == 201, response.text


@pytest.mark.asyncio
async def test_sync_config_create_fails_for_false_override(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    # When
    async with api_client(state, state.disabled) as client:
        response = await client.post(
            "/api/v1/admin/sync-configs",
            json=_operational_payload("false-override"),
        )

    # Then
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_sync_config_create_fails_when_feature_row_is_missing(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    async with state.session_maker() as session:
        await session.execute(delete(FeatureFlag).where(FeatureFlag.key == FEATURE_KEY))
        await session.commit()

    # When
    async with api_client(state, state.disabled) as client:
        response = await client.post(
            "/api/v1/admin/sync-configs",
            json=_operational_payload("missing-feature-row"),
        )

    # Then
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_sync_config_create_fails_closed_on_feature_storage_error(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    async with state.session_maker() as session:
        await session.execute(text("DROP TABLE feature_flags"))
        await session.commit()

    # When
    async with api_client(state, state.disabled) as client:
        response = await client.post(
            "/api/v1/admin/sync-configs",
            json=_operational_payload("storage-error"),
        )

    # Then
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_existing_sync_config_remains_visible_when_feature_disabled(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    config_id = await seed_operational_config(state, state.disabled)

    # When
    async with api_client(state, state.disabled) as client:
        response = await client.get(f"/api/v1/admin/sync-configs/{config_id}")

    # Then
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_sync_config_update_is_denied_without_mutation(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    config_id = await seed_operational_config(state, state.disabled)

    # When
    async with api_client(state, state.disabled) as client:
        response = await client.patch(
            f"/api/v1/admin/sync-configs/{config_id}",
            json={"is_active": False},
        )

    # Then
    assert response.status_code == 403
    async with state.session_maker() as session:
        config = await session.get(SyncConfiguration, config_id)
    assert config is not None and config.is_active is True


@pytest.mark.asyncio
async def test_sync_config_trigger_is_denied_before_work_creation(
    canonical_api_state: ApiState,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    state = canonical_api_state
    config_id = await seed_operational_config(state, state.disabled)
    monkeypatch.setattr(
        sync_router.dispatch_sync_run,
        "apply_async",
        lambda **_kwargs: SimpleNamespace(id="dispatch"),
    )

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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    state = canonical_api_state
    config_id = await seed_operational_config(state, state.disabled)
    monkeypatch.setattr(
        sync_router.dispatch_sync_run,
        "apply_async",
        lambda **_kwargs: SimpleNamespace(id="dispatch"),
    )

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


@pytest.mark.asyncio
async def test_repository_replacement_is_denied_without_mutation(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    config_id = await seed_repository_config(state, state.disabled)

    # When
    async with api_client(state, state.disabled) as client:
        response = await client.put(
            f"/api/v1/admin/sync-configs/{config_id}/repositories",
            json={"owner": "acme", "repos": ["acme/beta"]},
        )

    # Then
    assert response.status_code == 403
    async with state.session_maker() as session:
        sources = (
            await session.execute(
                select(sync_router.IntegrationSource)
                .where(sync_router.IntegrationSource.integration_id.is_not(None))
                .order_by(sync_router.IntegrationSource.external_id)
            )
        ).scalars()
    assert [(source.external_id, source.is_enabled) for source in sources] == [
        ("acme/alpha", True),
        ("acme/beta", True),
    ]


@pytest.mark.asyncio
async def test_repository_replacement_hides_cross_org_config(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    config_id = await seed_repository_config(state, state.disabled)

    # When
    async with api_client(state, state.enabled) as client:
        response = await client.put(
            f"/api/v1/admin/sync-configs/{config_id}/repositories",
            json={"owner": "acme", "repos": ["acme/beta"]},
        )

    # Then
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_repository_replacement_fails_closed_on_storage_error(
    canonical_api_state: ApiState,
) -> None:
    # Given
    state = canonical_api_state
    config_id = await seed_repository_config(state, state.enabled)
    async with state.session_maker() as session:
        await session.execute(text("DROP TABLE feature_flags"))
        await session.commit()

    # When
    async with api_client(state, state.enabled) as client:
        response = await client.put(
            f"/api/v1/admin/sync-configs/{config_id}/repositories",
            json={"owner": "acme", "repos": ["acme/beta"]},
        )

    # Then
    assert response.status_code == 403
