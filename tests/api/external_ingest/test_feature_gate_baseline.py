from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient

from tests.api.test_external_ingest_router import (
    BASE,
    SCHEMA_VERSION,
    VALID_PAYLOADS,
    _envelope,
    _record,
    router_mod,
)

pytest_plugins = ("tests.api.test_external_ingest_router",)


@pytest.mark.asyncio
async def test_public_schema_etag_is_independent_of_bearer_identity(
    client: AsyncClient,
) -> None:
    # Given
    path = f"{BASE}/schemas/{SCHEMA_VERSION}"

    # When
    anonymous = await client.get(path)
    authenticated = await client.get(
        path,
        headers={"Authorization": "Bearer fcpush_shape-only-metadata"},
    )

    # Then
    assert anonymous.status_code == 200
    assert authenticated.status_code == 200
    assert anonymous.headers["etag"] == authenticated.headers["etag"]
    assert anonymous.headers["cache-control"] == "public, max-age=3600, must-revalidate"
    assert anonymous.json() == authenticated.json()


@pytest.mark.asyncio
async def test_non_operational_validation_ignores_canonical_incident_feature(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    canonical_allowed = AsyncMock(return_value=False)
    monkeypatch.setattr(
        router_mod,
        "_canonical_incident_ingestion_allowed",
        canonical_allowed,
        raising=False,
    )
    envelope = _envelope([_record("commit.v1", "abc123", VALID_PAYLOADS["commit.v1"])])

    # When
    response = await client.post(f"{BASE}/validate", json=envelope)

    # Then
    assert response.status_code == 200
    assert response.json()["valid"] is True
    canonical_allowed.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_operational_acceptance_ignores_canonical_incident_feature(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given
    canonical_allowed = AsyncMock(return_value=False)
    monkeypatch.setattr(
        router_mod,
        "_canonical_incident_ingestion_allowed",
        canonical_allowed,
        raising=False,
    )
    envelope = _envelope([_record("commit.v1", "abc123", VALID_PAYLOADS["commit.v1"])])

    # When
    response = await client.post(f"{BASE}/batches", json=envelope)

    # Then
    assert response.status_code == 202
    canonical_allowed.assert_not_awaited()
