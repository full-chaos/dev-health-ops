from __future__ import annotations

import importlib
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.main import app


def _event(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "name": "chart_interacted",
        "schemaVersion": "2026-05-telemetry-v1",
        "eventId": "evt_123",
        "ts": "2026-05-25T12:00:00Z",
        "sessionId": "ses_123",
        "anonymousUserId": "anon_123",
        "orgIdHash": "org_hash_123",
        "routePattern": "/metrics",
        "payload": {
            "chart": "quadrant",
            "action": "overlay_toggled",
            "surface": "metrics",
            "scope": "org",
        },
    }
    event.update(overrides)
    return event


def _batch(events: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "orgIdHash": "org_hash_123",
        "source": "dev-health-web",
        "events": events if events is not None else [_event()],
    }


@pytest.mark.anyio
async def test_product_telemetry_accepts_valid_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    router = importlib.import_module("dev_health_ops.api.product_telemetry.router")

    async def write_batch(*args: Any, **kwargs: Any) -> str:
        return "product-telemetry:org_hash_123:events"

    monkeypatch.setattr(router, "write_product_telemetry_batch", write_batch)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/product-telemetry/events",
            json=_batch([_event(), _event(eventId="evt_456")]),
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["items_received"] == 2
    assert body["stream"] == "product-telemetry:org_hash_123:events"
    assert body["ingestion_id"]


@pytest.mark.anyio
async def test_product_telemetry_rejects_invalid_event_name() -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/product-telemetry/events",
            json=_batch([_event(name="quadrant_zone_overlay_toggled")]),
        )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_product_telemetry_rejects_empty_batch() -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/product-telemetry/events", json=_batch([])
        )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_product_telemetry_rejects_batches_larger_than_500() -> None:
    events = [_event(eventId=f"evt_{index}") for index in range(501)]
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/product-telemetry/events", json=_batch(events)
        )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_product_telemetry_accepts_when_redis_writer_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    router = importlib.import_module("dev_health_ops.api.product_telemetry.router")

    async def unavailable_writer(*args: Any, **kwargs: Any) -> str:
        raise ConnectionError("redis unavailable")

    monkeypatch.setattr(router, "write_product_telemetry_batch", unavailable_writer)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/api/v1/product-telemetry/events", json=_batch())

    assert response.status_code == 202
    assert response.json()["stream"] == "disabled"
