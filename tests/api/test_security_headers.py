"""Security-headers middleware tests (CHAOS security sprint)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.middleware.security_headers import SecurityHeadersMiddleware


@pytest.fixture
def app() -> FastAPI:
    a = FastAPI()
    a.add_middleware(SecurityHeadersMiddleware)

    @a.get("/ping")
    async def ping() -> dict[str, str]:
        return {"pong": "ok"}

    return a


@pytest.mark.asyncio
async def test_response_includes_hsts(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get("/ping")
    hsts = resp.headers.get("strict-transport-security", "")
    assert "max-age=" in hsts
    assert "includeSubDomains" in hsts


@pytest.mark.asyncio
async def test_response_includes_nosniff_and_frame_deny(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get("/ping")
    assert resp.headers.get("x-content-type-options") == "nosniff"
    assert resp.headers.get("x-frame-options") == "DENY"


@pytest.mark.asyncio
async def test_response_includes_referrer_policy_and_csp(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get("/ping")
    assert resp.headers.get("referrer-policy") == "strict-origin-when-cross-origin"
    csp = resp.headers.get("content-security-policy", "")
    assert "default-src 'none'" in csp
    assert "frame-ancestors 'none'" in csp


@pytest.mark.asyncio
async def test_existing_headers_are_not_overridden(app):
    @app.get("/custom")
    async def custom():
        from fastapi.responses import JSONResponse

        return JSONResponse(
            content={"ok": True},
            headers={"x-frame-options": "SAMEORIGIN"},
        )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get("/custom")
    # Middleware must NOT stomp an explicit per-response choice.
    assert resp.headers.get("x-frame-options") == "SAMEORIGIN"
