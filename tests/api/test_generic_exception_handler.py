"""Generic 500 exception handler returns sanitized JSON (CHAOS security sprint)."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.main import _generic_exception_handler


@pytest.fixture
def sanitized_app() -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(Exception, _generic_exception_handler)

    @app.get("/boom")
    async def boom():
        raise RuntimeError("super secret internal: DB password=hunter2")

    return app


@pytest.mark.asyncio
async def test_500_body_is_generic(sanitized_app):
    async with AsyncClient(
        transport=ASGITransport(app=sanitized_app, raise_app_exceptions=False),
        base_url="http://test",
    ) as ac:
        resp = await ac.get("/boom")
    assert resp.status_code == 500
    body = resp.json()
    assert body == {"detail": "Internal Server Error"}


@pytest.mark.asyncio
async def test_500_does_not_leak_exception_text(sanitized_app):
    async with AsyncClient(
        transport=ASGITransport(app=sanitized_app, raise_app_exceptions=False),
        base_url="http://test",
    ) as ac:
        resp = await ac.get("/boom")
    assert "hunter2" not in resp.text
    assert "RuntimeError" not in resp.text


@pytest.mark.asyncio
async def test_500_logs_original_exception(sanitized_app, caplog):
    import logging

    caplog.set_level(logging.ERROR, logger="dev_health_ops.api.main")
    async with AsyncClient(
        transport=ASGITransport(app=sanitized_app, raise_app_exceptions=False),
        base_url="http://test",
    ) as ac:
        await ac.get("/boom")
    # The full text must appear in the logs, not the response.
    assert any("hunter2" in rec.message or "hunter2" in (rec.exc_text or "")
               for rec in caplog.records)
