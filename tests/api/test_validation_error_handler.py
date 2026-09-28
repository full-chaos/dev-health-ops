from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.main import app


@pytest.mark.anyio
async def test_validation_error_uses_the_stock_fastapi_shape() -> None:
    """CHAOS-7015/D2802 regression proof.

    #3358 deleted Ask Dev's ``ask_dev_validation_error_handler``, which had
    been the REAL ``RequestValidationError`` handler for every non-
    ``/api/v1/dev`` REST route since Ask Dev existed (Starlette's exception-
    handler dict is last-write-wins, and it was registered after
    ``register_exception_handlers``) -- delegating straight to FastAPI's own
    stock handler. Deleting it silently un-shadowed ``_errors.py``'s
    structured handler for every OTHER route, changing the 422 body from the
    stock ``{"detail": [...]}`` shape to ``{"detail": {"message", "errors"}}``
    -- a shape ``internal/queryapi/server/pydantic_validation_error.go`` does
    not match. Observed RED on main before this fix (1b05473e33): this test
    got the dict shape instead of the stock list.

    ``/api/v1/product-telemetry/events`` needs no auth dependency override
    and already has a real pydantic constraint (``events: ... Field(...,
    min_length=1)``) that raises a genuine ``RequestValidationError``, so an
    empty batch is a real 422, not a hand-built one.
    """
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/product-telemetry/events",
            json={
                "orgIdHash": "org_hash_123",
                "source": "dev-health-web",
                "events": [],
            },
        )

    assert response.status_code == 422
    body = response.json()
    assert isinstance(body["detail"], list), (
        f"expected FastAPI's stock {{'detail': [...]}} shape, got {body!r}"
    )
    assert body["detail"], "expected at least one validation error entry"
    for error in body["detail"]:
        assert {"type", "loc", "msg"} <= set(error), error
