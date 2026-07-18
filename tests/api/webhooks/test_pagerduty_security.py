from __future__ import annotations

import hashlib
import hmac

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from dev_health_ops.api.webhooks.pagerduty import (
    MAX_WEBHOOK_BODY_BYTES,
    _read_body_limited,
    _verify_signature,
)


def test_verify_signature_accepts_a_rotated_v1_secret() -> None:
    body = b'{"event": "incident.triggered"}'
    secret = "webhook-secret"
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    accepted = _verify_signature(
        body,
        f"v1={'0' * len(expected)}, v1={expected}",
        secret,
    )

    assert accepted is True


@pytest.mark.anyio
async def test_read_body_limited_rejects_content_length_before_buffering() -> None:
    receive_called = False

    async def receive() -> dict[str, object]:
        nonlocal receive_called
        receive_called = True
        return {
            "type": "http.request",
            "body": b"x" * (MAX_WEBHOOK_BODY_BYTES + 1),
            "more_body": False,
        }

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "headers": [
                (
                    b"content-length",
                    str(MAX_WEBHOOK_BODY_BYTES + 1).encode(),
                )
            ],
        },
        receive,
    )

    with pytest.raises(HTTPException) as exc_info:
        await _read_body_limited(request)

    assert exc_info.value.status_code == 413
    assert receive_called is False
