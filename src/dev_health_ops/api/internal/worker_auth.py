"""Shared authentication for narrow internal worker bridges."""

from __future__ import annotations

import hmac
import os

from fastapi import HTTPException


def authorize_worker_bridge(authorization: str | None) -> None:
    """Require the fixed operational bridge token using constant-time comparison."""

    expected = os.environ.get("WORKER_OPERATIONAL_BRIDGE_TOKEN", "")
    supplied = authorization or ""
    if (
        not expected
        or not supplied.startswith("Bearer ")
        or not hmac.compare_digest(supplied[7:], expected)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
