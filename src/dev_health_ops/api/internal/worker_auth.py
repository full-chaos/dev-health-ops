"""Shared authentication for narrow internal worker bridges."""

from __future__ import annotations

import hmac
import os

from fastapi import HTTPException

_MAX_TOKEN_BYTES = 512


def authorize_worker_bridge(authorization: str | None) -> None:
    """Require the fixed operational bridge token using constant-time comparison."""

    _authorize_bearer(authorization, "WORKER_OPERATIONAL_BRIDGE_TOKEN")


def authorize_metric_repair(authorization: str | None) -> None:
    """Require the distinct operator-only metric repair token."""

    expected = _bounded_secret("WORKER_METRIC_REPAIR_TOKEN")
    bridge = _bounded_secret("WORKER_OPERATIONAL_BRIDGE_TOKEN")
    if expected is None or (
        bridge is not None and hmac.compare_digest(expected, bridge)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
    _authorize_bearer(authorization, "WORKER_METRIC_REPAIR_TOKEN")


def authorize_workgraph_repair(authorization: str | None) -> None:
    """Require a repair token distinct from the worker bridge capability."""

    expected = _bounded_secret("WORKER_WORKGRAPH_REPAIR_TOKEN")
    bridge = _bounded_secret("WORKER_OPERATIONAL_BRIDGE_TOKEN")
    if expected is None or (
        bridge is not None and hmac.compare_digest(expected, bridge)
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")
    _authorize_bearer(authorization, "WORKER_WORKGRAPH_REPAIR_TOKEN")


def _authorize_bearer(authorization: str | None, environment_name: str) -> None:
    expected = _bounded_secret(environment_name)
    supplied = authorization or ""
    if expected is None or not supplied.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    supplied_secret = _bounded_bytes(supplied[7:])
    if supplied_secret is None or not hmac.compare_digest(supplied_secret, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _bounded_secret(environment_name: str) -> bytes | None:
    return _bounded_bytes(os.environ.get(environment_name, ""))


def _bounded_bytes(value: str) -> bytes | None:
    encoded = value.encode()
    if not encoded or len(encoded) > _MAX_TOKEN_BYTES:
        return None
    return encoded
