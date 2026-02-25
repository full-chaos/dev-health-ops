from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dev_health_ops.api.billing.router import stripe_webhook
from dev_health_ops.api.billing.stripe_client import (
    get_private_key,
    get_stripe_client,
    get_webhook_secret,
)
from dev_health_ops.db import get_postgres_uri


class BillingEdgeHealthResponse(BaseModel):
    status: str
    services: dict[str, str]


async def _check_postgres_health() -> str:
    uri = get_postgres_uri()
    if not uri:
        return "not_configured"

    engine = create_async_engine(uri, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return "ok"
    except Exception:
        return "down"
    finally:
        await engine.dispose()


def _check_secrets_health() -> dict[str, str]:
    statuses: dict[str, str] = {}

    statuses["stripe_secret_key"] = (
        "ok" if os.getenv("STRIPE_SECRET_KEY") else "not_configured"
    )

    try:
        get_webhook_secret()
        statuses["stripe_webhook_secret"] = "ok"
    except RuntimeError:
        statuses["stripe_webhook_secret"] = "not_configured"

    try:
        get_private_key()
        statuses["license_private_key"] = "ok"
    except RuntimeError:
        statuses["license_private_key"] = "not_configured"

    try:
        get_stripe_client()
        statuses["stripe_client"] = "ok"
    except Exception:
        statuses["stripe_client"] = "down"

    return statuses


app = FastAPI(
    title="Dev Health Ops Billing Edge",
    version="1.0.0",
    docs_url=None,
    openapi_url=None,
)


@app.post("/api/v1/billing/webhooks/stripe")
async def stripe_webhook_public(request: Request) -> dict:
    return await stripe_webhook(request)


@app.api_route(
    "/health", methods=["GET", "HEAD"], response_model=BillingEdgeHealthResponse
)
async def health() -> BillingEdgeHealthResponse | JSONResponse:
    services = _check_secrets_health()
    services["postgres"] = await _check_postgres_health()

    required_ok = (
        services["postgres"] == "ok"
        and services["stripe_secret_key"] == "ok"
        and services["stripe_webhook_secret"] == "ok"
        and services["license_private_key"] == "ok"
    )
    status = "ok" if required_ok else "down"

    response = BillingEdgeHealthResponse(status=status, services=services)
    if status != "ok":
        content = (
            response.model_dump()
            if hasattr(response, "model_dump")
            else response.dict()
        )
        return JSONResponse(status_code=503, content=content)
    return response


@app.api_route(
    "/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]
)
async def reject_unapproved_paths(path: str) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": "Not Found"})
