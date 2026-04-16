"""Membership-aware X-Org-Id middleware tests (CHAOS security sprint).

Verifies that OrgIdMiddleware rejects a forged X-Org-Id header pointing at
an org the authenticated user is NOT a member of.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from dev_health_ops.api.middleware import OrgIdMiddleware
from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    _current_org_id,
    get_current_org_id,
)


def _fake_user(user_id: str, jwt_org_id: str) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=user_id,
        email="u@example.com",
        org_id=jwt_org_id,
        role="member",
    )


def _build_app(captured: dict) -> Any:
    async def _handler(scope, receive, send):
        captured["org_id"] = get_current_org_id()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": b"{}"})

    return OrgIdMiddleware(_handler)


@pytest.mark.asyncio
async def test_header_for_non_member_org_is_rejected():
    """X-Org-Id header for an org the user does NOT belong to must 403."""
    user_id = str(uuid.uuid4())
    member_org = str(uuid.uuid4())
    foreign_org = str(uuid.uuid4())
    user = _fake_user(user_id, member_org)

    captured: dict = {}
    app = _build_app(captured)

    with (
        patch(
            "dev_health_ops.api.middleware.get_authenticated_user_from_headers",
            return_value=user,
        ),
        patch(
            "dev_health_ops.api.middleware.user_is_member_of_org",
            return_value=False,
        ),
    ):
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    "/",
                    headers={
                        "authorization": "Bearer tok",
                        "x-org-id": foreign_org,
                    },
                )
        finally:
            _current_org_id.set(None)

    assert response.status_code == 403, response.text
    assert captured.get("org_id") is None


@pytest.mark.asyncio
async def test_header_for_member_org_is_accepted():
    """X-Org-Id for an org the user IS a member of must be accepted."""
    user_id = str(uuid.uuid4())
    member_org = str(uuid.uuid4())
    secondary_org = str(uuid.uuid4())
    user = _fake_user(user_id, member_org)

    captured: dict = {}
    app = _build_app(captured)

    with (
        patch(
            "dev_health_ops.api.middleware.get_authenticated_user_from_headers",
            return_value=user,
        ),
        patch(
            "dev_health_ops.api.middleware.user_is_member_of_org",
            return_value=True,
        ),
    ):
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    "/",
                    headers={
                        "authorization": "Bearer tok",
                        "x-org-id": secondary_org,
                    },
                )
        finally:
            _current_org_id.set(None)

    assert response.status_code == 200
    assert captured["org_id"] == secondary_org


@pytest.mark.asyncio
async def test_superuser_with_header_bypasses_membership_check():
    """Superusers may scope to any org via X-Org-Id without a Membership row."""
    user_id = str(uuid.uuid4())
    home_org = str(uuid.uuid4())
    other_org = str(uuid.uuid4())
    super_user = AuthenticatedUser(
        user_id=user_id,
        email="root@example.com",
        org_id=home_org,
        role="owner",
        is_superuser=True,
    )

    captured: dict = {}
    app = _build_app(captured)

    with patch(
        "dev_health_ops.api.middleware.get_authenticated_user_from_headers",
        return_value=super_user,
    ):
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    "/",
                    headers={
                        "authorization": "Bearer tok",
                        "x-org-id": other_org,
                    },
                )
        finally:
            _current_org_id.set(None)

    assert response.status_code == 200
    assert captured["org_id"] == other_org


@pytest.mark.asyncio
async def test_anonymous_request_with_header_passes_through():
    """Unauthenticated requests with X-Org-Id must NOT be 403-ed by middleware.

    Downstream endpoints that require auth will 401 via their own dependencies;
    the X-Org-Id header carries no security claim without a JWT anyway.
    """
    foreign_org = str(uuid.uuid4())

    captured: dict = {}
    app = _build_app(captured)

    with patch(
        "dev_health_ops.api.middleware.get_authenticated_user_from_headers",
        return_value=None,
    ):
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get("/", headers={"x-org-id": foreign_org})
        finally:
            _current_org_id.set(None)

    assert response.status_code == 200
    assert captured.get("org_id") is None


@pytest.mark.asyncio
async def test_missing_header_falls_back_to_jwt_org():
    """No X-Org-Id: JWT org_id is used and no membership check is performed."""
    user_id = str(uuid.uuid4())
    jwt_org = str(uuid.uuid4())
    user = _fake_user(user_id, jwt_org)

    captured: dict = {}
    app = _build_app(captured)

    with patch(
        "dev_health_ops.api.middleware.get_authenticated_user_from_headers",
        return_value=user,
    ):
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    "/", headers={"authorization": "Bearer tok"}
                )
        finally:
            _current_org_id.set(None)

    assert response.status_code == 200
    assert captured["org_id"] == jwt_org
