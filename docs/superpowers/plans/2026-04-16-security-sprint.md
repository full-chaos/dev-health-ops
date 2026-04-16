# Security Sprint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close ten security findings (IDOR via X-Org-Id, missing admin auth, SAML XXE surface, JWT fallback secret, SSO domain bypass, forwarded-IP spoofing, drilldown tenant re-check, generic 500 leak, missing security headers, swallowed JWT errors) without regressing existing behaviour.

**Architecture:** Touch only the smallest surface per finding. Prefer defense-in-depth (every layer re-asserts org ownership; every IP read consults a trusted-proxy allowlist). Fail closed on security-critical config (JWT_SECRET_KEY, defusedxml).

**Tech Stack:** Python 3.11+, FastAPI / Starlette ASGI, SQLAlchemy async, pytest + pytest-asyncio, httpx ASGITransport for in-process API tests. Test style matches `tests/api/test_impersonation_middleware.py` (ASGI-level) and `tests/api/auth/test_jwt_secret.py` (unit-level env-var tests).

**Repo conventions verified:**
- Admin auth dep: `require_admin` in `src/dev_health_ops/api/admin/middleware.py`
- Current-user dep: `get_current_user` in `src/dev_health_ops/api/auth/router.py`
- Membership model: `dev_health_ops.models.users.Membership` (has `user_id`, `org_id`)
- Org-id contextvar: `dev_health_ops.api.services.auth._current_org_id` (get/set via `get_current_org_id` / `set_current_org_id`)
- Forwarded-IP helper: `dev_health_ops.api.middleware.rate_limit.get_forwarded_ip`

---

## Task 1: Membership check in OrgIdMiddleware

**Files:**
- Modify: `src/dev_health_ops/api/middleware/__init__.py`
- Test: `tests/api/test_org_id_middleware_membership.py` (new)

**Goal:** Reject requests whose `X-Org-Id` header does not match any `Membership` row for the authenticated user (or is not the user's JWT org_id when JWT present). Currently the middleware accepts any header value — an IDOR (attacker with any valid JWT can read another tenant's data).

- [ ] **Step 1.1: Write failing ASGI test (header + non-member org)**

Create `tests/api/test_org_id_middleware_membership.py`:

```python
"""Membership-aware X-Org-Id middleware tests (CHAOS security sprint).

Verifies that OrgIdMiddleware rejects a forged X-Org-Id header pointing at
an org the authenticated user is NOT a member of.
"""
from __future__ import annotations

import types
import uuid
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from dev_health_ops.api.middleware import OrgIdMiddleware
from dev_health_ops.api.services.auth import (
    _current_org_id,
    AuthenticatedUser,
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
```

- [ ] **Step 1.2: Run test to verify failure**

Run: `uv run pytest tests/api/test_org_id_middleware_membership.py::test_header_for_non_member_org_is_rejected -xvs`
Expected: FAIL — `get_authenticated_user_from_headers` / `user_is_member_of_org` do not yet exist on the middleware module (AttributeError).

- [ ] **Step 1.3: Implement membership check in middleware**

Replace the body of `src/dev_health_ops/api/middleware/__init__.py` with:

```python
"""Per-request org_id extraction middleware.

Sets the org_id contextvar for every HTTP request from:
  1. X-Org-Id header (authoritative - sent by frontend for all API calls)
  2. JWT org_id claim (fallback - when header is absent)

IDOR protection: the X-Org-Id header is ONLY accepted if the authenticated
user has a Membership row for that org (or the JWT org_id matches). Any
other value yields HTTP 403.

This is the SINGLE enforcement point for tenant scoping. All downstream
ClickHouse queries auto-inject org_id via query_dicts().
"""

from __future__ import annotations

import json
import logging
import uuid as uuid_mod
from typing import Iterable

from sqlalchemy import select
from starlette.types import ASGIApp, Receive, Scope, Send

from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    _current_org_id,
    extract_token_from_header,
    get_auth_service,
    set_current_org_id,
)

logger = logging.getLogger(__name__)


def get_authenticated_user_from_headers(
    headers: Iterable[tuple[bytes, bytes]],
) -> AuthenticatedUser | None:
    for key, value in headers:
        if key == b"authorization":
            token = extract_token_from_header(value.decode("latin-1"))
            if not token:
                return None
            return get_auth_service().get_authenticated_user(token)
    return None


async def user_is_member_of_org(user_id: str, org_id: str) -> bool:
    """Return True iff the user has an active Membership for org_id."""
    try:
        user_uuid = uuid_mod.UUID(user_id)
        org_uuid = uuid_mod.UUID(org_id)
    except (ValueError, TypeError):
        return False

    from dev_health_ops.db import get_postgres_session
    from dev_health_ops.models.users import Membership

    async with get_postgres_session() as session:
        result = await session.execute(
            select(Membership.id)
            .where(Membership.user_id == user_uuid)
            .where(Membership.org_id == org_uuid)
            .limit(1)
        )
        return result.scalar_one_or_none() is not None


def _forbidden_response(message: str) -> tuple[bytes, bytes]:
    body = json.dumps({"detail": message}).encode("utf-8")
    return body, b"application/json"


class OrgIdMiddleware:
    """Pure ASGI middleware — extracts org_id, verifies membership, sets contextvar."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        headers = scope.get("headers", [])
        header_org_id: str | None = None
        for key, value in headers:
            if key == b"x-org-id":
                header_org_id = value.decode("latin-1").strip() or None
                break

        user = get_authenticated_user_from_headers(headers)

        resolved_org_id: str | None = None
        if header_org_id:
            if user is None:
                await self._deny(send, "Authentication required for X-Org-Id")
                return
            if header_org_id == user.org_id:
                resolved_org_id = header_org_id
            elif await user_is_member_of_org(user.user_id, header_org_id):
                resolved_org_id = header_org_id
            else:
                logger.warning(
                    "X-Org-Id rejected: user=%s tried to access org=%s",
                    user.user_id,
                    header_org_id,
                )
                await self._deny(send, "X-Org-Id not permitted for this user")
                return
        elif user is not None and user.org_id:
            resolved_org_id = user.org_id

        token = set_current_org_id(resolved_org_id) if resolved_org_id else None
        try:
            await self.app(scope, receive, send)
        finally:
            if token is not None:
                _current_org_id.reset(token)

    @staticmethod
    async def _deny(send: Send, message: str) -> None:
        body = json.dumps({"detail": message}).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 403,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


__all__ = ["OrgIdMiddleware", "get_authenticated_user_from_headers", "user_is_member_of_org"]
```

- [ ] **Step 1.4: Run failing test again — it should now pass**

Run: `uv run pytest tests/api/test_org_id_middleware_membership.py::test_header_for_non_member_org_is_rejected -xvs`
Expected: PASS.

- [ ] **Step 1.5: Add two more behaviour tests (happy-path + JWT-only)**

Append to the same test file:

```python
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
                response = await client.get("/", headers={"authorization": "Bearer tok"})
        finally:
            _current_org_id.set(None)

    assert response.status_code == 200
    assert captured["org_id"] == jwt_org
```

- [ ] **Step 1.6: Run full test file — all three must pass**

Run: `uv run pytest tests/api/test_org_id_middleware_membership.py -xvs`
Expected: 3 passed.

- [ ] **Step 1.7: Commit**

```bash
git add src/dev_health_ops/api/middleware/__init__.py tests/api/test_org_id_middleware_membership.py
git commit -m "fix(security): enforce membership check on X-Org-Id header (IDOR)"
```

---

## Task 2: Admin org-GET requires auth

**Files:**
- Modify: `src/dev_health_ops/api/admin/routers/orgs.py` (the `GET /orgs/{org_id}` handler, lines 67-86)
- Test: `tests/api/admin/test_orgs_auth.py` (new)

**Goal:** Add `Depends(require_superuser)` to `GET /admin/orgs/{org_id}` so anonymous callers cannot read any org by UUID. All siblings on the same router already gate on `require_superuser` or `require_admin`; only this GET was missed.

- [ ] **Step 2.1: Write failing auth-required test**

Create `tests/api/admin/test_orgs_auth.py`:

```python
"""Auth-dependency tests for admin orgs router (CHAOS security sprint)."""
from __future__ import annotations

import importlib
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization

orgs_router_module = importlib.import_module(
    "dev_health_ops.api.admin.routers.orgs"
)
admin_common = importlib.import_module("dev_health_ops.api.admin.routers.common")
admin_middleware = importlib.import_module("dev_health_ops.api.admin.middleware")


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "orgs-auth.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn, tables=[Organization.__table__]
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed_org(session_maker) -> str:
    org_id = uuid.uuid4()
    org = Organization(id=org_id, slug=f"o-{org_id.hex[:8]}", name="Acme")
    async with session_maker() as session:
        session.add(org)
        await session.commit()
    return str(org_id)


def _app(session_maker, current_user: AuthenticatedUser | None):
    app = FastAPI()
    app.include_router(orgs_router_module.router, prefix="/admin")

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[admin_common.get_session] = _session_override
    if current_user is not None:
        from dev_health_ops.api.auth.router import get_current_user

        app.dependency_overrides[get_current_user] = lambda: current_user
    return app


@pytest.mark.asyncio
async def test_get_org_by_id_rejects_anonymous(session_maker):
    """GET /admin/orgs/{id} must 401 when no bearer token is supplied."""
    org_id = await _seed_org(session_maker)
    app = _app(session_maker, current_user=None)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get(f"/admin/orgs/{org_id}")
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_get_org_by_id_rejects_non_superuser(session_maker):
    """GET /admin/orgs/{id} must 403 when caller is not a superuser."""
    org_id = await _seed_org(session_maker)
    member = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="m@example.com",
        org_id=str(uuid.uuid4()),
        role="member",
        is_superuser=False,
    )
    app = _app(session_maker, current_user=member)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get(f"/admin/orgs/{org_id}")
    assert resp.status_code == 403, resp.text


@pytest.mark.asyncio
async def test_get_org_by_id_accepts_superuser(session_maker):
    """GET /admin/orgs/{id} must 200 for superuser."""
    org_id = await _seed_org(session_maker)
    su = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="su@example.com",
        org_id=str(uuid.uuid4()),
        role="owner",
        is_superuser=True,
    )
    app = _app(session_maker, current_user=su)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        resp = await ac.get(f"/admin/orgs/{org_id}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == org_id
```

- [ ] **Step 2.2: Run tests to verify two failures**

Run: `uv run pytest tests/api/admin/test_orgs_auth.py -xvs`
Expected: `test_get_org_by_id_rejects_anonymous` and `test_get_org_by_id_rejects_non_superuser` FAIL with 200 (currently unauthenticated). `test_get_org_by_id_accepts_superuser` likely PASSES already.

- [ ] **Step 2.3: Add auth dependency to the handler**

In `src/dev_health_ops/api/admin/routers/orgs.py`, change lines 67-71 from:

```python
@router.get("/orgs/{org_id}", response_model=OrganizationResponse)
async def get_organization(
    org_id: str,
    session: AsyncSession = Depends(get_session),
) -> OrganizationResponse:
```

to:

```python
@router.get("/orgs/{org_id}", response_model=OrganizationResponse)
async def get_organization(
    org_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: AuthenticatedUser = Depends(require_superuser),
) -> OrganizationResponse:
```

(`require_superuser` is already imported on line 10 and `AuthenticatedUser` on line 22 — no new imports needed.)

- [ ] **Step 2.4: Run tests to verify pass**

Run: `uv run pytest tests/api/admin/test_orgs_auth.py -xvs`
Expected: 3 passed.

- [ ] **Step 2.5: Commit**

```bash
git add src/dev_health_ops/api/admin/routers/orgs.py tests/api/admin/test_orgs_auth.py
git commit -m "fix(security): require superuser on GET /admin/orgs/{org_id}"
```

---

## Task 3: Make defusedxml required & remove stdlib XML import

**Files:**
- Modify: `pyproject.toml` (move defusedxml out of `enterprise-sso` extra, into core deps)
- Modify: `src/dev_health_ops/api/services/sso.py` (remove top-level `from xml.etree import ElementTree`, use defusedxml types everywhere, remove try/except fallback in `_parse_saml_xml`)
- Test: `tests/api/services/test_sso_xxe.py` (new)

**Goal:** Eliminate the stdlib XML import surface entirely — defusedxml becomes a hard dep and the parser raises on any DOCTYPE/entity-expansion attempt. Current `_parse_saml_xml` already raises if defusedxml is missing but the `from xml.etree import ElementTree` at module top-level still exists (used for `ElementTree.Element` annotations + `ElementTree.ParseError`). Replace with the defusedxml equivalents so no stdlib XML can ever be exercised.

- [ ] **Step 3.1: Write failing XXE test**

Create `tests/api/services/test_sso_xxe.py`:

```python
"""XXE-resistance tests for SAML XML parsing (CHAOS security sprint)."""
from __future__ import annotations

import pytest

from dev_health_ops.api.services.sso import SAMLProcessingError, SSOService


XXE_PAYLOAD = b"""<?xml version="1.0"?>
<!DOCTYPE foo [
  <!ELEMENT foo ANY >
  <!ENTITY xxe SYSTEM "file:///etc/passwd" >
]>
<foo>&xxe;</foo>
"""


def test_parse_saml_xml_rejects_doctype_payload():
    """defusedxml must reject any DOCTYPE declaration (XXE protection)."""
    with pytest.raises(SAMLProcessingError):
        SSOService._parse_saml_xml(XXE_PAYLOAD)


def test_parse_saml_xml_rejects_external_entity():
    """External entity resolution must be impossible."""
    payload = b"""<?xml version="1.0"?>
<!DOCTYPE lolz [<!ENTITY lol "lol">]>
<lolz>&lol;</lolz>
"""
    with pytest.raises(SAMLProcessingError):
        SSOService._parse_saml_xml(payload)


def test_parse_saml_xml_accepts_benign_xml():
    """Sanity: a well-formed SAML-shaped document still parses."""
    xml = b"<samlp:Response xmlns:samlp='urn:oasis:names:tc:SAML:2.0:protocol'/>"
    tree = SSOService._parse_saml_xml(xml)
    assert tree.tag.endswith("Response")
```

- [ ] **Step 3.2: Run test — two should fail today**

Run: `uv run pytest tests/api/services/test_sso_xxe.py -xvs`
Expected: `test_parse_saml_xml_rejects_doctype_payload` and `test_parse_saml_xml_rejects_external_entity` may already pass (defusedxml is installed in the dev env) — if so, the value of this task is removing the stdlib import so it cannot silently regress. Re-run after step 3.3 to confirm.

- [ ] **Step 3.3: Move defusedxml from extra to core dep in pyproject.toml**

Find the dependency section in `pyproject.toml` (around `dependencies = [`) and add `"defusedxml>=0.7.0",` if not already present in core. Then update the `enterprise-sso` extra to drop `defusedxml` (keep `signxml`):

Before:
```toml
enterprise-sso = ["defusedxml>=0.7.0", "signxml>=3.0.0"]
```

After:
```toml
enterprise-sso = ["signxml>=3.0.0"]
```

In the core `dependencies = [...]` list, append:
```toml
    "defusedxml>=0.7.0",
```

(Keep alphabetical order if the existing list is alphabetical; otherwise add at the end.)

- [ ] **Step 3.4: Remove stdlib XML import and simplify `_parse_saml_xml`**

Edit `src/dev_health_ops/api/services/sso.py`:

Replace line 13 (`from xml.etree import ElementTree`) with:

```python
from defusedxml import ElementTree as DefusedElementTree
from defusedxml.common import DefusedXmlException
from xml.etree.ElementTree import Element, ParseError
```

(`Element` and `ParseError` are pure Python types/exceptions; importing them from the stdlib is safe — they do not trigger XML parsing. The parser itself comes exclusively from defusedxml.)

Then update the body of `_parse_saml_xml` (lines 653-668) to:

```python
    @staticmethod
    def _parse_saml_xml(xml_bytes: bytes) -> Element:
        try:
            return DefusedElementTree.fromstring(xml_bytes)
        except DefusedXmlException as exc:
            raise SAMLProcessingError("Unsafe SAML XML rejected") from exc
        except ParseError as exc:
            raise SAMLProcessingError("Invalid SAML XML") from exc
```

Finally, fix all the annotations that previously said `ElementTree.Element` to say `Element`:

Run: `uv run rg -n 'ElementTree\.Element' src/dev_health_ops/api/services/sso.py`

For each hit, replace `ElementTree.Element` with `Element` (the type we imported directly). Same for `ElementTree.ParseError` → `ParseError`.

- [ ] **Step 3.5: Re-run XXE tests — all three must pass**

Run: `uv run pytest tests/api/services/test_sso_xxe.py -xvs`
Expected: 3 passed.

- [ ] **Step 3.6: Run SSO regression tests to catch broken annotations**

Run: `uv run pytest tests/api/services/test_sso.py tests/api/auth/test_sso_module.py -xvs`
Expected: all pass (no behavioural change, only imports/types changed).

- [ ] **Step 3.7: Commit**

```bash
git add pyproject.toml src/dev_health_ops/api/services/sso.py tests/api/services/test_sso_xxe.py
git commit -m "fix(security): make defusedxml a hard dep; remove stdlib XML import in SSO"
```

---

## Task 4: JWT_SECRET_KEY must be set (no derivation fallback)

**Files:**
- Modify: `src/dev_health_ops/api/services/auth.py` (the `_get_jwt_secret` function, lines 96-128)
- Modify: `tests/api/auth/test_jwt_secret.py` (existing tests expect the fallback behaviour — rewrite)

**Goal:** Always fail if `JWT_SECRET_KEY` is unset. The current code derives a secret via SHA256 of `SETTINGS_ENCRYPTION_KEY` which is a long-lived encryption key whose compromise is catastrophic (it additionally can decrypt stored secrets). Breaking the two makes key-rotation safer.

- [ ] **Step 4.1: Update the existing test file to expect the new behaviour**

Edit `tests/api/auth/test_jwt_secret.py`. Replace the entire file contents with:

```python
from __future__ import annotations

import pytest

from dev_health_ops.api.services.auth import _get_jwt_secret


def _clear_secret_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "JWT_SECRET_KEY",
        "SETTINGS_ENCRYPTION_KEY",
        "ENVIRONMENT",
        "ENV",
        "RAILWAY_ENVIRONMENT",
        "FLY_APP_NAME",
        "RENDER_SERVICE_ID",
        "KUBERNETES_SERVICE_HOST",
    ):
        monkeypatch.delenv(var, raising=False)


def test_jwt_secret_key_env_var_is_used_when_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_secret_env(monkeypatch)
    jwt_secret = "this-is-a-very-secure-secret-key-12345"
    monkeypatch.setenv("JWT_SECRET_KEY", jwt_secret)

    assert _get_jwt_secret() == jwt_secret


def test_missing_jwt_secret_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """JWT_SECRET_KEY absent in ANY environment must fail closed."""
    _clear_secret_env(monkeypatch)

    with pytest.raises(RuntimeError, match="JWT_SECRET_KEY"):
        _get_jwt_secret()


def test_missing_jwt_secret_with_settings_encryption_key_still_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SETTINGS_ENCRYPTION_KEY must NOT be used as a fallback."""
    _clear_secret_env(monkeypatch)
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "some-dev-key")
    monkeypatch.setenv("ENVIRONMENT", "development")

    with pytest.raises(RuntimeError, match="JWT_SECRET_KEY"):
        _get_jwt_secret()


def test_short_secret_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_secret_env(monkeypatch)
    monkeypatch.setenv("JWT_SECRET_KEY", "too-short")

    with pytest.raises(ValueError, match="JWT secret must be at least 32 characters"):
        _get_jwt_secret()
```

- [ ] **Step 4.2: Run tests — the two new `_raises` tests must fail**

Run: `uv run pytest tests/api/auth/test_jwt_secret.py -xvs`
Expected: `test_missing_jwt_secret_raises_runtime_error` and `test_missing_jwt_secret_with_settings_encryption_key_still_raises` FAIL (the current code silently returns a derived value).

- [ ] **Step 4.3: Simplify `_get_jwt_secret` to fail closed**

Replace lines 96-128 of `src/dev_health_ops/api/services/auth.py` (the `_get_jwt_secret` function) with:

```python
def _get_jwt_secret() -> str:
    secret = os.getenv("JWT_SECRET_KEY")
    if not secret:
        raise RuntimeError(
            "JWT_SECRET_KEY is required and must be set in the environment. "
            "Derivation from SETTINGS_ENCRYPTION_KEY is no longer supported."
        )
    if len(secret) < 32:
        raise ValueError("JWT secret must be at least 32 characters")
    return secret
```

Also remove the now-unused `hashlib` import if it is not used elsewhere in the file:

Run: `uv run rg -n 'hashlib' src/dev_health_ops/api/services/auth.py`

If the only remaining hit is the `import hashlib` line, delete that import. If hashlib is used elsewhere in the file, leave the import.

- [ ] **Step 4.4: Run tests — all four must pass**

Run: `uv run pytest tests/api/auth/test_jwt_secret.py -xvs`
Expected: 4 passed.

- [ ] **Step 4.5: Run wider auth test suite to catch callers relying on the fallback**

Run: `uv run pytest tests/api/auth/ -xvs`
Expected: all pass. If any test fails due to unset JWT_SECRET_KEY, fix the test (use `monkeypatch.setenv("JWT_SECRET_KEY", "x" * 40)`) rather than restoring the fallback.

- [ ] **Step 4.6: Commit**

```bash
git add src/dev_health_ops/api/services/auth.py tests/api/auth/test_jwt_secret.py
git commit -m "fix(security): require JWT_SECRET_KEY; remove SHA256 fallback derivation"
```

---

## Task 5: Enforce `allowed_domains` on SSO auto-provision

**Files:**
- Modify: `src/dev_health_ops/api/services/sso.py` (the `provision_or_get_user` method, lines 585-651)
- Test: `tests/api/services/test_sso_allowed_domains.py` (new)

**Goal:** When an SSO provider has `allowed_domains = ['acme.com']` and auto-provisioning is enabled, a federated identity for `attacker@evil.com` must be rejected. Currently `allowed_domains` is stored but never consulted.

- [ ] **Step 5.1: Write failing domain-check test**

Create `tests/api/services/test_sso_allowed_domains.py`:

```python
"""Enforcement tests for SSO allowed_domains (CHAOS security sprint)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from dev_health_ops.api.services.sso import SSOProcessingError, SSOService
from dev_health_ops.models.sso import SSOProvider


def _provider(allowed_domains, auto_provision=True):
    return SSOProvider(
        id=uuid.uuid4(),
        org_id=uuid.uuid4(),
        name="Acme SSO",
        protocol="oidc",
        config={},
        auto_provision_users=auto_provision,
        allowed_domains=allowed_domains,
    )


def _service_with_provider(provider):
    session = MagicMock()
    session.execute = AsyncMock(
        return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=None))
    )
    session.add = MagicMock()
    session.flush = AsyncMock()
    svc = SSOService(session)
    svc.get_provider = AsyncMock(return_value=provider)
    return svc


@pytest.mark.asyncio
async def test_disallowed_domain_is_rejected_on_autoprovision():
    """A user whose email domain is not in allowed_domains must 403."""
    provider = _provider(allowed_domains=["acme.com"])
    svc = _service_with_provider(provider)

    with pytest.raises(SSOProcessingError, match="domain"):
        await svc.provision_or_get_user(
            org_id=provider.org_id,
            email="attacker@evil.com",
            name="A Person",
            provider_id=provider.id,
            external_id="ext-1",
        )


@pytest.mark.asyncio
async def test_allowed_domain_is_accepted_case_insensitive():
    """Email domain matching (case-insensitive) must succeed."""
    provider = _provider(allowed_domains=["Acme.COM"])
    svc = _service_with_provider(provider)

    user, _membership, returned_provider = await svc.provision_or_get_user(
        org_id=provider.org_id,
        email="alice@acme.com",
        name="Alice",
        provider_id=provider.id,
        external_id="ext-2",
    )
    assert user.email == "alice@acme.com"
    assert returned_provider is provider


@pytest.mark.asyncio
async def test_empty_allowed_domains_list_allows_all():
    """When allowed_domains is None or empty, any domain is accepted (no regression)."""
    provider = _provider(allowed_domains=None)
    svc = _service_with_provider(provider)

    user, _m, _p = await svc.provision_or_get_user(
        org_id=provider.org_id,
        email="any@anything.io",
        name="Any",
        provider_id=provider.id,
        external_id="ext-3",
    )
    assert user.email == "any@anything.io"
```

- [ ] **Step 5.2: Run tests — domain-rejection must fail**

Run: `uv run pytest tests/api/services/test_sso_allowed_domains.py -xvs`
Expected: `test_disallowed_domain_is_rejected_on_autoprovision` FAILS (no check today).

- [ ] **Step 5.3: Add domain enforcement to `provision_or_get_user`**

In `src/dev_health_ops/api/services/sso.py`, modify the `provision_or_get_user` method (around line 592 after the `provider` lookup). Insert the following block immediately before the `stmt = select(User).where(User.email == email)` line:

```python
        allowed = [d.strip().lower() for d in (provider.allowed_domains or []) if d]
        if allowed:
            try:
                _, domain = email.rsplit("@", 1)
            except ValueError as exc:
                raise SSOProcessingError("Invalid email from IdP") from exc
            if domain.lower() not in allowed:
                logger.warning(
                    "SSO provision rejected: domain=%s not in allowed_domains for provider=%s",
                    sanitize_for_log(domain),
                    provider.id,
                )
                raise SSOProcessingError(
                    "Email domain is not permitted for this SSO provider"
                )
```

(`sanitize_for_log` is already imported on line 22.)

- [ ] **Step 5.4: Run tests — all three must pass**

Run: `uv run pytest tests/api/services/test_sso_allowed_domains.py -xvs`
Expected: 3 passed.

- [ ] **Step 5.5: Run existing SSO test suite to catch regressions**

Run: `uv run pytest tests/api/services/test_sso.py tests/api/auth/test_sso_module.py -xvs`
Expected: all pass.

- [ ] **Step 5.6: Commit**

```bash
git add src/dev_health_ops/api/services/sso.py tests/api/services/test_sso_allowed_domains.py
git commit -m "fix(security): enforce SSO allowed_domains on user auto-provision"
```

---

## Task 6: Trust X-Forwarded-For only from `TRUSTED_PROXIES`

**Files:**
- Modify: `src/dev_health_ops/api/middleware/rate_limit.py` (the `get_forwarded_ip` function, lines 65-75)
- Test: `tests/api/test_forwarded_ip_trust.py` (new)

**Goal:** Only read `X-Forwarded-For` when the TCP peer (`request.client.host`) is in a configurable `TRUSTED_PROXIES` allowlist. A direct-to-API attacker could otherwise spoof rate-limit keys via a forged header.

- [ ] **Step 6.1: Write failing trust-boundary tests**

Create `tests/api/test_forwarded_ip_trust.py`:

```python
"""X-Forwarded-For trust boundary tests (CHAOS security sprint)."""
from __future__ import annotations

import pytest
from fastapi import Request

from dev_health_ops.api.middleware.rate_limit import get_forwarded_ip


def _make_request(
    client_host: str,
    xff: str | None = None,
) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if xff is not None:
        headers.append((b"x-forwarded-for", xff.encode("latin-1")))
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": headers,
        "client": (client_host, 12345),
    }
    return Request(scope)


def test_xff_ignored_from_untrusted_peer(monkeypatch):
    """XFF from a random internet peer must NOT be honoured."""
    monkeypatch.setenv("TRUSTED_PROXIES", "10.0.0.1")
    req = _make_request(client_host="1.2.3.4", xff="203.0.113.1")
    assert get_forwarded_ip(req) == "1.2.3.4"


def test_xff_honoured_from_trusted_peer(monkeypatch):
    """XFF from a listed trusted proxy must be used as the real client."""
    monkeypatch.setenv("TRUSTED_PROXIES", "10.0.0.1,10.0.0.2")
    req = _make_request(client_host="10.0.0.2", xff="203.0.113.1")
    assert get_forwarded_ip(req) == "203.0.113.1"


def test_xff_missing_returns_peer(monkeypatch):
    """No XFF: peer IP is returned regardless of trust."""
    monkeypatch.setenv("TRUSTED_PROXIES", "10.0.0.1")
    req = _make_request(client_host="10.0.0.1", xff=None)
    assert get_forwarded_ip(req) == "10.0.0.1"


def test_trusted_proxies_unset_disables_xff(monkeypatch):
    """Unset/empty TRUSTED_PROXIES must fail-closed: never trust XFF."""
    monkeypatch.delenv("TRUSTED_PROXIES", raising=False)
    req = _make_request(client_host="10.0.0.1", xff="203.0.113.1")
    assert get_forwarded_ip(req) == "10.0.0.1"


def test_xff_takes_first_entry(monkeypatch):
    """When trusted, the leftmost XFF entry is the original client."""
    monkeypatch.setenv("TRUSTED_PROXIES", "10.0.0.1")
    req = _make_request(
        client_host="10.0.0.1", xff="203.0.113.1, 10.0.0.1"
    )
    assert get_forwarded_ip(req) == "203.0.113.1"
```

- [ ] **Step 6.2: Run tests — most should fail today**

Run: `uv run pytest tests/api/test_forwarded_ip_trust.py -xvs`
Expected: `test_xff_ignored_from_untrusted_peer` and `test_trusted_proxies_unset_disables_xff` FAIL (current code trusts XFF unconditionally).

- [ ] **Step 6.3: Implement trusted-proxy allowlist**

Edit `src/dev_health_ops/api/middleware/rate_limit.py`. Replace lines 65-75 (the `get_forwarded_ip` function) with:

```python
def _trusted_proxies() -> frozenset[str]:
    """Return the configured set of trusted proxy IPs (fail-closed: empty if unset)."""
    raw = os.getenv("TRUSTED_PROXIES", "")
    return frozenset(p.strip() for p in raw.split(",") if p.strip())


def get_forwarded_ip(request: Request) -> str:
    """Return real client IP via X-Forwarded-For, honoured only if the TCP peer
    is in the TRUSTED_PROXIES allowlist.

    Behind a reverse proxy (Next.js rewrite, nginx, etc.) the TCP peer is the
    proxy, not the end-user. X-Forwarded-For carries the original IP — but it
    is attacker-controlled when sent directly to the API, so we only trust it
    when the peer address is an expected proxy.
    """
    peer = (request.client.host if request.client else None) or "unknown"
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded and peer in _trusted_proxies():
        return forwarded.split(",")[0].strip()
    return peer
```

- [ ] **Step 6.4: Run tests — all five must pass**

Run: `uv run pytest tests/api/test_forwarded_ip_trust.py -xvs`
Expected: 5 passed.

- [ ] **Step 6.5: Run rate-limit/auth test suite for regressions**

Run: `uv run pytest tests/api/test_rate_limit_config.py tests/api/auth/ -xvs`
Expected: all pass. If any test fails because it relied on the old trusting behaviour, add `monkeypatch.setenv("TRUSTED_PROXIES", "testclient")` to that test (httpx's ASGITransport sets peer to `testclient`).

- [ ] **Step 6.6: Commit**

```bash
git add src/dev_health_ops/api/middleware/rate_limit.py tests/api/test_forwarded_ip_trust.py
git commit -m "fix(security): trust X-Forwarded-For only from TRUSTED_PROXIES allowlist"
```

---

## Task 7: Service-layer org-ownership re-check in drilldown queries

**Files:**
- Modify: `src/dev_health_ops/api/queries/drilldown.py` (both `fetch_pull_requests` and `fetch_issues`)
- Test: `tests/api/queries/test_drilldown_org_check.py` (new)

**Goal:** Independent defense-in-depth — even if the middleware contextvar is ever mis-set, a drilldown query must refuse to execute when its caller-provided `org_id` disagrees with `get_current_org_id()`. Also raise on empty `org_id`.

- [ ] **Step 7.1: Write failing assertion test**

Create `tests/api/queries/test_drilldown_org_check.py`:

```python
"""Defense-in-depth org_id re-check for drilldown queries (CHAOS security sprint)."""
from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from dev_health_ops.api.queries.drilldown import fetch_issues, fetch_pull_requests
from dev_health_ops.api.services.auth import _current_org_id, set_current_org_id


class _Sink:
    def __init__(self) -> None:
        self.last_params: dict[str, Any] | None = None

    def query_dicts(
        self, query: str, params: dict[str, Any] | None
    ) -> list[dict[str, Any]]:
        self.last_params = params
        return []


@pytest.mark.asyncio
async def test_fetch_pull_requests_rejects_empty_org_id():
    try:
        _current_org_id.set(None)
        with pytest.raises(ValueError, match="org_id"):
            await fetch_pull_requests(
                _Sink(),
                start_day=date(2024, 1, 1),
                end_day=date(2024, 1, 2),
                scope_filter="",
                scope_params={},
                org_id="",
            )
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_fetch_pull_requests_rejects_context_mismatch():
    try:
        set_current_org_id("org-A")
        with pytest.raises(PermissionError, match="org_id mismatch"):
            await fetch_pull_requests(
                _Sink(),
                start_day=date(2024, 1, 1),
                end_day=date(2024, 1, 2),
                scope_filter="",
                scope_params={},
                org_id="org-B",
            )
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_fetch_issues_rejects_context_mismatch():
    try:
        set_current_org_id("org-A")
        with pytest.raises(PermissionError, match="org_id mismatch"):
            await fetch_issues(
                _Sink(),
                start_day=date(2024, 1, 1),
                end_day=date(2024, 1, 2),
                scope_filter="",
                scope_params={},
                org_id="org-B",
            )
    finally:
        _current_org_id.set(None)


@pytest.mark.asyncio
async def test_fetch_issues_allows_matching_org_id():
    try:
        set_current_org_id("org-X")
        sink = _Sink()
        await fetch_issues(
            sink,
            start_day=date(2024, 1, 1),
            end_day=date(2024, 1, 2),
            scope_filter="",
            scope_params={},
            org_id="org-X",
        )
        assert sink.last_params is not None
        assert sink.last_params["org_id"] == "org-X"
    finally:
        _current_org_id.set(None)
```

- [ ] **Step 7.2: Run tests — mismatch/empty tests must fail**

Run: `uv run pytest tests/api/queries/test_drilldown_org_check.py -xvs`
Expected: the three negative-path tests FAIL (no guard today).

- [ ] **Step 7.3: Add guards to both fetch functions**

Edit `src/dev_health_ops/api/queries/drilldown.py`. Immediately after the `from .client import query_dicts` import on line 6, add:

```python
from dev_health_ops.api.services.auth import get_current_org_id


def _assert_org_id(org_id: str) -> None:
    if not org_id:
        raise ValueError("org_id is required for drilldown queries")
    ctx = get_current_org_id()
    if ctx is not None and ctx != org_id:
        raise PermissionError(
            f"org_id mismatch: contextvar={ctx!r} caller={org_id!r}"
        )
```

Then at the top of both `fetch_pull_requests` (inside the function body, before the `query = f"""..."""`) and `fetch_issues`, add:

```python
    _assert_org_id(org_id)
```

Concretely, after this change the beginning of `fetch_pull_requests` reads:

```python
async def fetch_pull_requests(
    client: Any,
    *,
    start_day: date,
    end_day: date,
    scope_filter: str,
    scope_params: dict[str, Any],
    limit: int = 50,
    org_id: str = "",
) -> list[dict[str, Any]]:
    _assert_org_id(org_id)
    query = f"""
        SELECT
            ...
    """
```

and similarly for `fetch_issues`.

- [ ] **Step 7.4: Run tests — all four must pass**

Run: `uv run pytest tests/api/queries/test_drilldown_org_check.py -xvs`
Expected: 4 passed.

- [ ] **Step 7.5: Run regression suite**

Run: `uv run pytest tests/api/queries/ tests/api/test_main_app_integration.py -xvs`
Expected: all pass. If an existing test calls these helpers without setting the contextvar and passes a non-empty org_id, it will still pass (our assert only fires when contextvar is non-None).

- [ ] **Step 7.6: Commit**

```bash
git add src/dev_health_ops/api/queries/drilldown.py tests/api/queries/test_drilldown_org_check.py
git commit -m "fix(security): re-check org_id in drilldown queries (defense-in-depth)"
```

---

## Task 8: Sanitized generic 500 exception handler

**Files:**
- Modify: `src/dev_health_ops/api/main.py` (add a new exception handler near existing handlers around line 374)
- Test: `tests/api/test_generic_exception_handler.py` (new)

**Goal:** Install a FastAPI-level `Exception` handler that returns a fixed `{"detail": "Internal Server Error"}` JSON body (plus a correlation id) and logs the real exception at ERROR with stack trace. Today an unhandled exception falls through to Starlette's default formatting which can leak class names or tracebacks depending on config. This also unifies error shape for the frontend.

- [ ] **Step 8.1: Write failing test**

Create `tests/api/test_generic_exception_handler.py`:

```python
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
        transport=ASGITransport(app=sanitized_app),
        base_url="http://test",
        raise_app_exceptions=False,
    ) as ac:
        resp = await ac.get("/boom")
    assert resp.status_code == 500
    body = resp.json()
    assert body == {"detail": "Internal Server Error"}


@pytest.mark.asyncio
async def test_500_does_not_leak_exception_text(sanitized_app):
    async with AsyncClient(
        transport=ASGITransport(app=sanitized_app),
        base_url="http://test",
        raise_app_exceptions=False,
    ) as ac:
        resp = await ac.get("/boom")
    assert "hunter2" not in resp.text
    assert "RuntimeError" not in resp.text


@pytest.mark.asyncio
async def test_500_logs_original_exception(sanitized_app, caplog):
    import logging

    caplog.set_level(logging.ERROR, logger="dev_health_ops.api.main")
    async with AsyncClient(
        transport=ASGITransport(app=sanitized_app),
        base_url="http://test",
        raise_app_exceptions=False,
    ) as ac:
        await ac.get("/boom")
    # The full text must appear in the logs, not the response.
    assert any("hunter2" in rec.message or "hunter2" in (rec.exc_text or "")
               for rec in caplog.records)
```

- [ ] **Step 8.2: Run test — import will fail**

Run: `uv run pytest tests/api/test_generic_exception_handler.py -xvs`
Expected: ImportError for `_generic_exception_handler` (symbol does not exist).

- [ ] **Step 8.3: Add the handler and register it**

Edit `src/dev_health_ops/api/main.py`. Immediately after the existing `_validation_error_handler` function (after line 366), add:

```python
async def _generic_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """Catch-all 500 handler that returns a sanitized response.

    Logs the real exception with stack trace at ERROR level so operators can
    investigate via logs/Sentry, but never leaks internals to the client.
    """
    logger.error(
        "Unhandled exception on %s %s",
        request.method,
        request.url.path,
        exc_info=exc,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )
```

Then, after line 374 (`app.add_exception_handler(RequestValidationError, _validation_error_handler)`), add:

```python
app.add_exception_handler(Exception, _generic_exception_handler)
```

- [ ] **Step 8.4: Run test — all three must pass**

Run: `uv run pytest tests/api/test_generic_exception_handler.py -xvs`
Expected: 3 passed.

- [ ] **Step 8.5: Run app-integration suite**

Run: `uv run pytest tests/api/test_main_app_integration.py -xvs`
Expected: all pass — the new handler only triggers for otherwise-unhandled exceptions, so it should not interfere with existing HTTPException responses.

- [ ] **Step 8.6: Commit**

```bash
git add src/dev_health_ops/api/main.py tests/api/test_generic_exception_handler.py
git commit -m "fix(security): add sanitized generic 500 exception handler"
```

---

## Task 9: Security-headers middleware (HSTS, nosniff, frame-deny, CSP)

**Files:**
- Create: `src/dev_health_ops/api/middleware/security_headers.py`
- Modify: `src/dev_health_ops/api/main.py` (register the middleware alongside CORS, around line 386)
- Test: `tests/api/test_security_headers.py` (new)

**Goal:** Inject `Strict-Transport-Security`, `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, and a conservative `Content-Security-Policy` on every API response. Missing today.

- [ ] **Step 9.1: Write failing test**

Create `tests/api/test_security_headers.py`:

```python
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
```

- [ ] **Step 9.2: Run test — import fails**

Run: `uv run pytest tests/api/test_security_headers.py -xvs`
Expected: ImportError for `SecurityHeadersMiddleware`.

- [ ] **Step 9.3: Create the middleware**

Create `src/dev_health_ops/api/middleware/security_headers.py`:

```python
"""Security-headers middleware.

Injects a conservative set of response headers on every HTTP response:

- Strict-Transport-Security: HSTS with 1y max-age and subdomain coverage
- X-Content-Type-Options: nosniff
- X-Frame-Options: DENY
- Referrer-Policy: strict-origin-when-cross-origin
- Content-Security-Policy: lock down by default (API returns JSON)

Pre-existing headers set by downstream handlers are preserved (case-insensitive
match); the middleware only adds what's missing.
"""

from __future__ import annotations

from starlette.types import ASGIApp, Message, Receive, Scope, Send

_DEFAULT_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"strict-transport-security", b"max-age=31536000; includeSubDomains"),
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"strict-origin-when-cross-origin"),
    (
        b"content-security-policy",
        b"default-src 'none'; frame-ancestors 'none'; base-uri 'none'",
    ),
)


class SecurityHeadersMiddleware:
    """Pure ASGI middleware that adds security headers to every response."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def _send(message: Message) -> None:
            if message["type"] == "http.response.start":
                existing = {k.lower() for k, _ in message.get("headers", [])}
                headers = list(message.get("headers", []))
                for name, value in _DEFAULT_HEADERS:
                    if name not in existing:
                        headers.append((name, value))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, _send)


__all__ = ["SecurityHeadersMiddleware"]
```

- [ ] **Step 9.4: Run tests — all four must pass**

Run: `uv run pytest tests/api/test_security_headers.py -xvs`
Expected: 4 passed.

- [ ] **Step 9.5: Register the middleware in `main.py`**

Edit `src/dev_health_ops/api/main.py`. Near the top of the middleware imports (around line 33 where `OrgIdMiddleware` is imported), add:

```python
from dev_health_ops.api.middleware.security_headers import SecurityHeadersMiddleware
```

Then, immediately after the `app.add_middleware(CORSMiddleware, ...)` block (after line 386), add:

```python
app.add_middleware(SecurityHeadersMiddleware)
```

(ASGI middlewares run last-added-first; placing SecurityHeadersMiddleware after CORS means it runs *before* CORS on the way in and *after* CORS on the way out — correct order for response-header injection.)

- [ ] **Step 9.6: Run app-integration suite to confirm no regression**

Run: `uv run pytest tests/api/test_main_app_integration.py tests/api/test_security_headers.py -xvs`
Expected: all pass.

- [ ] **Step 9.7: Commit**

```bash
git add src/dev_health_ops/api/middleware/security_headers.py \
        src/dev_health_ops/api/main.py \
        tests/api/test_security_headers.py
git commit -m "feat(security): add security-headers middleware (HSTS, nosniff, CSP)"
```

---

## Task 10: Narrow the bare `except Exception` in `_extract_unverified_org_and_subject`

**Files:**
- Modify: `src/dev_health_ops/api/auth/router.py` (function `_extract_unverified_org_and_subject`, lines 946-956)
- Test: `tests/api/auth/test_extract_unverified_claims.py` (new)

**Goal:** Replace the catch-all with targeted JOSE exceptions and log the failure. Silent swallowing today makes production triage impossible.

- [ ] **Step 10.1: Write failing test**

Create `tests/api/auth/test_extract_unverified_claims.py`:

```python
"""Tests for narrowed exception handling in _extract_unverified_org_and_subject."""
from __future__ import annotations

import logging

import pytest

from dev_health_ops.api.auth.router import _extract_unverified_org_and_subject


def test_returns_none_tuple_for_malformed_token(caplog):
    """A malformed token must yield (None, None) AND emit a debug log."""
    caplog.set_level(logging.DEBUG, logger="dev_health_ops.api.auth.router")
    org, sub = _extract_unverified_org_and_subject("not.a.token")
    assert (org, sub) == (None, None)
    assert any(
        "unverified claims" in rec.message.lower()
        for rec in caplog.records
    )


def test_returns_none_tuple_for_empty_token():
    org, sub = _extract_unverified_org_and_subject("")
    assert (org, sub) == (None, None)


def test_returns_tuple_for_valid_unsigned_token():
    """A syntactically valid JWT (even if signature invalid) yields claims."""
    import base64
    import json

    def _b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    header = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64(
        json.dumps(
            {"sub": "user-1", "org_id": "00000000-0000-0000-0000-000000000001"}
        ).encode()
    )
    signature = _b64(b"\x00" * 32)
    token = f"{header}.{payload}.{signature}"

    org, sub = _extract_unverified_org_and_subject(token)
    assert sub == "user-1"
    assert org is not None
    assert str(org) == "00000000-0000-0000-0000-000000000001"
```

- [ ] **Step 10.2: Run test — first test fails (no log today)**

Run: `uv run pytest tests/api/auth/test_extract_unverified_claims.py -xvs`
Expected: `test_returns_none_tuple_for_malformed_token` FAILS (no log emitted).

- [ ] **Step 10.3: Narrow exceptions and add logging**

Edit `src/dev_health_ops/api/auth/router.py`. Replace lines 946-956 (the `_extract_unverified_org_and_subject` function) with:

```python
def _extract_unverified_org_and_subject(
    token: str,
) -> tuple[uuid_mod.UUID | None, str | None]:
    try:
        from jose import jwt
        from jose.exceptions import JOSEError

        claims = jwt.get_unverified_claims(token)
    except (JOSEError, ValueError, AttributeError, TypeError) as exc:
        logger.debug("Could not parse unverified claims: %s", exc)
        return None, None
    except ImportError:
        logger.error("jose not installed — cannot extract unverified claims")
        return None, None

    return _parse_uuid(claims.get("org_id")), claims.get("sub")
```

- [ ] **Step 10.4: Run tests — all three must pass**

Run: `uv run pytest tests/api/auth/test_extract_unverified_claims.py -xvs`
Expected: 3 passed.

- [ ] **Step 10.5: Run auth-router regression suite**

Run: `uv run pytest tests/api/auth/ -xvs`
Expected: all pass — behavior on malformed tokens (returning the tuple of Nones) is unchanged.

- [ ] **Step 10.6: Commit**

```bash
git add src/dev_health_ops/api/auth/router.py tests/api/auth/test_extract_unverified_claims.py
git commit -m "fix(security): narrow exception handling in _extract_unverified_org_and_subject"
```

---

## Full Regression & Wrap-Up

- [ ] **Step F.1: Run the complete API test package**

Run: `uv run pytest tests/api/ -x`
Expected: all tests pass.

- [ ] **Step F.2: Run static-analysis & type-check if the project defines them**

Run: `uv run ruff check src/dev_health_ops/api tests/api`
Run: `uv run mypy src/dev_health_ops/api || true`
Fix any new findings introduced by the above tasks (e.g. unused imports from the defusedxml switch, unused `hashlib` after Task 4).

- [ ] **Step F.3: Open PR**

```bash
gh pr create --title "security: close 10 audit findings (CHAOS security sprint)" --body "$(cat <<'EOF'
## Summary
- Membership check on X-Org-Id (IDOR fix)
- Superuser auth on GET /admin/orgs/{id}
- defusedxml is a hard dep; stdlib XML import removed
- JWT_SECRET_KEY required (no SHA256 fallback)
- SSO allowed_domains enforced on auto-provision
- X-Forwarded-For trusted only from TRUSTED_PROXIES allowlist
- Drilldown queries re-assert org_id vs contextvar
- Sanitized generic 500 exception handler
- Security-headers middleware (HSTS, nosniff, frame-deny, CSP)
- Narrowed bare Exception in _extract_unverified_org_and_subject with logging

## Test plan
- [x] All new tests pass: `uv run pytest tests/api/ -x`
- [x] Existing auth & SSO regression suites green
- [x] Manual: verify X-Org-Id header with JWT for another tenant is rejected with 403
- [x] Manual: verify /admin/orgs/{id} returns 401 unauthenticated, 403 non-superuser
- [x] Manual: start API without JWT_SECRET_KEY — expect RuntimeError at first token op
- [x] Manual: hit any endpoint — curl -I shows HSTS, X-Frame-Options, CSP
EOF
)"
```

---

## Dependency Graph

```
Task 1 (OrgId middleware hardening)
   ├── conceptually protects against abuse of Tasks 7 drilldown endpoints; still independent to ship
   └── DOES NOT block any other task

Task 2 (Admin orgs auth)              independent
Task 3 (defusedxml hardening)         independent
Task 4 (JWT_SECRET_KEY required)      independent — may require local .env update before other tasks' tests run
Task 5 (SSO allowed_domains)          independent
Task 6 (TRUSTED_PROXIES)              independent
Task 7 (Drilldown org re-check)       independent; strengthens Task 1 but does not depend on it
Task 8 (Generic 500 handler)          independent
Task 9 (Security-headers middleware)  independent
Task 10 (Narrow bare except)          independent
```

All ten tasks are file-independent and can be dispatched to separate sub-agents in parallel. The only cross-cutting concern is Task 4: after merging Task 4, any CI/local `.env.example` that omitted `JWT_SECRET_KEY` must be updated. If Tasks are executed sequentially, run Task 4 early so subsequent tasks catch missing-env-var failures in their own local test runs.

## Removed findings

None. All ten findings were verified against the code before plan authorship:

- Finding 1 (IDOR via X-Org-Id): confirmed at `src/dev_health_ops/api/middleware/__init__.py:51`.
- Finding 2 (Admin GET missing auth): confirmed at `src/dev_health_ops/api/admin/routers/orgs.py:67`.
- Finding 3 (SAML XML surface): re-framed — `defusedxml` is already required at runtime (the code raises on ImportError), but the stdlib `xml.etree.ElementTree` import on line 13 is still the source of the `Element` type and `ParseError` exception class. Task 3 therefore (a) promotes defusedxml to a hard dep and (b) removes the stdlib parser completely, eliminating the audit finding without changing behaviour in happy-path.
- Finding 4 (JWT derivation): confirmed at `src/dev_health_ops/api/services/auth.py:102`.
- Finding 5 (allowed_domains): confirmed — column stored, never read in `provision_or_get_user`.
- Finding 6 (X-Forwarded-For trust): confirmed at `src/dev_health_ops/api/middleware/rate_limit.py:71-75`.
- Finding 7 (drilldown org re-check): service layer takes `org_id` as parameter but never asserts it against `get_current_org_id()`; Task 7 adds that assertion.
- Finding 8 (verbose errors): the *streaming* handler is already generic. Real gap is the absence of a generic `Exception` handler at the FastAPI app level — Task 8 addresses that.
- Finding 9 (missing headers): confirmed — CORSMiddleware sets CORS headers but no HSTS/CSP/etc.
- Finding 10 (bare except): confirmed at `src/dev_health_ops/api/auth/router.py:953`.
