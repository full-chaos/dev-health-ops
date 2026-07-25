"""Tests for the real, DB-backed external-ingest auth dependency (CHAOS-2712).

Two layers, mirroring tests/api/webhooks/test_auth.py's direct-dependency-
function-call style plus a small end-to-end slice through the real router:

1. Direct calls into ``require_ingest_scope(scope)``'s returned dependency
   (an async-generator FastAPI dependency -- driven here the same way
   FastAPI itself drives it: ``await agen.__anext__()`` to reach the
   yielded ``IngestAuthContext``, ``await agen.aclose()`` to run its
   ``finally`` teardown). This lets tests assert precisely on ctx fields,
   audit-row persistence, and last_used bookkeeping without needing HTTP.
2. A handful of end-to-end ``httpx`` calls against a standalone app mounting
   the real ``external_ingest`` router, proving the full stack (error
   envelope shape, status codes) wires together correctly.

Covers master-spec CC14's 401/403 contract, the commit-before-raise audit
trap (docs/architecture/customer-push-authz.md), Design Decision 11's
last-used isolation, and regression coverage for the deleted
``EXTERNAL_INGEST_INSECURE_AUTH`` flag / ``X-Org-Id`` header path.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.external_ingest import auth as auth_module
from dev_health_ops.api.external_ingest.errors import (
    ExternalIngestError,
    register_external_ingest_error_handlers,
)
from dev_health_ops.api.external_ingest.schemas import SCHEMA_VERSION
from dev_health_ops.api.middleware import rate_limit as rate_limit_module
from dev_health_ops.api.services.auth import get_current_org_id
from dev_health_ops.models.audit import AuditAction, AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.ingest_auth import (
    IngestSource,
    IngestSourceMode,
    IngestToken,
    generate_ingest_token,
    hash_ingest_token,
)
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

# __init__.py exports the APIRouter as "router", shadowing the module name --
# force-load the actual module (mirrors tests/api/test_external_ingest_router.py).
importlib.import_module("dev_health_ops.api.external_ingest.router")
router_module = sys.modules["dev_health_ops.api.external_ingest.router"]

BASE = "/api/v1/external-ingest"

_TABLES = tables_of(
    Organization,
    User,
    IngestSource,
    IngestToken,
    AuditLog,
    FeatureFlag,
    OrgFeatureOverride,
    OrgLicense,
)
_CUSTOMER_PUSH_FEATURE = "customer_push_ingest"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "external_ingest_auth.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
def _patch_isolated_session(monkeypatch, session_maker):
    """Point auth.py's isolated-session writes (the last_used bump, called
    via ``get_postgres_session()`` rather than FastAPI's ``Depends()``) at
    this test's own sqlite engine -- otherwise it would try to reach the
    real (unconfigured) Postgres engine from db.py."""

    @asynccontextmanager
    async def _fake_get_postgres_session():
        async with session_maker() as session:
            yield session

    monkeypatch.setattr(auth_module, "get_postgres_session", _fake_get_postgres_session)


@pytest.fixture(autouse=True)
def _reset_ingest_auth_failure_limiter():
    """The shared ``limiter`` singleton (api/middleware/rate_limit.py) uses
    process-global in-memory storage -- without a reset, the new
    INGEST_AUTH_FAILURE_IP_LIMIT bucket (CHAOS-2712 adversarial-review fix)
    would accumulate hits across every failure-path test in this module that
    shares the default ``_make_request`` client_host, risking order-dependent
    429s in tests that don't intend to exercise the throttle itself."""
    reset = getattr(rate_limit_module.limiter, "reset", None)
    if reset is not None:
        try:
            reset()
        except Exception:
            pass
    yield


@pytest_asyncio.fixture(autouse=True)
async def _seed_customer_push_feature(session_maker):
    async with session_maker() as session:
        result = await session.execute(
            select(FeatureFlag).where(FeatureFlag.key == _CUSTOMER_PUSH_FEATURE)
        )
        if result.scalar_one_or_none() is None:
            session.add(
                FeatureFlag(
                    key=_CUSTOMER_PUSH_FEATURE,
                    name="Customer Push Ingest",
                    category="integrations",
                    min_tier="team",
                )
            )
            await session.commit()


@pytest_asyncio.fixture
async def org_id(session_maker) -> str:
    oid = uuid.uuid4()
    async with session_maker() as session:
        session.add(Organization(id=oid, slug="test-org", name="Test Org", tier="team"))
        await session.commit()
    return str(oid)


@pytest_asyncio.fixture
async def http_client(session_maker):
    """Standalone app mounting only the real external-ingest router, with
    ``get_postgres_session_dep`` (the Depends()-injected session used for
    token/source lookups and commit-before-raise audit writes) overridden to
    the same test engine. No dependency_overrides for auth itself -- the
    real ``require_ingest_scope`` body runs end-to-end."""
    app = FastAPI()
    app.include_router(router_module.router)
    register_external_ingest_error_handlers(app)

    async def _session_override():
        async with session_maker() as session:
            yield session

    app.dependency_overrides[auth_module.get_postgres_session_dep] = _session_override

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(
    *,
    authorization: str | None = None,
    client_host: str = "203.0.113.5",
    path: str = f"{BASE}/validate",
) -> Request:
    headers = []
    if authorization is not None:
        headers.append((b"authorization", authorization.encode()))
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": headers,
        "client": (client_host, 12345),
    }
    return Request(scope)


async def _create_token(
    session_maker,
    org_id: str,
    *,
    scopes: list[str],
    source_id: uuid.UUID | None = None,
    revoked: bool = False,
    expired: bool = False,
) -> tuple[str, IngestToken]:
    plaintext = generate_ingest_token()
    now = datetime.now(timezone.utc)
    token = IngestToken(
        org_id=org_id,
        source_id=source_id,
        name="test-token",
        token_hash=hash_ingest_token(plaintext),
        token_prefix=plaintext[:12],
        scopes=scopes,
        revoked_at=(now - timedelta(minutes=1)) if revoked else None,
        expires_at=(now - timedelta(minutes=1)) if expired else None,
    )
    async with session_maker() as session:
        session.add(token)
        await session.commit()
    return plaintext, token


def _minimal_envelope() -> dict:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "idempotencyKey": "test-key-1",
        "source": {
            "type": "customer_push",
            "system": "github",
            "instance": "acme/api",
        },
        "records": [{"kind": "commit.v1", "externalId": "c1", "payload": {}}],
    }


async def _audit_rows(session_maker, org_id: str) -> list[AuditLog]:
    async with session_maker() as session:
        result = await session.execute(
            select(AuditLog).where(AuditLog.org_id == uuid.UUID(org_id))
        )
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Direct dependency-level tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_authorization_header_401(session_maker):
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(authorization=None)
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()
    assert exc_info.value.status_code == 401
    assert exc_info.value.code == "invalid_token"


@pytest.mark.asyncio
async def test_malformed_bearer_prefix_401(session_maker):
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(authorization="Bearer not-an-fcpush-token")
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()
    assert exc_info.value.status_code == 401
    assert exc_info.value.code == "invalid_token"


@pytest.mark.asyncio
async def test_unknown_token_hash_401(session_maker):
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(
        authorization=f"Bearer {auth_module.TOKEN_PREFIX}does-not-exist-in-db"
    )
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()
    assert exc_info.value.status_code == 401
    assert exc_info.value.code == "invalid_token"


@pytest.mark.asyncio
async def test_unknown_token_hash_does_not_persist_audit_row(session_maker, org_id):
    # No org is resolvable for a hash that matches no row -- nothing valid
    # to attach an AuditLog FK to, so this must not crash and must not write.
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(
        authorization=f"Bearer {auth_module.TOKEN_PREFIX}does-not-exist-in-db"
    )
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError):
            await agen.__anext__()
    assert await _audit_rows(session_maker, org_id) == []


@pytest.mark.asyncio
async def test_revoked_token_401_and_audit_row_survives_the_raise(
    session_maker, org_id
):
    plaintext, token = await _create_token(
        session_maker, org_id, scopes=["schema:read"], revoked=True
    )
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(authorization=f"Bearer {plaintext}")
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()
    assert exc_info.value.status_code == 401
    assert exc_info.value.code == "invalid_token"

    # Regression (commit-before-raise, docs/architecture/customer-push-authz.md):
    # the audit row must persist even though this same request raised
    # immediately afterward on the same session.
    rows = await _audit_rows(session_maker, org_id)
    assert len(rows) == 1
    assert rows[0].action == AuditAction.INGEST_AUTH_FAILED.value
    assert rows[0].status == "failure"
    assert rows[0].resource_id == str(token.id)


@pytest.mark.asyncio
async def test_expired_token_401_and_audit_row_survives_the_raise(
    session_maker, org_id
):
    plaintext, token = await _create_token(
        session_maker, org_id, scopes=["schema:read"], expired=True
    )
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(authorization=f"Bearer {plaintext}")
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()
    assert exc_info.value.status_code == 401

    rows = await _audit_rows(session_maker, org_id)
    assert len(rows) == 1
    assert rows[0].resource_id == str(token.id)


@pytest.mark.asyncio
async def test_insufficient_scope_403_and_audit_row_survives_the_raise(
    session_maker, org_id
):
    plaintext, token = await _create_token(
        session_maker, org_id, scopes=["schema:read"]
    )
    dep = auth_module.require_ingest_scope("ingest:write")
    request = _make_request(authorization=f"Bearer {plaintext}")
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()
    assert exc_info.value.status_code == 403
    assert exc_info.value.code == "insufficient_scope"

    rows = await _audit_rows(session_maker, org_id)
    assert len(rows) == 1
    assert rows[0].action == AuditAction.INGEST_AUTH_FAILED.value
    assert rows[0].resource_id == str(token.id)

    # Design Decision 11: last_used is bumped on ANY presented valid token,
    # even one that ultimately fails its scope check.
    async with session_maker() as session:
        refreshed = await session.get(IngestToken, token.id)
    assert refreshed.last_used_at is not None


@pytest.mark.asyncio
async def test_valid_token_yields_ctx_and_resets_org_contextvar(session_maker, org_id):
    plaintext, token = await _create_token(
        session_maker, org_id, scopes=["schema:read", "ingest:status"]
    )
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(authorization=f"Bearer {plaintext}")
    before = get_current_org_id()
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        ctx = await agen.__anext__()
        assert ctx.org_id == org_id
        assert ctx.scopes == frozenset({"schema:read", "ingest:status"})
        assert ctx.token_id == str(token.id)
        assert ctx.source is None
        assert get_current_org_id() == org_id
        assert request.state.ingest_token_id == str(token.id)
        await agen.aclose()
    assert get_current_org_id() == before

    # last_used_at/last_used_ip recorded via the isolated session.
    async with session_maker() as session:
        refreshed = await session.get(IngestToken, token.id)
    assert refreshed.last_used_at is not None
    assert refreshed.last_used_ip == "203.0.113.5"


@pytest.mark.asyncio
async def test_valid_token_rejected_when_customer_push_feature_disabled(
    session_maker, org_id
):
    async with session_maker() as session:
        result = await session.execute(
            select(FeatureFlag).where(FeatureFlag.key == _CUSTOMER_PUSH_FEATURE)
        )
        feature = result.scalar_one()
        feature.is_enabled = False
        await session.commit()

    plaintext, token = await _create_token(
        session_maker, org_id, scopes=["schema:read"]
    )
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(authorization=f"Bearer {plaintext}")
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()

    assert exc_info.value.status_code == 403
    assert exc_info.value.code == "feature_not_enabled"

    rows = await _audit_rows(session_maker, org_id)
    assert len(rows) == 1
    assert rows[0].resource_id == str(token.id)


@pytest.mark.asyncio
async def test_valid_token_can_read_availability_when_customer_push_feature_disabled(
    session_maker, org_id
):
    async with session_maker() as session:
        result = await session.execute(
            select(FeatureFlag).where(FeatureFlag.key == _CUSTOMER_PUSH_FEATURE)
        )
        feature = result.scalar_one()
        feature.is_enabled = False
        await session.commit()

    plaintext, token = await _create_token(
        session_maker, org_id, scopes=["schema:read"]
    )
    dep = auth_module.require_ingest_scope(
        "schema:read",
        require_customer_push_feature=False,
    )
    request = _make_request(authorization=f"Bearer {plaintext}")
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        ctx = await agen.__anext__()
        await agen.aclose()

    assert ctx.org_id == org_id
    assert ctx.token_id == str(token.id)


@pytest.mark.asyncio
async def test_availability_route_reports_disabled_customer_push_for_valid_token(
    session_maker, org_id, http_client
):
    async with session_maker() as session:
        result = await session.execute(
            select(FeatureFlag).where(FeatureFlag.key == _CUSTOMER_PUSH_FEATURE)
        )
        feature = result.scalar_one()
        feature.is_enabled = False
        await session.commit()

    plaintext, _token = await _create_token(
        session_maker, org_id, scopes=["schema:read"]
    )

    response = await http_client.get(
        f"{BASE}/availability",
        headers={"Authorization": f"Bearer {plaintext}"},
    )

    assert response.status_code == 200
    assert response.json()["features"]["customerPushIngest"] is False
    assert response.json()["availableRecordKinds"] == []


@pytest.mark.asyncio
async def test_valid_token_rejected_when_org_below_customer_push_tier(
    session_maker, org_id
):
    async with session_maker() as session:
        org = await session.get(Organization, uuid.UUID(org_id))
        org.tier = "community"
        await session.commit()

    plaintext, _token = await _create_token(
        session_maker, org_id, scopes=["schema:read"]
    )
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(authorization=f"Bearer {plaintext}")
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()

    assert exc_info.value.status_code == 403
    assert exc_info.value.code == "feature_not_enabled"


@pytest.mark.asyncio
async def test_valid_token_with_bound_source_resolves_ctx_source(session_maker, org_id):
    source_id = uuid.uuid4()
    async with session_maker() as session:
        session.add(
            IngestSource(
                id=source_id,
                org_id=org_id,
                system="github",
                instance="acme/api",
                mode=IngestSourceMode.CUSTOMER_PUSH.value,
                enabled=True,
            )
        )
        await session.commit()

    plaintext, _token = await _create_token(
        session_maker, org_id, scopes=["ingest:write"], source_id=source_id
    )
    dep = auth_module.require_ingest_scope("ingest:write")
    request = _make_request(authorization=f"Bearer {plaintext}")
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        ctx = await agen.__anext__()
        assert ctx.source is not None
        assert ctx.source.id == source_id
        assert ctx.source.instance == "acme/api"
        await agen.aclose()


@pytest.mark.asyncio
async def test_two_tokens_resolve_to_their_own_distinct_orgs(session_maker):
    org_a = str(uuid.uuid4())
    org_b = str(uuid.uuid4())
    async with session_maker() as session:
        session.add_all(
            [
                Organization(
                    id=uuid.UUID(org_a), slug="org-a", name="Org A", tier="team"
                ),
                Organization(
                    id=uuid.UUID(org_b), slug="org-b", name="Org B", tier="team"
                ),
            ]
        )
        await session.commit()

    plaintext_a, _ = await _create_token(session_maker, org_a, scopes=["schema:read"])
    plaintext_b, _ = await _create_token(session_maker, org_b, scopes=["schema:read"])

    dep = auth_module.require_ingest_scope("schema:read")
    async with session_maker() as db:
        agen_a = dep(
            request=_make_request(authorization=f"Bearer {plaintext_a}"), db=db
        )
        ctx_a = await agen_a.__anext__()
        await agen_a.aclose()
    async with session_maker() as db:
        agen_b = dep(
            request=_make_request(authorization=f"Bearer {plaintext_b}"), db=db
        )
        ctx_b = await agen_b.__anext__()
        await agen_b.aclose()

    assert ctx_a.org_id == org_a
    assert ctx_b.org_id == org_b
    assert ctx_a.org_id != ctx_b.org_id


@pytest.mark.asyncio
async def test_revoked_after_first_use_fails_immediately_next_request(
    session_maker, org_id
):
    """No caching beyond a single request (team-lead scope item 5): a token
    valid on request 1 must fail on request 2 the instant it's revoked."""
    plaintext, token = await _create_token(
        session_maker, org_id, scopes=["schema:read"]
    )
    dep = auth_module.require_ingest_scope("schema:read")

    async with session_maker() as db:
        agen = dep(request=_make_request(authorization=f"Bearer {plaintext}"), db=db)
        ctx = await agen.__anext__()
        assert ctx.org_id == org_id
        await agen.aclose()

    async with session_maker() as session:
        row = await session.get(IngestToken, token.id)
        row.revoked_at = datetime.now(timezone.utc)
        await session.commit()

    async with session_maker() as db:
        agen = dep(request=_make_request(authorization=f"Bearer {plaintext}"), db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_insecure_auth_env_flag_no_longer_has_any_effect(
    session_maker, monkeypatch
):
    monkeypatch.setenv("EXTERNAL_INGEST_INSECURE_AUTH", "1")
    dep = auth_module.require_ingest_scope("schema:read")
    request = _make_request(authorization="Bearer totally-not-fcpush-prefixed")
    async with session_maker() as db:
        agen = dep(request=request, db=db)
        with pytest.raises(ExternalIngestError) as exc_info:
            await agen.__anext__()
    assert exc_info.value.status_code == 401


# ---------------------------------------------------------------------------
# Pre-auth IP throttle (CHAOS-2712 adversarial-review fix)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repeated_auth_failures_from_same_ip_eventually_rate_limited(
    session_maker,
):
    # A distinct IP, unbound to any other test's usage of the shared limiter.
    ip = "198.51.100.42"
    dep = auth_module.require_ingest_scope("schema:read")
    limit = int(rate_limit_module.INGEST_AUTH_FAILURE_IP_LIMIT.split("/")[0])

    async def _one_failed_attempt():
        request = _make_request(authorization="Bearer not-fcpush", client_host=ip)
        async with session_maker() as db:
            agen = dep(request=request, db=db)
            with pytest.raises(ExternalIngestError) as exc_info:
                await agen.__anext__()
            return exc_info.value

    for _ in range(limit):
        err = await _one_failed_attempt()
        assert err.status_code == 401

    # One more over the limit -- rate limited before it even reaches the
    # token-hash DB lookup, regardless of what the bearer value is.
    err = await _one_failed_attempt()
    assert err.status_code == 429
    assert err.code == "rate_limited"


@pytest.mark.asyncio
async def test_successful_auth_never_consumes_the_failure_limiter_bucket(
    session_maker, org_id
):
    # A token that succeeds every time must never trip the failure-only
    # throttle, however many times it's presented from the same IP.
    ip = "198.51.100.43"
    plaintext, _token = await _create_token(
        session_maker, org_id, scopes=["schema:read"]
    )
    dep = auth_module.require_ingest_scope("schema:read")
    limit = int(rate_limit_module.INGEST_AUTH_FAILURE_IP_LIMIT.split("/")[0])

    for _ in range(limit + 5):
        request = _make_request(authorization=f"Bearer {plaintext}", client_host=ip)
        async with session_maker() as db:
            agen = dep(request=request, db=db)
            ctx = await agen.__anext__()
            assert ctx.org_id == org_id
            await agen.aclose()


@pytest.mark.asyncio
async def test_concurrent_unknown_token_floods_are_bounded_by_attempt_limiter(
    session_maker,
):
    """2nd-round adversarial-review regression: a test()-then-hit() split
    is not atomic (the token-hash DB lookup between the two calls is itself
    an await point), so a burst of CONCURRENT invalid-token requests from
    one IP could previously all observe spare capacity and all reach
    Postgres before any of them were counted.
    ``_reserve_auth_attempt_or_429`` fixes this by consuming atomically via
    hit() before any DB work -- this test proves the number of DB lookups
    actually performed under a concurrent flood is bounded by the attempt
    ceiling, not merely the eventual (post-hoc) 429 count."""
    ip = "198.51.100.44"
    dep = auth_module.require_ingest_scope("schema:read")
    attempt_limit = int(rate_limit_module.INGEST_AUTH_ATTEMPT_IP_LIMIT.split("/")[0])
    overflow = 10

    lookup_count = 0
    real_execute = AsyncSession.execute

    async def _counting_execute(self, *args, **kwargs):
        nonlocal lookup_count
        lookup_count += 1
        return await real_execute(self, *args, **kwargs)

    async def _one_attempt(index: int):
        request = _make_request(
            authorization=f"Bearer {auth_module.TOKEN_PREFIX}nonexistent-{index}",
            client_host=ip,
        )
        async with session_maker() as db:
            db.execute = _counting_execute.__get__(db, AsyncSession)
            agen = dep(request=request, db=db)
            with pytest.raises(ExternalIngestError) as exc_info:
                await agen.__anext__()
            return exc_info.value.status_code

    results = await asyncio.gather(
        *[_one_attempt(i) for i in range(attempt_limit + overflow)]
    )

    assert results.count(429) == overflow
    assert results.count(401) == attempt_limit
    # The regression proof: every allowed-through request performs exactly
    # one DB lookup, and the reservation caps how many are ever allowed
    # through -- concurrency cannot inflate this beyond the ceiling.
    assert lookup_count == attempt_limit


# ---------------------------------------------------------------------------
# Source binding (CHAOS-2712 adversarial-review fix: require_matching_source)
# ---------------------------------------------------------------------------


def test_require_matching_source_passes_for_matching_write_eligible_source():
    source = IngestSource(
        org_id="org-1",
        system="github",
        instance="acme/api",
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=True,
    )
    ctx = auth_module.IngestAuthContext(
        org_id="org-1", scopes=frozenset({"ingest:write"}), source=source
    )
    assert auth_module.require_matching_source(ctx, "github", "acme/api") is source


def test_require_matching_source_rejects_mismatched_instance():
    source = IngestSource(
        org_id="org-1",
        system="github",
        instance="acme/api",
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=True,
    )
    ctx = auth_module.IngestAuthContext(
        org_id="org-1", scopes=frozenset({"ingest:write"}), source=source
    )
    with pytest.raises(ExternalIngestError) as exc_info:
        auth_module.require_matching_source(ctx, "github", "other/repo")
    assert exc_info.value.status_code == 403
    assert exc_info.value.code == "source_mismatch"


def test_require_matching_source_rejects_unbound_token():
    ctx = auth_module.IngestAuthContext(
        org_id="org-1", scopes=frozenset({"ingest:write"}), source=None
    )
    with pytest.raises(ExternalIngestError) as exc_info:
        auth_module.require_matching_source(ctx, "github", "acme/api")
    assert exc_info.value.status_code == 403
    assert exc_info.value.code == "source_mismatch"


def test_require_matching_source_rejects_disabled_source():
    source = IngestSource(
        org_id="org-1",
        system="github",
        instance="acme/api",
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=False,
    )
    ctx = auth_module.IngestAuthContext(
        org_id="org-1", scopes=frozenset({"ingest:write"}), source=source
    )
    with pytest.raises(ExternalIngestError) as exc_info:
        auth_module.require_matching_source(ctx, "github", "acme/api")
    assert exc_info.value.status_code == 403
    assert exc_info.value.code == "source_disabled"


def test_require_matching_source_rejects_non_customer_push_mode():
    source = IngestSource(
        org_id="org-1",
        system="github",
        instance="acme/api",
        mode=IngestSourceMode.FULLCHAOS_SYNC.value,
        enabled=True,
    )
    ctx = auth_module.IngestAuthContext(
        org_id="org-1", scopes=frozenset({"ingest:write"}), source=source
    )
    with pytest.raises(ExternalIngestError) as exc_info:
        auth_module.require_matching_source(ctx, "github", "acme/api")
    assert exc_info.value.status_code == 403
    assert exc_info.value.code == "source_disabled"


# ---------------------------------------------------------------------------
# End-to-end (real router, real auth dependency, no auth override)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_e2e_missing_authorization_header_401_envelope(http_client):
    resp = await http_client.post(f"{BASE}/validate", json=_minimal_envelope())
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_token"


@pytest.mark.asyncio
async def test_e2e_unknown_token_401_envelope(http_client):
    resp = await http_client.post(
        f"{BASE}/validate",
        json=_minimal_envelope(),
        headers={"Authorization": f"Bearer {auth_module.TOKEN_PREFIX}nope"},
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_token"


@pytest.mark.asyncio
async def test_e2e_valid_token_wrong_scope_403_envelope(
    session_maker, org_id, http_client
):
    plaintext, _ = await _create_token(session_maker, org_id, scopes=["ingest:status"])
    resp = await http_client.post(
        f"{BASE}/validate",
        json=_minimal_envelope(),
        headers={"Authorization": f"Bearer {plaintext}"},
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "insufficient_scope"


@pytest.mark.asyncio
async def test_e2e_valid_token_with_scope_reaches_route_handler(
    session_maker, org_id, http_client
):
    plaintext, _ = await _create_token(session_maker, org_id, scopes=["schema:read"])
    resp = await http_client.post(
        f"{BASE}/validate",
        json=_minimal_envelope(),
        headers={"Authorization": f"Bearer {plaintext}"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_e2e_insecure_auth_flag_and_x_org_id_header_have_no_effect(
    http_client, monkeypatch
):
    monkeypatch.setenv("EXTERNAL_INGEST_INSECURE_AUTH", "1")
    resp = await http_client.post(
        f"{BASE}/validate",
        json=_minimal_envelope(),
        headers={"Authorization": "Bearer dev-token", "X-Org-Id": "org-1"},
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_token"
