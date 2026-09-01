from __future__ import annotations

import hashlib
import hmac
import json
import sys
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.main import app

ENDPOINT = "/api/v1/ingest/commits"
VALID_PAYLOAD = {
    "org_id": "test-org",
    "repo_url": "https://github.com/org/repo",
    "items": [
        {
            "hash": "abc123",
            "message": "fix: resolve login bug",
            "author_name": "Alice",
            "author_email": "alice@example.com",
            "author_when": "2025-01-15T10:00:00Z",
        }
    ],
}


def _compute_signature(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_auth_fails_closed_when_api_keys_unset(client, monkeypatch):
    """CHAOS-4720 (guardrail G-47): missing INGEST_API_KEYS must REJECT, not
    skip, outside an explicit development environment. Exercises the real
    ASGI app (production middleware stack), not the router in isolation --
    the CHAOS-3271 lesson (G-69). ``ENVIRONMENT`` here is "test" (the
    suite-wide conftest default), which is deliberately NOT treated as a
    development environment: a config unset by accident (the exact prod
    incident) must not fall into the permissive branch.
    """
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    resp = await client.post(ENDPOINT, json=VALID_PAYLOAD)
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid API key"


@pytest.mark.asyncio
async def test_auth_permissive_only_with_explicit_dev_opt_in(client, monkeypatch):
    """The pre-fix permissive behaviour survives ONLY behind an explicit,
    non-default ENVIRONMENT=development opt-in (CHAOS-4720 required fix:
    "impossible to enable accidentally in staging/production")."""
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    resp = await client.post(ENDPOINT, json=VALID_PAYLOAD)
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_auth_enabled_valid_key(client, monkeypatch):
    monkeypatch.setenv("INGEST_API_KEYS", "key-one,key-two")
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    resp = await client.post(
        ENDPOINT, json=VALID_PAYLOAD, headers={"X-API-Key": "key-one"}
    )
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_auth_enabled_invalid_key(client, monkeypatch):
    monkeypatch.setenv("INGEST_API_KEYS", "key-one,key-two")
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    resp = await client.post(
        ENDPOINT, json=VALID_PAYLOAD, headers={"X-API-Key": "wrong-key"}
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid API key"


@pytest.mark.asyncio
async def test_auth_enabled_missing_key(client, monkeypatch):
    monkeypatch.setenv("INGEST_API_KEYS", "key-one")
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    resp = await client.post(ENDPOINT, json=VALID_PAYLOAD)
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid API key"


@pytest.mark.asyncio
async def test_hmac_fails_closed_when_neither_credential_configured(
    client, monkeypatch
):
    """CHAOS-4720: with INGEST_SIGNING_SECRET also unset (the reported prod
    state -- both variables unset together), the HMAC-optional layer cannot
    silently stand in for "no auth required" either. Per the ticket's
    acceptance criteria ("HMAC signature verification behaves the same way
    with respect to INGEST_SIGNING_SECRET"), this mirrors
    test_auth_fails_closed_when_api_keys_unset. A signing secret configured
    on its own (API keys unset) remains valid HMAC-only auth --see
    test_hmac_enabled_valid_signature.
    """
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    resp = await client.post(ENDPOINT, json=VALID_PAYLOAD)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_hmac_enabled_valid_signature(client, monkeypatch):
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.setenv("INGEST_SIGNING_SECRET", "my-secret")
    body = json.dumps(VALID_PAYLOAD).encode("utf-8")
    sig = _compute_signature(body, "my-secret")
    resp = await client.post(
        ENDPOINT,
        content=body,
        headers={"Content-Type": "application/json", "X-Signature-256": sig},
    )
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_hmac_enabled_invalid_signature(client, monkeypatch):
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.setenv("INGEST_SIGNING_SECRET", "my-secret")
    resp = await client.post(
        ENDPOINT,
        json=VALID_PAYLOAD,
        headers={"X-Signature-256": "sha256=badhex"},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid signature"


@pytest.mark.asyncio
async def test_hmac_enabled_missing_signature_header(client, monkeypatch):
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.setenv("INGEST_SIGNING_SECRET", "my-secret")
    resp = await client.post(ENDPOINT, json=VALID_PAYLOAD)
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid signature"


@pytest.mark.asyncio
async def test_idempotency_first_request(client, monkeypatch):
    # ENVIRONMENT=development: this test isolates idempotency behaviour from
    # the CHAOS-4720 fail-closed auth check (both auth env vars are
    # deliberately unset here to test idempotency alone).
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")

    mock_rc = MagicMock()
    mock_rc.set.return_value = True
    mock_redis_mod = MagicMock()
    mock_redis_mod.from_url.return_value = mock_rc

    with patch.dict(sys.modules, {"valkey": mock_redis_mod}):
        resp = await client.post(
            ENDPOINT,
            json=VALID_PAYLOAD,
            headers={"X-Idempotency-Key": "req-001"},
        )

    assert resp.status_code == 202
    mock_rc.set.assert_called_once_with("idem:req-001", "1", nx=True, ex=86400)


@pytest.mark.asyncio
async def test_idempotency_duplicate_request(client, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")

    mock_rc = MagicMock()
    mock_rc.set.return_value = False
    mock_redis_mod = MagicMock()
    mock_redis_mod.from_url.return_value = mock_rc

    with patch.dict(sys.modules, {"valkey": mock_redis_mod}):
        resp = await client.post(
            ENDPOINT,
            json=VALID_PAYLOAD,
            headers={"X-Idempotency-Key": "req-001"},
        )

    assert resp.status_code == 409
    assert resp.json()["detail"] == "Duplicate request"


@pytest.mark.asyncio
async def test_idempotency_redis_unavailable(client, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")

    mock_redis_mod = MagicMock()
    mock_redis_mod.from_url.side_effect = ConnectionError("Redis down")

    with patch.dict(sys.modules, {"valkey": mock_redis_mod}):
        resp = await client.post(
            ENDPOINT,
            json=VALID_PAYLOAD,
            headers={"X-Idempotency-Key": "req-001"},
        )

    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_no_idempotency_key(client, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    resp = await client.post(ENDPOINT, json=VALID_PAYLOAD)
    assert resp.status_code == 202
