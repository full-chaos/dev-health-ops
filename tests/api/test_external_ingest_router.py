"""Tests for the external-ingest REST contract (CHAOS-2691).

Covers the brief's test plan: envelope/kind/size validation (D2/D4), the D1
idempotency header/body match, D6's fail-closed stream-unavailable mapping,
D7's interim auth mechanical gate (master-spec CC14), and D8's schema
discovery endpoints.
"""

from __future__ import annotations

import importlib
import sys

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.external_ingest.auth import IngestAuthContext
from dev_health_ops.api.external_ingest.schemas import (
    RECORD_KIND_MODELS,
    SCHEMA_VERSION,
)
from dev_health_ops.api.external_ingest.streams import StreamUnavailableError
from dev_health_ops.api.main import app

# __init__.py exports the APIRouter as "router", shadowing the module name —
# force-load the actual module so we can reach its internals (the bound
# per-scope dependency objects, enqueue_batch) the same way
# tests/test_ingest_api.py reaches api.ingest.router's _persist_telemetry.
importlib.import_module("dev_health_ops.api.external_ingest.router")
router_mod = sys.modules["dev_health_ops.api.external_ingest.router"]

BASE = "/api/v1/external-ingest"

TEST_CTX = IngestAuthContext(
    org_id="test-org", scopes={"ingest:write", "ingest:status", "schema:read"}
)


@pytest_asyncio.fixture
async def client():
    # Overriding require_ingest_scope (the factory) would not intercept
    # anything: router.py binds each scope's closure once at import time via
    # Depends(_require_schema_read)/Depends(_require_ingest_write), and
    # FastAPI's dependency_overrides matches on that exact callable — not on
    # the factory that produced it. Override the bound objects directly so
    # unit tests never exercise D7's real (WARN-logging) interim auth body.
    app.dependency_overrides[router_mod._require_schema_read] = lambda: TEST_CTX
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: TEST_CTX
    # raise_app_exceptions=False: Starlette's ServerErrorMiddleware sends the
    # sanitized 500 response *then* re-raises so ASGI-server-level logging
    # still sees it; without this, httpx's ASGITransport re-raises instead of
    # returning the response (matches tests/api/test_generic_exception_handler.py).
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c
    finally:
        app.dependency_overrides.pop(router_mod._require_schema_read, None)
        app.dependency_overrides.pop(router_mod._require_ingest_write, None)


def _record(kind: str, external_id: str, payload: dict) -> dict:
    return {"kind": kind, "externalId": external_id, "payload": payload}


VALID_PAYLOADS: dict[str, dict] = {
    "repository.v1": {"externalId": "acme/api", "sourceSystem": "github"},
    "identity.v1": {"canonicalId": "user-1", "updatedAt": "2026-06-25T00:00:00Z"},
    "team.v1": {
        "id": "team-1",
        "name": "Team One",
        "updatedAt": "2026-06-25T00:00:00Z",
    },
    "work_item.v1": {
        "externalKey": "ABC-123",
        "provider": "jira",
        "title": "Fix bug",
        "status": "in_progress",
        "createdAt": "2026-06-25T00:00:00Z",
    },
    "work_item_transition.v1": {
        "externalKey": "ABC-123",
        "provider": "jira",
        "occurredAt": "2026-06-25T00:00:00Z",
        "fromStatus": "todo",
        "toStatus": "in_progress",
    },
    "work_item_dependency.v1": {
        "sourceExternalKey": "ABC-123",
        "targetExternalKey": "ABC-124",
        "relationshipType": "blocks",
    },
    "pull_request.v1": {
        "repositoryExternalId": "acme/api",
        "number": 1,
        "state": "open",
        "createdAt": "2026-06-25T00:00:00Z",
    },
    "review.v1": {
        "repositoryExternalId": "acme/api",
        "pullRequestNumber": 1,
        "reviewId": "rev-1",
        "reviewer": "alice",
        "state": "APPROVED",
        "submittedAt": "2026-06-25T00:00:00Z",
    },
    "commit.v1": {
        "repositoryExternalId": "acme/api",
        "hash": "abc1234567",
        "authorWhen": "2026-06-25T00:00:00Z",
    },
}


def _envelope(records: list[dict], idempotency_key: str = "test-key-1") -> dict:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "idempotencyKey": idempotency_key,
        "source": {"type": "customer_push", "system": "github", "instance": "acme/api"},
        "records": records,
    }


# ---------------------------------------------------------------------------
# POST /batches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_accept_minimal_valid_batch(client, monkeypatch):
    calls = []

    def fake_enqueue(**kwargs):
        calls.append(kwargs)
        return "external-ingest:test-org:batches"

    monkeypatch.setattr(router_mod, "enqueue_batch", fake_enqueue)

    envelope = _envelope([_record("commit.v1", "abc123", VALID_PAYLOADS["commit.v1"])])
    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["itemsReceived"] == 1
    assert "ingestionId" in body
    assert body["stream"] == "external-ingest:test-org:batches"
    assert len(calls) == 1
    assert calls[0]["org_id"] == "test-org"
    assert calls[0]["source_system"] == "github"
    assert calls[0]["record_count"] == 1


@pytest.mark.asyncio
async def test_accept_batch_with_all_nine_kinds(client, monkeypatch):
    monkeypatch.setattr(router_mod, "enqueue_batch", lambda **kwargs: "stream-x")
    records = [
        _record(kind, f"ext-{kind}", payload)
        for kind, payload in VALID_PAYLOADS.items()
    ]

    resp = await client.post(f"{BASE}/batches", json=_envelope(records))

    assert resp.status_code == 202
    assert resp.json()["itemsReceived"] == len(VALID_PAYLOADS)


@pytest.mark.asyncio
async def test_missing_schema_version_returns_invalid_envelope(client):
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
    del envelope["schemaVersion"]

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_envelope"


@pytest.mark.asyncio
async def test_unsupported_schema_version_rejected(client):
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
    envelope["schemaVersion"] = "external-ingest.v2"

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "unsupported_schema_version"


@pytest.mark.asyncio
async def test_unknown_record_kind_rejected(client):
    envelope = _envelope([_record("deployment.v1", "d1", {"status": "success"})])

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "unknown_record_kind"


@pytest.mark.asyncio
async def test_unknown_top_level_envelope_field_rejected(client):
    # extra="forbid" on the wrapper models too (adversarial-review finding):
    # a typo'd top-level field must be a loud 400, not silently ignored, on
    # a versioned public contract advertised as matching /schemas exactly.
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
    envelope["unexpectedField"] = "typo"

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_envelope"


@pytest.mark.asyncio
async def test_unknown_source_field_rejected(client):
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
    envelope["source"]["unexpectedField"] = "typo"

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_envelope"


@pytest.mark.asyncio
async def test_unknown_record_wrapper_field_rejected(client):
    record = _record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])
    record["unexpectedField"] = "typo"

    resp = await client.post(f"{BASE}/batches", json=_envelope([record]))

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_envelope"


@pytest.mark.asyncio
async def test_unknown_window_field_rejected(client):
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
    envelope["window"] = {
        "startedAt": "2026-06-25T00:00:00Z",
        "endedAt": "2026-06-26T00:00:00Z",
        "unexpectedField": "typo",
    }

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_envelope"


@pytest.mark.asyncio
async def test_batch_too_large_rejected(client):
    # Payload contents are not deep-validated at /batches accept-time (only
    # envelope shape + kind allowlist), so an empty payload dict is enough
    # to exercise the record-count limit cheaply.
    records = [_record("commit.v1", f"c{i}", {}) for i in range(1001)]

    resp = await client.post(f"{BASE}/batches", json=_envelope(records))

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "batch_too_large"


@pytest.mark.asyncio
async def test_payload_too_large_rejected(client, monkeypatch):
    # Shrink the limit rather than construct a literal ~10MB fixture body.
    monkeypatch.setenv("EXTERNAL_INGEST_MAX_BODY_BYTES", "10")
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 413
    assert resp.json()["error"]["code"] == "payload_too_large"


@pytest.mark.asyncio
async def test_idempotency_header_matching_body_is_accepted(client, monkeypatch):
    monkeypatch.setattr(router_mod, "enqueue_batch", lambda **kwargs: "stream-x")
    envelope = _envelope(
        [_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])],
        idempotency_key="key-1",
    )

    resp = await client.post(
        f"{BASE}/batches", json=envelope, headers={"Idempotency-Key": "key-1"}
    )

    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_idempotency_header_mismatch_rejected(client):
    envelope = _envelope(
        [_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])],
        idempotency_key="key-1",
    )

    resp = await client.post(
        f"{BASE}/batches", json=envelope, headers={"Idempotency-Key": "different-key"}
    )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "idempotency_key_mismatch"


@pytest.mark.asyncio
async def test_stream_unavailable_returns_503(client, monkeypatch):
    def _raise(**kwargs):
        raise StreamUnavailableError("boom")

    monkeypatch.setattr(router_mod, "enqueue_batch", _raise)
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "stream_unavailable"


@pytest.mark.asyncio
async def test_unexpected_exception_falls_through_to_generic_handler(
    client, monkeypatch
):
    def _raise(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(router_mod, "enqueue_batch", _raise)
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 500
    # ExternalIngestError's handler doesn't accidentally swallow an unrelated
    # RuntimeError (Starlette dispatches by exact type), but the generic
    # handler still returns the customer-facing envelope for this path
    # (adversarial-review finding: one stable {"error": {...}} shape for
    # every /api/v1/external-ingest/* response, even unexpected 500s).
    assert resp.json() == {
        "error": {"code": "internal_error", "message": "Internal Server Error"}
    }


# ---------------------------------------------------------------------------
# POST /validate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validate_reports_per_record_errors(client):
    work_item_missing_title = {
        k: v for k, v in VALID_PAYLOADS["work_item.v1"].items() if k != "title"
    }
    bad_pull_request = {**VALID_PAYLOADS["pull_request.v1"], "state": "bogus"}
    records = [
        _record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"]),
        _record("work_item.v1", "w1", work_item_missing_title),
        _record("team.v1", "t1", VALID_PAYLOADS["team.v1"]),
        _record("pull_request.v1", "p1", bad_pull_request),
        _record("identity.v1", "i1", VALID_PAYLOADS["identity.v1"]),
    ]

    resp = await client.post(f"{BASE}/validate", json=_envelope(records))

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["itemsAccepted"] == 3
    assert body["itemsRejected"] == 2
    codes = {(err["index"], err["code"]) for err in body["errors"]}
    assert (1, "missing_required_field") in codes
    assert (3, "invalid_literal") in codes


@pytest.mark.asyncio
async def test_validate_fully_valid_batch(client):
    records = [
        _record(kind, f"ext-{kind}", payload)
        for kind, payload in VALID_PAYLOADS.items()
    ]

    resp = await client.post(f"{BASE}/validate", json=_envelope(records))

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is True
    assert body["itemsRejected"] == 0
    assert body["itemsAccepted"] == len(records)


# ---------------------------------------------------------------------------
# GET /schemas, GET /schemas/{version}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_schemas_requires_no_auth(client):
    resp = await client.get(f"{BASE}/schemas")

    assert resp.status_code == 200
    body = resp.json()
    assert body["schemaVersions"] == [SCHEMA_VERSION]
    assert body["recordKinds"] == sorted(RECORD_KIND_MODELS)
    assert "limits" in body
    assert set(body["limits"]) == {"maxRecordsPerBatch", "maxBodyBytes"}


@pytest.mark.asyncio
async def test_get_schema_returns_json_schema_per_kind(client):
    resp = await client.get(f"{BASE}/schemas/{SCHEMA_VERSION}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["schemaVersion"] == SCHEMA_VERSION
    assert set(body["recordKinds"]) == set(RECORD_KIND_MODELS)
    commit_schema = body["recordKinds"]["commit.v1"]
    assert "properties" in commit_schema
    assert "properties" in body["envelope"]


@pytest.mark.asyncio
async def test_get_schema_unknown_version_returns_404(client):
    resp = await client.get(f"{BASE}/schemas/external-ingest.v99")

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "unsupported_schema_version"


# ---------------------------------------------------------------------------
# Auth (D7 / master-spec CC14) — exercised WITHOUT the dependency override so
# the real require_ingest_scope body actually runs.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_authorization_header_returns_401(monkeypatch):
    monkeypatch.setenv("EXTERNAL_INGEST_INSECURE_AUTH", "1")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
        resp = await c.post(
            f"{BASE}/batches", json=envelope, headers={"X-Org-Id": "org-1"}
        )

    assert resp.status_code == 401
    # Auth failures must use the same customer-facing envelope as every other
    # external-ingest error (master-spec CC16 explicitly includes auth
    # failures) — a bare HTTPException {"detail": ...} would force customer
    # SDKs to special-case parsing (adversarial-review finding).
    assert resp.json()["error"]["code"] == "invalid_token"


@pytest.mark.asyncio
async def test_missing_org_id_header_returns_400(monkeypatch):
    monkeypatch.setenv("EXTERNAL_INGEST_INSECURE_AUTH", "1")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
        resp = await c.post(
            f"{BASE}/batches",
            json=envelope,
            headers={"Authorization": "Bearer dev-token"},
        )

    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "missing_org_header"


@pytest.mark.asyncio
async def test_interim_auth_hard_fails_without_insecure_flag(monkeypatch):
    # CC14: mechanical guard — auth 503s unless EXTERNAL_INGEST_INSECURE_AUTH=1
    # is explicitly set, regardless of otherwise-valid credentials.
    monkeypatch.delenv("EXTERNAL_INGEST_INSECURE_AUTH", raising=False)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
        resp = await c.post(
            f"{BASE}/batches",
            json=envelope,
            headers={"Authorization": "Bearer dev-token", "X-Org-Id": "org-1"},
        )

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "auth_not_configured"


# ---------------------------------------------------------------------------
# OpenAPI + schema round-trip
# ---------------------------------------------------------------------------


def test_routes_appear_in_openapi_schema():
    paths = app.openapi()["paths"]

    assert f"{BASE}/batches" in paths
    assert f"{BASE}/validate" in paths
    assert f"{BASE}/schemas" in paths
    assert f"{BASE}/schemas/{{schema_version}}" in paths


def test_record_kind_models_produce_valid_json_schema():
    for model in RECORD_KIND_MODELS.values():
        schema = model.model_json_schema(by_alias=True)
        assert isinstance(schema, dict)
        assert "properties" in schema
