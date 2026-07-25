"""Tests for the external-ingest REST contract (CHAOS-2691 + CHAOS-2695).

Covers the brief's test plan: envelope/kind/size validation (D2/D4), the D1
idempotency header/body match, D6's fail-closed stream-unavailable mapping,
D8's schema discovery endpoints, and (CHAOS-2695) the CC22 accept flow --
NEW/REPLAY/CONFLICT/RETRY against a REAL aiosqlite-backed session (only
``enqueue_batch`` is faked; the status/payload/ownership tables are live, so
these tests exercise the actual store SQL). Auth (`require_ingest_scope`'s
real, DB-backed body -- CHAOS-2712) is exercised end-to-end in
tests/api/external_ingest/test_auth.py; this file overrides the bound scope
dependencies for every test below.
"""

from __future__ import annotations

import importlib
import json
import sys
import uuid as uuid_mod
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.external_ingest import status as status_mod
from dev_health_ops.api.external_ingest.auth import IngestAuthContext
from dev_health_ops.api.external_ingest.schemas import (
    RECORD_KIND_MODELS,
    SCHEMA_VERSION,
)
from dev_health_ops.api.external_ingest.streams import StreamUnavailableError
from dev_health_ops.api.main import app
from dev_health_ops.core.encryption import encrypt_value
from dev_health_ops.external_ingest.feature_gate import (
    ExternalIngestFeatureAvailability,
)
from dev_health_ops.external_ingest.payload_store import payload_exists
from dev_health_ops.models.external_ingest import (
    ExternalIngestBatch,
    ExternalIngestBatchPayload,
    ExternalIngestRecomputeJob,
    ExternalIngestRejection,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.ingest_auth import IngestSource, IngestSourceMode
from dev_health_ops.models.integrations import Integration, IntegrationSource
from dev_health_ops.models.settings import IntegrationCredential
from tests._helpers import tables_of

# __init__.py exports the APIRouter as "router", shadowing the module name —
# force-load the actual module so we can reach its internals (the bound
# per-scope dependency objects, enqueue_batch) the same way
# tests/test_ingest_api.py reaches api.ingest.router's _persist_telemetry.
importlib.import_module("dev_health_ops.api.external_ingest.router")
router_mod = sys.modules["dev_health_ops.api.external_ingest.router"]

BASE = "/api/v1/external-ingest"

# Matches the (system, instance) every _envelope() below declares -- accept_batch
# now enforces require_matching_source (CHAOS-2712 adversarial-review fix), so
# the write-scoped test context needs a bound, write-eligible source or every
# POST /batches test would 403 source_mismatch.
_TEST_SOURCE = IngestSource(
    org_id="test-org",
    system="github",
    instance="acme/api",
    mode=IngestSourceMode.CUSTOMER_PUSH.value,
    enabled=True,
)

TEST_CTX = IngestAuthContext(
    org_id="test-org",
    scopes=frozenset({"ingest:write", "ingest:status", "schema:read"}),
    source=_TEST_SOURCE,
)

_TABLES = tables_of(
    ExternalIngestBatch,
    ExternalIngestRejection,
    ExternalIngestBatchPayload,
    ExternalIngestRecomputeJob,
    IngestSource,
    Integration,
    IntegrationSource,
    IntegrationCredential,
)


async def _default_fake_enqueue(**kwargs) -> str:
    return "stream-x"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    """Real aiosqlite DB (CHAOS-2695): the accept flow's ownership resolve +
    idempotency store + payload upsert all run their actual SQL. Seeds the
    registered write-eligible source row matching TEST_CTX's bound source --
    ``resolve_effective_mode`` reads external_ingest_sources for it."""
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'external-ingest-router.db'}"
    )
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as session:
        session.add(
            IngestSource(
                org_id="test-org",
                system="github",
                instance="acme/api",
                mode=IngestSourceMode.CUSTOMER_PUSH.value,
                enabled=True,
            )
        )
        await session.commit()
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def client(session_maker, monkeypatch):
    # Overriding require_ingest_scope (the factory) would not intercept
    # anything: router.py binds each scope's closure once at import time via
    # Depends(_require_schema_read)/Depends(_require_ingest_write), and
    # FastAPI's dependency_overrides matches on that exact callable — not on
    # the factory that produced it. Override the bound objects directly so
    # unit tests never exercise the real DB-backed auth body.
    async def _session_override():
        async with session_maker() as session:
            yield session

    app.dependency_overrides[router_mod._require_schema_read] = lambda: TEST_CTX
    app.dependency_overrides[router_mod._require_schema_availability] = lambda: TEST_CTX
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: TEST_CTX
    app.dependency_overrides[router_mod.get_postgres_session_dep] = _session_override
    monkeypatch.setattr(
        router_mod,
        "_canonical_incident_ingestion_allowed",
        AsyncMock(return_value=True),
        raising=False,
    )
    monkeypatch.setattr(
        router_mod,
        "_external_ingest_feature_availability",
        AsyncMock(
            return_value=ExternalIngestFeatureAvailability(
                customer_push_ingest=True,
                canonical_incident_ingestion=True,
            )
        ),
        raising=False,
    )
    # Only the Valkey stream boundary is faked; individual tests override
    # enqueue_batch again for their own success/failure scenarios.
    monkeypatch.setattr(router_mod, "enqueue_batch", _default_fake_enqueue)
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
        app.dependency_overrides.pop(router_mod._require_schema_availability, None)
        app.dependency_overrides.pop(router_mod._require_ingest_write, None)
        app.dependency_overrides.pop(router_mod.get_postgres_session_dep, None)


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

    async def fake_enqueue(**kwargs):
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
async def test_accept_batch_with_all_nine_kinds(client):
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
    async def _fake_enqueue(**kwargs) -> str:
        return "stream-x"

    monkeypatch.setattr(router_mod, "enqueue_batch", _fake_enqueue)
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
async def test_enqueue_failure_keeps_payload_and_marks_stream_unavailable(
    client, session_maker, monkeypatch
):
    """CHAOS-2695 supersedes CHAOS-2693's interim orphan-delete: the payload
    row is now referenced by the durable status row (status
    ``stream_unavailable``) and the client's same-key retry reuses BOTH via
    the same ingestion_id -- deleting it could also black-hole an enqueue
    whose XADD actually landed before the error surfaced."""

    def _raise(**kwargs):
        raise StreamUnavailableError("boom")

    monkeypatch.setattr(router_mod, "enqueue_batch", _raise)
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 503
    assert resp.json()["error"]["code"] == "stream_unavailable"
    async with session_maker() as session:
        batch = await status_mod.find_existing_batch(
            session,
            org_id="test-org",
            source_system="github",
            source_instance="acme/api",
            idempotency_key="test-key-1",
        )
        assert batch is not None
        assert batch.status == "stream_unavailable"
        assert str(batch.ingestion_id) in resp.json()["error"]["message"]
        assert await payload_exists(
            session, ingestion_id=batch.ingestion_id, org_id="test-org"
        )


@pytest.mark.asyncio
async def test_retry_after_stream_unavailable_reuses_ingestion_id(
    client, session_maker, monkeypatch
):
    """RETRY (brief decision 7): a same-key resubmit after a 503 must
    re-attempt the enqueue on the SAME ingestion_id (attempts += 1), never
    mint a second row or replay the stale stream_unavailable status."""

    def _raise(**kwargs):
        raise StreamUnavailableError("boom")

    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])

    monkeypatch.setattr(router_mod, "enqueue_batch", _raise)
    first = await client.post(f"{BASE}/batches", json=envelope)
    assert first.status_code == 503

    enqueued: list[dict] = []

    async def _ok(**kwargs):
        enqueued.append(kwargs)
        return "stream-x"

    monkeypatch.setattr(router_mod, "enqueue_batch", _ok)
    second = await client.post(f"{BASE}/batches", json=envelope)

    assert second.status_code == 202
    body = second.json()
    async with session_maker() as session:
        batch = await status_mod.find_existing_batch(
            session,
            org_id="test-org",
            source_system="github",
            source_instance="acme/api",
            idempotency_key="test-key-1",
        )
    assert batch is not None
    assert body["ingestionId"] == str(batch.ingestion_id)
    assert batch.status == "accepted"
    assert batch.attempts == 2
    assert len(enqueued) == 1
    assert enqueued[0]["ingestion_id"] == str(batch.ingestion_id)


@pytest.mark.asyncio
async def test_accept_persists_batch_row_with_hash_and_producer(client, session_maker):
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
    envelope["source"]["producer"] = "acme-ci"
    envelope["source"]["producerVersion"] = "1.2.3"

    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 202
    async with session_maker() as session:
        batch = await status_mod.get_batch(
            session,
            org_id="test-org",
            ingestion_id=uuid_mod.UUID(resp.json()["ingestionId"]),
        )
    assert batch is not None
    assert batch.status == "accepted"
    assert batch.attempts == 1
    assert len(batch.payload_hash) == 64  # sha256 hex
    assert batch.producer == "acme-ci"
    assert batch.producer_version == "1.2.3"
    assert batch.items_received == 1


@pytest.mark.asyncio
async def test_replay_same_key_same_payload_returns_200_status_envelope(client):
    """REPLAY -> 200 (not 202) with the full GET /batches/{id}-shaped body
    (brief decision 8) and no second enqueue/row."""
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])

    first = await client.post(f"{BASE}/batches", json=envelope)
    assert first.status_code == 202

    second = await client.post(f"{BASE}/batches", json=envelope)

    assert second.status_code == 200
    body = second.json()
    assert body["ingestionId"] == first.json()["ingestionId"]
    assert body["status"] == "accepted"
    assert body["attempts"] == 1  # replay never increments
    assert body["itemsReceived"] == 1
    assert "recompute" in body  # full status envelope, not the 202 shape
    assert "stream" not in body


@pytest.mark.asyncio
async def test_replay_is_field_order_insensitive(client):
    """Canonicalization (brief decisions 1-2): reordering JSON keys must
    hash identically -> REPLAY, not CONFLICT."""
    record = _record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])
    envelope = _envelope([record])
    first = await client.post(f"{BASE}/batches", json=envelope)
    assert first.status_code == 202

    reordered = {
        "records": [
            {
                "payload": dict(reversed(list(record["payload"].items()))),
                "externalId": record["externalId"],
                "kind": record["kind"],
            }
        ],
        "source": envelope["source"],
        "idempotencyKey": envelope["idempotencyKey"],
        "schemaVersion": envelope["schemaVersion"],
    }
    second = await client.post(f"{BASE}/batches", json=reordered)

    assert second.status_code == 200
    assert second.json()["ingestionId"] == first.json()["ingestionId"]


@pytest.mark.asyncio
async def test_conflict_same_key_different_payload_409(client):
    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
    first = await client.post(f"{BASE}/batches", json=envelope)
    assert first.status_code == 202

    different = _envelope(
        [
            _record(
                "commit.v1",
                "OTHER",
                {**VALID_PAYLOADS["commit.v1"], "hash": "fff9999999"},
            )
        ]
    )
    second = await client.post(f"{BASE}/batches", json=different)

    assert second.status_code == 409
    assert second.json()["error"]["code"] == "idempotency_conflict"


@pytest.mark.asyncio
async def test_same_key_different_instance_is_independent(
    client, session_maker, monkeypatch
):
    """The idempotency namespace is (org, system, instance, key) -- reusing
    a key string against a DIFFERENT registered instance is a fresh NEW
    batch, not a conflict (brief §2)."""
    async with session_maker() as session:
        session.add(
            IngestSource(
                org_id="test-org",
                system="github",
                instance="acme/other",
                mode=IngestSourceMode.CUSTOMER_PUSH.value,
                enabled=True,
            )
        )
        await session.commit()
    other_ctx = IngestAuthContext(
        org_id="test-org",
        scopes=frozenset({"ingest:write"}),
        source=IngestSource(
            org_id="test-org",
            system="github",
            instance="acme/other",
            mode=IngestSourceMode.CUSTOMER_PUSH.value,
            enabled=True,
        ),
    )

    first = await client.post(
        f"{BASE}/batches",
        json=_envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])]),
    )
    assert first.status_code == 202

    app.dependency_overrides[router_mod._require_ingest_write] = lambda: other_ctx
    try:
        other_envelope = _envelope(
            [_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])]
        )
        other_envelope["source"]["instance"] = "acme/other"
        second = await client.post(f"{BASE}/batches", json=other_envelope)
    finally:
        app.dependency_overrides[router_mod._require_ingest_write] = lambda: TEST_CTX

    assert second.status_code == 202
    assert second.json()["ingestionId"] != first.json()["ingestionId"]


@pytest.mark.asyncio
async def test_accept_rejected_when_fullchaos_actively_owns_instance(
    client, session_maker
):
    """Accept-time one-active-owner re-check (CC5/CC14 defense in depth): a
    managed sync source connected to the same instance AFTER registration
    must 403 even though the token still binds to a write-eligible row."""
    async with session_maker() as session:
        integration = Integration(
            org_id="test-org", provider="GitHub", name="managed-github"
        )
        session.add(integration)
        await session.flush()
        session.add(
            IntegrationSource(
                org_id="test-org",
                integration_id=integration.id,
                provider="GitHub",  # mixed case on purpose: func.lower matching
                source_type="repository",
                external_id="acme/api",
                name="api",
                full_name="acme/api",
            )
        )
        await session.commit()

    envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
    resp = await client.post(f"{BASE}/batches", json=envelope)

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "source_owned_by_fullchaos_sync"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "instance"),
    (
        ("github", "https://ghe.acme.test:8443/api/v3"),
        ("gitlab", "https://gitlab.acme.test:8443/api/v4"),
    ),
)
async def test_operational_accept_rejects_linked_credential_host(
    client, session_maker, monkeypatch, provider: str, instance: str
):
    # Given: a registered operational source and managed integration sharing only a credential host.
    source = IngestSource(
        org_id="test-org",
        system=provider,
        instance=instance,
        entity_family="operational",
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=True,
    )
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")
    async with session_maker() as session:
        credential = IntegrationCredential(
            org_id="test-org",
            provider=provider,
            name=f"{provider}-credential",
            credentials_encrypted=encrypt_value(
                json.dumps({"token": "test-token", "base_url": instance})
            ),
        )
        session.add(credential)
        await session.flush()
        integration = Integration(
            org_id="test-org",
            provider=provider,
            credential_id=credential.id,
            name=f"managed-{provider}",
        )
        session.add_all((source, integration))
        await session.flush()
        session.add(
            IntegrationSource(
                org_id="test-org",
                integration_id=integration.id,
                provider=provider,
                source_type="repository",
                external_id="acme/api",
                name="api",
                full_name="acme/api",
            )
        )
        await session.commit()
    operational_context = IngestAuthContext(
        org_id="test-org",
        scopes=frozenset({"ingest:write"}),
        source=source,
    )
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: (
        operational_context
    )
    envelope = _envelope(
        [
            _record(
                "operational_incident.v1",
                "incident-1",
                {
                    "externalId": "incident-1",
                    "sourceVersionAt": "2026-07-17T00:00:00Z",
                    "title": "Database unavailable",
                },
            )
        ]
    )
    envelope["source"] = {
        "type": "customer_push",
        "system": provider,
        "instance": instance,
        "entityFamily": "operational",
    }

    # When: the credential-host source submits an operational incident.
    response = await client.post(f"{BASE}/batches", json=envelope)

    # Then: accept-time ownership prevents a duplicate operational writer.
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "source_owned_by_fullchaos_sync"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "instance", "environment_url"),
    (
        ("github", "https://ghe.acme.test:8443/api/v3", "GITHUB_URL"),
        ("gitlab", "https://gitlab.acme.test:8443/api/v4", "GITLAB_URL"),
    ),
)
async def test_operational_accept_rejects_environment_auth_host(
    client,
    session_maker,
    monkeypatch,
    provider: str,
    instance: str,
    environment_url: str,
):
    # Given: a registered source and credentialless managed integration sharing an environment host.
    source = IngestSource(
        org_id="test-org",
        system=provider,
        instance=instance,
        entity_family="operational",
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=True,
    )
    monkeypatch.setenv(f"{provider.upper()}_TOKEN", "test-token")
    monkeypatch.setenv(environment_url, instance)
    async with session_maker() as session:
        integration = Integration(
            org_id="test-org",
            provider=provider,
            name=f"managed-{provider}",
        )
        session.add_all((source, integration))
        await session.flush()
        session.add(
            IntegrationSource(
                org_id="test-org",
                integration_id=integration.id,
                provider=provider,
                source_type="repository",
                external_id="acme/api",
                name="api",
                full_name="acme/api",
            )
        )
        await session.commit()
    operational_context = IngestAuthContext(
        org_id="test-org",
        scopes=frozenset({"ingest:write"}),
        source=source,
    )
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: (
        operational_context
    )
    envelope = _envelope(
        [
            _record(
                "operational_incident.v1",
                "incident-1",
                {
                    "externalId": "incident-1",
                    "sourceVersionAt": "2026-07-17T00:00:00Z",
                    "title": "Database unavailable",
                },
            )
        ]
    )
    envelope["source"] = {
        "type": "customer_push",
        "system": provider,
        "instance": instance,
        "entityFamily": "operational",
    }

    # When: the environment-auth source submits an operational incident.
    response = await client.post(f"{BASE}/batches", json=envelope)

    # Then: accept-time ownership prevents a duplicate operational writer.
    assert response.status_code != 202
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "source_owned_by_fullchaos_sync"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "instance", "environment_url"),
    (
        ("github", "https://ghe.acme.test:8443/api/v3", "GITHUB_URL"),
        ("gitlab", "https://gitlab.acme.test:8443/api/v4", "GITLAB_URL"),
    ),
)
async def test_operational_accept_rejects_invalid_environment_auth_host(
    client,
    session_maker,
    monkeypatch,
    provider: str,
    instance: str,
    environment_url: str,
):
    # Given: a registered source and managed integration with an invalid environment host.
    source = IngestSource(
        org_id="test-org",
        system=provider,
        instance=instance,
        entity_family="operational",
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=True,
    )
    monkeypatch.setenv(f"{provider.upper()}_TOKEN", "test-token")
    monkeypatch.setenv(environment_url, "not a valid host")
    async with session_maker() as session:
        integration = Integration(
            org_id="test-org",
            provider=provider,
            name=f"managed-{provider}",
        )
        session.add_all((source, integration))
        await session.flush()
        session.add(
            IntegrationSource(
                org_id="test-org",
                integration_id=integration.id,
                provider=provider,
                source_type="repository",
                external_id="acme/api",
                name="api",
                full_name="acme/api",
            )
        )
        await session.commit()
    operational_context = IngestAuthContext(
        org_id="test-org",
        scopes=frozenset({"ingest:write"}),
        source=source,
    )
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: (
        operational_context
    )
    envelope = _envelope(
        [
            _record(
                "operational_incident.v1",
                "incident-1",
                {
                    "externalId": "incident-1",
                    "sourceVersionAt": "2026-07-17T00:00:00Z",
                    "title": "Database unavailable",
                },
            )
        ]
    )
    envelope["source"] = {
        "type": "customer_push",
        "system": provider,
        "instance": instance,
        "entityFamily": "operational",
    }

    # When: the source submits an operational incident for the same provider and org.
    response = await client.post(f"{BASE}/batches", json=envelope)

    # Then: accept fails closed instead of treating the invalid declaration as public.
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "ownership_resolution_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "instance", "environment_url"),
    (
        ("github", "https://ghe.other.test:8443/api/v3", "GITHUB_URL"),
        ("gitlab", "https://gitlab.other.test:8443/api/v4", "GITLAB_URL"),
    ),
)
async def test_operational_accept_allows_unrelated_host_without_environment_url(
    client,
    session_maker,
    monkeypatch,
    provider: str,
    instance: str,
    environment_url: str,
):
    # Given: a registered self-hosted source and a credentialless public managed integration.
    source = IngestSource(
        org_id="test-org",
        system=provider,
        instance=instance,
        entity_family="operational",
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=True,
    )
    monkeypatch.delenv(environment_url, raising=False)
    async with session_maker() as session:
        integration = Integration(
            org_id="test-org",
            provider=provider,
            name=f"managed-{provider}",
        )
        session.add_all((source, integration))
        await session.flush()
        session.add(
            IntegrationSource(
                org_id="test-org",
                integration_id=integration.id,
                provider=provider,
                source_type="repository",
                external_id="acme/api",
                name="api",
                full_name="acme/api",
            )
        )
        await session.commit()
    operational_context = IngestAuthContext(
        org_id="test-org",
        scopes=frozenset({"ingest:write"}),
        source=source,
    )
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: (
        operational_context
    )
    envelope = _envelope(
        [
            _record(
                "operational_incident.v1",
                "incident-1",
                {
                    "externalId": "incident-1",
                    "sourceVersionAt": "2026-07-17T00:00:00Z",
                    "title": "Database unavailable",
                },
            )
        ]
    )
    envelope["source"] = {
        "type": "customer_push",
        "system": provider,
        "instance": instance,
        "entityFamily": "operational",
    }

    # When: the unrelated self-hosted source submits an operational incident.
    response = await client.post(f"{BASE}/batches", json=envelope)

    # Then: no environment host preserves public ownership without blocking this push.
    assert response.status_code == 202


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "instance"),
    (
        ("github", "https://ghe.acme.test:8443/api/v3"),
        ("gitlab", "https://gitlab.acme.test:8443/api/v4"),
    ),
)
async def test_operational_accept_rejects_undecryptable_linked_credential_host(
    client, session_maker, monkeypatch, provider: str, instance: str
):
    # Given: a registered operational source and an active managed integration with no readable host.
    source = IngestSource(
        org_id="test-org",
        system=provider,
        instance=instance,
        entity_family="operational",
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=True,
    )
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")
    async with session_maker() as session:
        credential = IntegrationCredential(
            org_id="test-org",
            provider=provider,
            name=f"{provider}-credential",
            credentials_encrypted="undecryptable",
        )
        session.add(credential)
        await session.flush()
        integration = Integration(
            org_id="test-org",
            provider=provider,
            credential_id=credential.id,
            name=f"managed-{provider}",
        )
        session.add_all((source, integration))
        await session.flush()
        session.add(
            IntegrationSource(
                org_id="test-org",
                integration_id=integration.id,
                provider=provider,
                source_type="repository",
                external_id="acme/api",
                name="api",
                full_name="acme/api",
            )
        )
        await session.commit()
    operational_context = IngestAuthContext(
        org_id="test-org",
        scopes=frozenset({"ingest:write"}),
        source=source,
    )
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: (
        operational_context
    )
    envelope = _envelope(
        [
            _record(
                "operational_incident.v1",
                "incident-1",
                {
                    "externalId": "incident-1",
                    "sourceVersionAt": "2026-07-17T00:00:00Z",
                    "title": "Database unavailable",
                },
            )
        ]
    )
    envelope["source"] = {
        "type": "customer_push",
        "system": provider,
        "instance": instance,
        "entityFamily": "operational",
    }

    # When: the customer source submits an operational incident.
    response = await client.post(f"{BASE}/batches", json=envelope)

    # Then: accept fails closed while ownership cannot be resolved.
    assert response.status_code != 202
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "ownership_resolution_unavailable"
    assert "linked managed credential" in response.json()["error"]["message"]
    assert "undecryptable" not in response.json()["error"]["message"]


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
    # CHAOS-2692: recordKinds/envelope are $ref-indexed into $defs (built via
    # pydantic's models_json_schema), not full inlined per-kind schemas — see
    # tests/api/external_ingest/test_schemas_api.py for the full contract
    # (ETag, examples, 304 round-trip).
    commit_schema = body["recordKinds"]["commit.v1"]
    assert commit_schema["$ref"].startswith("#/$defs/")
    assert "properties" in body["$defs"][commit_schema["$ref"].rsplit("/", 1)[-1]]
    assert body["envelope"]["$ref"].startswith("#/$defs/")


@pytest.mark.asyncio
async def test_get_schema_unknown_version_returns_404(client):
    resp = await client.get(f"{BASE}/schemas/external-ingest.v99")

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "unsupported_schema_version"


# ---------------------------------------------------------------------------
# Source binding (CHAOS-2712 adversarial-review fix: require_matching_source)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_for_mismatched_source_rejected_403(client, monkeypatch):
    monkeypatch.setattr(router_mod, "enqueue_batch", lambda **kwargs: "stream-x")
    mismatched_ctx = IngestAuthContext(
        org_id="test-org",
        scopes=frozenset({"ingest:write"}),
        source=IngestSource(
            org_id="test-org",
            system="github",
            instance="other/repo",  # envelope declares acme/api -- CHAOS-2712 default
            mode=IngestSourceMode.CUSTOMER_PUSH.value,
            enabled=True,
        ),
    )
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: mismatched_ctx
    try:
        envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
        resp = await client.post(f"{BASE}/batches", json=envelope)
    finally:
        app.dependency_overrides[router_mod._require_ingest_write] = lambda: TEST_CTX

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "source_mismatch"


@pytest.mark.asyncio
async def test_batch_for_unbound_write_token_rejected_403(client):
    unbound_ctx = IngestAuthContext(
        org_id="test-org", scopes=frozenset({"ingest:write"}), source=None
    )
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: unbound_ctx
    try:
        envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
        resp = await client.post(f"{BASE}/batches", json=envelope)
    finally:
        app.dependency_overrides[router_mod._require_ingest_write] = lambda: TEST_CTX

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "source_mismatch"


@pytest.mark.asyncio
async def test_batch_for_disabled_source_rejected_403(client):
    disabled_ctx = IngestAuthContext(
        org_id="test-org",
        scopes=frozenset({"ingest:write"}),
        source=IngestSource(
            org_id="test-org",
            system="github",
            instance="acme/api",
            mode=IngestSourceMode.CUSTOMER_PUSH.value,
            enabled=False,
        ),
    )
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: disabled_ctx
    try:
        envelope = _envelope([_record("commit.v1", "c1", VALID_PAYLOADS["commit.v1"])])
        resp = await client.post(f"{BASE}/batches", json=envelope)
    finally:
        app.dependency_overrides[router_mod._require_ingest_write] = lambda: TEST_CTX

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "source_disabled"


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
