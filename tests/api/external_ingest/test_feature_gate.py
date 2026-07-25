from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient
from sqlalchemy.exc import SQLAlchemyError

from dev_health_ops.api.external_ingest.auth import IngestAuthContext
from dev_health_ops.api.external_ingest.schemas import (
    OPERATIONAL_RECORD_KINDS,
    RECORD_KIND_MODELS,
    SCHEMA_VERSION,
)
from dev_health_ops.api.main import app
from dev_health_ops.external_ingest import feature_gate as feature_gate_mod
from dev_health_ops.models.ingest_auth import IngestSource, IngestSourceMode
from tests.api.test_external_ingest_router import (
    BASE,
    VALID_PAYLOADS,
    _envelope,
    _record,
    router_mod,
)

pytest_plugins = ("tests.api.test_external_ingest_router",)


def _operational_incident() -> dict[str, object]:
    return _record(
        "operational_incident.v1",
        "incident-1",
        {
            "externalId": "incident-1",
            "sourceVersionAt": "2026-07-17T00:00:00Z",
            "title": "Database unavailable",
        },
    )


def _operational_envelope(*, mixed: bool = False) -> dict[str, object]:
    records = [_operational_incident()]
    if mixed:
        records.append(_record("commit.v1", "abc123", VALID_PAYLOADS["commit.v1"]))
    envelope = _envelope(records)
    envelope["source"] = {
        "type": "customer_push",
        "system": "github",
        "instance": "acme/api",
        "entityFamily": "operational",
    }
    return envelope


async def _use_operational_source(session_maker) -> None:
    source = IngestSource(
        org_id="test-org",
        system="github",
        instance="acme/api",
        entity_family="operational",
        mode=IngestSourceMode.CUSTOMER_PUSH.value,
        enabled=True,
    )
    async with session_maker() as session:
        session.add(source)
        await session.commit()
    app.dependency_overrides[router_mod._require_ingest_write] = lambda: (
        IngestAuthContext(
            org_id="test-org",
            scopes=frozenset({"ingest:write"}),
            source=source,
        )
    )


@pytest.mark.asyncio
async def test_authenticated_availability_hides_operational_kinds_when_disabled(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    availability = AsyncMock(
        return_value=feature_gate_mod.ExternalIngestFeatureAvailability(
            customer_push_ingest=True,
            canonical_incident_ingestion=False,
        )
    )
    monkeypatch.setattr(
        router_mod, "_external_ingest_feature_availability", availability
    )

    response = await client.get(f"{BASE}/availability")

    assert response.status_code == 200
    assert response.json() == {
        "schemaVersion": SCHEMA_VERSION,
        "features": {
            "customerPushIngest": True,
            "canonicalIncidentIngestion": False,
        },
        "availableRecordKinds": sorted(
            set(RECORD_KIND_MODELS) - OPERATIONAL_RECORD_KINDS
        ),
        "unavailableRecordKinds": sorted(OPERATIONAL_RECORD_KINDS),
    }
    availability.assert_awaited_once()


@pytest.mark.asyncio
async def test_authenticated_availability_reports_all_kinds_unavailable_when_customer_push_is_disabled(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        router_mod,
        "_external_ingest_feature_availability",
        AsyncMock(
            return_value=feature_gate_mod.ExternalIngestFeatureAvailability(
                customer_push_ingest=False,
                canonical_incident_ingestion=True,
            )
        ),
    )

    response = await client.get(f"{BASE}/availability")

    assert response.status_code == 200
    assert response.json() == {
        "schemaVersion": SCHEMA_VERSION,
        "features": {
            "customerPushIngest": False,
            "canonicalIncidentIngestion": True,
        },
        "availableRecordKinds": [],
        "unavailableRecordKinds": sorted(RECORD_KIND_MODELS),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(("enabled", "expected_status"), ((False, 403), (True, 200)))
async def test_operational_validation_requires_canonical_incident_feature(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    enabled: bool,
    expected_status: int,
) -> None:
    monkeypatch.setattr(
        router_mod,
        "_canonical_incident_ingestion_allowed",
        AsyncMock(return_value=enabled),
    )

    response = await client.post(f"{BASE}/validate", json=_operational_envelope())

    assert response.status_code == expected_status
    if not enabled:
        assert response.json()["error"]["code"] == "feature_not_enabled"


@pytest.mark.asyncio
async def test_operational_acceptance_denial_precedes_any_postgres_write(
    client: AsyncClient,
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _use_operational_source(session_maker)
    idempotency_write = AsyncMock()
    enqueue = AsyncMock()
    monkeypatch.setattr(
        router_mod,
        "_canonical_incident_ingestion_allowed",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(router_mod, "resolve_batch_idempotency", idempotency_write)
    monkeypatch.setattr(router_mod, "enqueue_batch", enqueue)

    response = await client.post(f"{BASE}/batches", json=_operational_envelope())

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "feature_not_enabled"
    idempotency_write.assert_not_awaited()
    enqueue.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("enabled", "expected_status"), ((False, 403), (True, 400)))
async def test_mixed_operational_and_legacy_batch_is_never_partially_accepted(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    enabled: bool,
    expected_status: int,
) -> None:
    monkeypatch.setattr(
        router_mod,
        "_canonical_incident_ingestion_allowed",
        AsyncMock(return_value=enabled),
    )
    enqueue = AsyncMock()
    monkeypatch.setattr(router_mod, "enqueue_batch", enqueue)

    response = await client.post(
        f"{BASE}/batches", json=_operational_envelope(mixed=True)
    )

    assert response.status_code == expected_status
    enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_operational_validation_is_isolated_by_organization(
    client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def allowed_for_org(_session, org_id: str) -> bool:
        return org_id == "enabled-org"

    monkeypatch.setattr(
        router_mod, "_canonical_incident_ingestion_allowed", allowed_for_org
    )

    disabled = await client.post(f"{BASE}/validate", json=_operational_envelope())
    app.dependency_overrides[router_mod._require_schema_read] = lambda: (
        IngestAuthContext(
            org_id="enabled-org",
            scopes=frozenset({"schema:read"}),
        )
    )
    enabled = await client.post(f"{BASE}/validate", json=_operational_envelope())

    assert disabled.status_code == 403
    assert enabled.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_error",
    (SQLAlchemyError(), RuntimeError("database session unavailable")),
)
async def test_canonical_incident_evaluator_storage_error_fails_closed(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    storage_error: Exception,
) -> None:
    monkeypatch.setattr(
        feature_gate_mod,
        "evaluate_org_features_async",
        AsyncMock(side_effect=storage_error),
    )
    async with session_maker() as session:
        allowed = await router_mod._canonical_incident_ingestion_allowed(
            session, "00000000-0000-0000-0000-000000003024"
        )

    assert allowed is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("customer_push", "canonical_incidents", "expected"),
    (
        (False, False, False),
        (True, False, False),
        (False, True, False),
        (True, True, True),
    ),
)
async def test_operational_worker_decision_requires_both_features(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    customer_push: bool,
    canonical_incidents: bool,
    expected: bool,
) -> None:
    decisions = {
        feature_gate_mod.CUSTOMER_PUSH_INGEST_FEATURE: type(
            "Decision", (), {"allowed": customer_push}
        )(),
        feature_gate_mod.CANONICAL_INCIDENT_INGESTION_FEATURE: type(
            "Decision", (), {"allowed": canonical_incidents}
        )(),
    }
    monkeypatch.setattr(
        feature_gate_mod,
        "evaluate_org_features_async",
        AsyncMock(return_value=decisions),
    )
    async with session_maker() as session:
        allowed = await feature_gate_mod.external_operational_ingestion_allowed(
            session, "00000000-0000-0000-0000-000000003024"
        )

    assert allowed is expected
