from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.api.internal.worker_sync import _units_belong_to_run
from dev_health_ops.api.main import app
from dev_health_ops.models import Base
from tests._helpers import seed_sync_dispatch_transport_routes
from tests.test_sync_units import _seed_run

_REFERENCE = {
    "organization_id": "00000000-0000-4000-8000-000000000010",
    "sync_run_id": "00000000-0000-4000-8000-000000000011",
    "outbox_id": "00000000-0000-4000-8000-000000000012",
    "route_generation": 3,
}


def test_sync_bridge_requires_token(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    response = TestClient(app).post(
        "/api/internal/worker-sync/dispatch", json=_REFERENCE
    )
    assert response.status_code == 401


def test_sync_bridge_stale_delivery_is_acknowledged_without_coordinator_call(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with (
        patch(
            "dev_health_ops.api.internal.worker_sync._current_river_reference",
            return_value=False,
        ),
        patch("dev_health_ops.api.internal.worker_sync.dispatch_sync_run.run") as run,
    ):
        response = TestClient(app).post(
            "/api/internal/worker-sync/dispatch",
            headers={"Authorization": "Bearer test-token"},
            json=_REFERENCE,
        )
    assert response.status_code == 200
    assert response.json() == {"status": "stale"}
    run.assert_not_called()


def test_sync_bridge_duplicate_finalize_uses_durable_finalization_ledger(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with (
        patch(
            "dev_health_ops.api.internal.worker_sync._current_river_reference",
            return_value=True,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.finalize_sync_run.run",
            side_effect=[{"status": "finalized"}, {"status": "already_dispatched"}],
        ) as run,
    ):
        client = TestClient(app)
        first = client.post(
            "/api/internal/worker-sync/finalize",
            headers={"Authorization": "Bearer test-token"},
            json=_REFERENCE,
        )
        duplicate = client.post(
            "/api/internal/worker-sync/finalize",
            headers={"Authorization": "Bearer test-token"},
            json=_REFERENCE,
        )
    assert first.json() == {"status": "finalized"}
    assert duplicate.json() == {"status": "already_dispatched"}
    assert run.call_count == 2


def test_sync_bridge_retries_failure_after_effect_without_publishing_celery_task(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    observed: list[str] = []

    def after_effect_then_retryable(*_args: object) -> dict[str, str]:
        observed.append("effect")
        if len(observed) == 1:
            raise RuntimeError("response lost after durable effect")
        return {"status": "pending"}

    with (
        patch(
            "dev_health_ops.api.internal.worker_sync._current_river_reference",
            return_value=True,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.finalize_sync_run.run",
            side_effect=after_effect_then_retryable,
        ),
    ):
        client = TestClient(app, raise_server_exceptions=False)
        first = client.post(
            "/api/internal/worker-sync/finalize",
            headers={"Authorization": "Bearer test-token"},
            json=_REFERENCE,
        )
        retry = client.post(
            "/api/internal/worker-sync/finalize",
            headers={"Authorization": "Bearer test-token"},
            json=_REFERENCE,
        )
    assert first.status_code == 500
    assert retry.status_code == 200
    assert retry.json() == {"status": "pending"}
    assert observed == ["effect", "effect"]


def test_team_autoimport_bridge_rejects_cross_org_run(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    reference = {
        "organization_id": _REFERENCE["organization_id"],
        "sync_run_id": _REFERENCE["sync_run_id"],
    }
    with (
        patch(
            "dev_health_ops.api.internal.worker_sync._current_sync_run_reference",
            return_value=False,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.run_post_sync_team_autoimport.run"
        ) as run,
    ):
        response = TestClient(app).post(
            "/api/internal/worker-sync/team-autoimport",
            headers={"Authorization": "Bearer test-token"},
            json=reference,
        )
    assert response.status_code == 200
    assert response.json() == {"status": "stale"}
    run.assert_not_called()


# CHAOS-4175: the narrow, identifiers-only bridge call the native Go
# reference-discovery gate uses for the one step credential resolution
# stays entirely Python-side (ruling, 2026-08-24 -- "credentials must stay
# entirely Python-side... a security property, not just a shape
# preference"). These tests pin both the request contract and the call
# behavior.


def test_reference_discovery_populate_request_is_identifiers_only() -> None:
    """Pins the request payload shape so nobody later "optimizes" secret
    material into it. TeamAutoImportReference (the model this endpoint
    reuses) must declare EXACTLY organization_id and sync_run_id, and
    forbid any extra field -- a client-supplied "credentials" key must be
    rejected at the schema layer, not silently accepted and ignored.
    """
    from dev_health_ops.api.internal.worker_sync import TeamAutoImportReference

    schema = TeamAutoImportReference.model_json_schema()
    assert set(schema["properties"]) == {"organization_id", "sync_run_id"}
    assert TeamAutoImportReference.model_config.get("extra") == "forbid"


def test_reference_discovery_populate_bridge_rejects_a_credentials_field(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    reference = {
        "organization_id": _REFERENCE["organization_id"],
        "sync_run_id": _REFERENCE["sync_run_id"],
        "credentials": {"token": "should-never-be-accepted-here"},
    }
    with patch(
        "dev_health_ops.api.internal.worker_sync.run_reference_discovery_populate_for_sync_run"
    ) as populate:
        response = TestClient(app).post(
            "/api/internal/worker-sync/reference-discovery-populate",
            headers={"Authorization": "Bearer test-token"},
            json=reference,
        )
    assert response.status_code == 422
    populate.assert_not_called()


def test_reference_discovery_populate_bridge_rejects_cross_org_run(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    reference = {
        "organization_id": _REFERENCE["organization_id"],
        "sync_run_id": _REFERENCE["sync_run_id"],
    }
    with (
        patch(
            "dev_health_ops.api.internal.worker_sync._current_sync_run_reference",
            return_value=False,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.run_reference_discovery_populate_for_sync_run"
        ) as populate,
    ):
        response = TestClient(app).post(
            "/api/internal/worker-sync/reference-discovery-populate",
            headers={"Authorization": "Bearer test-token"},
            json=reference,
        )
    assert response.status_code == 409
    populate.assert_not_called()


def test_reference_discovery_populate_bridge_calls_the_strict_entrypoint_with_the_run_id_only(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    reference = {
        "organization_id": _REFERENCE["organization_id"],
        "sync_run_id": _REFERENCE["sync_run_id"],
    }
    summary = {
        "status": "success",
        "provider": "linear",
        "org_id": _REFERENCE["organization_id"],
        "reference_team_keys": ["ENG"],
        "reference_sprint_ids": ["sprint-1"],
    }
    with (
        patch(
            "dev_health_ops.api.internal.worker_sync._current_sync_run_reference",
            return_value=True,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.run_reference_discovery_populate_for_sync_run",
            return_value=summary,
        ) as populate,
    ):
        response = TestClient(app).post(
            "/api/internal/worker-sync/reference-discovery-populate",
            headers={"Authorization": "Bearer test-token"},
            json=reference,
        )
    assert response.status_code == 200
    assert response.json() == summary
    populate.assert_called_once_with(_REFERENCE["sync_run_id"])


# --- /dispatch-budget-estimate (CHAOS-4175 family 3, BudgetGuard estimate
# bridge) -- both directions of this seam are contract-pinned per the
# ruling: request is identifiers only, response is the closed BudgetEstimate
# schema, extra="forbid" on this side, strict decode on the Go side.


_BUDGET_ESTIMATE_UNIT_ID = "00000000-0000-4000-8000-000000000020"

_BUDGET_ESTIMATE_REFERENCE = {
    "organization_id": _REFERENCE["organization_id"],
    "sync_run_id": _REFERENCE["sync_run_id"],
    "unit_ids": [_BUDGET_ESTIMATE_UNIT_ID],
}


def test_dispatch_budget_estimate_request_is_identifiers_only() -> None:
    """Pins the request payload shape: EXACTLY organization_id, sync_run_id,
    unit_ids -- no credential, provider, or estimate field can be smuggled
    in by a client, and no extra field is silently accepted.
    """
    from dev_health_ops.api.internal.worker_sync import DispatchBudgetEstimateReference

    schema = DispatchBudgetEstimateReference.model_json_schema()
    assert set(schema["properties"]) == {"organization_id", "sync_run_id", "unit_ids"}
    assert DispatchBudgetEstimateReference.model_config.get("extra") == "forbid"


def test_dispatch_budget_estimate_request_unit_ids_cap_matches_go() -> None:
    """Pins the request's own size ceiling (CHAOS-4175 codex round 2): Go's
    BudgetGuard port chunks its estimate-bridge calls at
    dispatchBudgetEstimateMaxUnitIDs (budget_estimate_bridge.go), a literal
    Go constant that has no way to introspect this Pydantic model at
    runtime. Python is the source of truth here -- this test is what makes
    a change to unit_ids' max_length a REVIEWED, visible break instead of a
    silent drift the Go constant would only ever discover via a live 422.
    """
    from dev_health_ops.api.internal.worker_sync import DispatchBudgetEstimateReference

    schema = DispatchBudgetEstimateReference.model_json_schema()
    assert schema["properties"]["unit_ids"]["maxItems"] == 500


def test_dispatch_budget_estimate_response_is_the_closed_estimate_schema() -> None:
    """Pins the response payload shape on the OTHER direction: only the
    fields BudgetEstimate/BudgetBucketKey actually carry, nothing else --
    a future field added to the dataclasses without updating this schema
    would need an explicit, reviewed change here too, not an implicit
    leak through the bridge.
    """
    from dev_health_ops.api.internal.worker_sync import (
        BudgetEstimateBucketPayload,
        BudgetEstimatePayload,
        DispatchBudgetEstimateResponse,
    )

    assert set(BudgetEstimateBucketPayload.model_json_schema()["properties"]) == {
        "provider",
        "org_id",
        "host",
        "credential_fingerprint",
        "dimension",
    }
    assert set(BudgetEstimatePayload.model_json_schema()["properties"]) == {
        "bucket",
        "estimated_units",
        "confidence",
        "route_family",
        "notes",
    }
    assert set(DispatchBudgetEstimateResponse.model_json_schema()["properties"]) == {
        "estimates"
    }
    for model in (
        BudgetEstimateBucketPayload,
        BudgetEstimatePayload,
        DispatchBudgetEstimateResponse,
    ):
        assert model.model_config.get("extra") == "forbid"


def test_dispatch_budget_estimate_bridge_rejects_a_credentials_field(
    monkeypatch,
) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    reference = {
        **_BUDGET_ESTIMATE_REFERENCE,
        "credentials": {"token": "should-never-be-accepted-here"},
    }
    with patch(
        "dev_health_ops.api.internal.worker_sync.batch_estimate_provider_budget_for_units"
    ) as estimate:
        response = TestClient(app).post(
            "/api/internal/worker-sync/dispatch-budget-estimate",
            headers={"Authorization": "Bearer test-token"},
            json=reference,
        )
    assert response.status_code == 422
    estimate.assert_not_called()


def test_dispatch_budget_estimate_bridge_rejects_cross_org_run(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with (
        patch(
            "dev_health_ops.api.internal.worker_sync._current_sync_run_reference",
            return_value=False,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.batch_estimate_provider_budget_for_units"
        ) as estimate,
    ):
        response = TestClient(app).post(
            "/api/internal/worker-sync/dispatch-budget-estimate",
            headers={"Authorization": "Bearer test-token"},
            json=_BUDGET_ESTIMATE_REFERENCE,
        )
    assert response.status_code == 409
    estimate.assert_not_called()


def test_dispatch_budget_estimate_bridge_rejects_units_outside_the_run(
    monkeypatch,
) -> None:
    """A unit id that does not belong to sync_run_id must reject the WHOLE
    batch (409) before any credential is decrypted -- never silently drop
    the mismatched id and estimate the rest.
    """
    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    with (
        patch(
            "dev_health_ops.api.internal.worker_sync._current_sync_run_reference",
            return_value=True,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.get_postgres_session_sync"
        ) as get_session,
        patch(
            "dev_health_ops.api.internal.worker_sync._units_belong_to_run",
            return_value=False,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.batch_estimate_provider_budget_for_units"
        ) as estimate,
    ):
        get_session.return_value.__enter__.return_value = MagicMock()
        response = TestClient(app).post(
            "/api/internal/worker-sync/dispatch-budget-estimate",
            headers={"Authorization": "Bearer test-token"},
            json=_BUDGET_ESTIMATE_REFERENCE,
        )
    assert response.status_code == 409
    estimate.assert_not_called()


def test_dispatch_budget_estimate_bridge_returns_the_batch_estimator_result(
    monkeypatch,
) -> None:
    from dev_health_ops.sync.budget_types import (
        BudgetBucketKey,
        BudgetDimension,
        BudgetEstimate,
    )

    monkeypatch.setenv("WORKER_OPERATIONAL_BRIDGE_TOKEN", "test-token")
    bucket = BudgetBucketKey(
        provider="github",
        org_id=str(_REFERENCE["organization_id"]),
        host="api.github.com",
        credential_fingerprint="fp-1",
        dimension=BudgetDimension.REST_CORE,
    )
    estimates_by_unit = {
        _BUDGET_ESTIMATE_UNIT_ID: (
            BudgetEstimate(
                bucket=bucket,
                estimated_units=42,
                confidence="high",
                route_family="work-items",
                notes=("first pass",),
            ),
        )
    }
    with (
        patch(
            "dev_health_ops.api.internal.worker_sync._current_sync_run_reference",
            return_value=True,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.get_postgres_session_sync"
        ) as get_session,
        patch(
            "dev_health_ops.api.internal.worker_sync._units_belong_to_run",
            return_value=True,
        ),
        patch(
            "dev_health_ops.api.internal.worker_sync.batch_estimate_provider_budget_for_units",
            return_value=estimates_by_unit,
        ) as estimate,
    ):
        get_session.return_value.__enter__.return_value = MagicMock()
        response = TestClient(app).post(
            "/api/internal/worker-sync/dispatch-budget-estimate",
            headers={"Authorization": "Bearer test-token"},
            json=_BUDGET_ESTIMATE_REFERENCE,
        )
    assert response.status_code == 200
    assert response.json() == {
        "estimates": {
            _BUDGET_ESTIMATE_UNIT_ID: [
                {
                    "bucket": {
                        "provider": "github",
                        "org_id": _REFERENCE["organization_id"],
                        "host": "api.github.com",
                        "credential_fingerprint": "fp-1",
                        "dimension": "rest_core",
                    },
                    "estimated_units": 42,
                    "confidence": "high",
                    "route_family": "work-items",
                    "notes": ["first pass"],
                }
            ]
        }
    }
    estimate.assert_called_once()


def _units_belong_to_run_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    seed_sync_dispatch_transport_routes(session)
    return session


def test_units_belong_to_run_is_true_when_every_unit_belongs() -> None:
    session = _units_belong_to_run_session()
    run, unit = _seed_run(session)

    assert (
        _units_belong_to_run(session, run.id, uuid.UUID(run.org_id), [unit.id]) is True
    )


def test_units_belong_to_run_is_false_for_a_unit_from_a_different_run() -> None:
    """The mismatched-batch case: a unit id that genuinely belongs to a
    DIFFERENT sync run (and org) must be rejected, not silently treated as
    a match -- this is the real query exercised directly, not the endpoint
    mocking the function away.
    """
    session = _units_belong_to_run_session()
    run_a, unit_a = _seed_run(session)
    run_b, unit_b = _seed_run(session)
    assert run_a.org_id != run_b.org_id  # _seed_run mints a fresh org_id each call

    assert (
        _units_belong_to_run(
            session, run_a.id, uuid.UUID(run_a.org_id), [unit_a.id, unit_b.id]
        )
        is False
    )
    # And the reverse must also reject -- the check is not order-dependent.
    assert (
        _units_belong_to_run(
            session, run_b.id, uuid.UUID(run_b.org_id), [unit_b.id, unit_a.id]
        )
        is False
    )


def test_units_belong_to_run_is_false_for_a_unit_id_that_does_not_exist() -> None:
    session = _units_belong_to_run_session()
    run, unit = _seed_run(session)
    missing_unit_id = uuid.uuid4()

    assert (
        _units_belong_to_run(
            session, run.id, uuid.UUID(run.org_id), [unit.id, missing_unit_id]
        )
        is False
    )
