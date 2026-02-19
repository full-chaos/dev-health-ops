from datetime import date

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.main import app
from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.models.schemas import (
    QuadrantAxes,
    QuadrantAxis,
    QuadrantPoint,
    QuadrantResponse,
)

_FAKE_USER = AuthenticatedUser(
    user_id="test-user", email="test@example.com",
    org_id="test-org", role="admin",
)


def _validate(model, payload):
    if hasattr(model, "model_validate"):
        return model.model_validate(payload)
    return model.parse_obj(payload)


@pytest.fixture
def client():
    app.dependency_overrides[get_current_user] = lambda: _FAKE_USER
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_quadrant_endpoint_schema(client, monkeypatch):
    sample = QuadrantResponse(
        axes=QuadrantAxes(
            x=QuadrantAxis(metric="churn", label="Churn", unit="loc"),
            y=QuadrantAxis(metric="throughput", label="Throughput", unit="items"),
        ),
        points=[
            QuadrantPoint(
                entity_id="team-a",
                entity_label="Team A",
                x=42.0,
                y=12.0,
                window_start=date(2024, 1, 1),
                window_end=date(2024, 1, 8),
                evidence_link="/api/v1/explain?metric=throughput",
                trajectory=None,
            )
        ],
        annotations=[],
    )

    async def _fake_quadrant(**_):
        return sample

    monkeypatch.setattr("dev_health_ops.api.main.build_quadrant_response", _fake_quadrant)

    response = client.get("/api/v1/quadrant", params={"type": "churn_throughput"})
    assert response.status_code == 200
    _validate(QuadrantResponse, response.json())
