"""Verify analytics endpoints require authentication (P0 security fix)."""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-analytics-auth")
os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

from fastapi.testclient import TestClient

from dev_health_ops.api.main import app

client = TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/api/v1/home"),
        ("POST", "/api/v1/home"),
        ("GET", "/api/v1/explain?metric=cycle_time"),
        ("GET", "/api/v1/heatmap?type=team&metric=commits"),
        ("GET", "/api/v1/work-units"),
        ("GET", "/api/v1/flame?entity_type=repo&entity_id=test"),
        ("GET", "/api/v1/quadrant?type=churn_throughput"),
        ("GET", "/api/v1/drilldown/prs"),
        ("GET", "/api/v1/drilldown/issues"),
        ("GET", "/api/v1/people"),
        ("GET", "/api/v1/opportunities"),
        ("GET", "/api/v1/investment"),
        ("GET", "/api/v1/sankey"),
        ("GET", "/api/v1/filters/options"),
    ],
)
def test_analytics_requires_auth(method: str, path: str):
    """Every analytics endpoint must return 401 without a Bearer token."""
    resp = getattr(client, method.lower())(path)
    assert resp.status_code == 401, (
        f"{method} {path} should require auth, got {resp.status_code}"
    )


@pytest.mark.parametrize("path", ["/health", "/api/v1/meta"])
def test_public_endpoints_do_not_require_auth(path: str):
    """Health and meta endpoints must remain publicly accessible."""
    resp = client.get(path)
    assert resp.status_code != 401, f"{path} should NOT require auth"
