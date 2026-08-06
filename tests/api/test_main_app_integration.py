from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api import main


def test_lifespan_aborts_startup_when_schema_status_is_unsatisfied(monkeypatch):
    """Companion proof for ``tests/api/conftest.py``'s autouse stub.

    That fixture stubs ``application_schema_status`` satisfied for every
    test in this package so CI's real-but-deliberately-unmigrated Postgres
    service doesn't fail tests unrelated to schema-revision checking (see
    the conftest docstring). This test overrides the stub back to
    unsatisfied and confirms ``lifespan()`` still aborts startup through
    the real ``with TestClient(main.app)`` seam -- proving the fixture
    narrows what's exercised without disabling the guard itself. The
    live-PostgreSQL-gated ancestor-walk proof lives in
    ``tests/test_ask_dev_v2_persistence_startup_gate.py``; this is the fast,
    CI-default companion at the mocked level.
    """

    async def _unsatisfied(dsn: str | None = None) -> tuple[bool, tuple[str, ...]]:
        return False, ("0071",)

    monkeypatch.setattr(
        "dev_health_ops.migrate.application_schema_status", _unsatisfied
    )
    # A real (but unreachable) DSN so _lifespan.py's `if postgres_uri:` gate
    # is entered -- the FeatureBundle validation step that runs first
    # tolerates the resulting connection failure as "DB not ready" and
    # continues on to the schema-revision check, which is stubbed above
    # and needs no real connection of its own.
    monkeypatch.setenv(
        "POSTGRES_URI",
        "postgresql+asyncpg://unreachable:unreachable@localhost:1/unreachable",
    )

    with pytest.raises(RuntimeError, match="application schema"):
        with TestClient(main.app):
            pytest.fail("lifespan must abort before yielding")


def test_health_endpoint_returns_ok_when_required_services_ok(monkeypatch):
    async def _pg_ok():
        return "postgres", "ok"

    async def _ch_ok():
        return "clickhouse", "ok"

    async def _redis_ok():
        return "redis", "ok"

    monkeypatch.setattr(main, "_check_postgres_health", _pg_ok)
    monkeypatch.setattr(main, "_check_clickhouse_health", _ch_ok)
    monkeypatch.setattr(main, "_check_redis_health", _redis_ok)

    with TestClient(main.app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["services"]["postgres"] == "ok"
    assert body["services"]["clickhouse"] == "ok"
    assert body["services"]["redis"] == "ok"


def test_health_endpoint_returns_503_when_required_service_is_down(monkeypatch):
    async def _pg_down():
        return "postgres", "down"

    async def _ch_ok():
        return "clickhouse", "ok"

    async def _redis_ok():
        return "redis", "ok"

    monkeypatch.setattr(main, "_check_postgres_health", _pg_down)
    monkeypatch.setattr(main, "_check_clickhouse_health", _ch_ok)
    monkeypatch.setattr(main, "_check_redis_health", _redis_ok)

    with TestClient(main.app) as client:
        response = client.get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "down"
    assert body["services"]["postgres"] == "down"
    assert body["services"]["clickhouse"] == "ok"
    assert body["services"]["redis"] == "ok"


def test_ready_endpoint_returns_ok_when_required_service_is_down(monkeypatch):
    async def _pg_down():
        return "postgres", "down"

    monkeypatch.setattr(main, "_check_postgres_health", _pg_down)

    with TestClient(main.app) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_health_workers_returns_celery_status(monkeypatch):
    async def _celery_ok():
        return "celery", "ok"

    monkeypatch.setattr(main, "_check_celery_health", _celery_ok)

    with TestClient(main.app) as client:
        response = client.get("/health/workers")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["services"]["celery"] == "ok"


def test_health_workers_returns_503_when_celery_down(monkeypatch):
    async def _celery_down():
        return "celery", "down"

    monkeypatch.setattr(main, "_check_celery_health", _celery_down)

    with TestClient(main.app) as client:
        response = client.get("/health/workers")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "down"
    assert body["services"]["celery"] == "down"


def test_readiness_route_reports_webhook_health_shape(monkeypatch):
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "gh-secret")
    monkeypatch.setenv("GITLAB_WEBHOOK_TOKEN", "gl-token")
    monkeypatch.setenv("JIRA_WEBHOOK_SECRET", "jira-secret")

    with TestClient(main.app) as client:
        response = client.get("/api/v1/webhooks/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["secrets_configured"] == {
        "github": True,
        "gitlab": True,
        "jira": True,
    }
    assert isinstance(body["celery_available"], bool)


def test_ingest_commits_route_is_wired_through_main_app(monkeypatch):
    monkeypatch.delenv("INGEST_API_KEYS", raising=False)
    monkeypatch.delenv("INGEST_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)

    with TestClient(main.app) as client:
        response = client.post(
            "/api/v1/ingest/commits",
            json={
                "org_id": "test-org",
                "repo_url": "https://github.com/org/repo",
                "items": [
                    {
                        "hash": "abc123",
                        "message": "fix: deterministic integration test",
                        "author_name": "Dev",
                        "author_email": "dev@example.com",
                        "author_when": "2025-01-15T10:00:00Z",
                    }
                ],
            },
        )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["items_received"] == 1
    assert body["stream"] == "ingest:test-org:commits"
    assert body["ingestion_id"]
