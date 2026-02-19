from __future__ import annotations

from fastapi.testclient import TestClient

from dev_health_ops.api import main


def test_health_endpoint_returns_ok_when_required_services_ok(monkeypatch):
    async def _pg_ok():
        return "postgres", "ok"

    async def _ch_ok():
        return "clickhouse", "ok"

    monkeypatch.setattr(main, "_check_postgres_health", _pg_ok)
    monkeypatch.setattr(main, "_check_clickhouse_health", _ch_ok)
    monkeypatch.setattr(main.HOME_CACHE, "status", lambda: "ok")

    with TestClient(main.app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "services": {"postgres": "ok", "clickhouse": "ok", "redis": "ok"},
    }


def test_health_endpoint_returns_503_when_required_service_is_down(monkeypatch):
    async def _pg_down():
        return "postgres", "down"

    async def _ch_ok():
        return "clickhouse", "ok"

    monkeypatch.setattr(main, "_check_postgres_health", _pg_down)
    monkeypatch.setattr(main, "_check_clickhouse_health", _ch_ok)
    monkeypatch.setattr(main.HOME_CACHE, "status", lambda: "ok")

    with TestClient(main.app) as client:
        response = client.get("/health")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "down"
    assert body["services"]["postgres"] == "down"
    assert body["services"]["clickhouse"] == "ok"
    assert body["services"]["redis"] == "ok"


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
    assert body["stream"] == "ingest:default:commits"
    assert body["ingestion_id"]
