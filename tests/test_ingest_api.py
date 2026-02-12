from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from dev_health_ops.api.main import app


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


VALID_COMMIT = {
    "hash": "abc123",
    "message": "fix: resolve login bug",
    "author_name": "Alice",
    "author_email": "alice@example.com",
    "author_when": "2025-01-15T10:00:00Z",
}

VALID_PR = {
    "number": 42,
    "title": "Add login feature",
    "state": "merged",
    "author_name": "Bob",
    "created_at": "2025-01-10T08:00:00Z",
    "merged_at": "2025-01-12T14:00:00Z",
}

VALID_WORK_ITEM = {
    "work_item_id": "jira:PROJ-123",
    "provider": "jira",
    "title": "Implement SSO",
    "type": "story",
    "status": "in_progress",
    "created_at": "2025-01-01T00:00:00Z",
}

VALID_DEPLOYMENT = {
    "deployment_id": "deploy-001",
    "status": "success",
    "environment": "production",
    "deployed_at": "2025-01-15T12:00:00Z",
}

VALID_INCIDENT = {
    "incident_id": "inc-001",
    "status": "resolved",
    "started_at": "2025-01-14T03:00:00Z",
    "resolved_at": "2025-01-14T05:30:00Z",
}


@pytest.mark.asyncio
async def test_ingest_commits_happy_path(client):
    resp = await client.post(
        "/api/v1/ingest/commits",
        json={"repo_url": "https://github.com/org/repo", "items": [VALID_COMMIT]},
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["items_received"] == 1
    assert body["stream"] == "ingest:default:commits"
    assert "ingestion_id" in body


@pytest.mark.asyncio
async def test_ingest_commits_empty_items(client):
    resp = await client.post(
        "/api/v1/ingest/commits",
        json={"repo_url": "https://github.com/org/repo", "items": []},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_commits_missing_required_field(client):
    bad_commit = {
        "message": "no hash",
        "author_name": "X",
        "author_email": "x@x.com",
        "author_when": "2025-01-01T00:00:00Z",
    }
    resp = await client.post(
        "/api/v1/ingest/commits",
        json={"repo_url": "https://github.com/org/repo", "items": [bad_commit]},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_pull_requests_happy_path(client):
    resp = await client.post(
        "/api/v1/ingest/pull-requests",
        json={"repo_url": "https://github.com/org/repo", "items": [VALID_PR]},
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["items_received"] == 1
    assert body["stream"] == "ingest:default:pull-requests"


@pytest.mark.asyncio
async def test_ingest_pull_requests_with_reviews(client):
    pr_with_reviews = {
        **VALID_PR,
        "reviews": [
            {
                "review_id": "r1",
                "reviewer": "carol",
                "state": "APPROVED",
                "submitted_at": "2025-01-11T10:00:00Z",
            }
        ],
    }
    resp = await client.post(
        "/api/v1/ingest/pull-requests",
        json={"repo_url": "https://github.com/org/repo", "items": [pr_with_reviews]},
    )
    assert resp.status_code == 202
    assert resp.json()["items_received"] == 1


@pytest.mark.asyncio
async def test_ingest_work_items_happy_path(client):
    resp = await client.post(
        "/api/v1/ingest/work-items",
        json={"items": [VALID_WORK_ITEM]},
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["items_received"] == 1
    assert body["stream"] == "ingest:default:work-items"


@pytest.mark.asyncio
async def test_ingest_work_items_no_repo_url_needed(client):
    resp = await client.post(
        "/api/v1/ingest/work-items",
        json={"org_id": "acme", "items": [VALID_WORK_ITEM]},
    )
    assert resp.status_code == 202
    assert resp.json()["stream"] == "ingest:acme:work-items"


@pytest.mark.asyncio
async def test_ingest_deployments_happy_path(client):
    resp = await client.post(
        "/api/v1/ingest/deployments",
        json={"repo_url": "https://github.com/org/repo", "items": [VALID_DEPLOYMENT]},
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["items_received"] == 1
    assert body["stream"] == "ingest:default:deployments"


@pytest.mark.asyncio
async def test_ingest_incidents_happy_path(client):
    resp = await client.post(
        "/api/v1/ingest/incidents",
        json={"repo_url": "https://github.com/org/repo", "items": [VALID_INCIDENT]},
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["items_received"] == 1
    assert body["stream"] == "ingest:default:incidents"


@pytest.mark.asyncio
async def test_ingest_custom_org_id(client):
    resp = await client.post(
        "/api/v1/ingest/commits",
        json={
            "org_id": "acme-corp",
            "repo_url": "https://github.com/acme/app",
            "items": [VALID_COMMIT],
        },
    )
    assert resp.status_code == 202
    assert resp.json()["stream"] == "ingest:acme-corp:commits"


@pytest.mark.asyncio
async def test_ingest_multiple_items(client):
    commits = [
        {**VALID_COMMIT, "hash": f"hash-{i}", "message": f"commit {i}"}
        for i in range(5)
    ]
    resp = await client.post(
        "/api/v1/ingest/commits",
        json={"repo_url": "https://github.com/org/repo", "items": commits},
    )
    assert resp.status_code == 202
    assert resp.json()["items_received"] == 5


@pytest.mark.asyncio
async def test_ingest_incidents_empty_items(client):
    resp = await client.post(
        "/api/v1/ingest/incidents",
        json={"repo_url": "https://github.com/org/repo", "items": []},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_ingest_deployments_missing_required_field(client):
    bad_deployment = {"status": "success", "environment": "prod"}
    resp = await client.post(
        "/api/v1/ingest/deployments",
        json={"repo_url": "https://github.com/org/repo", "items": [bad_deployment]},
    )
    assert resp.status_code == 422
