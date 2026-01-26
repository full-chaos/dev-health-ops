import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from dev_health_ops.api.main import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def github_secret(monkeypatch):
    secret = "test-github-secret"
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", secret)
    return secret


@pytest.fixture
def gitlab_token(monkeypatch):
    token = "test-gitlab-token"
    monkeypatch.setenv("GITLAB_WEBHOOK_TOKEN", token)
    return token


@pytest.fixture
def mock_celery(monkeypatch):
    calls = []

    def fake_delay(**kwargs):
        calls.append(kwargs)

    class FakeTask:
        delay = staticmethod(fake_delay)

    monkeypatch.setattr(
        "dev_health_ops.api.webhooks.router.process_webhook_event",
        FakeTask(),
        raising=False,
    )
    return calls


def _sign_github_payload(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


class TestWebhookHealthEndpoint:
    def test_health_returns_status(self, client):
        response = client.get("/api/v1/webhooks/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "secrets_configured" in data


class TestGitHubWebhook:
    def test_rejects_missing_secret_config(self, client, monkeypatch):
        monkeypatch.delenv("GITHUB_WEBHOOK_SECRET", raising=False)
        response = client.post(
            "/api/v1/webhooks/github",
            content=b"{}",
            headers={
                "X-GitHub-Event": "push",
                "X-GitHub-Delivery": "abc-123",
            },
        )
        assert response.status_code == 500

    def test_rejects_invalid_signature(self, client, github_secret):
        response = client.post(
            "/api/v1/webhooks/github",
            content=b'{"ref": "refs/heads/main"}',
            headers={
                "X-GitHub-Event": "push",
                "X-GitHub-Delivery": "abc-123",
                "X-Hub-Signature-256": "sha256=invalid",
            },
        )
        assert response.status_code == 401

    def test_accepts_valid_push_event(self, client, github_secret, mock_celery):
        payload = {"ref": "refs/heads/main", "commits": [{"id": "abc"}]}
        body = json.dumps(payload).encode()
        signature = _sign_github_payload(body, github_secret)

        response = client.post(
            "/api/v1/webhooks/github",
            content=body,
            headers={
                "X-GitHub-Event": "push",
                "X-GitHub-Delivery": "delivery-123",
                "X-Hub-Signature-256": signature,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert data["event_id"] is not None

    def test_accepts_pull_request_event(self, client, github_secret, mock_celery):
        payload = {
            "action": "opened",
            "pull_request": {"number": 42},
            "repository": {"full_name": "org/repo"},
        }
        body = json.dumps(payload).encode()
        signature = _sign_github_payload(body, github_secret)

        response = client.post(
            "/api/v1/webhooks/github",
            content=body,
            headers={
                "X-GitHub-Event": "pull_request",
                "X-GitHub-Delivery": "delivery-456",
                "X-Hub-Signature-256": signature,
            },
        )
        assert response.status_code == 200
        assert response.json()["status"] == "accepted"

    def test_returns_message_for_unsupported_event(
        self, client, github_secret, mock_celery
    ):
        payload = {"action": "created"}
        body = json.dumps(payload).encode()
        signature = _sign_github_payload(body, github_secret)

        response = client.post(
            "/api/v1/webhooks/github",
            content=body,
            headers={
                "X-GitHub-Event": "star",
                "X-GitHub-Delivery": "delivery-789",
                "X-Hub-Signature-256": signature,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert "not processed" in data["message"]


class TestGitLabWebhook:
    def test_rejects_missing_token_config(self, client, monkeypatch):
        monkeypatch.delenv("GITLAB_WEBHOOK_TOKEN", raising=False)
        response = client.post(
            "/api/v1/webhooks/gitlab",
            content=b"{}",
            headers={"X-Gitlab-Event": "Push Hook"},
        )
        assert response.status_code == 500

    def test_rejects_invalid_token(self, client, gitlab_token):
        response = client.post(
            "/api/v1/webhooks/gitlab",
            content=b"{}",
            headers={
                "X-Gitlab-Event": "Push Hook",
                "X-Gitlab-Token": "wrong-token",
            },
        )
        assert response.status_code == 401

    def test_accepts_valid_push_event(self, client, gitlab_token, mock_celery):
        payload = {
            "commits": [{"id": "abc123"}],
            "project": {"path_with_namespace": "group/project"},
        }
        body = json.dumps(payload).encode()

        response = client.post(
            "/api/v1/webhooks/gitlab",
            content=body,
            headers={
                "X-Gitlab-Event": "Push Hook",
                "X-Gitlab-Token": gitlab_token,
            },
        )
        assert response.status_code == 200
        assert response.json()["status"] == "accepted"

    def test_accepts_merge_request_event(self, client, gitlab_token, mock_celery):
        payload = {
            "object_attributes": {"iid": 10, "action": "open"},
            "project": {"path_with_namespace": "org/repo"},
        }
        body = json.dumps(payload).encode()

        response = client.post(
            "/api/v1/webhooks/gitlab",
            content=body,
            headers={
                "X-Gitlab-Event": "Merge Request Hook",
                "X-Gitlab-Token": gitlab_token,
            },
        )
        assert response.status_code == 200


class TestJiraWebhook:
    def test_accepts_issue_created(self, client, monkeypatch, mock_celery):
        monkeypatch.delenv("JIRA_WEBHOOK_SECRET", raising=False)

        payload = {
            "webhookEvent": "jira:issue_created",
            "issue": {
                "key": "PROJ-123",
                "fields": {"project": {"key": "PROJ"}, "summary": "Test"},
            },
        }
        body = json.dumps(payload).encode()

        response = client.post(
            "/api/v1/webhooks/jira",
            content=body,
        )
        assert response.status_code == 200
        assert response.json()["status"] == "accepted"

    def test_accepts_issue_updated(self, client, monkeypatch, mock_celery):
        monkeypatch.delenv("JIRA_WEBHOOK_SECRET", raising=False)

        payload = {
            "webhookEvent": "jira:issue_updated",
            "issue": {"key": "PROJ-456", "fields": {"project": {"key": "PROJ"}}},
            "changelog": {"items": [{"field": "status"}]},
        }
        body = json.dumps(payload).encode()

        response = client.post(
            "/api/v1/webhooks/jira",
            content=body,
        )
        assert response.status_code == 200

    def test_returns_message_for_unsupported_event(
        self, client, monkeypatch, mock_celery
    ):
        monkeypatch.delenv("JIRA_WEBHOOK_SECRET", raising=False)

        payload = {"webhookEvent": "jira:worklog_updated", "issue": {"key": "PROJ-789"}}
        body = json.dumps(payload).encode()

        response = client.post("/api/v1/webhooks/jira", content=body)
        assert response.status_code == 200
        assert "not processed" in response.json()["message"]
