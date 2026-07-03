import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.connectors.models import SecurityAlertData

FAKE_REPO_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
JAN_1 = datetime(2025, 1, 1, tzinfo=timezone.utc)
FEB_1 = datetime(2025, 2, 1, tzinfo=timezone.utc)


def _make_alert(source: str, alert_id: str = "test:1") -> SecurityAlertData:
    return SecurityAlertData(
        alert_id=alert_id,
        source=source,
        severity="high",
        state="open",
        package_name="lodash",
        cve_id="CVE-2025-0001",
        url="https://example.com/alert/1",
        title="Test alert",
        description="Test description",
        created_at=JAN_1,
        fixed_at=None,
        dismissed_at=None,
    )


class TestSecurityAlertData:
    def test_all_fields_accessible(self):
        alert = _make_alert("dependabot")
        assert alert.alert_id == "test:1"
        assert alert.source == "dependabot"
        assert alert.severity == "high"
        assert alert.state == "open"
        assert alert.package_name == "lodash"
        assert alert.cve_id == "CVE-2025-0001"
        assert alert.url == "https://example.com/alert/1"
        assert alert.title == "Test alert"
        assert alert.created_at == JAN_1
        assert alert.fixed_at is None
        assert alert.dismissed_at is None

    def test_optional_fields_default_none(self):
        alert = SecurityAlertData(alert_id="x", source="dependabot")
        assert alert.severity is None
        assert alert.state is None
        assert alert.package_name is None
        assert alert.cve_id is None
        assert alert.created_at is None


class TestSecurityAlertModel:
    def test_tablename(self):
        from dev_health_ops.models.git import SecurityAlert

        assert SecurityAlert.__tablename__ == "security_alerts"

    def test_instantiation(self):
        from dev_health_ops.models.git import SecurityAlert

        alert = SecurityAlert(
            repo_id=FAKE_REPO_ID,
            alert_id="dependabot:42",
            source="dependabot",
            severity="critical",
            state="open",
            created_at=JAN_1,
        )
        assert alert.repo_id == FAKE_REPO_ID
        assert alert.alert_id == "dependabot:42"
        assert alert.source == "dependabot"


class _StubGitHubCodeClient:
    """Stand-in for ``providers/github/code_client.py::GitHubCodeClient``
    (CHAOS-2773 CS3) that returns canned alerts without any HTTP transport --
    the transport-level behavior (pagination, 403/404/429, field mapping) is
    pinned separately by
    ``tests/providers/test_github_code_client_security.py``. This stub only
    exercises ``_fetch_github_security_alerts_async``'s orchestration:
    combining the three endpoints, the ``since`` filter, and the
    per-endpoint degrade-and-log behavior on a fetch failure."""

    def __init__(
        self,
        dependabot=None,
        code_scanning=None,
        advisories=None,
        code_scanning_error=None,
    ):
        self._dependabot = dependabot or []
        self._code_scanning = code_scanning or []
        self._advisories = advisories or []
        self._code_scanning_error = code_scanning_error

    async def get_dependabot_alerts(self, owner, repo, *, max_alerts=None):
        return self._dependabot

    async def get_code_scanning_alerts(self, owner, repo, *, max_alerts=None):
        if self._code_scanning_error is not None:
            raise self._code_scanning_error
        return self._code_scanning

    async def get_security_advisories(self, owner, repo, *, max_alerts=None):
        return self._advisories

    def drain_usage_observations(self) -> list[dict[str, Any]]:
        return []

    async def close(self) -> None:
        return None


class TestFetchGithubSecurityAlertsSync:
    def _make_client(
        self,
        dependabot=None,
        code_scanning=None,
        advisories=None,
        code_scanning_error=None,
    ):
        return _StubGitHubCodeClient(
            dependabot=dependabot,
            code_scanning=code_scanning,
            advisories=advisories,
            code_scanning_error=code_scanning_error,
        )

    @pytest.mark.asyncio
    async def test_combines_all_sources(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from dev_health_ops.processors import github as github_processor

        client = self._make_client(
            dependabot=[_make_alert("dependabot", "dependabot:1")],
            code_scanning=[_make_alert("code_scanning", "code_scanning:1")],
            advisories=[_make_alert("advisory", "advisory:1")],
        )
        monkeypatch.setattr(
            github_processor,
            "_github_code_client_from_connector",
            lambda connector: client,
        )
        result = await github_processor._fetch_github_security_alerts_async(
            MagicMock(), "owner", "repo", FAKE_REPO_ID, 100, None
        )
        sources = [a.source for a in result]
        assert "dependabot" in sources
        assert "code_scanning" in sources
        assert "advisory" in sources
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_filters_by_since(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from dev_health_ops.processors import github as github_processor

        client = self._make_client(dependabot=[_make_alert("dependabot")])
        monkeypatch.setattr(
            github_processor,
            "_github_code_client_from_connector",
            lambda connector: client,
        )
        result = await github_processor._fetch_github_security_alerts_async(
            MagicMock(), "owner", "repo", FAKE_REPO_ID, 100, FEB_1
        )
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_handles_partial_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dev_health_ops.processors import github as github_processor

        client = self._make_client(
            dependabot=[_make_alert("dependabot", "dependabot:1")],
            advisories=[_make_alert("advisory", "advisory:1")],
            code_scanning_error=Exception("API error"),
        )
        monkeypatch.setattr(
            github_processor,
            "_github_code_client_from_connector",
            lambda connector: client,
        )
        result = await github_processor._fetch_github_security_alerts_async(
            MagicMock(), "owner", "repo", FAKE_REPO_ID, 100, None
        )
        assert len(result) == 2
        sources = {a.source for a in result}
        assert "dependabot" in sources
        assert "advisory" in sources

    @pytest.mark.asyncio
    async def test_drains_usage_sink(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CHAOS-2803/CS2 contract: the client is drained into the
        caller-owned ``usage_sink`` list, mutated in place, regardless of
        fetch outcome."""
        from dev_health_ops.processors import github as github_processor

        client = self._make_client()
        client.drain_usage_observations = lambda: [
            {"route_family": "security", "request_count": 3}
        ]
        monkeypatch.setattr(
            github_processor,
            "_github_code_client_from_connector",
            lambda connector: client,
        )
        usage_sink: list[dict[str, Any]] = []
        await github_processor._fetch_github_security_alerts_async(
            MagicMock(), "owner", "repo", FAKE_REPO_ID, 100, None, usage_sink=usage_sink
        )
        assert usage_sink == [{"route_family": "security", "request_count": 3}]


class TestFetchGitlabSecurityAlertsSync:
    class _StubClient:
        def __init__(self, alerts=None, error=None):
            self._alerts = alerts or []
            self._error = error

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get_security_alerts(self, project_id, max_alerts=None):
            if self._error is not None:
                raise self._error
            return self._alerts

        def drain_usage_observations(self):
            return []

    def test_fetches_alerts(self, monkeypatch):
        from dev_health_ops.processors import gitlab as gitlab_processor

        stub = self._StubClient(
            alerts=[
                _make_alert("gitlab_vulnerability", "gitlab_vuln:1"),
                _make_alert("gitlab_dependency", "gitlab_dep:1"),
            ]
        )
        monkeypatch.setattr(
            gitlab_processor,
            "_gitlab_code_client_from_connector",
            lambda connector: stub,
        )
        result = gitlab_processor._fetch_gitlab_security_alerts_sync(
            MagicMock(), 123, FAKE_REPO_ID, 100, None
        )
        assert len(result) == 2

    def test_handles_failure_gracefully(self, monkeypatch):
        from dev_health_ops.processors import gitlab as gitlab_processor

        stub = self._StubClient(error=Exception("403 Forbidden"))
        monkeypatch.setattr(
            gitlab_processor,
            "_gitlab_code_client_from_connector",
            lambda connector: stub,
        )
        result = gitlab_processor._fetch_gitlab_security_alerts_sync(
            MagicMock(), 123, FAKE_REPO_ID, 100, None
        )
        assert result == []
