import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

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


class TestFetchGithubSecurityAlertsSync:
    def _make_connector(self, dependabot=None, code_scanning=None, advisories=None):
        connector = MagicMock()
        connector.get_dependabot_alerts = MagicMock(return_value=dependabot or [])
        connector.get_code_scanning_alerts = MagicMock(return_value=code_scanning or [])
        connector.get_security_advisories = MagicMock(return_value=advisories or [])
        return connector

    def test_combines_all_sources(self):
        from dev_health_ops.processors.github import (
            _fetch_github_security_alerts_sync,
        )

        connector = self._make_connector(
            dependabot=[_make_alert("dependabot", "dependabot:1")],
            code_scanning=[_make_alert("code_scanning", "code_scanning:1")],
            advisories=[_make_alert("advisory", "advisory:1")],
        )
        result = _fetch_github_security_alerts_sync(
            connector, "owner", "repo", FAKE_REPO_ID, 100, None
        )
        sources = [a.source for a in result]
        assert "dependabot" in sources
        assert "code_scanning" in sources
        assert "advisory" in sources
        assert len(result) == 3

    def test_filters_by_since(self):
        from dev_health_ops.processors.github import (
            _fetch_github_security_alerts_sync,
        )

        connector = self._make_connector(
            dependabot=[_make_alert("dependabot")],
        )
        result = _fetch_github_security_alerts_sync(
            connector, "owner", "repo", FAKE_REPO_ID, 100, FEB_1
        )
        assert len(result) == 0

    def test_handles_partial_failure(self):
        from dev_health_ops.processors.github import (
            _fetch_github_security_alerts_sync,
        )

        connector = self._make_connector(
            dependabot=[_make_alert("dependabot", "dependabot:1")],
            advisories=[_make_alert("advisory", "advisory:1")],
        )
        connector.get_code_scanning_alerts.side_effect = Exception("API error")
        connector.get_code_scanning_alerts.__name__ = "get_code_scanning_alerts"
        result = _fetch_github_security_alerts_sync(
            connector, "owner", "repo", FAKE_REPO_ID, 100, None
        )
        assert len(result) == 2
        sources = {a.source for a in result}
        assert "dependabot" in sources
        assert "advisory" in sources


class TestFetchGitlabSecurityAlertsSync:
    def test_fetches_alerts(self):
        from dev_health_ops.processors.gitlab import (
            _fetch_gitlab_security_alerts_sync,
        )

        connector = MagicMock()
        connector.get_security_alerts.return_value = [
            _make_alert("gitlab_vulnerability", "gitlab_vuln:1"),
            _make_alert("gitlab_dependency", "gitlab_dep:1"),
        ]
        result = _fetch_gitlab_security_alerts_sync(
            connector, 123, FAKE_REPO_ID, 100, None
        )
        assert len(result) == 2

    def test_handles_failure_gracefully(self):
        from dev_health_ops.processors.gitlab import (
            _fetch_gitlab_security_alerts_sync,
        )

        connector = MagicMock()
        connector.get_security_alerts.side_effect = Exception("403 Forbidden")
        result = _fetch_gitlab_security_alerts_sync(
            connector, 123, FAKE_REPO_ID, 100, None
        )
        assert result == []
