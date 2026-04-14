import argparse
import uuid
from unittest.mock import patch

import pytest

from dev_health_ops.fixtures.generator import SyntheticDataGenerator
from dev_health_ops.models.git import Repo, SecurityAlert

VALID_SOURCES = {
    "dependabot",
    "code_scanning",
    "advisory",
    "gitlab_vulnerability",
    "gitlab_dependency",
}
VALID_SEVERITIES = {"low", "medium", "high", "critical", "unknown"}
VALID_STATES = {"open", "fixed", "dismissed", "detected", "confirmed", "resolved"}


def _make_repo(name: str, provider: str = "github") -> Repo:
    namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    repo_id = uuid.uuid5(namespace, name)
    return Repo(
        id=repo_id,
        repo=name,
        ref="main",
        provider=provider,
        settings={},
        tags=[],
    )


class TestGenerateSecurityAlerts:
    def test_produces_realistic_distribution(self):
        """Core distribution test: count, valid fields, and terminal-state invariants."""
        repos = [
            _make_repo("acme/alpha"),
            _make_repo("acme/beta"),
            _make_repo("acme/gamma"),
        ]
        gen = SyntheticDataGenerator(repo_name="acme/alpha", seed=42)
        count_per_repo = 100
        alerts = gen.generate_security_alerts(
            repos, count_per_repo=count_per_repo, days=90
        )

        # Correct total count
        assert len(alerts) == len(repos) * count_per_repo

        for alert in alerts:
            assert isinstance(alert, SecurityAlert)

            # Valid enumerations
            assert alert.source in VALID_SOURCES, f"Invalid source: {alert.source}"
            assert alert.severity in VALID_SEVERITIES, (
                f"Invalid severity: {alert.severity}"
            )
            assert alert.state in VALID_STATES, f"Invalid state: {alert.state}"

            # fixed_at only when state is fixed or resolved
            if alert.fixed_at is not None:
                assert alert.state in {"fixed", "resolved"}, (
                    f"fixed_at set but state={alert.state}"
                )
            # dismissed_at only when state is dismissed
            if alert.dismissed_at is not None:
                assert alert.state == "dismissed", (
                    f"dismissed_at set but state={alert.state}"
                )

            # created_at is always present
            assert alert.created_at is not None

            # last_synced is always present
            assert alert.last_synced is not None

    def test_alert_ids_are_deterministic(self):
        """Running twice with the same seed produces identical alert_ids in the same order."""
        repos = [_make_repo("acme/determinism")]
        gen1 = SyntheticDataGenerator(repo_name="acme/determinism", seed=7)
        gen2 = SyntheticDataGenerator(repo_name="acme/determinism", seed=7)

        alerts1 = gen1.generate_security_alerts(repos, count_per_repo=20, days=30)
        alerts2 = gen2.generate_security_alerts(repos, count_per_repo=20, days=30)

        ids1 = [a.alert_id for a in alerts1]
        ids2 = [a.alert_id for a in alerts2]
        assert ids1 == ids2, "Alert IDs differ between runs with the same seed"

    def test_package_name_only_for_dependency_sources(self):
        """package_name must only be set for dependabot / gitlab_dependency alerts."""
        repos = [_make_repo("acme/packages")]
        gen = SyntheticDataGenerator(repo_name="acme/packages", seed=99)
        alerts = gen.generate_security_alerts(repos, count_per_repo=200, days=90)

        for alert in alerts:
            if alert.source not in {"dependabot", "gitlab_dependency"}:
                assert alert.package_name is None, (
                    f"Unexpected package_name for source={alert.source}: {alert.package_name}"
                )

    def test_gitlab_repo_biases_toward_gitlab_sources(self):
        """GitLab-provider repos should have a significant proportion of GitLab sources."""
        repos = [_make_repo("acme/gl-repo", provider="gitlab")]
        gen = SyntheticDataGenerator(repo_name="acme/gl-repo", seed=55)
        alerts = gen.generate_security_alerts(repos, count_per_repo=200, days=90)

        gitlab_count = sum(
            1
            for a in alerts
            if a.source in {"gitlab_vulnerability", "gitlab_dependency"}
        )
        # Expect at least 20% GitLab sources with the GitLab bias applied
        assert gitlab_count / len(alerts) >= 0.20, (
            f"Expected >=20% GitLab sources, got {gitlab_count}/{len(alerts)}"
        )

    def test_cve_id_format(self):
        """CVE IDs must match CVE-YYYY-NNNNN when present."""
        import re

        pattern = re.compile(r"^CVE-\d{4}-\d{5}$")
        repos = [_make_repo("acme/cve-check")]
        gen = SyntheticDataGenerator(repo_name="acme/cve-check", seed=11)
        alerts = gen.generate_security_alerts(repos, count_per_repo=100, days=90)

        for alert in alerts:
            if alert.cve_id is not None:
                assert pattern.match(alert.cve_id), f"Bad CVE format: {alert.cve_id}"

    def test_url_is_present_for_all_alerts(self):
        """Every alert should have a non-empty URL."""
        repos = [_make_repo("acme/url-check")]
        gen = SyntheticDataGenerator(repo_name="acme/url-check", seed=33)
        alerts = gen.generate_security_alerts(repos, count_per_repo=50, days=90)

        for alert in alerts:
            assert alert.url, f"Missing URL for alert {alert.alert_id}"


class TestRunnerCallsInsertSecurityAlerts:
    """Verify that the runner invokes insert_security_alerts with a non-empty list."""

    @pytest.mark.asyncio
    async def test_insert_security_alerts_called(self, tmp_path):
        from dev_health_ops.fixtures.runner import run_fixtures_generation
        from dev_health_ops.storage import SQLAlchemyStore

        db_file = tmp_path / "test_sec_alerts.db"
        db_uri = f"sqlite:///{db_file}"

        ns = argparse.Namespace(
            sink=db_uri,
            db_type="sqlite",
            org_id="test-org",
            repo_name="test/security-smoke",
            repo_count=1,
            days=7,
            commits_per_day=1,
            pr_count=1,
            seed=42,
            provider="synthetic",
            with_work_graph=False,
            with_metrics=False,
            team_count=1,
        )

        called_with = []
        original_insert = SQLAlchemyStore.insert_security_alerts

        async def _spy(self, alerts):
            called_with.extend(alerts)
            return await original_insert(self, alerts)

        with patch.object(SQLAlchemyStore, "insert_security_alerts", _spy):
            result = await run_fixtures_generation(ns)

        assert result == 0
        assert len(called_with) > 0, (
            "insert_security_alerts was never called with alerts"
        )
