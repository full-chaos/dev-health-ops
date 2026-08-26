"""Tests for custom Prometheus metrics (CHAOS-656).

Verifies metric registration, labeling, and convenience helpers
for GitHub API, Celery, ClickHouse, and LLM metrics.
"""

from __future__ import annotations

from prometheus_client import REGISTRY

from dev_health_ops.metrics.prometheus import (
    DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL,
    GITHUB_API_REQUESTS_TOTAL,
    GITHUB_RATE_LIMIT_REMAINING,
    record_github_api_request,
    record_github_rate_limit,
    record_team_autoimport_roster_preservation_failed,
)


class TestGitHubApiMetrics:
    """GitHub API Prometheus metrics."""

    def test_request_counter_increments(self):
        """record_github_api_request increments the counter with correct labels."""
        before = GITHUB_API_REQUESTS_TOTAL.labels(
            endpoint="/repos", status_code="200"
        )._value.get()

        record_github_api_request("/repos", "200")

        after = GITHUB_API_REQUESTS_TOTAL.labels(
            endpoint="/repos", status_code="200"
        )._value.get()
        assert after == before + 1

    def test_request_counter_different_status_codes(self):
        """Different status codes are tracked independently."""
        record_github_api_request("/pulls", "200")
        record_github_api_request("/pulls", "429")

        count_200 = GITHUB_API_REQUESTS_TOTAL.labels(
            endpoint="/pulls", status_code="200"
        )._value.get()
        count_429 = GITHUB_API_REQUESTS_TOTAL.labels(
            endpoint="/pulls", status_code="429"
        )._value.get()

        assert count_200 >= 1
        assert count_429 >= 1

    def test_rate_limit_gauge_set(self):
        """record_github_rate_limit sets the gauge value."""
        record_github_rate_limit("core", 4500)

        value = GITHUB_RATE_LIMIT_REMAINING.labels(resource="core")._value.get()
        assert value == 4500

    def test_rate_limit_gauge_updates(self):
        """Gauge reflects the latest value, not cumulative."""
        record_github_rate_limit("search", 30)
        record_github_rate_limit("search", 25)

        value = GITHUB_RATE_LIMIT_REMAINING.labels(resource="search")._value.get()
        assert value == 25

    def test_counter_registered_in_default_registry(self):
        """GitHub metrics are registered in the default Prometheus registry."""
        metric_names = [m.name for m in REGISTRY.collect()]
        # prometheus_client strips _total suffix from Counter names in registry
        assert "devhealth_github_api_requests" in metric_names

    def test_gauge_registered_in_default_registry(self):
        """Rate limit gauge is registered in the default Prometheus registry."""
        metric_names = [m.name for m in REGISTRY.collect()]
        assert "devhealth_github_rate_limit_remaining" in metric_names


class TestTeamAutoimportRosterPreservationFailedMetric:
    """CHAOS-4323 (team-lead 08-26): dev_health_team_autoimport_roster_preservation_failed_total.

    Exercises the REAL Counter object and record_* helper directly -- unlike
    the populator-level tests (tests/workers/test_team_autoimport_github_gitlab.py),
    which replace the recorder with a fake and so cannot catch a wrong metric
    name/label or a broken no-op fallback (codex adversarial-review,
    narrow round 4, MEDIUM)."""

    def test_counter_increments_with_correct_label(self):
        before = DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL.labels(
            provider="github"
        )._value.get()

        record_team_autoimport_roster_preservation_failed(provider="github")

        after = DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL.labels(
            provider="github"
        )._value.get()
        assert after == before + 1

    def test_counter_tracks_providers_independently(self):
        before_gitlab = (
            DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL.labels(
                provider="gitlab"
            )._value.get()
        )
        before_jira = (
            DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL.labels(
                provider="jira"
            )._value.get()
        )

        record_team_autoimport_roster_preservation_failed(provider="gitlab")

        after_gitlab = (
            DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL.labels(
                provider="gitlab"
            )._value.get()
        )
        after_jira = DEV_HEALTH_TEAM_AUTOIMPORT_ROSTER_PRESERVATION_FAILED_TOTAL.labels(
            provider="jira"
        )._value.get()
        assert after_gitlab == before_gitlab + 1
        assert after_jira == before_jira

    def test_counter_registered_in_default_registry(self):
        metric_names = [m.name for m in REGISTRY.collect()]
        assert "dev_health_team_autoimport_roster_preservation_failed" in metric_names
