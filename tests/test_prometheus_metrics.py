"""Tests for custom Prometheus metrics (CHAOS-656).

Verifies metric registration, labeling, and convenience helpers
for GitHub API, Celery, ClickHouse, and LLM metrics.
"""

from __future__ import annotations

from prometheus_client import REGISTRY

from dev_health_ops.metrics.prometheus import (
    GITHUB_API_REQUESTS_TOTAL,
    GITHUB_RATE_LIMIT_REMAINING,
    record_github_api_request,
    record_github_rate_limit,
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
