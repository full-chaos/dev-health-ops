"""Tests for GraphQL cost controls."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from dev_health_ops.api.graphql.cost import (
    CostLimits,
    DEFAULT_LIMITS,
    validate_buckets,
    validate_date_range,
    validate_sankey_limits,
    validate_sub_request_count,
    validate_top_n,
)
from dev_health_ops.api.graphql.errors import CostLimitExceededError


class TestValidateDateRange:
    """Tests for validate_date_range."""

    def test_valid_date_range(self):
        """Test that a valid date range passes."""
        start = date(2025, 1, 1)
        end = date(2025, 1, 30)
        # Should not raise
        validate_date_range(start, end)

    def test_date_range_exactly_at_limit(self):
        """Test that a date range exactly at the limit passes."""
        start = date(2025, 1, 1)
        end = start + timedelta(days=DEFAULT_LIMITS.max_days - 1)
        # Should not raise
        validate_date_range(start, end)

    def test_date_range_exceeds_limit(self):
        """Test that a date range exceeding the limit raises error."""
        start = date(2025, 1, 1)
        end = start + timedelta(days=DEFAULT_LIMITS.max_days + 1)

        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_date_range(start, end)

        assert exc_info.value.limit_name == "max_days"
        assert exc_info.value.code == "COST_LIMIT_EXCEEDED"

    def test_end_before_start(self):
        """Test that end_date before start_date raises error."""
        start = date(2025, 1, 30)
        end = date(2025, 1, 1)

        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_date_range(start, end)

        assert "start_date" in str(exc_info.value).lower()

    def test_custom_limits(self):
        """Test that custom limits are respected."""
        custom_limits = CostLimits(max_days=7)
        start = date(2025, 1, 1)
        end = date(2025, 1, 10)  # 10 days

        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_date_range(start, end, custom_limits)

        assert exc_info.value.limit_value == 7
        assert exc_info.value.requested_value == 10


class TestValidateTopN:
    """Tests for validate_top_n."""

    def test_valid_top_n(self):
        """Test that a valid top_n passes."""
        validate_top_n(10)
        validate_top_n(1)
        validate_top_n(DEFAULT_LIMITS.max_top_n)

    def test_top_n_exceeds_limit(self):
        """Test that top_n exceeding limit raises error."""
        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_top_n(DEFAULT_LIMITS.max_top_n + 1)

        assert exc_info.value.limit_name == "max_top_n"

    def test_top_n_zero(self):
        """Test that top_n of 0 raises error."""
        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_top_n(0)

        assert exc_info.value.limit_name == "top_n"

    def test_top_n_negative(self):
        """Test that negative top_n raises error."""
        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_top_n(-5)

        assert exc_info.value.limit_name == "top_n"


class TestValidateSankeyLimits:
    """Tests for validate_sankey_limits."""

    def test_valid_sankey_limits(self):
        """Test that valid Sankey limits pass."""
        validate_sankey_limits(50, 200)

    def test_max_nodes_exceeds_limit(self):
        """Test that max_nodes exceeding limit raises error."""
        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_sankey_limits(
                DEFAULT_LIMITS.max_sankey_nodes + 1,
                100,
            )

        assert exc_info.value.limit_name == "max_sankey_nodes"

    def test_max_edges_exceeds_limit(self):
        """Test that max_edges exceeding limit raises error."""
        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_sankey_limits(
                50,
                DEFAULT_LIMITS.max_sankey_edges + 1,
            )

        assert exc_info.value.limit_name == "max_sankey_edges"

    def test_max_nodes_zero(self):
        """Test that max_nodes of 0 raises error."""
        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_sankey_limits(0, 100)

        assert exc_info.value.limit_name == "max_nodes"

    def test_max_edges_negative(self):
        """Test that negative max_edges raises error."""
        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_sankey_limits(50, -10)

        assert exc_info.value.limit_name == "max_edges"


class TestValidateSubRequestCount:
    """Tests for validate_sub_request_count."""

    def test_valid_sub_request_count(self):
        """Test that valid sub-request counts pass."""
        validate_sub_request_count(
            timeseries_count=3,
            breakdowns_count=2,
            has_sankey=True,
        )

    def test_sub_requests_at_limit(self):
        """Test that sub-requests exactly at limit pass."""
        validate_sub_request_count(
            timeseries_count=DEFAULT_LIMITS.max_sub_requests,
            breakdowns_count=0,
            has_sankey=False,
        )

    def test_sub_requests_exceed_limit(self):
        """Test that sub-requests exceeding limit raises error."""
        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_sub_request_count(
                timeseries_count=DEFAULT_LIMITS.max_sub_requests,
                breakdowns_count=1,
                has_sankey=False,
            )

        assert exc_info.value.limit_name == "max_sub_requests"

    def test_sankey_counts_as_one(self):
        """Test that sankey counts as one sub-request."""
        # This should pass: 9 timeseries + 0 breakdowns + 1 sankey = 10
        validate_sub_request_count(
            timeseries_count=9,
            breakdowns_count=0,
            has_sankey=True,
        )

        # This should fail: 9 timeseries + 1 breakdowns + 1 sankey = 11
        with pytest.raises(CostLimitExceededError):
            validate_sub_request_count(
                timeseries_count=9,
                breakdowns_count=1,
                has_sankey=True,
            )


class TestValidateBuckets:
    """Tests for validate_buckets."""

    def test_valid_day_buckets(self):
        """Test that valid day buckets pass."""
        start = date(2025, 1, 1)
        end = date(2025, 1, 30)  # 30 buckets
        validate_buckets(start, end, "day")

    def test_valid_week_buckets(self):
        """Test that valid week buckets pass."""
        start = date(2025, 1, 1)
        end = date(2025, 12, 31)  # ~52 weeks
        validate_buckets(start, end, "week")

    def test_valid_month_buckets(self):
        """Test that valid month buckets pass."""
        start = date(2025, 1, 1)
        end = date(2025, 12, 31)  # ~12 months
        validate_buckets(start, end, "month")

    def test_day_buckets_exceed_limit(self):
        """Test that too many day buckets raises error."""
        start = date(2025, 1, 1)
        end = start + timedelta(days=DEFAULT_LIMITS.max_buckets + 10)

        with pytest.raises(CostLimitExceededError) as exc_info:
            validate_buckets(start, end, "day")

        assert exc_info.value.limit_name == "max_buckets"


class TestCostLimitsDataclass:
    """Tests for CostLimits dataclass."""

    def test_default_limits(self):
        """Test that default limits are set correctly."""
        assert DEFAULT_LIMITS.max_days == 3650
        assert DEFAULT_LIMITS.max_buckets == 100
        assert DEFAULT_LIMITS.max_top_n == 100
        assert DEFAULT_LIMITS.max_sankey_nodes == 100
        assert DEFAULT_LIMITS.max_sankey_edges == 500
        assert DEFAULT_LIMITS.max_sub_requests == 10
        assert DEFAULT_LIMITS.query_timeout_seconds == 30

    def test_custom_limits(self):
        """Test that custom limits can be created."""
        custom = CostLimits(max_days=30, max_buckets=50)
        assert custom.max_days == 30
        assert custom.max_buckets == 50
        # Other fields should still have defaults
        assert custom.max_top_n == 100
