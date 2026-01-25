"""
Unit tests for investment explanation caching.

Tests that explanations are cached and retrieved properly.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from dev_health_ops.api.services.investment_mix_explain import (
    _compute_cache_key,
    explain_investment_mix,
)


def test_compute_cache_key_deterministic():
    """Test that cache key computation is deterministic."""

    # Create a mock filter-like object
    class MockFilters:
        def model_dump(self, mode=None):
            return {
                "scope": {"level": "org", "ids": []},
                "time_range": {"range_days": 14, "compare_days": 14},
            }

    filters = MockFilters()

    key1 = _compute_cache_key(filters, theme=None, subcategory=None)
    key2 = _compute_cache_key(filters, theme=None, subcategory=None)

    assert key1 == key2
    assert len(key1) == 32  # SHA256 truncated to 32 chars


def test_compute_cache_key_different_themes():
    """Test that different themes produce different cache keys."""

    class MockFilters:
        def model_dump(self, mode=None):
            return {"scope": {"level": "org"}}

    filters = MockFilters()

    key_none = _compute_cache_key(filters, theme=None, subcategory=None)
    key_feature = _compute_cache_key(
        filters, theme="feature_delivery", subcategory=None
    )
    key_maintenance = _compute_cache_key(filters, theme="maintenance", subcategory=None)

    assert key_none != key_feature
    assert key_feature != key_maintenance
    assert key_none != key_maintenance


def test_compute_cache_key_different_subcategories():
    """Test that different subcategories produce different cache keys."""

    class MockFilters:
        def model_dump(self, mode=None):
            return {"scope": {"level": "org"}}

    filters = MockFilters()

    key1 = _compute_cache_key(
        filters, theme="feature_delivery", subcategory="feature_delivery.customer"
    )
    key2 = _compute_cache_key(
        filters, theme="feature_delivery", subcategory="feature_delivery.roadmap"
    )

    assert key1 != key2


def test_compute_cache_key_with_dict_filters():
    """Test cache key computation with dict-style filters (no model_dump)."""

    class MockFiltersDict:
        def dict(self):
            return {"scope": {"level": "repo", "ids": ["abc"]}}

    filters = MockFiltersDict()

    key = _compute_cache_key(filters, theme=None, subcategory=None)
    assert len(key) == 32


def test_compute_cache_key_with_string_fallback():
    """Test cache key computation when filter has neither model_dump nor dict."""

    class PlainFilter:
        def __str__(self):
            return "plain-filter-string"

    filters = PlainFilter()

    key = _compute_cache_key(filters, theme=None, subcategory=None)
    assert len(key) == 32


@pytest.mark.asyncio
async def test_explain_investment_mix_mock_provider_skips_cache():
    """Test that mock provider does not use cache."""
    # This test verifies that llm_provider='mock' bypasses cache lookup
    with (
        patch(
            "dev_health_ops.api.services.investment_mix_explain.build_investment_response"
        ) as mock_build,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.build_work_unit_investments"
        ) as mock_units,
        patch("dev_health_ops.api.services.investment_mix_explain.get_provider") as mock_get_provider,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.ClickHouseMetricsSink"
        ) as mock_sink_class,
    ):
        # Setup mocks
        mock_investment = MagicMock()
        mock_investment.theme_distribution = {
            "feature_delivery": 0.6,
            "maintenance": 0.4,
        }
        mock_investment.subcategory_distribution = {
            "feature_delivery.customer": 0.4,
            "feature_delivery.roadmap": 0.2,
            "maintenance.refactor": 0.4,
        }
        mock_build.return_value = mock_investment

        mock_units.return_value = []

        mock_provider = MagicMock()
        mock_provider.complete = AsyncMock(
            return_value='{"summary": "Test summary", "top_findings": [], "confidence": {"level": "moderate"}, "what_to_check_next": [], "anti_claims": []}'
        )
        mock_get_provider.return_value = mock_provider

        class MockFilters:
            def model_dump(self, mode=None):
                return {"scope": {"level": "org"}}

        filters = MockFilters()

        # Call with mock provider
        await explain_investment_mix(
            db_url="clickhouse://localhost:9000/test",
            filters=filters,
            llm_provider="mock",
        )

        # Cache should not be accessed for mock provider
        mock_sink_class.assert_not_called()
