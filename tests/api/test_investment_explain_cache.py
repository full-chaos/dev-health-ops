"""
Unit tests for investment explanation caching.

Tests that explanations are cached and retrieved properly.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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


def test_compute_cache_key_different_org_ids():
    """Regression (CHAOS-2393): cross-tenant cache keys must diverge.

    Two tenants with identical filters/theme/subcategory must NOT produce the
    same SHA256 cache key, otherwise org B reads org A's cached LLM
    explanation from the shared ``investment_explanations`` table. ``org_id``
    is now hashed into the key so the keys differ.
    """

    class MockFilters:
        def model_dump(self, mode=None):
            return {
                "scope": {"level": "org", "ids": []},
                "time_range": {"range_days": 14, "compare_days": 14},
            }

    filters = MockFilters()

    key_org_a = _compute_cache_key(
        filters, theme="feature_delivery", subcategory=None, org_id="orgA"
    )
    key_org_b = _compute_cache_key(
        filters, theme="feature_delivery", subcategory=None, org_id="orgB"
    )

    assert key_org_a != key_org_b
    # Still a well-formed key.
    assert len(key_org_a) == 32
    assert len(key_org_b) == 32


def test_compute_cache_key_same_org_id_is_stable():
    """Same org_id (and other args) must yield the same key (cache hits work).

    The org_id scoping must not break determinism: identical inputs including
    org_id always hash to the same cache key, so a tenant's repeated request
    hits its own cached explanation.
    """

    class MockFilters:
        def model_dump(self, mode=None):
            return {
                "scope": {"level": "org", "ids": []},
                "time_range": {"range_days": 14, "compare_days": 14},
            }

    filters = MockFilters()

    key1 = _compute_cache_key(
        filters, theme="feature_delivery", subcategory=None, org_id="orgA"
    )
    key2 = _compute_cache_key(
        filters, theme="feature_delivery", subcategory=None, org_id="orgA"
    )

    assert key1 == key2
    assert len(key1) == 32


def test_compute_cache_key_org_id_changes_default_key():
    """Supplying a non-default org_id must differ from the empty-default key.

    The default ``org_id=""`` (legacy/global cache slot) must not collide with
    a real tenant's key, confirming org_id genuinely participates in the hash.
    """

    class MockFilters:
        def model_dump(self, mode=None):
            return {"scope": {"level": "org"}}

    filters = MockFilters()

    key_default = _compute_cache_key(filters, theme=None, subcategory=None)
    key_tenant = _compute_cache_key(
        filters, theme=None, subcategory=None, org_id="org-7c2f1a9e"
    )

    assert key_default != key_tenant


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
        patch(
            "dev_health_ops.api.services.investment_mix_explain.get_provider"
        ) as mock_get_provider,
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


@pytest.mark.asyncio
async def test_explain_forwards_org_id_to_work_unit_evidence():
    """Regression (CHAOS-2374): the explanation path must forward the caller's
    org_id into build_work_unit_investments.

    work_unit_investments rows are tenant-scoped (org_id is part of the
    ReplacingMergeTree dedup key, migration 027) and every reader filters
    ``org_id = %(org_id)s``. If the explain path omits org_id, it defaults to
    "" and silently drops *all* persisted work-unit evidence for any real org,
    leaving the LLM/fallback ungrounded (work_unit_count=0, no quotes, no
    quality stats) despite a non-empty aggregate distribution.
    """
    real_org = "org-7c2f1a9e"

    with (
        patch(
            "dev_health_ops.api.services.investment_mix_explain.build_investment_response"
        ) as mock_build,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.build_work_unit_investments"
        ) as mock_units,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.get_provider"
        ) as mock_get_provider,
    ):
        mock_investment = MagicMock()
        mock_investment.theme_distribution = {"feature_delivery": 1.0}
        mock_investment.subcategory_distribution = {"feature_delivery.customer": 1.0}
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

        await explain_investment_mix(
            db_url="clickhouse://localhost:9000/test",
            filters=MockFilters(),
            org_id=real_org,
            llm_provider="mock",
        )

        build_call = mock_build.await_args
        units_call = mock_units.await_args
        assert build_call is not None
        assert units_call is not None
        # Aggregate response is org-scoped...
        assert build_call.kwargs["org_id"] == real_org
        # ...and so is the work-unit evidence reader (the bug: it defaulted to "").
        assert units_call.kwargs["org_id"] == real_org
