"""
Unit tests for work_unit_explain service.

Tests the LLM explanation service and mock provider.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from dev_health_ops.api.models.schemas import (
    EvidenceQuality,
    InvestmentBreakdown,
    WorkUnitEffort,
    WorkUnitEvidence,
    WorkUnitInvestment,
    WorkUnitTimeRange,
)
from dev_health_ops.llm import get_provider
from dev_health_ops.llm.providers.mock import MockProvider
from dev_health_ops.api.services.work_unit_explain import explain_work_unit


def _sample_investment() -> WorkUnitInvestment:
    """Create a sample WorkUnitInvestment for testing."""
    return WorkUnitInvestment(
        work_unit_id="test-work-unit-abc123",
        time_range=WorkUnitTimeRange(
            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end=datetime(2025, 1, 14, tzinfo=timezone.utc),
        ),
        effort=WorkUnitEffort(metric="churn_loc", value=1500.0),
        investment=InvestmentBreakdown(
            themes={
                "feature_delivery": 0.45,
                "maintenance": 0.30,
                "operational": 0.15,
                "quality": 0.10,
                "risk": 0.0,
            },
            subcategories={
                "feature_delivery.customer": 0.30,
                "feature_delivery.roadmap": 0.15,
                "maintenance.refactor": 0.30,
                "operational.support": 0.15,
                "quality.bugfix": 0.10,
            },
        ),
        evidence_quality=EvidenceQuality(value=0.72, band="moderate"),
        evidence=WorkUnitEvidence(
            structural=[
                {"type": "work_item_type", "work_item_type": "story", "count": 3},
                {"type": "graph_density", "nodes": 5, "edges": 4, "value": 0.8},
            ],
            textual=[
                {"type": "text_phrase", "phrase": "add", "source": "issue_title"},
            ],
            contextual=[
                {"type": "time_range", "span_days": 13.0, "score": 0.85},
            ],
        ),
    )


def test_mock_provider_returns_response():
    """Test that the mock provider returns a non-empty response."""
    provider = MockProvider()

    async def run_test():
        prompt = "Test prompt for work unit explanation."
        response = await provider.complete(prompt)
        assert response
        assert len(response) > 50  # Should be a meaningful response

    import asyncio

    asyncio.run(run_test())


def test_mock_provider_uses_approved_language():
    """Test that mock provider responses use approved language."""
    provider = MockProvider()

    async def run_test():
        prompt = """
        Evidence Quality: 0.72 (moderate)
          - feature_delivery: 48.00%
          - maintenance: 30.00%
        """
        response = await provider.complete(prompt)

        # Check for approved words
        response_lower = response.lower()
        assert (
            "appears" in response_lower
            or "leans" in response_lower
            or "suggests" in response_lower
        )

    import asyncio

    asyncio.run(run_test())


def test_mock_provider_avoids_forbidden_language():
    """Test that mock provider responses avoid forbidden language."""
    provider = MockProvider()

    async def run_test():
        prompt = "Evidence Quality: 0.72 (moderate)"
        response = await provider.complete(prompt)

        # The response should not contain these as standalone "certainty" words
        # Note: "is" might appear in words like "this", so we check for patterns
        words = set(response.lower().split())

        # These are the truly forbidden standalone words
        # The mock is designed to avoid them
        assert "detected" not in words
        assert "determined" not in words

    import asyncio

    asyncio.run(run_test())


def test_get_provider_returns_mock_without_api_keys():
    """Test that get_provider returns mock when no API keys are set."""
    with patch.dict(os.environ, {}, clear=True):
        provider = get_provider("auto")
        assert isinstance(provider, MockProvider)


def test_get_provider_explicit_mock():
    """Test that get_provider('mock') returns MockProvider."""
    provider = get_provider("mock")
    assert isinstance(provider, MockProvider)


@pytest.mark.asyncio
async def test_explain_work_unit_with_mock():
    """Test the full explain_work_unit flow with mock provider."""
    investment = _sample_investment()
    original_themes = dict(investment.investment.themes)

    explanation = await explain_work_unit(investment, llm_provider="mock")

    # Check that all required fields are present
    assert explanation.work_unit_id == investment.work_unit_id
    assert explanation.ai_generated is True
    assert explanation.summary
    assert explanation.category_rationale
    assert explanation.evidence_highlights
    assert explanation.uncertainty_disclosure
    assert explanation.evidence_quality_limits
    assert investment.investment.themes == original_themes

    # Check that the top category is mentioned
    assert (
        "feature_delivery" in explanation.summary.lower()
        or "feature_delivery" in str(explanation.category_rationale).lower()
    )


@pytest.mark.asyncio
async def test_explanation_includes_uncertainty_disclosure():
    """Test that explanation includes uncertainty disclosure."""
    investment = _sample_investment()

    explanation = await explain_work_unit(investment, llm_provider="mock")

    # Should have an uncertainty disclosure
    assert explanation.uncertainty_disclosure
    assert len(explanation.uncertainty_disclosure) > 20  # Meaningful content


@pytest.mark.asyncio
async def test_explanation_includes_evidence_quality_limits():
    """Test that explanation includes evidence quality limits."""
    investment = _sample_investment()

    explanation = await explain_work_unit(investment, llm_provider="mock")

    # Should mention evidence quality
    assert explanation.evidence_quality_limits
    assert (
        "moderate" in explanation.evidence_quality_limits.lower()
        or "evidence" in explanation.evidence_quality_limits.lower()
    )
