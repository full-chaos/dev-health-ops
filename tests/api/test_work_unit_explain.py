"""
Unit tests for work_unit_explain service.

Tests the LLM explanation service and mock provider.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dev_health_ops.api.models.schemas import (
    WorkUnitConfidence,
    WorkUnitEffort,
    WorkUnitEvidence,
    WorkUnitSignal,
    WorkUnitTimeRange,
)
from dev_health_ops.api.services.llm_providers import get_provider
from dev_health_ops.api.services.llm_providers.mock import MockProvider
from dev_health_ops.api.services.work_unit_explain import explain_work_unit


def _sample_signal() -> WorkUnitSignal:
    """Create a sample WorkUnitSignal for testing."""
    return WorkUnitSignal(
        work_unit_id="test-work-unit-abc123",
        time_range=WorkUnitTimeRange(
            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end=datetime(2025, 1, 14, tzinfo=timezone.utc),
        ),
        effort=WorkUnitEffort(metric="churn_loc", value=1500.0),
        categories={
            "feature": 0.45,
            "maintenance": 0.30,
            "operational": 0.15,
            "quality": 0.10,
        },
        confidence=WorkUnitConfidence(value=0.72, band="moderate"),
        evidence=WorkUnitEvidence(
            structural=[
                {"type": "work_item_type", "work_item_type": "story", "count": 3},
                {"type": "graph_density", "nodes": 5, "edges": 4, "value": 0.8},
            ],
            temporal=[
                {"type": "time_range", "span_days": 13.0, "score": 0.85},
            ],
            textual=[
                {"category": "feature", "keyword": "add", "magnitude": 0.02},
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
        Overall Confidence: 0.72 (moderate)
          - feature: 48.00%
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
        prompt = "Overall Confidence: 0.72 (moderate)"
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
    import os

    # Ensure no API keys are set
    old_openai = os.environ.pop("OPENAI_API_KEY", None)
    old_anthropic = os.environ.pop("ANTHROPIC_API_KEY", None)

    try:
        provider = get_provider("auto")
        assert isinstance(provider, MockProvider)
    finally:
        # Restore
        if old_openai:
            os.environ["OPENAI_API_KEY"] = old_openai
        if old_anthropic:
            os.environ["ANTHROPIC_API_KEY"] = old_anthropic


def test_get_provider_explicit_mock():
    """Test that get_provider('mock') returns MockProvider."""
    provider = get_provider("mock")
    assert isinstance(provider, MockProvider)


@pytest.mark.asyncio
async def test_explain_work_unit_with_mock():
    """Test the full explain_work_unit flow with mock provider."""
    signal = _sample_signal()

    explanation = await explain_work_unit(signal, llm_provider="mock")

    # Check that all required fields are present
    assert explanation.work_unit_id == signal.work_unit_id
    assert explanation.summary
    assert explanation.category_rationale
    assert explanation.signal_importance
    assert explanation.uncertainty_disclosure
    assert explanation.confidence_limits

    # Check that the top category is mentioned
    assert (
        "feature" in explanation.summary.lower()
        or "feature" in str(explanation.category_rationale).lower()
    )


@pytest.mark.asyncio
async def test_explanation_includes_uncertainty_disclosure():
    """Test that explanation includes uncertainty disclosure."""
    signal = _sample_signal()

    explanation = await explain_work_unit(signal, llm_provider="mock")

    # Should have an uncertainty disclosure
    assert explanation.uncertainty_disclosure
    assert len(explanation.uncertainty_disclosure) > 20  # Meaningful content


@pytest.mark.asyncio
async def test_explanation_includes_confidence_limits():
    """Test that explanation includes confidence limits."""
    signal = _sample_signal()

    explanation = await explain_work_unit(signal, llm_provider="mock")

    # Should mention confidence
    assert explanation.confidence_limits
    assert (
        "moderate" in explanation.confidence_limits.lower()
        or "confidence" in explanation.confidence_limits.lower()
    )
