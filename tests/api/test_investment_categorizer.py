from __future__ import annotations

import pytest

from analytics.work_units import CANONICAL_SUBCATEGORIES
from dev_health_ops.api.services.investment_categorizer import (
    categorize_investment_texts,
)


@pytest.mark.asyncio
async def test_categorize_investment_texts_returns_distribution():
    texts_by_source = {
        "issue_title": ["Hotfix outage response for login service"],
        "commit_message": ["Patch incident response workflow"],
    }

    result = await categorize_investment_texts(texts_by_source, llm_provider="mock")

    assert result is not None
    assert result.subcategories
    assert set(result.subcategories.keys()) == set(CANONICAL_SUBCATEGORIES)
    total = sum(result.subcategories.values())
    assert abs(total - 1.0) < 0.01
    assert result.textual_evidence
    assert result.uncertainty
