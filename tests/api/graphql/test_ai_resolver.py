"""Tests for the AI GraphQL input types.

The AI analytics operations, aiOpportunities included, are served by query-api
and covered by its Go tests; their Strawberry fields raise.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.graphql.models.ai import AIAttributionScopeInput


def test_ai_attribution_scope_input_does_not_expose_work_type():
    """CHAOS-2744 (Oracle NO-GO): ai_attribution_resolved has no work_type
    column, so AIAttributionScopeInput must not expose the field at all --
    accepting-and-ignoring it was the original bug (silent no-op filter).
    Use the shared AIScopeInput (which does carry work_type) for queries
    backed by tables that actually have that column.
    """
    with pytest.raises(TypeError):
        AIAttributionScopeInput(work_type="bug")  # type: ignore[call-arg]
