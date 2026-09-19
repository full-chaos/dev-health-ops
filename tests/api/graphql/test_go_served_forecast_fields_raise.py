"""The three query-api-served forecast fields never answer from Python."""

from __future__ import annotations

import pytest

from dev_health_ops.api.graphql.schema import (
    GoServedOperationUnavailableError,
    schema,
)

ORG_ID = "org-1"

_QUERIES = {
    "capacityForecast": (
        'query { capacityForecast(orgId: "org-1") { forecastId } }',
        None,
    ),
    "capacityForecasts": (
        'query { capacityForecasts(orgId: "org-1") { totalCount } }',
        None,
    ),
    "throughputForecast": (
        'query { throughputForecast(orgId: "org-1", '
        'input: {historyWeeks: 12, teamIds: ["team-1"]}) { forecastId } }',
        None,
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", sorted(_QUERIES))
async def test_field_returns_error_and_no_data(operation: str) -> None:
    query, _ = _QUERIES[operation]
    result = await schema.execute(query, context_value=None)

    assert result.errors is not None
    assert len(result.errors) == 1
    assert isinstance(
        result.errors[0].original_error, GoServedOperationUnavailableError
    )
    assert operation in result.errors[0].message
    assert not (result.data or {}).get(operation)
