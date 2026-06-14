from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.api.models.filters import MetricFilter
from dev_health_ops.api.services.investment_segments import build_segment_investment


@pytest.mark.asyncio
async def test_build_segment_investment_threads_org_id_to_downstream():
    """org_id passed to build_segment_investment must be forwarded to each
    downstream ClickHouse query helper (multi-tenant isolation, CHAOS-2394)."""
    rows = [
        {
            "work_unit_id": "wu-1",
            "categorization_run_id": "run-1",
            "effort_value": 100.0,
            "evidence_quality": 0.8,
            "theme_distribution_json": {"Feature Delivery": 1.0},
            "subcategory_distribution_json": {"feature_delivery.customer": 1.0},
        }
    ]

    filters = MagicMock(spec=MetricFilter)

    resolve_stub = AsyncMock(return_value=["repo-1"])
    investments_stub = AsyncMock(return_value=rows)
    quotes_stub = AsyncMock(return_value=[])

    with (
        patch(
            "dev_health_ops.api.services.investment_segments.time_window",
            return_value=(date(2024, 1, 1), date(2024, 1, 31), None, None),
        ),
        patch(
            "dev_health_ops.api.services.investment_segments.clickhouse_client"
        ) as mock_client_cm,
        patch(
            "dev_health_ops.api.services.investment_segments.require_clickhouse_backend"
        ),
        patch(
            "dev_health_ops.api.services.investment_segments.resolve_repo_filter_ids",
            resolve_stub,
        ),
        patch(
            "dev_health_ops.api.services.investment_segments.fetch_work_unit_investments",
            investments_stub,
        ),
        patch(
            "dev_health_ops.api.services.investment_segments.fetch_work_unit_investment_quotes",
            quotes_stub,
        ),
    ):
        mock_client = MagicMock()
        mock_client.backend_type = "clickhouse"
        mock_client_cm.return_value.__aenter__.return_value = mock_client

        result = await build_segment_investment(
            db_url="mock://",
            filters=filters,
            theme=None,
            subcategory=None,
            org_id="org-xyz",
        )

    assert result is not None

    # resolve_repo_filter_ids(sink, filters, org_id=...)
    resolve_stub.assert_awaited_once()
    assert resolve_stub.await_args is not None
    assert resolve_stub.await_args.kwargs["org_id"] == "org-xyz"

    # fetch_work_unit_investments(sink, ..., org_id=...)
    investments_stub.assert_awaited_once()
    assert investments_stub.await_args is not None
    assert investments_stub.await_args.kwargs["org_id"] == "org-xyz"

    # fetch_work_unit_investment_quotes(sink, unit_runs=..., org_id=...)
    quotes_stub.assert_awaited_once()
    assert quotes_stub.await_args is not None
    assert quotes_stub.await_args.kwargs["org_id"] == "org-xyz"
