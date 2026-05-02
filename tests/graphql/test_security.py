"""Tests for security alert GraphQL resolvers.

Mirrors tests/graphql/test_work_graph.py conventions:
- Mock query_dicts via AsyncMock.
- Call resolver functions directly (no HTTP layer).
- Assert on result shape and SQL parameter encoding.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.models.inputs import (
    SecurityAlertFilterInput,
    SecurityPaginationInput,
    SecuritySeverityInput,
    SecuritySourceInput,
    SecurityStateInput,
)
from dev_health_ops.api.graphql.resolvers.security import (
    _decode_cursor,
    _encode_cursor,
    resolve_security_alerts,
    resolve_security_overview,
)


class MockClient:
    pass


@pytest.fixture
def mock_context():
    return GraphQLContext(
        org_id="test-org",
        db_url="clickhouse://localhost:8123/default",
        client=MockClient(),
    )


def make_alert_row(
    alert_id: str = "alert-1",
    repo_id: str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    repo_name: str = "acme/my-repo",
    repo_url: str | None = None,
    source: str = "dependabot",
    severity: str = "high",
    state: str = "open",
    package_name: str | None = "lodash",
    cve_id: str | None = "CVE-2021-12345",
    url: str | None = "https://github.com/acme/my-repo/security/dependabot/1",
    title: str | None = "Prototype pollution in lodash",
    description: str | None = None,
    created_at: str = "2024-01-15T10:00:00",
    fixed_at: str | None = None,
    dismissed_at: str | None = None,
) -> dict[str, Any]:
    return {
        "alert_id": alert_id,
        "repo_id": repo_id,
        "repo_name": repo_name,
        "repo_url": repo_url,
        "source": source,
        "severity": severity,
        "state": state,
        "package_name": package_name,
        "cve_id": cve_id,
        "url": url,
        "title": title,
        "description": description,
        "created_at": created_at,
        "fixed_at": fixed_at,
        "dismissed_at": dismissed_at,
    }


# =============================================================================
# Cursor helpers
# =============================================================================


class TestCursorHelpers:
    def test_encode_decode_roundtrip(self):
        for offset in (0, 1, 50, 999):
            assert _decode_cursor(_encode_cursor(offset)) == offset

    def test_decode_none_returns_zero(self):
        assert _decode_cursor(None) == 0

    def test_decode_invalid_returns_zero(self):
        assert _decode_cursor("not-a-number") == 0

    def test_decode_negative_clamps_to_zero(self):
        assert _decode_cursor("-5") == 0


# =============================================================================
# resolve_security_alerts
# =============================================================================


class TestResolveSecurityAlerts:
    @pytest.mark.asyncio
    async def test_empty_result_returns_empty_connection(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            result = await resolve_security_alerts(mock_context, "test-org")

            assert result.edges == []
            assert result.page_info.has_next_page is False
            assert result.page_info.has_previous_page is False
            assert result.page_info.start_cursor is None
            assert result.page_info.end_cursor is None

    @pytest.mark.asyncio
    async def test_connection_shape(self, mock_context):
        rows = [make_alert_row(alert_id=f"a{i}") for i in range(3)]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows

            result = await resolve_security_alerts(mock_context, "test-org")

            assert len(result.edges) == 3
            assert result.page_info.has_previous_page is False
            # 3 rows < default first=50, so no next page
            assert result.page_info.has_next_page is False
            assert result.page_info.start_cursor == "1"
            assert result.page_info.end_cursor == "3"

    @pytest.mark.asyncio
    async def test_node_fields_populated(self, mock_context):
        row = make_alert_row(
            alert_id="sec-42",
            repo_id="repo-uuid-1",
            repo_name="org/repo",
            source="code_scanning",
            severity="critical",
            state="open",
            package_name="requests",
            cve_id="CVE-2023-99999",
            url="https://github.com/org/repo/security/code-scanning/42",
            title="SQL injection",
        )

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = [row]

            result = await resolve_security_alerts(mock_context, "test-org")

            node = result.edges[0].node
            assert node.alert_id == "sec-42"
            assert node.repo_id == "repo-uuid-1"
            assert node.repo_name == "org/repo"
            assert node.repo_url is None
            assert node.source == "code_scanning"
            assert node.severity == "critical"
            assert node.state == "open"
            assert node.package_name == "requests"
            assert node.cve_id == "CVE-2023-99999"
            assert node.title == "SQL injection"
            assert node.url == "https://github.com/org/repo/security/code-scanning/42"

    @pytest.mark.asyncio
    async def test_open_only_coerces_to_open_state_set(self, mock_context):
        """open_only=True must override any explicit states and filter to {open, detected, confirmed}."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = SecurityAlertFilterInput(
                open_only=True,
                states=[SecurityStateInput.FIXED],  # should be overridden
            )
            await resolve_security_alerts(mock_context, "test-org", filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "sa.state IN %(open_states)s" in sql
            assert set(params["open_states"]) == {"open", "detected", "confirmed"}
            # The states param should NOT be present since open_only took over
            assert "%(states)s" not in sql

    @pytest.mark.asyncio
    async def test_states_filter_without_open_only(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = SecurityAlertFilterInput(
                states=[SecurityStateInput.FIXED, SecurityStateInput.DISMISSED],
            )
            await resolve_security_alerts(mock_context, "test-org", filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "sa.state IN %(states)s" in sql
            assert set(params["states"]) == {"fixed", "dismissed"}

    @pytest.mark.asyncio
    async def test_severities_filter(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = SecurityAlertFilterInput(
                severities=[SecuritySeverityInput.CRITICAL, SecuritySeverityInput.HIGH],
            )
            await resolve_security_alerts(mock_context, "test-org", filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "sa.severity IN %(severities)s" in sql
            assert set(params["severities"]) == {"critical", "high"}

    @pytest.mark.asyncio
    async def test_sources_filter(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = SecurityAlertFilterInput(
                sources=[
                    SecuritySourceInput.DEPENDABOT,
                    SecuritySourceInput.CODE_SCANNING,
                ],
            )
            await resolve_security_alerts(mock_context, "test-org", filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "sa.source IN %(sources)s" in sql
            assert set(params["sources"]) == {"dependabot", "code_scanning"}

    @pytest.mark.asyncio
    async def test_repo_ids_filter(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = SecurityAlertFilterInput(repo_ids=["repo-1", "repo-2"])
            await resolve_security_alerts(mock_context, "test-org", filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "toString(sa.repo_id) IN %(repo_ids)s" in sql
            assert params["repo_ids"] == ["repo-1", "repo-2"]

    @pytest.mark.asyncio
    async def test_search_filter_uses_ilike_pattern(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            filters = SecurityAlertFilterInput(search="lodash")
            await resolve_security_alerts(mock_context, "test-org", filters)

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "ilike(sa.title, %(search_pattern)s)" in sql
            assert "ilike(sa.package_name, %(search_pattern)s)" in sql
            assert "ilike(sa.cve_id, %(search_pattern)s)" in sql
            assert params["search_pattern"] == "%lodash%"

    @pytest.mark.asyncio
    async def test_org_id_scoping_always_present(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = []

            await resolve_security_alerts(mock_context, "test-org")

            call_args = mock_query.call_args
            sql = call_args[0][1]
            params = call_args[0][2]

            assert "r.org_id = %(org_id)s" in sql
            assert params["org_id"] == "test-org"

    @pytest.mark.asyncio
    async def test_cursor_roundtrip_returns_next_page(self, mock_context):
        """After receiving page 1, the cursor from its end_cursor should offset page 2."""
        page1_rows = [make_alert_row(alert_id=f"a{i}") for i in range(3)]
        page2_rows = [make_alert_row(alert_id=f"b{i}") for i in range(2)]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            # First call returns page 1
            mock_query.return_value = page1_rows
            result1 = await resolve_security_alerts(
                mock_context, "test-org", pagination=SecurityPaginationInput(first=3)
            )
            end_cursor = result1.page_info.end_cursor
            assert end_cursor == "3"  # next offset after page 1 (3 rows starting at 0)

            # Second call uses the cursor from page 1
            mock_query.return_value = page2_rows
            result2 = await resolve_security_alerts(
                mock_context,
                "test-org",
                pagination=SecurityPaginationInput(first=3, after=end_cursor),
            )

            # Verify the offset was passed in the SQL params
            call_args = mock_query.call_args
            params = call_args[0][2]
            assert params["offset"] == 3  # decoded from "3"

            assert len(result2.edges) == 2
            assert result2.page_info.has_previous_page is True
            # Edge cursors should continue from where page 1 left off
            assert result2.edges[0].cursor == "4"
            assert result2.edges[1].cursor == "5"

    @pytest.mark.asyncio
    async def test_has_next_page_when_full_page_returned(self, mock_context):
        # n+1 probe: return first+1 rows to signal there is a next page
        rows = [make_alert_row(alert_id=f"a{i}") for i in range(6)]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.return_value = rows

            result = await resolve_security_alerts(
                mock_context, "test-org", pagination=SecurityPaginationInput(first=5)
            )

            assert result.page_info.has_next_page is True
            # Only first=5 edges emitted despite 6 rows fetched
            assert len(result.edges) == 5

    @pytest.mark.asyncio
    async def test_raises_when_client_missing(self):
        context = GraphQLContext(
            org_id="test-org",
            db_url="clickhouse://localhost:8123/default",
            client=None,
        )

        with pytest.raises(RuntimeError, match="Database client not available"):
            await resolve_security_alerts(context, "test-org")


# =============================================================================
# resolve_security_overview
# =============================================================================


def make_kpi_row(
    open_total: int = 10,
    critical: int = 2,
    high: int = 4,
    mean_days_to_fix_30d: float | None = 5.3,
    open_delta_30d: int = 3,
) -> dict[str, Any]:
    return {
        "open_total": open_total,
        "critical": critical,
        "high": high,
        "mean_days_to_fix_30d": mean_days_to_fix_30d,
        "open_delta_30d": open_delta_30d,
    }


class TestResolveSecurityOverview:
    @pytest.mark.asyncio
    async def test_fires_four_queries_in_parallel(self, mock_context):
        """Overview must fire all 4 sub-queries (via asyncio.gather)."""
        kpi = [make_kpi_row()]
        breakdown = [{"severity": "high", "count": 4}]
        top_repos = [
            {"repo_id": "r1", "repo_name": "org/a", "repo_url": None, "count": 4}
        ]
        trend = [{"day": date(2024, 1, 15), "opened": 2, "fixed": 1}]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [kpi, breakdown, top_repos, trend]

            result = await resolve_security_overview(mock_context, "test-org")

            assert mock_query.call_count == 4
            assert result.kpis.open_total == 10

    @pytest.mark.asyncio
    async def test_kpis_fields_populated(self, mock_context):
        kpi = [
            make_kpi_row(
                open_total=20,
                critical=5,
                high=8,
                mean_days_to_fix_30d=3.7,
                open_delta_30d=-2,
            )
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [kpi, [], [], []]

            result = await resolve_security_overview(mock_context, "test-org")

            assert result.kpis.open_total == 20
            assert result.kpis.critical == 5
            assert result.kpis.high == 8
            assert result.kpis.mean_days_to_fix_30d is not None
            assert abs(result.kpis.mean_days_to_fix_30d - 3.7) < 0.01
            assert result.kpis.open_delta_30d == -2

    @pytest.mark.asyncio
    async def test_null_mean_days_when_no_fixed_alerts(self, mock_context):
        kpi = [make_kpi_row(mean_days_to_fix_30d=None)]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [kpi, [], [], []]

            result = await resolve_security_overview(mock_context, "test-org")

            assert result.kpis.mean_days_to_fix_30d is None

    @pytest.mark.asyncio
    async def test_severity_breakdown_shape(self, mock_context):
        breakdown = [
            {"severity": "critical", "count": 3},
            {"severity": "high", "count": 7},
            {"severity": "medium", "count": 2},
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[make_kpi_row()], breakdown, [], []]

            result = await resolve_security_overview(mock_context, "test-org")

            assert len(result.severity_breakdown) == 3
            assert result.severity_breakdown[0].severity == "critical"
            assert result.severity_breakdown[0].count == 3

    @pytest.mark.asyncio
    async def test_top_repos_includes_repo_name(self, mock_context):
        """Top repos must include repo_name, not just repo_id."""
        top_repos = [
            {
                "repo_id": "uuid-1",
                "repo_name": "acme/backend",
                "repo_url": None,
                "count": 10,
            },
            {
                "repo_id": "uuid-2",
                "repo_name": "acme/frontend",
                "repo_url": None,
                "count": 5,
            },
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[make_kpi_row()], [], top_repos, []]

            result = await resolve_security_overview(mock_context, "test-org")

            assert len(result.top_repos) == 2
            assert result.top_repos[0].repo_id == "uuid-1"
            assert result.top_repos[0].repo_name == "acme/backend"
            assert result.top_repos[0].repo_url is None
            assert result.top_repos[0].count == 10

    @pytest.mark.asyncio
    async def test_trend_shape(self, mock_context):
        trend = [
            {"day": date(2024, 1, 10), "opened": 3, "fixed": 1},
            {"day": date(2024, 1, 11), "opened": 0, "fixed": 2},
        ]

        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[make_kpi_row()], [], [], trend]

            result = await resolve_security_overview(mock_context, "test-org")

            assert len(result.trend) == 2
            assert result.trend[0].day == date(2024, 1, 10)
            assert result.trend[0].opened == 3
            assert result.trend[0].fixed == 1

    @pytest.mark.asyncio
    async def test_empty_result_returns_zero_kpis(self, mock_context):
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[], [], [], []]

            result = await resolve_security_overview(mock_context, "test-org")

            assert result.kpis.open_total == 0
            assert result.kpis.critical == 0
            assert result.kpis.high == 0
            assert result.kpis.mean_days_to_fix_30d is None
            assert result.kpis.open_delta_30d == 0
            assert result.severity_breakdown == []
            assert result.top_repos == []
            assert result.trend == []

    @pytest.mark.asyncio
    async def test_filters_apply_to_all_queries(self, mock_context):
        """When filters are provided, all 4 sub-queries must include the WHERE fragment."""
        with patch(
            "dev_health_ops.api.queries.client.query_dicts",
            new_callable=AsyncMock,
        ) as mock_query:
            mock_query.side_effect = [[make_kpi_row()], [], [], []]

            filters = SecurityAlertFilterInput(
                severities=[SecuritySeverityInput.CRITICAL],
            )
            await resolve_security_overview(mock_context, "test-org", filters)

            assert mock_query.call_count == 4
            for c in mock_query.call_args_list:
                sql = c[0][1]
                params = c[0][2]
                assert "sa.severity IN %(severities)s" in sql
                assert "critical" in params["severities"]

    @pytest.mark.asyncio
    async def test_raises_when_client_missing(self):
        context = GraphQLContext(
            org_id="test-org",
            db_url="clickhouse://localhost:8123/default",
            client=None,
        )

        with pytest.raises(RuntimeError, match="Database client not available"):
            await resolve_security_overview(context, "test-org")
