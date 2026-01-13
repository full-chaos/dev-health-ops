import pytest
from datetime import date
from unittest.mock import MagicMock, patch
from api.services.investment_flow import build_investment_flow_response
from api.models.filters import MetricFilter


@pytest.mark.asyncio
async def test_build_investment_flow_prefers_team():
    # Setup mock data for team coverage >= 0.7 and 2+ targets
    team_edges = [
        {"source": "Sub1", "target": "Team A", "value": 40},
        {"source": "Sub1", "target": "Team B", "value": 40},
        {"source": "Sub1", "target": "unassigned", "value": 20},
    ]
    repo_edges = [
        {"source": "Sub1", "target": "Repo 1", "value": 10},
        {"source": "Sub1", "target": "unassigned", "value": 90},
    ]

    # We need to mock the context manager and the queries
    filters = MagicMock(spec=MetricFilter)
    filters.scope = MagicMock()
    filters.scope.level = "organization"

    with (
        patch(
            "api.services.investment_flow.time_window",
            return_value=(date(2024, 1, 1), date(2024, 1, 31), None, None),
        ),
        patch(
            "api.services.investment_flow._split_category_filters",
            return_value=([], []),
        ),
        patch(
            "api.services.investment_flow.clickhouse_client"
        ) as mock_client_cm,
        patch(
            "api.services.investment_flow._tables_present",
            return_value=True,
        ),
        patch(
            "api.services.investment_flow._columns_present",
            return_value=True,
        ),
        patch(
            "api.services.investment_flow.fetch_investment_team_edges",
            return_value=team_edges,
        ),
        patch(
            "api.services.investment_flow.fetch_investment_subcategory_edges",
            return_value=repo_edges,
        ),
    ):
        # Mock the async context manager
        mock_client = MagicMock()
        mock_client_cm.return_value.__aenter__.return_value = mock_client

        response = await build_investment_flow_response(
            db_url="mock://", filters=filters
        )

        # team_coverage = 80/100 = 0.8
        # distinct_teams = 2 (Team A, Team B)
        # Should prefer team
        assert response.chosen_mode == "team"
        assert response.team_coverage == 0.8
        assert response.distinct_team_targets == 2
        assert response.label == "Subcategory → Team"
        assert any(link.target == "Team A" for link in response.links)


@pytest.mark.asyncio
async def test_build_investment_flow_prefers_repo():
    # Setup team coverage < 0.7, but repo coverage >= 0.7
    team_edges = [
        {"source": "Sub1", "target": "unassigned", "value": 100},
    ]
    repo_edges = [
        {"source": "Sub1", "target": "Repo 1", "value": 50},
        {"source": "Sub1", "target": "Repo 2", "value": 30},
        {"source": "Sub1", "target": "unassigned", "value": 20},
    ]

    filters = MagicMock(spec=MetricFilter)
    filters.scope = MagicMock()
    filters.scope.level = "organization"

    with (
        patch(
            "api.services.investment_flow.time_window",
            return_value=(date(2024, 1, 1), date(2024, 1, 31), None, None),
        ),
        patch(
            "api.services.investment_flow._split_category_filters",
            return_value=([], []),
        ),
        patch(
            "api.services.investment_flow.clickhouse_client"
        ) as mock_client_cm,
        patch(
            "api.services.investment_flow._tables_present",
            return_value=True,
        ),
        patch(
            "api.services.investment_flow._columns_present",
            return_value=True,
        ),
        patch(
            "api.services.investment_flow.fetch_investment_team_edges",
            return_value=team_edges,
        ),
        patch(
            "api.services.investment_flow.fetch_investment_subcategory_edges",
            return_value=repo_edges,
        ),
    ):
        mock_client = MagicMock()
        mock_client_cm.return_value.__aenter__.return_value = mock_client

        response = await build_investment_flow_response(
            db_url="mock://", filters=filters
        )

        assert response.chosen_mode == "repo_scope"
        assert response.repo_coverage == 0.8
        assert response.distinct_repo_targets == 2
        assert response.label == "Subcategory → Repo scope"


@pytest.mark.asyncio
async def test_build_investment_flow_fallbacks():
    # Setup neither satisfying thresholds
    team_edges = [{"source": "Sub1", "target": "unassigned", "value": 100}]
    repo_edges = [{"source": "Sub1", "target": "unassigned", "value": 100}]

    filters = MagicMock(spec=MetricFilter)
    filters.scope = MagicMock()
    filters.scope.level = "organization"

    with (
        patch(
            "api.services.investment_flow.time_window",
            return_value=(date(2024, 1, 1), date(2024, 1, 31), None, None),
        ),
        patch(
            "api.services.investment_flow._split_category_filters",
            return_value=([], []),
        ),
        patch(
            "api.services.investment_flow.clickhouse_client"
        ) as mock_client_cm,
        patch(
            "api.services.investment_flow._tables_present",
            return_value=True,
        ),
        patch(
            "api.services.investment_flow._columns_present",
            return_value=True,
        ),
        patch(
            "api.services.investment_flow.fetch_investment_team_edges",
            return_value=team_edges,
        ),
        patch(
            "api.services.investment_flow.fetch_investment_subcategory_edges",
            return_value=repo_edges,
        ),
    ):
        mock_client = MagicMock()
        mock_client_cm.return_value.__aenter__.return_value = mock_client

        response = await build_investment_flow_response(
            db_url="mock://", filters=filters
        )

        assert response.chosen_mode == "fallback"
        assert response.links == []
        assert response.label == "Investment allocation"
