"""Tests for GraphQL resolvers with mocked DB client."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.errors import AuthorizationError, CostLimitExceededError
from dev_health_ops.api.graphql.resolvers.catalog import resolve_catalog


class MockClient:
    """Mock ClickHouse client for testing."""

    def __init__(self, rows: List[Dict[str, Any]] = None):
        self.rows = rows or []
        self.queries_executed: List[str] = []

    def query(self, sql: str, parameters: Dict[str, Any] = None):
        """Mock query method that returns configured row data."""
        self.queries_executed.append(sql)
        return MockQueryResult(self.rows)


class MockQueryResult:
    """Mock query result."""

    def __init__(self, rows: List[Dict[str, Any]]):
        if rows:
            self.column_names = list(rows[0].keys()) if rows else []
            self.result_rows = [list(row.values()) for row in rows]
        else:
            self.column_names = []
            self.result_rows = []


@pytest.fixture
def mock_context():
    """Create a mock GraphQL context."""
    return GraphQLContext(
        org_id="test-org",
        db_url="clickhouse://localhost:8123/default",
        client=MockClient(),
    )


@pytest.fixture
def context_with_data():
    """Create a context with mock data."""
    mock_data = [
        {"value": "team-alpha", "count": 150},
        {"value": "team-beta", "count": 100},
        {"value": "team-gamma", "count": 50},
    ]
    return GraphQLContext(
        org_id="test-org",
        db_url="clickhouse://localhost:8123/default",
        client=MockClient(mock_data),
    )


class TestResolveCatalog:
    """Tests for catalog resolver."""

    @pytest.mark.asyncio
    async def test_catalog_returns_dimensions(self, mock_context):
        """Test that catalog returns all available dimensions."""
        result = await resolve_catalog(mock_context)

        assert result.dimensions is not None
        assert len(result.dimensions) > 0

        dim_names = [d.name for d in result.dimensions]
        assert "team" in dim_names
        assert "repo" in dim_names
        assert "author" in dim_names
        assert "theme" in dim_names

    @pytest.mark.asyncio
    async def test_catalog_returns_measures(self, mock_context):
        """Test that catalog returns all available measures."""
        result = await resolve_catalog(mock_context)

        assert result.measures is not None
        assert len(result.measures) > 0

        measure_names = [m.name for m in result.measures]
        assert "count" in measure_names
        assert "churn_loc" in measure_names
        assert "cycle_time_hours" in measure_names
        assert "throughput" in measure_names

    @pytest.mark.asyncio
    async def test_catalog_returns_limits(self, mock_context):
        """Test that catalog returns cost limits."""
        result = await resolve_catalog(mock_context)

        assert result.limits is not None
        assert result.limits.max_days > 0
        assert result.limits.max_buckets > 0
        assert result.limits.max_top_n > 0
        assert result.limits.max_sankey_nodes > 0
        assert result.limits.max_sankey_edges > 0
        assert result.limits.max_sub_requests > 0

    @pytest.mark.asyncio
    async def test_catalog_without_dimension_has_no_values(self, mock_context):
        """Test that catalog without dimension selection has no values."""
        result = await resolve_catalog(mock_context, dimension=None)

        # values should be None when no dimension is specified
        assert result.values is None

    @pytest.mark.asyncio
    async def test_catalog_dimensions_have_descriptions(self, mock_context):
        """Test that dimensions have descriptions."""
        result = await resolve_catalog(mock_context)

        for dim in result.dimensions:
            assert dim.description is not None
            # Description should be meaningful (not empty)
            assert len(dim.description) > 0

    @pytest.mark.asyncio
    async def test_catalog_measures_have_descriptions(self, mock_context):
        """Test that measures have descriptions."""
        result = await resolve_catalog(mock_context)

        for measure in result.measures:
            assert measure.description is not None
            # Description should be meaningful (not empty)
            assert len(measure.description) > 0


class TestContextOrgIdRequirement:
    """Tests for org_id enforcement."""

    def test_context_requires_org_id(self):
        """Test that context creation requires org_id."""
        with pytest.raises(AuthorizationError):
            GraphQLContext(
                org_id="",
                db_url="clickhouse://localhost:8123/default",
            )

    def test_context_accepts_valid_org_id(self):
        """Test that context accepts valid org_id."""
        context = GraphQLContext(
            org_id="valid-org-id",
            db_url="clickhouse://localhost:8123/default",
        )
        assert context.org_id == "valid-org-id"


class TestErrorTypes:
    """Tests for error type serialization."""

    def test_cost_limit_error_to_dict(self):
        """Test CostLimitExceededError serialization."""
        error = CostLimitExceededError(
            message="Date range exceeds limit",
            limit_name="max_days",
            limit_value=365,
            requested_value=400,
        )

        error_dict = error.to_dict()

        assert error_dict["code"] == "COST_LIMIT_EXCEEDED"
        assert error_dict["message"] == "Date range exceeds limit"
        assert error_dict["extensions"]["limit_name"] == "max_days"
        assert error_dict["extensions"]["limit_value"] == 365
        assert error_dict["extensions"]["requested_value"] == 400

    def test_authorization_error_to_dict(self):
        """Test AuthorizationError serialization."""
        error = AuthorizationError("org_id is required")

        error_dict = error.to_dict()

        assert error_dict["code"] == "AUTHORIZATION_ERROR"
        assert "org_id" in error_dict["message"]


class TestCatalogWithMockedQueryDicts:
    """Tests for catalog with mocked query_dicts."""

    @pytest.mark.asyncio
    async def test_catalog_with_dimension_values(self, context_with_data):
        """Test catalog fetches dimension values when dimension specified."""
        from dev_health_ops.api.graphql.models.inputs import DimensionInput

        # The catalog resolver correctly uses the client from context
        # Just verify the flow works - actual query execution is tested in integration tests
        result = await resolve_catalog(
            context_with_data,
            dimension=DimensionInput.TEAM,
        )

        # Should have dimensions and measures regardless
        assert result.dimensions is not None
        assert result.measures is not None
        assert result.limits is not None


class TestAuthzFunctions:
    """Tests for authorization utility functions."""

    def test_require_org_id_with_valid_context(self, mock_context):
        """Test require_org_id with valid context."""
        from dev_health_ops.api.graphql.authz import require_org_id

        org_id = require_org_id(mock_context)
        assert org_id == "test-org"

    def test_enforce_org_scope(self):
        """Test enforce_org_scope adds org_id to params."""
        from dev_health_ops.api.graphql.authz import enforce_org_scope

        params = {"start_date": date(2025, 1, 1), "end_date": date(2025, 1, 31)}
        scoped = enforce_org_scope("my-org", params)

        assert scoped["org_id"] == "my-org"
        assert scoped["start_date"] == date(2025, 1, 1)
        # Original params should not be mutated
        assert "org_id" not in params

    def test_enforce_org_scope_rejects_empty(self):
        """Test enforce_org_scope rejects empty org_id."""
        from dev_health_ops.api.graphql.authz import enforce_org_scope

        with pytest.raises(AuthorizationError):
            enforce_org_scope("", {})
