"""Tests for GraphQL SQL compiler."""

from __future__ import annotations

from datetime import date

import pytest

from dev_health_ops.api.graphql.errors import ValidationError
from dev_health_ops.api.graphql.sql.compiler import (
    BreakdownRequest,
    CatalogValuesRequest,
    SankeyRequest,
    TimeseriesRequest,
    compile_breakdown,
    compile_catalog_values,
    compile_sankey,
    compile_timeseries,
)
from dev_health_ops.api.graphql.sql.validate import (
    Dimension,
    Measure,
    validate_dimension,
    validate_measure,
    validate_sankey_path,
)


class TestValidateDimension:
    """Tests for dimension validation."""

    def test_valid_dimensions(self):
        """Test that all valid dimensions are accepted."""
        for dim_name in Dimension.values():
            result = validate_dimension(dim_name)
            assert result.value == dim_name

    def test_case_insensitive(self):
        """Test that dimension validation is case-insensitive."""
        assert validate_dimension("TEAM") == Dimension.TEAM
        assert validate_dimension("Team") == Dimension.TEAM
        assert validate_dimension("team") == Dimension.TEAM

    def test_invalid_dimension(self):
        """Test that invalid dimension raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_dimension("invalid_dim")

        assert exc_info.value.field == "dimension"
        assert exc_info.value.code == "VALIDATION_ERROR"
        assert "invalid_dim" in str(exc_info.value.value)


class TestValidateMeasure:
    """Tests for measure validation."""

    def test_valid_measures(self):
        """Test that all valid measures are accepted."""
        for measure_name in Measure.values():
            result = validate_measure(measure_name)
            assert result.value == measure_name

    def test_case_insensitive(self):
        """Test that measure validation is case-insensitive."""
        assert validate_measure("COUNT") == Measure.COUNT
        assert validate_measure("Count") == Measure.COUNT
        assert validate_measure("count") == Measure.COUNT

    def test_invalid_measure(self):
        """Test that invalid measure raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_measure("invalid_measure")

        assert exc_info.value.field == "measure"
        assert exc_info.value.code == "VALIDATION_ERROR"


class TestValidateSankeyPath:
    """Tests for Sankey path validation."""

    def test_valid_path(self):
        """Test that valid path is accepted."""
        result = validate_sankey_path(["team", "repo"])
        assert len(result) == 2
        assert result[0] == Dimension.TEAM
        assert result[1] == Dimension.REPO

    def test_three_dimension_path(self):
        """Test that 3-dimension path is accepted."""
        result = validate_sankey_path(["work_type", "repo", "team"])
        assert len(result) == 3

    def test_empty_path(self):
        """Test that empty path raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_sankey_path([])

        assert exc_info.value.field == "path"

    def test_single_dimension_path(self):
        """Test that single-dimension path raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_sankey_path(["team"])

        assert exc_info.value.field == "path"

    def test_duplicate_dimensions(self):
        """Test that duplicate dimensions raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_sankey_path(["team", "repo", "team"])

        assert "duplicate" in str(exc_info.value).lower()


class TestCompileTimeseries:
    """Tests for compile_timeseries."""

    def test_basic_timeseries(self):
        """Test basic timeseries SQL generation."""
        # Use TEAM as it points to the default table
        request = TimeseriesRequest(
            dimension="team",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        sql, params = compile_timeseries(request, org_id="org1")

        assert "SELECT" in sql
        assert "date_trunc('day', day) AS bucket" in sql
        assert "team_id AS dimension_value" in sql
        assert "SUM(work_items_completed)" in sql
        assert "FROM investment_metrics_daily" in sql
        assert "day >= %(start_date)s AND day <= %(end_date)s" in sql
        assert params["start_date"] == date(2025, 1, 1)
        assert params["end_date"] == date(2025, 1, 7)
        assert "timeout" in params

    def test_pr_rework_ratio_timeseries_uses_repo_metrics(self):
        request = TimeseriesRequest(
            dimension="repo",
            measure="pr_rework_ratio",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )

        sql, params = compile_timeseries(request, org_id="org1")

        assert "FROM repo_metrics_daily" in sql
        assert "SUM(pr_rework_ratio * prs_merged) / NULLIF(SUM(prs_merged), 0)" in sql
        assert "repo_id AS dimension_value" in sql
        assert params["org_id"] == "org1"

    def test_invalid_dimension(self):
        """Test that invalid dimension raises ValidationError."""
        request = TimeseriesRequest(
            dimension="invalid",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
        )

        with pytest.raises(ValidationError):
            compile_timeseries(request, "test-org")

    def test_org_id_always_in_params(self):
        """Test that org_id is always included in params."""
        request = TimeseriesRequest(
            dimension="repo",
            measure="churn_loc",
            interval="week",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 3, 31),
        )

        _, params = compile_timeseries(request, "my-org-123")
        assert params["org_id"] == "my-org-123"


class TestCompileBreakdown:
    """Tests for compile_breakdown."""

    def test_basic_breakdown(self):
        """Test basic breakdown SQL generation."""
        # Use THEME to trigger investment logic
        request = BreakdownRequest(
            dimension="theme",
            measure="count",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            top_n=20,
        )
        sql, params = compile_breakdown(request, org_id="org1")

        assert "SELECT" in sql
        assert "splitByChar('.', subcategory_kv.1)[1] AS dimension_value" in sql
        assert "SUM(subcategory_kv.2 * effort_value)" in sql
        assert "FROM work_unit_investments" in sql
        assert "ARRAY JOIN" in sql
        assert "work_unit_investments.from_ts < %(end_date)s" in sql
        assert params["org_id"] == "org1"
        assert params["top_n"] == 20

    def test_org_id_always_in_params(self):
        """Test that org_id is always included in params."""
        request = BreakdownRequest(
            dimension="author",
            measure="count",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            top_n=10,
        )

        _, params = compile_breakdown(request, "different-org")
        assert params["org_id"] == "different-org"


class TestCompileSankey:
    """Tests for compile_sankey."""

    def test_basic_sankey(self):
        """Test basic Sankey compilation."""
        request = SankeyRequest(
            path=["work_type", "repo", "team"],
            measure="count",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            max_nodes=50,
            max_edges=200,
        )
        org_id = "test-org"

        nodes_queries, edges_queries = compile_sankey(request, org_id)

        # Should have 1 nodes query and 2 edges queries (for 3-dim path)
        assert len(nodes_queries) == 1
        assert len(edges_queries) == 2

        # Check nodes query
        nodes_sql, nodes_params = nodes_queries[0]
        assert "UNION ALL" in nodes_sql
        # WORK_TYPE auto-routes through use_investment=True (CHAOS-1752 — the
        # investment_metrics_daily rollup has no work_item_type column, so the
        # compiler now sources WORK_TYPE from the work_unit_investments table
        # using its work_unit_type column).
        assert "work_unit_type" in nodes_sql  # work_type column on investment table
        assert "repo_id" in nodes_sql
        assert "team_id" in nodes_sql
        assert nodes_params["org_id"] == org_id
        assert "work_unit_investments" in nodes_sql

        # Check edges queries
        for edge_sql, edge_params in edges_queries:
            assert "SELECT" in edge_sql
            assert "source" in edge_sql.lower() or "target" in edge_sql.lower()
            assert edge_params["org_id"] == org_id

    def test_invalid_path(self):
        """Test that invalid path raises ValidationError."""
        request = SankeyRequest(
            path=["invalid_dimension"],
            measure="count",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            max_nodes=50,
            max_edges=200,
        )

        with pytest.raises(ValidationError):
            compile_sankey(request, "test-org")


class TestCompileCatalogValues:
    """Tests for compile_catalog_values."""

    def test_team_catalog_uses_teams_table_as_source_of_truth(self):
        """CHAOS-1751: TEAM catalog must come from the semantic `teams`
        table (LEFT JOIN counts from the event table) so the picker
        surfaces the active roster including teams with count=0."""
        request = CatalogValuesRequest(dimension="team", limit=100)
        org_id = "test-org"

        sql, params = compile_catalog_values(request, org_id)

        # Source of truth is `teams FINAL`, not the event table.
        assert "teams FINAL" in sql
        assert "is_active = 1" in sql
        # Activity counts come from the event table via LEFT JOIN.
        assert "LEFT JOIN" in sql
        assert "COALESCE(activity.count, 0)" in sql
        assert "investment_metrics_daily" in sql
        # Teams with zero activity must still surface honestly.
        assert "GROUP BY team_id" in sql
        assert "LIMIT" in sql

        assert params["org_id"] == org_id
        assert params["limit"] == 100

    def test_non_team_catalog_uses_event_table_directly(self):
        """REPO and other non-team dimensions retain the original
        event-table catalog behavior."""
        request = CatalogValuesRequest(dimension="repo", limit=100)
        org_id = "test-org"

        sql, _ = compile_catalog_values(request, org_id)

        assert "SELECT" in sql
        assert "repo_id" in sql
        assert "COUNT(*)" in sql
        assert "GROUP BY" in sql
        assert "investment_metrics_daily" in sql
        # No teams FINAL or LEFT JOIN for non-team dimensions.
        assert "teams FINAL" not in sql
        assert "LEFT JOIN" not in sql

    def test_org_id_always_in_params(self):
        """Test that org_id is always included in params."""
        request = CatalogValuesRequest(
            dimension="repo",
            limit=50,
        )

        _, params = compile_catalog_values(request, "another-org")
        assert params["org_id"] == "another-org"


class TestDimensionDbColumn:
    """Tests for Dimension.db_column mapping."""

    @pytest.mark.parametrize("dim", list(Dimension))
    def test_all_dimensions_have_columns(self, dim):
        """Test that all dimensions map to database columns."""
        col = Dimension.db_column(dim)
        assert col is not None
        assert len(col) > 0

    def test_specific_mappings(self):
        """Test specific dimension to database column mappings."""
        # Non-investment (default)
        assert Dimension.db_column(Dimension.TEAM) == "team_id"
        assert Dimension.db_column(Dimension.REPO) == "repo_id"
        assert Dimension.db_column(Dimension.THEME) == "investment_area"

        # Investment
        assert (
            Dimension.db_column(Dimension.THEME, use_investment=True)
            == "splitByChar('.', subcategory_kv.1)[1]"
        )
        assert (
            Dimension.db_column(Dimension.SUBCATEGORY, use_investment=True)
            == "subcategory_kv.1"
        )


class TestMeasureDbExpression:
    """Tests for Measure.db_expression mapping."""

    @pytest.mark.parametrize("measure", list(Measure))
    def test_all_measures_have_expressions(self, measure):
        """Test that all measures map to SQL expressions."""
        expr = Measure.db_expression(measure)
        assert expr is not None
        assert len(expr) > 0

    def test_specific_expressions(self):
        """Test specific measure to SQL expression mappings."""
        # Non-investment (default)
        assert Measure.db_expression(Measure.COUNT) == "SUM(work_items_completed)"
        assert Measure.db_expression(Measure.THROUGHPUT) == "SUM(work_items_completed)"
        assert (
            Measure.db_expression(Measure.PR_REWORK_RATIO)
            == "SUM(pr_rework_ratio * prs_merged) / NULLIF(SUM(prs_merged), 0)"
        )

        # Investment path — expressions over real columns in work_unit_investments
        # (CHAOS-1754: old non-existent column refs replaced with valid expressions)
        assert (
            Measure.db_expression(Measure.COUNT, use_investment=True)
            == "SUM(subcategory_kv.2 * effort_value)"
        )
        assert (
            Measure.db_expression(Measure.THROUGHPUT, use_investment=True)
            == "SUM(subcategory_kv.2)"
        )
        assert (
            Measure.db_expression(Measure.CHURN_LOC, use_investment=True)
            == "SUM(if(effort_metric = 'churn_loc', subcategory_kv.2 * effort_value, 0))"
        )
        assert (
            Measure.db_expression(Measure.CYCLE_TIME_HOURS, use_investment=True)
            == "AVG(dateDiff('hour', from_ts, to_ts))"
        )
