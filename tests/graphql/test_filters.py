"""Tests for GraphQL filter translation and application."""

from __future__ import annotations

from datetime import date

from dev_health_ops.api.graphql.models.inputs import (
    FilterInput,
    WhoFilterInput,
    WhatFilterInput,
    WhyFilterInput,
    ScopeFilterInput,
    ScopeLevelInput,
)

from dev_health_ops.api.graphql.sql.compiler import (
    TimeseriesRequest,
    BreakdownRequest,
    compile_timeseries,
    compile_breakdown,
)


class TestFilterTranslation:
    """Tests for translating FilterInput to SQL."""

    def test_scope_filter_team(self):
        """Test scope filter with team level."""
        filters = FilterInput(
            scope=ScopeFilterInput(level=ScopeLevelInput.TEAM, ids=["team-1", "team-2"])
        )
        request = TimeseriesRequest(
            dimension="repo",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        sql, params = compile_timeseries(request, org_id="org1", filters=filters)

        assert "team_id IN %(scope_ids)s" in sql
        assert params["scope_ids"] == ["team-1", "team-2"]

    def test_scope_filter_repo(self):
        """Test scope filter with repo level."""
        filters = FilterInput(
            scope=ScopeFilterInput(level=ScopeLevelInput.REPO, ids=["repo-1"])
        )
        request = TimeseriesRequest(
            dimension="team",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        sql, params = compile_timeseries(request, org_id="org1", filters=filters)

        assert "repo_id IN %(scope_ids)s" in sql
        assert params["scope_ids"] == ["repo-1"]

    def test_who_filter(self):
        """Test who filter (developers)."""
        filters = FilterInput(who=WhoFilterInput(developers=["dev-1", "dev-2"]))
        request = TimeseriesRequest(
            dimension="team",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        sql, params = compile_timeseries(request, org_id="org1", filters=filters)

        assert "author_id IN %(developer_ids)s" in sql
        assert params["developer_ids"] == ["dev-1", "dev-2"]

    def test_what_filter_repos(self):
        """Test what filter (repos)."""
        filters = FilterInput(what=WhatFilterInput(repos=["repo-a", "repo-b"]))
        request = TimeseriesRequest(
            dimension="team",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        sql, params = compile_timeseries(request, org_id="org1", filters=filters)

        assert "repo_id IN %(repo_filter_ids)s" in sql
        assert params["repo_filter_ids"] == ["repo-a", "repo-b"]

    def test_why_filter_work_category(self):
        """Test why filter (work category)."""
        filters = FilterInput(why=WhyFilterInput(work_category=["Feature", "Bug"]))
        request = TimeseriesRequest(
            dimension="team",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        sql, params = compile_timeseries(request, org_id="org1", filters=filters)

        # Non-investment table uses investment_area for category
        assert "investment_area IN %(work_categories)s" in sql
        assert params["work_categories"] == ["Feature", "Bug"]

    def test_multiple_filters_combined(self):
        """Test multiple filters are ANDed together."""
        filters = FilterInput(
            scope=ScopeFilterInput(level=ScopeLevelInput.TEAM, ids=["team-1"]),
            what=WhatFilterInput(repos=["repo-1"]),
        )
        request = TimeseriesRequest(
            dimension="author",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        sql, params = compile_timeseries(request, org_id="org1", filters=filters)

        assert "team_id IN %(scope_ids)s" in sql
        assert "repo_id IN %(repo_filter_ids)s" in sql
        assert params["scope_ids"] == ["team-1"]
        assert params["repo_filter_ids"] == ["repo-1"]

    def test_investment_filters(self):
        """Test filters with investment table (use_investment=True)."""
        filters = FilterInput(
            why=WhyFilterInput(work_category=["Roadmap"]),
            scope=ScopeFilterInput(level=ScopeLevelInput.TEAM, ids=["team-xyz"]),
        )
        request = BreakdownRequest(
            dimension="theme",
            measure="count",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            use_investment=True,
        )
        sql, params = compile_breakdown(request, org_id="org1", filters=filters)

        assert "FROM work_unit_investments" in sql
        assert "ut.team_label IN %(scope_ids)s" in sql
        assert "ut.team_id IN %(scope_ids)s" in sql
        # Investment table uses subcategory_kv key for categories
        assert "splitByChar('.', subcategory_kv.1)[1] IN %(work_categories)s" in sql
        assert params["scope_ids"] == ["team-xyz"]
        assert params["work_categories"] == ["Roadmap"]

    def test_none_filters(self):
        """Test that None filters returns no additional clauses."""
        request = TimeseriesRequest(
            dimension="author",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        sql, params = compile_timeseries(request, org_id="org1", filters=None)
        # Should only have the date filter in WHERE
        assert "scope_ids" not in params
        assert "work_categories" not in params
        assert "developer_ids" not in params
        assert "repo_filter_ids" not in params
