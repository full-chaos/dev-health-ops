"""Tests for GraphQL filter translation and application."""

from __future__ import annotations

from datetime import date

import pytest

from dev_health_ops.api.graphql.errors import ValidationError
from dev_health_ops.api.graphql.models.inputs import (
    FilterInput,
    ScopeFilterInput,
    ScopeLevelInput,
    WhatFilterInput,
    WhoFilterInput,
    WhyFilterInput,
)
from dev_health_ops.api.graphql.sql.compiler import (
    BreakdownRequest,
    TimeseriesRequest,
    compile_breakdown,
    compile_timeseries,
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

    def test_who_filter_rejected_for_non_investment_query(self):
        """CHAOS-2385/2492: who.developers is rejected (not silently applied)
        for non-investment queries -- investment_metrics_daily carries no
        per-developer breakdown at all."""
        filters = FilterInput(who=WhoFilterInput(developers=["alice@example.com"]))
        request = TimeseriesRequest(
            dimension="team",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )

        with pytest.raises(ValidationError) as exc_info:
            compile_timeseries(request, org_id="org1", filters=filters)
        assert exc_info.value.field == "who"

    def test_who_filter_uses_hasany_for_investment_query(self):
        """CHAOS-2492: who.developers on an investment query resolves via
        the au join's hasAny(author_emails) array-membership predicate."""
        filters = FilterInput(who=WhoFilterInput(developers=["alice@example.com"]))
        request = BreakdownRequest(
            dimension="theme",
            measure="count",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            use_investment=True,
        )

        sql, params = compile_breakdown(request, org_id="org1", filters=filters)

        assert "work_unit_authors" in sql
        assert "hasAny(au.author_emails, %(developer_ids)s)" in sql
        assert params["developer_ids"] == ["alice@example.com"]

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
            dimension="repo",
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
            dimension="repo",
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

    def test_scope_filter_developer_rejected_for_non_investment_query(self):
        """CHAOS-2385/2492: scope.level=developer is rejected for
        non-investment queries -- same gap as who.developers above."""
        filters = FilterInput(
            scope=ScopeFilterInput(
                level=ScopeLevelInput.DEVELOPER, ids=["alice@example.com"]
            )
        )
        request = TimeseriesRequest(
            dimension="team",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )

        with pytest.raises(ValidationError) as exc_info:
            compile_timeseries(request, org_id="org1", filters=filters)
        assert exc_info.value.field == "scope"

    def test_who_filter_preserves_stale_non_email_values_for_investment_query(self):
        """CHAOS-2746: stale bookmarked URLs can still carry historical
        non-email developer tokens. The advanced picker no longer offers or
        adds those values, but old URLs should compile to an honest-empty
        author_email predicate instead of rejecting page load."""
        filters = FilterInput(who=WhoFilterInput(developers=["alice, bob"]))
        request = BreakdownRequest(
            dimension="theme",
            measure="count",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            use_investment=True,
        )

        sql, params = compile_breakdown(request, org_id="org1", filters=filters)

        assert "work_unit_authors" in sql
        assert "hasAny(au.author_emails, %(developer_ids)s)" in sql
        assert params["developer_ids"] == ["alice, bob"]

    def test_scope_filter_developer_rejects_non_email_values(self):
        """CHAOS-2385: scope.level=developer ids must look like email
        addresses -- same gap as who.developers above."""
        filters = FilterInput(
            scope=ScopeFilterInput(
                level=ScopeLevelInput.DEVELOPER, ids=["not-an-email"]
            )
        )
        request = TimeseriesRequest(
            dimension="team",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )

        with pytest.raises(ValidationError) as exc_info:
            compile_timeseries(request, org_id="org1", filters=filters)
        assert exc_info.value.field == "scope"
        assert "email" in str(exc_info.value).lower()

    def test_scope_filter_developer_rejects_angle_bracket_email_values(self):
        """CHAOS-2746: the GraphQL _EMAIL_PATTERN previously lacked the
        <> exclusion present in the REST picker regex (_EMAIL_VALUE_RE in
        api/queries/filters.py), so a raw GraphQL request could sneak
        'alice@example.com>' past strict scope.level=developer validation
        even though the picker/REST/web stack would never emit or accept
        such a value. Pins the stricter, REST-aligned shape."""
        filters = FilterInput(
            scope=ScopeFilterInput(
                level=ScopeLevelInput.DEVELOPER, ids=["alice@example.com>"]
            )
        )
        request = TimeseriesRequest(
            dimension="team",
            measure="count",
            interval="day",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )

        with pytest.raises(ValidationError) as exc_info:
            compile_timeseries(request, org_id="org1", filters=filters)
        assert exc_info.value.field == "scope"
        assert "email" in str(exc_info.value).lower()

    def test_scope_filter_developer_uses_hasany_for_investment_query(self):
        """CHAOS-2492: scope.level=developer on an investment query resolves
        via the au join's hasAny(author_emails) predicate."""
        filters = FilterInput(
            scope=ScopeFilterInput(
                level=ScopeLevelInput.DEVELOPER, ids=["alice@example.com"]
            )
        )
        request = BreakdownRequest(
            dimension="theme",
            measure="count",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            use_investment=True,
        )

        sql, params = compile_breakdown(request, org_id="org1", filters=filters)

        assert "work_unit_authors" in sql
        assert "hasAny(au.author_emails, %(scope_ids)s)" in sql
        assert params["scope_ids"] == ["alice@example.com"]

    def test_scope_filter_developer_rejects_non_email_values_for_investment_query(
        self,
    ):
        """W2 (Oracle NO-GO on CHAOS-2492): the investment scope.level=developer
        branch built the hasAny() predicate WITHOUT calling
        _validate_developer_emails, unlike the who.developers branch -- so an
        invalid scope.ids value silently produced an empty/no-op filter
        instead of a rejection. Mirrors
        test_scope_filter_developer_rejects_non_email_values above, but with
        use_investment=True so it exercises the hasAny() branch instead of
        the non-investment rejection branch."""
        filters = FilterInput(
            scope=ScopeFilterInput(
                level=ScopeLevelInput.DEVELOPER, ids=["not-an-email"]
            )
        )
        request = BreakdownRequest(
            dimension="theme",
            measure="count",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            use_investment=True,
        )

        with pytest.raises(ValidationError) as exc_info:
            compile_breakdown(request, org_id="org1", filters=filters)
        assert exc_info.value.field == "scope"
        assert "email" in str(exc_info.value).lower()
