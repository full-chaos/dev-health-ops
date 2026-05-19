"""Tests for OrgScopedQuery builder."""

from __future__ import annotations

import pytest

from dev_health_ops.metrics.query_builder import OrgScopedQuery


class TestOrgScopedQuery:
    def test_empty_org_no_filter(self) -> None:
        q = OrgScopedQuery("")
        assert q.filter() == ""
        assert q.filter(alias="c") == ""
        assert q.inject({"x": 1}) == {"x": 1}

    def test_with_org_emits_filter(self) -> None:
        q = OrgScopedQuery("acme")
        assert q.filter() == " AND org_id = {org_id:String}"

    def test_aliased_filter(self) -> None:
        q = OrgScopedQuery("acme")
        assert q.filter(alias="c") == " AND c.org_id = {org_id:String}"

    def test_inject_adds_org_id(self) -> None:
        q = OrgScopedQuery("acme")
        params = {"start": "2024-01-01", "end": "2024-12-31"}
        result = q.inject(params)
        assert result == {
            "start": "2024-01-01",
            "end": "2024-12-31",
            "org_id": "acme",
        }

    def test_inject_is_non_mutating(self) -> None:
        q = OrgScopedQuery("acme")
        original = {"k": 1}
        q.inject(original)
        assert "org_id" not in original

    def test_bool_truthiness(self) -> None:
        assert bool(OrgScopedQuery("")) is False
        assert bool(OrgScopedQuery("acme")) is True

    @pytest.mark.parametrize(
        "bad_alias",
        [
            "c; DROP TABLE users--",
            "c OR 1=1",
            "c.d",
            "1c",
            "c-d",
            "c d",
            "'; --",
        ],
    )
    def test_filter_rejects_non_identifier_alias(self, bad_alias: str) -> None:
        q = OrgScopedQuery("acme")
        with pytest.raises(ValueError, match="valid identifier"):
            q.filter(alias=bad_alias)

    def test_filter_accepts_snake_case_alias(self) -> None:
        q = OrgScopedQuery("acme")
        assert (
            q.filter(alias="work_units") == " AND work_units.org_id = {org_id:String}"
        )

    def test_expression_empty_org(self) -> None:
        q = OrgScopedQuery("")
        assert q.expression() == ""
        assert q.expression(alias="c") == ""

    def test_expression_with_org(self) -> None:
        q = OrgScopedQuery("acme")
        assert q.expression() == "org_id = {org_id:String}"

    def test_expression_with_alias(self) -> None:
        q = OrgScopedQuery("acme")
        assert q.expression(alias="attr") == "attr.org_id = {org_id:String}"

    @pytest.mark.parametrize(
        "bad_alias",
        ["c; DROP TABLE users--", "c OR 1=1", "c.d", "1c", "c-d", "'; --"],
    )
    def test_expression_rejects_non_identifier_alias(self, bad_alias: str) -> None:
        q = OrgScopedQuery("acme")
        with pytest.raises(ValueError, match="valid identifier"):
            q.expression(alias=bad_alias)

    def test_expression_composes_cleanly_in_filter_list(self) -> None:
        """Regression for CHAOS-1716: callers that build a filter LIST and
        join with ``" AND "`` must use :meth:`expression`, not
        :meth:`filter`. Using :meth:`filter` produces a double-AND that
        ClickHouse rejects with SYNTAX_ERROR."""
        q = OrgScopedQuery("acme")
        filters = [
            "day >= {start_day:Date}",
            "day <= {end_day:Date}",
        ]
        org_expr = q.expression()
        if org_expr:
            filters.append(org_expr)
        where_clause = " AND ".join(filters)
        assert where_clause == (
            "day >= {start_day:Date} AND day <= {end_day:Date} "
            "AND org_id = {org_id:String}"
        )
        assert " AND  AND " not in where_clause

    def test_filter_still_useful_for_concatenation_callers(self) -> None:
        """Confirm :meth:`filter` stays the right tool when the caller is
        appending onto a finished WHERE clause via string concatenation,
        which is how every non-AI loader uses it."""
        q = OrgScopedQuery("acme")
        sql = "SELECT 1 FROM t WHERE foo = 1" + q.filter()
        assert sql == "SELECT 1 FROM t WHERE foo = 1 AND org_id = {org_id:String}"
