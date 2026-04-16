"""Tests for OrgScopedQuery builder."""

from __future__ import annotations

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
