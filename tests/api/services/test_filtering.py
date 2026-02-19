from __future__ import annotations

from datetime import date

import pytest

from dev_health_ops.api.models.filters import MetricFilter
from dev_health_ops.api.services import filtering as filtering_mod


def test_filter_cache_key_is_stable_for_equivalent_filters():
    filters = MetricFilter()
    key_a = filtering_mod.filter_cache_key("home", filters, extra={"scope": "org"})
    key_b = filtering_mod.filter_cache_key("home", filters, extra={"scope": "org"})
    assert key_a == key_b


def test_time_window_uses_explicit_start_and_end():
    filters = MetricFilter()
    filters.time.start_date = date(2026, 2, 1)
    filters.time.end_date = date(2026, 2, 10)
    filters.time.compare_days = 5

    start_day, end_day, compare_start, compare_end = filtering_mod.time_window(filters)

    assert start_day == date(2026, 2, 1)
    assert end_day == date(2026, 2, 11)
    assert compare_end == date(2026, 2, 1)
    assert compare_start == date(2026, 1, 27)


def test_time_window_clamps_invalid_start_date():
    filters = MetricFilter()
    filters.time.start_date = date(2026, 2, 10)
    filters.time.end_date = date(2026, 2, 10)

    start_day, end_day, _, _ = filtering_mod.time_window(filters)

    assert end_day == date(2026, 2, 11)
    assert start_day == date(2026, 2, 10)


def test_work_category_filter_strips_empty_values():
    filters = MetricFilter()
    filters.why.work_category = ["", " Feature Delivery ", None, "Maintenance"]

    sql, params = filtering_mod.work_category_filter(filters)

    assert sql == " AND investment_area IN %(work_categories)s"
    assert params == {"work_categories": ["Feature Delivery", "Maintenance"]}


@pytest.mark.asyncio
async def test_scope_filter_for_metric_team(monkeypatch):
    filters = MetricFilter()
    filters.scope.level = "team"
    filters.scope.ids = ["t1", "t2"]

    def fake_build_scope_filter_multi(scope, ids, team_column, repo_column):
        assert scope == "team"
        assert ids == ["t1", "t2"]
        assert team_column == "team"
        assert repo_column == "repo"
        return " team_sql ", {"team_ids": ids}

    monkeypatch.setattr(filtering_mod, "build_scope_filter_multi", fake_build_scope_filter_multi)

    sql, params = await filtering_mod.scope_filter_for_metric(
        sink=object(),
        metric_scope="team",
        filters=filters,
        team_column="team",
        repo_column="repo",
    )

    assert sql == " team_sql "
    assert params == {"team_ids": ["t1", "t2"]}


@pytest.mark.asyncio
async def test_scope_filter_for_metric_repo_uses_resolved_repo_ids(monkeypatch):
    filters = MetricFilter()

    async def fake_resolve_repo_filter_ids(_sink, _filters):
        return ["r1", "r2"]

    def fake_build_scope_filter_multi(scope, ids, team_column, repo_column):
        assert scope == "repo"
        assert ids == ["r1", "r2"]
        return " repo_sql ", {"repo_ids": ids}

    monkeypatch.setattr(filtering_mod, "resolve_repo_filter_ids", fake_resolve_repo_filter_ids)
    monkeypatch.setattr(filtering_mod, "build_scope_filter_multi", fake_build_scope_filter_multi)

    sql, params = await filtering_mod.scope_filter_for_metric(
        sink=object(),
        metric_scope="repo",
        filters=filters,
    )

    assert sql == " repo_sql "
    assert params == {"repo_ids": ["r1", "r2"]}
