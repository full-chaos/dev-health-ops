"""Unit tests for the bounded-recompute planner (CHAOS-2699, brief D5-D9).

Pure -- ``plan_recompute()`` takes a scope, returns a plan, no I/O, no
mocks needed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from dev_health_ops.external_ingest.recompute import RecomputeScope, plan_recompute

ORG = "org-1"
SYSTEM = "github"
INSTANCE = "acme/api"


def _scope(**overrides: Any) -> RecomputeScope:
    defaults: dict[str, Any] = dict(
        org_id=ORG,
        source_system=SYSTEM,
        source_instance=INSTANCE,
        repo_ids=frozenset(),
        team_ids=frozenset(),
        record_kinds=frozenset(),
        ingestion_ids=frozenset({"ing-1"}),
        window_start=datetime(2026, 6, 25, tzinfo=timezone.utc),
        window_end=datetime(2026, 6, 26, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return RecomputeScope(**defaults)


def test_git_kinds_single_repo_per_repo_chain_no_fallback() -> None:
    scope = _scope(
        record_kinds=frozenset({"pull_request.v1"}), repo_ids=frozenset({"repo-a"})
    )
    plan = plan_recompute(scope)

    assert plan.trigger is True
    assert plan.dispatch_daily is True
    assert plan.repo_ids == ("repo-a",)
    assert plan.fallback_org_wide_daily is False
    assert plan.skip_investment_no_scope is False
    assert plan.capped_days is False
    assert plan.capped_repos is False
    assert plan.day == "2026-06-26"
    assert plan.backfill_days == 2


def test_work_item_kinds_empty_repo_ids_falls_back_org_wide() -> None:
    scope = _scope(record_kinds=frozenset({"work_item.v1"}), repo_ids=frozenset())
    plan = plan_recompute(scope)

    assert plan.trigger is True
    assert plan.dispatch_daily is True
    assert plan.repo_ids == ()
    assert plan.fallback_org_wide_daily is True
    # D4: no team_ids either -> investment has nothing to scope onto.
    assert plan.skip_investment_no_scope is True


def test_team_kinds_only_no_daily_investment_team_ids_only() -> None:
    scope = _scope(
        record_kinds=frozenset({"identity.v1"}), team_ids=frozenset({"team-a"})
    )
    plan = plan_recompute(scope)

    assert plan.trigger is True
    assert plan.dispatch_daily is False
    assert plan.fallback_org_wide_daily is False
    assert plan.team_ids == ("team-a",)
    assert plan.skip_investment_no_scope is False


def test_repo_only_kind_not_applicable() -> None:
    scope = _scope(record_kinds=frozenset({"repository.v1"}))
    plan = plan_recompute(scope)

    assert plan.trigger is False
    assert plan.dispatch_daily is False
    assert plan.repo_ids == ()
    assert plan.team_ids == ()


def test_empty_record_kinds_not_applicable() -> None:
    plan = plan_recompute(_scope(record_kinds=frozenset()))
    assert plan.trigger is False


def test_mixed_kinds_repo_and_team_scope_single_investment_call() -> None:
    scope = _scope(
        record_kinds=frozenset({"pull_request.v1", "identity.v1"}),
        repo_ids=frozenset({"repo-a", "repo-b"}),
        team_ids=frozenset({"team-a"}),
    )
    plan = plan_recompute(scope)

    assert plan.dispatch_daily is True
    assert plan.repo_ids == ("repo-a", "repo-b")
    assert plan.team_ids == ("team-a",)
    assert plan.fallback_org_wide_daily is False
    assert plan.skip_investment_no_scope is False


def test_window_spanning_40_days_capped_to_14() -> None:
    scope = _scope(
        record_kinds=frozenset({"pull_request.v1"}),
        repo_ids=frozenset({"repo-a"}),
        window_start=datetime(2026, 5, 18, tzinfo=timezone.utc),
        window_end=datetime(2026, 6, 26, tzinfo=timezone.utc),
    )
    plan = plan_recompute(scope)

    assert plan.capped_days is True
    assert plan.backfill_days == 14
    assert plan.day == "2026-06-26"
    expected_from = datetime(2026, 6, 13, tzinfo=timezone.utc)
    assert plan.from_date == expected_from.isoformat()


def test_60_repo_ids_capped_to_25_stable_sorted() -> None:
    repo_ids = frozenset(f"repo-{i:03d}" for i in range(60))
    scope = _scope(record_kinds=frozenset({"pull_request.v1"}), repo_ids=repo_ids)
    plan = plan_recompute(scope)

    assert plan.capped_repos is True
    assert len(plan.repo_ids) == 25
    assert plan.repo_ids == tuple(sorted(repo_ids)[:25])


def test_git_only_empty_repo_ids_defensive_skip_investment_no_scope() -> None:
    """Structurally impossible in practice (PR/review/commit always carry a
    resolved repo_id) but the defensive skip path is asserted anyway per
    the brief's D4 hard-invariant test plan bullet."""
    scope = _scope(record_kinds=frozenset({"pull_request.v1"}), repo_ids=frozenset())
    plan = plan_recompute(scope)

    assert plan.dispatch_daily is True
    assert plan.repo_ids == ()
    # D8 fallback is work-item-only; git kinds never get it.
    assert plan.fallback_org_wide_daily is False
    assert plan.skip_investment_no_scope is True


def test_single_day_window_backfill_days_is_one() -> None:
    same_day = datetime(2026, 6, 26, 10, tzinfo=timezone.utc)
    scope = _scope(
        record_kinds=frozenset({"pull_request.v1"}),
        repo_ids=frozenset({"repo-a"}),
        window_start=same_day,
        window_end=same_day + timedelta(hours=2),
    )
    plan = plan_recompute(scope)

    assert plan.backfill_days == 1
    assert plan.capped_days is False


def test_missing_window_defaults_to_now() -> None:
    scope = _scope(
        record_kinds=frozenset({"pull_request.v1"}),
        repo_ids=frozenset({"repo-a"}),
        window_start=None,
        window_end=None,
    )
    plan = plan_recompute(scope)

    assert plan.trigger is True
    assert plan.day is not None
    assert plan.backfill_days == 1
