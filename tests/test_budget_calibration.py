"""Actual-vs-estimated calibration join (CHAOS-2759).

Pure-function tests for ``workers/sync_units.py``'s
``_join_budget_estimates_with_actuals`` / ``_attach_budget_comparison``: the
join between a unit's run-time budget audit (``estimate_provider_budget``
output shape, see ``sync/budget.py`` / ``sync/budget_types.py``) and
CHAOS-2754's normalized ``provider_usage`` actuals (``providers/usage.py``).
No DB, no Celery -- these operate purely on the already-serialized dict
shapes both sides emit, mirroring ``BudgetEstimate.to_dict()`` and
``UsageRecorder.drain()``.

Integration-level coverage (full ``run_sync_unit`` success path, the
underestimation warning log end-to-end, and the result-stamp/admin-passthrough
contract) lives in ``tests/test_sync_units.py``.
"""

from __future__ import annotations

import copy
import logging
from datetime import datetime, timezone

from dev_health_ops.workers.sync_units import (
    _attach_budget_comparison,
    _comparison_budget_key,
    _join_budget_estimates_with_actuals,
)


def _estimate(
    *,
    route_family: str,
    dimension: str,
    estimated_units: int,
    provider: str = "github",
    org_id: str = "org-1",
    host: str = "api.github.com",
    credential_fingerprint: str = "fp-1",
    confidence: str = "medium",
) -> dict:
    """Shape of ``BudgetEstimate.to_dict()`` (sync/budget_types.py)."""

    return {
        "bucket": {
            "provider": provider,
            "org_id": org_id,
            "host": host,
            "credential_fingerprint": credential_fingerprint,
            "dimension": dimension,
        },
        "estimated_units": estimated_units,
        "confidence": confidence,
        "route_family": route_family,
        "notes": [],
    }


def _actual(
    *,
    route_family: str,
    dimension: str,
    request_count: int,
    transport: str = "rest",
) -> dict:
    """Shape of one ``UsageRecorder.drain()`` entry (providers/usage.py)."""

    return {
        "transport": transport,
        "route_family": route_family,
        "dimension": dimension,
        "request_count": request_count,
        "example_operation": "GET /example",
    }


_OVERFLOW_MARKER = {
    "transport": "summary",
    "route_family": "overflow",
    "dimension": "summary",
    "dropped_operation_count": 3,
}


def test_budget_comparison_attached_per_route_family():
    budget_audit = [
        _estimate(route_family="git", dimension="rest_core", estimated_units=2),
        _estimate(
            route_family="pr_social", dimension="graphql_cost", estimated_units=4
        ),
    ]
    provider_usage = [
        _actual(route_family="git", dimension="rest_core", request_count=5),
        _actual(
            route_family="pr_social",
            dimension="graphql_cost",
            request_count=1,
            transport="graphql",
        ),
    ]

    comparisons = _join_budget_estimates_with_actuals(budget_audit, provider_usage)

    by_family = {row["route_family"]: row for row in comparisons}
    assert set(by_family) == {"git", "pr_social"}

    git_row = by_family["git"]
    assert git_row["dimension"] == "rest_core"
    assert git_row["estimated_units"] == 2
    assert git_row["actual_requests"] == 5
    assert git_row["ratio"] == 2.5
    assert git_row["underestimated"] is True
    assert git_row["incomplete"] is False
    assert git_row["budget_key"] == _comparison_budget_key(
        git_row["bucket"], route_family="git"
    )

    social_row = by_family["pr_social"]
    assert social_row["estimated_units"] == 4
    assert social_row["actual_requests"] == 1
    assert social_row["ratio"] == 0.25
    assert social_row["underestimated"] is False
    assert social_row["incomplete"] is False


def test_no_actuals_no_false_signal():
    """A unit with estimates but no drained actuals for a family (code
    datasets, LaunchDarkly's unwired families) must produce NO row -- never a
    fabricated 100% over-estimation."""

    budget_audit = [
        _estimate(route_family="git", dimension="rest_core", estimated_units=2),
    ]

    assert _join_budget_estimates_with_actuals(budget_audit, []) == []
    # Actuals present, but only for an unrelated family -- still no row for
    # "git" (no cross-family fallback / guessing).
    unrelated_actuals = [
        _actual(route_family="flags", dimension="rest_core", request_count=9)
    ]
    assert _join_budget_estimates_with_actuals(budget_audit, unrelated_actuals) == []


def test_overflow_marks_comparison_incomplete():
    """The recorder's 50-key overflow marker means dropped operations could
    belong to ANY route_family (the recorder never learns which family a
    dropped operation would have joined), so every row for the unit is
    incomplete and a ratio <= 1 must not be read as a confirmed
    over-estimation."""

    budget_audit = [
        _estimate(route_family="git", dimension="rest_core", estimated_units=10),
    ]
    provider_usage = [
        _actual(route_family="git", dimension="rest_core", request_count=3),
        _OVERFLOW_MARKER,
    ]

    comparisons = _join_budget_estimates_with_actuals(budget_audit, provider_usage)

    assert len(comparisons) == 1
    row = comparisons[0]
    assert row["actual_requests"] == 3
    assert row["estimated_units"] == 10
    assert row["incomplete"] is True
    # Looks over-estimated on the visible numbers, but callers must treat that
    # as unproven while incomplete -- the flag itself is that signal.
    assert row["underestimated"] is False


def test_overflow_does_not_suppress_a_real_underestimation_signal():
    """Even when capped, a visible actual that already exceeds the estimate
    is still a valid underestimation signal: the true (uncapped) actual can
    only be larger, never smaller."""

    budget_audit = [
        _estimate(route_family="git", dimension="rest_core", estimated_units=2),
    ]
    provider_usage = [
        _actual(route_family="git", dimension="rest_core", request_count=5),
        _OVERFLOW_MARKER,
    ]

    comparisons = _join_budget_estimates_with_actuals(budget_audit, provider_usage)

    assert len(comparisons) == 1
    row = comparisons[0]
    assert row["underestimated"] is True
    assert row["incomplete"] is True


def test_estimates_never_mutated_by_comparison():
    """No auto-tuning regression guard: joining estimates to actuals must
    never mutate either input (CHAOS-2759 is OBSERVE-ONLY)."""

    budget_audit = [
        _estimate(route_family="git", dimension="rest_core", estimated_units=2),
    ]
    provider_usage = [
        _actual(route_family="git", dimension="rest_core", request_count=5),
    ]
    budget_audit_before = copy.deepcopy(budget_audit)
    provider_usage_before = copy.deepcopy(provider_usage)

    comparisons = _join_budget_estimates_with_actuals(budget_audit, provider_usage)

    assert budget_audit == budget_audit_before
    assert provider_usage == provider_usage_before
    assert comparisons  # sanity: the join actually produced a row

    # The row's bucket is a defensive copy, not the same object -- mutating
    # the comparison output must never leak back into the estimate it was
    # built from.
    comparisons[0]["bucket"]["provider"] = "mutated"
    assert budget_audit[0]["bucket"]["provider"] == "github"


def test_attach_budget_comparison_is_noop_without_budget_audit():
    result = {
        "ok": True,
        "observations": {
            "provider_usage": [
                _actual(route_family="git", dimension="rest_core", request_count=5)
            ]
        },
    }

    attached = _attach_budget_comparison(
        result, None, log_ctx={}, computed_at=datetime.now(timezone.utc)
    )

    assert attached is result
    assert "budget_comparison" not in attached["observations"]


def test_attach_budget_comparison_is_noop_without_provider_usage():
    budget_audit = [
        _estimate(route_family="git", dimension="rest_core", estimated_units=2),
    ]
    result = {"ok": True, "observations": {}}

    attached = _attach_budget_comparison(
        result, budget_audit, log_ctx={}, computed_at=datetime.now(timezone.utc)
    )

    assert attached is result
    assert "budget_comparison" not in attached["observations"]


def test_attach_budget_comparison_logs_underestimated_warning(caplog):
    budget_audit = [
        _estimate(route_family="git", dimension="rest_core", estimated_units=2),
    ]
    result = {
        "ok": True,
        "observations": {
            "provider_usage": [
                _actual(route_family="git", dimension="rest_core", request_count=9)
            ]
        },
    }
    log_ctx = {
        "sync_run_id": "run-1",
        "unit_id": "unit-1",
        "source_id": "src-1",
        "dataset_key": "commits",
        "provider": "github",
        "cost_class": "medium",
    }
    computed_at = datetime.now(timezone.utc)

    with caplog.at_level(logging.WARNING, logger="dev_health_ops.workers.sync_units"):
        attached = _attach_budget_comparison(
            result, budget_audit, log_ctx=log_ctx, computed_at=computed_at
        )

    comparison = attached["observations"]["budget_comparison"][0]
    assert comparison["underestimated"] is True
    assert (
        attached["observations"]["budget_comparison_computed_at"]
        == computed_at.isoformat()
    )

    warning_records = [
        r for r in caplog.records if r.message == "run_sync_unit.budget_underestimated"
    ]
    assert len(warning_records) == 1
    record = warning_records[0]
    # Same structured-field vocabulary BudgetGuard's admission logs use
    # (bucket, budget_key, estimated_units, route_family -- budget_guard.py
    # `_observe_estimate`), so operators can correlate a calibration warning
    # with the run's actual admission decision.
    assert record.route_family == "git"
    assert record.dimension == "rest_core"
    assert record.estimated_units == 2
    assert record.actual_requests == 9
    assert record.budget_key == "github:org-1:api.github.com:fp-1:rest_core:git"
    assert record.bucket["dimension"] == "rest_core"
    assert record.sync_run_id == "run-1"
    assert record.unit_id == "unit-1"


def test_attach_budget_comparison_does_not_log_for_over_estimated_rows(caplog):
    budget_audit = [
        _estimate(route_family="git", dimension="rest_core", estimated_units=10),
    ]
    result = {
        "ok": True,
        "observations": {
            "provider_usage": [
                _actual(route_family="git", dimension="rest_core", request_count=1)
            ]
        },
    }

    with caplog.at_level(logging.WARNING, logger="dev_health_ops.workers.sync_units"):
        attached = _attach_budget_comparison(
            result,
            budget_audit,
            log_ctx={},
            computed_at=datetime.now(timezone.utc),
        )

    assert attached["observations"]["budget_comparison"][0]["underestimated"] is False
    assert not [
        r for r in caplog.records if r.message == "run_sync_unit.budget_underestimated"
    ]
