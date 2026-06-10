"""Metric-coherence validation for synthetic fixture data.

Mirrors *Rule 1* from ``web/docs/metric-coherence.md``:

    Figures on a surface must reconcile under their stated denominator,
    or the relationship must be explicitly explained.

Every ``check_*`` function inspects a collection of generated rows and
raises ``CoherenceError`` if it finds a violation.  The top-level
``validate_all`` helper calls every registered check in one pass and
collects every violation before raising, giving callers a complete
picture rather than stopping at the first problem.

The checks below define the *ops-side coherence contract*; see
``docs/fixtures/metric-coherence.md`` for the full rationale.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------


class CoherenceError(ValueError):
    """Raised when one or more fixture rows violate a coherence invariant."""

    def __init__(self, violations: list[str]) -> None:
        self.violations = violations
        joined = "\n".join(f"  • {v}" for v in violations)
        super().__init__(f"Fixture coherence violation(s):\n{joined}")


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def check_coverage_snapshots(rows: list[dict[str, Any]]) -> list[str]:
    """Invariants for coverage snapshot rows.

    1. ``branch_coverage_pct ≤ line_coverage_pct``
       Branch coverage is a strict subset of line coverage; a branch can
       only be covered if its containing line is covered.

    2. ``lines_covered ≤ lines_total``
       Covered count cannot exceed the total.

    3. ``branches_covered ≤ branches_total``
       Same constraint for branches.
    """
    violations: list[str] = []
    for i, row in enumerate(rows):
        line_pct = row.get("line_coverage_pct", 0.0)
        branch_pct = row.get("branch_coverage_pct", 0.0)
        lines_total = row.get("lines_total", 0)
        lines_covered = row.get("lines_covered", 0)
        branches_total = row.get("branches_total", 0)
        branches_covered = row.get("branches_covered", 0)
        run_id = row.get("run_id", f"row[{i}]")

        if branch_pct > line_pct:
            violations.append(
                f"coverage/{run_id}: branch_coverage_pct ({branch_pct}) "
                f"> line_coverage_pct ({line_pct}) — "
                "branch coverage is a subset of line coverage"
            )
        if lines_total > 0 and lines_covered > lines_total:
            violations.append(
                f"coverage/{run_id}: lines_covered ({lines_covered}) "
                f"> lines_total ({lines_total})"
            )
        if branches_total > 0 and branches_covered > branches_total:
            violations.append(
                f"coverage/{run_id}: branches_covered ({branches_covered}) "
                f"> branches_total ({branches_total})"
            )
    return violations


def check_pipeline_runs(rows: list[dict[str, Any]]) -> list[str]:
    """Invariants for CI pipeline run rows.

    1. ``status`` is a known terminal or in-flight value.
       The denominator Rule 1 note from ``web/docs/metric-coherence.md``
       applies here: ``success_rate + failure_rate`` need not equal 100 %
       because some runs are ``cancelled`` or still ``running``.  The
       ops-side contract is that *each run is counted in exactly one status
       bucket* — no run contributes to two categories simultaneously — so
       the status distribution naturally reconciles without manual clamping.

    2. ``queued_at ≤ started_at ≤ finished_at`` (when all three are present).
       Timeline order is required for duration and queue-wait calculations
       to be non-negative.
    """
    _VALID_STATUSES = frozenset(
        {
            "success",
            "failure",
            "failed",
            "cancelled",
            "canceled",
            "timeout",
            "running",
            "queued",
            "skipped",
        }
    )
    violations: list[str] = []
    for i, row in enumerate(rows):
        run_id = row.get("run_id", f"row[{i}]")
        status = row.get("status")

        if status is not None and str(status).lower() not in _VALID_STATUSES:
            violations.append(
                f"pipeline_run/{run_id}: unknown status '{status}'; "
                f"expected one of {sorted(_VALID_STATUSES)}"
            )

        queued_at = row.get("queued_at")
        started_at = row.get("started_at")
        finished_at = row.get("finished_at")

        if queued_at and started_at and started_at < queued_at:
            violations.append(
                f"pipeline_run/{run_id}: started_at ({started_at}) "
                f"< queued_at ({queued_at})"
            )
        if started_at and finished_at and finished_at < started_at:
            violations.append(
                f"pipeline_run/{run_id}: finished_at ({finished_at}) "
                f"< started_at ({started_at})"
            )
    return violations


def check_test_suite_results(rows: list[dict[str, Any]]) -> list[str]:
    """Invariants for test suite result rows.

    ``passed_count + failed_count + skipped_count + error_count ≤ total_count``

    The sub-counts are shares of the total.  They will equal *exactly*
    ``total_count`` in fixture-generated data (the generator uses sequential
    slot allocation so passed absorbs all remaining capacity).  They may be
    *less than* total_count in real-world data where some tests are pending
    or in an unrecognised state.

    **Quarantined count:** ``quarantined_count`` is a separate *annotation*
    on tests that may overlap with any status bucket.  A quarantined test
    is still run and still falls into passed / failed / skipped / error; its
    quarantine flag only suppresses the result from the build health signal.
    Quarantined count therefore does NOT participate in the denominator check
    — it is not a fifth exclusive slot.
    """
    violations: list[str] = []
    for i, row in enumerate(rows):
        total = row.get("total_count", 0)
        passed = row.get("passed_count", 0)
        failed = row.get("failed_count", 0)
        skipped = row.get("skipped_count", 0)
        errors = row.get("error_count", 0)
        suite_id = row.get("suite_id", f"row[{i}]")

        sub_sum = passed + failed + skipped + errors
        if sub_sum > total:
            violations.append(
                f"test_suite/{suite_id}: "
                f"passed({passed}) + failed({failed}) + skipped({skipped}) "
                f"+ error({errors}) = {sub_sum} > total_count({total})"
            )
    return violations


def check_work_item_metrics(rows: list[dict[str, Any]]) -> list[str]:
    """Invariants for work_item_metrics_daily rows.

    1. ``items_completed_unassigned ≤ items_completed``
       Unassigned completions are a subset of all completions.

    2. ``items_started_unassigned ≤ items_started``
       Unassigned starts are a subset of all starts.

    3. ``wip_unassigned_end_of_day ≤ wip_count_end_of_day``
       Unassigned WIP is a subset of total WIP.

    4. ``cycle_time_p50_hours ≤ cycle_time_p90_hours``
       Percentiles must be non-decreasing.

    5. ``lead_time_p50_hours ≤ lead_time_p90_hours``
       Percentiles must be non-decreasing.

    6. ``cycle_time_p50_hours ≤ lead_time_p50_hours``
       Lead time = queue + cycle time, so lead time ≥ cycle time.

    7. ``wip_age_p50_hours ≤ wip_age_p90_hours``
       Percentiles must be non-decreasing.
    """
    violations: list[str] = []
    for i, row in enumerate(rows):
        label = f"{row.get('work_scope_id', '?')}@{row.get('day', '?')}/{row.get('team_id', f'row[{i}]')}"

        completed = row.get("items_completed", 0) or 0
        completed_unassigned = row.get("items_completed_unassigned", 0) or 0
        started = row.get("items_started", 0) or 0
        started_unassigned = row.get("items_started_unassigned", 0) or 0
        wip = row.get("wip_count_end_of_day", 0) or 0
        wip_unassigned = row.get("wip_unassigned_end_of_day", 0) or 0

        ct_p50 = row.get("cycle_time_p50_hours") or 0.0
        ct_p90 = row.get("cycle_time_p90_hours") or 0.0
        lt_p50 = row.get("lead_time_p50_hours") or 0.0
        lt_p90 = row.get("lead_time_p90_hours") or 0.0
        wip_age_p50 = row.get("wip_age_p50_hours") or 0.0
        wip_age_p90 = row.get("wip_age_p90_hours") or 0.0

        if completed_unassigned > completed:
            violations.append(
                f"work_item_metrics/{label}: "
                f"items_completed_unassigned({completed_unassigned}) "
                f"> items_completed({completed})"
            )
        if started_unassigned > started:
            violations.append(
                f"work_item_metrics/{label}: "
                f"items_started_unassigned({started_unassigned}) "
                f"> items_started({started})"
            )
        if wip_unassigned > wip:
            violations.append(
                f"work_item_metrics/{label}: "
                f"wip_unassigned_end_of_day({wip_unassigned}) "
                f"> wip_count_end_of_day({wip})"
            )
        if ct_p90 < ct_p50:
            violations.append(
                f"work_item_metrics/{label}: "
                f"cycle_time_p90({ct_p90}) < cycle_time_p50({ct_p50})"
            )
        if lt_p90 < lt_p50:
            violations.append(
                f"work_item_metrics/{label}: "
                f"lead_time_p90({lt_p90}) < lead_time_p50({lt_p50})"
            )
        if lt_p50 < ct_p50:
            violations.append(
                f"work_item_metrics/{label}: "
                f"lead_time_p50({lt_p50}) < cycle_time_p50({ct_p50}) — "
                "lead time must be ≥ cycle time (lead = queue + cycle)"
            )
        if wip_age_p90 < wip_age_p50:
            violations.append(
                f"work_item_metrics/{label}: "
                f"wip_age_p90({wip_age_p90}) < wip_age_p50({wip_age_p50})"
            )
    return violations


def check_commit_stats(rows: list[dict[str, Any]]) -> list[str]:
    """Invariants for commit stat rows.

    ``deletions ≤ additions``
    A commit stat records what was added and removed in one file.
    Deletions cannot exceed additions within a single diff hunk
    (measured as absolute line counts, not net change).
    """
    violations: list[str] = []
    for i, row in enumerate(rows):
        additions = row.get("additions", 0) or 0
        deletions = row.get("deletions", 0) or 0
        commit_hash = row.get("commit_hash", f"row[{i}]")
        file_path = row.get("file_path", "?")

        if deletions > additions:
            violations.append(
                f"commit_stat/{commit_hash}/{file_path}: "
                f"deletions({deletions}) > additions({additions})"
            )
    return violations


# ---------------------------------------------------------------------------
# Registry and top-level validator
# ---------------------------------------------------------------------------

#: Map of domain name → check function for ``validate_all``.
_CHECKS: dict[str, Any] = {
    "pipeline_runs": check_pipeline_runs,
    "coverage_snapshots": check_coverage_snapshots,
    "test_suite_results": check_test_suite_results,
    "work_item_metrics": check_work_item_metrics,
    "commit_stats": check_commit_stats,
}


@dataclass
class FixtureBundle:
    """A typed container for fixture row collections passed to ``validate_all``.

    Pass only the collections you want to validate; ``None`` values are
    skipped silently (useful when a caller does not generate every domain).
    """

    pipeline_runs: list[dict[str, Any]] | None = None
    coverage_snapshots: list[dict[str, Any]] | None = None
    test_suite_results: list[dict[str, Any]] | None = None
    work_item_metrics: list[dict[str, Any]] | None = None
    commit_stats: list[dict[str, Any]] | None = None


def validate_all(bundle: FixtureBundle) -> None:
    """Run all registered coherence checks against *bundle*.

    Collects every violation across all domains before raising so callers
    get the full picture in one pass.

    Raises:
        CoherenceError: if any invariant is violated.
    """
    all_violations: list[str] = []

    for domain, check_fn in _CHECKS.items():
        collection = getattr(bundle, domain, None)
        if collection is None:
            continue
        violations = check_fn(collection)
        all_violations.extend(violations)

    if all_violations:
        raise CoherenceError(all_violations)
