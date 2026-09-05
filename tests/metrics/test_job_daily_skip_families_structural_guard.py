"""CHAOS-3092 close condition 3: structural guard against a deleted family's
compute call reappearing in job_daily.py.

Chris's ruling (verbatim, twice, 2026-09-05): "work_item_attribution python
doesn't need a skip, it just needs to be deleted" / "once go is in main that
does the same thing, skip flags are pointless." The standing rule going
forward for CHAOS-3092: for every family NATIVE on main, DELETE its Python
compute call from job_daily.py -- never add or extend a skip_families gate.

DELETED_NATIVE_FAMILY_COMPUTE_FUNCTIONS below is the deletion ledger: one
entry per family whose job_daily.py compute call has already been removed
(CHAOS-5233 is the first entry, work_item_attribution). It is expected to
GROW, one deletion PR at a time, as CHAOS-3092's audit (CHAOS-5234) works
through the rest of the native family list -- this is NOT yet a blanket
"no native family may call its compute function from job_daily.py" check,
because most families have not been migrated from skip-gating to deletion
yet. Widen this set as each subsequent deletion PR lands; do not remove
entries once added.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JOB_DAILY_SOURCE = ROOT / "src" / "dev_health_ops" / "metrics" / "job_daily.py"

# family name -> the compute function job_daily.py must no longer reference
# at all (no import, no call) once that family's deletion PR has landed.
DELETED_NATIVE_FAMILY_COMPUTE_FUNCTIONS = {
    # CHAOS-5233: work_item_attribution's daily compute deleted from
    # job_daily.py -- the native Go executor (WorkItemAttributionExecutor,
    # #2246/CHAOS-5078) is the only writer of work_item_team_attributions
    # for a daily partition now. compute_work_item_team_attributions itself
    # is NOT deleted from the codebase (job_work_items.py's
    # run_work_items_sync_job still calls it for an unrelated full-backfill
    # sync job), only job_daily.py's own reference to it.
    "work_item_attribution": "compute_work_item_team_attributions",
    # CHAOS-5234: ai_governance's daily compute deleted from job_daily.py --
    # the native Go executor (AIGovernanceExecutor, CHAOS-4285) is the only
    # writer of ai_policy_events/ai_governance_coverage_daily for a daily
    # partition now. Unlike work_item_attribution, build_governance_rows_
    # for_day itself was ALSO deleted (from audit/ai_governance/loaders.py)
    # -- codegraph_explore + rg confirmed job_daily.py was its only real
    # caller.
    "ai_governance": "build_governance_rows_for_day",
}


def test_deleted_native_families_have_no_compute_reference_in_job_daily() -> None:
    assert JOB_DAILY_SOURCE.is_file(), f"missing source: {JOB_DAILY_SOURCE}"
    assert DELETED_NATIVE_FAMILY_COMPUTE_FUNCTIONS, (
        "the deletion ledger is empty -- CHAOS-5233 should have added the "
        "first entry; if this legitimately regressed to zero, this test's "
        "own docstring needs updating too"
    )

    source = JOB_DAILY_SOURCE.read_text(encoding="utf-8")
    # Code lines only -- job_daily.py's OWN explanatory comments deliberately
    # name the deleted function (to explain the deletion), and a naive
    # substring search over the whole file would flag those comments the
    # same way it would flag a real reference. A real reference is an
    # import or a call, never inside a `#...` comment.
    code_lines = [
        line for line in source.splitlines() if not line.strip().startswith("#")
    ]
    code_only = "\n".join(code_lines)
    still_referenced = sorted(
        f"{family} ({function})"
        for family, function in DELETED_NATIVE_FAMILY_COMPUTE_FUNCTIONS.items()
        if function in code_only
    )
    assert not still_referenced, (
        f"job_daily.py still references a compute function CHAOS-3092's "
        f"deletion ledger says was already removed: {still_referenced}. "
        "Either the deletion regressed (a re-import or a new call site "
        "crept back in) or this test's ledger is stale and the function "
        "genuinely has a new, deliberate caller in job_daily.py -- in that "
        "second case, remove the ledger entry and explain why in this "
        "file's docstring, do not just widen the check to tolerate it."
    )
