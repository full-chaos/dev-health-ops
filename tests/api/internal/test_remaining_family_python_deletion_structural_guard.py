"""CHAOS-3092/CHAOS-5234 close condition 3, `metrics.remaining.*` shape.

Chris's ruling (verbatim, twice, 2026-09-05): "work_item_attribution python
doesn't need a skip, it just needs to be deleted" / "once go is in main that
does the same thing, skip flags are pointless." For a `metrics.remaining.*`
family whose native Go executor already has NO Python fallback (checked at
``cmd/dev-health-worker/daily.go``'s own worker registration -- see
``feedback_no_straddle_allowed_python_edits_list.md``), the corresponding
Python compute orchestrator AND its dispatch entry in
``src/dev_health_ops/api/internal/worker_metrics.py``'s ``_REMAINING_RUNNERS``
must be DELETED, not skip-gated.

This is the ``metrics.remaining.*`` counterpart to
``tests/metrics/test_job_daily_skip_families_structural_guard.py``'s
``DELETED_NATIVE_FAMILY_COMPUTE_FUNCTIONS`` ledger, which covers the SAME
close condition for ``job_daily.py``-resident families. The mechanism differs
because the two dispatch shapes differ: a ``metrics.remaining.*`` family's
Python entry point is a dedicated CLI job module plus a
``worker_metrics.py`` bridge-handler dict entry, not a single function call
inside one monolithic daily-compute function.

DELETED_REMAINING_FAMILY_PYTHON_MODULES below is the deletion ledger: one
entry per family whose ``metrics.remaining.*`` Python orchestrator module has
already been removed (CHAOS-5244 is the first entry, release_impact;
CHAOS-4291 the second, complexity). It is expected to GROW, one deletion PR
at a time, as CHAOS-3092's audit (CHAOS-5234) works through the rest of the
native remaining-family list -- this is NOT yet a blanket "every native
remaining family must have its Python deleted" check, because most
remaining families (capacity, dora) have not been migrated from
skip-gating/no-fallback to full deletion yet. Widen this set as each
subsequent deletion PR lands; do not remove entries once added.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
WORKER_METRICS_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "api" / "internal" / "worker_metrics.py"
)

# family name -> the Python job module that must no longer exist on disk AT
# ALL once that family's deletion PR has landed (its CLI verb and its
# worker_metrics.py HTTP-bridge handler both lived in this one module).
DELETED_REMAINING_FAMILY_PYTHON_MODULES: dict[str, Path] = {
    # CHAOS-5244 (CHAOS-4296/CHAOS-3092): release_impact's daily compute
    # orchestrator (run_release_impact_job, _cmd_release_impact,
    # register_commands) deleted entirely -- the native Go executor
    # (ReleaseImpactExecutor, #2262) already has NO Python fallback in
    # daily.go's own worker registration (`if releaseImpactExecutor == nil {
    # continue }`), so there was nothing left for job_release_impact.py to
    # serve. release_impact.py's own `_compute_day` per-day helper is NOT
    # deleted -- `src/dev_health_ops/fixtures/runner.py` imports it directly
    # (as `_compute_release_impact_day`) for local/CI golden-fixture
    # generation, a real, live, non-production-job caller.
    "release_impact": ROOT
    / "src"
    / "dev_health_ops"
    / "metrics"
    / "job_release_impact.py",
    # CHAOS-4291: complexity's native ComplexityExecutor (this same PR) has
    # no Python fallback either -- daily.go's KindRemainingComplexity case
    # dispatches to it directly, no `compatibility` bridge call. Unlike
    # release_impact, job_complexity_db.py had THREE live non-production
    # callers before this PR (fixtures/runner.py's fixture seeding,
    # ci/local_validate.sh's own gate-tooling readback, and its own CLI
    # registration that gate stage called) -- chris's 2026-09-05 standing
    # ruling (superseding the earlier CHAOS-5244/CHAOS-5250 "keep it for
    # fixtures" precedent) is that fixture/gate tooling does not keep
    # Python compute alive either: fixtures/runner.py now seeds
    # file_complexity_snapshots/repo_complexity_daily from the frozen
    # golden JSON (a plain load, no compute) and local_validate.sh's
    # readback stage dropped complexity entirely. The whole module is
    # gone, not just its dispatch entry.
    "complexity": ROOT / "src" / "dev_health_ops" / "metrics" / "job_complexity_db.py",
}


def test_deleted_remaining_family_python_modules_do_not_exist() -> None:
    assert DELETED_REMAINING_FAMILY_PYTHON_MODULES, (
        "the deletion ledger is empty -- CHAOS-5244 should have added the "
        "first entry; if this legitimately regressed to zero, this test's "
        "own docstring needs updating too"
    )

    resurrected = sorted(
        f"{family} ({module_path.relative_to(ROOT)})"
        for family, module_path in DELETED_REMAINING_FAMILY_PYTHON_MODULES.items()
        if module_path.is_file()
    )
    assert not resurrected, (
        f"a Python module CHAOS-3092's remaining-family deletion ledger says "
        f"was already removed exists again on disk: {resurrected}. Either "
        "the deletion regressed (the file was re-added) or this test's "
        "ledger is stale and the module genuinely has a new, deliberate "
        "reason to exist -- in that second case, remove the ledger entry "
        "and explain why in this file's docstring, do not just widen the "
        "check to tolerate it."
    )


def test_deleted_remaining_family_python_modules_have_no_dispatch_entry() -> None:
    """The corresponding worker_metrics.py `_REMAINING_RUNNERS` dispatch key
    must also be gone -- checking only file-existence would miss a
    resurrected inline handler that never got its own module back."""
    assert WORKER_METRICS_SOURCE.is_file(), f"missing source: {WORKER_METRICS_SOURCE}"
    source = WORKER_METRICS_SOURCE.read_text(encoding="utf-8")

    still_dispatched = sorted(
        family
        for family in DELETED_REMAINING_FAMILY_PYTHON_MODULES
        if f'"{family}": _run_{family}' in source
    )
    assert not still_dispatched, (
        f"worker_metrics.py's _REMAINING_RUNNERS still dispatches a family "
        f"CHAOS-3092's deletion ledger says was already removed: "
        f"{still_dispatched}. This is a resurrection, not a legitimate new "
        "caller -- a deleted family's Python bridge handler has no reason "
        "to come back once its native Go executor has no fallback."
    )
