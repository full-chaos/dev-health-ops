"""Regression guards for the reviewed CHAOS-3128 Beat retirement."""

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SOURCE_ROOT = _REPO_ROOT / "src" / "dev_health_ops"


def _call_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _scheduled_job_writer_types() -> list[tuple[Path, int, str | None]]:
    writers: list[tuple[Path, int, str | None]] = []
    for path in sorted(_SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if (
                not isinstance(node, ast.Call)
                or _call_name(node.func) != "ScheduledJob"
            ):
                continue
            job_type = next(
                (kw.value for kw in node.keywords if kw.arg == "job_type"), None
            )
            if not isinstance(job_type, ast.Constant) or not isinstance(
                job_type.value, str
            ):
                writers.append((path.relative_to(_REPO_ROOT), node.lineno, None))
                continue
            writers.append((path.relative_to(_REPO_ROOT), node.lineno, job_type.value))
    return writers


def test_production_scheduled_job_writers_cannot_reintroduce_metrics_sweeps():
    """The deleted sweep had no rows to sweep; new writers need a new review."""
    writers = _scheduled_job_writer_types()

    assert writers
    assert {job_type for _, _, job_type in writers} == {"report", "sync"}


# CHAOS-3093 (2026-09-09): test_retired_dispatcher_has_no_runtime_exports_or_
# beat_entry tested workers/metrics_daily.py, deleted outright with this
# cleanup (its own run_daily_metrics task had zero Celery consumers since
# 2026-08-19; external_ingest/recompute.py's fan-out already dispatches it by
# task-name string via celery_app.send_task, no import needed). The module's
# absence is now pinned by tests/workers/test_celery_dead_code_contract.py's
# _DELETED_MODULES. dispatch_scheduled_metrics/dispatch-scheduled-metrics
# (the CHAOS-3128 retirement this file's docstring is about -- a separate,
# earlier ticket than CHAOS-4026/CHAOS-3093) was never in metrics_daily.py
# to begin with; its own absence stays pinned by
# contracts/jobs/v1/transitional-inventory.json's retired_beat_entries +
# ci/check_transitional_inventory.py, unaffected by this change.
