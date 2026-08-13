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


def test_retired_dispatcher_has_no_runtime_exports_or_beat_entry():
    from dev_health_ops.workers import metrics_daily, metrics_tasks, tasks
    from dev_health_ops.workers.config import beat_schedule

    assert not hasattr(metrics_daily, "dispatch_scheduled_metrics")
    assert "dispatch_scheduled_metrics" not in metrics_tasks.__all__
    assert "dispatch_scheduled_metrics" not in tasks.__all__
    assert "dispatch-scheduled-metrics" not in beat_schedule
