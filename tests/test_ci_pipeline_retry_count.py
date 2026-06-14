"""Unit tests proving retry_count is wired through the BASE sync_cicd path.

Regression coverage for CHAOS-2380: testops_pipeline_metrics_daily.rerun_rate
was flat 0% because ci_pipeline_runs.retry_count was only populated by the
extended TestOps path (gated behind the non-default sync_tests target). These
tests pin the seam on the base path:

  GitHub run_attempt  -> _fetch_github_workflow_runs_sync
                      -> build_ci_pipeline_run(retry_count=...)
                      -> CiPipelineRun.retry_count

so real orgs syncing only sync_cicd get a non-zero rerun_rate.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

from dev_health_ops.processors.base_git import build_ci_pipeline_run
from dev_health_ops.processors.github import _fetch_github_workflow_runs_sync
from dev_health_ops.processors.gitlab import _fetch_gitlab_pipelines_sync

_STARTED = datetime(2023, 1, 1, 0, 1, 0, tzinfo=timezone.utc)


def _gh_run(run_id: str, *, run_attempt: object = "unset") -> SimpleNamespace:
    """Build a minimal GitHub WorkflowRun-like object.

    Uses SimpleNamespace (not Mock) so an omitted run_attempt is genuinely
    absent rather than auto-vivified into a truthy Mock.
    """
    fields: dict[str, object] = {
        "id": run_id,
        "conclusion": "success",
        "status": "completed",
        "created_at": datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "run_started_at": _STARTED,
        "updated_at": datetime(2023, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
    }
    if run_attempt != "unset":
        fields["run_attempt"] = run_attempt
    return SimpleNamespace(**fields)


def test_build_ci_pipeline_run_carries_retry_count():
    """build_ci_pipeline_run threads retry_count onto the model (default 0)."""
    run = build_ci_pipeline_run(
        repo_id=None,
        run_id="1",
        status="success",
        queued_at=None,
        started_at=_STARTED,
        finished_at=None,
        retry_count=2,
    )
    assert run.retry_count == 2

    defaulted = build_ci_pipeline_run(
        repo_id=None,
        run_id="2",
        status="success",
        queued_at=None,
        started_at=_STARTED,
        finished_at=None,
    )
    assert defaulted.retry_count == 0


def test_github_workflow_run_maps_run_attempt_to_retry_count():
    """run_attempt=3 -> retry_count=2 on the base sync_cicd path."""
    gh_repo = SimpleNamespace(get_workflow_runs=lambda: [_gh_run("100", run_attempt=3)])

    runs = _fetch_github_workflow_runs_sync(
        gh_repo, repo_id=None, max_runs=10, since=None
    )

    assert len(runs) == 1
    assert runs[0].run_id == "100"
    assert runs[0].retry_count == 2


def test_github_workflow_run_first_attempt_is_zero_retries():
    """run_attempt=1 (first, non-retried) -> retry_count=0."""
    gh_repo = SimpleNamespace(get_workflow_runs=lambda: [_gh_run("101", run_attempt=1)])

    runs = _fetch_github_workflow_runs_sync(
        gh_repo, repo_id=None, max_runs=10, since=None
    )

    assert runs[0].retry_count == 0


def test_github_workflow_run_absent_run_attempt_defaults_to_zero():
    """Missing run_attempt -> retry_count=0 (no crash, no false positives)."""
    gh_repo = SimpleNamespace(get_workflow_runs=lambda: [_gh_run("102")])

    runs = _fetch_github_workflow_runs_sync(
        gh_repo, repo_id=None, max_runs=10, since=None
    )

    assert runs[0].retry_count == 0


def test_gitlab_pipeline_retry_count_defaults_to_zero():
    """GitLab pipelines have no clean attempt counter -> retry_count=0."""
    from unittest.mock import Mock

    pipeline = Mock()
    pipeline.id = 1
    pipeline.status = "success"
    pipeline.created_at = "2023-01-01T00:00:00Z"
    pipeline.started_at = "2023-01-01T00:01:00Z"
    pipeline.finished_at = "2023-01-01T00:05:00Z"

    gl_project = Mock()
    gl_project.pipelines.list.return_value = [pipeline]

    pipelines = _fetch_gitlab_pipelines_sync(
        gl_project, repo_id=None, max_pipelines=10, since=None
    )

    assert len(pipelines) == 1
    assert pipelines[0].retry_count == 0
