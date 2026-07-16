import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "docs-guards.yml"


def _load_workflow() -> dict[str, object]:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _aggregate_script() -> str:
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    aggregate_job = jobs["docs-guards"]
    assert isinstance(aggregate_job, dict)
    steps = aggregate_job["steps"]
    assert isinstance(steps, list)
    run_script = steps[0]["run"]
    assert isinstance(run_script, str)
    return run_script


def test_aggregate_job_depends_on_both_changes_and_docs_guards_job() -> None:
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    aggregate_job = jobs["docs-guards"]
    assert isinstance(aggregate_job, dict)

    assert set(aggregate_job["needs"]) == {"changes", "docs-guards-job"}


@pytest.mark.parametrize(
    ("changes_result", "docs_guards_result", "expected_exit_code"),
    [
        ("success", "success", 0),
        ("success", "skipped", 0),
        ("success", "failure", 1),
        ("success", "cancelled", 1),
        ("failure", "skipped", 1),
        ("cancelled", "skipped", 1),
    ],
)
def test_aggregate_bash_script_exit_code(
    changes_result: str, docs_guards_result: str, expected_exit_code: int
) -> None:
    """Given the aggregate job's real run script, when the upstream changes job
    fails and docs-guards-job is consequently skipped, then the aggregate must
    fail rather than report a false pass ("skipped required work")."""
    script = _aggregate_script()

    result = subprocess.run(
        ["bash", "-c", script],
        check=False,
        env={
            "PATH": "/usr/bin:/bin",
            "CHANGES_RESULT": changes_result,
            "DOCS_GUARDS_RESULT": docs_guards_result,
        },
        capture_output=True,
        text=True,
    )

    assert result.returncode == expected_exit_code, result.stdout + result.stderr
