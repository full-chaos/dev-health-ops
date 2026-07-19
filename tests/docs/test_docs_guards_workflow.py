import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "docs-guards.yml"


def _load_workflow() -> dict[str, object]:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _docs_steps() -> list[dict[str, object]]:
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    docs_guards_job = jobs["docs-guards-job"]
    assert isinstance(docs_guards_job, dict)
    steps = docs_guards_job["steps"]
    assert isinstance(steps, list)
    return [step for step in steps if isinstance(step, dict)]


def _step_by_name(name: str) -> dict[str, object]:
    return next(step for step in _docs_steps() if step.get("name") == name)


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


def test_docs_guards_has_no_node_or_playwright_runtime() -> None:
    uses = [str(step.get("uses", "")) for step in _docs_steps()]
    run_scripts = "\n".join(str(step.get("run", "")) for step in _docs_steps())

    assert not any(value.startswith("actions/setup-node@") for value in uses)
    assert "docs-qa" not in run_scripts
    assert "playwright" not in run_scripts.lower()
    assert "pnpm" not in run_scripts.lower()


def test_docs_guards_runs_the_reader_critical_checks() -> None:
    names = {str(step.get("name", "")) for step in _docs_steps()}

    assert {
        "Validate publication inventory, IA placement, redirects, and source links",
        "Strict candidate build",
        "Check rendered internal links, anchors, and assets",
        "Check task-based search acceptance",
        "Check structural accessibility invariants",
        "Check objective candidate facts",
    }.issubset(names)


def test_streamed_docs_checks_propagate_checker_failures() -> None:
    streamed_steps = {
        "Validate publication inventory, IA placement, redirects, and source links",
        "Strict candidate build",
        "Check rendered internal links, anchors, and assets",
        "Check task-based search acceptance",
        "Check structural accessibility invariants",
        "Check objective candidate facts",
    }

    for name in streamed_steps:
        run_script = _step_by_name(name).get("run")
        assert isinstance(run_script, str)
        assert "set -o pipefail" in run_script
        assert "| tee " in run_script


def test_docs_guards_does_not_run_external_network_or_visual_contracts() -> None:
    run_scripts = "\n".join(str(step.get("run", "")) for step in _docs_steps()).lower()

    assert "check_external_links.py" not in run_scripts
    assert "screenshot" not in run_scripts
    assert "visual regression" not in run_scripts
    assert "exact prose" not in run_scripts


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
    """The required aggregate check must not turn failed upstream work into a pass."""
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
