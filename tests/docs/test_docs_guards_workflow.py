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


def test_docs_guards_pins_setup_node_to_the_v4_0_4_commit() -> None:
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    docs_guards_job = jobs["docs-guards-job"]
    assert isinstance(docs_guards_job, dict)
    steps = docs_guards_job["steps"]
    assert isinstance(steps, list)

    setup_node_uses = [
        step["uses"]
        for step in steps
        if isinstance(step, dict)
        and step.get("uses", "").startswith("actions/setup-node@")
    ]

    assert setup_node_uses == [
        "actions/setup-node@0a44ba7841725637a19e28fa30b79a866c81b0a6"
    ]


def test_docs_guards_installs_the_project_pinned_pnpm_version() -> None:
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    docs_guards_job = jobs["docs-guards-job"]
    assert isinstance(docs_guards_job, dict)
    steps = docs_guards_job["steps"]
    assert isinstance(steps, list)

    install_step = next(
        step
        for step in steps
        if isinstance(step, dict)
        and step.get("name") == "Install docs browser QA dependencies"
    )

    assert install_step["working-directory"] == "docs-qa"
    assert (
        install_step["run"].strip()
        == "corepack enable\ncorepack install\npnpm install --frozen-lockfile"
    )


def test_docs_search_step_uses_the_docs_qa_working_directory() -> None:
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    docs_guards_job = jobs["docs-guards-job"]
    assert isinstance(docs_guards_job, dict)
    steps = docs_guards_job["steps"]
    assert isinstance(steps, list)

    search_step = next(
        step
        for step in steps
        if isinstance(step, dict) and step.get("name") == "Run docs search acceptance"
    )

    assert search_step["working-directory"] == "docs-qa"
    assert search_step["run"].strip() == "pnpm run typecheck\npnpm run test:search"


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
