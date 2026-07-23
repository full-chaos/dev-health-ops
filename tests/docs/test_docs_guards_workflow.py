"""Durable contract for the single canonical ``Docs Guards`` workflow.

The documentation gate is one job, ``docs-guards``. The previous
``changes`` -> ``docs-guards-job`` -> aggregate architecture no longer
exists, so these tests encode the properties that must stay true of the
canonical single-job workflow rather than the old job graph.
"""

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "docs-guards.yml"

# The streamed reader-critical checks, in workflow order. Each writes its
# output through ``tee`` under ``set -o pipefail`` so a checker failure is
# never masked by the ``tee`` exit code.
STREAMED_CHECK_STEPS = (
    "Validate publication inventory and IA placement",
    "Strict canonical build",
    "Check rendered links, anchors, and assets",
    "Check task-based search acceptance",
    "Check structural accessibility invariants",
    "Check objective documentation facts",
)


def _load_workflow() -> dict[object, Any]:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _on_section(workflow: dict[object, Any]) -> dict[str, object]:
    # PyYAML parses the bare ``on:`` key as the boolean ``True``.
    section = workflow.get("on", workflow.get(True))
    assert isinstance(section, dict)
    return section


def _guards_job() -> dict[str, object]:
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)
    job = jobs["docs-guards"]
    assert isinstance(job, dict)
    return job


def _docs_steps() -> list[dict[str, object]]:
    steps = _guards_job()["steps"]
    assert isinstance(steps, list)
    return [step for step in steps if isinstance(step, dict)]


def _step_by_name(name: str) -> dict[str, object]:
    return next(step for step in _docs_steps() if step.get("name") == name)


def test_workflow_is_a_single_canonical_docs_guards_job() -> None:
    """One canonical job named ``docs-guards``; no aggregator/matrix jobs."""
    workflow = _load_workflow()
    jobs = workflow["jobs"]
    assert isinstance(jobs, dict)

    assert list(jobs.keys()) == ["docs-guards"]
    assert "changes" not in jobs
    assert "docs-guards-job" not in jobs

    job = jobs["docs-guards"]
    assert isinstance(job, dict)
    # A single canonical job runs directly; it does not depend on an
    # upstream change-detection or fan-in job.
    assert "needs" not in job
    assert "strategy" not in job


def test_contents_permission_is_read_only() -> None:
    """The gate only reads the repository; it must not request write scope."""
    workflow = _load_workflow()
    permissions = workflow["permissions"]

    assert permissions == {"contents": "read"}


def test_path_filters_watch_canonical_docs_and_mkdocs() -> None:
    """Both push and pull_request triggers scope to the canonical sources."""
    on_section = _on_section(_load_workflow())

    for trigger in ("push", "pull_request"):
        event = on_section[trigger]
        assert isinstance(event, dict)
        paths = event["paths"]
        assert isinstance(paths, list)
        assert "docs/**" in paths
        assert "mkdocs.yml" in paths


def test_python_312_is_configured() -> None:
    setup_steps = [
        step
        for step in _docs_steps()
        if str(step.get("uses", "")).startswith("actions/setup-python@")
    ]
    assert setup_steps, "expected an actions/setup-python step"
    for step in setup_steps:
        params = step.get("with")
        assert isinstance(params, dict)
        assert str(params.get("python-version")) == "3.12"


def test_docs_guards_has_no_node_or_playwright_runtime() -> None:
    uses = [str(step.get("uses", "")) for step in _docs_steps()]
    run_scripts = "\n".join(str(step.get("run", "")) for step in _docs_steps()).lower()

    assert not any(value.startswith("actions/setup-node@") for value in uses)
    assert "docs-qa" not in run_scripts
    assert "playwright" not in run_scripts
    assert "pnpm" not in run_scripts
    assert "npx" not in run_scripts
    assert "wrangler" not in run_scripts


def test_docs_guards_runs_the_reader_critical_checks() -> None:
    names = {str(step.get("name", "")) for step in _docs_steps()}

    assert set(STREAMED_CHECK_STEPS).issubset(names)


def test_reader_critical_checks_invoke_the_canonical_scripts() -> None:
    """Each streamed check runs its canonical script against the strict build."""
    run_by_name = {
        str(step.get("name", "")): str(step.get("run", "")) for step in _docs_steps()
    }

    assert "validate_docs_v2_publication.py" in run_by_name[STREAMED_CHECK_STEPS[0]]

    strict_build = run_by_name[STREAMED_CHECK_STEPS[1]]
    assert "mkdocs build --strict" in strict_build
    assert "--config-file mkdocs.yml" in strict_build

    links = run_by_name[STREAMED_CHECK_STEPS[2]]
    assert "check_built_site_links.py" in links
    assert "--site-dir .build/docs" in links

    search = run_by_name[STREAMED_CHECK_STEPS[3]]
    assert "check_docs_candidate_search.py" in search
    assert ".github/documentation-program/phase-10/search-acceptance.json" in search

    accessibility = run_by_name[STREAMED_CHECK_STEPS[4]]
    assert "check_docs_candidate_accessibility.py" in accessibility

    facts = run_by_name[STREAMED_CHECK_STEPS[5]]
    assert "check_docs_candidate_facts.py" in facts


def test_streamed_docs_checks_propagate_checker_failures() -> None:
    for name in STREAMED_CHECK_STEPS:
        run_script = _step_by_name(name).get("run")
        assert isinstance(run_script, str)
        assert "set -o pipefail" in run_script
        assert "| tee " in run_script


def test_evidence_artifact_is_always_uploaded() -> None:
    upload_steps = [
        step
        for step in _docs_steps()
        if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert len(upload_steps) == 1
    upload = upload_steps[0]

    assert upload.get("if") == "always()"
    params = upload.get("with")
    assert isinstance(params, dict)
    path_spec = str(params.get("path"))
    assert ".build/docs-*.log" in path_spec


def test_docs_guards_does_not_run_external_network_or_visual_contracts() -> None:
    run_scripts = "\n".join(str(step.get("run", "")) for step in _docs_steps()).lower()

    assert "check_external_links.py" not in run_scripts
    assert "screenshot" not in run_scripts
    assert "visual regression" not in run_scripts
    assert "exact prose" not in run_scripts
