"""Workflows install Python dependencies from uv.lock, not from an unlocked requirements file.

WHY THIS TEST EXISTS (CHAOS-6589)
---------------------------------
`requirements.txt` is `-e .[dev]`: `pip install -r requirements.txt` resolves
every specifier in pyproject.toml against PyPI on every run and never reads
uv.lock. On 2026-09-24 SQLAlchemy 2.1.0 was picked up that way by the Live
Backend E2E / metrics-executed-proof jobs and broke main while every job that
installs the lock stayed green (CHAOS-6588 bounded it in pyproject as the
stopgap). A job installs the locked, deployed set with
`uv sync --frozen --all-extras --dev`.

WHAT THIS PINS
--------------
1. No `run:` step of any workflow installs from the repo-root
   `requirements.txt` (`pip install -r requirements.txt`, `uv pip install
   -r requirements.txt`, `pip install -e .`), except the (workflow, job)
   pairs in ALLOWED_UNLOCKED below, each with its reason.
2. The allowlist cannot go stale: an allowed pair that no longer installs
   unlocked FAILS the test, so a job that is fixed must leave the list.
3. live-e2e.yml (both of its jobs: live-e2e and metrics-executed-proof)
   installs with `uv sync --frozen` and puts `.venv/bin` on PATH (the run
   scripts call `python3`), and its path filter selects on uv.lock.
4. The scan is not vacuous: it must read at least 50 workflow `run:` steps.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"

# (workflow file, job id) -> why it is still unlocked. Empty since CHAOS-6646
# moved the last four jobs to `uv sync --frozen`; an entry needs a reason a
# reviewer can accept and the test below fails once the job is locked again.
ALLOWED_UNLOCKED: dict[tuple[str, str], str] = {}

E2E_WORKFLOW = "live-e2e.yml"
E2E_JOBS = ("live-e2e", "metrics-executed-proof")

# `pip install ... -r requirements.txt`, `--requirement=requirements.txt`,
# `uv pip install -r requirements.txt`, or an editable install of the project.
# The negative look-ahead keeps `-r ci/requirements-*.txt` and
# `requirements-docs.txt` (different files) out.
_ROOT_REQUIREMENTS = re.compile(
    r"(?:-r|--requirement)[ =]+(?:\./)?requirements\.txt(?![\w.-])"
)
_EDITABLE_PROJECT = re.compile(r"\bpip3?\s+install\b[^\n]*\s-e\s+\.")


def _installs_unlocked(script: str) -> bool:
    """True when a shell script installs the project unlocked."""
    for line in script.replace("\\\n", " ").splitlines():
        code = line.split("#", 1)[0]
        if "install" not in code:
            continue
        if _ROOT_REQUIREMENTS.search(code) or _EDITABLE_PROJECT.search(code):
            return True
    return False


def _load(path: Path) -> dict:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return document if isinstance(document, dict) else {}


def _unlocked_jobs(workflows_dir: Path) -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for path in sorted(workflows_dir.glob("*.yml")):
        for job_id, job in (_load(path).get("jobs") or {}).items():
            for step in (job or {}).get("steps") or []:
                run = step.get("run")
                if isinstance(run, str) and _installs_unlocked(run):
                    found.add((path.name, job_id))
    return found


def _run_steps_scanned(workflows_dir: Path) -> int:
    count = 0
    for path in sorted(workflows_dir.glob("*.yml")):
        for job in (_load(path).get("jobs") or {}).values():
            count += sum(
                1
                for step in (job or {}).get("steps") or []
                if isinstance(step.get("run"), str)
            )
    return count


def test_no_unlisted_workflow_job_installs_from_the_unlocked_requirements_file() -> (
    None
):
    found = _unlocked_jobs(WORKFLOWS_DIR)
    unlisted = sorted(found - set(ALLOWED_UNLOCKED))
    assert not unlisted, (
        "workflow job(s) install from the unlocked requirements.txt, so a PyPI "
        "release can change the tested set with no diff (CHAOS-6589). Use "
        "`uv sync --frozen --all-extras --dev` (and put .venv/bin on "
        f"GITHUB_PATH), or justify an exception in ALLOWED_UNLOCKED: {unlisted}"
    )


def test_the_allowlist_is_not_stale() -> None:
    found = _unlocked_jobs(WORKFLOWS_DIR)
    stale = sorted(set(ALLOWED_UNLOCKED) - found)
    assert not stale, (
        f"ALLOWED_UNLOCKED names job(s) that no longer install unlocked: {stale}. "
        "Delete the entry -- an exception that outlives its reason hides the "
        "next regression in that job."
    )
    # Vacuity guard: with nothing to find, prove the scan still read the tree.
    assert _run_steps_scanned(WORKFLOWS_DIR) >= 50, (
        "the scan read fewer than 50 workflow `run:` steps -- it is broken"
    )


def test_the_e2e_jobs_install_the_lock() -> None:
    workflow = _load(WORKFLOWS_DIR / E2E_WORKFLOW)
    for job_id in E2E_JOBS:
        job = (workflow.get("jobs") or {}).get(job_id)
        assert job is not None, f"{E2E_WORKFLOW} has no job {job_id!r}"
        runs = [
            s["run"] for s in job.get("steps") or [] if isinstance(s.get("run"), str)
        ]
        assert any("uv sync --frozen" in r for r in runs), (
            f"{E2E_WORKFLOW}:{job_id} must install with `uv sync --frozen ...`"
        )
        assert any(".venv/bin" in r and "GITHUB_PATH" in r for r in runs), (
            f"{E2E_WORKFLOW}:{job_id} must put .venv/bin on GITHUB_PATH: its "
            "scripts call python3 from PATH"
        )
        assert not any(_installs_unlocked(r) for r in runs)
        assert any(
            "astral-sh/setup-uv" in str(s.get("uses", ""))
            for s in job.get("steps") or []
        ), f"{E2E_WORKFLOW}:{job_id} must set up uv"


def test_the_e2e_path_filter_selects_on_the_lock() -> None:
    """The install now reads uv.lock, so a lock-only change must run the job."""
    assert "- 'uv.lock'" in (WORKFLOWS_DIR / E2E_WORKFLOW).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# The detector fails on planted defects (baseline passes, defect is caught).
# ---------------------------------------------------------------------------


def _workflow_with(tmp_path: Path, run: str) -> Path:
    workflows = tmp_path / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "w.yml").write_text(
        yaml.safe_dump({"jobs": {"j": {"steps": [{"run": run}]}}}), encoding="utf-8"
    )
    return workflows


def test_detector_accepts_locked_and_other_requirement_files(tmp_path: Path) -> None:
    for i, run in enumerate(
        (
            "uv sync --frozen --all-extras --dev\n",
            "python -m pip install --no-deps -r ci/requirements-live-python-oracles.txt\n",
            "pip install -r requirements-docs.txt\n",
            "uv pip install --python .venv/bin/python -r requirements-docs.txt\n",
            "# pip install -r requirements.txt (a comment)\n",
        )
    ):
        assert _unlocked_jobs(_workflow_with(tmp_path / f"ok{i}", run)) == set(), run


def test_detector_catches_the_unlocked_install_shapes(tmp_path: Path) -> None:
    for i, run in enumerate(
        (
            "pip install -r requirements.txt\n",
            "python -m pip install --upgrade pip\npip install -r requirements.txt\n",
            "pip install --requirement=requirements.txt\n",
            "uv pip install -r ./requirements.txt\n",
            "pip install -e .[dev]\n",
            "pip install \\\n  -r requirements.txt\n",
        )
    ):
        found = _unlocked_jobs(_workflow_with(tmp_path / f"bad{i}", run))
        assert found == {("w.yml", "j")}, run
