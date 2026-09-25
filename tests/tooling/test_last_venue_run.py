"""`ci/last-venue-run.sh` and the loud skip report in venue-oracles.yml.

WHY. A main-push venue run that skips its oracle step reports SUCCESS, and
that green was once read as a green main venue. Definition of record
(Trap #408): green main venue = the newest run whose oracle step EXECUTED is
green, and no later EXECUTED run failed. A skipped run is neither green nor
red.

The script reads the GitHub API through `gh`; these tests feed it fixtures
through LAST_VENUE_RUN_GH, so no network and no token are needed.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ci" / "last-venue-run.sh"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "venue-oracles.yml"
STEP = "Run this shard's venue differential oracles against the live Python api"


def _job(shard: int, oracle: str) -> dict:
    """One matrix leg; `oracle` is the oracle step's conclusion (or 'absent')."""
    steps = [{"name": "Set up job", "conclusion": "success"}]
    if oracle != "absent":
        steps.append({"name": STEP, "conclusion": oracle})
    return {"name": f"venue-oracles (shard {shard}/5)", "steps": steps}


def _legs(*conclusions: str) -> list[dict]:
    return [_job(i + 1, c) for i, c in enumerate(conclusions)]


def _run_script(tmp_path: Path, runs: list[tuple[int, str, list[dict]]], *args: str):
    """runs: newest first, (id, run conclusion, jobs)."""
    fixture = {
        "runs": {
            "workflow_runs": [
                {"id": rid, "head_sha": f"{rid:040x}", "conclusion": concl}
                for rid, concl, _ in runs
            ]
        },
        "jobs": {str(rid): {"jobs": jobs} for rid, _, jobs in runs},
    }
    (tmp_path / "fixture.json").write_text(json.dumps(fixture))
    gh = tmp_path / "fake-gh"
    gh.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        'endpoint="$2"\n'
        f'fx="{tmp_path}/fixture.json"\n'
        'case "${endpoint}" in\n'
        '  */workflows/venue-oracles.yml/runs*) jq .runs "${fx}" ;;\n'
        '  */actions/runs/*/jobs*) id="${endpoint#*/actions/runs/}"; id="${id%%/*}"; jq ".jobs[\\"${id}\\"]" "${fx}" ;;\n'
        "  *) exit 9 ;;\n"
        "esac\n"
    )
    gh.chmod(gh.stat().st_mode | stat.S_IXUSR)
    env = {**os.environ, "LAST_VENUE_RUN_GH": str(gh)}
    return subprocess.run(
        ["bash", str(SCRIPT), *args], capture_output=True, text=True, env=env
    )


def _verdict(proc: subprocess.CompletedProcess) -> str:
    return proc.stdout.strip().splitlines()[-1]


ALL_OK = _legs(*["success"] * 5)
SKIPPED = _legs(*["absent"] * 5)


def test_executed_green(tmp_path):
    proc = _run_script(tmp_path, [(3, "success", ALL_OK)])
    assert proc.returncode == 0, proc.stderr
    assert _verdict(proc).startswith("venue-main: GREEN last-executed=3 ")
    assert "skipped-since=0" in _verdict(proc)


def test_skipped_run_after_green_names_the_executed_run(tmp_path):
    proc = _run_script(tmp_path, [(9, "success", SKIPPED), (5, "success", ALL_OK)])
    assert proc.returncode == 0, proc.stderr
    line = _verdict(proc)
    assert line.startswith("venue-main: GREEN last-executed=5 ")
    assert "skipped-since=1" in line
    assert "run=9 " in proc.stdout and "class=skipped" in proc.stdout


def test_skipped_only_is_none_never_green(tmp_path):
    proc = _run_script(tmp_path, [(9, "success", SKIPPED), (8, "success", SKIPPED)])
    assert proc.returncode == 3
    assert _verdict(proc).startswith("venue-main: NONE ")
    assert "skipped-since=2" in _verdict(proc)


def test_executed_red_then_skipped_stays_red(tmp_path):
    """The defect this exists for: a later SKIPPED success must not mask a red."""
    red = _legs("success", "failure", "success", "success", "success")
    proc = _run_script(tmp_path, [(9, "success", SKIPPED), (5, "failure", red)])
    assert proc.returncode == 1
    line = _verdict(proc)
    assert line.startswith("venue-main: RED last-executed=5 ")
    assert "conclusion=failure" in line and "skipped-since=1" in line


def test_newer_executed_green_after_red_is_green(tmp_path):
    red = _legs("failure", *["success"] * 4)
    proc = _run_script(tmp_path, [(9, "success", ALL_OK), (5, "failure", red)])
    assert proc.returncode == 0
    assert _verdict(proc).startswith("venue-main: GREEN last-executed=9 ")


def test_cancelled_leg_is_incomplete_not_green(tmp_path):
    cancelled = _legs("success", "success", "success", "success", "absent")
    proc = _run_script(tmp_path, [(7, "cancelled", cancelled)])
    assert proc.returncode == 1
    assert _verdict(proc).startswith("venue-main: INCOMPLETE last-executed=7 ")


def test_no_runs_is_none(tmp_path):
    proc = _run_script(tmp_path, [])
    assert proc.returncode == 3
    assert _verdict(proc).startswith("venue-main: NONE ")


def test_api_failure_is_a_usage_error_not_a_verdict(tmp_path):
    env = {**os.environ, "LAST_VENUE_RUN_GH": "false"}
    proc = subprocess.run(
        ["bash", str(SCRIPT)], capture_output=True, text=True, env=env
    )
    assert proc.returncode == 2
    assert "venue-main:" not in proc.stdout


def test_summary_mode_is_markdown_with_the_verdict(tmp_path):
    proc = _run_script(tmp_path, [(5, "success", ALL_OK)], "--summary")
    assert proc.returncode == 0
    assert proc.stdout.startswith("### Main venue-oracles state")
    assert "venue-main: GREEN" in proc.stdout


def test_bad_limit_is_rejected(tmp_path):
    proc = _run_script(tmp_path, [], "--limit", "x")
    assert proc.returncode == 2


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_oracle_step_name_matches_the_script_filter():
    """The script keys on the step name prefix; a rename must fail here."""
    steps = _workflow()["jobs"]["venue-oracles"]["steps"]
    assert STEP in [s.get("name") for s in steps]
    assert 'startswith("Run this shard")' in SCRIPT.read_text(encoding="utf-8")
    assert 'startswith("venue-oracles")' in SCRIPT.read_text(encoding="utf-8")


def test_skip_step_is_loud_and_best_effort():
    doc = _workflow()
    steps = doc["jobs"]["venue-oracles"]["steps"]
    skip = next(
        s
        for s in steps
        if s.get("name", "").startswith("Report that the gate was skipped")
    )
    assert skip["if"] == "steps.relevance.outputs.relevant != 'true'"
    run = skip["run"]
    assert "::notice title=VENUE ORACLES NOT RUN::" in run
    assert "VENUE ORACLES NOT RUN:" in run and "GITHUB_STEP_SUMMARY" in run
    assert "ci/last-venue-run.sh" in run
    # best effort: the API read must not be able to fail the skip step
    assert "|| true" in run
    assert "GH_TOKEN" in skip["env"]
    assert doc["permissions"].get("actions") == "read"
    # the skip must not turn into a failure of the run (lead: not failing the run)
    assert "exit 1" not in run


def test_definition_of_record_is_in_the_workflow_and_script_headers():
    for path in (WORKFLOW, SCRIPT):
        text = path.read_text(encoding="utf-8")
        assert "Trap #408" in text, path
        assert "EXECUTED" in text, path
