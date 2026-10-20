"""`ci/venue-main-alert.sh` (CHAOS-9597). The workflow-shape tests of venue-oracles-main-alert.yml are in
internal/cigates/workflows/workflows_venue_main_alert_test.go (CHAOS-10177).

A red main-push `Venue oracles` run opens or updates ONE GitHub issue; a green
executed run closes it; a skipped or stale run changes nothing. The script reads
the GitHub API through `gh`; these tests feed it a fake `gh` (VENUE_ALERT_GH and
LAST_VENUE_RUN_GH), so no network and no token are needed. The fake records every
call, and the tests assert which writes happened.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ci" / "venue-main-alert.sh"
STEP = "Run this shard's venue differential oracles against the live Python api"
LABEL = "venue-oracles-main-red"


def _job(shard: int, oracle: str) -> dict:
    steps = [{"name": "Set up job", "conclusion": "success"}]
    if oracle != "absent":
        steps.append({"name": STEP, "conclusion": oracle})
    return {"name": f"venue-oracles (shard {shard}/3)", "steps": steps}


def _legs(*conclusions: str) -> list[dict]:
    return [_job(i + 1, c) for i, c in enumerate(conclusions)]


GREEN = _legs("success", "success", "success")
RED = _legs("success", "failure", "success")
SKIPPED = _legs("absent", "absent", "absent")
PARTIAL = _legs("success", "failure", "absent")

FAKE_GH = r"""#!/usr/bin/env bash
set -eu
fx="$FAKE_FIXTURE"
printf '%s\n' "$*" >> "$FAKE_CALLS"
if [ "$1" = "api" ]; then
  endpoint="$2"
  case "${endpoint}" in
    */workflows/venue-oracles.yml/runs*) n="${endpoint##*per_page=}"; jq ".runs | .workflow_runs |= .[:${n}]" "${fx}" ;;
    */actions/runs/*/jobs*) id="${endpoint#*/actions/runs/}"; id="${id%%/*}"; jq ".jobs[\"${id}\"]" "${fx}" ;;
    *) exit 9 ;;
  esac
  exit 0
fi
if [ "${FAKE_GH_FAIL_ISSUE_LIST:-0}" = "1" ] && [ "$1 $2" = "issue list" ]; then exit 1; fi
case "$1 $2" in
  "issue list") jq -r '[.issues[].number] | sort | .[0] // empty' "${fx}" ;;
  "issue view") jq -r --argjson n "$3" '.issues[] | select(.number == $n) | ([.body] + .comments) | join("\n")' "${fx}" ;;
  "issue create") echo "https://github.com/o/r/issues/99" ;;
  "issue comment"|"issue close"|"label create") ;;
  *) exit 9 ;;
esac
"""


def _run(
    tmp_path: Path,
    runs: list[tuple[int, str, list[dict]]],
    run_id: int,
    issues: list[dict] | None = None,
    **extra_env: str,
):
    """runs: newest first, (id, run conclusion, jobs). issues: open labelled issues."""
    fixture = {
        "runs": {
            "workflow_runs": [
                {"id": rid, "head_sha": f"{rid:040x}", "conclusion": concl}
                for rid, concl, _ in runs
            ]
        },
        "jobs": {str(rid): {"jobs": jobs} for rid, _, jobs in runs},
        "issues": issues or [],
    }
    (tmp_path / "fixture.json").write_text(json.dumps(fixture))
    calls = tmp_path / "calls.log"
    calls.write_text("")
    gh = tmp_path / "fake-gh"
    gh.write_text(FAKE_GH)
    gh.chmod(gh.stat().st_mode | stat.S_IXUSR)
    env = {
        **os.environ,
        "VENUE_ALERT_GH": str(gh),
        "LAST_VENUE_RUN_GH": str(gh),
        "FAKE_FIXTURE": str(tmp_path / "fixture.json"),
        "FAKE_CALLS": str(calls),
        "GITHUB_REPOSITORY": "o/r",
        "RUN_ID": str(run_id),
        "RUN_CONCLUSION": next((c for rid, c, _ in runs if rid == run_id), "unknown"),
        "RUN_SHA": f"{run_id:040x}",
        **extra_env,
    }
    proc = subprocess.run(
        ["bash", str(SCRIPT)], capture_output=True, text=True, env=env
    )
    lines = calls.read_text().splitlines()
    writes = [
        line
        for line in lines
        if line.startswith(
            ("issue create", "issue comment", "issue close", "label create")
        )
    ]
    return proc, writes


def _issue(number: int, body: str = "", comments: list[str] | None = None) -> dict:
    return {"number": number, "body": body, "comments": comments or []}


def _api_calls(tmp_path: Path) -> list[str]:
    return [
        line
        for line in (tmp_path / "calls.log").read_text().splitlines()
        if line.startswith("api ")
    ]


def test_failed_run_opens_one_labelled_issue(tmp_path):
    proc, writes = _run(tmp_path, [(300, "failure", RED), (200, "success", GREEN)], 300)
    assert proc.returncode == 0, proc.stderr
    assert [w.split(" -R")[0] for w in writes] == [
        "label create venue-oracles-main-red",
        "issue create",
    ]
    create = writes[1]
    assert f"--label {LABEL}" in create
    assert "actions/runs/300" in create
    assert "concluded `failure`" in create
    assert "--title Main venue-oracles run is red" in create


def test_the_conclusion_alone_decides_red(tmp_path):
    for conclusion, jobs in (
        ("failure", GREEN),
        ("failure", SKIPPED),
        ("failure", PARTIAL),
        ("timed_out", SKIPPED),
        ("startup_failure", SKIPPED),
        ("cancelled", SKIPPED),
    ):
        proc, writes = _run(tmp_path, [(300, conclusion, jobs)], 300)
        assert proc.returncode == 0, proc.stderr
        assert writes[-1].startswith("issue create"), (conclusion, writes)
        assert f"concluded `{conclusion}`" in writes[-1]
        assert _api_calls(tmp_path) == []


def test_a_red_run_behind_many_skipped_runs_still_opens_the_issue(tmp_path):
    skipped = [(1000 + i, "success", SKIPPED) for i in range(40)]
    proc, writes = _run(tmp_path, [(1100, "failure", RED), *skipped], 1100)
    assert proc.returncode == 0, proc.stderr
    assert writes[-1].startswith("issue create")


def test_failed_run_with_open_issue_comments_instead_of_a_second_issue(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "failure", RED)],
        300,
        issues=[_issue(41, body="Run https://github.com/o/r/actions/runs/200 at x")],
    )
    assert proc.returncode == 0, proc.stderr
    assert len(writes) == 1
    assert writes[0].startswith("issue comment 41 ")
    assert "actions/runs/300" in writes[0]


def test_a_run_already_on_the_issue_is_not_commented_twice(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "failure", RED)],
        300,
        issues=[
            _issue(
                41,
                comments=[
                    "Still red. Run https://github.com/o/r/actions/runs/300 at x"
                ],
            )
        ],
    )
    assert proc.returncode == 0, proc.stderr
    assert writes == []


def test_a_run_id_that_is_a_prefix_of_a_reported_id_is_still_reported(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(30, "failure", RED)],
        30,
        issues=[_issue(41, body="Run https://github.com/o/r/actions/runs/300 at x")],
    )
    assert proc.returncode == 0, proc.stderr
    assert len(writes) == 1 and writes[0].startswith("issue comment 41 ")


def test_the_lowest_numbered_open_issue_is_the_one(tmp_path):
    proc, writes = _run(
        tmp_path, [(300, "failure", RED)], 300, issues=[_issue(50), _issue(41)]
    )
    assert proc.returncode == 0, proc.stderr
    assert len(writes) == 1 and writes[0].startswith("issue comment 41 ")


def test_proven_executed_green_run_closes_the_open_issue(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "success", GREEN), (200, "failure", RED)],
        300,
        issues=[_issue(41)],
    )
    assert proc.returncode == 0, proc.stderr
    assert len(writes) == 1
    assert writes[0].startswith("issue close 41 ")
    assert "actions/runs/300" in writes[0]


def test_a_skipped_success_never_closes_the_issue(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "success", SKIPPED), (200, "failure", RED)],
        300,
        issues=[_issue(41)],
    )
    assert proc.returncode == 0, proc.stderr
    assert writes == []
    assert "stays open" in proc.stdout


def test_a_success_that_is_not_the_newest_executed_run_closes_nothing(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(400, "success", GREEN), (300, "success", GREEN)],
        300,
        issues=[_issue(41)],
    )
    assert proc.returncode == 0, proc.stderr
    assert writes == []


def test_success_with_no_open_issue_reads_nothing(tmp_path):
    proc, writes = _run(tmp_path, [(300, "success", GREEN)], 300)
    assert proc.returncode == 0, proc.stderr
    assert writes == []
    assert _api_calls(tmp_path) == []


def test_other_conclusions_do_nothing(tmp_path):
    for conclusion in ("skipped", "neutral", "action_required", "stale"):
        proc, writes = _run(
            tmp_path, [(300, conclusion, GREEN)], 300, issues=[_issue(41)]
        )
        assert proc.returncode == 0, proc.stderr
        assert writes == []


def test_a_failed_issue_read_fails_the_job(tmp_path):
    proc, writes = _run(
        tmp_path, [(300, "failure", RED)], 300, FAKE_GH_FAIL_ISSUE_LIST="1"
    )
    assert proc.returncode != 0
    assert writes == []


def test_a_failed_state_read_fails_the_job_and_closes_nothing(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "success", GREEN)],
        300,
        issues=[_issue(41)],
        VENUE_ALERT_LAST_RUN="/nonexistent/last-run.sh",
    )
    assert proc.returncode != 0
    assert writes == []


def test_dry_run_prints_the_write_and_does_not_make_it(tmp_path):
    proc, writes = _run(tmp_path, [(300, "failure", RED)], 300, DRY_RUN="1")
    assert proc.returncode == 0, proc.stderr
    assert writes == []
    assert "DRY-RUN: gh issue create" in proc.stdout


def test_inputs_are_validated(tmp_path):
    for name in ("RUN_ID", "RUN_CONCLUSION", "RUN_SHA"):
        proc, writes = _run(tmp_path, [(300, "failure", RED)], 300, **{name: "x;rm"})
        assert proc.returncode == 2, name
        assert writes == []
