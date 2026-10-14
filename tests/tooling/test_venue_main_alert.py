"""`ci/venue-main-alert.sh` and venue-oracles-main-alert.yml (CHAOS-9597).

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

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ci" / "venue-main-alert.sh"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "venue-oracles-main-alert.yml"
VENUE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "venue-oracles.yml"
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
  "issue list") jq -c '[.issues[] | {number}]' "${fx}" | jq -r 'map(.number) | sort | .[0] // empty' ;;
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
        **extra_env,
    }
    proc = subprocess.run(
        ["bash", str(SCRIPT)], capture_output=True, text=True, env=env
    )
    lines = calls.read_text().splitlines()
    writes = [
        line
        for line in lines
        if line.startswith(("issue create", "issue comment", "issue close", "label create"))
    ]
    return proc, writes


def _issue(number: int, body: str = "", comments: list[str] | None = None) -> dict:
    return {"number": number, "body": body, "comments": comments or []}


def test_red_executed_run_opens_one_labelled_issue(tmp_path):
    proc, writes = _run(tmp_path, [(300, "failure", RED), (200, "success", GREEN)], 300)
    assert proc.returncode == 0, proc.stderr
    assert [w.split(" -R")[0] for w in writes] == ["label create venue-oracles-main-red", "issue create"]
    create = writes[1]
    assert f"--label {LABEL}" in create
    assert "actions/runs/300" in create
    assert "--title Main venue-oracles run is red" in create


def test_red_run_with_open_issue_comments_instead_of_a_second_issue(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "failure", RED), (200, "failure", RED)],
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
        issues=[_issue(41, comments=["Still red. Run https://github.com/o/r/actions/runs/300 at x"])],
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


def test_lowest_numbered_issue_is_the_one_when_two_are_open(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "failure", RED)],
        300,
        issues=[_issue(50), _issue(41)],
    )
    assert proc.returncode == 0, proc.stderr
    assert writes[0].startswith("issue comment 41 ")


def test_incomplete_run_is_reported_as_not_green(tmp_path):
    proc, writes = _run(tmp_path, [(300, "failure", PARTIAL)], 300)
    assert proc.returncode == 0, proc.stderr
    assert writes[-1].startswith("issue create")
    assert "did not finish" in writes[-1]


def test_green_executed_run_closes_the_open_issue(tmp_path):
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


def test_green_oracle_steps_on_a_failed_run_do_not_close_the_issue(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "failure", GREEN), (200, "failure", RED)],
        300,
        issues=[_issue(41)],
    )
    assert proc.returncode == 0, proc.stderr
    assert writes == []
    assert "not success" in proc.stdout


def test_red_run_behind_many_skipped_runs_still_opens_the_issue(tmp_path):
    skipped = [(1000 + i, "success", SKIPPED) for i in range(25)]
    proc, writes = _run(tmp_path, [*skipped, (200, "failure", RED)], 1024)
    assert proc.returncode == 0, proc.stderr
    assert writes[-1].startswith("issue create")
    assert "actions/runs/200" in writes[-1]


def test_green_with_no_open_issue_changes_nothing(tmp_path):
    proc, writes = _run(tmp_path, [(300, "success", GREEN)], 300)
    assert proc.returncode == 0, proc.stderr
    assert writes == []


def test_skipped_run_over_a_red_state_never_closes_the_issue(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "success", SKIPPED), (200, "failure", RED)],
        300,
        issues=[_issue(41, body="Run https://github.com/o/r/actions/runs/200 at x")],
    )
    assert proc.returncode == 0, proc.stderr
    assert writes == []
    assert "already on issue #41" in proc.stdout


def test_skipped_run_makes_up_a_lost_red_alert_without_duplicating(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "success", SKIPPED), (200, "failure", RED)],
        300,
        issues=[_issue(41, body="Run https://github.com/o/r/actions/runs/100 at x")],
    )
    assert proc.returncode == 0, proc.stderr
    assert len(writes) == 1 and writes[0].startswith("issue comment 41 ")
    assert "actions/runs/200" in writes[0]


def test_older_run_that_completes_late_reads_the_current_state(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "success", GREEN), (200, "failure", RED)],
        200,
    )
    assert proc.returncode == 0, proc.stderr
    assert writes == []


def test_older_red_event_does_not_reopen_while_main_is_green(tmp_path):
    proc, writes = _run(
        tmp_path,
        [(300, "success", GREEN), (200, "failure", RED)],
        200,
        issues=[_issue(41)],
    )
    assert proc.returncode == 0, proc.stderr
    assert len(writes) == 1 and writes[0].startswith("issue close 41 ")


def test_no_executed_run_in_the_window_warns_and_changes_nothing(tmp_path):
    proc, writes = _run(tmp_path, [(300, "success", SKIPPED)], 300)
    assert proc.returncode == 0, proc.stderr
    assert writes == []
    assert "::warning" in proc.stdout


def test_a_failed_issue_read_fails_the_job(tmp_path):
    proc, writes = _run(
        tmp_path, [(300, "failure", RED)], 300, FAKE_GH_FAIL_ISSUE_LIST="1"
    )
    assert proc.returncode != 0
    assert writes == []


def test_a_failed_state_read_fails_the_job(tmp_path):
    proc, writes = _run(
        tmp_path, [(300, "failure", RED)], 300, VENUE_ALERT_LAST_RUN="/nonexistent/last-run.sh"
    )
    assert proc.returncode != 0
    assert writes == []


def test_dry_run_prints_the_write_and_does_not_make_it(tmp_path):
    proc, writes = _run(tmp_path, [(300, "failure", RED)], 300, DRY_RUN="1")
    assert proc.returncode == 0, proc.stderr
    assert writes == []
    assert "DRY-RUN: gh issue create" in proc.stdout


def test_run_id_must_be_numeric(tmp_path):
    proc, _ = _run(tmp_path, [(300, "failure", RED)], 300, RUN_ID="3;rm")
    assert proc.returncode == 2


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_workflow_listens_to_the_venue_workflow_by_its_name():
    doc = _workflow()
    triggers = doc.get(True) or doc.get("on")
    venue_name = yaml.safe_load(VENUE_WORKFLOW.read_text(encoding="utf-8"))["name"]
    assert triggers["workflow_run"]["workflows"] == [venue_name]
    assert triggers["workflow_run"]["types"] == ["completed"]


def test_workflow_only_handles_main_push_runs():
    job = _workflow()["jobs"]["alert"]
    assert job["if"] == (
        "github.event.workflow_run.head_branch == 'main'"
        " && github.event.workflow_run.event == 'push'"
    )


def test_workflow_token_is_least_privilege_at_job_level():
    doc = _workflow()
    assert doc["permissions"] == {}
    assert doc["jobs"]["alert"]["permissions"] == {
        "contents": "read",
        "actions": "read",
        "issues": "write",
    }


def test_workflow_serialises_alert_jobs_after_the_filter_and_never_cancels_one():
    doc = _workflow()
    assert "concurrency" not in doc
    concurrency = doc["jobs"]["alert"]["concurrency"]
    assert concurrency["group"] == "venue-oracles-main-alert"
    assert concurrency["cancel-in-progress"] is False


def test_workflow_passes_event_data_through_env_only():
    text = WORKFLOW.read_text(encoding="utf-8")
    steps = _workflow()["jobs"]["alert"]["steps"]
    run_steps = [s for s in steps if "run" in s]
    assert len(run_steps) == 1
    assert "${{" not in run_steps[0]["run"]
    assert run_steps[0]["env"]["RUN_ID"] == "${{ github.event.workflow_run.id }}"
    assert "secrets." not in text
