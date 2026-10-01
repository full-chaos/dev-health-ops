"""The Python-free Go job: tripwire, closed-list ratchet and the three-state watch (CHAOS-7384).

WHY. Hosted runners have python3, so a green Go job does not prove a frozen test is Python-free.
`ci/python_tripwire.sh` makes every Python start a named failure and `ci/python_free_ratchet.sh`
holds the failures to a closed list so red does not become normal. These tests drive the real
scripts with fixtures: no network, no GitHub token, no Docker.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
TRIPWIRE = REPO_ROOT / "ci" / "python_tripwire.sh"
RATCHET = REPO_ROOT / "ci" / "python_free_ratchet.sh"
LAST_RUN = REPO_ROOT / "ci" / "last-python-free-run.sh"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "go-python-free.yml"
PKG = "github.com/x/y/internal/p"


def _run(cmd: list[str], env: dict[str, str] | None = None, cwd: Path | None = None):
    merged = {
        k: v
        for k, v in os.environ.items()
        if k not in ("GITHUB_ACTIONS", "PYTHON_TRIPWIRE_ALLOW_LOCAL")
    }
    merged.update(env or {})
    return subprocess.run(
        cmd, capture_output=True, text=True, env=merged, cwd=cwd, check=False
    )


def _events(*items: tuple[str, str, str, str]) -> str:
    """go test -json lines: (action, test, output, package)."""
    lines = []
    for action, test, output, package in items:
        event = {"Action": action, "Package": package or PKG}
        if test:
            event["Test"] = test
        if output:
            event["Output"] = output
        lines.append(json.dumps(event))
    return "\n".join(lines) + "\n"


def _classify(tmp_path: Path, stream: str, log: str = ""):
    out = tmp_path / "stream.json"
    out.write_text(stream)
    hits = tmp_path / "x.hits"
    args = ["bash", str(RATCHET), "classify", str(out), str(hits)]
    if log:
        path = tmp_path / "trip.log"
        path.write_text(log)
        args.append(str(path))
    result = _run(args)
    return result, hits.read_text() if hits.exists() else ""


def test_a_failed_test_that_names_the_tripwire_is_a_hit_and_other_failures_are_not(
    tmp_path: Path,
) -> None:
    stream = _events(
        ("output", "TestSpawns", "PYTHON TRIPWIRE: python3 invoked with: -c 1\n", ""),
        ("fail", "TestSpawns", "", ""),
        (
            "output",
            "TestViaPyoracle/sub",
            "fork/exec /python-tripwire/no-python: no such file or directory\n",
            "",
        ),
        ("fail", "TestViaPyoracle/sub", "", ""),
        ("fail", "TestViaPyoracle", "", ""),
    )
    result, hits = _classify(tmp_path, stream)
    assert result.returncode == 0, result.stderr
    assert hits.splitlines() == [f"{PKG}\tTestSpawns", f"{PKG}\tTestViaPyoracle"]

    regression = stream + _events(
        ("output", "TestReal", "boom\n", ""), ("fail", "TestReal", "", "")
    )
    result, _ = _classify(tmp_path, regression)
    assert result.returncode == 1
    assert "NON-TRIPWIRE FAILURE" in result.stderr and "TestReal" in result.stderr


def test_a_package_level_failure_and_an_empty_stream_are_failures_not_passes(
    tmp_path: Path,
) -> None:
    result, _ = _classify(tmp_path, _events(("fail", "", "", "")))
    assert result.returncode == 1
    assert "package-level failure" in result.stderr
    empty = tmp_path / "empty.json"
    empty.write_text("")
    result = _run(
        ["bash", str(RATCHET), "classify", str(empty), str(tmp_path / "e.hits")]
    )
    assert result.returncode == 2 and "did not happen" in result.stderr


def test_a_swallowed_python_start_is_unattributed_and_fails(tmp_path: Path) -> None:
    log = "pid=1 ppid=2 shim=python3 argv=-c 1 parent=/tmp/go-build/b001/p.test -test.run X\n"
    result, _ = _classify(tmp_path, _events(("pass", "TestQuiet", "", "")), log)
    assert result.returncode == 1
    assert "UNATTRIBUTED PYTHON START" in result.stderr


def _compare(tmp_path: Path, known: str, hits: dict[str, str]):
    listed = tmp_path / "known.tsv"
    listed.write_text(known)
    directory = tmp_path / "hits"
    directory.mkdir(exist_ok=True)
    for name, body in hits.items():
        (directory / name).write_text(body)
    return _run(["bash", str(RATCHET), "compare", str(listed), str(directory)])


def test_the_ratchet_is_green_only_when_the_hit_set_equals_the_list(
    tmp_path: Path,
) -> None:
    known = f"# header\n{PKG}\tTestA\tCHAOS-7001\n{PKG}\tTestB\tCHAOS-7002\n"
    both = f"{PKG}\tTestA\n{PKG}\tTestB\n"
    ok = _compare(tmp_path, known, {"a.hits": both})
    assert ok.returncode == 0 and "listed=2 hit=2 new=0 stale=0" in ok.stdout

    new = _compare(tmp_path, known, {"a.hits": both + f"{PKG}\tTestC\n"})
    assert (
        new.returncode == 1
        and "listed=2 hit=3 new=1 stale=0" in new.stdout
        and "TestC" in new.stderr
    )

    stale = _compare(tmp_path, known, {"a.hits": f"{PKG}\tTestA\n"})
    assert (
        stale.returncode == 1
        and "listed=2 hit=1 new=0 stale=1" in stale.stdout
        and "TestB" in stale.stderr
    )


def test_report_only_mode_prints_every_hit_and_exits_zero(tmp_path: Path) -> None:
    listed = tmp_path / "known.tsv"
    listed.write_text("# provisional\n")
    directory = tmp_path / "hits"
    directory.mkdir()
    (directory / "a.hits").write_text(f"{PKG}\tTestA\n")
    env = {"PYTHON_FREE_REPORT_ONLY": "1"}
    result = _run(["bash", str(RATCHET), "compare", str(listed), str(directory)], env)
    assert result.returncode == 0
    assert f"HIT\t{PKG}\tTestA" in result.stdout and "new=1" in result.stdout
    enforcing = _run(["bash", str(RATCHET), "compare", str(listed), str(directory)])
    assert enforcing.returncode == 1


def test_the_ratchet_refuses_a_list_row_without_a_ticket_and_a_run_that_reported_nothing(
    tmp_path: Path,
) -> None:
    bad = _compare(tmp_path, f"{PKG}\tTestA\n", {"a.hits": ""})
    assert bad.returncode == 2 and "CHAOS ticket" in bad.stderr
    listed = tmp_path / "known.tsv"
    listed.write_text("# empty list\n")
    empty = tmp_path / "nohits"
    empty.mkdir()
    result = _run(["bash", str(RATCHET), "compare", str(listed), str(empty)])
    assert result.returncode == 2 and "not a pass" in result.stderr


def test_the_tripwire_refuses_outside_github_actions_and_changes_nothing(
    tmp_path: Path,
) -> None:
    probe = (
        'source "$1"; rc=$?; echo "rc=$rc path=$PATH py=${DEV_HEALTH_PYTHON:-unset}"'
    )
    result = _run(["bash", "-c", probe, "x", str(TRIPWIRE)])
    assert "REFUSING" in result.stderr
    assert "py=unset" in result.stdout and "rc=1" in result.stdout


def test_the_armed_tripwire_names_the_caller_and_exits_97(tmp_path: Path) -> None:
    env = {"GITHUB_ACTIONS": "true", "RUNNER_TEMP": str(tmp_path)}
    probe = 'source "$1" 2>/dev/null; python3 -c 1; echo "rc=$?"; echo "py=$DEV_HEALTH_PYTHON"; cat "$PYTHON_TRIPWIRE_LOG"'
    result = _run(["bash", "-c", probe, "x", str(TRIPWIRE)], env)
    assert "PYTHON TRIPWIRE: python3 invoked with: -c 1" in result.stderr
    assert "rc=97" in result.stdout and "py=/python-tripwire/no-python" in result.stdout
    assert "shim=python3" in result.stdout


def test_the_tripwire_script_never_changes_a_mode_outside_its_own_shims() -> None:
    source = TRIPWIRE.read_text()
    chmods = [
        line.strip()
        for line in source.splitlines()
        if "chmod" in line and not line.strip().startswith("#")
    ]
    assert chmods == ['chmod +x "${PYTHON_TRIPWIRE_DIR}/${_python_tripwire_name}"']


def test_the_workflow_is_path_scoped_and_keeps_python_out_of_the_shard() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    triggers = workflow.get("on") or workflow.get(True)
    assert "schedule" in triggers and "workflow_dispatch" in triggers
    # A pull request runs it only when it edits the workflow, the tripwire/ratchet scripts or the list.
    assert triggers["pull_request"]["paths"] == triggers["push"]["paths"]
    assert "ci/python_free_known.tsv" in triggers["pull_request"]["paths"]
    shard_steps = workflow["jobs"]["shard"]["steps"]
    assert not any("setup-python" in str(step.get("uses", "")) for step in shard_steps)
    chmod_step = next(
        step for step in shard_steps if "non-executable" in step.get("name", "")
    )
    assert chmod_step["if"] == "runner.environment == 'github-hosted'"
    run_step = next(
        step
        for step in shard_steps
        if str(step.get("name", "")).startswith("Run isolated")
    )
    assert run_step["env"]["GO_PYTHON_FREE"] == "1"
    assert workflow["jobs"]["ratchet"]["if"] == "always()"


def _gh(tmp_path: Path, runs: list[dict]) -> Path:
    fake = tmp_path / "gh"
    fake.write_text(
        "#!/bin/sh\ncat <<'EOF'\n" + json.dumps({"workflow_runs": runs}) + "\nEOF\n"
    )
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
    return fake


def test_the_three_state_watch_never_reads_a_missing_run_as_green(
    tmp_path: Path,
) -> None:
    now = 1_790_000_000
    fresh = "2026-09-30T00:00:00Z"
    env = lambda gh: {
        "LAST_PYTHON_FREE_RUN_GH": str(gh),
        "LAST_PYTHON_FREE_RUN_NOW": str(now),
    }  # noqa: E731
    created = int(
        subprocess.run(
            ["date", "-u", "-d", fresh, "+%s"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )
    env_fresh = lambda gh: {
        "LAST_PYTHON_FREE_RUN_GH": str(gh),
        "LAST_PYTHON_FREE_RUN_NOW": str(created + 3600),
    }  # noqa: E731
    run = {"id": 7, "head_sha": "abc", "created_at": fresh}
    passed = _run(
        ["bash", str(LAST_RUN)],
        env_fresh(_gh(tmp_path, [{**run, "conclusion": "success"}])),
    )
    failed = _run(
        ["bash", str(LAST_RUN)],
        env_fresh(_gh(tmp_path, [{**run, "conclusion": "failure"}])),
    )
    none = _run(["bash", str(LAST_RUN)], env(_gh(tmp_path, [])))
    old = _run(
        ["bash", str(LAST_RUN)],
        {
            "LAST_PYTHON_FREE_RUN_GH": str(
                _gh(tmp_path, [{**run, "conclusion": "success"}])
            ),
            "LAST_PYTHON_FREE_RUN_NOW": str(created + 9 * 3600),
        },
    )
    assert (passed.returncode, "PASSED" in passed.stdout) == (0, True)
    assert (failed.returncode, "FAILED" in failed.stdout) == (1, True)
    assert (none.returncode, "NOT RUN" in none.stdout) == (3, True)
    assert (old.returncode, "NOT RUN" in old.stdout) == (3, True)
