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
            "fork/exec /python-tripwire.AbC123/python3: no such file or directory\n",
            "",
        ),
        ("fail", "TestViaPyoracle/sub", "", ""),
        ("fail", "TestViaPyoracle", "", ""),
    )
    result, hits = _classify(tmp_path, stream)
    assert result.returncode == 0, result.stderr
    assert hits.splitlines() == [
        f"{PKG}\tTestSpawns\ttripwire",
        f"{PKG}\tTestViaPyoracle\ttripwire",
    ]

    regression = stream + _events(
        ("output", "TestReal", "boom\n", ""), ("fail", "TestReal", "", "")
    )
    result, _ = _classify(tmp_path, regression)
    assert result.returncode == 1
    assert "NON-TRIPWIRE FAILURE" in result.stderr and "TestReal" in result.stderr


def test_a_subtest_regression_is_not_hidden_by_a_sibling_that_hit_the_tripwire(
    tmp_path: Path,
) -> None:
    stream = _events(
        ("output", "TestMixed/python", "PYTHON TRIPWIRE: python3 invoked\n", ""),
        ("fail", "TestMixed/python", "", ""),
        ("output", "TestMixed/regression", "assertion failed: got=4 want=5\n", ""),
        ("fail", "TestMixed/regression", "", ""),
        ("fail", "TestMixed", "", ""),
    )
    result, hits = _classify(tmp_path, stream)
    assert result.returncode == 1
    assert "NON-TRIPWIRE FAILURE" in result.stderr and "TestMixed" in result.stderr
    assert hits == ""


def test_exit_status_97_alone_counts_only_when_the_tripwire_log_shows_the_test_binary(
    tmp_path: Path,
) -> None:
    stream = _events(
        ("output", "TestShows", "helper: exit status 97\n", ""),
        ("fail", "TestShows", "", ""),
    )
    unrelated, _ = _classify(tmp_path, stream)
    assert unrelated.returncode == 1 and "NON-TRIPWIRE FAILURE" in unrelated.stderr
    log = "pid=1 ppid=2 shim=python3 argv=-c 1 parent=/tmp/go-build/b001/p.test -test.run X\n"
    related, hits = _classify(tmp_path, stream, log)
    assert related.returncode == 0, related.stderr
    assert hits.splitlines() == [f"{PKG}\tTestShows\ttripwire"]


def test_a_skip_that_says_python_is_missing_is_a_second_class_but_a_gated_skip_is_not(
    tmp_path: Path,
) -> None:
    stream = _events(
        (
            "output",
            "TestNeedsPython",
            "neither python3 nor python is on PATH; skipping\n",
            "",
        ),
        ("skip", "TestNeedsPython", "", ""),
        (
            "output",
            "TestGated",
            "the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1\n",
            "",
        ),
        ("skip", "TestGated", "", ""),
        ("output", "TestOtherSkip", "skipping: no docker\n", ""),
        ("skip", "TestOtherSkip", "", ""),
    )
    result, hits = _classify(tmp_path, stream)
    assert result.returncode == 0, result.stderr
    assert hits.splitlines() == [f"{PKG}\tTestNeedsPython\tskips-without-python"]


def test_a_skip_caused_by_the_shim_failing_is_the_skip_class_and_not_a_swallowed_start(
    tmp_path: Path,
) -> None:
    stream = _events(
        (
            "output",
            "TestNeedsCPython",
            "x_test.go:9: /tmp/python-tripwire.AbC/python3 did not report its implementation: exit status 97\n",
            "",
        ),
        ("skip", "TestNeedsCPython", "", ""),
    )
    log = "pid=1 ppid=2 shim=python3 argv=-c 1 parent=/tmp/go-build/b001/p.test -test.run X\n"
    result, hits = _classify(tmp_path, stream, log)
    assert result.returncode == 0, result.stderr
    assert hits.splitlines() == [f"{PKG}\tTestNeedsCPython\tskips-without-python"]


def test_a_closed_list_row_needs_a_class(tmp_path: Path) -> None:
    result = _compare(tmp_path, f"{PKG}\tTestA\tCHAOS-7001\tmaybe\n", {"a.hits": ""})
    assert result.returncode == 2 and "without a class" in result.stderr


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


# Real `go test -json` streams (go1.27.0, captured from throwaway packages, never authored). No test fails
# in any of these packages: each is a package-level failure. bf/x does not build; bf/y's TestMain exits 1
# after its tests pass; bf/z's test binary is killed after its tests pass; bf/w's dependency bf/dep does not
# build; bf/v fails go test's vet step.
_PACKAGE_LEVEL_STREAM = (
    "\n".join(
        [
            '{"ImportPath":"bf/x [bf/x.test]","Action":"build-output","Output":"# bf/x [bf/x.test]\\n"}',
            '{"ImportPath":"bf/x [bf/x.test]","Action":"build-output","Output":"x/x.go:2:23: undefined: undefinedThing\\n"}',
            '{"ImportPath":"bf/x [bf/x.test]","Action":"build-fail"}',
            '{"Time":"2026-10-01T18:14:55.75850528Z","Action":"start","Package":"bf/x"}',
            '{"Time":"2026-10-01T18:14:55.758573Z","Action":"output","Package":"bf/x","Output":"FAIL\\tbf/x [build failed]\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:14:55.75859Z","Action":"fail","Package":"bf/x","Elapsed":0,"FailedBuild":"bf/x [bf/x.test]"}',
            '{"Time":"2026-10-01T18:14:55.909743223Z","Action":"start","Package":"bf/y"}',
            '{"Time":"2026-10-01T18:14:55.916602759Z","Action":"run","Package":"bf/y","Test":"TestY"}',
            '{"Time":"2026-10-01T18:14:55.916681319Z","Action":"output","Package":"bf/y","Test":"TestY","Output":"=== RUN   TestY\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:14:55.916698119Z","Action":"output","Package":"bf/y","Test":"TestY","Output":"--- PASS: TestY (0.00s)\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:14:55.916702919Z","Action":"pass","Package":"bf/y","Test":"TestY","Elapsed":0}',
            '{"Time":"2026-10-01T18:14:55.916709559Z","Action":"output","Package":"bf/y","Output":"PASS\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:14:55.91720508Z","Action":"output","Package":"bf/y","Output":"FAIL\\tbf/y\\t0.007s\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:14:55.9172418Z","Action":"fail","Package":"bf/y","Elapsed":0.008}',
            '{"Time":"2026-10-01T18:14:55.976367294Z","Action":"start","Package":"bf/z"}',
            '{"Time":"2026-10-01T18:14:55.98326883Z","Action":"run","Package":"bf/z","Test":"TestZ"}',
            '{"Time":"2026-10-01T18:14:55.98335299Z","Action":"output","Package":"bf/z","Test":"TestZ","Output":"=== RUN   TestZ\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:14:55.98337235Z","Action":"output","Package":"bf/z","Test":"TestZ","Output":"--- PASS: TestZ (0.00s)\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:14:55.98338075Z","Action":"pass","Package":"bf/z","Test":"TestZ","Elapsed":0}',
            '{"Time":"2026-10-01T18:14:55.98338651Z","Action":"output","Package":"bf/z","Output":"PASS\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:14:55.983884071Z","Action":"output","Package":"bf/z","Output":"signal: killed\\n"}',
            '{"Time":"2026-10-01T18:14:55.983922192Z","Action":"output","Package":"bf/z","Output":"FAIL\\tbf/z\\t0.007s\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:14:55.983930912Z","Action":"fail","Package":"bf/z","Elapsed":0.008}',
            '{"ImportPath":"bf/dep","Action":"build-output","Output":"# bf/dep\\n"}',
            '{"ImportPath":"bf/dep","Action":"build-output","Output":"dep/dep.go:2:23: undefined: missing\\n"}',
            '{"ImportPath":"bf/dep","Action":"build-fail"}',
            '{"Time":"2026-10-01T18:17:52.621785462Z","Action":"start","Package":"bf/w"}',
            '{"Time":"2026-10-01T18:17:52.621927063Z","Action":"output","Package":"bf/w","Output":"FAIL\\tbf/w [build failed]\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:17:52.621952823Z","Action":"fail","Package":"bf/w","Elapsed":0,"FailedBuild":"bf/dep"}',
            '{"ImportPath":"bf/v [bf/v.test]","Action":"build-output","Output":"# bf/v\\n"}',
            '{"ImportPath":"bf/v [bf/v.test]","Action":"build-output","Output":"# [bf/v]\\n"}',
            '{"ImportPath":"bf/v [bf/v.test]","Action":"build-output","Output":"v/v_test.go:3:39: fmt.Printf format %d has arg \\"s\\" of wrong type string\\n"}',
            '{"ImportPath":"bf/v [bf/v.test]","Action":"build-fail"}',
            '{"Time":"2026-10-01T18:17:52.836137028Z","Action":"start","Package":"bf/v"}',
            '{"Time":"2026-10-01T18:17:52.836183628Z","Action":"output","Package":"bf/v","Output":"FAIL\\tbf/v [build failed]\\n","OutputType":"frame"}',
            '{"Time":"2026-10-01T18:17:52.836195268Z","Action":"fail","Package":"bf/v","Elapsed":0,"FailedBuild":"bf/v [bf/v.test]"}',
        ]
    )
    + "\n"
)


def _failure_blocks(stderr: str) -> dict[str, str]:
    """The detail lines printed under each NON-TRIPWIRE FAILURE line, by package."""
    blocks: dict[str, str] = {}
    current = ""
    for line in stderr.splitlines():
        if line.startswith("NON-TRIPWIRE FAILURE: "):
            current = line.split()[2]
            blocks[current] = ""
        elif current and line.startswith("    | "):
            blocks[current] += line[len("    | ") :] + "\n"
        else:
            current = ""
    return blocks


def test_a_package_level_failure_prints_why_the_package_failed(tmp_path: Path) -> None:
    result, _ = _classify(tmp_path, _PACKAGE_LEVEL_STREAM)
    assert result.returncode == 1
    blocks = _failure_blocks(result.stderr)
    assert set(blocks) == {"bf/x", "bf/y", "bf/z", "bf/w", "bf/v"}, result.stderr
    # The build's own output: go test -json reports it under ImportPath "bf/x [bf/x.test]", not Package.
    assert "undefined: undefinedThing" in blocks["bf/x"], result.stderr
    assert "[build failed]" in blocks["bf/x"]
    # A dependency that does not build: its output is under ImportPath "bf/dep", named by FailedBuild.
    assert "undefined: missing" in blocks["bf/w"], result.stderr
    # go test's vet step.
    assert 'arg "s" of wrong type string' in blocks["bf/v"], result.stderr
    # A TestMain that exits 1 after every test passed: the binary's own PASS, then the package's FAIL.
    assert "PASS\nFAIL\tbf/y" in blocks["bf/y"], result.stderr
    # A killed test binary.
    assert "signal: killed" in blocks["bf/z"], result.stderr
    # Each block holds its own package's output only.
    assert "undefined" not in blocks["bf/y"] + blocks["bf/z"] + blocks["bf/v"]
    assert "undefinedThing" not in blocks["bf/w"] and "missing" not in blocks["bf/x"]
    assert "signal: killed" not in blocks["bf/x"] + blocks["bf/y"] + blocks["bf/w"]
    # Not a test's own output: that is printed for a failed test, under its own name.
    assert not any(
        "=== RUN" in block or "--- PASS" in block for block in blocks.values()
    )


def test_a_package_level_failure_with_no_output_says_where_to_look(
    tmp_path: Path,
) -> None:
    result, _ = _classify(tmp_path, _events(("fail", "", "", "")))
    assert result.returncode == 1
    assert "holds no output for this package" in result.stderr


def _block(stderr: str, package: str) -> list[str]:
    return _failure_blocks(stderr)[package].splitlines()


def test_a_long_output_keeps_its_first_and_its_last_lines_and_counts_what_it_cut(
    tmp_path: Path,
) -> None:
    # The cause can be the first line (a TestMain says why, then prints a long cleanup) or the last (the
    # assertion after a long log). Both ends stay, and the cut is counted and says where the rest is.
    package_stream = _events(
        ("output", "", "CAUSE: required migration version is unsupported\n", ""),
        *[("output", "", f"cleanup diagnostic {i:03d}\n", "") for i in range(100)],
        ("output", "", "FAIL\tgithub.com/x/y/internal/p\t0.1s\n", ""),
        ("fail", "", "", ""),
    )
    result, _ = _classify(tmp_path, package_stream)
    lines = _block(result.stderr, PKG)
    assert lines[0] == "CAUSE: required migration version is unsupported", result.stderr
    assert lines[-1].startswith("FAIL\t")
    assert len(lines) == 40 + 1 + 60
    assert (
        lines[40].startswith("... (2 lines left out here;")
        and "python-free-stream-" in lines[40]
    )
    assert (
        lines[39] == "cleanup diagnostic 038" and lines[41] == "cleanup diagnostic 041"
    )

    test_stream = _events(
        ("output", "TestLong", "FIRST: the request that was sent\n", ""),
        *[("output", "TestLong", f"progress {i:03d}\n", "") for i in range(150)],
        ("output", "TestLong", "LAST: the assertion that failed\n", ""),
        ("fail", "TestLong", "", ""),
    )
    result, _ = _classify(tmp_path, test_stream)
    lines = _block(result.stderr, PKG)
    assert lines[0] == "FIRST: the request that was sent", result.stderr
    assert lines[-1] == "LAST: the assertion that failed"
    assert lines[40].startswith("... (52 lines left out here;")
    assert len(lines) == 101

    # An output that fits is printed whole, with no cut line.
    fits = _events(
        *[("output", "", f"line {i:03d}\n", "") for i in range(100)],
        ("fail", "", "", ""),
    )
    result, _ = _classify(tmp_path, fits)
    lines = _block(result.stderr, PKG)
    assert lines == [f"line {i:03d}" for i in range(100)], result.stderr


def test_the_workflow_keeps_each_shard_stream_apart_from_the_hits() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    for job, label in (
        ("shard", "${{ matrix.target }}-${{ matrix.shard }}"),
        ("unit", "unit"),
    ):
        uploads = [
            step["with"]
            for step in workflow["jobs"][job]["steps"]
            if "upload-artifact" in str(step.get("uses", ""))
        ]
        streams = [u for u in uploads if u["path"].endswith("/python-free/*.json")]
        assert len(streams) == 1, (job, uploads)
        assert streams[0]["name"] == "python-free-stream-" + label
        # The run step always writes its stream: none is a measurement that did not happen.
        assert streams[0]["if-no-files-found"] == "error"
        hits = [u for u in uploads if u["path"].endswith("/python-free/*.hits")]
        assert len(hits) == 1 and hits[0]["name"] == "python-free-hits-" + label
        # An upload keeps something only when it runs after the step that writes it, and when that step
        # failed too: a red shard is the one whose stream is read.
        steps = workflow["jobs"][job]["steps"]
        ran = next(
            index
            for index, step in enumerate(steps)
            if step.get("env", {}).get("GO_PYTHON_FREE") == "1"
        )
        for index, step in enumerate(steps):
            if "upload-artifact" in str(step.get("uses", "")):
                assert index > ran, step["name"]
                assert step.get("if") == "always()", step["name"]
    # The ratchet compares the hits only: a stream must never land in its download.
    download = next(
        step["with"]
        for step in workflow["jobs"]["ratchet"]["steps"]
        if "download-artifact" in str(step.get("uses", ""))
    )
    assert download["pattern"] == "python-free-hits-*"
    assert not "python-free-stream-unit".startswith(download["pattern"].rstrip("*"))


def test_a_swallowed_python_start_is_unattributed_and_fails(tmp_path: Path) -> None:
    log = "pid=1 ppid=2 shim=python3 argv=-c 1 parent=/tmp/go-build/b001/p.test -test.run X\n"
    result, _ = _classify(tmp_path, _events(("pass", "TestQuiet", "", "")), log)
    assert result.returncode == 1
    assert "SWALLOWED START" in result.stderr


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
    known = f"# header\n{PKG}\tTestA\tCHAOS-7001\ttripwire\n{PKG}\tTestB\tCHAOS-7002\ttripwire\n"
    both = f"{PKG}\tTestA\ttripwire\n{PKG}\tTestB\ttripwire\n"
    ok = _compare(tmp_path, known, {"a.hits": both})
    assert ok.returncode == 0 and "listed=2 hit=2 new=0 stale=0" in ok.stdout

    new = _compare(tmp_path, known, {"a.hits": both + f"{PKG}\tTestC\ttripwire\n"})
    assert (
        new.returncode == 1
        and "listed=2 hit=3 new=1 stale=0" in new.stdout
        and "TestC" in new.stderr
    )

    stale = _compare(tmp_path, known, {"a.hits": f"{PKG}\tTestA\ttripwire\n"})
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
    (directory / "a.hits").write_text(f"{PKG}\tTestA\ttripwire\n")
    env = {"PYTHON_FREE_REPORT_ONLY": "1"}
    result = _run(["bash", str(RATCHET), "compare", str(listed), str(directory)], env)
    assert result.returncode == 0
    assert f"HIT\t{PKG}\tTestA\ttripwire" in result.stdout and "new=1" in result.stdout
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
    assert "rc=97" in result.stdout
    assert (
        f"py={tmp_path}/python-tripwire." in result.stdout
        and "/python3" in result.stdout
    )
    assert "shim=python3" in result.stdout


def test_arming_the_tripwire_changes_no_file_mode_outside_its_own_directory(
    tmp_path: Path,
) -> None:
    system = tmp_path / "usr-bin"
    system.mkdir()
    fake_python = system / "python3"
    fake_python.write_text("#!/bin/sh\necho real\n")
    fake_python.chmod(0o755)
    before = fake_python.stat().st_mode
    env = {
        "GITHUB_ACTIONS": "true",
        "RUNNER_TEMP": str(tmp_path),
        "PATH": f"{system}:{os.environ['PATH']}",
    }
    probe = 'source "$1" 2>/dev/null; command -v python3; ls "$PYTHON_TRIPWIRE_DIR"'
    result = _run(["bash", "-c", probe, "x", str(TRIPWIRE)], env)
    assert fake_python.stat().st_mode == before
    assert result.stdout.splitlines()[0].startswith(str(tmp_path / "python-tripwire."))
    assert "python3" in result.stdout.splitlines()[1:]
    assert ".shim-template" not in result.stdout


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
    assert set(workflow["jobs"]["ratchet"]["needs"]) == {"plan", "shard", "unit"}
    unit_steps = workflow["jobs"]["unit"]["steps"]
    assert not any("setup-python" in str(step.get("uses", "")) for step in unit_steps)
    compare = next(
        step
        for step in workflow["jobs"]["ratchet"]["steps"]
        if "closed list" in step.get("name", "")
    )
    assert "PYTHON_FREE_REPORT_ONLY" not in compare.get("env", {}), (
        "the ratchet must enforce"
    )


def test_every_closed_list_row_cites_a_ticket_and_the_list_has_no_duplicates() -> None:
    rows = [
        line.split("\t")
        for line in (REPO_ROOT / "ci" / "python_free_known.tsv")
        .read_text()
        .splitlines()
        if line and not line.startswith("#")
    ]
    assert rows and all(
        len(row) == 4
        and row[2].startswith("CHAOS-")
        and row[3] in ("tripwire", "skips-without-python")
        for row in rows
    )
    assert len({(row[0], row[1]) for row in rows}) == len(rows)


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
    created = int(
        subprocess.run(
            ["date", "-u", "-d", fresh, "+%s"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )

    def env(gh: Path) -> dict[str, str]:
        return {
            "LAST_PYTHON_FREE_RUN_GH": str(gh),
            "LAST_PYTHON_FREE_RUN_NOW": str(now),
        }

    def env_fresh(gh: Path) -> dict[str, str]:
        return {
            "LAST_PYTHON_FREE_RUN_GH": str(gh),
            "LAST_PYTHON_FREE_RUN_NOW": str(created + 3600),
        }

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
