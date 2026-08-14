"""Contract tests for the shared mutation harness.

Every failure mode the harness claims to close has a test here that shows it
FAILING on a synthetic tree. A guard with no demonstrated failure mode is the
same class of false pass the harness was built to prevent, so "it passes on the
real tree" is not accepted as evidence for any of these.

The proof commands are real argv arrays run as real subprocesses -- a fake
runner would let the harness's own protocol go untested -- but they inspect a
text file instead of compiling anything, so these tests need no Go toolchain and
run in milliseconds.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts.mutation_harness import (
    _LOCK_HELD_BY_UNKNOWN,
    VERDICT_BASELINE_FAILED,
    VERDICT_INVALID,
    VERDICT_KILLED,
    VERDICT_SURVIVED,
    VERDICT_SURVIVED_DECLARED,
    HarnessError,
    Result,
    _load_sharding_plan,
    _ProgressEmitter,
    accept_manual_repair,
    acquire_lock,
    coordinator_run,
    main,
    recover_run,
    restore,
    run_plan,
    stage_execution_tree,
    verify,
)

SOURCE_NAME = "widget.txt"
GUARD = "if guardEnabled {"
DISABLED_GUARD = "if false && guardEnabled {"

# A proof command that passes only while the guard line is intact. Real argv,
# real subprocess, no toolchain.
GUARD_PRESENT_PROOF = [
    sys.executable,
    "-c",
    (
        "import pathlib,sys;"
        f"text=pathlib.Path({SOURCE_NAME!r}).read_text();"
        f"sys.exit(0 if {GUARD!r} in text else 1)"
    ),
]
ALWAYS_PASSES_PROOF = [sys.executable, "-c", "pass"]


def _source(body: str) -> str:
    return f"package widget\n\nfunc Run() {{\n\t{body}\n\t\tdoThing()\n\t}}\n}}\n"


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    (tmp_path / SOURCE_NAME).write_text(_source(GUARD), encoding="utf-8")
    return tmp_path


def _plan(tmp_path: Path, mutations: list[dict[str, object]]) -> Path:
    path = tmp_path / "plan.json"
    path.write_text(
        json.dumps({"schema_version": 1, "name": "synthetic", "mutations": mutations}),
        encoding="utf-8",
    )
    return path


def _mutation(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "id": "M1",
        "file": SOURCE_NAME,
        "find": GUARD,
        "replace": DISABLED_GUARD,
        "proof": [GUARD_PRESENT_PROOF],
        "rationale": "the guard must be observable",
    }
    base.update(overrides)
    return base


def test_verify_passes_on_a_clean_tree(tree: Path) -> None:
    assert verify(tree) == []


def test_coordinator_state_blocks_verify_until_recovery(tree: Path) -> None:
    state = tree / ".mutation-harness"
    state.mkdir()
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "coordinator_run": {
                    "run_id": "run-3807",
                    "lifecycle": "staging",
                },
            }
        ),
        encoding="utf-8",
    )

    assert verify(tree) == [
        "coordinator run run-3807 remains in lifecycle staging. No result from "
        "this tree is trustworthy until it is recovered. Recover it with: "
        "python3 scripts/mutation_harness.py recover-run --run-id run-3807"
    ]


def test_sharding_interfaces_are_wired_fail_closed_seams(tree: Path) -> None:
    plan = _plan(tree, [_mutation()])

    assert _load_sharding_plan(plan, {}) is None
    with pytest.raises(HarnessError, match="git rev-parse"):
        stage_execution_tree(
            tree,
            tree / "shard",
            run_id="run-3807",
            shard_index=0,
            source_manifest_digest="source-digest",
            plan_digest="plan-digest",
            workspace_inputs=(),
        )
    with pytest.raises(HarnessError, match="does not opt in to sharding"):
        coordinator_run(
            tree,
            plan,
            None,
            False,
            requested_shards=2,
            progress="human",
        )
    with pytest.raises(HarnessError, match="state"):
        recover_run(tree, "run-3807")


def test_progress_flushes_the_selected_stderr_stream(tree: Path) -> None:
    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding="utf-8", line_buffering=False)
    try:
        emitter = _ProgressEmitter(tree, "flush-test", "human", stream=stream)
        emitter.emit("run_started", completed=0, active=0, total=1, phase="running")

        assert raw.getvalue() == (
            b"mutation harness: run_started (0/1 complete, 0 active)\n"
        )
    finally:
        stream.detach()


def test_progress_modes_keep_events_off_stdout(
    tree: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    plan = _plan(tree, [_mutation()])

    run_plan(tree, plan, None, assert_all_killed=False, progress="human")
    human = capsys.readouterr()
    assert human.out == ""
    assert "mutation harness: mutation_started M1" in human.err

    run_plan(tree, plan, None, assert_all_killed=False, progress="jsonl")
    jsonl = capsys.readouterr()
    assert jsonl.out == ""
    stderr_events = [json.loads(line) for line in jsonl.err.splitlines()]
    assert [event["sequence"] for event in stderr_events] == list(
        range(1, len(stderr_events) + 1)
    )
    assert [event["event"] for event in stderr_events] == [
        "run_started",
        "mutation_started",
        "mutation_finished",
        "run_finished",
    ]

    run_plan(tree, plan, None, assert_all_killed=False, progress="none")
    silent = capsys.readouterr()
    assert silent.out == ""
    assert silent.err == ""


def test_events_exclude_captured_proof_output(tree: Path) -> None:
    proof_secret = "proof-output-must-not-enter-events"
    noisy_proof = [
        sys.executable,
        "-c",
        (
            "import pathlib,sys;"
            f"print({proof_secret!r});"
            f"text=pathlib.Path({SOURCE_NAME!r}).read_text();"
            f"sys.exit(0 if {GUARD!r} in text else 1)"
        ),
    ]
    plan = _plan(tree, [_mutation(proof=[noisy_proof])])

    results, _ = run_plan(tree, plan, None, assert_all_killed=False)

    assert proof_secret in results[0].detail
    run_directory = next((tree / ".mutation-harness" / "runs").iterdir())
    event_text = (run_directory / "events.jsonl").read_text(encoding="utf-8")
    assert proof_secret not in event_text
    events = [json.loads(line) for line in event_text.splitlines()]
    assert [event["event"] for event in events] == [
        "run_started",
        "mutation_started",
        "mutation_finished",
        "run_finished",
    ]
    assert events[0] | {"elapsed_ms": 0} == {
        "schema_version": 1,
        "sequence": 1,
        "run_id": events[0]["run_id"],
        "event": "run_started",
        "phase": "running",
        "completed": 0,
        "active": 0,
        "total": 1,
        "elapsed_ms": 0,
    }
    assert events[1]["plan_ordinal"] == 0
    assert events[1]["mutation_id"] == "M1"
    assert (events[1]["completed"], events[1]["active"], events[1]["total"]) == (
        0,
        1,
        1,
    )
    assert events[2]["verdict"] == "KILLED"
    assert (events[2]["completed"], events[2]["active"], events[2]["total"]) == (
        1,
        0,
        1,
    )
    assert all("shard_index" not in event for event in events)
    assert all(
        set(event)
        <= {
            "schema_version",
            "sequence",
            "run_id",
            "event",
            "shard_index",
            "plan_ordinal",
            "mutation_id",
            "phase",
            "verdict",
            "completed",
            "active",
            "total",
            "elapsed_ms",
        }
        for event in events
    )


def test_result_stream_survives_an_abort_before_the_next_mutation(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.mutation_harness as harness

    plan = _plan(tree, [_mutation(id="M1"), _mutation(id="M2")])
    calls = 0

    def controlled_run(
        _root: Path, mutation: object, _snapshot_directory: Path
    ) -> Result:
        nonlocal calls
        calls += 1
        if calls == 1:
            return Result("M1", VERDICT_KILLED, detail="measured before abort")
        run_directory = next((tree / ".mutation-harness" / "runs").iterdir())
        records = [
            json.loads(line)
            for line in (run_directory / "results.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        assert [record["id"] for record in records] == ["M1"]
        raise HarnessError("controlled abort before M2 completes")

    monkeypatch.setattr(harness, "_run_one", controlled_run)

    with pytest.raises(HarnessError, match="controlled abort"):
        run_plan(tree, plan, None, assert_all_killed=False)

    run_directory = next((tree / ".mutation-harness" / "runs").iterdir())
    result_lines = (
        (run_directory / "results.jsonl").read_text(encoding="utf-8").splitlines()
    )
    assert len(result_lines) == 1
    assert json.loads(result_lines[0]) == {
        "schema_version": 1,
        "run_id": run_directory.name,
        "plan_ordinal": 0,
        "id": "M1",
        "verdict": "KILLED",
        "detail": "measured before abort",
        "failing_proof": None,
        "warnings": [],
    }
    assert not (tree / ".mutation-harness" / "report.json").exists()


def test_serial_report_preserves_result_fields_and_adds_stream_paths(
    tree: Path,
) -> None:
    plan = _plan(
        tree,
        [
            _mutation(
                proof=[ALWAYS_PASSES_PROOF],
                build=ALWAYS_PASSES_PROOF,
            )
        ],
    )

    run_plan(tree, plan, None, assert_all_killed=False)

    report = json.loads(
        (tree / ".mutation-harness" / "report.json").read_text(encoding="utf-8")
    )
    assert report["schema_version"] == 1
    assert report["plan"] == "synthetic"
    assert report["plan_path"] == str(plan)
    assert report["results"] == [
        {
            "id": "M1",
            "verdict": "SURVIVED",
            "detail": (
                "every proof command still passed with the mutation applied. Either "
                "a test is missing, or this mutation is invalid (no-op, wrong target, "
                "or asserted against the constant it mutates). Classify it by "
                "declaring expected_survivor_reason, or add the missing test."
            ),
            "failing_proof": None,
            "warnings": [],
        }
    ]
    assert report["mode"] == "serial"
    assert report["event_log"] == (
        f".mutation-harness/runs/{report['run_id']}/events.jsonl"
    )
    assert report["result_stream"] == (
        f".mutation-harness/runs/{report['run_id']}/results.jsonl"
    )


@pytest.mark.parametrize("shard_arguments", [[], ["--shards", "1"]])
def test_serial_cli_stdout_is_byte_compatible_golden(
    tree: Path,
    capsys: pytest.CaptureFixture[str],
    shard_arguments: list[str],
) -> None:
    plan = _plan(
        tree,
        [
            _mutation(
                proof=[ALWAYS_PASSES_PROOF],
                build=ALWAYS_PASSES_PROOF,
            )
        ],
    )

    assert (
        main(
            [
                "--root",
                str(tree),
                "run",
                "--plan",
                str(plan),
                "--progress",
                "none",
                *shard_arguments,
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == (
        "\n"
        "| mutation | verdict | detail |\n"
        "| --- | --- | --- |\n"
        "| M1 | SURVIVED | every proof command still passed with the mutation "
        "applied. Either a test is missing, or this mutation is inva |\n"
        "\n"
        "\n"
        "M1 SURVIVED:\n"
        "every proof command still passed with the mutation applied. Either a "
        "test is missing, or this mutation is invalid (no-op, wrong target, or "
        "asserted against the constant it mutates). Classify it by declaring "
        "expected_survivor_reason, or add the missing test.\n"
    )


def test_cli_progress_defaults_to_human(
    tree: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    plan = _plan(tree, [_mutation()])

    assert main(["--root", str(tree), "run", "--plan", str(plan)]) == 0

    captured = capsys.readouterr()
    assert "| M1 | KILLED |" in captured.out
    assert "mutation harness: run_started" in captured.err
    assert "mutation harness: mutation_finished M1" in captured.err


def test_durable_event_is_readable_while_the_proof_child_is_alive(
    tree: Path,
) -> None:
    release = tree / "release-proof"
    proof_pid = tree / "proof.pid"
    blocking_proof = [
        sys.executable,
        "-c",
        (
            "import os\n"
            "import pathlib\n"
            "import time\n"
            f"pathlib.Path({proof_pid.name!r}).write_text(str(os.getpid()))\n"
            f"release = pathlib.Path({release.name!r})\n"
            "while not release.exists():\n"
            "    time.sleep(0.01)\n"
            f"text = pathlib.Path({SOURCE_NAME!r}).read_text()\n"
            f"raise SystemExit(0 if {GUARD!r} in text else 1)\n"
        ),
    ]
    plan = _plan(tree, [_mutation(proof=[blocking_proof])])
    script = Path(__file__).parents[2] / "scripts" / "mutation_harness.py"
    stdout_path = tree / "harness.stdout"
    stderr_path = tree / "harness.stderr"
    with stdout_path.open("wb") as stdout_file, stderr_path.open("wb") as stderr_file:
        process = subprocess.Popen(
            [
                sys.executable,
                str(script),
                "--root",
                str(tree),
                "run",
                "--plan",
                str(plan),
                "--assert-all-killed",
                "--progress",
                "none",
            ],
            cwd=script.parents[1],
            stdout=stdout_file,
            stderr=stderr_file,
            start_new_session=True,
        )
    assert process.stdout is None
    assert process.stderr is None

    try:
        deadline = time.monotonic() + 10
        live_event: dict[str, object] | None = None
        while time.monotonic() < deadline:
            event_logs = list(
                (tree / ".mutation-harness" / "runs").glob("*/events.jsonl")
            )
            if proof_pid.exists() and event_logs:
                child_pid = int(proof_pid.read_text(encoding="utf-8"))
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    pass
                else:
                    raw_lines = event_logs[0].read_text(encoding="utf-8").splitlines()
                    parsed = [json.loads(line) for line in raw_lines]
                    live_event = next(
                        (
                            event
                            for event in parsed
                            if event["event"] == "mutation_started"
                        ),
                        None,
                    )
                    if live_event is not None and process.poll() is None:
                        break
            time.sleep(0.01)

        assert live_event is not None
        assert live_event["mutation_id"] == "M1"
        assert process.poll() is None
        release.write_text("continue\n", encoding="utf-8")
        assert process.wait(timeout=10) == 0
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)

    event_log = next((tree / ".mutation-harness" / "runs").glob("*/events.jsonl"))
    event_lines = event_log.read_text(encoding="utf-8").splitlines()
    assert event_lines
    assert all(isinstance(json.loads(line), dict) for line in event_lines)


def test_a_mutation_the_proof_notices_is_killed(tree: Path) -> None:
    plan = _plan(tree, [_mutation()])
    results, exit_code = run_plan(tree, plan, None, assert_all_killed=True)
    assert [result.verdict for result in results] == [VERDICT_KILLED]
    assert exit_code == 0


def test_the_file_is_byte_identical_after_a_run(tree: Path) -> None:
    """The restore is the whole risk, so pin it on bytes, not on a build."""

    before = (tree / SOURCE_NAME).read_bytes()
    plan = _plan(tree, [_mutation()])
    run_plan(tree, plan, None, assert_all_killed=False)
    assert (tree / SOURCE_NAME).read_bytes() == before
    assert verify(tree) == []


def test_restore_preserves_edits_that_no_commit_contains(tree: Path) -> None:
    """Closes the `git checkout` restore that reverted unrelated edits.

    The harness must restore the bytes it snapshotted, not any committed
    version. Here the working tree carries an edit that exists in no commit --
    indeed in no git repository at all -- and it must survive the run.
    """

    marked = _source(GUARD).replace("doThing()", "doThing() // local WIP edit")
    (tree / SOURCE_NAME).write_text(marked, encoding="utf-8")
    plan = _plan(tree, [_mutation()])
    run_plan(tree, plan, None, assert_all_killed=False)
    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == marked


def test_an_unnoticed_mutation_survives_and_fails_the_assertion(tree: Path) -> None:
    plan = _plan(tree, [_mutation(proof=[ALWAYS_PASSES_PROOF])])
    results, exit_code = run_plan(tree, plan, None, assert_all_killed=True)
    assert [result.verdict for result in results] == [VERDICT_SURVIVED]
    assert exit_code == 1


def test_a_declared_survivor_does_not_fail_the_assertion(tree: Path) -> None:
    """An unobservable mutation is a reviewed decision, not a silent footnote."""

    plan = _plan(
        tree,
        [
            _mutation(
                proof=[ALWAYS_PASSES_PROOF],
                expected_survivor_reason=(
                    "no behavioural test can observe a derived identifier; "
                    "covered by a direct unit assertion instead"
                ),
            )
        ],
    )
    results, exit_code = run_plan(tree, plan, None, assert_all_killed=True)
    assert [result.verdict for result in results] == [VERDICT_SURVIVED_DECLARED]
    assert exit_code == 0


def test_a_red_baseline_is_reported_and_the_file_is_never_mutated(tree: Path) -> None:
    """A mutation measured against an already-failing test proves nothing."""

    before = (tree / SOURCE_NAME).read_bytes()
    always_fails = [sys.executable, "-c", "raise SystemExit(1)"]
    plan = _plan(tree, [_mutation(proof=[always_fails])])
    results, exit_code = run_plan(tree, plan, None, assert_all_killed=False)
    assert [result.verdict for result in results] == [VERDICT_BASELINE_FAILED]
    assert exit_code == 1
    assert (tree / SOURCE_NAME).read_bytes() == before


def test_an_anchor_matching_twice_is_refused(tree: Path) -> None:
    """Closes the mutation that landed in a doc comment.

    The real incident: the anchor text appeared twice in the file, the mutation
    hit the doc comment instead of the SQL, and the result read as a coverage
    gap. An occurrence count that disagrees with the declaration is refused.
    """

    doubled = _source(GUARD).replace(
        "package widget", f"package widget\n\n// Explains {GUARD} in prose."
    )
    (tree / SOURCE_NAME).write_text(doubled, encoding="utf-8")
    plan = _plan(tree, [_mutation()])
    results, _ = run_plan(tree, plan, None, assert_all_killed=False)
    assert results[0].verdict == VERDICT_INVALID
    assert "matches 2 time(s)" in results[0].detail
    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == doubled


def test_an_anchor_on_a_comment_line_is_refused(tree: Path) -> None:
    (tree / SOURCE_NAME).write_text(
        "package widget\n// TODO tighten the guard\n", encoding="utf-8"
    )
    plan = _plan(
        tree,
        [
            _mutation(
                find="TODO tighten the guard",
                replace="TODO loosen the guard",
                proof=[ALWAYS_PASSES_PROOF],
            )
        ],
    )
    results, _ = run_plan(tree, plan, None, assert_all_killed=False)
    assert results[0].verdict == VERDICT_INVALID
    assert "is a comment" in results[0].detail


def test_a_comment_anchor_is_allowed_when_declared(tree: Path) -> None:
    (tree / SOURCE_NAME).write_text(
        "package widget\n// TODO tighten the guard\n", encoding="utf-8"
    )
    plan = _plan(
        tree,
        [
            _mutation(
                find="TODO tighten the guard",
                replace="TODO loosen the guard",
                proof=[ALWAYS_PASSES_PROOF],
                allow_comment_anchor=True,
                expected_survivor_reason="deliberately mutating documentation",
            )
        ],
    )
    results, _ = run_plan(tree, plan, None, assert_all_killed=False)
    assert results[0].verdict == VERDICT_SURVIVED_DECLARED


def test_a_leftover_mutation_blocks_verify_and_refuses_a_new_run(tree: Path) -> None:
    """Closes the trap-based restore that dies with its harness.

    Reproduces the real incident exactly: `if false && guard` is left on disk,
    the file still parses and would still build, and the operator believes a
    monitor restored it. Crash-safety is the record on disk, so `verify` fails
    and a new run refuses to start.
    """

    (tree / SOURCE_NAME).write_text(_source(DISABLED_GUARD), encoding="utf-8")
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "snapshots" / "M1-abc.snapshot").write_bytes(b"unused")
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-abc.snapshot",
                },
            }
        ),
        encoding="utf-8",
    )

    blockers = verify(tree)
    assert len(blockers) == 1
    assert "still applied" in blockers[0]

    plan = _plan(tree, [_mutation()])
    with pytest.raises(HarnessError, match="refusing to run"):
        run_plan(tree, plan, None, assert_all_killed=False)


def test_a_disabled_guard_is_caught_even_though_the_file_still_parses(
    tree: Path,
) -> None:
    """The regression test for verifying a restore with the wrong tool.

    `if false && guard` is valid source: a compile or parse check passes. This
    asserts the parse-equivalent really does pass, so the test cannot silently
    stop exercising the point, and that `verify` fails regardless.
    """

    mutated = _source(DISABLED_GUARD)
    (tree / SOURCE_NAME).write_text(mutated, encoding="utf-8")
    state = tree / ".mutation-harness"
    state.mkdir()
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {"mutation_id": "M1", "file": SOURCE_NAME},
            }
        ),
        encoding="utf-8",
    )

    # The stand-in for `go build`: the mutated text is still well-formed, so a
    # structural check is blind to it. That blindness is the whole point.
    assert DISABLED_GUARD in mutated
    assert mutated.count("{") == mutated.count("}")

    assert verify(tree) != []


def test_restore_repairs_a_leftover_and_clears_the_record(tree: Path) -> None:
    original = (tree / SOURCE_NAME).read_bytes()
    mutated = _source(DISABLED_GUARD)
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "snapshots" / "M1-x.snapshot").write_bytes(original)
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-x.snapshot",
                    "mutated_sha256": hashlib.sha256(mutated.encode()).hexdigest(),
                },
            }
        ),
        encoding="utf-8",
    )
    (tree / SOURCE_NAME).write_text(mutated, encoding="utf-8")

    message = restore(tree)
    assert "restored" in message
    assert (tree / SOURCE_NAME).read_bytes() == original
    assert verify(tree) == []


def test_restore_refuses_a_snapshot_that_does_not_match_its_digest(tree: Path) -> None:
    """A snapshot that has been tampered with must never be written over source."""

    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "snapshots" / "M1-x.snapshot").write_bytes(b"not the original")
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-x.snapshot",
                    "original_sha256": "0" * 64,
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(HarnessError, match="does not match its recorded digest"):
        restore(tree)


def test_restore_refuses_when_the_snapshot_is_missing(tree: Path) -> None:
    state = tree / ".mutation-harness"
    state.mkdir()
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "gone.snapshot",
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(HarnessError, match="is missing"):
        restore(tree)


def test_an_anchor_present_in_test_sources_warns(tree: Path) -> None:
    """Closes the mutation killed only by a test asserting the mutated constant."""

    (tree / "widget_test.go").write_text(
        f"package widget\n\n// asserts against {GUARD} directly\n", encoding="utf-8"
    )
    plan = _plan(tree, [_mutation()])
    results, _ = run_plan(tree, plan, None, assert_all_killed=False)
    assert results[0].warnings
    assert "also appears in test sources" in results[0].warnings[0]


def test_a_shell_string_proof_is_rejected(tree: Path) -> None:
    plan = _plan(tree, [_mutation(proof=["go test ./... && echo ok"])])
    with pytest.raises(HarnessError, match="argv array"):
        run_plan(tree, plan, None, assert_all_killed=False)


def test_a_mutation_without_a_rationale_is_rejected(tree: Path) -> None:
    mutation = _mutation()
    del mutation["rationale"]
    plan = _plan(tree, [mutation])
    with pytest.raises(HarnessError, match="rationale"):
        run_plan(tree, plan, None, assert_all_killed=False)


def test_a_mutation_without_a_proof_is_rejected(tree: Path) -> None:
    """A mutation nothing observes is not a measurement."""

    plan = _plan(tree, [_mutation(proof=[])])
    with pytest.raises(HarnessError, match="at least one command"):
        run_plan(tree, plan, None, assert_all_killed=False)


def test_an_identity_mutation_is_rejected(tree: Path) -> None:
    plan = _plan(tree, [_mutation(replace=GUARD)])
    with pytest.raises(HarnessError, match="identical"):
        run_plan(tree, plan, None, assert_all_killed=False)


def test_duplicate_mutation_ids_are_rejected(tree: Path) -> None:
    plan = _plan(tree, [_mutation(), _mutation()])
    with pytest.raises(HarnessError, match="duplicate mutation id"):
        run_plan(tree, plan, None, assert_all_killed=False)


def test_a_path_escaping_the_root_is_rejected(tree: Path) -> None:
    plan = _plan(tree, [_mutation(file="../outside.txt")])
    with pytest.raises(HarnessError, match="escapes the repository root"):
        run_plan(tree, plan, None, assert_all_killed=False)


def test_a_live_run_in_its_baseline_is_not_reported_clean(tree: Path) -> None:
    """Closes the time-of-check hole in the gate.

    A run holds the lock during its baseline but has not applied anything yet, so
    a check that looks only for an applied record reports clean -- and the gate
    proceeds while the mutation lands underneath it. A held lock alone has to be
    enough to distrust the tree.
    """

    acquire_lock(tree)
    assert (tree / ".mutation-harness" / "state.json").exists() is False
    blockers = verify(tree)
    assert len(blockers) == 1
    assert "IN PROGRESS" in blockers[0]
    assert "has not applied its mutation yet" in blockers[0]

    plan = _plan(tree, [_mutation()])
    with pytest.raises(HarnessError, match="refusing to run"):
        run_plan(tree, plan, None, assert_all_killed=False)


def test_a_stale_lock_from_a_dead_run_names_the_repair(tree: Path) -> None:
    """No `pgrep` waiters: contention is an immediate, explained refusal."""

    lock = acquire_lock(tree)
    # A pid that cannot exist: the lock is stale, so verify falls through and
    # acquire_lock is what refuses -- and it must say how to break the lock.
    (lock / "pid").write_text("2147483646\n", encoding="utf-8")
    plan = _plan(tree, [_mutation()])
    with pytest.raises(HarnessError, match="holds"):
        run_plan(tree, plan, None, assert_all_killed=False)


def test_only_selects_a_subset_and_rejects_unknown_ids(tree: Path) -> None:
    plan = _plan(tree, [_mutation(), _mutation(id="M2")])
    results, _ = run_plan(tree, plan, {"M2"}, assert_all_killed=False)
    assert [result.identifier for result in results] == ["M2"]
    with pytest.raises(HarnessError, match="unknown mutations"):
        run_plan(tree, plan, {"M9"}, assert_all_killed=False)


def test_a_proof_that_stops_passing_after_the_restore_aborts_the_run(
    tree: Path,
) -> None:
    """Byte-identity is necessary but not sufficient.

    If the proof passes clean, fails mutated, then fails again on the restored
    tree, something changed underneath the run and every result is void. The
    harness must stop rather than report the mutation as killed.
    """

    counter = tree / "invocations"
    counter.write_text("", encoding="utf-8")
    # Passes on call 1, fails on call 2 and after: the mutation looks killed,
    # then the post-restore check exposes the instability.
    flaky = [
        sys.executable,
        "-c",
        (
            "import pathlib;"
            f"p=pathlib.Path({str(counter.name)!r});"
            "n=len(p.read_text());"
            "p.write_text('x'*(n+1));"
            "raise SystemExit(0 if n == 0 else 1)"
        ),
    ]
    plan = _plan(tree, [_mutation(proof=[flaky])])
    with pytest.raises(HarnessError, match="now fails on the restored tree"):
        run_plan(tree, plan, None, assert_all_killed=False)


def _write_applied_record(tree: Path) -> Path:
    state = tree / ".mutation-harness"
    state.mkdir(exist_ok=True)
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {"mutation_id": "M1", "file": SOURCE_NAME},
            }
        ),
        encoding="utf-8",
    )
    return state


def test_verify_distinguishes_a_live_run_from_a_leak(tree: Path) -> None:
    """A mutation on disk means "wait" or "repair", never an ambiguous both.

    Conflating the two produced a false accusation against a working harness.
    Both are gate failures -- no result from a mutated tree is trustworthy --
    but the advice is opposite, so the messages must be distinguishable.
    """

    state = _write_applied_record(tree)
    lock = state / "lock"
    lock.mkdir()
    # Our own pid is certainly alive, which is exactly the live-run case.
    (lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    live = verify(tree)
    assert len(live) == 1
    assert "IN PROGRESS" in live[0]
    # The liveness signal is "a process with that pid exists", and pids are
    # reused, so the message must disclose that rather than asserting the run is
    # definitely alive -- otherwise a leak whose pid got recycled reads as
    # permanently in-progress and the advice is to wait forever.
    assert "pids are reused" in live[0]
    assert "--force" in live[0]

    # A pid that cannot exist turns the same state into the leak diagnosis.
    (lock / "pid").write_text("2147483646\n", encoding="utf-8")
    leaked = verify(tree)
    assert len(leaked) == 1
    assert "leaked from a dead run" in leaked[0]
    assert "restore" in leaked[0]


def test_every_known_pid_message_names_a_repair_that_actually_works(
    tree: Path,
) -> None:
    """Advice the tool's own guards refuse is not advice.

    `verify` used to answer a live-but-unrelated pid with "wait" (forever, if
    that process is long-lived) and, on the applied path, with
    `restore --force` -- which refuses BY DESIGN while a process with the
    recorded pid exists, so following it could only ever fail. Both branches must
    name the step that works: break the lock by hand, having checked the pid.
    """

    import shutil

    original = (tree / SOURCE_NAME).read_bytes()
    lock = tree / ".mutation-harness" / "lock"
    lock.mkdir(parents=True)
    # Our own pid is certainly alive. Standing in for a reused pid: a process
    # exists, and it is not the harness.
    (lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    # No record: nothing is applied, so breaking the lock is the whole repair.
    not_applied = verify(tree)
    assert len(not_applied) == 1
    assert f"rm -rf {lock}" in not_applied[0]

    _stranded_record(tree, snapshot=True)
    (lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")
    applied = verify(tree)
    assert len(applied) == 1
    assert f"rm -rf {lock}" in applied[0]
    assert "mutation_harness.py restore" in applied[0]

    # The old recommendation, followed literally: it cannot work, by design.
    with pytest.raises(HarnessError, match="--force refused"):
        restore(tree, force=True)
    assert verify(tree) != []

    # The current one, followed literally: it works. That is the property --
    # not which sentence is printed, but whether doing what it says repairs the
    # tree.
    shutil.rmtree(lock)
    assert "restored" in restore(tree)
    assert (tree / SOURCE_NAME).read_bytes() == original
    assert verify(tree) == []


def test_every_emitted_recovery_command_survives_the_shell(tree: Path) -> None:
    """Parsed, not substring-matched. A hint that mis-parses is worse than none.

    Two ways these commands break on a legitimate path. A space splits it into
    two arguments -- and for `rm -rf` that means deleting something that was
    never named. A leading dash is read as options by every tool that has any,
    which `shlex.quote` does NOT fix, so the path needs an option terminator too.
    """

    import scripts.mutation_harness as harness

    for path in ("src/my guard.py", "-guard.py", "src/it's here.py"):
        inner = harness._accept_hint(path).split("$(", 1)[1].rsplit(")", 1)[0]
        words = shlex.split(inner)[: shlex.split(inner).index("|")]
        assert words[0] == "shasum"
        # Exactly one non-option argument, and it is the whole path.
        assert words[words.index("--") + 1 :] == [path], path

    # The lock paths in `verify`'s advice go to `rm -rf`, where a split argument
    # deletes a prefix of the intended target. Drive the real messages.
    awkward = tree / "a lock dir"
    awkward.mkdir()
    (awkward / ".mutation-harness" / "lock").mkdir(parents=True)
    for blocker in verify(awkward):
        for line in blocker.splitlines():
            if "rm -rf" not in line:
                continue
            words = shlex.split(line[line.index("rm -rf") :].strip())
            assert words[:2] == ["rm", "-rf"]
            assert words[2:] == [str(awkward / ".mutation-harness" / "lock")], line


def test_a_baseline_build_that_edits_the_target_is_not_reported_as_untouched(
    tree: Path,
) -> None:
    """Every early return claims the file was never touched, so each must earn it.

    A build command that writes the target and then fails would otherwise leave
    changed source, no record and a clean `verify`, underneath a report saying
    nothing was mutated.
    """

    edited = _source(GUARD).replace("doThing()", "doThing() // written by the build")
    editing_build = [
        sys.executable,
        "-c",
        (
            "import pathlib;"
            f"pathlib.Path({SOURCE_NAME!r}).write_text({edited!r});"
            "raise SystemExit(1)"
        ),
    ]
    plan = _plan(tree, [_mutation(build=editing_build)])
    with pytest.raises(HarnessError, match="changed while the baseline build ran"):
        run_plan(tree, plan, None, assert_all_killed=False)

    # The build's write survives -- nothing overwrote it -- and no record was left.
    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == edited
    assert verify(tree) == []


def test_a_baseline_proof_that_edits_the_target_is_not_reported_as_untouched(
    tree: Path,
) -> None:
    """The other early return, which had the guard but no test proving it works.

    A failing build and a failing proof both return BASELINE_FAILED claiming the
    file was never touched. Only the build path was covered, so deleting the
    proof path's freshness check left every test green while a proof that wrote
    the target and then failed produced changed source, no record and a clean
    `verify`.
    """

    edited = _source(GUARD).replace("doThing()", "doThing() // written by the proof")
    editing_and_failing_proof = [
        sys.executable,
        "-c",
        (
            "import pathlib;"
            f"pathlib.Path({SOURCE_NAME!r}).write_text({edited!r});"
            "raise SystemExit(1)"
        ),
    ]
    plan = _plan(tree, [_mutation(proof=[editing_and_failing_proof])])
    with pytest.raises(HarnessError, match="changed while the baseline proofs ran"):
        run_plan(tree, plan, None, assert_all_killed=False)

    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == edited
    assert verify(tree) == []


def test_a_crash_while_applying_still_leaves_a_record_for_verify(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The applied record must be written before the file is touched.

    Written afterwards, a crash in the window between the write and the record
    leaves a mutated file that `verify` calls clean -- exactly the hole this
    harness exists to close. Simulates a hard crash (not a HarnessError) inside
    the apply step and asserts the tree is not reported clean.
    """

    import scripts.mutation_harness as harness

    def _explode(*_args: object, **_kwargs: object) -> None:
        raise KeyboardInterrupt("simulated kill mid-apply")

    monkeypatch.setattr(harness, "_apply", _explode)
    plan = _plan(tree, [_mutation()])
    with pytest.raises(KeyboardInterrupt):
        run_plan(tree, plan, None, assert_all_killed=False)

    blockers = verify(tree)
    assert blockers, "a crash during apply must leave the tree unverified"
    assert "still applied" in blockers[0]
    # And the record must be actionable: the snapshot it names has to exist, or
    # the advice to run `restore` would be a dead end. Here the crash preceded
    # any write, so restore has nothing to put back -- it must say so and still
    # clear the record rather than reporting a write it did not perform.
    message = restore(tree)
    assert "already matches" in message
    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == _source(GUARD)
    assert verify(tree) == []


def test_an_invalid_mutation_clears_its_record(tree: Path) -> None:
    """The pessimistic record must not strand a run that never wrote anything."""

    doubled = _source(GUARD).replace(
        "package widget", f"package widget\n\n// Explains {GUARD} in prose."
    )
    (tree / SOURCE_NAME).write_text(doubled, encoding="utf-8")
    plan = _plan(tree, [_mutation()])
    results, _ = run_plan(tree, plan, None, assert_all_killed=False)
    assert results[0].verdict == VERDICT_INVALID
    assert verify(tree) == []


def test_the_cli_verify_command_exits_non_zero_on_a_leftover(tree: Path) -> None:
    state = tree / ".mutation-harness"
    state.mkdir()
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {"mutation_id": "M1", "file": SOURCE_NAME},
            }
        ),
        encoding="utf-8",
    )
    assert main(["--root", str(tree), "verify"]) == 1
    assert main(["--root", str(tree), "restore"]) == 2  # snapshot absent


def test_a_file_edited_during_the_baseline_is_never_overwritten(tree: Path) -> None:
    """The worst defect this tool could have: silently eating a developer's work.

    The snapshot is taken before a baseline that may run for minutes. If an
    editor, a formatter, or a proof command with a side effect saves during that
    window, writing the mutation from the stale snapshot destroys that change --
    and the restore destroys it a second time. Refuse, write nothing, and say so.
    """

    edited = _source(GUARD).replace("doThing()", "doThing() // saved mid-baseline")
    # A proof that mutates the tree stands in for the editor. It passes, so the
    # baseline succeeds and the harness reaches the point where it would write.
    editing_proof = [
        sys.executable,
        "-c",
        (
            "import pathlib;"
            f"pathlib.Path({SOURCE_NAME!r}).write_text({edited!r});"
            "raise SystemExit(0)"
        ),
    ]
    plan = _plan(tree, [_mutation(proof=[editing_proof])])
    with pytest.raises(HarnessError, match="changed while the baseline ran"):
        run_plan(tree, plan, None, assert_all_killed=False)

    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == edited
    assert verify(tree) == []


def test_a_file_replaced_during_observe_is_left_exactly_as_found(tree: Path) -> None:
    """Byte-identity is not enough if the bytes are no longer ours to restore.

    If the target holds neither the mutation nor the original when OBSERVE ends,
    a third party wrote it. Restoring the snapshot would lose that write and
    re-applying would lose it too, so the only safe action is to touch nothing,
    keep the record so nothing else runs, and hand it to a human.
    """

    foreign = "package widget\n// replaced by something else entirely\n"
    replacing_proof = [
        sys.executable,
        "-c",
        (
            "import pathlib,sys;"
            f"p=pathlib.Path({SOURCE_NAME!r});"
            "t=p.read_text();"
            f"p.write_text({foreign!r}) if 'false &&' in t else None;"
            "sys.exit(0)"
        ),
    ]
    plan = _plan(tree, [_mutation(proof=[replacing_proof])])
    with pytest.raises(HarnessError, match="changed during OBSERVE"):
        run_plan(tree, plan, None, assert_all_killed=False)

    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == foreign
    # The record is deliberately KEPT so the next invocation refuses to run.
    assert verify(tree) != []


def test_an_invalid_mutation_fails_the_run(tree: Path) -> None:
    """An anchor that drifted measured nothing, so the run must not exit 0.

    Previously INVALID returned exit 0, so a self-check whose anchor had moved
    reported success while part of the plan never executed.
    """

    doubled = _source(GUARD).replace(
        "package widget", f"package widget\n\n// Explains {GUARD} in prose."
    )
    (tree / SOURCE_NAME).write_text(doubled, encoding="utf-8")
    plan = _plan(tree, [_mutation()])
    results, exit_code = run_plan(tree, plan, None, assert_all_killed=False)
    assert results[0].verdict == VERDICT_INVALID
    assert exit_code == 1


def test_a_mutation_id_that_is_not_a_plain_name_is_rejected(tree: Path) -> None:
    """The id reaches the filesystem as part of the snapshot name."""

    for hostile in ("../../escape", "/absolute", "with/slash", ".leading-dot"):
        plan = _plan(tree, [_mutation(id=hostile)])
        with pytest.raises(HarnessError, match="not a plain name"):
            run_plan(tree, plan, None, assert_all_killed=False)


def test_restore_refuses_while_a_live_run_holds_the_lock(tree: Path) -> None:
    """Closes a hole reachable through this tool's own repair path.

    A `restore` racing a live run can write the snapshot and clear the record in
    the window before that run applies its mutation. The run then applies it,
    dies, and leaves a mutation with no record -- so `verify` reports clean.
    """

    original = (tree / SOURCE_NAME).read_bytes()
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "snapshots" / "M1-x.snapshot").write_bytes(original)
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-x.snapshot",
                    "mutated_sha256": hashlib.sha256(
                        _source(DISABLED_GUARD).encode()
                    ).hexdigest(),
                },
            }
        ),
        encoding="utf-8",
    )
    lock = state / "lock"
    lock.mkdir()
    (lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")
    (tree / SOURCE_NAME).write_text(_source(DISABLED_GUARD), encoding="utf-8")

    with pytest.raises(HarnessError, match="refusing to restore underneath it"):
        restore(tree)
    # Still mutated, and the record still stands: nothing was silently cleared.
    assert DISABLED_GUARD in (tree / SOURCE_NAME).read_text(encoding="utf-8")
    assert verify(tree) != []

    # --force must ALSO refuse while that pid is alive: evicting a live run's
    # lock lets it apply its mutation and die with nothing recording it.
    with pytest.raises(HarnessError, match="--force refused"):
        restore(tree, force=True)
    # Asserted as STATE, not as wording. Without the live-holder guard, --force
    # falls through and does three things it must not: writes the source, clears
    # the record, and deletes the live run's lock. A test that only matched the
    # message would report a kill for the specialised phrasing while every one of
    # those effects still happened.
    assert DISABLED_GUARD in (tree / SOURCE_NAME).read_text(encoding="utf-8")
    assert verify(tree) != []
    assert lock.is_dir(), "--force must not evict a live run's lock"

    # Once the holder is provably gone, --force is the documented escape.
    (lock / "pid").write_text("2147483646\n", encoding="utf-8")
    assert restore(tree, force=True).startswith("restored")
    assert (tree / SOURCE_NAME).read_bytes() == original


def test_restore_force_refuses_a_lock_whose_holder_is_unknown(tree: Path) -> None:
    """Force cannot prove a pid-less lock stale during mkdir -> pid.

    The lock owner may be a live run in the real gap between creating the lock
    directory and recording its pid. Evicting that lock lets the run continue
    after recovery clears its applied record, leaving an unrecorded mutation.
    Unknown ownership therefore requires the deliberate manual lock break.
    """

    state = _stranded_record(tree, snapshot=True)
    lock = state / "lock"
    lock.mkdir()

    with pytest.raises(HarnessError, match="holder is unknown"):
        restore(tree, force=True)

    assert DISABLED_GUARD in (tree / SOURCE_NAME).read_text(encoding="utf-8")
    assert (state / "state.json").is_file()
    assert lock.is_dir(), "--force must not evict a lock with an unknown holder"


def _stranded_record(
    tree: Path,
    *,
    snapshot: bool = False,
    proof: list[list[str]] | None = None,
) -> Path:
    """A record whose snapshot is gone -- the `git clean -fdX` case.

    `restore` cannot help here by design: there are no original bytes to write.
    That is the state finding #4 named, where every safe path refused and none
    of them left the operator anywhere to go.

    The recorded proof passes only while the guard line is intact, which is what
    a real plan's proof does: it is the command that NOTICES this mutation.
    """

    original = _source(GUARD)
    mutated = _source(DISABLED_GUARD)
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    if snapshot:
        (state / "snapshots" / "M1-x.snapshot").write_bytes(original.encode())
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-x.snapshot",
                    "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
                    "mutated_sha256": hashlib.sha256(mutated.encode()).hexdigest(),
                    "find": GUARD,
                    "replace": DISABLED_GUARD,
                    "proof": proof if proof is not None else [GUARD_PRESENT_PROOF],
                },
            }
        ),
        encoding="utf-8",
    )
    (tree / SOURCE_NAME).write_text(mutated, encoding="utf-8")
    return state


def _digest_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_a_by_hand_repair_can_be_accepted_and_clears_the_record(tree: Path) -> None:
    """The exit from a terminal-but-safe refusal.

    The operator has undone the mutation themselves and kept an unrelated local
    edit, so the file matches NEITHER recorded digest and `restore` refuses --
    correctly, because writing the snapshot would destroy that edit. Before this,
    the record could then only be cleared by a successful restore that can never
    happen: the gate stayed red with nothing left to do, and the only way out was
    deleting the record blind. A safe path that dead-ends teaches people to take
    the unsafe one.
    """

    _stranded_record(tree, snapshot=True)
    repaired = _source(GUARD).replace("doThing()", "doThing() // unrelated local work")
    (tree / SOURCE_NAME).write_text(repaired, encoding="utf-8")

    # Every existing path is a dead end, and says so with the way out.
    with pytest.raises(HarnessError, match="does not hold the mutation") as refusal:
        restore(tree)
    assert "accept --digest" in str(refusal.value)
    assert verify(tree) != []

    message = accept_manual_repair(tree, _digest_of(tree / SOURCE_NAME))
    assert "proof command(s) pass" in message
    # The record is gone, the gate is green, and the operator's edit survived:
    # acceptance clears a record, it never writes source.
    assert verify(tree) == []
    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == repaired


def test_accept_refuses_while_the_mutation_is_still_on_disk(tree: Path) -> None:
    """The leak itself is not a repair to acknowledge."""

    _stranded_record(tree)
    with pytest.raises(HarnessError, match="still holds the recorded mutation"):
        accept_manual_repair(tree, _digest_of(tree / SOURCE_NAME))
    assert verify(tree) != []


def test_accept_refuses_while_the_mutations_own_proof_still_fails(tree: Path) -> None:
    """The load-bearing check, and the one that closed a whole defect class.

    Three rounds of review each found a new way to defeat a TEXTUAL test for
    "is the mutation gone": counting anchors was satisfied by comments, blanking
    anchors before searching was defeated by overlapping occurrences, and any
    exact-string test is defeated by reformatting. They are one defect -- reading
    the current text cannot decide the question, because the mutation's effect
    survives being respelled.

    So the question asked is behavioural: does the command the plan declared as
    the thing that NOTICES this mutation pass now? Here the file is still
    disabled but respelled, so it matches neither recorded digest and contains
    neither exact string -- every textual check clears it -- and the proof still
    fails, which is the whole point.
    """

    _stranded_record(tree)
    respelled = _source(GUARD).replace(GUARD, "if false && (guardEnabled) {")
    (tree / SOURCE_NAME).write_text(respelled, encoding="utf-8")
    # Every textual check is satisfied: neither exact string is present, and the
    # content is not the recorded mutation.
    assert GUARD not in respelled
    assert DISABLED_GUARD not in respelled
    assert (
        _digest_of(tree / SOURCE_NAME)
        != hashlib.sha256(_source(DISABLED_GUARD).encode()).hexdigest()
    )

    with pytest.raises(HarnessError, match="own proof command still fails"):
        accept_manual_repair(tree, _digest_of(tree / SOURCE_NAME))
    assert verify(tree) != []


def test_accept_refuses_an_overlapping_anchor_reconstruction(tree: Path) -> None:
    """Round 5's Critical, kept as a test even though its mechanism is gone.

    With `find="aba"` and `replace="ba"`, repairing "baba" to "ababa" creates
    overlapping anchor occurrences, and the previous check -- blank the intact
    anchors, then look for the replacement -- erased its own evidence. The proof
    does not care: the text is not the original, so the command that notices the
    mutation still fails.
    """

    find, replace = "aba", "ba"
    original = "ababa\nababa\n"
    mutated = original.replace(find, replace)
    exact_proof = [
        sys.executable,
        "-c",
        (
            "import pathlib,sys;"
            f"sys.exit(0 if pathlib.Path({SOURCE_NAME!r}).read_text() == {original!r}"
            " else 1)"
        ),
    ]
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-x.snapshot",
                    "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
                    "mutated_sha256": hashlib.sha256(mutated.encode()).hexdigest(),
                    "find": find,
                    "replace": replace,
                    "proof": [exact_proof],
                },
            }
        ),
        encoding="utf-8",
    )
    (tree / SOURCE_NAME).write_text(mutated + "// unrelated\n", encoding="utf-8")

    with pytest.raises(HarnessError, match="own proof command still fails"):
        accept_manual_repair(tree, _digest_of(tree / SOURCE_NAME))
    assert verify(tree) != []

    # Genuinely repaired: the same command accepts.
    (tree / SOURCE_NAME).write_text(original, encoding="utf-8")
    assert "proof command(s) pass" in accept_manual_repair(
        tree, _digest_of(tree / SOURCE_NAME)
    )
    assert verify(tree) == []


def test_accept_refuses_when_the_replacement_text_is_still_present(tree: Path) -> None:
    """The cheap tripwire, which exists only to give a better message.

    It can refuse and never accept, so it decides nothing on its own -- but a
    plainly still-present replacement deserves to be named rather than reported
    as a failing test.
    """

    _stranded_record(tree)
    (tree / SOURCE_NAME).write_text(
        _source(DISABLED_GUARD) + "// reformatted since\n", encoding="utf-8"
    )
    with pytest.raises(HarnessError, match="replacement text is still in the file"):
        accept_manual_repair(tree, _digest_of(tree / SOURCE_NAME))
    assert verify(tree) != []


def test_accept_refuses_a_partial_repair_of_a_multi_site_mutation(tree: Path) -> None:
    """`str.replace` rewrites every site, so a partial repair is still mutated.

    The deleted-clause shape: `replace` is a substring of `find`, so the textual
    tripwire is skipped entirely and only the proof can answer.
    """

    find, replace = "checkA();checkB()", "checkB()"
    original = f"package widget\nfunc One() {{ {find} }}\nfunc Two() {{ {find} }}\n"
    mutated = original.replace(find, replace)
    # A proof that looks at the CODE lines, not at loose text -- so prose
    # containing the anchor cannot satisfy it.
    both_sites_proof = [
        sys.executable,
        "-c",
        (
            "import pathlib,sys;"
            f"t=pathlib.Path({SOURCE_NAME!r}).read_text();"
            f"sys.exit(0 if 'func One() {{ {find} }}' in t"
            f" and 'func Two() {{ {find} }}' in t else 1)"
        ),
    ]
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-x.snapshot",
                    "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
                    "mutated_sha256": hashlib.sha256(mutated.encode()).hexdigest(),
                    "find": find,
                    "replace": replace,
                    "proof": [both_sites_proof],
                },
            }
        ),
        encoding="utf-8",
    )

    partial = mutated.replace(replace, find, 1)
    (tree / SOURCE_NAME).write_text(partial, encoding="utf-8")
    with pytest.raises(HarnessError, match="own proof command still fails"):
        accept_manual_repair(tree, _digest_of(tree / SOURCE_NAME))
    assert verify(tree) != []

    # Round 4's reproduction, which defeated the counting check: leave BOTH code
    # sites mutated and add comments carrying the anchor text. A count is
    # satisfied; the proof is not.
    spoofed = mutated + f"// explains {find} in prose\n// and again: {find}\n"
    (tree / SOURCE_NAME).write_text(spoofed, encoding="utf-8")
    assert spoofed.count(find) == 2, "a counting check would pass here"
    with pytest.raises(HarnessError, match="own proof command still fails"):
        accept_manual_repair(tree, _digest_of(tree / SOURCE_NAME))
    assert verify(tree) != []

    # And it is a discriminator, not a blanket refusal.
    (tree / SOURCE_NAME).write_text(original, encoding="utf-8")
    assert "proof command(s) pass" in accept_manual_repair(
        tree, _digest_of(tree / SOURCE_NAME)
    )
    assert verify(tree) == []


def test_accept_refuses_a_digest_that_does_not_name_the_current_content(
    tree: Path,
) -> None:
    """The digest pins the decision to content, not to a moment.

    Without it, a file written between the operator looking and the record being
    cleared would be accepted on the strength of an inspection of different bytes.
    """

    _stranded_record(tree)
    repaired = _source(GUARD)
    (tree / SOURCE_NAME).write_text(repaired, encoding="utf-8")
    inspected = _digest_of(tree / SOURCE_NAME)
    # Something writes the file after the operator looked at it.
    (tree / SOURCE_NAME).write_text(repaired + "// written since\n", encoding="utf-8")

    with pytest.raises(HarnessError, match="but the file hashes to"):
        accept_manual_repair(tree, inspected)
    assert verify(tree) != []


def test_accept_refuses_a_record_that_carries_no_proof_commands(tree: Path) -> None:
    """No declared observation means no evidence, and no evidence means no accept."""

    _stranded_record(tree, proof=[])
    (tree / SOURCE_NAME).write_text(_source(GUARD), encoding="utf-8")
    with pytest.raises(HarnessError, match="carries no proof commands"):
        accept_manual_repair(tree, _digest_of(tree / SOURCE_NAME))
    assert verify(tree) != []


def test_restore_refuses_a_record_whose_two_digests_agree(tree: Path) -> None:
    """A record naming one digest as both original and mutated describes nothing.

    No mutation this harness produces leaves the file identical to the original --
    a no-op replacement is refused before any write -- so such a record is corrupt.
    It has to be rejected before the content short-circuit, which would otherwise
    clear the record on a digest that equally names the MUTATED content, with the
    mutation still on disk.
    """

    mutated = _source(DISABLED_GUARD)
    same = hashlib.sha256(mutated.encode()).hexdigest()
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-x.snapshot",
                    "original_sha256": same,
                    "mutated_sha256": same,
                    "find": GUARD,
                    "replace": DISABLED_GUARD,
                    "proof": [GUARD_PRESENT_PROOF],
                },
            }
        ),
        encoding="utf-8",
    )
    (tree / SOURCE_NAME).write_text(mutated, encoding="utf-8")

    with pytest.raises(HarnessError, match="same digest as both"):
        restore(tree)
    # The mutation is still there and the record still stands.
    assert DISABLED_GUARD in (tree / SOURCE_NAME).read_text(encoding="utf-8")
    assert verify(tree) != []


def test_accept_refuses_while_a_live_run_holds_the_lock(tree: Path) -> None:
    """Acceptance is a record write, so it serialises like every other one."""

    state = _stranded_record(tree)
    (tree / SOURCE_NAME).write_text(_source(GUARD), encoding="utf-8")
    lock = state / "lock"
    lock.mkdir()
    (lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    with pytest.raises(HarnessError, match="holds"):
        accept_manual_repair(tree, _digest_of(tree / SOURCE_NAME))
    assert verify(tree) != []
    assert lock.is_dir()


def test_a_hand_undone_mutation_clears_even_with_the_snapshot_deleted(
    tree: Path,
) -> None:
    """Content is examined before the snapshot is required.

    A proof command running `git clean -fdX` deletes the snapshots, because
    `.mutation-harness/` is gitignored. `restore` used to reject the missing
    snapshot BEFORE looking at the file, so an operator who had already undone
    the mutation could not clear the record at all -- a refusal to write bytes
    that are no longer needed.
    """

    _stranded_record(tree)
    (tree / SOURCE_NAME).write_text(_source(GUARD), encoding="utf-8")
    assert "already matches" in restore(tree)
    assert verify(tree) == []


def test_restore_names_the_acceptance_path_when_the_snapshot_is_gone(
    tree: Path,
) -> None:
    """A refusal without an exit is what sends people to `rm` the record."""

    _stranded_record(tree)
    (tree / SOURCE_NAME).write_text(
        _source(GUARD).replace("doThing()", "doThing() // local"), encoding="utf-8"
    )
    with pytest.raises(HarnessError, match="is missing") as refusal:
        restore(tree)
    assert "accept --digest" in str(refusal.value)


def test_the_cli_accepts_a_by_hand_repair(tree: Path) -> None:
    _stranded_record(tree)
    repaired = _source(GUARD).replace("doThing()", "doThing() // local")
    (tree / SOURCE_NAME).write_text(repaired, encoding="utf-8")

    assert main(["--root", str(tree), "verify"]) == 1
    assert main(["--root", str(tree), "restore"]) == 2
    assert (
        main(
            [
                "--root",
                str(tree),
                "accept",
                "--digest",
                _digest_of(tree / SOURCE_NAME),
            ]
        )
        == 0
    )
    assert main(["--root", str(tree), "verify"]) == 0


def test_a_run_records_the_proof_commands_for_the_acceptance_path(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The acceptance check is only as real as the record behind it.

    Crash mid-apply so a record survives, then read it: without the proof
    commands on it, `accept` has nothing to run and degrades to the blanket
    override it must never be.
    """

    import scripts.mutation_harness as harness

    def _explode(*_args: object, **_kwargs: object) -> None:
        raise KeyboardInterrupt("simulated kill mid-apply")

    monkeypatch.setattr(harness, "_apply", _explode)
    plan = _plan(tree, [_mutation()])
    with pytest.raises(KeyboardInterrupt):
        run_plan(tree, plan, None, assert_all_killed=False)

    record = json.loads(
        (tree / ".mutation-harness" / "state.json").read_text(encoding="utf-8")
    )["applied"]
    assert record["proof"] == [GUARD_PRESENT_PROOF]
    assert record["find"] == GUARD
    assert record["replace"] == DISABLED_GUARD


def test_restore_refuses_a_recorded_path_outside_the_repository(tree: Path) -> None:
    """Recovery must re-validate the path, not trust the record."""

    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "snapshots" / "M1-x.snapshot").write_bytes(b"anything")
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": "../outside.txt",
                    "snapshot": "M1-x.snapshot",
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(HarnessError, match="escapes the repository root"):
        restore(tree)


def test_unparseable_state_is_not_treated_as_absent(tree: Path) -> None:
    """A state file that exists but is corrupt may still record a mutation."""

    state = tree / ".mutation-harness"
    state.mkdir()
    (state / "state.json").write_text("{ not json", encoding="utf-8")
    with pytest.raises(HarnessError, match="could not be parsed"):
        verify(tree)


def test_state_that_raises_a_real_oserror_is_not_treated_as_absent(
    tree: Path,
) -> None:
    """`is_file()` answers False for "cannot stat" as well as "absent".

    A recorded mutation behind an I/O error would otherwise read as no mutation
    and the gate would report clean. Malformed JSON does NOT exercise this: that
    path raises after a successful read. This makes `read_bytes()` itself fail
    with a non-ENOENT OSError -- a directory where the file should be, which
    raises IsADirectoryError (EISDIR) on macOS and Linux alike, with none of the
    root-or-not variability a permission arrangement would carry.
    """

    state = tree / ".mutation-harness"
    state.mkdir()
    (state / "state.json").mkdir()

    # The mechanism itself, asserted rather than assumed: if a future Python
    # made this ENOENT or succeeded, the test below would pass while proving
    # nothing about the OSError branch.
    with pytest.raises(OSError) as raised:
        (state / "state.json").read_bytes()
    assert not isinstance(raised.value, FileNotFoundError)

    with pytest.raises(HarnessError, match="could not be read"):
        verify(tree)


def test_restore_refuses_content_that_is_not_the_recorded_mutation(tree: Path) -> None:
    """Recovery must refuse the same way the in-run path does.

    Without an expected mutated digest, `restore` treats any non-original content
    as safe to overwrite -- so a crash after a proof or an editor touched the file
    turns `verify`'s own advice into the data loss this tool exists to prevent.
    """

    original = (tree / SOURCE_NAME).read_bytes()
    mutated = _source(DISABLED_GUARD)
    edited_after = mutated.replace("doThing()", "doThing() // saved after the crash")
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "snapshots" / "M1-x.snapshot").write_bytes(original)
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-x.snapshot",
                    "mutated_sha256": hashlib.sha256(mutated.encode()).hexdigest(),
                },
            }
        ),
        encoding="utf-8",
    )
    (tree / SOURCE_NAME).write_text(edited_after, encoding="utf-8")

    with pytest.raises(HarnessError, match="does not hold the mutation"):
        restore(tree)
    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == edited_after
    assert verify(tree) != []


def test_restore_refuses_a_record_with_no_mutated_digest(tree: Path) -> None:
    """An interrupted run that never wrote leaves no mutated digest.

    Content differing from the snapshot then came from somewhere else, and
    overwriting it would be a guess.
    """

    original = (tree / SOURCE_NAME).read_bytes()
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "snapshots" / "M1-x.snapshot").write_bytes(original)
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": SOURCE_NAME,
                    "snapshot": "M1-x.snapshot",
                    "mutated_sha256": None,
                },
            }
        ),
        encoding="utf-8",
    )
    (tree / SOURCE_NAME).write_text("something else entirely\n", encoding="utf-8")

    with pytest.raises(HarnessError, match="no mutated digest"):
        restore(tree)


def test_a_lock_without_a_pid_file_is_treated_as_held(tree: Path) -> None:
    """`mkdir` and the pid write are not atomic, and that gap is a real state.

    Treating "no pid file" as "no live run" lets the gate report clean while a run
    is starting. Unknown is reported as held: a false wait costs patience, a false
    clean costs a trusted-but-void test result.

    The assertions are on the ACTIONABLE part, not on the diagnosis. An earlier
    version asserted only that the message said "pid unknown", which it did --
    while the surrounding sentence interpolated the sentinel and told the reader
    to "wait for pid -1". Since `verify` is the first stage of
    ci/local_validate.sh, that left the gate red forever behind advice nobody
    could act on: a message with no action in it is a failure of this check, so
    the test has to be able to see one.
    """

    state = tree / ".mutation-harness"
    lock = state / "lock"
    lock.mkdir(parents=True)

    blockers = verify(tree)
    assert len(blockers) == 1
    assert f"pid {_LOCK_HELD_BY_UNKNOWN}" not in blockers[0], "the sentinel leaked"
    assert "Wait for" not in blockers[0]
    assert f"rm -rf {lock}" in blockers[0]
    assert "STALE" in blockers[0]

    # The same lock with a mutation recorded is a DIFFERENT instruction: break
    # the lock, then repair the file. Falling into the pid-shaped branch here
    # would name a pid that does not exist for a tree that needs repairing.
    _write_applied_record(tree)
    with_mutation = verify(tree)
    assert len(with_mutation) == 1
    assert f"pid {_LOCK_HELD_BY_UNKNOWN}" not in with_mutation[0], "the sentinel leaked"
    assert f"rm -rf {lock}" in with_mutation[0]
    assert "mutation_harness.py restore" in with_mutation[0]


def test_atomic_write_preserves_mode_and_leaves_no_temporary(tree: Path) -> None:
    """A rename replaces the inode, so the mode must be carried across.

    Without this an executable comes back non-executable while a byte-digest check
    still reports success -- a restore that certifies itself while having changed
    something that matters.
    """

    import scripts.mutation_harness as harness

    target = tree / "script.sh"
    target.write_text("#!/bin/sh\necho original\n", encoding="utf-8")
    target.chmod(0o755)

    harness._atomic_write(target, b"#!/bin/sh\necho replaced\n")

    assert target.read_text(encoding="utf-8") == "#!/bin/sh\necho replaced\n"
    assert target.stat().st_mode & 0o777 == 0o755
    assert [entry.name for entry in tree.iterdir() if ".mh-tmp" in entry.name] == []


def test_atomic_write_does_not_follow_a_planted_symlink(tree: Path) -> None:
    """A predictable temp name is an arbitrary-overwrite primitive.

    Pre-place a symlink where the temporary would go and a naive implementation
    follows it, truncating an unrelated file, then renames the symlink over the
    source. O_EXCL plus a random suffix removes both halves.
    """

    import scripts.mutation_harness as harness

    outsider = tree / "outsider.txt"
    outsider.write_text("must survive\n", encoding="utf-8")
    target = tree / "victim.txt"
    target.write_text("original\n", encoding="utf-8")
    # The old predictable name, and a plausible guess at the new scheme.
    for name in (
        f".{target.name}.mutation-harness.tmp",
        f".{target.name}.mh-tmp",
    ):
        (tree / name).symlink_to(outsider)

    harness._atomic_write(target, b"replaced\n")

    assert outsider.read_text(encoding="utf-8") == "must survive\n"
    assert target.read_text(encoding="utf-8") == "replaced\n"
    assert not target.is_symlink()


def test_a_declared_survivor_that_gets_killed_is_an_anomaly(tree: Path) -> None:
    """A stale declaration is a standing instruction to ignore a real signal.

    `expected_survivor_reason` records a judgement about code as it was. When the
    code changes and the mutation becomes observable, silently accepting the kill
    keeps the obsolete reasoning in the plan, where the next reader will trust it.
    """

    from scripts.mutation_harness import VERDICT_STALE_DECLARATION

    plan = _plan(
        tree,
        [
            _mutation(
                expected_survivor_reason="no reachable state can observe this",
            )
        ],
    )
    results, exit_code = run_plan(tree, plan, None, assert_all_killed=False)
    assert results[0].verdict == VERDICT_STALE_DECLARATION
    assert "declaration is stale" in results[0].detail
    assert exit_code == 1


def test_a_mutation_that_does_not_build_is_invalid_not_killed(tree: Path) -> None:
    """A build break and a failing assertion are the same exit code.

    Real instance: `if evaluated {` mutated to `if true {` orphaned the variable,
    Go refused to compile, `go test` exited non-zero, and the run recorded KILLED.
    Nothing ran, so it proved nothing about any assertion -- worse than a
    panic-kill, which at least executes the code.
    """

    # A "build" that rejects the mutated text, standing in for a compiler.
    build = [
        sys.executable,
        "-c",
        (
            "import pathlib,sys;"
            f"t=pathlib.Path({SOURCE_NAME!r}).read_text();"
            "sys.exit(1 if 'false &&' in t else 0)"
        ),
    ]
    plan = _plan(tree, [_mutation(build=build, proof=[GUARD_PRESENT_PROOF])])
    results, exit_code = run_plan(tree, plan, None, assert_all_killed=False)

    assert results[0].verdict == VERDICT_INVALID
    assert "does not build" in results[0].detail
    assert exit_code == 1
    # And the source must be back, because the early return still restores.
    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == _source(GUARD)
    assert verify(tree) == []


def test_a_build_that_fails_on_the_clean_tree_is_a_baseline_failure(
    tree: Path,
) -> None:
    """A wrong build command must not read as a plan full of bad mutations.

    Without this the build runs only after mutating, so a build command that
    cannot pass on the unmutated tree reports every mutation INVALID -- "the
    mutated source does not build" -- blaming the mutations for a defect in the
    plan's own build command. Measured while authoring this repository's
    self-check plan: the first build command failed on the clean file.
    """

    before = (tree / SOURCE_NAME).read_bytes()
    always_fails = [sys.executable, "-c", "raise SystemExit(3)"]
    plan = _plan(tree, [_mutation(build=always_fails)])
    results, exit_code = run_plan(tree, plan, None, assert_all_killed=False)

    assert results[0].verdict == VERDICT_BASELINE_FAILED
    assert "fails on the CLEAN tree" in results[0].detail
    assert exit_code == 1
    # Nothing was mutated, and no record was left behind.
    assert (tree / SOURCE_NAME).read_bytes() == before
    assert verify(tree) == []


def test_a_plan_without_a_build_command_warns(tree: Path) -> None:
    """Absence of a build check weakens every kill in the plan, so say so."""

    plan = _plan(tree, [_mutation()])
    results, _ = run_plan(tree, plan, None, assert_all_killed=False)
    assert any("no 'build' command" in warning for warning in results[0].warnings)


def test_a_building_mutation_is_still_killed_normally(tree: Path) -> None:
    """The build gate must not turn real kills into INVALID."""

    build = [sys.executable, "-c", "pass"]
    plan = _plan(tree, [_mutation(build=build)])
    results, exit_code = run_plan(tree, plan, None, assert_all_killed=True)
    assert results[0].verdict == VERDICT_KILLED
    assert exit_code == 0


def test_a_kill_reports_where_it_died(tree: Path) -> None:
    """A verdict alone cannot distinguish a real kill from an accidental one.

    A mutation killed by a setup precondition, a panic, or a build failure is
    materially weaker evidence than one killed by the assertion written for it,
    and that difference is invisible in a boolean.
    """

    # Passes clean, and on failure prints a file:line the way a test runner does.
    proof_with_site = [
        sys.executable,
        "-c",
        (
            "import pathlib,sys;"
            f"text=pathlib.Path({SOURCE_NAME!r}).read_text();"
            f"ok={GUARD!r} in text;"
            "print('' if ok else 'widget_test.go:42: guard assertion failed');"
            "sys.exit(0 if ok else 1)"
        ),
    ]
    plan = _plan(tree, [_mutation(proof=[proof_with_site])])
    results, _ = run_plan(tree, plan, None, assert_all_killed=True)

    assert results[0].verdict == VERDICT_KILLED
    assert "widget_test.go:42" in results[0].detail
    assert "Confirm it is the assertion this mutation targets" in results[0].detail


def test_no_write_follows_a_planted_symlink_anywhere(tree: Path) -> None:
    """The CLASS fix, not the instance.

    Round 2 hardened only the mutation write. The state record, the snapshot and
    the report all had predictable names and wrote directly, so planting any of
    them as a symlink truncated its target. Every write now goes through the
    audited primitive, which refuses a symlinked destination.
    """

    outsider = tree / "outsider.txt"
    outsider.write_text("must survive\n", encoding="utf-8")
    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    # Every predictable auxiliary path, planted.
    for planted in (
        state / "state.json",
        state / "state.json.tmp",
        state / "report.json",
    ):
        planted.symlink_to(outsider)

    plan = _plan(tree, [_mutation()])
    # The run may refuse or succeed, but it must not write through any link.
    try:
        run_plan(tree, plan, None, assert_all_killed=False)
    except HarnessError:
        pass
    assert outsider.read_text(encoding="utf-8") == "must survive\n"


def test_a_symlinked_state_directory_is_refused(tree: Path) -> None:
    """The state directory is part of the trust boundary, not just its contents."""

    elsewhere = tree / "elsewhere"
    elsewhere.mkdir()
    (tree / ".mutation-harness").symlink_to(elsewhere)

    with pytest.raises(HarnessError, match="is a symlink"):
        verify(tree)


def test_a_symlinked_snapshot_directory_is_refused_before_run_io(tree: Path) -> None:
    """A trusted state directory does not make its child path trusted.

    Following a pre-created ``snapshots`` symlink writes the source snapshot
    outside the state directory before the mutation run takes its lock.
    """

    state = tree / ".mutation-harness"
    state.mkdir()
    elsewhere = tree / "elsewhere"
    elsewhere.mkdir()
    (state / "snapshots").symlink_to(elsewhere)

    plan = _plan(tree, [_mutation()])
    with pytest.raises(HarnessError, match="snapshot directory.*symlink"):
        run_plan(tree, plan, None, assert_all_killed=False)

    assert list(elsewhere.iterdir()) == []
    assert (tree / SOURCE_NAME).read_text(encoding="utf-8") == _source(GUARD)


def test_restore_refuses_a_symlinked_snapshot_directory_before_io(
    tree: Path,
) -> None:
    """Recovery must not trust snapshot bytes reached through a child symlink."""

    state = _stranded_record(tree, snapshot=True)
    snapshot = state / "snapshots" / "M1-x.snapshot"
    elsewhere = tree / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / snapshot.name).write_bytes(snapshot.read_bytes())
    snapshot.unlink()
    snapshot.parent.rmdir()
    snapshot.parent.symlink_to(elsewhere)

    with pytest.raises(HarnessError, match="snapshot directory.*symlink"):
        restore(tree)

    assert DISABLED_GUARD in (tree / SOURCE_NAME).read_text(encoding="utf-8")
    assert (state / "state.json").is_file()


def test_restore_refuses_when_the_path_resolves_to_a_different_file(
    tree: Path,
) -> None:
    """Recovery must bind to the FILE, not to the path.

    Re-resolving the recorded path is not identity: an in-repo symlink retargeted
    after a crash resolves to a different file, and restore would repair that one
    while the originally-mutated file stayed broken -- then clear the record and
    hide it.
    """

    real = tree / "real.txt"
    real.write_text(_source(GUARD), encoding="utf-8")
    decoy = tree / "decoy.txt"
    decoy.write_text(_source(DISABLED_GUARD), encoding="utf-8")
    link = tree / "link.txt"
    link.symlink_to(real)

    state = tree / ".mutation-harness"
    (state / "snapshots").mkdir(parents=True)
    (state / "snapshots" / "M1-x.snapshot").write_bytes(real.read_bytes())
    import os as _os

    real_stat = real.stat()
    (state / "state.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "applied": {
                    "mutation_id": "M1",
                    "file": "link.txt",
                    "snapshot": "M1-x.snapshot",
                    "mutated_sha256": hashlib.sha256(
                        _source(DISABLED_GUARD).encode()
                    ).hexdigest(),
                    "identity": f"{real_stat.st_dev}:{real_stat.st_ino}",
                },
            }
        ),
        encoding="utf-8",
    )
    # Retarget the symlink: the path still resolves, to the wrong file.
    link.unlink()
    link.symlink_to(decoy)
    assert _os.path.samefile(link, decoy)

    with pytest.raises(HarnessError, match="no longer resolves to the file"):
        restore(tree)
    # The decoy is untouched and the record is kept.
    assert decoy.read_text(encoding="utf-8") == _source(DISABLED_GUARD)
    assert verify(tree) != []
