"""Independent contracts for the CHAOS-3807 mutation-shard coordinator."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from importlib.util import cache_from_source
from pathlib import Path

import pytest

# The shared harness restores bytes without restoring mtimes. A same-size
# mutation can otherwise leave a timestamp-valid pyc and make POST-RESTORE run
# mutated bytecode after the production source is clean.
Path(cache_from_source("scripts/mutation_harness_coordinator.py")).unlink(
    missing_ok=True
)

from scripts.mutation_harness import HarnessError, Result, _read_state  # noqa: E402
from scripts.mutation_harness_coordinator import (  # noqa: E402
    AGGREGATE_CHILD_FAILED,
    AGGREGATE_COMPLETE,
    AGGREGATE_SOURCE_DRIFTED,
    AggregateRefusal,
    ChildSpec,
    DetailNormalizationError,
    ShardAssignment,
    aggregate_child_results,
    append_durable_jsonl,
    begin_coordinator_run,
    canonical_result_projection,
    coordinator_run,
    normalize_detail,
    select_and_assign,
    write_aggregate_report,
)

PLAN_DIGEST = "a" * 64
SOURCE_DIGEST = "b" * 64
RUN_ID = "run-lane-c"


def _source_manifest() -> dict[str, object]:
    return {"head": "head", "entries": [], "digest": SOURCE_DIGEST}


def _raw_result(
    mutation_id: str,
    ordinal: int,
    shard_index: int,
    *,
    run_id: str = RUN_ID,
    plan_digest: str = PLAN_DIGEST,
    verdict: str = "KILLED",
) -> dict[str, object]:
    """Build the child wire object without using coordinator serializers."""

    return {
        "schema_version": 1,
        "run_id": run_id,
        "shard_index": shard_index,
        "plan_digest": plan_digest,
        "plan_ordinal": ordinal,
        "mutation_id": mutation_id,
        "result": {
            "id": mutation_id,
            "verdict": verdict,
            "detail": f"observed {mutation_id}",
            "failing_proof": "proof command",
            "warnings": [],
        },
    }


def _assignments(ids: Sequence[str], shards: int = 2) -> tuple[ShardAssignment, ...]:
    return select_and_assign(ids, None, shards)


def test_only_selection_is_assigned_by_selected_ordinal_not_original_ordinal() -> None:
    identifiers = ["M0", "M1", "M2", "M3", "M4", "M5"]

    assignments = select_and_assign(identifiers, {"M1", "M4", "M5"}, 2)

    # Independent oracle: filter in plan order, enumerate that selected list,
    # then apply selected ordinal modulo the effective shard count.
    selected = [(1, "M1"), (4, "M4"), (5, "M5")]
    expected = {
        0: [("M1", 1, 0), ("M5", 5, 2)],
        1: [("M4", 4, 1)],
    }
    assert {
        assignment.shard_index: [
            (item.identifier, item.plan_ordinal, item.selected_ordinal)
            for item in assignment.mutations
        ]
        for assignment in assignments
    } == expected
    assert [item for _ordinal, item in selected] == ["M1", "M4", "M5"]


@pytest.mark.parametrize(
    ("records", "message"),
    [
        ([_raw_result("M1", 0, 0)], "missing mutation results: ['M2']"),
        (
            [
                _raw_result("M1", 0, 0),
                _raw_result("M1", 0, 0),
                _raw_result("M2", 1, 1),
            ],
            "duplicate mutation results: ['M1']",
        ),
        (
            [
                _raw_result("M1", 0, 0),
                _raw_result("M2", 1, 1),
                _raw_result("FOREIGN", 2, 0),
            ],
            "unknown mutation results: ['FOREIGN']",
        ),
        (
            [
                _raw_result("M1", 0, 0, plan_digest="c" * 64),
                _raw_result("M2", 1, 1),
            ],
            "plan digest mismatch",
        ),
    ],
)
def test_malformed_child_results_are_refused(
    records: Sequence[Mapping[str, object]], message: str
) -> None:
    with pytest.raises(
        AggregateRefusal, match=message.replace("[", r"\[").replace("]", r"\]")
    ):
        aggregate_child_results(
            records,
            _assignments(["M1", "M2"]),
            run_id=RUN_ID,
            plan_digest=PLAN_DIGEST,
        )


def test_aggregate_reorders_shard_streams_by_selected_plan_ordinal() -> None:
    assignments = _assignments(["M1", "M2", "M3"], shards=2)
    shard_order = [
        _raw_result("M1", 0, 0),
        _raw_result("M3", 2, 0),
        _raw_result("M2", 1, 1),
    ]

    aggregate = aggregate_child_results(
        shard_order,
        assignments,
        run_id=RUN_ID,
        plan_digest=PLAN_DIGEST,
    )

    assert [result.identifier for result in aggregate.results] == ["M1", "M2", "M3"]


def test_durable_result_append_is_visible_before_a_later_result(tmp_path: Path) -> None:
    stream = tmp_path / "shard" / "results.jsonl"

    append_durable_jsonl(stream, _raw_result("M1", 0, 0))

    assert json.loads(stream.read_text(encoding="utf-8"))["mutation_id"] == "M1"
    append_durable_jsonl(stream, _raw_result("M2", 1, 0))
    assert [
        json.loads(line)["mutation_id"]
        for line in stream.read_text(encoding="utf-8").splitlines()
    ] == ["M1", "M2"]


def test_normalizer_changes_only_named_runtime_bytes_and_preserves_real_difference(
    tmp_path: Path,
) -> None:
    shard = tmp_path / "shard-0"
    temp = tmp_path / "pytest-temp"
    left = Result(
        "M1",
        "KILLED",
        f"{shard}/pkg/check_test.go:8 --- FAIL: TestGuard (0.31s) "
        f"tmp={temp}/x url=https://example.test/a expected=1",
        failing_proof="go test",
    )
    right = Result(
        "M1",
        "KILLED",
        f"{shard}/pkg/check_test.go:8 --- FAIL: TestGuard (1.22s) "
        f"tmp={temp}/y url=https://example.test/a expected=2",
        failing_proof="go test",
    )

    left_projection = canonical_result_projection(
        [left], shard_roots=[shard], temporary_roots=[temp]
    )
    right_projection = canonical_result_projection(
        [right], shard_roots=[shard], temporary_roots=[temp]
    )

    assert "<SHARD_ROOT>" in left_projection[0]["detail"]
    assert "<TMP>" in left_projection[0]["detail"]
    assert "<DURATION>" in left_projection[0]["detail"]
    assert left_projection != right_projection


def test_normalizer_refuses_an_unrecognised_absolute_path(tmp_path: Path) -> None:
    with pytest.raises(DetailNormalizationError, match="unrecognised absolute path"):
        normalize_detail(
            "failure read /opt/foreign/state.txt",
            shard_roots=[tmp_path / "shard"],
            temporary_roots=[tmp_path / "temp"],
        )


def test_root_lock_is_owned_before_staging_and_is_mutually_exclusive(
    tmp_path: Path,
) -> None:
    plan = tmp_path / "plan.json"
    plan.write_text("{}", encoding="utf-8")
    first = begin_coordinator_run(
        tmp_path,
        run_id="first",
        source_head="head",
        source_manifest=_source_manifest(),
        source_manifest_digest=SOURCE_DIGEST,
        plan_path=plan,
        plan_digest=PLAN_DIGEST,
        requested_shards=2,
        effective_shards=2,
    )
    try:
        state = _read_state(tmp_path)
        assert state is not None
        assert state["coordinator_run"]["lifecycle"] == "staging"
        assert (tmp_path / ".mutation-harness" / "lock").is_dir()
        with pytest.raises(HarnessError, match="another mutation run holds"):
            begin_coordinator_run(
                tmp_path,
                run_id="second",
                source_head="head",
                source_manifest=_source_manifest(),
                source_manifest_digest=SOURCE_DIGEST,
                plan_path=plan,
                plan_digest=PLAN_DIGEST,
                requested_shards=2,
                effective_shards=2,
            )
    finally:
        first.clear_and_release()


def test_final_report_refuses_incomplete_children(tmp_path: Path) -> None:
    report = tmp_path / "report.json"

    with pytest.raises(HarnessError, match="before all children complete"):
        write_aggregate_report(
            report,
            {"schema_version": 1, "results": []},
            all_children_complete=False,
        )

    assert not report.exists()


def test_child_protocol_refuses_a_result_stream_in_the_source_root(
    tmp_path: Path,
) -> None:
    children = tmp_path / "children"
    children.mkdir()
    foreign_result = tmp_path / "child-result.jsonl"

    def bad_factory(assignment: ShardAssignment, run_id: str) -> ChildSpec:
        spec = _factory(children)(assignment, run_id)
        return replace(spec, result_stream=foreign_result)

    with pytest.raises(HarnessError, match="result_stream escapes shard root"):
        _coordinate(tmp_path, mutation_ids=("M1",), factory=bad_factory)

    assert not foreign_result.exists()


def _child_code(
    assignment: ShardAssignment,
    *,
    write_results: bool = True,
    exit_code: int = 0,
) -> str:
    selected = [
        (item.identifier, item.selected_ordinal) for item in assignment.mutations
    ]
    return "\n".join(
        [
            "import json, os, pathlib, sys",
            f"selected = {selected!r}",
            "result_path = pathlib.Path(os.environ['MUTATION_HARNESS_RESULT_STREAM'])",
            "result_path.parent.mkdir(parents=True, exist_ok=True)",
            "for mutation_id, ordinal in selected:",
            "    common = {'schema_version': 1, 'run_id': os.environ['MUTATION_HARNESS_RUN_ID'], 'shard_index': int(os.environ['MUTATION_HARNESS_SHARD_INDEX']), 'plan_ordinal': ordinal, 'mutation_id': mutation_id}",
            "    print(json.dumps({**common, 'event': 'mutation_started', 'phase': 'baseline'}), flush=True)",
            "    print(json.dumps({**common, 'event': 'mutation_finished', 'verdict': 'KILLED'}), flush=True)",
            *(
                [
                    "    record = {**common, 'plan_digest': os.environ['MUTATION_HARNESS_PLAN_DIGEST'], 'result': {'id': mutation_id, 'verdict': 'KILLED', 'detail': 'observed ' + mutation_id, 'failing_proof': 'proof', 'warnings': []}}",
                    "    with result_path.open('a', encoding='utf-8') as handle:",
                    "        handle.write(json.dumps(record) + '\\n')",
                    "        handle.flush()",
                    "        os.fsync(handle.fileno())",
                ]
                if write_results
                else []
            ),
            f"sys.exit({exit_code})",
        ]
    )


def _factory(
    tmp_path: Path,
    *,
    write_results: bool = True,
    exit_code: int = 0,
    assert_root_locked: Path | None = None,
) -> Callable[[ShardAssignment, str], ChildSpec]:
    def build(assignment: ShardAssignment, run_id: str) -> ChildSpec:
        if assert_root_locked is not None:
            state = _read_state(assert_root_locked)
            assert state is not None
            assert state["coordinator_run"]["run_id"] == run_id
            assert state["coordinator_run"]["lifecycle"] == "staging"
            assert (assert_root_locked / ".mutation-harness" / "lock").is_dir()
        shard = tmp_path / f"shard-{assignment.shard_index}"
        shard.mkdir()
        marker = shard / ".mutation-owner.json"
        marker.write_text(run_id, encoding="utf-8")
        return ChildSpec(
            assignment=assignment,
            root=shard,
            source_root=assert_root_locked or tmp_path.parent,
            temporary_root=tmp_path,
            argv=(
                sys.executable,
                "-c",
                _child_code(
                    assignment,
                    write_results=write_results,
                    exit_code=exit_code,
                ),
            ),
            result_stream=shard / ".mutation-harness" / "results.jsonl",
            ownership_marker=marker,
            liveness_lock=shard / ".mutation-harness" / "child.liveness",
        )

    return build


def _coordinate(
    tmp_path: Path,
    *,
    mutation_ids: Sequence[str] = ("M1", "M2", "M3"),
    source_reader: Callable[[], str] | None = None,
    factory: Callable[[ShardAssignment, str], ChildSpec] | None = None,
    before_report: Callable[[], None] | None = None,
):
    plan = tmp_path / "plan.json"
    plan.write_text("{}", encoding="utf-8")
    shards = tmp_path / "children"
    shards.mkdir(exist_ok=True)
    return coordinator_run(
        tmp_path,
        plan,
        "lane-c-plan",
        mutation_ids,
        None,
        True,
        requested_shards=2,
        progress="none",
        source_head="head",
        source_manifest=_source_manifest(),
        source_manifest_digest=SOURCE_DIGEST,
        plan_digest=PLAN_DIGEST,
        source_manifest_reader=source_reader or (lambda: SOURCE_DIGEST),
        child_factory=factory or _factory(shards, assert_root_locked=tmp_path),
        run_id=RUN_ID,
        before_report=before_report,
    )


def test_coordinator_resequences_events_orders_results_and_reports_after_children(
    tmp_path: Path,
) -> None:
    completed_marker = tmp_path / "checked-before-report"

    def before_report() -> None:
        # Every child wrote its durable terminal result before this hook.
        streams = list(
            (tmp_path / "children").glob("*/.mutation-harness/results.jsonl")
        )
        assert len(streams) == 2
        assert all(
            stream.read_text(encoding="utf-8").endswith("\n") for stream in streams
        )
        completed_marker.write_text("complete", encoding="utf-8")

    outcome = _coordinate(tmp_path, before_report=before_report)

    assert outcome.aggregate_status == AGGREGATE_COMPLETE
    assert outcome.exit_code == 0
    assert [result.identifier for result in outcome.results] == ["M1", "M2", "M3"]
    events = [
        json.loads(line)
        for line in outcome.event_log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))
    assert events[-1]["event"] == "run_finished"
    assert all("proof" not in event for event in events)
    report = json.loads(outcome.report_path.read_text(encoding="utf-8"))
    manifest = json.loads(
        (outcome.event_log_path.parent / "manifest.json").read_text(encoding="utf-8")
    )
    assert completed_marker.read_text(encoding="utf-8") == "complete"
    assert report["aggregate_status"] == AGGREGATE_COMPLETE
    assert [result["id"] for result in report["results"]] == ["M1", "M2", "M3"]
    assert report["canonical_projection"]["normalized"] == ["detail"]
    assert manifest["source_manifest"] == _source_manifest()
    assert manifest["source_root"] == str(tmp_path)
    assert all(shard["temporary_root"] for shard in manifest["shards"])
    assert all(shard["liveness_lock"] for shard in manifest["shards"])
    assert _read_state(tmp_path) is None


def test_result_drop_after_finished_event_is_measured_lost_not_survivor(
    tmp_path: Path,
) -> None:
    children = tmp_path / "children"
    children.mkdir()
    outcome = _coordinate(
        tmp_path,
        mutation_ids=("M1",),
        factory=_factory(children, write_results=False, exit_code=7),
    )

    assert outcome.aggregate_status == AGGREGATE_CHILD_FAILED
    assert outcome.exit_code == 1
    assert outcome.results == ()
    assert outcome.measured_lost_ids == ("M1",)
    assert outcome.unmeasured_ids == ()
    report = json.loads(outcome.report_path.read_text(encoding="utf-8"))
    assert report["measured_lost_mutation_ids"] == ["M1"]
    assert "SURVIVED" not in outcome.report_path.read_text(encoding="utf-8")


def test_child_crash_before_measurement_is_unmeasured_not_survivor(
    tmp_path: Path,
) -> None:
    children = tmp_path / "children"
    children.mkdir()

    def crash_factory(assignment: ShardAssignment, run_id: str) -> ChildSpec:
        spec = _factory(children)(assignment, run_id)
        return ChildSpec(
            assignment=spec.assignment,
            root=spec.root,
            source_root=spec.source_root,
            temporary_root=spec.temporary_root,
            argv=(sys.executable, "-c", "raise SystemExit(9)"),
            result_stream=spec.result_stream,
            ownership_marker=spec.ownership_marker,
            liveness_lock=spec.liveness_lock,
        )

    outcome = _coordinate(
        tmp_path,
        mutation_ids=("M1", "M2"),
        factory=crash_factory,
    )

    assert outcome.aggregate_status == AGGREGATE_CHILD_FAILED
    assert outcome.exit_code == 1
    assert outcome.results == ()
    assert outcome.unmeasured_ids == ("M1", "M2")
    assert outcome.measured_lost_ids == ()


def test_source_manifest_drift_before_aggregate_is_non_authoritative(
    tmp_path: Path,
) -> None:
    readings = iter((SOURCE_DIGEST, SOURCE_DIGEST, "drifted"))
    outcome = _coordinate(tmp_path, source_reader=lambda: next(readings))

    assert outcome.aggregate_status == AGGREGATE_SOURCE_DRIFTED
    assert outcome.exit_code == 1
    report = json.loads(outcome.report_path.read_text(encoding="utf-8"))
    assert "source manifest changed before aggregation" in report["aggregate_errors"]
