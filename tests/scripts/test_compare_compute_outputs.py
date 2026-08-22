"""Unit coverage for the shared compute-port comparator (CHAOS-3092 P0).

These tests exercise the comparison core over in-memory snapshots, so they run
in the standard unit tier with no database. The live end-to-end proof against
two scratch ClickHouse databases is
``tests/scripts/test_compare_compute_outputs_live.py`` (``-m clickhouse``).

The three negative controls the slice is accepted on -- a mutated row, a
dropped row, and a float nudged past its declared policy -- are asserted here
at snapshot level and again live.
"""

from __future__ import annotations

import copy
import datetime as dt
import importlib.util
import json
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
MANIFEST_DIR = ROOT / "contracts/compute-parity/v1"


def _load(name: str, relative: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # Registered before execution: the module defines dataclasses, and
    # dataclasses resolves field types through sys.modules.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


comparator = _load(
    "compare_compute_outputs", "scripts/worker/compare_compute_outputs.py"
)


# --------------------------------------------------------------------------
# Manifest fixtures
# --------------------------------------------------------------------------


def base_manifest() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "metrics.example",
        "description": "test manifest",
        "migrations": {"clickhouse": "src/dev_health_ops/migrations/clickhouse"},
        "fixture": {
            "source": "x.py:seed",
            "command": ["x.py", "seed"],
            "seed": 1,
            "digest_algorithm": "sha256",
        },
        "determinism": {
            "clock": {"policy": "pinned_run_day", "notes": "n"},
            "seed": {"policy": "fixed", "value": 1, "notes": "n"},
            "id_source": {"policy": "derived_from_fixture", "notes": "n"},
        },
        "producers": {"python": {"status": "reference"}},
        "inputs": [
            {"table": "deployments", "store": "clickhouse", "select": "SELECT 1"}
        ],
        "outputs": [
            {
                "table": "t",
                "store": "clickhouse",
                "select": "SELECT org_id, day, metric_name, value, computed_at FROM t",
                "semantic_key": ["org_id", "day", "metric_name"],
                "repeat_policy": "idempotent",
                "fields": {
                    "org_id": {"type": "string"},
                    "day": {"type": "date"},
                    "metric_name": {"type": "string"},
                    "value": {"type": "float", "numeric": {"policy": "exact"}},
                    "computed_at": {"type": "datetime", "volatile": "drop"},
                },
            }
        ],
    }


def write_manifest(tmp_path: Path, document: dict[str, Any]) -> Any:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return comparator.load_manifest(path)


def spec_of(tmp_path: Path, document: dict[str, Any] | None = None) -> Any:
    return write_manifest(tmp_path, document or base_manifest()).outputs[0]


def rows() -> list[dict[str, Any]]:
    stamp = dt.datetime(2026, 8, 22, 1, 2, 3, tzinfo=dt.timezone.utc)
    return [
        {
            "org_id": "org",
            "day": dt.date(2026, 8, 20),
            "metric_name": "deployment_frequency",
            "value": 3.0,
            "computed_at": stamp,
        },
        {
            "org_id": "org",
            "day": dt.date(2026, 8, 20),
            "metric_name": "lead_time_for_changes",
            "value": 42.5,
            "computed_at": stamp,
        },
        {
            "org_id": "org",
            "day": dt.date(2026, 8, 21),
            "metric_name": "change_failure_rate",
            "value": 0.25,
            "computed_at": stamp,
        },
    ]


DECLARED_COLUMNS = ("org_id", "day", "metric_name", "value", "computed_at")


def snap(data: list[dict[str, Any]], columns: tuple[str, ...] | None = None) -> Any:
    if columns is None:
        columns = tuple(data[0].keys()) if data else DECLARED_COLUMNS
    return comparator.Snapshot(table="t", columns=columns, rows=tuple(data))


# --------------------------------------------------------------------------
# Exact numeric handling (publisher.go UseNumber/big.Rat precedent)
# --------------------------------------------------------------------------


def test_json_numbers_beyond_float64_precision_stay_distinct():
    """The exact case publisher.go's UseNumber comment names.

    Both integers collapse to the same float64. A comparator that decoded JSON
    the default way would call two genuinely different rows equal.
    """
    low = comparator.canonical_json(
        comparator.decode_json_preserving_numbers('{"scope": 9007199254740992}')
    )
    high = comparator.canonical_json(
        comparator.decode_json_preserving_numbers('{"scope": 9007199254740993}')
    )
    assert float(9007199254740992) == float(9007199254740993)
    assert low != high


def test_json_number_spelling_does_not_create_a_difference():
    """1, 1.0 and 1e0 denote one value; storage does not preserve spelling."""
    forms = ["1", "1.0", "1e0", "0.1e1", "10e-1"]
    canonical = {
        comparator.canonical_json(comparator.decode_json_preserving_numbers(form))
        for form in forms
    }
    assert len(canonical) == 1


def test_json_key_order_does_not_create_a_difference():
    left = comparator.canonical_json(
        comparator.decode_json_preserving_numbers('{"a": 1, "b": 2}')
    )
    right = comparator.canonical_json(
        comparator.decode_json_preserving_numbers('{"b": 2, "a": 1}')
    )
    assert left == right


def test_naive_timestamps_are_read_as_utc():
    naive = dt.datetime(2026, 8, 22, 12, 0, 0)
    aware = dt.datetime(2026, 8, 22, 12, 0, 0, tzinfo=dt.timezone.utc)
    assert comparator.canonical_datetime(naive) == comparator.canonical_datetime(aware)


def test_timestamps_in_other_zones_normalize_to_utc():
    eastern = dt.datetime(
        2026, 8, 22, 8, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=-4))
    )
    utc = dt.datetime(2026, 8, 22, 12, 0, 0, tzinfo=dt.timezone.utc)
    assert comparator.canonical_datetime(eastern) == comparator.canonical_datetime(utc)


# --------------------------------------------------------------------------
# Manifest guards
# --------------------------------------------------------------------------


def test_identical_snapshots_report_equal(tmp_path):
    spec = spec_of(tmp_path)
    result = comparator.compare_snapshots(spec, snap(rows()), snap(rows()))
    assert result.equal
    assert result.left_row_digest == result.right_row_digest
    assert result.left_key_digest == result.right_key_digest


def test_volatile_drop_column_does_not_create_a_difference(tmp_path):
    spec = spec_of(tmp_path)
    right = copy.deepcopy(rows())
    for row in right:
        row["computed_at"] = dt.datetime(2027, 1, 1, tzinfo=dt.timezone.utc)
    result = comparator.compare_snapshots(spec, snap(rows()), snap(right))
    assert result.equal
    assert "computed_at" in result.digest_excluded_fields


def test_global_tolerance_key_is_refused(tmp_path):
    document = base_manifest()
    document["tolerance"] = 0.01
    with pytest.raises(comparator.ComparisonError, match="global_tolerance_forbidden"):
        write_manifest(tmp_path, document)


def test_tolerance_without_a_reason_is_refused(tmp_path):
    document = base_manifest()
    document["outputs"][0]["fields"]["value"]["numeric"] = {
        "policy": "absolute_tolerance",
        "tolerance": 0.5,
    }
    with pytest.raises(comparator.ComparisonError, match="tolerance_without_reason"):
        write_manifest(tmp_path, document)


def test_exact_policy_carrying_a_tolerance_is_refused(tmp_path):
    document = base_manifest()
    document["outputs"][0]["fields"]["value"]["numeric"] = {
        "policy": "exact",
        "tolerance": 0.5,
    }
    with pytest.raises(
        comparator.ComparisonError, match="exact_policy_carries_tolerance"
    ):
        write_manifest(tmp_path, document)


def test_canonicalized_away_key_column_is_refused(tmp_path):
    """A dropped key column would collapse distinct rows into one key."""
    document = base_manifest()
    document["outputs"][0]["fields"]["metric_name"]["volatile"] = "drop"
    with pytest.raises(comparator.ComparisonError, match="volatile_semantic_key"):
        write_manifest(tmp_path, document)


def test_undeclared_key_column_is_refused(tmp_path):
    document = base_manifest()
    document["outputs"][0]["semantic_key"] = ["org_id", "nope"]
    with pytest.raises(comparator.ComparisonError, match="key_column_undeclared"):
        write_manifest(tmp_path, document)


def test_column_drift_between_select_and_manifest_is_refused(tmp_path):
    spec = spec_of(tmp_path)
    drifted = copy.deepcopy(rows())
    for row in drifted:
        row["surprise"] = 1
    with pytest.raises(comparator.ComparisonError, match="manifest_column_drift"):
        comparator.validate_columns(spec, snap(drifted), "python")


def test_column_drift_is_caught_on_an_EMPTY_result_too(tmp_path):
    """The fail-open Codex found: a wrong projection that returns no rows.

    Columns are validated from the driver's result metadata, so a select that
    reads the wrong columns is refused even when it matched nothing -- rather
    than sailing through as two equal empty tables.
    """
    spec = spec_of(tmp_path)
    empty_wrong_projection = comparator.Snapshot(
        table="t", columns=("org_id", "day"), rows=()
    )
    with pytest.raises(comparator.ComparisonError, match="manifest_column_drift"):
        comparator.validate_columns(spec, empty_wrong_projection, "python")


def test_tombstone_policy_without_a_predicate_is_refused(tmp_path):
    document = base_manifest()
    document["outputs"][0]["repeat_policy"] = "tombstone"
    with pytest.raises(
        comparator.ComparisonError, match="tombstone_predicate_required"
    ):
        write_manifest(tmp_path, document)


def test_tombstone_predicate_without_the_policy_is_refused(tmp_path):
    document = base_manifest()
    document["outputs"][0]["tombstone_predicate"] = "is_deleted = 1"
    with pytest.raises(comparator.ComparisonError, match="tombstone_predicate_unused"):
        write_manifest(tmp_path, document)


# --------------------------------------------------------------------------
# Negative controls
# --------------------------------------------------------------------------


def test_negative_control_mutated_row_is_reported_precisely(tmp_path):
    spec = spec_of(tmp_path)
    right = copy.deepcopy(rows())
    right[1]["value"] = 41.5
    result = comparator.compare_snapshots(spec, snap(rows()), snap(right))

    assert not result.equal
    assert result.left_count == result.right_count
    assert result.left_key_digest == result.right_key_digest
    assert result.left_row_digest != result.right_row_digest
    mutations = [d for d in result.differences if d["shape"] == "row_mutated"]
    assert len(mutations) == 1
    assert mutations[0]["semantic_key"]["metric_name"] == "s:lead_time_for_changes"
    assert mutations[0]["fields"] == [
        {
            "column": "value",
            "shape": "value_mismatch",
            "left": "42.5",
            "right": "41.5",
        }
    ]


def test_negative_control_dropped_row_is_reported_precisely(tmp_path):
    spec = spec_of(tmp_path)
    right = copy.deepcopy(rows())
    dropped = right.pop(2)
    result = comparator.compare_snapshots(spec, snap(rows()), snap(right))

    assert not result.equal
    counts = [d for d in result.differences if d["shape"] == "count_mismatch"]
    assert counts == [{"shape": "count_mismatch", "table": "t", "left": 3, "right": 2}]
    missing = [d for d in result.differences if d["shape"] == "row_missing_on_right"]
    assert len(missing) == 1
    assert missing[0]["semantic_key"]["metric_name"] == "s:" + dropped["metric_name"]
    assert missing[0]["semantic_key"]["day"] == "d:2026-08-21"
    assert result.left_key_digest != result.right_key_digest


def test_negative_control_extra_row_is_reported_as_extra(tmp_path):
    spec = spec_of(tmp_path)
    right = copy.deepcopy(rows())
    right.append({**right[0], "metric_name": "time_to_restore_service"})
    result = comparator.compare_snapshots(spec, snap(rows()), snap(right))
    extra = [d for d in result.differences if d["shape"] == "row_extra_on_right"]
    assert len(extra) == 1
    assert extra[0]["semantic_key"]["metric_name"] == "s:time_to_restore_service"


def test_negative_control_float_nudged_past_exact_policy_is_reported(tmp_path):
    """Under the default exact policy a one-ULP nudge is a difference."""
    spec = spec_of(tmp_path)
    right = copy.deepcopy(rows())
    nudged = 0.25 + 2**-52
    assert nudged != 0.25
    right[2]["value"] = nudged
    result = comparator.compare_snapshots(spec, snap(rows()), snap(right))
    mutations = [d for d in result.differences if d["shape"] == "row_mutated"]
    assert len(mutations) == 1
    assert mutations[0]["fields"][0]["column"] == "value"
    assert mutations[0]["fields"][0]["right"] == repr(nudged)


def test_declared_tolerance_absorbs_a_nudge_inside_it_and_reports_one_past_it(tmp_path):
    document = base_manifest()
    document["outputs"][0]["fields"]["value"]["numeric"] = {
        "policy": "absolute_tolerance",
        "tolerance": 0.01,
        "reason": "persisted as Float32; the last decimal digit is not representable",
    }
    spec = spec_of(tmp_path, document)
    assert "value" in spec.digest_excluded

    inside = copy.deepcopy(rows())
    inside[2]["value"] = 0.255
    absorbed = comparator.compare_snapshots(spec, snap(rows()), snap(inside))
    assert absorbed.equal

    outside = copy.deepcopy(rows())
    outside[2]["value"] = 0.30
    reported = comparator.compare_snapshots(spec, snap(rows()), snap(outside))
    assert not reported.equal
    difference = [d for d in reported.differences if d["shape"] == "row_mutated"][0]
    assert difference["fields"][0]["shape"] == "numeric_out_of_policy"
    assert difference["fields"][0]["tolerance"] == 0.01
    assert "Float32" in difference["fields"][0]["reason"]


def test_duplicate_key_multiplicity_difference_is_reported(tmp_path):
    spec = spec_of(tmp_path)
    right = copy.deepcopy(rows())
    right.append(copy.deepcopy(right[0]))
    result = comparator.compare_snapshots(spec, snap(rows()), snap(right))
    shapes = {d["shape"] for d in result.differences}
    assert "key_multiplicity_mismatch" in shapes
    assert "count_mismatch" in shapes


def test_sampling_truncates_but_records_how_many_were_withheld(tmp_path):
    spec = spec_of(tmp_path)
    left = [
        {
            "org_id": "org",
            "day": dt.date(2026, 8, 1),
            "metric_name": f"m{index}",
            "value": float(index),
            "computed_at": dt.datetime(2026, 8, 1, tzinfo=dt.timezone.utc),
        }
        for index in range(20)
    ]
    result = comparator.compare_snapshots(spec, snap(left), snap([]), sample=3)
    reported = [
        d
        for d in result.differences
        if d["shape"] == "row_missing_on_right" and "semantic_key" in d
    ]
    truncated = [
        d
        for d in result.differences
        if d["shape"] == "row_missing_on_right" and "truncated" in d
    ]
    assert len(reported) == 3
    assert truncated == [
        {"shape": "row_missing_on_right", "table": "t", "truncated": 17}
    ]


# --------------------------------------------------------------------------
# Volatile canonicalization
# --------------------------------------------------------------------------


def test_placeholder_volatile_compares_null_ness_only(tmp_path):
    document = base_manifest()
    document["outputs"][0]["fields"]["run_id"] = {
        "type": "uuid",
        "volatile": "placeholder",
    }
    spec = spec_of(tmp_path, document)
    left = [{**row, "run_id": uuid.uuid4()} for row in rows()]
    right = [{**row, "run_id": uuid.uuid4()} for row in rows()]
    assert comparator.compare_snapshots(spec, snap(left), snap(right)).equal

    right_null = [{**row, "run_id": None} for row in rows()]
    result = comparator.compare_snapshots(spec, snap(left), snap(right_null))
    assert not result.equal


def test_ordinal_volatile_compares_identifier_structure_not_identity(tmp_path):
    document = base_manifest()
    document["outputs"][0]["fields"]["batch_id"] = {
        "type": "string",
        "volatile": "ordinal",
    }
    spec = spec_of(tmp_path, document)
    left = [
        {**row, "batch_id": value} for row, value in zip(rows(), ["a-1", "a-1", "a-2"])
    ]
    same_structure = [
        {**row, "batch_id": value} for row, value in zip(rows(), ["z-9", "z-9", "z-99"])
    ]
    different_structure = [
        {**row, "batch_id": value}
        for row, value in zip(rows(), ["z-9", "z-99", "z-99"])
    ]
    assert comparator.compare_snapshots(spec, snap(left), snap(same_structure)).equal
    assert not comparator.compare_snapshots(
        spec, snap(left), snap(different_structure)
    ).equal


# --------------------------------------------------------------------------
# Repeat policy
# --------------------------------------------------------------------------


def stats(spec: Any, data: list[dict[str, Any]]) -> Any:
    return comparator.snapshot_stats(spec, snap(data))


def test_repeat_detects_idempotent_replay(tmp_path):
    spec = spec_of(tmp_path)
    first = stats(spec, rows())
    second = stats(spec, rows())
    verdict = comparator.evaluate_repeat(spec, "python", first, second)
    assert verdict["observed"] == "idempotent"
    assert verdict["matches_declared_policy"]


def test_repeat_detects_append_duplicates_and_flags_the_declaration(tmp_path):
    """A plain MergeTree producer that never deletes appends on replay."""
    spec = spec_of(tmp_path)
    first = stats(spec, rows())
    second = stats(spec, rows() + rows())
    verdict = comparator.evaluate_repeat(spec, "python", first, second)
    assert verdict["observed"] == "append_duplicates"
    assert verdict["key_set_stable"]
    # The manifest under test declares `idempotent`, so the observed append is
    # a violation rather than a silently accepted behaviour.
    assert not verdict["matches_declared_policy"]


def test_repeat_declared_append_duplicates_accepts_growth(tmp_path):
    document = base_manifest()
    document["outputs"][0]["repeat_policy"] = "append_duplicates"
    spec = spec_of(tmp_path, document)
    verdict = comparator.evaluate_repeat(
        spec, "python", stats(spec, rows()), stats(spec, rows() + rows())
    )
    assert verdict["matches_declared_policy"]


def tombstone_spec(tmp_path: Path) -> Any:
    document = base_manifest()
    document["outputs"][0]["repeat_policy"] = "tombstone"
    document["outputs"][0]["tombstone_predicate"] = "value < 0"
    return spec_of(tmp_path, document)


def test_tombstone_policy_is_violated_when_no_marker_row_appears(tmp_path):
    """The other fail-open Codex found.

    Before the fix, a `tombstone` declaration was satisfied by any replay that
    merely kept the key set stable -- including a producer that deleted nothing
    and marked nothing, which is exactly the behaviour the policy exists to
    catch.
    """
    spec = tombstone_spec(tmp_path)
    verdict = comparator.evaluate_repeat(
        spec,
        "python",
        stats(spec, rows()),
        stats(spec, rows()),
        tombstones_first=0,
        tombstones_second=0,
    )
    assert verdict["tombstone_status"] == "absent"
    assert not verdict["matches_declared_policy"]


def test_tombstone_policy_is_satisfied_when_marker_rows_exist(tmp_path):
    spec = tombstone_spec(tmp_path)
    verdict = comparator.evaluate_repeat(
        spec,
        "python",
        stats(spec, rows()),
        stats(spec, rows()),
        tombstones_first=0,
        tombstones_second=2,
    )
    assert verdict["tombstone_status"] == "present"
    assert verdict["matches_declared_policy"]


def test_tombstone_policy_is_unproven_when_counts_were_not_read(tmp_path):
    spec = tombstone_spec(tmp_path)
    verdict = comparator.evaluate_repeat(
        spec, "python", stats(spec, rows()), stats(spec, rows())
    )
    assert verdict["tombstone_status"] == "not_evaluated"
    assert not verdict["matches_declared_policy"]


def test_repeat_detects_a_changed_key_set(tmp_path):
    spec = spec_of(tmp_path)
    second_rows = copy.deepcopy(rows())
    second_rows[0]["metric_name"] = "something_else"
    verdict = comparator.evaluate_repeat(
        spec, "python", stats(spec, rows()), stats(spec, second_rows)
    )
    assert verdict["observed"] == "changed_key_set"
    assert not verdict["matches_declared_policy"]


# --------------------------------------------------------------------------
# Destination safety
# --------------------------------------------------------------------------


def test_a_delimiter_inside_a_value_cannot_forge_a_field_boundary(tmp_path):
    """Row values are database content; they may contain the join delimiter."""
    spec = spec_of(tmp_path)
    stamp = dt.datetime(2026, 8, 22, tzinfo=dt.timezone.utc)
    left = [
        {
            "org_id": "a",
            "day": dt.date(2026, 8, 20),
            "metric_name": "x\x1fy",
            "value": 1.0,
            "computed_at": stamp,
        }
    ]
    right = [
        {
            "org_id": "a",
            "day": dt.date(2026, 8, 20),
            "metric_name": "x",
            "value": 1.0,
            "computed_at": stamp,
        }
    ]
    result = comparator.compare_snapshots(spec, snap(left), snap(right))
    assert not result.equal
    assert comparator.row_key(spec, left[0]) != comparator.row_key(spec, right[0])


def test_default_database_is_refused_as_a_comparison_side():
    with pytest.raises(comparator.ComparisonError, match="destination_refused"):
        comparator.guard_destination(
            "clickhouse://ch:ch@localhost:8123/default", "left"
        )


def test_scratch_database_is_accepted():
    comparator.guard_destination(
        "clickhouse://ch:ch@localhost:8123/parity_left_abc", "left"
    )


# --------------------------------------------------------------------------
# Checked-in manifests
# --------------------------------------------------------------------------


def manifest_paths() -> list[Path]:
    return sorted(
        path
        for path in MANIFEST_DIR.glob("*.json")
        if not path.name.endswith(".schema.json")
    )


def test_at_least_one_manifest_is_wired():
    assert manifest_paths(), (
        "P0 wires metrics.dora; a kind with no manifest is unproven"
    )


@pytest.mark.parametrize("path", manifest_paths(), ids=lambda p: p.name)
def test_checked_in_manifest_loads_and_declares_its_variation(path: Path):
    manifest = comparator.load_manifest(path)
    assert manifest.kind == path.stem
    assert manifest.inputs, "a kind with no declared inputs cannot verify its fixture"
    for spec in manifest.outputs:
        for column in spec.semantic_key:
            spec.field(column)
    for key in ("clock", "seed", "id_source"):
        assert manifest.determinism[key]["notes"].strip(), (
            f"{path.name} leaves {key} undocumented; an undeclared source of "
            "variation surfaces as a false difference in every future lane"
        )
    reference = [
        label
        for label, entry in manifest.producers.items()
        if entry.get("status") == "reference"
    ]
    assert len(reference) == 1, "exactly one implementation is the oracle"


# --------------------------------------------------------------------------
# runtime mode: the operational-health claim, kept separate from row parity
# --------------------------------------------------------------------------


def go_observation(**overrides: Any) -> dict[str, Any]:
    document = {
        "schema_version": 1,
        "runtime": "go",
        "captured_at": "2026-08-22T18:00:00Z",
        "window": {
            "start": "2026-08-22T17:00:00Z",
            "end": "2026-08-22T18:00:00Z",
            "timezone": "UTC",
        },
        "dataset_scope": "a" * 64,
        "run_scope": "b" * 64,
        "build": {"revision": "f4839e3d1", "image_digest": "sha256:" + "0" * 64},
        "families": {
            "metrics": {
                "counts": {"success": 200, "retry": 0, "failure": 0, "discard": 0}
            }
        },
        "profiles": {
            "general": {
                "cpu_cores": 0.001,
                "memory_bytes": 1_000_000_000,
                "oldest_queue_age_seconds_p95": 1.0,
            }
        },
    }
    document.update(overrides)
    return document


def write_observation(tmp_path: Path, document: dict[str, Any]) -> Path:
    path = tmp_path / "observation.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_runtime_mode_is_a_separate_claim_from_row_parity(tmp_path):
    report = comparator.compare_runtime(write_observation(tmp_path, go_observation()))
    assert report["claim"] == comparator.CLAIM_RUNTIME
    assert report["claim"] != comparator.CLAIM_ROWS
    # An operational report must not be readable as a row-parity report: it
    # carries none of the row-parity structure a reader would look for.
    assert "runs" not in report
    assert "inputs" not in report
    assert "repeat" not in report


def test_runtime_mode_fails_closed_while_the_v3_thresholds_are_unapproved(tmp_path):
    """The checked-in thresholds carry `review.approved: false`.

    Until a human approves them, no operational-health pass can be claimed, and
    the tool must say UNPROVEN rather than WITHIN_ENVELOPE. This mirrors
    canary_release_proof.py's `thresholds_unapproved` rule rather than
    re-deciding it.
    """
    report = comparator.compare_runtime(write_observation(tmp_path, go_observation()))
    assert report["verdict"] == comparator.VERDICT_UNPROVEN
    assert report["reason"] == "thresholds_unapproved"
    assert report["evidence"]["thresholds"]["review_approved"] is False
    assert report["evidence"]["baseline"]["sha256"]


def test_runtime_mode_records_an_absent_baseline_series_as_missing_not_zero(tmp_path):
    """Evidence rule: an empty result is `missing`, never a numeric zero."""
    observation = go_observation()
    observation["families"]["no_such_family"] = {
        "counts": {"success": 1, "retry": 0, "failure": 0, "discard": 0}
    }
    report = comparator.compare_runtime(write_observation(tmp_path, observation))
    entry = [c for c in report["comparisons"] if c.get("name") == "no_such_family"][0]
    assert entry["status"] == "missing_in_baseline"
    assert "go_error_count" not in entry


def test_runtime_mode_measures_error_and_resource_findings(tmp_path):
    observation = go_observation()
    observation["families"]["metrics"]["counts"]["failure"] = 3
    observation["profiles"]["general"]["cpu_cores"] = 10.0
    report = comparator.compare_runtime(write_observation(tmp_path, observation))
    checks = {finding["check"] for finding in report["findings"]}
    assert "error_parity_failed" in checks
    assert "go_error_count_ceiling_failed" in checks
    assert "cpu_cores_budget_failed" in checks


def test_runtime_mode_refuses_an_observation_that_is_not_the_go_runtime(tmp_path):
    observation = go_observation()
    observation["runtime"] = "celery"
    with pytest.raises(comparator.ComparisonError, match="observation_runtime_not_go"):
        comparator.compare_runtime(write_observation(tmp_path, observation))


def test_runtime_mode_refuses_an_observation_carrying_tenant_identifiers(tmp_path):
    """The evidence rules forbid tenant identifiers; reuse the proof's own list."""
    observation = go_observation()
    observation["org_id"] = "acme"
    with pytest.raises(Exception):
        comparator.compare_runtime(write_observation(tmp_path, observation))


def test_runtime_mode_refuses_an_observation_with_no_series(tmp_path):
    """A truncated capture must not satisfy the operational-health gate.

    Before the fix, the evaluator iterated only over what the observation
    contained, so an observation carrying no families and no profiles produced
    no findings at all -- and, once thresholds are approved, would have read as
    WITHIN_ENVELOPE purely because nothing was measured.
    """
    for key in ("families", "profiles"):
        observation = go_observation()
        observation[key] = {}
        with pytest.raises(
            comparator.ComparisonError, match=f"observation_{key}_missing"
        ):
            comparator.compare_runtime(write_observation(tmp_path, observation))


def test_runtime_mode_refuses_invalid_counts(tmp_path):
    observation = go_observation()
    observation["families"]["metrics"]["counts"]["failure"] = -1
    with pytest.raises(comparator.ComparisonError, match="observation_counts_invalid"):
        comparator.compare_runtime(write_observation(tmp_path, observation))

    observation = go_observation()
    del observation["families"]["metrics"]["counts"]["discard"]
    with pytest.raises(comparator.ComparisonError, match="observation_counts_invalid"):
        comparator.compare_runtime(write_observation(tmp_path, observation))


def test_runtime_mode_requires_coverage_of_every_recorded_baseline_series(tmp_path):
    """Coverage is checked against the pinned baseline, not against the input."""
    report = comparator.compare_runtime(write_observation(tmp_path, go_observation()))
    not_observed = {
        finding["check"]
        for finding in report["findings"]
        if finding["check"].startswith("baseline_")
    }
    assert "baseline_family_not_observed" in not_observed
    statuses = {
        entry["name"]: entry["status"]
        for entry in report["comparisons"]
        if entry.get("status") == "not_observed"
    }
    assert statuses, "the single-family observation covers almost none of v0"


def test_runtime_mode_treats_an_unmeasurable_budget_as_a_finding(tmp_path):
    observation = go_observation()
    del observation["profiles"]["general"]["cpu_cores"]
    report = comparator.compare_runtime(write_observation(tmp_path, observation))
    assert {"check": "cpu_cores_not_measurable", "profile": "general"} in report[
        "findings"
    ]


# --------------------------------------------------------------------------
# Fail-closed numerics and identifiers (second adversarial-review round)
# --------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity", "-1"])
def test_runtime_mode_refuses_a_non_finite_or_negative_measurement(tmp_path, bad):
    """`>` is False for NaN, for Infinity on the wrong side, and for negatives.

    A malformed observation would therefore produce no findings at all, which
    is indistinguishable from a clean one. json.loads accepts NaN/Infinity, so
    this is reachable from a file on disk.
    """
    observation = go_observation()
    path = tmp_path / "observation.json"
    path.write_text(
        json.dumps(observation).replace('"cpu_cores": 0.001', f'"cpu_cores": {bad}'),
        encoding="utf-8",
    )
    with pytest.raises(comparator.ComparisonError, match="observation_measure_"):
        comparator.compare_runtime(path)


def test_runtime_mode_refuses_a_non_numeric_measurement(tmp_path):
    observation = go_observation()
    observation["profiles"]["general"]["memory_bytes"] = "lots"
    with pytest.raises(
        comparator.ComparisonError, match="observation_measure_not_a_number"
    ):
        comparator.compare_runtime(write_observation(tmp_path, observation))


def test_a_non_finite_baseline_is_treated_as_no_baseline():
    for value in (float("nan"), float("inf"), -1.0):
        assert comparator._baseline_scalar({"p50": value}) is None
    assert comparator._baseline_scalar({"p50": 2.0}) == 2.0


def test_tolerance_comparison_does_not_lose_exactness_through_float(tmp_path):
    """A zero tolerance must not equate the 2^53 neighbours.

    `float()`-coercing both operands before the comparison made the pair this
    module's JSON handling exists to keep apart compare equal at any tolerance,
    including zero -- so a declared numeric policy could pass on values that
    genuinely differ.
    """
    document = base_manifest()
    document["outputs"][0]["fields"]["value"] = {
        "type": "int",
        "numeric": {
            "policy": "absolute_tolerance",
            "tolerance": 0,
            "reason": "declared to exercise the boundary; no slack is intended",
        },
    }
    spec = spec_of(tmp_path, document)
    policy = spec.field("value").numeric
    assert float(9007199254740992) == float(9007199254740993)
    assert not policy.within(9007199254740992, 9007199254740993)
    assert policy.within(9007199254740993, 9007199254740993)


def test_tolerance_comparison_refuses_a_non_finite_operand(tmp_path):
    document = base_manifest()
    document["outputs"][0]["fields"]["value"]["numeric"] = {
        "policy": "absolute_tolerance",
        "tolerance": 1.0,
        "reason": "Float32 column",
    }
    policy = spec_of(tmp_path, document).field("value").numeric
    assert not policy.within(float("nan"), float("nan"))
    assert not policy.within(float("inf"), float("inf"))
    assert policy.within(1.0, 1.5)


def test_tolerance_honours_the_decimal_spelling_of_the_declared_tolerance(tmp_path):
    document = base_manifest()
    document["outputs"][0]["fields"]["value"]["numeric"] = {
        "policy": "absolute_tolerance",
        "tolerance": 0.1,
        "reason": "Float32 column",
    }
    policy = spec_of(tmp_path, document).field("value").numeric
    assert policy.within("1.0", "1.1")
    assert not policy.within("1.0", "1.100000001")


def test_runtime_coverage_includes_queue_age_only_baseline_profiles(tmp_path):
    """v0 carries profiles that exist in the queue-age series but not in CPU.

    Deriving required coverage from the CPU series alone let those disappear
    from an observation without a finding.
    """
    proof = comparator._load_proof_module()
    measurements = proof.load_pinned_documents()["baseline"].value["measurements"]
    queue_only = set(measurements["oldest_queue_age_seconds_by_profile"]) - set(
        measurements["worker_cpu_cores_by_profile"]
    )
    assert queue_only, "v0 is expected to carry queue-age-only profiles"

    report = comparator.compare_runtime(write_observation(tmp_path, go_observation()))
    checks = [
        f for f in report["findings"] if f["check"] == "lag_seconds_not_measurable"
    ]
    assert checks, "an unmeasured queue age is not a satisfied queue-age budget"


# --------------------------------------------------------------------------
# Execution and attestation must be proven, not assumed
# --------------------------------------------------------------------------


def test_a_side_with_no_producer_command_is_refused(tmp_path):
    """`go` ships as `not_ported`, so it resolves to no command.

    Skipping that side's execution would compare the reference against whatever
    was already sitting in the destination and report EQUAL for an
    implementation that never ran.
    """
    manifest = write_manifest(tmp_path, base_manifest())
    with pytest.raises(
        comparator.ComparisonError, match="producer_command_unresolved:go"
    ):
        comparator.compare_rows(
            manifest,
            left_dsn="clickhouse://ch:ch@localhost:8123/parity_a",
            right_dsn="clickhouse://ch:ch@localhost:8123/parity_b",
            left_label="python",
            right_label="go",
            left_command=["/bin/true"],
            right_command=None,
            no_exec=False,
            repeat=2,
            sample=10,
            as_of="2026-08-22T00:00:00Z",
        )


def test_the_checked_in_manifest_leaves_go_without_a_command():
    """The premise of the test above, asserted against the real manifest."""
    manifest = comparator.load_manifest(MANIFEST_DIR / "metrics.dora.json")
    assert manifest.producers["go"]["status"] == "not_ported"
    assert comparator.resolve_command(manifest, "go", None) is None
    assert comparator.resolve_command(manifest, "python", None) is not None


@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda o: o.pop("dataset_scope"), "observation_missing_fields"),
        (lambda o: o.update(extra=1), "observation_unknown_fields"),
        (
            lambda o: o.update(schema_version=2),
            "observation_schema_version_unsupported",
        ),
        (lambda o: o.update(dataset_scope="nope"), "observation_scope_digest_invalid"),
        (
            lambda o: o.update(
                build={"revision": "zz", "image_digest": "sha256:" + "0" * 64}
            ),
            "observation_build_revision_invalid",
        ),
        (
            lambda o: o.update(build={"revision": "abc1234", "image_digest": "nope"}),
            "observation_build_image_digest_invalid",
        ),
        (
            lambda o: o["window"].update(timezone="America/New_York"),
            "observation_window_not_utc",
        ),
        (
            lambda o: o["window"].update(start="2026-08-22T19:00:00Z"),
            "observation_window_order_invalid",
        ),
    ],
)
def test_runtime_mode_enforces_the_observation_attestation(tmp_path, mutate, expected):
    """A set of in-envelope numbers is not an observation.

    Without this, a forged or truncated file could be handed to the gate with no
    statement of which build, dataset, run, or window produced it.
    """
    observation = go_observation()
    mutate(observation)
    with pytest.raises(comparator.ComparisonError, match=expected):
        comparator.compare_runtime(write_observation(tmp_path, observation))


def test_a_well_formed_observation_still_reaches_the_measurement_stage(tmp_path):
    report = comparator.compare_runtime(write_observation(tmp_path, go_observation()))
    assert report["claim"] == comparator.CLAIM_RUNTIME
    assert report["comparisons"]
