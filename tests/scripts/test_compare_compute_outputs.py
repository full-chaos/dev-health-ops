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


def snap(data: list[dict[str, Any]]) -> Any:
    return comparator.Snapshot(table="t", rows=tuple(data))


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
