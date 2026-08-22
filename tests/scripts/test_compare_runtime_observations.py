"""Runtime-observation mode: the operational-health claim (CHAOS-3092 P0).

Kept strictly apart from product-row parity. Rows are compared in Go by
internal/testsupport/computeparity; nothing here looks at a product row, and
the two verdict vocabularies are disjoint so a reader cannot mistake one claim
for the other.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


comparator = _load(
    "compare_runtime_observations", "scripts/worker/compare_runtime_observations.py"
)


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
    # Row parity is not merely a different value of the same field -- it is a
    # different tool now (internal/testsupport/computeparity). This module has
    # no row-parity claim to be confused with.
    assert not hasattr(comparator, "CLAIM_ROWS")
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


# --------------------------------------------------------------------------
# The manifest is a contract, not a suggestion (fifth adversarial-review round)
# --------------------------------------------------------------------------
