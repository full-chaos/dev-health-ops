#!/usr/bin/env python3
"""Compare a normalized Go runtime observation against the pinned v0/v3 evidence.

This is the OPERATIONAL-HEALTH claim, and it is deliberately a separate tool
from product-row parity (CHAOS-3092 P0). The two are not the same question and
neither implies the other: a port can write byte-identical rows while burning
four times the memory, and it can behave impeccably at runtime while computing
the wrong numbers. Product rows are compared in Go by
``internal/testsupport/computeparity`` on top of the one row-comparison
vocabulary this repo has; nothing here ever looks at a product row.

It stays in Python because it reuses ``canary_release_proof.py`` -- that module
already owns how the v0 baseline and the v3 thresholds are pinned (path AND
sha256), how a threshold set is validated, and the fail-closed
``thresholds_unapproved`` rule. Re-deriving any of that in another language
would create a second, divergent definition of the same claim, which is the
exact failure this program is trying to stop.

``canary_release_proof.validate_observation`` is NOT reused wholesale: it
validates the v3 *canary* artifact, which pairs a Celery observation with a Go
one and requires a route transport, a route-generation step, and rollback
evidence. Post-cutover there is no Celery side and no route flip, so that
schema does not describe this input. What is reused is its attestation rigour
and its primitives, against the schema declared in
``contracts/compute-parity/v1/runtime-observation.schema.json``.

Verdicts are disjoint from the row-parity vocabulary on purpose, so a reader
cannot mistake one claim for the other: WITHIN_ENVELOPE / OUTSIDE_ENVELOPE /
UNPROVEN here, never EQUAL / DIFFERENT.

STATUS: this comparator is UNUSED BY RULING, not awaiting approval (chris,
2026-08-23, recorded on CHAOS-3090). Go's runtime performance is known-better,
so no threshold set will be approved and UNPROVEN is the permanent answer. It is
kept because the fail-closed ``thresholds_unapproved`` path is the correct
behaviour for an unapproved threshold set, and because deleting a comparator is
harder to reverse than leaving one that refuses to claim anything. Do not build
a readiness signal on it. Row parity -- the half that catches real port defects
-- is unaffected and remains mandatory for every compute family.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

REPORT_SCHEMA_VERSION = 1
CLAIM_RUNTIME = "operational_health"

VERDICT_WITHIN_ENVELOPE = "WITHIN_ENVELOPE"
VERDICT_OUTSIDE_ENVELOPE = "OUTSIDE_ENVELOPE"
VERDICT_UNPROVEN = "UNPROVEN"


class ComparisonError(RuntimeError):
    """A safe, non-sensitive reason the claim cannot be evaluated."""


def _load_proof_module() -> Any:
    """Load ``canary_release_proof`` for its pinned-document and threshold logic.

    Reused rather than reimplemented: that module already owns how the v0
    baseline and the v3 thresholds are pinned (path *and* sha256), how a
    threshold set is validated, and the fail-closed ``thresholds_unapproved``
    rule. Re-deriving any of that here would create a second, divergent
    definition of the same operational claim.
    """
    import importlib.util

    path = Path(__file__).resolve().parent / "canary_release_proof.py"
    spec = importlib.util.spec_from_file_location("canary_release_proof", path)
    if spec is None or spec.loader is None:
        raise ComparisonError("canary_release_proof_unloadable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def compare_runtime(observation_path: Path) -> dict[str, Any]:
    proof = _load_proof_module()
    documents = proof.load_pinned_documents()
    proof.validate_baseline(documents["baseline"].value)
    proof.validate_thresholds(documents["thresholds"].value, documents["baseline"])

    try:
        observation = json.loads(observation_path.read_bytes())
    except (OSError, ValueError) as error:
        raise ComparisonError("observation_unreadable") from error
    if not isinstance(observation, dict):
        raise ComparisonError("observation_shape_invalid")
    if observation.get("runtime") != "go":
        raise ComparisonError("observation_runtime_not_go")
    proof.reject_sensitive_keys(observation)
    _validate_runtime_observation(observation, proof)

    baseline = documents["baseline"].value
    thresholds = documents["thresholds"].value["thresholds"]
    measurements = baseline["measurements"]

    findings: list[dict[str, Any]] = []
    comparisons: list[dict[str, Any]] = []

    observed_families = observation.get("families") or {}
    observed_profiles = observation.get("profiles") or {}
    if not isinstance(observed_families, Mapping) or not observed_families:
        raise ComparisonError("observation_families_missing")
    if not isinstance(observed_profiles, Mapping) or not observed_profiles:
        raise ComparisonError("observation_profiles_missing")

    # Coverage, checked against the pinned baseline rather than against
    # whatever the observation happened to include. An operational-health claim
    # over an arbitrary subset is not the claim: a truncated capture would
    # otherwise produce no findings and read as compliant.
    baseline_families = {
        name
        for name, recorded in measurements["task_outcome_rates_by_family"].items()
        if isinstance(recorded, Mapping) and "counts" in recorded
    }
    # The union of every profile series the baseline actually recorded, not
    # just the CPU one: v0 carries queue-age profiles (external_ingest,
    # monitoring) that are not CPU profiles, and deriving coverage from CPU
    # alone let those disappear from an observation without a finding.
    baseline_profiles: set[str] = set()
    for series, scalar_key in (
        ("worker_cpu_cores_by_profile", "p50"),
        ("worker_memory_bytes_by_profile", "p50"),
        ("oldest_queue_age_seconds_by_profile", "p95"),
    ):
        baseline_profiles |= {
            name
            for name, recorded in measurements[series].items()
            if _baseline_scalar(recorded, scalar_key) is not None
        }
    for name in sorted(baseline_families - set(observed_families)):
        findings.append({"check": "baseline_family_not_observed", "family": name})
        comparisons.append({"scope": "family", "name": name, "status": "not_observed"})
    for name in sorted(baseline_profiles - set(observed_profiles)):
        findings.append({"check": "baseline_profile_not_observed", "profile": name})
        comparisons.append({"scope": "profile", "name": name, "status": "not_observed"})

    for family, observed in observed_families.items():
        recorded = measurements["task_outcome_rates_by_family"].get(family)
        if not isinstance(recorded, Mapping) or "counts" not in recorded:
            # Evidence rule: an absent series is `missing`, never a numeric
            # zero -- and a series with no baseline cannot be inside an
            # envelope the baseline never drew, so it is a finding too.
            comparisons.append(
                {"scope": "family", "name": family, "status": "missing_in_baseline"}
            )
            findings.append({"check": "series_missing_in_baseline", "family": family})
            continue
        _require_counts(observed, family)
        # An observation where nothing ran is not evidence of health. Only
        # failure+discard were compared below, so an all-zero family produced
        # no findings at all and -- once thresholds are approved -- would have
        # read as WITHIN_ENVELOPE for a worker that processed no jobs. The
        # absence of failures is not the presence of work.
        processed = sum(
            int(observed["counts"][key])
            for key in ("success", "retry", "failure", "discard")
        )
        if processed == 0:
            findings.append({"check": "family_processed_no_work", "family": family})
        baseline_errors = int(recorded["counts"]["failure"]) + int(
            recorded["counts"]["discard"]
        )
        observed_errors = int(observed["counts"]["failure"]) + int(
            observed["counts"]["discard"]
        )
        error_delta = observed_errors - baseline_errors
        comparisons.append(
            {
                "scope": "family",
                "name": family,
                "status": "compared",
                "baseline_error_count": baseline_errors,
                "go_error_count": observed_errors,
                "error_count_delta": error_delta,
            }
        )
        if error_delta > thresholds["error_count_delta_max"]:
            findings.append({"check": "error_parity_failed", "family": family})
        if observed_errors > thresholds["go_error_count_max"]:
            findings.append(
                {"check": "go_error_count_ceiling_failed", "family": family}
            )

    for profile, observed in (observation.get("profiles") or {}).items():
        entry: dict[str, Any] = {
            "scope": "profile",
            "name": profile,
            "status": "compared",
        }
        for key, baseline_key, threshold_key in (
            ("cpu_cores", "worker_cpu_cores_by_profile", "cpu_cores_multiplier_max"),
            (
                "memory_bytes",
                "worker_memory_bytes_by_profile",
                "memory_bytes_multiplier_max",
            ),
        ):
            recorded = measurements[baseline_key].get(profile)
            baseline_value = _baseline_scalar(recorded)
            observed_value = observed.get(key)
            if observed_value is not None:
                observed_value = _require_measure(observed_value, f"{profile}.{key}")
            if baseline_value is None or observed_value is None:
                entry[key] = {"status": "missing"}
                # An unmeasurable budget is not a satisfied budget.
                findings.append({"check": f"{key}_not_measurable", "profile": profile})
                continue
            ratio = proof.ratio(float(observed_value), float(baseline_value))
            entry[key] = {
                "baseline": baseline_value,
                "go": observed_value,
                "ratio": ratio,
            }
            if ratio is None or ratio > thresholds[threshold_key]:
                findings.append(
                    {
                        "check": f"{key}_budget_failed",
                        "profile": profile,
                        "ratio": ratio,
                    }
                )
        lag_recorded = measurements["oldest_queue_age_seconds_by_profile"].get(profile)
        baseline_lag = _baseline_scalar(lag_recorded, "p95")
        observed_lag = observed.get("oldest_queue_age_seconds_p95")
        if observed_lag is not None:
            observed_lag = _require_measure(
                observed_lag, f"{profile}.oldest_queue_age_seconds_p95"
            )
        if baseline_lag is None or observed_lag is None:
            entry["lag_seconds"] = {"status": "missing"}
            # Same rule as the CPU and memory budgets: queue health that was
            # not measured has not been shown to be inside the envelope.
            findings.append({"check": "lag_seconds_not_measurable", "profile": profile})
        else:
            lag_delta = float(observed_lag) - float(baseline_lag)
            entry["lag_seconds"] = {
                "baseline": baseline_lag,
                "go": observed_lag,
                "delta": lag_delta,
            }
            if lag_delta > thresholds["lag_seconds_delta_max"]:
                findings.append({"check": "lag_parity_failed", "profile": profile})
        comparisons.append(entry)

    approved = bool(proof.threshold_review_approved(documents))
    authoritative = bool(baseline.get("authoritative_for_baseline"))

    if not approved or not authoritative:
        verdict = VERDICT_UNPROVEN
        reason = (
            "thresholds_unapproved" if not approved else "baseline_not_authoritative"
        )
    elif findings:
        verdict = VERDICT_OUTSIDE_ENVELOPE
        reason = None
    else:
        verdict = VERDICT_WITHIN_ENVELOPE
        reason = None

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "claim": CLAIM_RUNTIME,
        "evidence": {
            "baseline": {
                "path": documents["baseline"].path,
                "sha256": documents["baseline"].sha256,
                "scope": baseline.get("scope"),
                "authoritative_for_baseline": authoritative,
            },
            "thresholds": {
                "path": documents["thresholds"].path,
                "sha256": documents["thresholds"].sha256,
                "review_approved": approved,
            },
        },
        "observation": {
            "build": observation.get("build"),
            "window": observation.get("window"),
            "dataset_scope": observation.get("dataset_scope"),
            "run_scope": observation.get("run_scope"),
            # The attestation is SELF-DECLARED. The digests and the build
            # identity are checked for shape and internal consistency, not
            # recomputed from an independent source and not resolved against an
            # artifact registry, because no such attestation plane exists in
            # this repo yet. A reader must weigh this claim by how much they
            # trust whoever produced the file. Recorded here rather than left
            # implied, and it is the reason this mode never says "verified".
            "attestation": "self_declared",
        },
        "comparisons": comparisons,
        "findings": findings,
        "verdict": verdict,
        "reason": reason,
    }


RUNTIME_OBSERVATION_KEYS = {
    "schema_version",
    "runtime",
    "captured_at",
    "window",
    "build",
    "dataset_scope",
    "run_scope",
    "families",
    "profiles",
}


def _validate_runtime_observation(observation: Mapping[str, Any], proof: Any) -> None:
    """Enforce the attestation contract for a normalized runtime observation.

    ``canary_release_proof.validate_observation`` is deliberately NOT reused
    wholesale: it validates the v3 *canary* artifact, which pairs a Celery
    observation with a Go one and requires a route transport, a route
    generation step, and rollback evidence. Post-cutover there is no Celery
    side and no route flip, so that schema does not describe this input.

    What is reused is its attestation rigour and its primitives -- the digest
    and revision patterns, the timestamp parser, the sensitive-key refusal --
    so that a set of in-envelope numbers cannot be accepted without saying
    which build, which dataset, and which window produced them.
    """
    unknown = sorted(set(observation) - RUNTIME_OBSERVATION_KEYS)
    if unknown:
        raise ComparisonError(f"observation_unknown_fields:{','.join(unknown)}")
    missing = sorted(RUNTIME_OBSERVATION_KEYS - set(observation))
    if missing:
        raise ComparisonError(f"observation_missing_fields:{','.join(missing)}")
    if observation["schema_version"] != 1:
        raise ComparisonError("observation_schema_version_unsupported")
    for key in ("dataset_scope", "run_scope"):
        if not proof.valid_digest(observation[key]):
            raise ComparisonError(f"observation_scope_digest_invalid:{key}")

    build = observation["build"]
    if not isinstance(build, Mapping) or set(build) != {"revision", "image_digest"}:
        raise ComparisonError("observation_build_invalid")
    if not proof.REVISION.match(str(build["revision"])):
        raise ComparisonError("observation_build_revision_invalid")
    digest = str(build["image_digest"])
    if not digest.startswith("sha256:") or not proof.valid_digest(digest[7:]):
        raise ComparisonError("observation_build_image_digest_invalid")

    window = observation["window"]
    if not isinstance(window, Mapping) or set(window) != {"start", "end", "timezone"}:
        raise ComparisonError("observation_window_invalid")
    if window["timezone"] != "UTC":
        raise ComparisonError("observation_window_not_utc")
    start = proof.parse_utc_timestamp(window["start"])
    end = proof.parse_utc_timestamp(window["end"])
    captured = proof.parse_utc_timestamp(observation["captured_at"])
    if not (start and end and captured) or not (start < end <= captured):
        raise ComparisonError("observation_window_order_invalid")


def _require_measure(value: Any, name: str) -> float:
    """Refuse a runtime measurement that arithmetic cannot be trusted on.

    Python's JSON decoder accepts ``NaN`` and ``Infinity``, and nothing in the
    thresholds rejects a negative. Any of the three makes every ``>`` check
    below return False, so a malformed observation would produce no findings at
    all -- the shape of a pass. Reject before comparing, not after.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ComparisonError(f"observation_measure_not_a_number:{name}")
    number = float(value)
    # math.isfinite covers NaN and both infinities in one predicate. The
    # earlier `number != number` spelling was the C idiom for NaN and CodeQL
    # flagged it as a comparison of identical expressions -- correctly, since
    # a reader cannot tell an intentional NaN test from a typo.
    if not math.isfinite(number):
        raise ComparisonError(f"observation_measure_not_finite:{name}")
    if number < 0:
        raise ComparisonError(f"observation_measure_negative:{name}")
    return number


def _require_counts(observed: Any, name: str) -> None:
    """Refuse an observation series whose counts are absent or not integers."""
    if not isinstance(observed, Mapping):
        raise ComparisonError(f"observation_series_shape_invalid:{name}")
    counts = observed.get("counts")
    if not isinstance(counts, Mapping):
        raise ComparisonError(f"observation_counts_missing:{name}")
    for key in ("success", "retry", "failure", "discard"):
        value = counts.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ComparisonError(f"observation_counts_invalid:{name}.{key}")


def _baseline_scalar(recorded: Any, key: str = "p50") -> float | None:
    if not isinstance(recorded, Mapping):
        return None
    if recorded.get("status") not in (None, "observed"):
        return None
    value = recorded.get(key, recorded.get("value"))
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    # A non-finite or negative baseline is unusable as a denominator or a
    # reference point; treat it as no baseline rather than compare against it.
    if not math.isfinite(number) or number < 0:
        return None
    return number


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observation", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    try:
        report = compare_runtime(args.observation)
    except ComparisonError as error:
        print(json.dumps({"status": "error", "failure": str(error)}), file=sys.stderr)
        return 2
    text = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.out is None:
        print(text)
    else:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")
        print(json.dumps({"verdict": report["verdict"], "report": str(args.out)}))
    if report["verdict"] == VERDICT_WITHIN_ENVELOPE:
        return 0
    if report["verdict"] == VERDICT_OUTSIDE_ENVELOPE:
        return 1
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
