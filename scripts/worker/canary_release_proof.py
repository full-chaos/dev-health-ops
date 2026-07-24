#!/usr/bin/env python3
"""Fail-closed, redacted Celery/Go canary release-proof evaluator.

The runner evaluates two bounded observations and writes a durable *candidate*
artifact.  It never changes a route, starts workers, or writes either database.
It also never promotes local comparison to a release proof.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class ProofError(ValueError):
    """A safe, non-sensitive reason the proof cannot be accepted."""


REPO_ROOT = Path(__file__).resolve().parents[2]
PINNED_PATHS = {
    "registry": REPO_ROOT / "contracts/jobs/v1/registry.json",
    "migration_state": REPO_ROOT / "contracts/jobs/v1/migration-state.json",
    "thresholds": REPO_ROOT
    / "docs/architecture/evidence/go-worker-migration/v3-canary-release-proof/parity-thresholds.json",
    "baseline": REPO_ROOT
    / "docs/architecture/evidence/go-worker-migration/v0-celery-baseline/capture.json",
}
REQUIRED = {
    "schema_version",
    "runtime",
    "dataset_scope",
    "run_scope",
    "build",
    "route",
    "checks",
    "quiescence",
    "rollback",
}
CHECKS = {
    "input_digest",
    "output_digest",
    "state_digest",
    "idempotency_digest",
    "lag_seconds",
    "error_count",
    "cpu_cores",
    "memory_bytes",
}
ROLLBACK = {
    "kind",
    "source_route",
    "restored_route",
    "completed",
    "success",
    "operation_digest",
    "evidence_digest",
    "completed_at",
    "quiescence",
    "post_rollback",
}
ROUTE = {"kind", "generation", "transport", "paused", "source", "observed_at"}
ROLLBACK_ROUTE = {"kind", "generation", "transport", "paused"}
QUIESCENCE = {
    "outbox_pending_or_claimed",
    "semantic_runs_running",
    "river_jobs_active",
    "external_quiescer",
    "evidence_digest",
}
FORBIDDEN = {
    "dsn",
    "uri",
    "password",
    "token",
    "payload",
    "args",
    "tenant",
    "org_id",
    "run_id",
    "snapshot",
}
SHA256 = re.compile(r"^[0-9a-f]{64}$")
REVISION = re.compile(r"^[0-9a-f]{7,64}$")
KIND = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")


class PinnedDocument:
    """A checked-in document plus the exact bytes used for this evaluation."""

    def __init__(self, path: str, sha256: str, value: dict[str, Any]) -> None:
        self.path = path
        self.sha256 = sha256
        self.value = value


def relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError as error:
        raise ProofError("checked_in_contract_override_rejected") from error


def load_json(path: Path, unavailable: str) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ProofError(unavailable) from error
    if not isinstance(value, dict):
        raise ProofError("contract_shape_invalid")
    return value, hashlib.sha256(raw).hexdigest()


def load(path: Path) -> dict[str, Any]:
    value, _ = load_json(path, "observation_unavailable")
    return validate_observation(value)


def valid_int(value: Any, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def valid_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def valid_digest(value: Any) -> bool:
    return isinstance(value, str) and SHA256.fullmatch(value) is not None


def parse_utc_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return None
    if parsed.tzinfo != UTC:
        return None
    return parsed


def valid_route(value: Any, transports: set[str]) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == ROUTE
        and isinstance(value["kind"], str)
        and KIND.fullmatch(value["kind"]) is not None
        and valid_int(value["generation"], 1)
        and value["transport"] in transports
        and isinstance(value["paused"], bool)
        and value["paused"] is False
        and value["source"] == "worker_job_routes"
        and parse_utc_timestamp(value["observed_at"]) is not None
    )


def valid_rollback_route(value: Any, transports: set[str]) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == ROLLBACK_ROUTE
        and isinstance(value["kind"], str)
        and KIND.fullmatch(value["kind"]) is not None
        and valid_int(value["generation"], 1)
        and value["transport"] in transports
        and value["paused"] is False
    )


def valid_quiescence(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == QUIESCENCE
        and valid_int(value["outbox_pending_or_claimed"], 0)
        and valid_int(value["semantic_runs_running"], 0)
        and valid_int(value["river_jobs_active"], 0)
        and value["external_quiescer"] in {"passed", "failed"}
        and valid_digest(value["evidence_digest"])
    )


def is_quiescent(value: Any) -> bool:
    return valid_quiescence(value) and (
        value["outbox_pending_or_claimed"] == 0
        and value["semantic_runs_running"] == 0
        and value["river_jobs_active"] == 0
        and value["external_quiescer"] == "passed"
    )


def validate_rollback(value: Any) -> None:
    if not isinstance(value, dict) or set(value) != ROLLBACK:
        raise ProofError("rollback_invalid")
    if (
        not isinstance(value["kind"], str)
        or KIND.fullmatch(value["kind"]) is None
        or not valid_rollback_route(value["source_route"], {"river_canary"})
        or not valid_rollback_route(value["restored_route"], {"celery"})
        or not isinstance(value["completed"], bool)
        or not isinstance(value["success"], bool)
        or not valid_digest(value["operation_digest"])
        or not valid_digest(value["evidence_digest"])
        or parse_utc_timestamp(value["completed_at"]) is None
        or not valid_quiescence(value["quiescence"])
        or not isinstance(value["post_rollback"], dict)
        or set(value["post_rollback"])
        != {
            "served_by",
            "observed",
            "route_generation",
            "observation_digest",
            "observed_at",
        }
        or value["post_rollback"]["served_by"] != "celery"
        or not isinstance(value["post_rollback"]["observed"], bool)
        or not valid_int(value["post_rollback"]["route_generation"], 1)
        or not valid_digest(value["post_rollback"]["observation_digest"])
        or parse_utc_timestamp(value["post_rollback"]["observed_at"]) is None
    ):
        raise ProofError("rollback_invalid")


def validate_observation(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) != REQUIRED
        or value.get("schema_version") != 2
    ):
        raise ProofError("observation_shape_invalid")
    if value.get("runtime") not in {"celery", "go"}:
        raise ProofError("runtime_invalid")
    for name in ("dataset_scope", "run_scope"):
        if not valid_digest(value.get(name)):
            raise ProofError("scope_invalid")
    if (
        not isinstance(value["build"], dict)
        or set(value["build"]) != {"revision", "image_digest"}
        or not isinstance(value["build"]["revision"], str)
        or REVISION.fullmatch(value["build"]["revision"]) is None
        or not isinstance(value["build"]["image_digest"], str)
        or not value["build"]["image_digest"].startswith("sha256:")
        or SHA256.fullmatch(value["build"]["image_digest"][7:]) is None
    ):
        raise ProofError("build_invalid")
    if not valid_route(value["route"], {"celery", "river_canary"}):
        raise ProofError("route_invalid")
    if not isinstance(value["checks"], dict) or set(value["checks"]) != CHECKS:
        raise ProofError("checks_invalid")
    for name in ("input_digest", "output_digest", "state_digest", "idempotency_digest"):
        if not valid_digest(value["checks"][name]):
            raise ProofError("checks_invalid")
    if (
        not valid_number(value["checks"]["lag_seconds"])
        or not valid_int(value["checks"]["error_count"], 0)
        or not valid_number(value["checks"]["cpu_cores"])
        or not valid_number(value["checks"]["memory_bytes"])
    ):
        raise ProofError("checks_invalid")
    if not valid_quiescence(value["quiescence"]):
        raise ProofError("quiescence_invalid")
    validate_rollback(value["rollback"])
    reject_sensitive_keys(value)
    return value


def validate_registry(value: dict[str, Any]) -> None:
    jobs = value.get("jobs")
    if value.get("schema_version") != 1 or not isinstance(jobs, list) or not jobs:
        raise ProofError("registry_invalid")
    seen: set[str] = set()
    for job in jobs:
        if (
            not isinstance(job, dict)
            or not isinstance(job.get("kind"), str)
            or KIND.fullmatch(job["kind"]) is None
            or not isinstance(job.get("handler_owner"), str)
            or not job["handler_owner"]
            or not valid_int(job.get("current_version"), 1)
            or not isinstance(job.get("supported_versions"), list)
            or not job["supported_versions"]
            or not all(valid_int(version, 1) for version in job["supported_versions"])
            or job["current_version"] not in job["supported_versions"]
            or not isinstance(job.get("profile"), str)
            or not job["profile"]
            or job["kind"] in seen
        ):
            raise ProofError("registry_invalid")
        seen.add(job["kind"])


def validate_migration_state(value: dict[str, Any]) -> None:
    jobs = value.get("jobs")
    allowed_states = {
        "inventory",
        "contract_frozen",
        "go_implemented",
        "shadow",
        "canary",
        "go_default",
        "celery_fallback_only",
        "celery_removed",
    }
    if value.get("schema_version") != 1 or not isinstance(jobs, list) or not jobs:
        raise ProofError("migration_state_invalid")
    seen: set[str] = set()
    for job in jobs:
        if (
            not isinstance(job, dict)
            or not isinstance(job.get("kind"), str)
            or KIND.fullmatch(job["kind"]) is None
            or job.get("state") not in allowed_states
            or job.get("route")
            not in {"celery", "shadow", "river_canary", "river", "removed"}
            or job.get("rollback_route") not in {"celery", "river", "none"}
            or not valid_int(job.get("producer_version"), 1)
            or not isinstance(job.get("consumer_versions"), list)
            or not job["consumer_versions"]
            or not all(valid_int(version, 1) for version in job["consumer_versions"])
            or not isinstance(job.get("required_profiles"), list)
            or not job["required_profiles"]
            or not all(
                isinstance(profile, str) and profile
                for profile in job["required_profiles"]
            )
            or job["kind"] in seen
        ):
            raise ProofError("migration_state_invalid")
        seen.add(job["kind"])


def validate_baseline(value: dict[str, Any]) -> None:
    review = value.get("review")
    gates = value.get("gates")
    gaps = value.get("observability_gaps")
    if (
        value.get("schema_version") != 1
        or not isinstance(review, dict)
        or not isinstance(review.get("parity_thresholds_approved"), bool)
        or not isinstance(value.get("authoritative_for_canary"), bool)
        or not isinstance(gaps, list)
        or not isinstance(gates, dict)
        or not isinstance(gates.get("production_canary"), str)
    ):
        raise ProofError("baseline_invalid")


def validate_thresholds(value: dict[str, Any], baseline: PinnedDocument) -> None:
    required = {
        "schema_version",
        "evidence_version",
        "baseline",
        "thresholds",
        "review",
    }
    if (
        set(value) != required
        or value.get("schema_version") != 1
        or value.get("evidence_version") != "v3-canary-release-proof"
    ):
        raise ProofError("thresholds_invalid")
    source = value["baseline"]
    thresholds = value["thresholds"]
    review = value["review"]
    if (
        not isinstance(source, dict)
        or set(source) != {"path", "sha256"}
        or source["path"] != baseline.path
        or source["sha256"] != baseline.sha256
        or not isinstance(thresholds, dict)
        or set(thresholds)
        != {
            "lag_seconds_delta_max",
            "error_count_delta_max",
            "celery_error_count_max",
            "go_error_count_max",
            "cpu_cores_multiplier_max",
            "memory_bytes_multiplier_max",
        }
        or not all(valid_number(item) for item in thresholds.values())
        or not isinstance(review, dict)
        or set(review) != {"reviewed_by", "reviewed_at", "approved"}
        or not isinstance(review["reviewed_by"], list)
        or not all(isinstance(item, str) and item for item in review["reviewed_by"])
        or review["reviewed_at"] is not None
        and parse_utc_timestamp(review["reviewed_at"]) is None
        or not isinstance(review["approved"], bool)
    ):
        raise ProofError("thresholds_invalid")


def load_pinned_documents() -> dict[str, PinnedDocument]:
    documents: dict[str, PinnedDocument] = {}
    for name, path in PINNED_PATHS.items():
        value, digest = load_json(path, "checked_in_contract_unavailable")
        documents[name] = PinnedDocument(relative_path(path), digest, value)
    validate_documents(documents)
    return documents


def validate_documents(documents: dict[str, PinnedDocument]) -> None:
    if set(documents) != set(PINNED_PATHS):
        raise ProofError("checked_in_contract_invalid")
    registry = documents["registry"].value
    migration_state = documents["migration_state"].value
    validate_registry(registry)
    validate_migration_state(migration_state)
    validate_baseline(documents["baseline"].value)
    validate_thresholds(documents["thresholds"].value, documents["baseline"])
    registry_by_kind = {job["kind"]: job for job in registry["jobs"]}
    migration_by_kind = {job["kind"]: job for job in migration_state["jobs"]}
    if set(registry_by_kind) != set(migration_by_kind):
        raise ProofError("contract_kind_set_mismatch")
    for kind, policy in migration_by_kind.items():
        registered = registry_by_kind[kind]
        if (
            policy["producer_version"] != registered["current_version"]
            or not set(policy["consumer_versions"]).issubset(
                set(registered["supported_versions"])
            )
            or set(policy["required_profiles"]) != {registered["profile"]}
        ):
            raise ProofError("contract_job_policy_mismatch")


def reject_sensitive_keys(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in FORBIDDEN:
                raise ProofError("observation_contains_sensitive_key")
            reject_sensitive_keys(child)
    elif isinstance(value, list):
        for child in value:
            reject_sensitive_keys(child)


def canary_supported(documents: dict[str, PinnedDocument], kind: str) -> bool:
    if kind == "investment.dispatch":
        return False
    registered = any(
        job["kind"] == kind and job["handler_owner"]
        for job in documents["registry"].value["jobs"]
    )
    return (
        any(
            job["kind"] == kind
            and job["state"] == "canary"
            and job["route"] == "river_canary"
            and job["rollback_route"] == "celery"
            for job in documents["migration_state"].value["jobs"]
        )
        and registered
    )


def threshold_review_approved(documents: dict[str, PinnedDocument]) -> bool:
    baseline = documents["baseline"].value
    return bool(
        documents["thresholds"].value["review"]["approved"]
        and documents["thresholds"].value["review"]["reviewed_by"]
        and documents["thresholds"].value["review"]["reviewed_at"]
        and baseline["review"]["parity_thresholds_approved"]
        and baseline["authoritative_for_canary"]
        and not baseline["observability_gaps"]
        and baseline["gates"]["production_canary"] == "approved"
    )


def ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return 0.0 if numerator == 0 else None
    return numerator / denominator


def measurements(
    celery: dict[str, Any], go: dict[str, Any]
) -> dict[str, float | int | None]:
    return {
        "lag_seconds_delta": float(go["checks"]["lag_seconds"])
        - float(celery["checks"]["lag_seconds"]),
        "error_count_delta": int(go["checks"]["error_count"])
        - int(celery["checks"]["error_count"]),
        "cpu_cores_ratio": ratio(
            float(go["checks"]["cpu_cores"]), float(celery["checks"]["cpu_cores"])
        ),
        "memory_bytes_ratio": ratio(
            float(go["checks"]["memory_bytes"]),
            float(celery["checks"]["memory_bytes"]),
        ),
    }


def failures(
    celery: dict[str, Any], go: dict[str, Any], documents: dict[str, PinnedDocument]
) -> list[str]:
    result: list[str] = []
    validate_documents(documents)
    if celery["runtime"] != "celery" or go["runtime"] != "go":
        result.append("runtime_pair_invalid")
    if (
        celery["dataset_scope"] != go["dataset_scope"]
        or celery["run_scope"] != go["run_scope"]
    ):
        result.append("scope_mismatch")
    if celery["route"]["kind"] != go["route"]["kind"]:
        result.append("kind_mismatch")
    kind = str(go["route"]["kind"])
    if not canary_supported(documents, kind):
        result.append("canary_not_executable")
    if (
        celery["route"]["transport"] != "celery"
        or go["route"]["transport"] != "river_canary"
    ):
        result.append("route_transport_invalid")
    if go["route"]["generation"] != celery["route"]["generation"] + 1:
        result.append("canary_route_generation_invalid")
    for key in ("input_digest", "output_digest", "state_digest", "idempotency_digest"):
        if celery["checks"][key] != go["checks"][key]:
            result.append(f"{key}_mismatch")
    for observation in (celery, go):
        if not is_quiescent(observation["quiescence"]):
            result.append("route_quiescence_unproven")
    rollback = go["rollback"]
    if celery["rollback"] != rollback:
        result.append("rollback_evidence_mismatch")
    if (
        rollback["kind"] != kind
        or rollback["source_route"] != {key: go["route"][key] for key in ROLLBACK_ROUTE}
        or rollback["restored_route"]["kind"] != kind
        or rollback["restored_route"]["transport"] != "celery"
        or rollback["restored_route"]["generation"] != go["route"]["generation"] + 1
        or not rollback["completed"]
        or not rollback["success"]
        or not is_quiescent(rollback["quiescence"])
        or not rollback["post_rollback"]["observed"]
        or rollback["post_rollback"]["served_by"] != "celery"
        or rollback["post_rollback"]["route_generation"]
        != rollback["restored_route"]["generation"]
    ):
        result.append("rollback_unproven")
    baseline_at = parse_utc_timestamp(celery["route"]["observed_at"])
    canary_at = parse_utc_timestamp(go["route"]["observed_at"])
    completed_at = parse_utc_timestamp(rollback["completed_at"])
    served_at = parse_utc_timestamp(rollback["post_rollback"]["observed_at"])
    if not (baseline_at and canary_at and completed_at and served_at) or not (
        baseline_at <= canary_at <= completed_at <= served_at
    ):
        result.append("route_evidence_order_invalid")
    if not threshold_review_approved(documents):
        result.append("thresholds_unapproved")
    else:
        current = measurements(celery, go)
        threshold = documents["thresholds"].value["thresholds"]
        if current["lag_seconds_delta"] > threshold["lag_seconds_delta_max"]:
            result.append("lag_parity_failed")
        if current["error_count_delta"] > threshold["error_count_delta_max"]:
            result.append("error_parity_failed")
        if int(celery["checks"]["error_count"]) > threshold["celery_error_count_max"]:
            result.append("celery_error_count_ceiling_failed")
        if int(go["checks"]["error_count"]) > threshold["go_error_count_max"]:
            result.append("go_error_count_ceiling_failed")
        if (
            current["cpu_cores_ratio"] is None
            or current["cpu_cores_ratio"] > threshold["cpu_cores_multiplier_max"]
        ):
            result.append("cpu_cores_budget_failed")
        if (
            current["memory_bytes_ratio"] is None
            or current["memory_bytes_ratio"] > threshold["memory_bytes_multiplier_max"]
        ):
            result.append("memory_bytes_budget_failed")
    return sorted(set(result))


def artifact(
    celery: dict[str, Any], go: dict[str, Any], documents: dict[str, PinnedDocument]
) -> dict[str, Any]:
    celery = validate_observation(celery)
    go = validate_observation(go)
    result = failures(celery, go, documents)
    inputs = {
        "dataset_scope": celery["dataset_scope"],
        "run_scope": celery["run_scope"],
        "kind": celery["route"]["kind"],
        "celery_baseline_generation": celery["route"]["generation"],
        "river_canary_generation": go["route"]["generation"],
        "restored_celery_generation": go["rollback"]["restored_route"]["generation"],
    }
    fingerprint = hashlib.sha256(
        json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    approved = threshold_review_approved(documents)
    return {
        "schema_version": 2,
        "evidence_version": "v3-canary-release-proof",
        "captured_at": datetime.now(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "scope": "local_or_preproduction_canary",
        "input_fingerprint": fingerprint,
        "inputs": inputs,
        "builds": {"celery": celery["build"], "go": go["build"]},
        "checked_in_documents": {
            name: {"path": item.path, "sha256": item.sha256}
            for name, item in documents.items()
        },
        "measurements": measurements(celery, go),
        "result": {"status": "pass" if not result else "fail", "failures": result},
        "proofs": {
            "output_state_idempotency": not any(
                "digest_mismatch" in item for item in result
            ),
            "lag_error_resource": approved
            and not any(item.endswith("_failed") for item in result),
            "thresholds_reviewed_and_approved": approved,
            "route_quiescence": "route_quiescence_unproven" not in result,
            "rollback": not any(
                item.startswith("rollback_") or item == "route_evidence_order_invalid"
                for item in result
            ),
        },
        "release_eligibility": {
            "eligible": False,
            "reason": "requires_reviewed_thresholds_and_two_independently_attested_stable_production_releases",
        },
        "redaction": {
            "contains_payloads_or_identifiers": False,
            "contains_credentials_or_dsns": False,
        },
    }


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def invalidate_output(path: Path) -> None:
    """Remove only the requested file entry; never recurse or follow a symlink."""
    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        return
    except OSError as error:
        raise ProofError("output_path_invalid") from error
    if not (stat.S_ISREG(mode) or stat.S_ISLNK(mode)):
        raise ProofError("output_path_invalid")
    try:
        path.unlink()
        fsync_directory(path.parent)
    except OSError as error:
        raise ProofError("output_invalidation_failed") from error


def reject_override(path: Path | None, expected: Path) -> None:
    if path is not None:
        try:
            same_path = path.resolve() == expected.resolve()
        except OSError as error:
            raise ProofError("checked_in_contract_override_rejected") from error
        if not same_path:
            raise ProofError("checked_in_contract_override_rejected")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--celery-observation", type=Path, required=True)
    parser.add_argument("--go-observation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    # Compatibility flags are deliberately accepted only for the exact pinned file.
    parser.add_argument("--registry", type=Path)
    parser.add_argument("--migration-state", type=Path)
    parser.add_argument("--thresholds", type=Path)
    args = parser.parse_args(argv)
    try:
        reject_override(args.registry, PINNED_PATHS["registry"])
        reject_override(args.migration_state, PINNED_PATHS["migration_state"])
        reject_override(args.thresholds, PINNED_PATHS["thresholds"])
        proof = artifact(
            load(args.celery_observation),
            load(args.go_observation),
            load_pinned_documents(),
        )
        if proof["result"]["status"] != "pass":
            invalidate_output(args.output)
            print(
                json.dumps(
                    {
                        "status": "fail",
                        "failures": proof["result"]["failures"],
                        "measurements": proof["measurements"],
                    }
                )
            )
            return 1
        atomic_write(args.output, proof)
    except (ProofError, OSError) as error:
        try:
            invalidate_output(args.output)
        except ProofError as invalidation_error:
            failure = str(invalidation_error)
        else:
            failure = (
                str(error) if isinstance(error, ProofError) else "output_unavailable"
            )
        print(json.dumps({"status": "fail", "failure": failure}))
        return 2
    print(json.dumps({"status": "pass", "output": str(args.output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
