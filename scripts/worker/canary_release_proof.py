#!/usr/bin/env python3
"""Fail-closed, redacted Celery/Go canary release-proof evaluator.

The runners collect two bounded observations of the same dataset/run scope and
this program writes a durable *candidate* artifact. It never claims a release
is proven: production attestation and two stable releases remain external.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class ProofError(ValueError):
    """A safe, non-sensitive reason the proof cannot be accepted."""


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


def load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProofError("observation_unavailable") from error
    return validate_observation(value)


def validate_observation(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or set(value) != REQUIRED
        or value.get("schema_version") != 1
    ):
        raise ProofError("observation_shape_invalid")
    if value.get("runtime") not in {"celery", "go"}:
        raise ProofError("runtime_invalid")
    for name in ("dataset_scope", "run_scope"):
        if (
            not isinstance(value.get(name), str)
            or SHA256.fullmatch(value[name]) is None
        ):
            raise ProofError("scope_invalid")
    if (
        not isinstance(value["build"], dict)
        or set(value["build"])
        != {
            "revision",
            "image_digest",
        }
        or not isinstance(value["build"]["revision"], str)
        or REVISION.fullmatch(value["build"]["revision"]) is None
        or not isinstance(value["build"]["image_digest"], str)
        or not value["build"]["image_digest"].startswith("sha256:")
        or SHA256.fullmatch(value["build"]["image_digest"][7:]) is None
    ):
        raise ProofError("build_invalid")
    if (
        not isinstance(value["route"], dict)
        or set(value["route"])
        != {
            "kind",
            "generation",
            "transport",
        }
        or not isinstance(value["route"]["kind"], str)
        or KIND.fullmatch(value["route"]["kind"]) is None
        or not valid_int(value["route"]["generation"], 1)
        or value["route"]["transport"] not in {"celery", "river_canary"}
    ):
        raise ProofError("route_invalid")
    if not isinstance(value["checks"], dict) or set(value["checks"]) != CHECKS:
        raise ProofError("checks_invalid")
    for name in ("input_digest", "output_digest", "state_digest", "idempotency_digest"):
        if (
            not isinstance(value["checks"][name], str)
            or SHA256.fullmatch(value["checks"][name]) is None
        ):
            raise ProofError("checks_invalid")
    if (
        not valid_number(value["checks"]["lag_seconds"])
        or not valid_int(value["checks"]["error_count"], 0)
        or not valid_number(value["checks"]["cpu_cores"])
        or not valid_number(value["checks"]["memory_bytes"])
    ):
        raise ProofError("checks_invalid")
    if (
        not isinstance(value["quiescence"], dict)
        or set(value["quiescence"])
        != {
            "proven",
            "celery_pending",
            "river_pending",
        }
        or not isinstance(value["quiescence"]["proven"], bool)
        or not valid_int(value["quiescence"]["celery_pending"], 0)
        or not valid_int(value["quiescence"]["river_pending"], 0)
    ):
        raise ProofError("quiescence_invalid")
    if (
        not isinstance(value["rollback"], dict)
        or set(value["rollback"])
        != {
            "attempted",
            "transport",
            "generation",
            "celery_pending",
            "river_pending",
        }
        or not isinstance(value["rollback"]["attempted"], bool)
        or value["rollback"]["transport"] != "celery"
        or not valid_int(value["rollback"]["generation"], 1)
        or not valid_int(value["rollback"]["celery_pending"], 0)
        or not valid_int(value["rollback"]["river_pending"], 0)
    ):
        raise ProofError("rollback_invalid")
    reject_sensitive_keys(value)
    return value


def valid_int(value: Any, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def valid_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def load_document(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProofError("contract_unavailable") from error
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise ProofError("contract_shape_invalid")
    return value


def reject_sensitive_keys(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in FORBIDDEN:
                raise ProofError("observation_contains_sensitive_key")
            reject_sensitive_keys(child)
    elif isinstance(value, list):
        for child in value:
            reject_sensitive_keys(child)


def canary_supported(
    registry: dict[str, Any], migration_state: dict[str, Any], kind: str
) -> bool:
    if kind == "investment.dispatch":
        return False
    registered = any(
        job.get("kind") == kind and job.get("handler_owner")
        for job in registry.get("jobs", [])
    )
    for job in migration_state.get("jobs", []):
        if job.get("kind") == kind:
            return (
                registered
                and job.get("state") == "canary"
                and job.get("route") == "river_canary"
                and job.get("rollback_route") == "celery"
            )
    return False


def failures(
    celery: dict[str, Any],
    go: dict[str, Any],
    registry: dict[str, Any],
    migration_state: dict[str, Any],
) -> list[str]:
    result: list[str] = []
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
    if not canary_supported(registry, migration_state, kind):
        result.append("canary_not_executable")
    if (
        celery["route"]["transport"] != "celery"
        or go["route"]["transport"] != "river_canary"
    ):
        result.append("route_transport_invalid")
    if celery["route"]["generation"] != go["route"]["generation"]:
        result.append("route_generation_mismatch")
    for key in ("input_digest", "output_digest", "state_digest", "idempotency_digest"):
        if celery["checks"][key] != go["checks"][key]:
            result.append(f"{key}_mismatch")
    if float(go["checks"]["lag_seconds"]) > float(celery["checks"]["lag_seconds"]) + 5:
        result.append("lag_parity_failed")
    if int(go["checks"]["error_count"]) > int(celery["checks"]["error_count"]):
        result.append("error_parity_failed")
    for key in ("cpu_cores", "memory_bytes"):
        if float(go["checks"][key]) > float(celery["checks"][key]) * 1.25:
            result.append(f"{key}_budget_failed")
    for observation in (celery, go):
        if observation["quiescence"] != {
            "proven": True,
            "celery_pending": 0,
            "river_pending": 0,
        }:
            result.append("route_quiescence_unproven")
        rollback = observation["rollback"]
        if (
            not rollback["attempted"]
            or rollback["transport"] != "celery"
            or rollback["generation"] <= observation["route"]["generation"]
            or rollback["celery_pending"]
            or rollback["river_pending"]
        ):
            result.append("rollback_unproven")
    return sorted(set(result))


def artifact(
    celery: dict[str, Any],
    go: dict[str, Any],
    registry: dict[str, Any],
    migration_state: dict[str, Any],
) -> dict[str, Any]:
    celery = validate_observation(celery)
    go = validate_observation(go)
    result = failures(celery, go, registry, migration_state)
    inputs = {
        "dataset_scope": celery["dataset_scope"],
        "run_scope": celery["run_scope"],
        "kind": celery["route"]["kind"],
        "route_generation": celery["route"]["generation"],
    }
    fingerprint = hashlib.sha256(
        json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "schema_version": 1,
        "evidence_version": "v3-canary-release-proof",
        "captured_at": datetime.now(UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "scope": "local_or_preproduction_canary",
        "input_fingerprint": fingerprint,
        "inputs": inputs,
        "builds": {"celery": celery["build"], "go": go["build"]},
        "result": {"status": "pass" if not result else "fail", "failures": result},
        "proofs": {
            "output_state_idempotency": not any(
                "digest_mismatch" in item for item in result
            ),
            "lag_error_resource": not any(item.endswith("_failed") for item in result),
            "route_quiescence": "route_quiescence_unproven" not in result,
            "rollback": "rollback_unproven" not in result,
        },
        "release_eligibility": {
            "eligible": False,
            "reason": "requires_two_independently_attested_stable_production_releases",
        },
        "redaction": {
            "contains_payloads_or_identifiers": False,
            "contains_credentials_or_dsns": False,
        },
    }


def atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--celery-observation", type=Path, required=True)
    parser.add_argument("--go-observation", type=Path, required=True)
    parser.add_argument(
        "--registry", type=Path, default=Path("contracts/jobs/v1/registry.json")
    )
    parser.add_argument(
        "--migration-state",
        type=Path,
        default=Path("contracts/jobs/v1/migration-state.json"),
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        proof = artifact(
            load(args.celery_observation),
            load(args.go_observation),
            load_document(args.registry),
            load_document(args.migration_state),
        )
    except ProofError as error:
        print(json.dumps({"status": "fail", "failure": str(error)}))
        return 2
    atomic_write(args.output, proof)
    print(json.dumps({"status": proof["result"]["status"], "output": str(args.output)}))
    return 0 if proof["result"]["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
