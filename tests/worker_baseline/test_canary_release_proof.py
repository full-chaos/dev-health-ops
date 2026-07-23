from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import re
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "canary_release_proof", ROOT / "scripts/worker/canary_release_proof.py"
)
assert SPEC and SPEC.loader
proof = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(proof)


def route(kind: str, generation: int, transport: str, observed_at: str) -> dict:
    return {
        "kind": kind,
        "generation": generation,
        "transport": transport,
        "paused": False,
        "source": "worker_job_routes",
        "observed_at": observed_at,
    }


def rollback(kind: str = "sync.provider_unit") -> dict:
    digest = "1" * 64
    return {
        "kind": kind,
        "source_route": {
            "kind": kind,
            "generation": 8,
            "transport": "river_canary",
            "paused": False,
        },
        "restored_route": {
            "kind": kind,
            "generation": 9,
            "transport": "celery",
            "paused": False,
        },
        "completed": True,
        "success": True,
        "operation_digest": digest,
        "evidence_digest": "2" * 64,
        "completed_at": "2026-07-23T00:00:02Z",
        "quiescence": {
            "outbox_pending_or_claimed": 0,
            "semantic_runs_running": 0,
            "river_jobs_active": 0,
            "external_quiescer": "passed",
            "evidence_digest": "3" * 64,
        },
        "post_rollback": {
            "served_by": "celery",
            "observed": True,
            "route_generation": 9,
            "observation_digest": "4" * 64,
            "observed_at": "2026-07-23T00:00:03Z",
        },
    }


def observation(runtime: str, *, kind: str = "sync.provider_unit") -> dict:
    digest = "a" * 64
    return {
        "schema_version": 2,
        "runtime": runtime,
        "dataset_scope": digest,
        "run_scope": "b" * 64,
        "build": {"revision": "c" * 40, "image_digest": "sha256:" + "d" * 64},
        "route": route(
            kind,
            7 if runtime == "celery" else 8,
            "celery" if runtime == "celery" else "river_canary",
            "2026-07-23T00:00:00Z" if runtime == "celery" else "2026-07-23T00:00:01Z",
        ),
        "checks": {
            "input_digest": digest,
            "output_digest": "e" * 64,
            "state_digest": "f" * 64,
            "idempotency_digest": "0" * 64,
            "lag_seconds": 10,
            "error_count": 0,
            "cpu_cores": 1,
            "memory_bytes": 100,
        },
        "quiescence": {
            "outbox_pending_or_claimed": 0,
            "semantic_runs_running": 0,
            "river_jobs_active": 0,
            "external_quiescer": "passed",
            "evidence_digest": "5" * 64,
        },
        "rollback": rollback(kind),
    }


def documents() -> dict:
    return proof.load_pinned_documents()


def approved_documents() -> dict:
    result = copy.deepcopy(documents())
    baseline = copy.deepcopy(result["baseline"].value)
    baseline["review"]["parity_thresholds_approved"] = True
    baseline["authoritative_for_canary"] = True
    baseline["observability_gaps"] = []
    baseline["gates"]["production_canary"] = "approved"
    thresholds = copy.deepcopy(result["thresholds"].value)
    thresholds["review"] = {
        "reviewed_by": ["operator"],
        "reviewed_at": "2026-07-23T00:00:00Z",
        "approved": True,
    }
    result["baseline"] = proof.PinnedDocument(
        result["baseline"].path, result["baseline"].sha256, baseline
    )
    result["thresholds"] = proof.PinnedDocument(
        result["thresholds"].path, result["thresholds"].sha256, thresholds
    )
    return result


def assert_schema_valid(
    value: Any, schema: dict[str, Any], root: dict[str, Any]
) -> None:
    if "$ref" in schema:
        reference = schema["$ref"]
        assert reference.startswith("#/$defs/")
        return assert_schema_valid(
            value, root["$defs"][reference.rsplit("/", 1)[1]], root
        )
    if "const" in schema:
        assert value == schema["const"]
    if "enum" in schema:
        assert value in schema["enum"]
    types = schema.get("type")
    if types is not None:
        valid_types = types if isinstance(types, list) else [types]
        assert any(
            (
                item == "object"
                and isinstance(value, dict)
                or item == "array"
                and isinstance(value, list)
                or item == "string"
                and isinstance(value, str)
                or item == "integer"
                and isinstance(value, int)
                and not isinstance(value, bool)
                or item == "number"
                and isinstance(value, (int, float))
                and not isinstance(value, bool)
                or item == "boolean"
                and isinstance(value, bool)
                or item == "null"
                and value is None
            )
            for item in valid_types
        )
    if "pattern" in schema:
        assert isinstance(value, str) and re.fullmatch(schema["pattern"], value)
    if "minimum" in schema:
        assert value >= schema["minimum"]
    if isinstance(value, dict):
        required = schema.get("required", [])
        assert set(required) <= set(value)
        if schema.get("additionalProperties") is False:
            assert set(value) <= set(schema.get("properties", {}))
        for key, child_schema in schema.get("properties", {}).items():
            if key in value:
                assert_schema_valid(value[key], child_schema, root)
    if isinstance(value, list) and "items" in schema:
        for child in value:
            assert_schema_valid(child, schema["items"], root)


def test_current_baseline_records_measurements_but_rejects_unapproved_thresholds() -> (
    None
):
    result = proof.artifact(observation("celery"), observation("go"), documents())
    assert result["result"] == {"status": "fail", "failures": ["thresholds_unapproved"]}
    assert result["measurements"] == {
        "lag_seconds_delta": 0.0,
        "error_count_delta": 0,
        "cpu_cores_ratio": 1.0,
        "memory_bytes_ratio": 1.0,
    }
    assert result["proofs"]["lag_error_resource"] is False
    assert result["proofs"]["thresholds_reviewed_and_approved"] is False
    assert result["release_eligibility"]["eligible"] is False
    assert result["checked_in_documents"]["registry"] == {
        "path": "contracts/jobs/v1/registry.json",
        "sha256": hashlib.sha256(
            (ROOT / "contracts/jobs/v1/registry.json").read_bytes()
        ).hexdigest(),
    }


def test_approved_thresholds_allow_only_a_real_three_generation_route_chain() -> None:
    result = proof.artifact(
        observation("celery"), observation("go"), approved_documents()
    )
    assert result["result"] == {"status": "pass", "failures": []}
    assert result["proofs"]["rollback"] is True
    assert result["proofs"]["lag_error_resource"] is True


def test_emitted_v2_artifact_conforms_to_the_checked_in_schema() -> None:
    schema = json.loads(
        (
            ROOT
            / "docs/architecture/evidence/go-worker-migration/v3-canary-release-proof/artifact.schema.json"
        ).read_text()
    )
    assert_schema_valid(
        proof.artifact(observation("celery"), observation("go"), approved_documents()),
        schema,
        schema,
    )


def test_threshold_document_approval_alone_cannot_override_unapproved_baseline() -> (
    None
):
    result = documents()
    thresholds = copy.deepcopy(result["thresholds"].value)
    thresholds["review"] = {
        "reviewed_by": ["operator"],
        "reviewed_at": "2026-07-23T00:00:00Z",
        "approved": True,
    }
    result["thresholds"] = proof.PinnedDocument(
        result["thresholds"].path, result["thresholds"].sha256, thresholds
    )
    artifact = proof.artifact(observation("celery"), observation("go"), result)
    assert artifact["result"]["failures"] == ["thresholds_unapproved"]


def test_absolute_error_ceilings_and_zero_baseline_resource_budget_fail_closed() -> (
    None
):
    celery = observation("celery")
    go = observation("go")
    celery["checks"]["error_count"] = go["checks"]["error_count"] = 1_000_000
    celery["checks"]["cpu_cores"] = 0
    go["checks"]["cpu_cores"] = 1
    artifact = proof.artifact(celery, go, approved_documents())
    assert {
        "celery_error_count_ceiling_failed",
        "go_error_count_ceiling_failed",
        "cpu_cores_budget_failed",
    } <= set(artifact["result"]["failures"])
    assert artifact["measurements"]["cpu_cores_ratio"] is None


def test_baseline_authority_gaps_and_gate_are_independent_threshold_blockers() -> None:
    result = approved_documents()
    baseline = copy.deepcopy(result["baseline"].value)
    baseline["authoritative_for_canary"] = False
    baseline["observability_gaps"] = [{"code": "gap"}]
    baseline["gates"]["production_canary"] = "blocked"
    result["baseline"] = proof.PinnedDocument(
        result["baseline"].path, result["baseline"].sha256, baseline
    )
    artifact = proof.artifact(observation("celery"), observation("go"), result)
    assert artifact["result"]["failures"] == ["thresholds_unapproved"]


def test_cross_document_kind_version_and_profile_drift_rejects_evaluation() -> None:
    result = approved_documents()
    migration_state = copy.deepcopy(result["migration_state"].value)
    matching = next(
        job for job in migration_state["jobs"] if job["kind"] == "sync.provider_unit"
    )
    matching["required_profiles"] = ["heavy"]
    result["migration_state"] = proof.PinnedDocument(
        result["migration_state"].path,
        result["migration_state"].sha256,
        migration_state,
    )
    with pytest.raises(proof.ProofError, match="contract_job_policy_mismatch"):
        proof.artifact(observation("celery"), observation("go"), result)


def test_route_evidence_chronology_invalidates_rollback_proof() -> None:
    go = observation("go")
    go["rollback"]["post_rollback"]["observed_at"] = "2026-07-23T00:00:00Z"
    artifact = proof.artifact(observation("celery"), go, approved_documents())
    assert "route_evidence_order_invalid" in artifact["result"]["failures"]
    assert artifact["proofs"]["rollback"] is False


@pytest.mark.parametrize("kind", ["investment.dispatch", "investment.chunk"])
def test_investment_dispatch_and_non_canary_routes_fail_closed(kind: str) -> None:
    result = proof.artifact(
        observation("celery", kind=kind),
        observation("go", kind=kind),
        approved_documents(),
    )
    assert "canary_not_executable" in result["result"]["failures"]


def test_route_generation_and_shared_rollback_evidence_are_bound() -> None:
    celery = observation("celery")
    go = observation("go")
    go["route"]["generation"] = 7
    go["rollback"]["operation_digest"] = "9" * 64
    failures = proof.artifact(celery, go, approved_documents())["result"]["failures"]
    assert {
        "canary_route_generation_invalid",
        "rollback_evidence_mismatch",
        "rollback_unproven",
    } <= set(failures)


@pytest.mark.parametrize(
    ("path", "value", "failure"),
    [
        ("rollback.quiescence.outbox_pending_or_claimed", 1, "rollback_unproven"),
        ("rollback.quiescence.external_quiescer", "failed", "rollback_unproven"),
        ("rollback.post_rollback.served_by", "river", "rollback_invalid"),
        ("route.observed_at", "2026-07-23T00:00:01+00:00", "route_invalid"),
    ],
)
def test_adversarial_rollback_and_control_plane_observations_fail_closed(
    path: str, value: object, failure: str
) -> None:
    go = observation("go")
    target: dict = go
    parts = path.split(".")
    for part in parts[:-1]:
        target = target[part]
    target[parts[-1]] = value
    if failure == "rollback_invalid" or failure == "route_invalid":
        with pytest.raises(proof.ProofError, match=failure):
            proof.artifact(observation("celery"), go, approved_documents())
    else:
        assert (
            failure
            in proof.artifact(observation("celery"), go, approved_documents())[
                "result"
            ]["failures"]
        )


def test_cli_pins_documents_rejects_overrides_and_invalidates_stale_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    celery = tmp_path / "celery.json"
    go = tmp_path / "go.json"
    output = tmp_path / "candidate.json"
    celery.write_text(json.dumps(observation("celery")))
    go.write_text(json.dumps(observation("go")))
    output.write_text('{"status":"pass"}')
    assert (
        proof.main(
            [
                "--celery-observation",
                str(celery),
                "--go-observation",
                str(go),
                "--output",
                str(output),
            ]
        )
        == 1
    )
    assert json.loads(capsys.readouterr().out) == {
        "status": "fail",
        "failures": ["thresholds_unapproved"],
        "measurements": {
            "lag_seconds_delta": 0.0,
            "error_count_delta": 0,
            "cpu_cores_ratio": 1.0,
            "memory_bytes_ratio": 1.0,
        },
    }
    assert not output.exists()
    output.write_text('{"status":"pass"}')
    assert (
        proof.main(
            [
                "--celery-observation",
                str(celery),
                "--go-observation",
                str(go),
                "--output",
                str(output),
                "--registry",
                str(tmp_path / "untrusted.json"),
            ]
        )
        == 2
    )
    assert not output.exists()


def test_cli_malformed_checked_in_document_returns_safe_json_and_removes_only_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    celery = tmp_path / "celery.json"
    go = tmp_path / "go.json"
    malformed = tmp_path / "registry.json"
    target = tmp_path / "keep.json"
    output = tmp_path / "candidate.json"
    celery.write_text(json.dumps(observation("celery")))
    go.write_text(json.dumps(observation("go")))
    malformed.write_text('{"schema_version":1,"jobs":[null]}')
    target.write_text("keep")
    output.symlink_to(target)
    monkeypatch.setitem(proof.PINNED_PATHS, "registry", malformed)
    monkeypatch.setattr(
        proof, "relative_path", lambda _path: "contracts/jobs/v1/registry.json"
    )
    assert (
        proof.main(
            [
                "--celery-observation",
                str(celery),
                "--go-observation",
                str(go),
                "--output",
                str(output),
            ]
        )
        == 2
    )
    assert json.loads(capsys.readouterr().out) == {
        "status": "fail",
        "failure": "registry_invalid",
    }
    assert not output.exists()
    assert target.read_text() == "keep"


def test_atomic_write_fsyncs_file_and_parent_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "candidate.json"
    fsync_calls: list[int] = []
    directory_calls: list[Path] = []
    monkeypatch.setattr(
        proof.os, "fsync", lambda descriptor: fsync_calls.append(descriptor)
    )
    monkeypatch.setattr(
        proof, "fsync_directory", lambda path: directory_calls.append(path)
    )
    proof.atomic_write(output, {"proof": "candidate"})
    assert json.loads(output.read_text()) == {"proof": "candidate"}
    assert len(fsync_calls) == 1
    assert directory_calls == [tmp_path]
