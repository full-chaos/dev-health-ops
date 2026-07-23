from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "canary_release_proof", ROOT / "scripts/worker/canary_release_proof.py"
)
assert SPEC and SPEC.loader
proof = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(proof)


def observation(runtime: str, *, kind: str = "sync.provider_unit") -> dict:
    digest = "a" * 64
    return {
        "schema_version": 1,
        "runtime": runtime,
        "dataset_scope": digest,
        "run_scope": "b" * 64,
        "build": {
            "revision": "c" * 40,
            "image_digest": "sha256:" + "d" * 64,
        },
        "route": {
            "kind": kind,
            "generation": 7,
            "transport": "celery" if runtime == "celery" else "river_canary",
        },
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
        "quiescence": {"proven": True, "celery_pending": 0, "river_pending": 0},
        "rollback": {
            "attempted": True,
            "transport": "celery",
            "generation": 8,
            "celery_pending": 0,
            "river_pending": 0,
        },
    }


def contracts() -> tuple[dict, dict]:
    return (
        json.loads((ROOT / "contracts/jobs/v1/registry.json").read_text()),
        json.loads((ROOT / "contracts/jobs/v1/migration-state.json").read_text()),
    )


def test_canary_proof_passes_only_for_checked_in_canary_and_stays_release_ineligible() -> (
    None
):
    registry, state = contracts()
    result = proof.artifact(observation("celery"), observation("go"), registry, state)
    assert result["result"] == {"status": "pass", "failures": []}
    assert result["proofs"] == {
        "output_state_idempotency": True,
        "lag_error_resource": True,
        "route_quiescence": True,
        "rollback": True,
    }
    assert result["release_eligibility"]["eligible"] is False


def test_investment_dispatch_and_non_canary_routes_fail_closed() -> None:
    registry, state = contracts()
    for kind in ("investment.dispatch", "investment.chunk"):
        result = proof.artifact(
            observation("celery", kind=kind),
            observation("go", kind=kind),
            registry,
            state,
        )
        assert "canary_not_executable" in result["result"]["failures"]


def test_parity_quiescence_and_rollback_failures_are_recorded() -> None:
    registry, state = contracts()
    go = observation("go")
    go["checks"]["output_digest"] = "1" * 64
    go["checks"]["lag_seconds"] = 16
    go["quiescence"]["river_pending"] = 1
    go["rollback"]["generation"] = 7
    failures = proof.artifact(observation("celery"), go, registry, state)["result"][
        "failures"
    ]
    assert {
        "output_digest_mismatch",
        "lag_parity_failed",
        "route_quiescence_unproven",
        "rollback_unproven",
    } <= set(failures)


def test_cli_writes_artifact_and_rejects_sensitive_input(tmp_path: Path) -> None:
    celery = tmp_path / "celery.json"
    go = tmp_path / "go.json"
    output = tmp_path / "evidence.json"
    celery.write_text(json.dumps(observation("celery")))
    go.write_text(json.dumps(observation("go")))
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
                str(ROOT / "contracts/jobs/v1/registry.json"),
                "--migration-state",
                str(ROOT / "contracts/jobs/v1/migration-state.json"),
            ]
        )
        == 0
    )
    assert json.loads(output.read_text())["result"]["status"] == "pass"
    rendered = output.read_text()
    assert "hashed-shared-dataset" not in rendered
    assert "hashed-bounded-run" not in rendered
    bad = observation("go")
    bad["payload"] = "forbidden"
    go.write_text(json.dumps(bad))
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


def test_malformed_scalars_identifiers_and_nonfinite_values_fail_before_output(
    tmp_path: Path,
) -> None:
    celery = tmp_path / "celery.json"
    go = tmp_path / "go.json"
    output = tmp_path / "evidence.json"
    celery.write_text(json.dumps(observation("celery")))
    cases = [
        ("dataset_scope", "tenant-123"),
        ("checks.lag_seconds", float("nan")),
        ("checks.cpu_cores", float("inf")),
        ("route.generation", True),
        ("build.revision", 42),
        ("rollback.transport", "river"),
    ]
    for path, value in cases:
        bad = observation("go")
        target: dict = bad
        parts = path.split(".")
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = value
        go.write_text(json.dumps(bad))
        output.unlink(missing_ok=True)
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
        assert not output.exists()
