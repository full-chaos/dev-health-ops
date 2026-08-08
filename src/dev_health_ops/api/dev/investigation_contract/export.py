"""Generate or verify the checked-in investigation-contract artifacts.

CHAOS-3615 deliverables 2, 5 and 6.

The third instance of the repository's established exporter pattern (after
``export_contracts`` for ``contracts/ask-dev/v1`` and ``export_contracts_v2``
for ``contracts/ask-dev/v2``), writing to its own root,
``contracts/ask-dev-investigation/v1``.

**Why its own root rather than a new entry in ``CONTRACT_MODELS_V2``.**
``contracts/ask-dev/v2`` is reserved for wire contracts served to real
clients — ``scripts/acceptance/corpus/receipt.py:6-7`` says so explicitly,
and ``dev-health-web``'s contract sync reads that tree. The investigation
packet is an internal trial artifact that is never client-served, and
filing it under the client tree would both misrepresent it and put a
CHAOS-3616 iteration on the critical path of a web contract regen.

**What is exported.** Draft 2020-12 JSON Schema per contract; the positive
golden; the anti-drift positive variants; every arm-shaped negative fixture;
the four registries (question families, scoring dimensions, fault modes,
source/relationship allowlists and slice boundaries) as machine-readable
JSON so CHAOS-3616 can consume them without importing Python; and a manifest
with a sha256 for every file.

``check`` mode is the drift gate: it compares the *full* artifact set, so a
stale file, a missing file and an unexpected extra file are all failures.
``_validate_fixtures`` runs first in both modes, and hard-fails unless every
registered contract has a positive fixture and at least one negative fixture
that genuinely fails validation — a negative fixture that quietly started
passing is the exact shape of a guard that has stopped guarding.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from .allowlists import (
    SLICE_BOUNDARIES,
    TRIAL_SOURCE_ALLOWLIST,
    TRIAL_SOURCE_RATIONALE,
)
from .fixtures import (
    negative_fixtures,
    positive_fixtures,
    positive_variant_fixtures,
)
from .packet import INVESTIGATION_CONTRACT_MODELS
from .question_families import (
    ALL_QUESTION_FAMILY_IDS,
    MANDATORY_PROHIBITED_REDUCTIONS,
    QUESTION_FAMILY_REGISTRY,
)
from .relationships import ALL_RELATIONSHIP_TYPES, RELATIONSHIP_ALLOWLIST
from .scoring import (
    ALL_FAULT_MODE_IDS,
    ALL_SCORING_DIMENSION_IDS,
    FAULT_MODE_REGISTRY,
    SCORING_DIMENSION_REGISTRY,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[5]
ARTIFACT_ROOT = REPOSITORY_ROOT / "contracts" / "ask-dev-investigation" / "v1"

SCHEMA_ID_PREFIX = "https://api.fullchaos.dev/contracts/ask-dev-investigation/v1"

__all__ = [
    "ARTIFACT_ROOT",
    "check_artifacts",
    "expected_artifacts",
    "main",
    "write_artifacts",
]


def _json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def _sha256(contents: str) -> str:
    return hashlib.sha256(contents.encode("utf-8")).hexdigest()


def _schema(name: str) -> dict[str, Any]:
    schema = INVESTIGATION_CONTRACT_MODELS[name].model_json_schema(mode="validation")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"{SCHEMA_ID_PREFIX}/{name}.schema.json",
        **schema,
    }


def _validate_positive_variants() -> None:
    variants = positive_variant_fixtures()
    unregistered = sorted(set(variants) - set(INVESTIGATION_CONTRACT_MODELS))
    if unregistered:
        raise RuntimeError(
            f"positive variants name unregistered contracts: {unregistered}"
        )
    for name, cases in variants.items():
        if not cases:
            raise RuntimeError(f"{name} declares an empty positive variant list")
        labels = [label for label, _ in cases]
        if len(labels) != len(set(labels)):
            raise RuntimeError(f"{name} has duplicate positive variant labels")
        for label, payload in cases:
            if not label:
                raise RuntimeError(f"{name} has an unlabelled positive variant")
            INVESTIGATION_CONTRACT_MODELS[name].model_validate(payload)


def _validate_fixtures() -> None:
    positives = positive_fixtures()
    negatives = negative_fixtures()
    if set(positives) != set(INVESTIGATION_CONTRACT_MODELS):
        raise RuntimeError("positive fixture coverage does not match contract registry")
    if set(negatives) != set(INVESTIGATION_CONTRACT_MODELS):
        raise RuntimeError("negative fixture coverage does not match contract registry")
    for name, payload in positives.items():
        INVESTIGATION_CONTRACT_MODELS[name].model_validate(payload)
    _validate_positive_variants()
    for name, cases in negatives.items():
        if not cases:
            raise RuntimeError(f"{name} has no negative fixture")
        labels = [label for label, _ in cases]
        if len(labels) != len(set(labels)):
            raise RuntimeError(f"{name} has duplicate negative fixture labels")
        for label, payload in cases:
            try:
                INVESTIGATION_CONTRACT_MODELS[name].model_validate(payload)
            except ValidationError:
                continue
            raise RuntimeError(
                f"negative fixture unexpectedly passed: {name}/{label}; a "
                "negative fixture that validates is a guard that has stopped "
                "guarding"
            )


def _registry_artifacts() -> dict[str, Any]:
    """The four registries, as JSON a non-Python consumer can read."""

    question_families = {
        "schema_version": "ask_dev_question_family_registry.v1",
        "mandatory_prohibited_reductions": [
            str(item) for item in MANDATORY_PROHIBITED_REDUCTIONS
        ],
        "families": [
            QUESTION_FAMILY_REGISTRY[family_id].model_dump(mode="json")
            for family_id in ALL_QUESTION_FAMILY_IDS
        ],
    }
    scoring = {
        "schema_version": "ask_dev_scoring_registry.v1",
        "aggregate_score_prohibited": True,
        "reporting_shape": "per_question_family_x_per_dimension",
        "dimensions": [
            SCORING_DIMENSION_REGISTRY[dimension_id].model_dump(mode="json")
            for dimension_id in ALL_SCORING_DIMENSION_IDS
        ],
    }
    fault_modes = {
        "schema_version": "ask_dev_fault_mode_registry.v1",
        "fault_modes": [
            FAULT_MODE_REGISTRY[fault_id].model_dump(mode="json")
            for fault_id in ALL_FAULT_MODE_IDS
        ],
    }
    allowlists = {
        "schema_version": "ask_dev_trial_allowlists.v1",
        "source_classes": [
            {"source_class": str(item), "rationale": TRIAL_SOURCE_RATIONALE[item]}
            for item in TRIAL_SOURCE_ALLOWLIST
        ],
        "relationship_types": [
            RELATIONSHIP_ALLOWLIST[relationship].model_dump(mode="json")
            for relationship in ALL_RELATIONSHIP_TYPES
        ],
        "slice_boundaries": [
            boundary.model_dump(mode="json") for boundary in SLICE_BOUNDARIES.values()
        ],
    }
    return {
        "registries/question_families.json": question_families,
        "registries/scoring_dimensions.json": scoring,
        "registries/fault_modes.json": fault_modes,
        "registries/trial_allowlists.json": allowlists,
    }


def expected_artifacts() -> dict[str, str]:
    _validate_fixtures()
    artifacts: dict[str, str] = {}
    manifest_entries: list[dict[str, Any]] = []
    positives = positive_fixtures()
    negatives = negative_fixtures()
    variants = positive_variant_fixtures()
    for name in INVESTIGATION_CONTRACT_MODELS:
        schema_path = f"schemas/{name}.schema.json"
        positive_path = f"examples/positive/{name}.json"
        schema_contents = _json(_schema(name))
        positive_contents = _json(positives[name])
        artifacts[schema_path] = schema_contents
        artifacts[positive_path] = positive_contents
        variant_entries = []
        for label, payload in variants.get(name, []):
            variant_path = f"examples/positive/{name}.{label}.json"
            if variant_path in artifacts:
                raise RuntimeError(f"positive variant path collides: {variant_path}")
            variant_contents = _json(payload)
            artifacts[variant_path] = variant_contents
            variant_entries.append(
                {
                    "case": label,
                    "path": variant_path,
                    "sha256": _sha256(variant_contents),
                }
            )
        negative_entries = []
        for label, payload in negatives[name]:
            negative_path = f"examples/negative/{name}.{label}.json"
            negative_contents = _json(payload)
            artifacts[negative_path] = negative_contents
            negative_entries.append(
                {
                    "case": label,
                    "path": negative_path,
                    "sha256": _sha256(negative_contents),
                }
            )
        manifest_entries.append(
            {
                "schema_version": name,
                "schema": {"path": schema_path, "sha256": _sha256(schema_contents)},
                "positive": {
                    "path": positive_path,
                    "sha256": _sha256(positive_contents),
                },
                "positive_variants": variant_entries,
                "negative": negative_entries,
            }
        )
    registry_entries = []
    for path, payload in _registry_artifacts().items():
        contents = _json(payload)
        artifacts[path] = contents
        registry_entries.append({"path": path, "sha256": _sha256(contents)})
    manifest = {
        "schema_version": "ask_dev_investigation_contract_manifest.v1",
        "compatibility": "internal-trial-artifact-not-client-served",
        # Adversarial review round 1, finding H1. Pydantic emits structural
        # JSON Schema only: required/type/enum/pattern/length. None of this
        # contract's cross-field rules -- commitment evidence, authorization
        # scope, symptom-vs-driver standing, evidence closure, family
        # obligations -- survive into the emitted schema, so a consumer that
        # schema-validates and stops accepts almost every arm-shaped bad
        # packet in examples/negative. Saying so here, in the artifact a
        # consumer actually reads, rather than leaving it to be discovered.
        #
        # The split is measured by execution, not asserted: see
        # tests/api/dev/test_chaos_3615_schema_validator_differential.py,
        # which runs every negative fixture through a real JSON Schema
        # validator AND the Python model and pins exactly which fixtures each
        # one catches.
        "validation_policy": {
            "canonical_validator": (
                "dev_health_ops.api.dev.investigation_contract.packet"
                ".INVESTIGATION_CONTRACT_MODELS"
            ),
            "json_schema_scope": "structural_only",
            "schema_only_validation_is_sufficient": False,
            "note": (
                "The generated JSON Schemas describe field shape, not the "
                "contract's semantics. A packet that validates against the "
                "schema alone has not been checked for authorization scope, "
                "evidence closure, driver standing, family obligations, "
                "historical comparability or any other cross-field rule. Any "
                "consumer -- including a future non-Python arm -- must run "
                "the canonical validator or reimplement these rules and prove "
                "equivalence against examples/negative."
            ),
        },
        "contracts": manifest_entries,
        "registries": registry_entries,
    }
    artifacts["manifest.json"] = _json(manifest)
    return artifacts


def _current_artifact_paths() -> set[str]:
    if not ARTIFACT_ROOT.exists():
        return set()
    return {
        str(path.relative_to(ARTIFACT_ROOT))
        for path in ARTIFACT_ROOT.rglob("*")
        if path.is_file()
    }


def write_artifacts(artifacts: dict[str, str]) -> None:
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    for relative_path, contents in artifacts.items():
        destination = ARTIFACT_ROOT / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(contents, encoding="utf-8")
    for stale in _current_artifact_paths() - set(artifacts):
        (ARTIFACT_ROOT / stale).unlink()


def check_artifacts(artifacts: dict[str, str]) -> None:
    actual_paths = _current_artifact_paths()
    expected_paths = set(artifacts)
    if actual_paths != expected_paths:
        missing = sorted(expected_paths - actual_paths)
        stale = sorted(actual_paths - expected_paths)
        raise RuntimeError(
            f"contract artifact set drifted; missing={missing}, stale={stale}"
        )
    drifted = [
        relative_path
        for relative_path, expected in artifacts.items()
        if (ARTIFACT_ROOT / relative_path).read_text(encoding="utf-8") != expected
    ]
    if drifted:
        raise RuntimeError(f"contract artifacts drifted: {sorted(drifted)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("write", "check"))
    args = parser.parse_args()
    artifacts = expected_artifacts()
    if args.mode == "write":
        write_artifacts(artifacts)
        print(f"wrote {len(artifacts)} Ask Dev investigation contract artifacts")
    else:
        check_artifacts(artifacts)
        print(f"verified {len(artifacts)} Ask Dev investigation contract artifacts")


if __name__ == "__main__":
    main()
