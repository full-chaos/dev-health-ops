"""CHAOS-3615: what the generated JSON Schema actually catches, measured.

Adversarial review round 1, finding H1. The contract has two
implementations of the same rules — the canonical Pydantic models, and the
Draft 2020-12 schemas generated from them — and no type checker, linter or
code index can tell you whether they agree. Only running both over the same
inputs can, so that is what this module does: every negative fixture goes
through a real JSON Schema validator *and* the Python model, and the split
is pinned.

The answer is uncomfortable and is the point of writing it down. Pydantic
emits structural constraints only (``required``/``type``/``enum``/
``pattern``/``minItems``), so **3 of 41** arm-shaped bad packets are caught
by the schema and **38 are not**. A consumer that schema-validates and stops
would accept a packet that commits to the wrong subject on a fuzzy match,
routes a path through an unauthorized entity, promotes a symptom to
principal driver, or claims a historical comparison it never reconstructed.

Two things follow, and both are enforced here rather than trusted:

* the canonical validator is the Python model, and the manifest says so in
  the artifact tree a consumer reads;
* the exact set of schema-catchable fixtures is pinned, so a fixture
  quietly changing category — or a schema regeneration that silently drops
  a structural constraint — is a red test, not a discovery made later by
  CHAOS-3616.

Encoding the cross-field rules into JSON Schema by hand was rejected: the
corrective plan requires one canonical source of truth and forbids
hand-maintained duplicate schema definitions, and a second hand-written
implementation of these validators would be exactly the divergence this
module exists to detect.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.investigation_contract import (
    INVESTIGATION_CONTRACT_MODELS,
)
from dev_health_ops.api.dev.investigation_contract.export import ARTIFACT_ROOT
from dev_health_ops.api.dev.investigation_contract.fixtures import (
    negative_fixtures,
    positive_fixtures,
    positive_variant_fixtures,
)

#: The only negative fixtures the emitted JSON Schema is able to reject, and
#: the structural keyword that does it. Everything else in
#: ``examples/negative`` is semantic and reaches the wire unchallenged
#: without the canonical validator.
SCHEMA_CATCHABLE: dict[str, str] = {
    "ask_dev_subject_discovery.v1::absent_truncation_disclosure_field": "required",
    "ask_dev_investigation_versions.v1::no_source_contract_versions": "minItems",
    "ask_dev_evidence_coverage.v1::graph_native_field_smuggled_as_extra": (
        "additionalProperties"
    ),
}


def _schema(contract: str) -> dict[str, Any]:
    path = ARTIFACT_ROOT / "schemas" / f"{contract}.schema.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _schema_rejects(contract: str, payload: dict[str, Any]) -> bool:
    try:
        jsonschema.validate(payload, _schema(contract))
    except jsonschema.ValidationError:
        return True
    return False


def _model_rejects(contract: str, payload: dict[str, Any]) -> bool:
    try:
        INVESTIGATION_CONTRACT_MODELS[contract].model_validate(payload)
    except ValidationError:
        return True
    return False


def _all_negative_cases() -> list[tuple[str, str, dict[str, Any]]]:
    return [
        (contract, label, payload)
        for contract, cases in sorted(negative_fixtures().items())
        for label, payload in cases
    ]


def test_every_negative_fixture_is_caught_by_the_canonical_validator() -> None:
    """The floor. A fixture caught by neither would be a hole in both."""

    escaped = [
        f"{contract}::{label}"
        for contract, label, payload in _all_negative_cases()
        if not _model_rejects(contract, payload)
    ]
    assert not escaped, f"negative fixtures no validator rejects: {escaped}"


def test_the_schema_catchable_set_is_exactly_as_pinned() -> None:
    """Measured by execution, not asserted from the shape of the schema.

    A fixture moving in or out of this set means either the schema gained or
    lost a structural constraint, or a fixture stopped exercising the
    semantic rule it was written for. Both are worth a red test.
    """

    measured = {
        f"{contract}::{label}"
        for contract, label, payload in _all_negative_cases()
        if _schema_rejects(contract, payload)
    }
    pinned = set(SCHEMA_CATCHABLE)
    assert measured == pinned, (
        "the JSON Schema's real coverage has drifted from the pinned set; "
        f"newly caught={sorted(measured - pinned)}, "
        f"no longer caught={sorted(pinned - measured)}"
    )


def test_the_schema_misses_the_overwhelming_majority_of_semantic_faults() -> None:
    """The claim the manifest makes, restated as an assertion.

    Not decoration: if a future change made the schema catch most faults,
    the manifest's ``schema_only_validation_is_sufficient: false`` would be
    needlessly alarming, and if it caught fewer, the gap would be wider than
    documented. Either way the artifact tree's own statement should be
    re-derived rather than trusted.
    """

    total = len(_all_negative_cases())
    caught = len(SCHEMA_CATCHABLE)
    assert total >= 20, f"only {total} negative fixtures; this ratio is not meaningful"
    assert caught * 4 < total, (
        f"the schema now catches {caught}/{total} negatives; if schema-only "
        "validation has become close to sufficient, revisit the manifest's "
        "validation_policy rather than leaving it overstated"
    )


@pytest.mark.parametrize(
    ("contract", "label"),
    sorted((key.split("::")[0], key.split("::")[1]) for key in SCHEMA_CATCHABLE),
)
def test_structurally_catchable_fixtures_are_caught_by_both(
    contract: str, label: str
) -> None:
    """Where the two implementations agree, they must agree completely."""

    payload = next(
        fixture
        for fixture_contract, fixture_label, fixture in _all_negative_cases()
        if (fixture_contract, fixture_label) == (contract, label)
        for _ in (0,)
        for fixture in (fixture,)
    )
    assert _schema_rejects(contract, payload)
    assert _model_rejects(contract, payload)


@pytest.mark.parametrize("contract", sorted(INVESTIGATION_CONTRACT_MODELS))
def test_positive_goldens_validate_against_the_generated_schema(
    contract: str,
) -> None:
    """The agreement direction that must be total.

    The two implementations may disagree about what to *reject* — that is
    the measured gap above — but a packet the canonical validator accepts
    must never fail its own generated schema. That would mean the schema
    describes something the producer cannot emit, which is a real drift bug
    rather than a known limitation.
    """

    jsonschema.validate(positive_fixtures()[contract], _schema(contract))


def test_positive_variants_validate_against_the_generated_schema() -> None:
    for contract, cases in positive_variant_fixtures().items():
        for label, payload in cases:
            try:
                jsonschema.validate(payload, _schema(contract))
            except jsonschema.ValidationError as error:  # pragma: no cover - failure
                raise AssertionError(
                    f"positive variant {contract}/{label} fails its own "
                    f"generated schema: {error.message}"
                ) from error


def test_manifest_declares_the_schema_is_not_sufficient_on_its_own() -> None:
    manifest = json.loads((ARTIFACT_ROOT / "manifest.json").read_text(encoding="utf-8"))
    policy = manifest["validation_policy"]
    assert policy["schema_only_validation_is_sufficient"] is False
    assert policy["json_schema_scope"] == "structural_only"
    assert "INVESTIGATION_CONTRACT_MODELS" in policy["canonical_validator"]


def test_schemas_carry_no_hand_written_cross_field_rules() -> None:
    """No duplicate semantic implementation crept into the emitted schemas.

    The corrective plan forbids hand-maintained duplicate schema
    definitions. ``anyOf`` is permitted because Pydantic emits it for
    nullable unions; the composition keywords that would encode a *rule*
    are not.
    """

    banned = {"allOf", "oneOf", "if", "then", "else", "dependentRequired"}
    offenders: list[str] = []

    def walk(node: object, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in banned:
                    offenders.append(f"{path}/{key}")
                walk(value, f"{path}/{key}")
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, f"{path}[{index}]")

    for path in sorted((ARTIFACT_ROOT / "schemas").glob("*.json")):
        walk(json.loads(path.read_text(encoding="utf-8")), path.name)
    assert not offenders, (
        "generated schemas contain composition keywords, which means a "
        f"second implementation of the rules has appeared: {offenders}"
    )


def test_artifact_root_exists_and_is_populated() -> None:
    assert (ARTIFACT_ROOT / "schemas").is_dir()
    assert len(list((ARTIFACT_ROOT / "schemas").glob("*.json"))) == len(
        INVESTIGATION_CONTRACT_MODELS
    )


def test_no_negative_fixture_directory_drift() -> None:
    """Every negative fixture on disk corresponds to a declared case."""

    on_disk = {
        path.stem for path in (ARTIFACT_ROOT / "examples" / "negative").glob("*.json")
    }
    declared = {f"{contract}.{label}" for contract, label, _ in _all_negative_cases()}
    assert on_disk == declared, (
        f"stale={sorted(on_disk - declared)}, missing={sorted(declared - on_disk)}"
    )


def test_repository_root_resolves(tmp_path: Path) -> None:
    """Guards the path arithmetic this whole module depends on."""

    assert ARTIFACT_ROOT.name == "v1"
    assert ARTIFACT_ROOT.parent.name == "ask-dev-investigation"
    assert tmp_path.exists()
