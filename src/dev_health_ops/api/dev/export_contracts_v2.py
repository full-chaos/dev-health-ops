"""Generate or verify the checked-in Ask Dev v2 schema and fixture artifacts.

Mirrors ``dev_health_ops.api.dev.export_contracts`` (v1), writing to
``contracts/ask-dev/v2`` instead of ``contracts/ask-dev/v1`` — the same
producer layout ``dev-health-web``'s ``scripts/ask-dev-contracts.mjs``
already consumes for v1, so a future web regen (CHAOS-3298, out of scope
here) only has to repoint its ``SOURCE_PREFIX``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from .contract_fixtures_v2 import (
    negative_fixtures,
    positive_fixtures,
    positive_variant_fixtures,
    stream_fixtures,
)
from .contracts_v2 import CONTRACT_MODELS_V2, DevStreamEventV2, validate_stream_v2

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
ARTIFACT_ROOT = REPOSITORY_ROOT / "contracts" / "ask-dev" / "v2"


def _json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def _sha256(contents: str) -> str:
    return hashlib.sha256(contents.encode("utf-8")).hexdigest()


def _schema(name: str) -> dict[str, Any]:
    schema = CONTRACT_MODELS_V2[name].model_json_schema(mode="validation")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"https://api.fullchaos.dev/contracts/ask-dev/v2/{name}.schema.json",
        **schema,
    }


def _validate_positive_variants() -> None:
    """CHAOS-3338: mirrors ``export_contracts._validate_positive_variants``."""

    variants = positive_variant_fixtures()
    unregistered = sorted(set(variants) - set(CONTRACT_MODELS_V2))
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
            CONTRACT_MODELS_V2[name].model_validate(payload)


def _validate_fixtures() -> None:
    positives = positive_fixtures()
    negatives = negative_fixtures()
    if set(positives) != set(CONTRACT_MODELS_V2):
        raise RuntimeError("positive fixture coverage does not match contract registry")
    if set(negatives) != set(CONTRACT_MODELS_V2):
        raise RuntimeError("negative fixture coverage does not match contract registry")
    for name, payload in positives.items():
        CONTRACT_MODELS_V2[name].model_validate(payload)
    _validate_positive_variants()
    for name, cases in negatives.items():
        if not cases:
            raise RuntimeError(f"{name} has no negative fixture")
        for label, payload in cases:
            try:
                CONTRACT_MODELS_V2[name].model_validate(payload)
            except ValidationError:
                continue
            raise RuntimeError(f"negative fixture unexpectedly passed: {name}/{label}")
    streams = stream_fixtures()
    # CHAOS-3297 stack #4: "valid_with_narrative_fallback" is the same
    # lifecycle plus the optional answer.narrative_fallback marker -- also
    # expected to validate cleanly, not a negative fixture. See
    # test_contracts_v2._VALID_STREAM_FIXTURE_NAMES for the same exemption.
    valid_stream_labels = {"valid", "valid_with_narrative_fallback"}
    for label in valid_stream_labels:
        validate_stream_v2(
            [DevStreamEventV2.model_validate(item) for item in streams[label]]
        )
    for label, payloads in streams.items():
        if label in valid_stream_labels:
            continue
        try:
            validate_stream_v2(
                [DevStreamEventV2.model_validate(item) for item in payloads]
            )
        except (ValidationError, ValueError):
            continue
        raise RuntimeError(f"negative stream fixture unexpectedly passed: {label}")


def expected_artifacts() -> dict[str, str]:
    _validate_fixtures()
    artifacts: dict[str, str] = {}
    manifest_entries: list[dict[str, Any]] = []
    positives = positive_fixtures()
    negatives = negative_fixtures()
    variants = positive_variant_fixtures()
    for name in CONTRACT_MODELS_V2:
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
    for label, stream_payloads in stream_fixtures().items():
        path = f"examples/streams/{label}.json"
        artifacts[path] = _json(stream_payloads)
    manifest = {
        "schema_version": "ask_dev_contract_manifest.v2",
        "compatibility": "additive-v2-alongside-v1",
        "contracts": manifest_entries,
        "stream_sequences": [
            {"case": label, "path": f"examples/streams/{label}.json"}
            for label in stream_fixtures()
        ],
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
        print(f"wrote {len(artifacts)} Ask Dev v2 contract artifacts")
    else:
        check_artifacts(artifacts)
        print(f"verified {len(artifacts)} Ask Dev v2 contract artifacts")


if __name__ == "__main__":
    main()
