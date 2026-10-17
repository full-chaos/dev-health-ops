"""Golden-corpus tests for the ``endpoint-auth-profile.v1`` wire contract.

The Go runner, this file and the ajv check read the SAME manifest over the
SAME fixture files, so the inventory lives only in
``contracts/auth/v1/examples/endpoint-auth-profile/manifest.json``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from referencing.exceptions import Unresolvable

from dev_health_ops.authclient.contracts import (
    contracts_dir,
    validate,
    violations,
)

SURFACE = "endpoint-auth-profile.v1"
FIXTURES = contracts_dir() / "examples" / "endpoint-auth-profile"
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text(encoding="utf-8"))
ACCEPT = [e["file"] for e in MANIFEST["accept"]]
REJECT = MANIFEST["reject"]


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_the_manifest_is_not_empty() -> None:
    assert ACCEPT, "manifest declares no accept fixtures"
    assert REJECT, "manifest declares no reject fixtures"


def test_every_fixture_file_on_disk_is_claimed_by_the_manifest() -> None:
    listed = set(ACCEPT) | {e["file"] for e in REJECT}
    on_disk = {p.name for p in FIXTURES.glob("*.json") if p.name != "manifest.json"}
    assert on_disk == listed, (
        f"unclaimed on disk: {sorted(on_disk - listed)}; "
        f"claimed but absent: {sorted(listed - on_disk)}"
    )


@pytest.mark.parametrize("name", ACCEPT)
def test_accept_fixtures_validate(name: str) -> None:
    validate(SURFACE, _load(name))


@pytest.mark.parametrize("entry", REJECT, ids=lambda e: e["file"])
def test_reject_fixtures_fail_at_the_declared_location(entry: dict[str, Any]) -> None:
    found = violations(SURFACE, _load(entry["file"]))
    assert found, f"{entry['file']} was expected to fail validation but passed"
    assert {v.instance_location for v in found} == {
        entry["expect_instance_location"]
    }, f"{entry['file']}: {[str(v) for v in found]}"
    assert entry["expect_keyword"] in {v.keyword for v in found}, (
        f"{entry['file']} should trip {entry['expect_keyword']!r}, "
        f"got {sorted({v.keyword for v in found})}"
    )


def test_a_reference_outside_the_contracts_registry_is_an_error(tmp_path: Path) -> None:
    """A cross-schema $ref resolves against files in this tree only.

    No retrieval hook is installed, so an unknown $id must raise rather than
    silently skip the constraint or reach the network.
    """
    schemas = tmp_path / "contracts" / "auth" / "v1" / "jsonschema"
    schemas.mkdir(parents=True)
    (schemas / "x.v1.schema.json").write_text(
        json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "https://fullchaos.dev/contracts/auth/v1/jsonschema/x.v1.schema.json",
                "$ref": "https://example.com/elsewhere.schema.json",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(Unresolvable):
        violations("x.v1", {}, root=tmp_path)


def test_the_class_vocabulary_is_referenced_not_copied() -> None:
    schema = json.loads(
        (contracts_dir() / "jsonschema" / f"{SURFACE}.schema.json").read_text(
            encoding="utf-8"
        )
    )
    items = schema["properties"]["accepted_credential_classes"]["items"]
    assert items["$ref"].startswith("principal.v1.schema.json#")
    assert set(items) <= {"$ref", "description"}, (
        "a sibling keyword restates the vocabulary"
    )


def test_python_accepts_exactly_the_frozen_class_set_both_ways() -> None:
    frozen = {
        c["class_id"]
        for c in json.loads(
            (contracts_dir() / "credential-classes.json").read_text(encoding="utf-8")
        )["classes"]
    }
    wave_one = {"user_access_token", "refresh_credential", "workload_token"}
    principal = json.loads(
        (contracts_dir() / "jsonschema" / "principal.v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    enum = set(principal["properties"]["credential"]["properties"]["class"]["enum"])
    candidates = (
        frozen | wave_one | enum | {"bearer_any", "jwt", "", "USER_ACCESS_TOKEN"}
    )
    base = _load("valid-protected-opaque-class-no-token.json")
    accepted = {
        c
        for c in candidates
        if not violations(SURFACE, {**base, "accepted_credential_classes": [c]})
    }
    assert accepted == frozen | wave_one
