"""Golden-corpus tests for ``provisioning-event.v1`` and ``audit-event.v1``.

The Go runner (``provisioning_event_goldens_test.go``), the ajv runner and this
file read the SAME manifests over the SAME fixtures (G-70).
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from dev_health_ops.authclient.contracts import (
    ContractError,
    contracts_dir,
    validate,
    validator_for,
    violations,
)
from dev_health_ops.authclient.provisioning_event import (
    ConsumerCursor,
    ProvisioningEvent,
    parse_audit_event,
    parse_provisioning_event,
)

EXAMPLES = contracts_dir() / "examples"
SURFACES = {
    "provisioning-event.v1": ("provisioning-event", parse_provisioning_event),
    "audit-event.v1": ("audit-event", parse_audit_event),
}


def _manifest(directory: str) -> dict[str, Any]:
    return json.loads((EXAMPLES / directory / "manifest.json").read_text("utf-8"))


def _cases(group: str) -> list[Any]:
    out = []
    for surface, (directory, parse) in SURFACES.items():
        for entry in _manifest(directory)[group]:
            out.append(
                pytest.param(
                    surface, directory, parse, entry, id=f"{directory}/{entry['file']}"
                )
            )
    return out


def _raw(directory: str, name: str) -> bytes:
    return (EXAMPLES / directory / name).read_bytes()


@pytest.mark.parametrize("directory", [d for d, _ in SURFACES.values()])
def test_every_fixture_on_disk_is_claimed_and_no_group_is_empty(directory: str) -> None:
    manifest = _manifest(directory)
    assert manifest["accept"] and manifest["reject"] and manifest["reject_by_client"]
    listed = {
        e["file"] for g in ("accept", "reject", "reject_by_client") for e in manifest[g]
    }
    on_disk = {
        p.name
        for p in (EXAMPLES / directory).glob("*.json")
        if p.name != "manifest.json"
    }
    assert on_disk == listed, (
        f"unclaimed: {sorted(on_disk - listed)}; absent: {sorted(listed - on_disk)}"
    )


@pytest.mark.parametrize(("surface", "directory", "parse", "entry"), _cases("accept"))
def test_accept_fixtures_validate_and_parse(
    surface: str, directory: str, parse: Any, entry: dict[str, Any]
) -> None:
    validate(surface, json.loads(_raw(directory, entry["file"])))
    parse(_raw(directory, entry["file"]))


@pytest.mark.parametrize(("surface", "directory", "parse", "entry"), _cases("reject"))
def test_reject_fixtures_fail_at_the_declared_location(
    surface: str, directory: str, parse: Any, entry: dict[str, Any]
) -> None:
    document = json.loads(_raw(directory, entry["file"]))
    found = violations(surface, document)
    assert found, f"{entry['file']} was expected to fail"
    assert {v.instance_location for v in found} == {entry["expect_instance_location"]}
    assert entry["expect_keyword"] in {v.keyword for v in found}
    with pytest.raises(ContractError):
        parse(_raw(directory, entry["file"]))


@pytest.mark.parametrize(
    ("surface", "directory", "parse", "entry"), _cases("reject_by_client")
)
def test_client_enforced_fixtures_are_valid_but_the_client_refuses_them(
    surface: str, directory: str, parse: Any, entry: dict[str, Any]
) -> None:
    validate(surface, json.loads(_raw(directory, entry["file"])))
    with pytest.raises(ContractError, match="decimal or exponent form|does not equal"):
        parse(_raw(directory, entry["file"]))


def test_every_aggregate_type_has_an_identity_and_vocabulary_branch() -> None:
    schema = validator_for("provisioning-event.v1").schema
    assert isinstance(schema, dict)
    aggregates = schema["properties"]["aggregate_type"]["enum"]
    assert aggregates
    branches = {
        b["if"]["properties"]["aggregate_type"]["const"]: b["then"]["properties"]
        for b in schema["allOf"]
    }
    for aggregate in aggregates:
        assert aggregate in branches, f"{aggregate} has no conditional branch"
        assert {"aggregate_id", "event_type", "payload"} <= set(branches[aggregate])
        assert "change" in branches[aggregate]["payload"]["properties"]


def test_duplicate_members_and_trailing_content_are_refused_in_every_parser() -> None:
    valid = _raw("provisioning-event", "valid-minimal.json").decode()
    valid_audit = _raw("audit-event", "valid-minimal.json").decode()
    with pytest.raises(ContractError, match="duplicate object member"):
        parse_provisioning_event(
            valid.replace('"revision": 1,', '"revision": 2, "revision": 1,', 1)
        )
    with pytest.raises(ContractError, match="duplicate object member"):
        parse_audit_event(
            valid_audit.replace(
                '"attributes": {}', '"attributes": {}, "attributes": {"note": "x"}', 1
            )
        )
    with pytest.raises(ContractError, match="not valid JSON"):
        parse_provisioning_event(valid + "\n{}")
    with pytest.raises(ContractError, match="not valid JSON"):
        parse_audit_event(valid_audit + "x")
    with pytest.raises(ContractError, match="not valid JSON"):
        parse_provisioning_event(b'{"aggregate_type": "\xff"}')


def test_consumer_vectors_replay_to_the_declared_dispositions() -> None:
    vectors = _manifest("provisioning-event")["consumer_vectors"]
    assert vectors["cases"]
    for case in vectors["cases"]:
        cursor = ConsumerCursor()
        for index, (aggregate_type, aggregate_id, revision, expected) in enumerate(
            case["steps"]
        ):
            got = cursor.observe(aggregate_type, aggregate_id, revision)
            assert got == expected, f"{case['name']} step {index}: {got} != {expected}"
    for example in vectors["dedupe_key_examples"]:
        event = ProvisioningEvent(
            aggregate_type=example["aggregate_type"],
            aggregate_id=example["aggregate_id"],
            revision=example["revision"],
            event_type="x",
            idempotency_key="x",
            created_at="x",
        )
        assert event.dedupe_key() == example["key"]
