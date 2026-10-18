"""Golden-corpus tests for the authorization wire surfaces.

The Go runner, this file and the ajv check read the SAME manifest over the SAME
fixture files; the inventory lives only in
``contracts/auth/v1/examples/authorization/manifest.json``.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.authclient.authorization import (
    BATCH_REQUEST_SURFACE,
    BATCH_RESPONSE_SURFACE,
    DECISION_SURFACE,
    REQUEST_SURFACE,
    parse_batch_response_bytes,
    parse_decision_bytes,
)
from dev_health_ops.authclient.contracts import (
    ContractError,
    contracts_dir,
    validate,
    violations,
)

SURFACES = [
    REQUEST_SURFACE,
    DECISION_SURFACE,
    BATCH_REQUEST_SURFACE,
    BATCH_RESPONSE_SURFACE,
]


def _dir(surface: str) -> Path:
    # examples/<schema stem without .v1>/, the layout the strict ajv runner pairs
    # with <stem>.v1.schema.json.
    return contracts_dir() / "examples" / surface.removesuffix(".v1")


MANIFESTS = {
    surface: json.loads((_dir(surface) / "manifest.json").read_text(encoding="utf-8"))
    for surface in SURFACES
}


def _entries(group: str) -> list[dict[str, Any]]:
    return [
        {**entry, "surface": surface}
        for surface, manifest in MANIFESTS.items()
        for entry in manifest.get(group, [])
    ]


ACCEPT = _entries("accept")
REJECT = _entries("reject")
CLIENT = _entries("reject_by_client")
BATCH_REQUEST = json.dumps(MANIFESTS[BATCH_RESPONSE_SURFACE]["request"]).encode()


#: 30 seconds before the fixtures' decision expiry: expiry checks do not depend on
#: the date the suite runs.
NOW = datetime(2026, 9, 3, 5, 0, 0, tzinfo=timezone.utc)
EXPIRY = NOW + timedelta(seconds=30)


def _load(surface: str, name: str) -> Any:
    return json.loads((_dir(surface) / name).read_text(encoding="utf-8"))


def _bytes(surface: str, name: str) -> bytes:
    return (_dir(surface) / name).read_bytes()


def test_the_manifest_is_not_empty_and_covers_every_surface() -> None:
    assert ACCEPT and REJECT and CLIENT
    assert {e["surface"] for e in ACCEPT} == {
        REQUEST_SURFACE,
        DECISION_SURFACE,
        BATCH_REQUEST_SURFACE,
        BATCH_RESPONSE_SURFACE,
    }


def test_every_fixture_file_on_disk_is_claimed_by_the_manifest() -> None:
    for surface in SURFACES:
        listed = {
            e["file"]
            for group in ("accept", "reject", "reject_by_client")
            for e in MANIFESTS[surface].get(group, [])
        }
        on_disk = {
            p.name for p in _dir(surface).glob("*.json") if p.name != "manifest.json"
        }
        assert on_disk == listed, (
            f"{surface}: unclaimed on disk: {sorted(on_disk - listed)}; "
            f"claimed but absent: {sorted(listed - on_disk)}"
        )


def test_the_batch_request_in_the_response_corpus_is_the_valid_batch_request_fixture() -> (
    None
):
    fixture = _load(BATCH_REQUEST_SURFACE, "batch-request-valid-two-items.json")
    assert MANIFESTS[BATCH_RESPONSE_SURFACE]["request"] == fixture


@pytest.mark.parametrize("entry", ACCEPT, ids=lambda e: e["file"])
def test_accept_fixtures_validate(entry: dict[str, Any]) -> None:
    validate(entry["surface"], _load(entry["surface"], entry["file"]))


@pytest.mark.parametrize("entry", REJECT, ids=lambda e: e["file"])
def test_reject_fixtures_fail_at_the_declared_location(entry: dict[str, Any]) -> None:
    found = violations(entry["surface"], _load(entry["surface"], entry["file"]))
    assert found, f"{entry['file']} was expected to fail validation but passed"
    assert {v.instance_location for v in found} == {
        entry["expect_instance_location"]
    }, f"{entry['file']}: {[str(v) for v in found]}"
    assert entry["expect_keyword"] in {v.keyword for v in found}


@pytest.mark.parametrize("entry", CLIENT, ids=lambda e: e["file"])
def test_client_enforced_fixtures_validate_but_are_refused(
    entry: dict[str, Any],
) -> None:
    raw = _bytes(entry["surface"], entry["file"])
    # The bytes may repeat a member; the decoded survivor must still validate.
    validate(entry["surface"], json.loads(raw))
    with pytest.raises(ContractError):
        if entry["surface"] == DECISION_SURFACE:
            parse_decision_bytes(raw, now=NOW)
        else:
            parse_batch_response_bytes(BATCH_REQUEST, raw, now=NOW)


@pytest.mark.parametrize(
    "entry",
    [e for e in REJECT if e["surface"] == DECISION_SURFACE],
    ids=lambda e: e["file"],
)
def test_the_client_never_turns_a_rejected_decision_into_an_accept(
    entry: dict[str, Any],
) -> None:
    with pytest.raises(ContractError):
        parse_decision_bytes(_bytes(entry["surface"], entry["file"]), now=NOW)


def test_a_disagreeing_batch_keeps_each_decision_bound_to_its_request() -> None:
    decisions = parse_batch_response_bytes(
        _bytes(BATCH_REQUEST_SURFACE, "batch-request-valid-two-items.json"),
        _bytes(BATCH_RESPONSE_SURFACE, "batch-response-valid-items-disagree.json"),
        now=NOW,
    )
    assert [d.allowed for d in decisions] == [True, False]
    assert decisions[0].reason_code != decisions[1].reason_code


def test_a_decision_parses_to_its_verdict_and_obligations() -> None:
    allow = parse_decision_bytes(
        _bytes(DECISION_SURFACE, "decision-valid-allow-with-obligations.json"), now=NOW
    )
    assert allow.allowed is True
    assert allow.policy_revision == 42
    assert allow.obligations is not None
    assert allow.obligations.minimum_assurance == "aal2"
    deny = parse_decision_bytes(
        _bytes(DECISION_SURFACE, "decision-valid-deny.json"), now=NOW
    )
    assert deny.allowed is False
    assert deny.obligations is None


def test_the_decision_has_no_default_verdict_in_the_schema() -> None:
    schema = json.loads(
        (contracts_dir() / "jsonschema" / f"{DECISION_SURFACE}.schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert "allowed" in schema["required"]

    def keys(node: Any) -> set[str]:
        if isinstance(node, dict):
            return set(node) | {k for v in node.values() for k in keys(v)}
        if isinstance(node, list):
            return {k for v in node for k in keys(v)}
        return set()

    assert "default" not in keys(schema)


def test_an_error_envelope_is_never_a_decision() -> None:
    envelope = {
        "schema_version": "error.v1",
        "status": 503,
        "reason_code": "authority_unavailable",
        "request_id": "req_EXAMPLE0000000000000000001",
        "occurred_at": "2026-09-03T05:00:00Z",
        "retry_after_seconds": 5,
    }
    assert violations(DECISION_SURFACE, envelope)
    assert violations("error.v1", _load(DECISION_SURFACE, "decision-valid-deny.json"))


def test_a_decision_is_usable_only_strictly_before_its_expiry() -> None:
    raw = _bytes(DECISION_SURFACE, "decision-valid-allow-with-obligations.json")
    assert parse_decision_bytes(raw, now=EXPIRY - timedelta(seconds=1)).allowed is True
    for now in (EXPIRY, EXPIRY + timedelta(seconds=1), EXPIRY + timedelta(days=365)):
        with pytest.raises(ContractError, match="expired"):
            parse_decision_bytes(raw, now=now)
    with pytest.raises(ContractError, match="expired"):
        parse_decision_bytes(
            _bytes(DECISION_SURFACE, "decision-valid-deny.json"), now=EXPIRY
        )
    request = _bytes(BATCH_REQUEST_SURFACE, "batch-request-valid-two-items.json")
    response = _bytes(
        BATCH_RESPONSE_SURFACE, "batch-response-valid-items-disagree.json"
    )
    assert (
        len(
            parse_batch_response_bytes(
                request, response, now=EXPIRY - timedelta(seconds=1)
            )
        )
        == 2
    )
    with pytest.raises(ContractError, match="expired"):
        parse_batch_response_bytes(request, response, now=EXPIRY)


def test_a_naive_now_is_refused() -> None:
    raw = _bytes(DECISION_SURFACE, "decision-valid-deny.json")
    with pytest.raises(ContractError, match="timezone"):
        parse_decision_bytes(raw, now=datetime(2026, 9, 3, 5, 0, 0))


def test_the_typed_parser_refuses_an_impossible_calendar_date() -> None:
    raw = _bytes(DECISION_SURFACE, "decision-valid-deny.json").replace(
        b"2026-09-03T05:00:30Z", b"2026-02-30T05:00:30Z"
    )
    assert b"2026-02-30" in raw
    with pytest.raises(ContractError):
        parse_decision_bytes(raw, now=NOW)
