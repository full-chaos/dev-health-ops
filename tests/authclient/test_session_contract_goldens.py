"""Golden-corpus tests for session.v1, refresh-request.v1, refresh-response.v1
and auth-outcome.v1 (CHAOS-10155). Same manifests and fixtures as the Go and
ajv runners; nothing is enumerated here."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.authclient import session_contracts as sc
from dev_health_ops.authclient.contracts import ContractError, contracts_dir, violations

SURFACES: dict[str, tuple[str, Callable[[bytes], Any]]] = {
    "session": (sc.SESSION, sc.parse_session),
    "refresh-request": (sc.REFRESH_REQUEST, sc.parse_refresh_request),
    "refresh-response": (sc.REFRESH_RESPONSE, sc.parse_refresh_response),
    "auth-outcome": (sc.AUTH_OUTCOME, sc.parse_auth_outcome),
}


def _dir(directory: str) -> Path:
    return contracts_dir() / "examples" / directory


def _cases(group: str) -> list[Any]:
    cases = []
    for directory in SURFACES:
        manifest = json.loads((_dir(directory) / "manifest.json").read_text("utf-8"))
        for entry in manifest[group]:
            cases.append(
                pytest.param(directory, entry, id=f"{directory}/{entry['file']}")
            )
    return cases


@pytest.mark.parametrize("directory", list(SURFACES))
def test_corpus_is_non_empty_and_fully_claimed(directory: str) -> None:
    manifest = json.loads((_dir(directory) / "manifest.json").read_text("utf-8"))
    for group in ("accept", "reject", "reject_by_client"):
        assert manifest[group], f"{directory}: empty group {group}"
    claimed = {
        e["file"] for g in ("accept", "reject", "reject_by_client") for e in manifest[g]
    }
    on_disk = {
        p.name for p in _dir(directory).glob("*.json") if p.name != "manifest.json"
    }
    assert on_disk == claimed


@pytest.mark.parametrize(("directory", "entry"), _cases("accept"))
def test_accept_fixtures_parse(directory: str, entry: dict[str, Any]) -> None:
    SURFACES[directory][1]((_dir(directory) / entry["file"]).read_bytes())


@pytest.mark.parametrize(("directory", "entry"), _cases("reject"))
def test_reject_fixtures_fail_at_the_declared_location(
    directory: str, entry: dict[str, Any]
) -> None:
    surface, parse = SURFACES[directory]
    raw = (_dir(directory) / entry["file"]).read_bytes()
    found = violations(surface, json.loads(raw))
    assert found, "validated cleanly"
    assert {v.instance_location for v in found} == {entry["expect_instance_location"]}
    assert entry["expect_keyword"] in {v.keyword for v in found}
    with pytest.raises(ContractError):
        parse(raw)


@pytest.mark.parametrize(("directory", "entry"), _cases("reject_by_client"))
def test_client_rejected_fixtures_pass_the_schema_but_are_refused(
    directory: str, entry: dict[str, Any]
) -> None:
    surface, parse = SURFACES[directory]
    raw = (_dir(directory) / entry["file"]).read_bytes()
    document = json.loads(raw)
    assert not violations(surface, document), (
        "client-rejected fixture must pass the schema"
    )
    with pytest.raises(ContractError):
        parse(raw)


def _schema(surface: str) -> dict[str, Any]:
    path = contracts_dir() / "jsonschema" / f"{surface}.schema.json"
    return json.loads(path.read_text("utf-8"))


def test_auth_outcome_vocabulary_matches_the_schema() -> None:
    schema = _schema(sc.AUTH_OUTCOME)
    assert (
        set(schema["properties"]["reason_code"]["enum"]) == sc.AUTH_OUTCOME_REASON_CODES
    )
    assert (
        set(schema["then"]["properties"]["reason_code"]["enum"])
        == sc.AUTH_OUTCOME_SUCCESS_REASONS
    )


def test_every_auth_outcome_reason_fits_the_error_envelope_pattern() -> None:
    for code in sc.AUTH_OUTCOME_REASON_CODES:
        document = {
            "schema_version": "error.v1",
            "status": 403,
            "reason_code": code,
            "request_id": "req_EXAMPLE0000000000000000001",
            "occurred_at": "2026-10-18T10:00:00Z",
        }
        assert not violations("error.v1", document), code


def test_session_liveness() -> None:
    live = sc.parse_session((_dir("session") / "valid-live.json").read_bytes())
    revoked = sc.parse_session((_dir("session") / "valid-revoked.json").read_bytes())
    now = datetime(2026, 10, 18, 11, 0, tzinfo=timezone.utc)
    assert live.is_live(now)
    assert not live.is_live(live.expires_at)
    assert not revoked.is_live(now)


def test_secrets_are_not_in_repr() -> None:
    raw = (_dir("refresh-response") / "valid-full-lifetimes.json").read_bytes()
    text = repr(sc.parse_refresh_response(raw)) + repr(
        sc.parse_refresh_request(
            (_dir("refresh-request") / "valid-minimal.json").read_bytes()
        )
    )
    assert (
        "EXAMPLEHEADER" not in text
        and "EXAMPLEREFRESH" not in text
        and "EXAMPLEROTATED" not in text
    )
