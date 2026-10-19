"""Closed vocabularies for revoked_reason and the audit reason_code (CHAOS-10194).

contracts/auth/v1/vocabulary/reason-codes.json is the single source. The
behaviour under test: every listed value validates, any other value (a
secret-shaped one included) is refused. A separate doc-sync check pins the
schemas' enums to the file.
"""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from dev_health_ops.authclient.contracts import contracts_dir, violations

VOCAB = json.loads(
    (contracts_dir() / "vocabulary" / "reason-codes.json").read_text(encoding="utf-8")
)
REVOKED = VOCAB["revoked_reason"]["values"]
AUDIT = VOCAB["audit_reason_code"]["values"]
NOT_IN_THE_VOCABULARY = [
    "secret_0123456789abcdef0123",
    "key_compromised",
    "Compromised",
    "compromised ",
    "compromise",
    "owner_request_",
    "",
    "revoked because bob@example.test leaked it",
]


def _example(surface_dir: str, name: str) -> Any:
    path = contracts_dir() / "examples" / surface_dir / name
    return json.loads(path.read_text(encoding="utf-8"))


def _entry(value: str) -> Any:
    doc = _example("credential-entry", "valid-revoked.json")
    doc["revoked_reason"] = value
    return doc


def _list(value: str) -> Any:
    doc = _example("credential-list", "valid-two-items.json")
    doc["items"][1]["revoked_reason"] = value
    return doc


def _audit(value: str) -> Any:
    doc = copy.deepcopy(
        _example("audit-event", "invalid-attributes-reason-code-prose.json")
    )
    doc["attributes"]["reason_code"] = value
    return doc


REVOKED_CASES = [("credential-entry.v1", _entry), ("credential-list.v1", _list)]


def test_the_vocabulary_is_not_empty() -> None:
    assert REVOKED and AUDIT


@pytest.mark.parametrize("surface,build", REVOKED_CASES)
def test_every_listed_revoked_reason_validates(surface: str, build: Any) -> None:
    for value in REVOKED:
        assert not violations(surface, build(value)), (surface, value)


@pytest.mark.parametrize("surface,build", REVOKED_CASES)
def test_a_revoked_reason_outside_the_vocabulary_is_refused(
    surface: str, build: Any
) -> None:
    for value in NOT_IN_THE_VOCABULARY:
        assert violations(surface, build(value)), (surface, value)


def test_every_listed_audit_reason_code_validates() -> None:
    for value in AUDIT:
        assert not violations("audit-event.v1", _audit(value)), value


def test_an_audit_reason_code_outside_the_vocabulary_is_refused() -> None:
    for value in NOT_IN_THE_VOCABULARY:
        assert violations("audit-event.v1", _audit(value)), value


def test_every_revoked_reason_is_also_an_audit_reason_code() -> None:
    assert set(REVOKED) <= set(AUDIT)


def _schema(name: str) -> Any:
    return json.loads(
        (contracts_dir() / "jsonschema" / name).read_text(encoding="utf-8")
    )


def test_the_schema_enums_equal_the_vocabulary_file() -> None:
    entry = _schema("credential-entry.v1.schema.json")["properties"]["revoked_reason"]
    items = _schema("credential-list.v1.schema.json")["properties"]["items"]["items"][
        "properties"
    ]["revoked_reason"]
    audit = _schema("audit-event.v1.schema.json")["properties"]["attributes"][
        "properties"
    ]["reason_code"]
    assert sorted(entry.get("enum", [])) == sorted(REVOKED)
    assert sorted(items.get("enum", [])) == sorted(REVOKED)
    assert sorted(audit.get("enum", [])) == sorted(AUDIT)


# Approved lists, written by hand on purpose (see the Go test of the same name):
# widening the vocabulary file and the schema enums together must also need a
# deliberate edit here.
APPROVED_REVOKED = [
    "compromised",
    "owner_request",
]
APPROVED_AUDIT = [
    "allowed_by_repository_grant",
    "grant_absent",
    "repository_not_granted",
    "platform_role_absent",
    "credential_missing",
    "rate_limited",
    "current_state_unavailable",
    "authority_unavailable",
    "upstream_unavailable",
    "not_found",
    "revision_conflict",
    "internal_error",
    "compromised",
    "owner_request",
    "role_granted",
    "role_revoked",
    "membership_added",
    "membership_removed",
    "membership_status_changed",
    "grant_created",
    "grant_revoked",
    "credential_issued",
    "credential_rotated",
    "credential_revoked",
    "session_revoked",
    "organization_created",
    "organization_updated",
    "principal_disabled",
    "principal_enabled",
    "identity_linked",
    "identity_unlinked",
    "policy_revision_activated",
    "entitlement_snapshot_applied",
    "denied_no_membership",
    "denied_no_grant",
    "denied_policy",
]


def test_the_vocabulary_file_is_exactly_the_approved_lists() -> None:
    assert sorted(REVOKED) == sorted(APPROVED_REVOKED)
    assert sorted(AUDIT) == sorted(APPROVED_AUDIT)
