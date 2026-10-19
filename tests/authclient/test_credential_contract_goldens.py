"""Golden-corpus tests for the credential metadata surfaces.

``credential-entry.v1``, ``credential-list.v1`` and ``credential-receipt.v1``.
The Go runner (``credential_goldens_test.go``), the ajv runner and this file
read the SAME manifests over the SAME fixtures (G-70).
"""

from __future__ import annotations

import dataclasses
import json
import re
from datetime import datetime
from typing import Any

import pytest

from dev_health_ops.authclient.contracts import (
    ContractError,
    contracts_dir,
    validate,
    validator_for,
    violations,
)
from dev_health_ops.authclient.credential import (
    CredentialBody,
    CredentialEntry,
    CredentialList,
    CredentialReceipt,
    check_credential_rotation,
    check_credential_transition,
    parse_credential_entry,
    parse_credential_list,
    parse_credential_receipt,
)

EXAMPLES = contracts_dir() / "examples"
SURFACES = {
    "credential-entry.v1": ("credential-entry", parse_credential_entry),
    "credential-list.v1": ("credential-list", parse_credential_list),
    "credential-receipt.v1": ("credential-receipt", parse_credential_receipt),
}


def _manifest(directory: str) -> dict[str, Any]:
    return json.loads((EXAMPLES / directory / "manifest.json").read_text("utf-8"))


def _clock(directory: str) -> datetime:
    value = _manifest(directory).get("clock_now", "2026-10-18T00:00:00Z")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _raw(directory: str, name: str) -> bytes:
    return (EXAMPLES / directory / name).read_bytes()


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
    accepted = {e["file"] for e in manifest["accept"]}
    for entry in manifest["reject_by_client"]:
        assert entry.get("after", None) in (None, *accepted), entry


@pytest.mark.parametrize(("surface", "directory", "parse", "entry"), _cases("accept"))
def test_accept_fixtures_validate_and_parse(
    surface: str, directory: str, parse: Any, entry: dict[str, Any]
) -> None:
    validate(surface, json.loads(_raw(directory, entry["file"])))
    parse(_raw(directory, entry["file"]), _clock(directory))


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
        parse(_raw(directory, entry["file"]), _clock(directory))


@pytest.mark.parametrize(
    ("surface", "directory", "parse", "entry"), _cases("reject_by_client")
)
def test_client_enforced_fixtures_are_valid_but_the_client_refuses_them(
    surface: str, directory: str, parse: Any, entry: dict[str, Any]
) -> None:
    validate(surface, json.loads(_raw(directory, entry["file"])))
    now = _clock(directory)
    after = entry.get("after")
    if after is None:
        with pytest.raises(ContractError):
            parse(_raw(directory, entry["file"]), now)
        return
    prev = parse_credential_entry(_raw(directory, after), now).body
    # `next` parses on its own (the document is fine); the transition is refused.
    nxt = parse_credential_entry(_raw(directory, entry["file"]), now).body
    with pytest.raises(ContractError):
        check_credential_transition(prev, nxt)


def test_every_accepted_entry_transitions_to_itself_and_narrowing_is_allowed() -> None:
    directory = "credential-entry"
    now = _clock(directory)
    for entry in _manifest(directory)["accept"]:
        body = parse_credential_entry(_raw(directory, entry["file"]), now).body
        check_credential_transition(body, body)
    active = parse_credential_entry(_raw(directory, "valid-active.json"), now).body
    revoked = parse_credential_entry(_raw(directory, "valid-revoked.json"), now).body
    check_credential_transition(
        active, dataclasses.replace(active, scopes=("api:read",))
    )
    check_credential_transition(
        active,
        dataclasses.replace(
            active,
            status="revoked",
            revoked_at=revoked.revoked_at,
            revoked_reason=revoked.revoked_reason,
        ),
    )


def _schema(surface: str) -> dict[str, Any]:
    schema = validator_for(surface).schema
    assert isinstance(schema, dict)
    return schema


def test_parsed_types_carry_every_schema_member() -> None:
    entry = set(_schema("credential-entry.v1")["properties"]) - {"schema_version"}
    got = {f.name for f in dataclasses.fields(CredentialBody)}
    assert got == entry, f"drops {sorted(entry - got)} / invents {sorted(got - entry)}"
    assert {f.name for f in dataclasses.fields(CredentialEntry)} == {"body"}
    assert {f.name for f in dataclasses.fields(CredentialList)} == {"items"}
    assert {f.name for f in dataclasses.fields(CredentialReceipt)} == {
        "kind",
        "credential",
        "secret",
    }
    items = _schema("credential-list.v1")["properties"]["items"]["items"]["properties"]
    assert set(items) == entry
    receipt = _schema("credential-receipt.v1")["properties"]
    assert set(receipt) - {"schema_version"} == {"kind", "credential", "secret"}


def test_list_items_equal_the_entry_and_receipt_credential_is_a_stricter_subset() -> (
    None
):
    entry = {
        k: v
        for k, v in _schema("credential-entry.v1")["properties"].items()
        if k != "schema_version"
    }
    items = _schema("credential-list.v1")["properties"]["items"]["items"]["properties"]
    assert items == entry
    credential = _schema("credential-receipt.v1")["properties"]["credential"][
        "properties"
    ]
    for name, schema in credential.items():
        assert name in entry, f"receipt credential member {name} is not an entry member"
        if name != "status":
            assert schema == entry[name], f"receipt credential member {name} differs"


def _declared_names(node: Any, into: set[str]) -> None:
    if isinstance(node, dict):
        props = node.get("properties")
        if isinstance(props, dict):
            into.update(props)
        for child in node.values():
            _declared_names(child, into)
    elif isinstance(node, list):
        for child in node:
            _declared_names(child, into)


def test_only_the_receipt_schema_can_carry_a_secret() -> None:
    secretish = {
        "secret",
        "secret_hash",
        "hash_algorithm",
        "token",
        "password",
        "api_key",
    }
    for surface in SURFACES:
        names: set[str] = set()
        _declared_names(_schema(surface), names)
        if surface == "credential-receipt.v1":
            assert "secret" in names
        else:
            assert not (names & secretish), (
                f"{surface} declares {sorted(names & secretish)}"
            )


def test_credential_class_enum_equals_principal_v1s() -> None:
    principal = _schema("principal.v1")["properties"]["credential"]["properties"][
        "class"
    ]["enum"]
    assert principal
    assert (
        _schema("credential-entry.v1")["properties"]["credential_class"]["enum"]
        == principal
    )
    receipt = _schema("credential-receipt.v1")["properties"]["credential"]["properties"]
    assert receipt["credential_class"]["enum"] == principal


def test_every_receipt_secret_in_the_corpus_is_marked_synthetic() -> None:
    manifest = _manifest("credential-receipt")
    for entry in manifest["accept"]:
        secret = json.loads(_raw("credential-receipt", entry["file"]))["secret"]
        assert "EXAMPLE" in secret, (
            f"{entry['file']}: {secret!r} lacks the EXAMPLE marker"
        )


def test_the_entry_matches_the_registry_columns_in_both_directions() -> None:
    from pathlib import Path

    migrations = Path(
        str(
            contracts_dir()
            / ".."
            / ".."
            / ".."
            / "internal"
            / "storage"
            / "postgres"
            / "authschema"
            / "migrations"
        )
    ).resolve()
    assert migrations.is_dir(), f"the producer migrations are missing: {migrations}"
    base = (migrations / "0003_sessions_and_credentials.sql").read_text("utf-8")
    start = base.index("CREATE TABLE credential_registry (")
    body = base[start : base.index("\n);", start)].split("\n")[1:]
    columns = []
    for line in body:
        if (
            not line.startswith("    ")
            or line.startswith("     ")
            or line.strip().startswith("--")
        ):
            continue
        match = re.match(r"^ {4}([a-z_][a-z0-9_]*)\s+\S", line)
        assert match, f"unclassifiable line {line!r}"
        columns.append(match.group(1))
    for line in (
        (migrations / "0007_credential_registry_trd_columns.sql")
        .read_text("utf-8")
        .split("\n")
    ):
        match = re.match(r"^\s*ADD COLUMN\s+([a-z_][a-z0-9_]*)\b", line)
        if match:
            columns.append(match.group(1))
    excluded = {"secret_hash", "hash_algorithm"}
    derived = {"status"}
    members = set(_schema("credential-entry.v1")["properties"]) - {"schema_version"}
    assert members - derived == set(columns) - excluded, (
        f"entry-only {sorted(members - derived - set(columns))}; "
        f"column-only {sorted(set(columns) - excluded - members)}"
    )


def test_every_display_prefix_the_entry_allows_can_begin_a_valid_receipt_secret() -> (
    None
):
    """The allowed states are closed under issue/rotate.

    An entry that validates describes a credential whose receipt can validate:
    round 1 found a 10-character prefix the receipt's secret pattern could not
    begin with.
    """
    shapes = [
        "",
        "a",
        "a" * 16,
        "Ab_9" * 4,
        "_",
        "_" * 16,
        "ABCDEFGH_ijklm_9",
        "exmpl_abcd",
    ]
    for entry in _manifest("credential-entry")["accept"]:
        shapes.append(
            json.loads(_raw("credential-entry", entry["file"]))["display_prefix"]
        )
    template = json.loads(_raw("credential-receipt", "valid-issue.json"))
    now = _clock("credential-receipt")
    for prefix in shapes:
        document = json.loads(json.dumps(template))
        document["credential"]["display_prefix"] = prefix
        document["secret"] = prefix + "EXAMPLE" * 6
        validate("credential-receipt.v1", document)
        parse_credential_receipt(json.dumps(document), now)


def test_a_naive_clock_is_a_contract_error_not_a_type_error() -> None:
    naive = datetime(2026, 10, 18)
    for directory, parse, name in (
        ("credential-entry", parse_credential_entry, "valid-active.json"),
        ("credential-list", parse_credential_list, "valid-two-items.json"),
        ("credential-receipt", parse_credential_receipt, "valid-issue.json"),
    ):
        with pytest.raises(ContractError, match="timezone-aware"):
            parse(_raw(directory, name), naive)


def test_a_list_larger_than_any_bound_is_representable() -> None:
    """No pagination exists, so a count bound would hide truncation."""
    base = json.loads(_raw("credential-list", "valid-two-items.json"))
    items = []
    for i in range(1000):
        item = dict(base["items"][0])
        item["id"] = f"crd_EXAMPLE{i:020d}"
        items.append(item)
    document = {"schema_version": "credential-list.v1", "items": items}
    validate("credential-list.v1", document)
    assert (
        len(
            parse_credential_list(json.dumps(document), _clock("credential-list")).items
        )
        == 1000
    )


def _rotation_pair() -> tuple[CredentialBody, CredentialBody]:
    now = _clock("credential-receipt")
    child = parse_credential_receipt(
        _raw("credential-receipt", "valid-rotate.json"), now
    ).credential
    active = parse_credential_entry(
        _raw("credential-entry", "valid-active.json"), now
    ).body
    assert child.rotation_parent_id is not None
    return dataclasses.replace(active, id=child.rotation_parent_id), child


def test_a_rotation_that_preserves_or_narrows_its_parent_is_accepted() -> None:
    parent, child = _rotation_pair()
    check_credential_rotation(parent, child)
    check_credential_rotation(parent, dataclasses.replace(child, scopes=("api:read",)))


@pytest.mark.parametrize(
    "mutation",
    [
        {"scopes": ("api:read", "api:write", "api:admin")},
        {"audiences": ("dev-health-api", "another-audience")},
        {"principal_id": "prn_EXAMPLE0000000000000002"},
        {"credential_class": "api_key"},
        {"domain_owner": "other-owner"},
        {"rotation_parent_id": "crd_EXAMPLE0000000000000009"},
        {"rotation_parent_id": None},
    ],
)
def test_a_rotation_that_widens_or_changes_identity_is_refused(
    mutation: dict[str, Any],
) -> None:
    parent, child = _rotation_pair()
    with pytest.raises(ContractError):
        check_credential_rotation(parent, dataclasses.replace(child, **mutation))


def test_a_revoked_parent_cannot_be_rotated() -> None:
    parent, child = _rotation_pair()
    revoked = parse_credential_entry(
        _raw("credential-entry", "valid-revoked.json"), _clock("credential-entry")
    ).body
    gone = dataclasses.replace(
        parent,
        status="revoked",
        revoked_at=revoked.revoked_at,
        revoked_reason=revoked.revoked_reason,
    )
    with pytest.raises(ContractError):
        check_credential_rotation(gone, child)


def test_a_list_is_checked_against_one_clock_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.authclient import credential

    real = credential._now
    calls: list[int] = []

    def counting(now: datetime | None) -> datetime:
        calls.append(1)
        return real(now)

    monkeypatch.setattr(credential, "_now", counting)
    parse_credential_list(_raw("credential-list", "valid-two-items.json"), None)
    assert len(calls) == 1
