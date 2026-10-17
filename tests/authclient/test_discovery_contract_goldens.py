"""Golden-corpus tests for the ``discovery.v1`` wire contract.

The Go runner (``internal/auth/contracts/discovery_goldens_test.go``) and this
file read the SAME manifest over the SAME fixtures (G-70).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.authclient.contracts import (
    ContractError,
    contracts_dir,
    validate,
    violations,
)
from dev_health_ops.authclient.discovery import parse_discovery

SURFACE = "discovery.v1"
FIXTURES: Path = contracts_dir() / "examples" / "discovery"
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text(encoding="utf-8"))
ACCEPT = [e["file"] for e in MANIFEST["accept"]]
REJECT = MANIFEST["reject"]
REJECT_BY_CLIENT = [e["file"] for e in MANIFEST["reject_by_client"]]

TRUSTED_ISSUER = {
    "valid-minimal.json": "https://auth.example.test",
    "valid-port-and-path-prefix.json": "https://auth.example.test:8443/dev-health",
    "client-rejected-jwks-uri-other-origin.json": "https://auth.example.test",
    "client-rejected-jwks-uri-other-port.json": "https://auth.example.test",
    "client-rejected-jwks-uri-other-path-prefix.json": (
        "https://auth.example.test:8443/dev-health"
    ),
    "client-rejected-jwks-uri-nested-under-issuer.json": "https://auth.example.test",
    "client-rejected-issuer-not-pinned.json": "https://auth.example.test",
}


def _raw(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _load(name: str) -> Any:
    return json.loads(_raw(name))


def test_the_manifest_is_not_empty() -> None:
    assert ACCEPT and REJECT and REJECT_BY_CLIENT


def test_every_fixture_file_on_disk_is_claimed_by_the_manifest() -> None:
    listed = set(ACCEPT) | {e["file"] for e in REJECT} | set(REJECT_BY_CLIENT)
    on_disk = {p.name for p in FIXTURES.glob("*.json") if p.name != "manifest.json"}
    assert on_disk == listed, (
        f"unclaimed on disk: {sorted(on_disk - listed)}; "
        f"claimed but absent: {sorted(listed - on_disk)}"
    )


def test_every_client_checked_fixture_has_a_trusted_issuer() -> None:
    missing = [f for f in ACCEPT + REJECT_BY_CLIENT if f not in TRUSTED_ISSUER]
    assert not missing, f"no trusted issuer for {missing}"


@pytest.mark.parametrize("name", ACCEPT)
def test_accept_fixtures_validate_and_parse(name: str) -> None:
    validate(SURFACE, _load(name))
    parsed = parse_discovery(_raw(name), TRUSTED_ISSUER[name])
    assert parsed.issuer == TRUSTED_ISSUER[name]
    assert parsed.id_token_signing_alg_values_supported == ("EdDSA",)


@pytest.mark.parametrize("entry", REJECT, ids=lambda e: e["file"])
def test_reject_fixtures_fail_at_the_declared_location(entry: dict[str, Any]) -> None:
    found = violations(SURFACE, _load(entry["file"]))
    assert found, f"{entry['file']} was expected to fail validation but passed"
    assert {v.instance_location for v in found} == {entry["expect_instance_location"]}
    assert entry["expect_keyword"] in {v.keyword for v in found}


@pytest.mark.parametrize("name", REJECT_BY_CLIENT)
def test_client_enforced_fixtures_are_valid_but_the_client_refuses_them(
    name: str,
) -> None:
    validate(SURFACE, _load(name))
    with pytest.raises(
        ContractError,
        match="disagrees with its own endpoint URL|not the trusted issuer",
    ):
        parse_discovery(_raw(name), TRUSTED_ISSUER[name])


def test_a_document_from_another_issuer_is_refused() -> None:
    raw = _raw("valid-minimal.json")
    with pytest.raises(ContractError, match="not the trusted issuer"):
        parse_discovery(raw, "https://other.example.test")
    with pytest.raises(ContractError):
        parse_discovery(raw, "")


def test_a_duplicate_member_is_refused_before_validation() -> None:
    raw = """{
      "issuer": "https://auth.example.test",
      "jwks_uri": "https://evil.example.test/.well-known/jwks.json",
      "jwks_uri": "https://auth.example.test/.well-known/jwks.json",
      "id_token_signing_alg_values_supported": ["EdDSA"]
    }"""
    with pytest.raises(ContractError, match="duplicate object member"):
        parse_discovery(raw, "https://auth.example.test")


def test_malformed_utf8_is_a_contract_error_not_a_decode_error() -> None:
    with pytest.raises(ContractError, match="not valid JSON"):
        parse_discovery(b'{"issuer": "\xff"}', "https://auth.example.test")


def test_the_client_is_exported_from_the_package() -> None:
    from dev_health_ops import authclient

    assert authclient.parse_discovery is parse_discovery


@pytest.mark.parametrize(
    "trailing", ["\nnull", '\n{"issuer": "https://evil.example.test"}', "x"]
)
def test_trailing_content_after_the_document_is_refused(trailing: str) -> None:
    raw = _raw("valid-minimal.json").decode("utf-8") + trailing
    with pytest.raises(ContractError, match="not valid JSON"):
        parse_discovery(raw, "https://auth.example.test")
