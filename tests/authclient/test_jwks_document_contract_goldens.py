"""Golden-corpus tests for the ``jwks.v1`` wire contract.

The Go runner and this file read the SAME manifest over the SAME fixture
files. Two copies of one corpus is the drift G-70 exists to catch, so the
inventory lives in ``contracts/auth/v1/examples/jwks/manifest.json`` and is
never duplicated into a runner.

WHAT EACH RUNNER OWNS. This surface is a PRODUCER/CONSUMER pair, not two
independent parsers, and the two suites are split along that seam:

  * this file pins the PRODUCER -- ``build_envelope_jwks()`` must emit a
    document the schema accepts;
  * the Go runner pins the CONSUMER -- the real
    ``authverify.Ed25519JWKSVerifier`` must accept every ``accept`` fixture and
    refuse every ``reject`` one.

Neither half is sufficient alone, and together they compose into the property
that actually matters: what Python writes, Go can read. Nothing asserts that
end to end in one process, because the consumer lives in another repository;
the schema is the bridge, which is the entire reason it exists.
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
    validator_for,
    violations,
)

SURFACE = "jwks.v1"


def _fixture_dir() -> Path:
    return contracts_dir() / "examples" / "jwks"


def _load(name: str) -> Any:
    return json.loads((_fixture_dir() / name).read_text(encoding="utf-8"))


FIXTURES = _fixture_dir()
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text(encoding="utf-8"))
ACCEPT = [e["file"] for e in MANIFEST["accept"]]
REJECT = MANIFEST["reject"]
REJECT_BY_CLIENT = MANIFEST["reject_by_client"]


def test_the_manifest_is_not_empty() -> None:
    """A manifest that lost its fixtures would make every case below vacuous.

    Each parametrised test draws its cases from the manifest, so an empty list
    collects zero tests and the suite passes green while checking nothing.
    """
    assert ACCEPT, "manifest declares no accept fixtures"
    assert REJECT, "manifest declares no reject fixtures"
    assert REJECT_BY_CLIENT, "manifest declares no client-enforced fixtures"


def test_every_fixture_file_on_disk_is_claimed_by_the_manifest() -> None:
    """A fixture nobody reads is worse than no fixture.

    It sits in the directory reading as a test that runs, and nothing fails
    when it stops being true.
    """
    listed = {e["file"] for e in MANIFEST["accept"]}
    listed |= {e["file"] for e in REJECT}
    listed |= {e["file"] for e in REJECT_BY_CLIENT}
    on_disk = {p.name for p in FIXTURES.glob("*.json") if p.name != "manifest.json"}
    assert on_disk == listed, (
        f"unclaimed on disk: {sorted(on_disk - listed)}; "
        f"claimed but absent: {sorted(listed - on_disk)}"
    )


@pytest.mark.parametrize("name", ACCEPT)
def test_accept_fixtures_validate(name: str) -> None:
    """The positive controls.

    A schema that is too strict passes every rejection test while refusing
    every real document, and that failure is invisible to a suite asserting
    only rejections.
    """
    validate(SURFACE, _load(name))


@pytest.mark.parametrize("entry", REJECT, ids=lambda e: e["file"])
def test_reject_fixtures_fail_at_the_declared_location(entry: dict[str, Any]) -> None:
    """Each rejection fails for the declared REASON, not merely somewhere.

    Asserting only "this does not validate" lets a fixture drift into failing
    for an unrelated reason -- a typo in a neighbouring field -- while still
    passing. Pinning the instance location and the keyword is what keeps the
    fixture testing the thing it was written for.

    The instance location is asserted HERE and not in the Go runner, because
    Go's validator reports a pointer into the SCHEMA rather than the instance
    and stops at the first violation. Between the two runners every field of
    every manifest entry is checked; neither runner checks all of them.
    """
    found = violations(SURFACE, _load(entry["file"]))
    assert found, f"{entry['file']} was expected to fail validation but passed"
    locations = {v.instance_location for v in found}
    assert locations == {entry["expect_instance_location"]}, (
        f"{entry['file']} should violate exactly "
        f"{entry['expect_instance_location']!r}, got {sorted(locations)}"
    )
    keywords = {v.keyword for v in found}
    assert entry["expect_keyword"] in keywords, (
        f"{entry['file']} should trip {entry['expect_keyword']!r}, "
        f"got {sorted(keywords)}"
    )


@pytest.mark.parametrize("entry", REJECT_BY_CLIENT, ids=lambda e: e["file"])
def test_client_enforced_fixtures_are_valid_against_the_schema(
    entry: dict[str, Any],
) -> None:
    """Half of the category's point; the Go runner asserts the other half.

    These documents are VALID -- proving the schema does not catch them is
    what makes the consumer's check load-bearing rather than belt-and-braces.
    That the CONSUMER refuses them is asserted in the Go runner, because the
    consumer is Go code and no Python reader of this surface exists.
    """
    validate(SURFACE, _load(entry["file"]))


def test_the_duplicate_kid_fixture_really_does_repeat_a_kid() -> None:
    """The client-enforced fixture must keep carrying the thing it exists for.

    Without this, the fixture could be "tidied" into two distinct kids and the
    suite would stay green: the schema would still accept it and the consumer
    would still... accept it too, turning a real assertion into a vacuous one
    in the one test whose whole subject is a rule the schema cannot express.
    """
    keys = _load("client-rejected-duplicate-kid.json")["keys"]
    kids = [k["kid"] for k in keys]
    assert len(kids) > 1, "fixture no longer carries more than one key"
    assert len(set(kids)) == 1, f"fixture no longer repeats a kid: {kids}"
    assert len({k["x"] for k in keys}) == len(keys), (
        "the keys must differ in key material, or `uniqueItems` would catch "
        "them and the rule would not be client-enforced at all"
    )


def test_the_schema_pins_x_to_a_length_that_forces_a_32_byte_key() -> None:
    """The `x` pattern's character count is load-bearing, so it is asserted.

    43 characters of base64url decode to exactly 32 bytes. That is what closes
    the wrong-key-length class -- and that class is not cosmetic: Go's
    ed25519.Verify PANICS on a 31- or 33-byte public key rather than returning
    an error, and both languages' base64 decoders accept those lengths without
    complaint. If someone relaxes this pattern to a plain "base64url" shape,
    this test is what says why they must not.
    """
    schema = validator_for(SURFACE).schema
    assert isinstance(schema, dict), "jwks.v1 resolved to a boolean schema"
    pattern = schema["properties"]["keys"]["items"]["properties"]["x"]["pattern"]
    assert pattern == "^[A-Za-z0-9_-]{43}$", (
        f"the x pattern changed to {pattern!r}; if that is deliberate, prove "
        "the new form still admits only 32-byte keys before editing this test"
    )


def test_the_kid_pattern_keeps_code_points_and_bytes_the_same_number() -> None:
    """Why `kid` is ASCII-restricted rather than bounded by `maxLength`.

    The consumer bounds `kid` at 256 BYTES; JSON Schema's `maxLength` counts
    CODE POINTS. A one-byte-per-character alphabet is what makes the two
    counts identical. This test fails the moment someone widens the alphabet
    without also confronting that gap.
    """
    schema = validator_for(SURFACE).schema
    assert isinstance(schema, dict), "jwks.v1 resolved to a boolean schema"
    pattern = schema["properties"]["keys"]["items"]["properties"]["kid"]["pattern"]
    assert pattern == "^[!-~]{1,256}$", (
        f"the kid pattern changed to {pattern!r}; if that is deliberate, show "
        "that every admitted character is one UTF-8 byte, or the schema will "
        "start accepting kids the consumer refuses"
    )
    # The gap itself, executed rather than described: 256 code points that are
    # 512 bytes. maxLength would admit it; the consumer would not.
    over_the_byte_bound = "é" * 256
    assert len(over_the_byte_bound) == 256
    assert len(over_the_byte_bound.encode("utf-8")) == 512
    with pytest.raises(ContractError):
        validate(
            SURFACE,
            {
                "keys": [
                    {
                        "kty": "OKP",
                        "crv": "Ed25519",
                        "alg": "EdDSA",
                        "kid": over_the_byte_bound,
                        "x": "A" * 43,
                    }
                ]
            },
        )


def test_the_live_producer_emits_a_document_this_schema_accepts() -> None:
    """The contract's reason for existing, asserted against the real producer.

    ``build_envelope_jwks()`` is what actually writes the document the Go
    verifier mounts. A schema that describes an idealised JWKS while the
    producer emits something slightly different would be worse than no schema:
    it would pass its own corpus forever and never touch production.

    The signing key is generated HERE and thrown away. No key material is
    committed, printed, or read from the environment the developer happens to
    have (G-16/G-73), and generating one is also what keeps this test from
    silently skipping on a machine where the real variable is unset -- a skip
    is how a test like this stops running without anyone noticing.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    pem = (
        Ed25519PrivateKey.generate()
        .private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        .decode("utf-8")
    )

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setenv("GO_API_ENVELOPE_PRIVATE_KEY", pem)
        from dev_health_ops.api.graphql.principal_envelope import build_envelope_jwks

        document = build_envelope_jwks()
    finally:
        monkeypatch.undo()

    validate(SURFACE, document)

    # The shape assertions the schema cannot make about a LIVE value: that the
    # producer emitted exactly one key, and that its kid is the one an envelope
    # header will be stamped with. A schema-valid document carrying the wrong
    # kid verifies nothing.
    assert len(document["keys"]) == 1
    from dev_health_ops.api.graphql.principal_envelope import ENVELOPE_KEY_ID

    assert document["keys"][0]["kid"] == ENVELOPE_KEY_ID
