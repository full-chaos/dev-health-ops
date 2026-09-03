"""Cross-language golden tests: the Python half of the principal.v1 corpus.

Go (``internal/auth/contracts``) and TypeScript (dev-health-web) run the SAME
manifest over the SAME fixture files. Three copies of one corpus is the drift
class these tests exist to catch, so this module reads
``contracts/auth/v1/examples/principal/manifest.json`` and must never
enumerate fixtures itself.

The corpus has two halves and they catch different failure modes. The
``reject`` half answers "does the contract catch the bad document". The
``accept`` half answers "does the contract accept every document it is
SUPPOSED to accept" -- a validator that is too strict passes every rejection
test while breaking every real caller, and that failure is invisible to a
suite that only asserts rejections.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dev_health_ops.authclient import Principal, contracts_dir, violations

SURFACE = "principal.v1"
FIXTURE_DIRNAME = "principal"


def _fixture_dir() -> Path:
    return contracts_dir() / "examples" / FIXTURE_DIRNAME


def _manifest() -> dict[str, Any]:
    return json.loads((_fixture_dir() / "manifest.json").read_text(encoding="utf-8"))


def _load(name: str) -> Any:
    return json.loads((_fixture_dir() / name).read_text(encoding="utf-8"))


MANIFEST = _manifest()
ACCEPT = [entry["file"] for entry in MANIFEST["accept"]]
REJECT = [
    (entry["file"], entry["expect_instance_location"], entry["expect_keyword"])
    for entry in MANIFEST["reject"]
]


def test_the_corpus_is_not_empty_in_either_direction() -> None:
    """A manifest that lost its fixtures would make every case below vacuous.

    Both halves are asserted non-empty. A parametrised test over an empty list
    reports zero failures and exits 0, which is indistinguishable from a
    passing suite -- the same shape as a skipped test reporting package-level
    ``ok``.
    """
    assert ACCEPT, "manifest declares no accept fixtures"
    assert REJECT, "manifest declares no reject fixtures"


def test_every_fixture_file_on_disk_is_claimed_by_the_manifest() -> None:
    """A fixture nobody runs is worse than a missing one: it looks like coverage.

    Catches the half-landed change that adds a fixture and forgets the
    manifest entry -- the file sits in the directory reading as a test that
    exists, while no runner in any of the three languages ever opens it.
    """
    on_disk = {
        path.name
        for path in _fixture_dir().glob("*.json")
        if path.name != "manifest.json"
    }
    claimed = set(ACCEPT) | {name for name, _, _ in REJECT}
    assert on_disk == claimed, (
        f"unclaimed on disk: {sorted(on_disk - claimed)}; "
        f"claimed but absent: {sorted(claimed - on_disk)}"
    )


@pytest.mark.parametrize("name", ACCEPT)
def test_accepted_fixture_validates(name: str) -> None:
    found = violations(SURFACE, _load(name))
    assert found == [], f"{name} should validate but did not: " + "; ".join(
        str(v) for v in found
    )


@pytest.mark.parametrize("name", ACCEPT)
def test_accepted_fixture_round_trips_through_the_client(name: str) -> None:
    """Validation alone does not prove the client can read the document.

    The nested objects are where a wrong key hides: the document validates,
    the dataclass constructs, and a field is silently absent or empty with
    nothing asserting on it. Every required leaf is checked, and the actor
    chain's length is compared against the raw document so a chain that
    silently decoded to empty cannot make the delegation assertions vacuous.
    """
    raw = _load(name)
    principal = Principal.from_wire(raw)

    assert principal.principal_id
    assert principal.principal_type
    assert principal.credential.cls
    assert principal.credential.credential_id
    assert principal.credential.issuer
    assert principal.credential.audience
    assert principal.authentication.methods
    assert principal.authentication.assurance
    assert principal.authentication.authenticated_at is not None
    assert principal.issued_at is not None
    assert principal.expires_at is not None

    assert len(principal.actor_chain) == len(raw["actor_chain"])
    for hop in principal.actor_chain:
        assert hop.actor_principal_id
        assert hop.delegation_id
        assert hop.reason
        assert hop.expires_at is not None


@pytest.mark.parametrize("name", ACCEPT)
def test_effective_deadline_is_never_later_than_any_component_expiry(name: str) -> None:
    """ACP-ADR-03's bound must hold whichever expiry is the earliest.

    Asserts the property rather than the implementation: the deadline is
    <= the principal's own expiry AND <= every delegation's. A version that
    returned only ``self.expires_at`` passes the first half and fails the
    second on the delegated fixtures, which is the case that matters.
    """
    principal = Principal.from_wire(_load(name))
    deadline = principal.effective_deadline()
    assert deadline <= principal.expires_at
    for hop in principal.actor_chain:
        assert deadline <= hop.expires_at


def test_the_cache_key_binds_every_dimension_g31_requires() -> None:
    """G-31 names the dimensions; this asserts each one actually moves the key.

    Checking that the tuple has the right length would pass with two fields
    swapped or a constant in a slot. Instead each dimension is perturbed on
    its own and the key must change -- a dimension that is named but not
    bound is a cache a revision bump cannot invalidate, which is the exact
    failure G-31 exists to prevent.
    """
    base = _load("valid-human-delegated-one-hop.json")
    baseline = Principal.from_wire(base).cache_key_dimensions()

    perturbations: dict[str, Any] = {
        "policy_revision": {"policy_revision": base["policy_revision"] + 1},
        "membership_revision": {"membership_revision": base["membership_revision"] + 1},
        "grant_revision": {"grant_revision": base["grant_revision"] + 1},
        "entitlement_revision": {
            "entitlement_revision": base["entitlement_revision"] + 1
        },
        "principal_id": {"principal_id": "prn_EXAMPLE0000000000000009"},
        "organization_id": {"organization_id": "org_EXAMPLE0000000000000009"},
        "credential_id": {
            "credential": {
                **base["credential"],
                "credential_id": "ses_EXAMPLE0000000000000009",
            }
        },
        "assurance": {
            "authentication": {**base["authentication"], "assurance": "aal2"}
        },
        "actor_chain": {
            "actor_chain": [
                {
                    **base["actor_chain"][0],
                    "delegation_id": "dlg_EXAMPLE0000000000000009",
                }
            ]
        },
    }
    for dimension, override in perturbations.items():
        changed = Principal.from_wire({**base, **override}).cache_key_dimensions()
        assert changed != baseline, (
            f"changing {dimension} did not change the cache key, so an allow decision cached "
            f"against it would survive a change G-31 requires to invalidate it"
        )


@pytest.mark.parametrize(("name", "pointer", "keyword"), REJECT)
def test_rejected_fixture_is_rejected_by_the_declared_rule(
    name: str, pointer: str, keyword: str
) -> None:
    """Assert WHICH rule rejected the document, not merely that one did.

    A negative fixture that fails for the wrong reason has stopped testing
    what it claims to test, and nothing about the exit code says so.
    """
    found = violations(SURFACE, _load(name))
    assert found, f"{name} validated cleanly; expected {keyword} at {pointer}"
    matched = [
        v for v in found if v.instance_location == pointer and v.keyword == keyword
    ]
    assert matched, (
        f"{name} was rejected, but not by {keyword} at {pointer}. Got: "
        + "; ".join(str(v) for v in found)
    )


def test_entitlement_cannot_be_smuggled_into_a_principal() -> None:
    """ACP-ADR-07 decision 2 / G-14, asserted directly rather than only via the corpus.

    This duplicates one manifest row on purpose. The corpus row proves the
    fixture is rejected; this test names the ADR, so a future change that
    deletes the fixture cannot quietly delete the rule with it -- and the
    failure message tells the next reader why the fields are forbidden rather
    than leaving them to read it as an oversight and "fix" it.
    """
    document = _load("valid-human-minimal.json")
    document["tier"] = "enterprise"
    document["licensed_features"] = ["agent_context_runtime"]
    found = violations(SURFACE, document)
    assert any(v.keyword == "additionalProperties" for v in found), (
        "principal.v1 accepted entitlement claims (tier/licensed_features). "
        "ACP-ADR-07 decision 2 makes entitlement an input to a decision and "
        "never a claim in a credential; G-14 forbids it by name."
    )


@pytest.mark.parametrize(("name", "pointer", "keyword"), REJECT)
def test_rejected_fixture_violates_exactly_one_instance_location(
    name: str, pointer: str, keyword: str
) -> None:
    """Pin each reject fixture to a single locus, for the Go runner's sake.

    ``github.com/google/jsonschema-go`` stops at the FIRST violation and
    reports no instance location, so the Go runner can only assert "rejected,
    and the message names this keyword". That assertion is sound only while a
    fixture cannot violate two rules at two different places -- otherwise Go
    could legitimately report the other one and the mismatch would read as a
    defect in the contract rather than in the fixture.

    More than one violation AT THE SAME location is fine and expected: a
    timestamp fixture trips both ``pattern`` and ``format``, by design.
    """
    found = violations(SURFACE, _load(name))
    locations = {v.instance_location for v in found}
    assert locations == {pointer}, (
        f"{name} must violate exactly one instance location so the Go runner's "
        f"first-error assertion is unambiguous; got {sorted(locations)} from: "
        + "; ".join(str(v) for v in found)
    )


def test_format_assertion_is_actually_enabled_not_merely_requested() -> None:
    """Prove the format check can fail, rather than trusting that it is on.

    Draft 2020-12 makes ``format`` annotation-only unless the validator opts
    in, and ``jsonschema`` registers no ``date-time`` checker at all unless
    ``rfc3339-validator`` is importable -- so a suite can pass a
    ``format_checker`` and still assert nothing. This drives a value that is
    well-formed under the schema's ``pattern`` but is NOT a real RFC 3339
    instant (month 13), so only ``format`` can reject it. If the assertion
    is off, this test goes red instead of the corpus silently weakening.
    """
    document = _load("valid-human-minimal.json")
    document["expires_at"] = "2026-13-02T23:25:00Z"
    found = violations(SURFACE, document)
    keywords = {v.keyword for v in found}
    assert "pattern" not in keywords, (
        "the control is wrong: month 13 should satisfy the shape pattern, so that "
        "this test isolates `format`. Got: " + "; ".join(str(v) for v in found)
    )
    assert "format" in keywords, (
        "month 13 was accepted, so `format: date-time` is annotation-only here. "
        "Install rfc3339-validator (declared in pyproject.toml) -- without it the "
        "format check silently checks nothing while the suite stays green. Got: "
        + ("; ".join(str(v) for v in found) or "no violations at all")
    )


def test_the_manifest_declares_that_format_assertion_is_required() -> None:
    """The manifest is the contract between the three runners; pin its flag."""
    assert MANIFEST["requires_format_assertion"] is True
