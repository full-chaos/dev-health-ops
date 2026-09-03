"""Golden-corpus tests for the ``error.v1`` wire contract.

The Go runner, this file, and the ajv check read the SAME manifest over the
SAME fixture files. Three copies of one corpus is the drift G-70 exists to
catch, so the inventory lives in
``contracts/auth/v1/examples/error/manifest.json`` and must never be
duplicated into a runner.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
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
from dev_health_ops.authclient.error_envelope import (
    MAX_CLOCK_SKEW,
    SURFACE,
    TRANSIENT_STATUSES,
    parse,
)


def _fixture_dir() -> Path:
    return contracts_dir() / "examples" / "error"


def _manifest() -> dict[str, Any]:
    return json.loads((_fixture_dir() / "manifest.json").read_text(encoding="utf-8"))


def _load(name: str) -> Any:
    return json.loads((_fixture_dir() / name).read_text(encoding="utf-8"))


FIXTURES = _fixture_dir()
MANIFEST = _manifest()
ACCEPT = [e["file"] for e in MANIFEST["accept"]]
REJECT = MANIFEST["reject"]
REJECT_BY_CLIENT = MANIFEST["reject_by_client"]

#: A reference instant later than every fixture's ``occurred_at`` except the
#: far-future one, so the skew check is exercised deterministically rather than
#: depending on when the suite happens to run. Without this the corpus would
#: start failing on its own the moment real time passed 2027.
NOW = datetime(2026, 9, 3, 6, 0, 0, tzinfo=timezone.utc)


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

    A validator that is too strict passes every rejection test while breaking
    every real caller, and that failure is invisible to a suite asserting only
    rejections.
    """
    validate(SURFACE, _load(name))


@pytest.mark.parametrize("entry", REJECT, ids=lambda e: e["file"])
def test_reject_fixtures_fail_at_the_declared_location(entry: dict[str, Any]) -> None:
    """Each rejection fails for the declared REASON, not merely somewhere.

    Asserting only "this does not validate" lets a fixture drift into failing
    for an unrelated reason -- a typo in an unrelated field -- while still
    passing. Pinning the instance location and keyword is what keeps the
    fixture testing the thing it was written for.
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
def test_client_enforced_fixtures_validate_but_are_refused(
    entry: dict[str, Any],
) -> None:
    """The category's whole point, in one assertion pair.

    These documents are VALID -- the first assertion proves the schema does
    not catch them, which is what makes the client check load-bearing rather
    than belt-and-braces. The second proves the client does.
    """
    document = _load(entry["file"])
    validate(SURFACE, document)  # must NOT raise
    with pytest.raises(ContractError):
        parse(document, entry["http_status"], now=NOW)


def test_transient_statuses_match_the_schema() -> None:
    """The client's TRANSIENT_STATUSES must equal the schema's `if` branch.

    Two sources of truth for the same fact is the drift generator this whole
    programme exists to remove, so the duplication is allowed only because
    this test makes it detectable. Read from the schema rather than restating
    it here, or the assertion would compare a literal against itself.
    """
    schema = validator_for(SURFACE).schema
    # A Draft 2020-12 schema is legally `true` or `false`, so the type is
    # `bool | Mapping`. Narrowing it is not ceremony: a boolean schema here
    # would mean the file had degenerated into "accept anything", and the
    # assertion below would then be comparing against nothing.
    assert isinstance(schema, dict), "error.v1 resolved to a boolean schema"
    from_schema = set(schema["if"]["properties"]["status"]["enum"])
    assert from_schema, "read no statuses out of the schema's if branch"
    assert from_schema == set(TRANSIENT_STATUSES), (
        f"schema says {sorted(from_schema)}, client says {sorted(TRANSIENT_STATUSES)}"
    )


def test_a_matching_status_is_accepted() -> None:
    """The accepting half of the status-agreement check.

    Without this, a client that refused EVERY envelope would pass all three
    reject_by_client cases.
    """
    envelope = parse(_load("valid-403-grant-absent.json"), 403, now=NOW)
    assert envelope.status == 403
    assert envelope.reason_code == "grant_absent"
    assert envelope.retry_after_seconds is None
    assert not envelope.is_transient


def test_a_transient_envelope_reports_itself_transient() -> None:
    envelope = parse(_load("valid-429-with-retry.json"), 429, now=NOW)
    assert envelope.is_transient
    assert envelope.retry_after_seconds == 30


def test_clock_skew_is_tolerated_up_to_the_bound_and_refused_past_it() -> None:
    """Both sides of MAX_CLOCK_SKEW, because a one-sided bound is not a bound.

    The at-the-bound case is what stops a future tightening from silently
    refusing envelopes that arrive from a host a few seconds fast.
    """
    document = _load("valid-403-grant-absent.json")
    stamped = datetime.fromisoformat(document["occurred_at"].replace("Z", "+00:00"))

    at_bound = stamped - MAX_CLOCK_SKEW
    parse(document, 403, now=at_bound)  # exactly at the bound: accepted

    past_bound = at_bound - timedelta(seconds=1)
    with pytest.raises(ContractError, match="ahead of"):
        parse(document, 403, now=past_bound)


def test_a_past_timestamp_is_never_refused() -> None:
    """The skew bound is one-directional on purpose.

    An error stamped in the past is normal -- queueing, retries, slow logs --
    and refusing it would break every replayed or queued envelope. This test
    is what stops the bound being made symmetric by someone tidying it.
    """
    document = _load("valid-403-grant-absent.json")
    much_later = NOW + timedelta(days=365)
    parse(document, 403, now=much_later)
