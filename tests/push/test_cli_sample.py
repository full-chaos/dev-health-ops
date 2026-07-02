"""Tests for `dev-hops push sample` (CHAOS-2700, master-spec CC18/CC29:
samples are loaded via `schema_registry.load_example`, not a packaged
`push/samples/` dir)."""

from __future__ import annotations

import argparse
import json

import pytest

from dev_health_ops.api.external_ingest.schemas import (
    RECORD_KIND_MODELS,
    SCHEMA_VERSION,
)
from dev_health_ops.push import cli as push_cli
from dev_health_ops.push import output as out
from dev_health_ops.push.validate import validate_payload


def test_kind_type_accepts_versioned_form() -> None:
    assert push_cli._kind_type("pull_request.v1") == "pull_request.v1"


def test_kind_type_accepts_bare_form() -> None:
    """Linear's literal acceptance criteria show `--kind pull_request`
    (bare); master-spec CC1 makes the versioned form canonical everywhere.
    Both must work."""
    assert push_cli._kind_type("pull_request") == "pull_request.v1"


def test_kind_type_rejects_unknown() -> None:
    with pytest.raises(Exception):  # argparse.ArgumentTypeError
        push_cli._kind_type("not_a_kind")


@pytest.mark.parametrize("kind", sorted(RECORD_KIND_MODELS))
def test_cmd_sample_kind_produces_valid_envelope(
    kind: str, capsys: pytest.CaptureFixture[str]
) -> None:
    ns = argparse.Namespace(all=False, kind=kind)

    exit_code = push_cli._cmd_sample(ns)

    assert exit_code == out.EXIT_OK
    envelope = json.loads(capsys.readouterr().out)
    assert envelope["schemaVersion"] == SCHEMA_VERSION
    assert len(envelope["records"]) == 1
    assert envelope["records"][0]["kind"] == kind

    outcome = validate_payload(json.dumps(envelope).encode())
    assert outcome.valid, outcome.errors


def test_cmd_sample_all_produces_valid_combined_envelope(
    capsys: pytest.CaptureFixture[str],
) -> None:
    ns = argparse.Namespace(all=True, kind=None)

    exit_code = push_cli._cmd_sample(ns)

    assert exit_code == out.EXIT_OK
    envelope = json.loads(capsys.readouterr().out)
    assert len(envelope["records"]) == len(RECORD_KIND_MODELS)
    assert {r["kind"] for r in envelope["records"]} == set(RECORD_KIND_MODELS)

    outcome = validate_payload(json.dumps(envelope).encode())
    assert outcome.valid, outcome.errors
    assert outcome.items_accepted == len(RECORD_KIND_MODELS)


def test_sample_payload_matches_packaged_example_exactly() -> None:
    """`push sample`'s `payload` field must contain no local re-authoring/
    drift from CHAOS-2692's packaged example.

    Value equality (not raw-byte string equality) is the correct check
    here: `_sample_record` passes the freshly-`json.loads`-ed example dict
    straight through to `payload` with no field added/removed/mutated, and
    JSON key order carries no semantic meaning -- `push sample`'s enclosing
    envelope is pretty-printed with `sort_keys=True` for readability, which
    necessarily reorders keys relative to the source file but changes zero
    values. Byte-identity of the raw *file* (`docs/examples/external-
    ingest/*.json` vs `api/external_ingest/examples/*.json`) is CHAOS-2701's
    drift test (master-spec CC18) -- a different, file-to-file comparison,
    not this CLI's re-serialized stdout."""
    from dev_health_ops.api.external_ingest.schema_registry import load_example

    for kind in RECORD_KIND_MODELS:
        canonical = load_example(kind)
        record = push_cli._sample_record(kind)

        assert record["payload"] == canonical
        # Round-trip through the exact serialization `push sample` emits
        # and back -- proves no precision/type loss for numbers, nested
        # structures, or unicode, not just top-level dict equality.
        round_tripped = json.loads(
            json.dumps(record["payload"], sort_keys=True, default=str)
        )
        assert round_tripped == canonical
