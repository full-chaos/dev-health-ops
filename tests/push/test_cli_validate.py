"""Tests for `dev-hops push validate` (CHAOS-2700).

Covers the brief's test plan: valid/invalid payloads, missing-field error
shape, stdin (`-`) reading, and malformed JSON handled cleanly (not a raw
traceback).
"""

from __future__ import annotations

import argparse
import io
import json

import pytest

from dev_health_ops.api.external_ingest.schemas import SCHEMA_VERSION
from dev_health_ops.push import cli as push_cli
from dev_health_ops.push import output as out
from dev_health_ops.push.validate import PayloadParseError, validate_payload

VALID_RECORD_PAYLOAD = {
    "externalId": "acme/api",
    "sourceSystem": "github",
}


def _envelope(records: list[dict], idempotency_key: str = "test-key-1") -> dict:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "idempotencyKey": idempotency_key,
        "source": {"type": "customer_push", "system": "github", "instance": "acme/api"},
        "records": records,
    }


def _record(kind: str, external_id: str, payload: dict) -> dict:
    return {"kind": kind, "externalId": external_id, "payload": payload}


def test_validate_payload_valid_envelope() -> None:
    envelope = _envelope([_record("repository.v1", "acme/api", VALID_RECORD_PAYLOAD)])
    outcome = validate_payload(json.dumps(envelope).encode())

    assert outcome.valid is True
    assert outcome.items_accepted == 1
    assert outcome.items_rejected == 0
    assert outcome.errors == []


def test_validate_payload_missing_required_field() -> None:
    bad_payload = {"externalId": "acme/api"}  # missing required sourceSystem
    envelope = _envelope([_record("repository.v1", "acme/api", bad_payload)])
    outcome = validate_payload(json.dumps(envelope).encode())

    assert outcome.valid is False
    assert outcome.items_accepted == 0
    assert outcome.items_rejected == 1
    assert len(outcome.errors) == 1
    error = outcome.errors[0]
    assert set(error) == {"index", "kind", "code", "message", "path"}
    assert error["index"] == 0
    assert error["kind"] == "repository.v1"
    assert error["code"] == "missing_required_field"
    assert error["path"] == "records[0].payload.sourceSystem"


def test_validate_payload_unknown_kind() -> None:
    envelope = _envelope([_record("not_a_kind", "x", {})])
    outcome = validate_payload(json.dumps(envelope).encode())

    assert outcome.valid is False
    assert outcome.errors[0]["code"] == "unknown_kind"


def test_validate_payload_malformed_json_raises_parse_error() -> None:
    with pytest.raises(PayloadParseError):
        validate_payload(b"not json")


def test_validate_payload_malformed_envelope_raises_parse_error() -> None:
    with pytest.raises(PayloadParseError):
        validate_payload(json.dumps({"not": "an envelope"}).encode())


def test_cmd_validate_malformed_json_is_clean_exit_1(
    capsys: pytest.CaptureFixture[str], tmp_path
) -> None:
    """A malformed payload must not crash with a raw traceback -- exit 1
    with a JSON error shape when --json is set."""
    bad_file = tmp_path / "bad.json"
    bad_file.write_text("not json")
    ns = argparse.Namespace(payload=str(bad_file), json=True)

    exit_code = push_cli._cmd_validate(ns)

    assert exit_code == out.EXIT_DATA_FAILURE
    captured = capsys.readouterr()
    body = json.loads(captured.out)
    assert body["valid"] is False
    assert body["errors"][0]["code"] == "invalid_envelope"


class _FakeStdin:
    def __init__(self, data: bytes) -> None:
        self.buffer = io.BytesIO(data)


def test_cmd_validate_stdin(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    envelope = _envelope([_record("repository.v1", "acme/api", VALID_RECORD_PAYLOAD)])
    # pytest's captured sys.stdin (DontReadFromInput) exposes `.buffer` as a
    # read-only property -- replace the whole `sys.stdin` object rather than
    # trying to set the attribute on it.
    monkeypatch.setattr("sys.stdin", _FakeStdin(json.dumps(envelope).encode()))
    ns = argparse.Namespace(payload="-", json=True)

    exit_code = push_cli._cmd_validate(ns)

    assert exit_code == out.EXIT_OK
    body = json.loads(capsys.readouterr().out)
    assert body["valid"] is True
    assert body["itemsAccepted"] == 1


def test_cmd_validate_missing_file(capsys: pytest.CaptureFixture[str]) -> None:
    ns = argparse.Namespace(payload="/nonexistent/path/payload.json", json=False)

    exit_code = push_cli._cmd_validate(ns)

    assert exit_code == out.EXIT_USAGE_ERROR
    assert "cannot read payload" in capsys.readouterr().err


def test_cmd_validate_oversized_payload_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Codex adversarial-review finding: `_read_payload_arg` must bound the
    read against the (local, hardcoded-default) size limit rather than
    fully buffering an oversized file before rejecting it."""
    from dev_health_ops.push.limits import BatchLimits

    big_file = tmp_path / "big.json"
    big_file.write_text("x" * 1000)
    monkeypatch.setattr(
        "dev_health_ops.push.cli.DEFAULT_LIMITS",
        BatchLimits(max_records_per_batch=1000, max_body_bytes=10),
    )
    ns = argparse.Namespace(payload=str(big_file), json=True)

    exit_code = push_cli._cmd_validate(ns)

    assert exit_code == out.EXIT_DATA_FAILURE
    body = json.loads(capsys.readouterr().out)
    assert body["errors"][0]["code"] == "payload_too_large"
