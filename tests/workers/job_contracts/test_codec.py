from __future__ import annotations

import json
from pathlib import Path

import pytest

from dev_health_ops.workers.job_contracts import (
    KIND_HEARTBEAT,
    KIND_REPORT_EXECUTE_ON_DEMAND,
    KIND_REPORT_EXECUTE_SCHEDULED,
    KIND_RETENTION_CLEANUP,
    MAX_ENVELOPE_BYTES,
    RETENTION_WORKER_TERMINAL,
    ContractDecodeError,
    HeartbeatPayload,
    OnDemandReportExecutionPayload,
    RetentionCleanupPayload,
    ScheduledReportExecutionPayload,
    build_envelope,
    decode_envelope,
    default_contract_root,
    encode_envelope,
    load_registry,
)


@pytest.mark.parametrize(
    ("kind", "fixture", "expected_payload"),
    [
        (
            KIND_HEARTBEAT,
            "system.heartbeat.v1.json",
            HeartbeatPayload(scheduled_for="2026-07-21T12:00:00Z"),
        ),
        (
            KIND_RETENTION_CLEANUP,
            "system.retention_cleanup.v1.json",
            RetentionCleanupPayload(
                batch_size=250,
                delete_before="2026-07-14T12:00:00Z",
                retention_policy=RETENTION_WORKER_TERMINAL,
            ),
        ),
        (
            KIND_REPORT_EXECUTE_ON_DEMAND,
            "report.execute_on_demand.v1.json",
            OnDemandReportExecutionPayload(
                report_id="00000000-0000-4000-8000-000000000002"
            ),
        ),
        (
            KIND_REPORT_EXECUTE_SCHEDULED,
            "report.execute_scheduled.v1.json",
            ScheduledReportExecutionPayload(
                report_id="00000000-0000-4000-8000-000000000004"
            ),
        ),
    ],
)
def test_go_and_python_share_canonical_golden_fixtures(
    kind: str, fixture: str, expected_payload: object
) -> None:
    data = (default_contract_root() / "examples" / fixture).read_bytes()
    envelope = decode_envelope(kind, data)
    assert envelope.payload == expected_payload
    assert encode_envelope(envelope) == data


def test_registry_fixtures_all_cross_decode() -> None:
    root = default_contract_root()
    registry_document = json.loads((root / "registry.json").read_text())
    registered = load_registry(root)
    assert len(registry_document["jobs"]) == len(registered.contracts)
    for job in registry_document["jobs"]:
        for version, fixtures in job["fixtures"].items():
            assert int(version) in registered.by_kind(job["kind"]).supported_versions
            for fixture in fixtures:
                data = (root / fixture).read_bytes()
                assert encode_envelope(decode_envelope(job["kind"], data)) == data


def test_transitional_producer_adapter_uses_same_validation() -> None:
    envelope = build_envelope(
        HeartbeatPayload(scheduled_for="2026-07-21T12:00:00Z"),
        correlation_id="job-heartbeat-0001",
        idempotency_key="heartbeat:2026-07-21T12:00:00Z",
        domain_id="00000000-0000-4000-8000-000000000001",
    )
    assert (
        encode_envelope(envelope)
        == (default_contract_root() / "examples/system.heartbeat.v1.json").read_bytes()
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update({"contract_version": 2}),
        lambda value: value.update({"extra": True}),
        lambda value: value["payload"].update({"extra": True}),
        lambda value: value.update(
            {"organization_id": "00000000-0000-4000-8000-000000000009"}
        ),
        lambda value: value.update({"correlation_id": "unsafe value\n"}),
        lambda value: value["domain"].update({"type": "other"}),
        lambda value: value["payload"].update(
            {"scheduled_for": "2026-07-21T12:00:00-07:00"}
        ),
        lambda value: value["payload"].update(
            {"scheduled_for": "2026-07-21 12:00:00Z"}
        ),
    ],
)
def test_decoder_rejects_unknown_or_unsafe_fields(mutate: object) -> None:
    fixture = default_contract_root() / "examples/system.heartbeat.v1.json"
    document = json.loads(fixture.read_text())
    mutate(document)  # type: ignore[operator]
    with pytest.raises(ContractDecodeError):
        decode_envelope(KIND_HEARTBEAT, json.dumps(document))


@pytest.mark.parametrize(
    "payload",
    [
        '{"contract_version":1,"contract_version":1}',
        "{} {}",
        '{"contract_version":NaN}',
        b"\xff",
    ],
)
def test_decoder_rejects_ambiguous_json(payload: bytes | str) -> None:
    with pytest.raises(ContractDecodeError):
        decode_envelope(KIND_HEARTBEAT, payload)


def test_decoder_rejects_unknown_kind_and_oversized_args() -> None:
    fixture = (
        default_contract_root() / "examples/system.heartbeat.v1.json"
    ).read_bytes()
    with pytest.raises(ContractDecodeError):
        decode_envelope("system.not_registered", fixture)
    with pytest.raises(ContractDecodeError):
        decode_envelope(KIND_HEARTBEAT, b" " * (MAX_ENVELOPE_BYTES + 1))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("batch_size", 0),
        ("batch_size", 1001),
        ("delete_before", "not-a-time"),
        ("retention_policy", "all_rows"),
    ],
)
def test_retention_payload_is_bounded(field: str, value: object) -> None:
    path = default_contract_root() / "examples/system.retention_cleanup.v1.json"
    document = json.loads(path.read_text())
    document["payload"][field] = value
    with pytest.raises(ContractDecodeError):
        decode_envelope(KIND_RETENTION_CLEANUP, json.dumps(document))


def test_default_contract_root_points_to_regular_artifacts() -> None:
    root = default_contract_root()
    assert isinstance(root, Path)
    assert (root / "registry.json").is_file()
