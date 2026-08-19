from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from dev_health_ops.workers.job_contracts import (
    CapabilityReport,
    ContractCapability,
    ContractDecodeError,
    MigrationJob,
    capabilities_for_queues,
    check_rollout_capabilities,
    default_contract_root,
    load_migration_jobs,
    load_registry,
)
from dev_health_ops.workers.job_contracts.registry import _validate_version_window


def test_registry_enforces_n_and_n_minus_one() -> None:
    _validate_version_window(1, (1,))
    _validate_version_window(2, (1, 2))
    with pytest.raises(ContractDecodeError, match="N-1"):
        _validate_version_window(2, (2,))


def test_python_registry_rejects_envelope_schema_drift(tmp_path: Path) -> None:
    candidate = tmp_path / "v1"
    shutil.copytree(default_contract_root(), candidate)
    registry_path = candidate / "registry.json"
    document = json.loads(registry_path.read_text())
    document["envelope_schema"] = "schemas/system.heartbeat.v1.schema.json"
    registry_path.write_text(json.dumps(document))

    with pytest.raises(ContractDecodeError, match="identity"):
        load_registry(candidate)


def test_migration_registry_rejects_unknown_route(tmp_path: Path) -> None:
    candidate = tmp_path / "v1"
    shutil.copytree(default_contract_root(), candidate)
    state_path = candidate / "migration-state.json"
    document = json.loads(state_path.read_text())
    document["jobs"][0]["route"] = "environment_override"
    state_path.write_text(json.dumps(document))

    with pytest.raises(ContractDecodeError, match="route is unsupported"):
        load_migration_jobs(candidate)


def test_migration_registry_accepts_terminal_rollback_route(tmp_path: Path) -> None:
    candidate = tmp_path / "v1"
    shutil.copytree(default_contract_root(), candidate)
    state_path = candidate / "migration-state.json"
    document = json.loads(state_path.read_text())
    document["jobs"][0].update(
        {"state": "celery_removed", "route": "river", "rollback_route": "none"}
    )
    state_path.write_text(json.dumps(document))

    jobs = load_migration_jobs(candidate)

    assert jobs[0].route == "river"


def test_migration_registry_rejects_state_route_mismatch(tmp_path: Path) -> None:
    candidate = tmp_path / "v1"
    shutil.copytree(default_contract_root(), candidate)
    state_path = candidate / "migration-state.json"
    document = json.loads(state_path.read_text())
    document["jobs"][0]["route"] = "shadow"
    state_path.write_text(json.dumps(document))

    with pytest.raises(ContractDecodeError, match="state route is inconsistent"):
        load_migration_jobs(candidate)


def test_python_registry_rejects_profile_field(tmp_path: Path) -> None:
    candidate = tmp_path / "v1"
    shutil.copytree(default_contract_root(), candidate)
    registry_path = candidate / "registry.json"
    document = json.loads(registry_path.read_text())
    document["jobs"][0]["profile"] = "ops"
    registry_path.write_text(json.dumps(document))

    with pytest.raises(ContractDecodeError, match="registry job must be an object"):
        load_registry(candidate)


def test_migration_registry_rejects_required_profiles_field(tmp_path: Path) -> None:
    candidate = tmp_path / "v1"
    shutil.copytree(default_contract_root(), candidate)
    state_path = candidate / "migration-state.json"
    document = json.loads(state_path.read_text())
    document["jobs"][0]["required_profiles"] = ["ops"]
    state_path.write_text(json.dumps(document))

    with pytest.raises(ContractDecodeError, match="migration job must be an object"):
        load_migration_jobs(candidate)


def test_capability_reports_canonicalize_queue_claims() -> None:
    report = CapabilityReport(
        contracts=(),
        queues=("sync", "ops", "sync"),
    )

    assert report.queues == ("ops", "sync")


def test_rollout_requires_every_live_queue_report() -> None:
    registry = load_registry()
    current = capabilities_for_queues(
        registry,
        ("coverage", "heartbeat", "retention", "webhooks"),
    )
    heavy = capabilities_for_queues(
        registry,
        ("investment", "metrics", "reports", "workgraph"),
    )
    sync = capabilities_for_queues(registry, ("sync", "sync_provider"))
    overlapping = capabilities_for_queues(
        registry,
        ("retention", "heartbeat", "coverage", "heartbeat"),
    )
    heartbeat_job = (
        MigrationJob(
            kind="system.heartbeat",
            producer_version=1,
            required_queues=("heartbeat",),
        ),
    )
    assert overlapping.queues == ("coverage", "heartbeat", "retention")
    assert current.queues == ("coverage", "heartbeat", "retention", "webhooks")
    assert heavy.queues == ("investment", "metrics", "reports", "workgraph")
    assert sync.queues == ("sync", "sync_provider")
    with pytest.raises(
        ContractDecodeError, match="required queue has no capability report"
    ):
        check_rollout_capabilities(
            heartbeat_job,
            (CapabilityReport(contracts=current.contracts, queues=("retention",)),),
            (current,),
        )
    check_rollout_capabilities(
        heartbeat_job,
        (current, heavy, sync),
        (current, heavy, sync),
    )

    stale = CapabilityReport(
        contracts=(
            ContractCapability(
                kind="system.heartbeat",
                versions=(2,),
                schema_digests=((2, "sha256:" + "a" * 64),),
            ),
        ),
        queues=("heartbeat",),
    )
    with pytest.raises(ContractDecodeError, match="lacks producer support"):
        check_rollout_capabilities(
            heartbeat_job,
            (current, stale),
            (current,),
        )

    stale_digest = CapabilityReport(
        contracts=(
            ContractCapability(
                kind="system.heartbeat",
                versions=(1,),
                schema_digests=((1, "sha256:" + "0" * 64),),
            ),
        ),
        queues=("heartbeat",),
    )
    with pytest.raises(ContractDecodeError, match="lacks producer support"):
        check_rollout_capabilities(
            heartbeat_job,
            (current, stale_digest),
            (current,),
        )
    with pytest.raises(ContractDecodeError, match="no capability report"):
        check_rollout_capabilities(heartbeat_job, (), (current,))


def test_rolling_deployment_holds_producer_at_n_minus_one() -> None:
    current = capabilities_for_queues(
        load_registry(),
        ("coverage", "heartbeat", "retention", "webhooks"),
    )
    heartbeat = next(
        contract
        for contract in current.contracts
        if contract.kind == "system.heartbeat"
    )
    digest = dict(heartbeat.schema_digests)[1]
    old_binary = CapabilityReport(
        contracts=(
            ContractCapability(
                kind="system.heartbeat",
                versions=(1,),
                schema_digests=((1, digest),),
            ),
        ),
        queues=("heartbeat",),
    )
    new_binary = CapabilityReport(
        contracts=(
            ContractCapability(
                kind="system.heartbeat",
                versions=(1, 2),
                schema_digests=((1, digest), (2, digest)),
            ),
        ),
        queues=("heartbeat",),
    )
    producer_n_minus_one = (
        MigrationJob(
            kind="system.heartbeat",
            producer_version=1,
            required_queues=("heartbeat",),
        ),
    )
    check_rollout_capabilities(
        producer_n_minus_one, (old_binary, new_binary), (new_binary,)
    )

    producer_n = (
        MigrationJob(
            kind="system.heartbeat",
            producer_version=2,
            required_queues=("heartbeat",),
        ),
    )
    with pytest.raises(ContractDecodeError, match="lacks producer support"):
        check_rollout_capabilities(producer_n, (old_binary, new_binary), (new_binary,))
    check_rollout_capabilities(producer_n, (new_binary,), (new_binary,))


def test_contract_artifacts_contain_no_secret_or_raw_payload_fields() -> None:
    forbidden_keys = {
        "access_token",
        "api_key",
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "database_url",
        "dsn",
        "headers",
        "password",
        "private_key",
        "provider_payload",
        "raw_payload",
        "secret",
        "sql",
        "token",
        "webhook_body",
    }
    forbidden_values = (
        "postgres://",
        "postgresql://",
        "redis://",
        "valkey://",
        "bearer ",
        "-----begin",
        "password=",
    )
    root = default_contract_root()
    for path in sorted((root / "examples").glob("*.json")):
        document = json.loads(path.read_text())
        _assert_safe(document, forbidden_keys, forbidden_values, path)


def _assert_safe(
    value: object,
    forbidden_keys: set[str],
    forbidden_values: tuple[str, ...],
    path: Path,
) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            assert key.lower() not in forbidden_keys, f"{path}: forbidden key {key}"
            _assert_safe(child, forbidden_keys, forbidden_values, path)
    elif isinstance(value, list):
        for child in value:
            _assert_safe(child, forbidden_keys, forbidden_values, path)
    elif isinstance(value, str):
        lowered = value.lower()
        assert not any(marker in lowered for marker in forbidden_values), path
