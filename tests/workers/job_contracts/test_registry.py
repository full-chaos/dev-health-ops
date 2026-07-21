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
    capabilities_for_profile,
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


def test_rollout_requires_every_live_profile_report() -> None:
    registry = load_registry()
    migration_jobs = load_migration_jobs()
    current = capabilities_for_profile(registry, "ops")
    check_rollout_capabilities(migration_jobs, (current, current), (current,))

    stale = CapabilityReport(
        profile="ops",
        contracts=(
            ContractCapability(
                kind="system.heartbeat",
                versions=(2,),
                schema_digests=((2, "sha256:" + "a" * 64),),
            ),
            current.contracts[1],
        ),
    )
    with pytest.raises(ContractDecodeError, match="lacks producer support"):
        check_rollout_capabilities(migration_jobs, (current, stale), (current,))

    stale_digest = CapabilityReport(
        profile="ops",
        contracts=(
            ContractCapability(
                kind=current.contracts[0].kind,
                versions=current.contracts[0].versions,
                schema_digests=((1, "sha256:" + "0" * 64),),
            ),
            current.contracts[1],
        ),
    )
    with pytest.raises(ContractDecodeError, match="lacks producer support"):
        check_rollout_capabilities(migration_jobs, (current, stale_digest), (current,))
    with pytest.raises(ContractDecodeError, match="no capability report"):
        check_rollout_capabilities(migration_jobs, (), (current,))


def test_rolling_deployment_holds_producer_at_n_minus_one() -> None:
    current = capabilities_for_profile(load_registry(), "ops")
    digest = dict(current.contracts[0].schema_digests)[1]
    old_binary = CapabilityReport(
        profile="ops",
        contracts=(
            ContractCapability(
                kind="system.heartbeat",
                versions=(1,),
                schema_digests=((1, digest),),
            ),
        ),
    )
    new_binary = CapabilityReport(
        profile="ops",
        contracts=(
            ContractCapability(
                kind="system.heartbeat",
                versions=(1, 2),
                schema_digests=((1, digest), (2, digest)),
            ),
        ),
    )
    producer_n_minus_one = (
        MigrationJob(
            kind="system.heartbeat",
            producer_version=1,
            required_profiles=("ops",),
        ),
    )
    check_rollout_capabilities(
        producer_n_minus_one, (old_binary, new_binary), (new_binary,)
    )

    producer_n = (
        MigrationJob(
            kind="system.heartbeat",
            producer_version=2,
            required_profiles=("ops",),
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
