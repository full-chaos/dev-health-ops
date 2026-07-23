"""Registry and rolling-deployment capability adapters for Python producers."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .codec import ContractDecodeError, load_json_document
from .models import (
    CONTRACT_VERSION_V1,
    KIND_HEARTBEAT,
    KIND_REPORT_EXECUTE_ON_DEMAND,
    KIND_REPORT_EXECUTE_SCHEDULED,
    KIND_RETENTION_CLEANUP,
)

_MAX_ARTIFACT_BYTES = 512 * 1024
_REGISTRY_JOB_FIELDS = {
    "kind",
    "current_version",
    "supported_versions",
    "profile",
    "queue",
    "handler_owner",
    "execution_mode",
    "priority",
    "timeout_seconds",
    "max_attempts",
    "retry_policy",
    "cancellation",
    "delivery",
    "idempotency",
    "concurrency",
    "sensitive_fields",
    "domain_link",
    "organization_scope",
    "schema_versions",
    "fixtures",
}
_MIGRATION_JOB_FIELDS = {
    "kind",
    "state",
    "producer_version",
    "consumer_versions",
    "required_profiles",
    "route",
    "rollback_route",
    "evidence",
}
_MIGRATION_ROUTES = frozenset({"celery", "shadow", "river_canary", "river", "removed"})
_MIGRATION_ROLLBACK_ROUTES = frozenset({"celery", "river", "none"})
_MIGRATION_STATE_ROUTES = {
    "inventory": ("celery", "celery"),
    "contract_frozen": ("celery", "celery"),
    "go_implemented": ("celery", "celery"),
    "shadow": ("shadow", "celery"),
    "canary": ("river_canary", "celery"),
    "go_default": ("river", "celery"),
    "celery_fallback_only": ("river", "celery"),
    "celery_removed": ("river", "none"),
}


@dataclass(frozen=True, slots=True)
class RegisteredContract:
    kind: str
    current_version: int
    supported_versions: tuple[int, ...]
    profile: str
    queue: str
    priority: int
    max_attempts: int
    organization_scope: str
    schema_versions: tuple[tuple[int, str], ...]


@dataclass(frozen=True, slots=True)
class Registry:
    root: Path
    contracts: tuple[RegisteredContract, ...]

    def by_kind(self, kind: str) -> RegisteredContract:
        for contract in self.contracts:
            if contract.kind == kind:
                return contract
        raise ContractDecodeError("unknown registry kind")


@dataclass(frozen=True, slots=True)
class ContractCapability:
    kind: str
    versions: tuple[int, ...]
    schema_digests: tuple[tuple[int, str], ...]


@dataclass(frozen=True, slots=True)
class CapabilityReport:
    profile: str
    contracts: tuple[ContractCapability, ...]


@dataclass(frozen=True, slots=True)
class MigrationJob:
    kind: str
    producer_version: int
    required_profiles: tuple[str, ...]
    route: str = "celery"


def default_contract_root() -> Path:
    """Find the repository contract tree from a source checkout."""

    return Path(__file__).resolve().parents[4] / "contracts" / "jobs" / "v1"


def load_registry(root: Path | None = None) -> Registry:
    contract_root = root or default_contract_root()
    document = _read_document(contract_root / "registry.json")
    if not isinstance(document, dict) or set(document) != {
        "schema_version",
        "contract_family",
        "envelope_schema",
        "version_policy",
        "jobs",
    }:
        raise ContractDecodeError("registry shape is invalid")
    if (
        document["schema_version"] != 1
        or document["contract_family"] != "dev-health.jobs"
        or document["envelope_schema"] != "envelope.schema.json"
    ):
        raise ContractDecodeError("registry identity is unsupported")
    if document["version_policy"] != {
        "compatibility": "additive_optional_only",
        "minimum_consumer_window": 2,
        "same_version_rollout": "schema_digest_all_live_profiles",
    }:
        raise ContractDecodeError("registry version policy is unsupported")
    jobs = document["jobs"]
    if not isinstance(jobs, list) or not jobs:
        raise ContractDecodeError("registry jobs are missing")

    contracts: list[RegisteredContract] = []
    for raw in jobs:
        if not isinstance(raw, dict) or set(raw) != _REGISTRY_JOB_FIELDS:
            raise ContractDecodeError("registry job must be an object")
        kind = _required_string(raw, "kind")
        current = _required_int(raw, "current_version")
        supported = _version_tuple(raw.get("supported_versions"))
        _validate_version_window(current, supported)
        profile = _required_string(raw, "profile")
        queue = _required_string(raw, "queue")
        priority = _required_int(raw, "priority")
        max_attempts = _required_int(raw, "max_attempts")
        if not 1 <= priority <= 4 or not 1 <= max_attempts <= 25:
            raise ContractDecodeError("registry insertion policy is invalid")
        scope = _required_string(raw, "organization_scope")
        schemas = raw.get("schema_versions")
        if not isinstance(schemas, dict):
            raise ContractDecodeError("schema_versions is invalid")
        if not all(
            isinstance(version, str) and version.isdigit() and int(version) > 0
            for version in schemas
        ):
            raise ContractDecodeError("schema_versions keys are invalid")
        schema_versions = tuple(
            (int(version), _required_string(schemas, version))
            for version in sorted(schemas, key=int)
        )
        schema_map = dict(schema_versions)
        for version in supported:
            relative = schema_map.get(version)
            if relative is None:
                raise ContractDecodeError("supported version has no schema")
            _read_contract_artifact(contract_root, relative)
        contracts.append(
            RegisteredContract(
                kind=kind,
                current_version=current,
                supported_versions=supported,
                profile=profile,
                queue=queue,
                priority=priority,
                max_attempts=max_attempts,
                organization_scope=scope,
                schema_versions=schema_versions,
            )
        )
    if tuple(contract.kind for contract in contracts) != tuple(
        sorted(contract.kind for contract in contracts)
    ):
        raise ContractDecodeError("registry jobs are not sorted")
    if len({contract.kind for contract in contracts}) != len(contracts):
        raise ContractDecodeError("registry contains duplicate kinds")

    expected = {
        KIND_REPORT_EXECUTE_ON_DEMAND: (CONTRACT_VERSION_V1,),
        KIND_REPORT_EXECUTE_SCHEDULED: (CONTRACT_VERSION_V1,),
        KIND_HEARTBEAT: (CONTRACT_VERSION_V1,),
        KIND_RETENTION_CLEANUP: (CONTRACT_VERSION_V1,),
    }
    if {
        contract.kind: contract.supported_versions for contract in contracts
    } != expected:
        raise ContractDecodeError("registry drifts from Python contract types")
    return Registry(root=contract_root, contracts=tuple(contracts))


def load_migration_jobs(root: Path | None = None) -> tuple[MigrationJob, ...]:
    contract_root = root or default_contract_root()
    document = _read_document(contract_root / "migration-state.json")
    if not isinstance(document, dict) or set(document) != {"schema_version", "jobs"}:
        raise ContractDecodeError("migration state shape is invalid")
    if document["schema_version"] != 1:
        raise ContractDecodeError("migration state version is unsupported")
    raw_jobs = document["jobs"]
    if not isinstance(raw_jobs, list):
        raise ContractDecodeError("migration jobs must be an array")
    jobs: list[MigrationJob] = []
    for raw in raw_jobs:
        if not isinstance(raw, dict) or set(raw) != _MIGRATION_JOB_FIELDS:
            raise ContractDecodeError("migration job must be an object")
        profiles = raw.get("required_profiles")
        if not isinstance(profiles, list) or not all(
            isinstance(profile, str) and profile for profile in profiles
        ):
            raise ContractDecodeError("required_profiles is invalid")
        route = _required_string(raw, "route")
        rollback_route = _required_string(raw, "rollback_route")
        state = _required_string(raw, "state")
        if (
            route not in _MIGRATION_ROUTES
            or rollback_route not in _MIGRATION_ROLLBACK_ROUTES
        ):
            raise ContractDecodeError("migration route is unsupported")
        if _MIGRATION_STATE_ROUTES.get(state) != (route, rollback_route):
            raise ContractDecodeError("migration state route is inconsistent")
        jobs.append(
            MigrationJob(
                kind=_required_string(raw, "kind"),
                producer_version=_required_int(raw, "producer_version"),
                required_profiles=tuple(profiles),
                route=route,
            )
        )
    if len({job.kind for job in jobs}) != len(jobs):
        raise ContractDecodeError("migration state contains duplicate kinds")
    return tuple(jobs)


def capabilities_for_profile(registry: Registry, profile: str) -> CapabilityReport:
    contracts = tuple(
        ContractCapability(
            kind=contract.kind,
            versions=contract.supported_versions,
            schema_digests=tuple(
                (
                    version,
                    "sha256:"
                    + _contract_schema_digest(
                        _read_contract_artifact(registry.root, "envelope.schema.json"),
                        _read_contract_artifact(
                            registry.root,
                            dict(contract.schema_versions)[version],
                        ),
                    ),
                )
                for version in contract.supported_versions
            ),
        )
        for contract in registry.contracts
        if contract.profile == profile
    )
    if not contracts:
        raise ContractDecodeError("profile has no registered contracts")
    return CapabilityReport(profile=profile, contracts=contracts)


def check_rollout_capabilities(
    jobs: tuple[MigrationJob, ...],
    reports: tuple[CapabilityReport, ...],
    expected_reports: tuple[CapabilityReport, ...],
) -> None:
    """Fail if any live report for a required profile lacks producer support."""

    by_profile: dict[str, list[CapabilityReport]] = {}
    for report in reports:
        by_profile.setdefault(report.profile, []).append(report)
    expected_by_profile = {report.profile: report for report in expected_reports}
    for job in jobs:
        for profile in job.required_profiles:
            expected = expected_by_profile.get(profile)
            if expected is None:
                raise ContractDecodeError("required profile has no expected capability")
            expected_digest = _find_digest(expected, job.kind, job.producer_version)
            if expected_digest is None:
                raise ContractDecodeError("expected capability lacks producer support")
            profile_reports = by_profile.get(profile, [])
            if not profile_reports:
                raise ContractDecodeError("required profile has no capability report")
            for report in profile_reports:
                if (
                    _find_digest(report, job.kind, job.producer_version)
                    != expected_digest
                ):
                    raise ContractDecodeError("live profile lacks producer support")


def _find_digest(report: CapabilityReport, kind: str, version: int) -> str | None:
    for capability in report.contracts:
        if capability.kind == kind and version in capability.versions:
            return dict(capability.schema_digests).get(version)
    return None


def _validate_version_window(current: int, supported: tuple[int, ...]) -> None:
    if current < 1 or not supported or supported != tuple(sorted(set(supported))):
        raise ContractDecodeError(
            "supported versions must be sorted unique positive integers"
        )
    if any(version < 1 for version in supported) or current not in supported:
        raise ContractDecodeError("current version is not supported")
    if current > 1 and current - 1 not in supported:
        raise ContractDecodeError("N-1 version is not supported")


def _read_document(path: Path) -> Any:
    if path.is_symlink() or not path.is_file():
        raise ContractDecodeError("contract artifact must be a regular file")
    return load_json_document(path.read_bytes(), max_bytes=_MAX_ARTIFACT_BYTES)


def _read_contract_artifact(root: Path, relative: str) -> bytes:
    relative_path = Path(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise ContractDecodeError("contract path escapes root")
    root_resolved = root.resolve()
    path = root_resolved / relative_path
    if path.is_symlink() or not path.is_file():
        raise ContractDecodeError("contract artifact must be a regular file")
    try:
        path.resolve().relative_to(root_resolved)
    except ValueError as error:
        raise ContractDecodeError("contract path escapes root") from error
    data = path.read_bytes()
    if len(data) > _MAX_ARTIFACT_BYTES:
        raise ContractDecodeError("contract artifact exceeds size limit")
    return data


def _contract_schema_digest(envelope_schema: bytes, payload_schema: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(envelope_schema)
    digest.update(b"\x00")
    digest.update(payload_schema)
    return digest.hexdigest()


def _required_string(document: dict[str, Any], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise ContractDecodeError("registry string field is invalid")
    return value


def _required_int(document: dict[str, Any], key: str) -> int:
    value = document.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ContractDecodeError("registry integer field is invalid")
    return value


def _version_tuple(value: Any) -> tuple[int, ...]:
    if not isinstance(value, list) or not all(
        isinstance(version, int) and not isinstance(version, bool) for version in value
    ):
        raise ContractDecodeError("supported_versions is invalid")
    return tuple(value)
