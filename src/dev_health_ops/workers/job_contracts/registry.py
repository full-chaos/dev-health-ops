"""Registry and rolling-deployment capability adapters for Python producers."""

from __future__ import annotations

import hashlib
import sysconfig
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .codec import ContractDecodeError, load_json_document
from .models import (
    CONTRACT_VERSION_V1,
    CONTRACT_VERSION_V2,
    CONTRACT_VERSION_V3,
    KIND_BILLING_NOTIFICATION,
    KIND_DAILY_METRICS_DISPATCH,
    KIND_DAILY_METRICS_FINALIZE,
    KIND_DAILY_METRICS_PARTITION,
    KIND_HEARTBEAT,
    KIND_INVESTMENT_CHUNK,
    KIND_INVESTMENT_DISPATCH,
    KIND_INVESTMENT_FINALIZE,
    KIND_INVESTMENT_MATERIALIZE,
    KIND_REMAINING_CAPACITY,
    KIND_REMAINING_COMPLEXITY,
    KIND_REMAINING_DORA,
    KIND_REMAINING_MEMBERSHIP,
    KIND_REMAINING_RECOMMENDATIONS,
    KIND_REMAINING_RELEASE_IMPACT,
    KIND_REPORT_EXECUTE_ON_DEMAND,
    KIND_REPORT_EXECUTE_SCHEDULED,
    KIND_RETENTION_CLEANUP,
    KIND_SYNC_COVERAGE_REFRESH,
    KIND_SYNC_PROVIDER_UNIT,
    KIND_TEAM_AUTOIMPORT,
    KIND_TEAM_REPO_OWNERSHIP_DERIVATION,
    KIND_WEBHOOK_DELIVERY,
    KIND_WORK_GRAPH_BUILD,
)

_MAX_ARTIFACT_BYTES = 512 * 1024
_REGISTRY_JOB_FIELDS = {
    "kind",
    "current_version",
    "supported_versions",
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
    "required_queues",
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
    contracts: tuple[ContractCapability, ...]
    queues: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "queues", _sorted_unique_strings(self.queues))


@dataclass(frozen=True, slots=True)
class MigrationJob:
    kind: str
    producer_version: int
    required_queues: tuple[str, ...]
    route: str = "celery"

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "required_queues", _sorted_unique_strings(self.required_queues)
        )


def default_contract_root() -> Path:
    """Find the job contract tree in a checkout or installed distribution."""

    checkout_root = Path(__file__).resolve().parents[4] / "contracts" / "jobs" / "v1"
    if checkout_root.is_dir():
        return checkout_root
    return Path(sysconfig.get_path("data")) / "contracts" / "jobs" / "v1"


def load_registry(root: Path | None = None) -> Registry:
    contract_root = root or default_contract_root()
    document = _read_document(contract_root / "registry.json")
    if not isinstance(document, dict) or set(document) != {
        "schema_version",
        "contract_family",
        "envelope_schema",
        "version_policy",
        "jobs",
        "retired_kinds",
    }:
        raise ContractDecodeError("registry shape is invalid")
    retired_kinds = document["retired_kinds"]
    if not isinstance(retired_kinds, list):
        raise ContractDecodeError("registry retired_kinds is invalid")
    for retired in retired_kinds:
        if not isinstance(retired, dict) or set(retired) != {
            "kind",
            "retired_on",
            "reason",
            "ticket",
            "replacement",
        }:
            raise ContractDecodeError("registry retired kind must be an object")
        if not all(
            isinstance(retired[field], str) and retired[field] for field in retired
        ):
            raise ContractDecodeError(
                "registry retired kind is missing a required field"
            )
    if (
        document["schema_version"] != 1
        or document["contract_family"] != "dev-health.jobs"
        or document["envelope_schema"] != "envelope.schema.json"
    ):
        raise ContractDecodeError("registry identity is unsupported")
    if document["version_policy"] != {
        "compatibility": "additive_optional_only",
        "minimum_consumer_window": 2,
        "same_version_rollout": "schema_digest_all_live_queues",
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
        KIND_BILLING_NOTIFICATION: (CONTRACT_VERSION_V1,),
        KIND_WEBHOOK_DELIVERY: (CONTRACT_VERSION_V1,),
        KIND_REPORT_EXECUTE_ON_DEMAND: (CONTRACT_VERSION_V1,),
        KIND_REPORT_EXECUTE_SCHEDULED: (CONTRACT_VERSION_V1,),
        KIND_HEARTBEAT: (CONTRACT_VERSION_V1,),
        KIND_SYNC_COVERAGE_REFRESH: (CONTRACT_VERSION_V1,),
        KIND_RETENTION_CLEANUP: (
            CONTRACT_VERSION_V1,
            CONTRACT_VERSION_V2,
            CONTRACT_VERSION_V3,
        ),
        KIND_DAILY_METRICS_DISPATCH: (CONTRACT_VERSION_V1,),
        KIND_DAILY_METRICS_PARTITION: (CONTRACT_VERSION_V1,),
        KIND_DAILY_METRICS_FINALIZE: (CONTRACT_VERSION_V1,),
        KIND_INVESTMENT_CHUNK: (CONTRACT_VERSION_V1,),
        KIND_INVESTMENT_DISPATCH: (CONTRACT_VERSION_V1,),
        KIND_INVESTMENT_FINALIZE: (CONTRACT_VERSION_V1,),
        KIND_INVESTMENT_MATERIALIZE: (CONTRACT_VERSION_V1,),
        KIND_WORK_GRAPH_BUILD: (CONTRACT_VERSION_V1,),
        KIND_REMAINING_CAPACITY: (CONTRACT_VERSION_V1,),
        KIND_REMAINING_COMPLEXITY: (CONTRACT_VERSION_V1,),
        KIND_REMAINING_DORA: (CONTRACT_VERSION_V1,),
        KIND_REMAINING_MEMBERSHIP: (CONTRACT_VERSION_V1,),
        KIND_REMAINING_RECOMMENDATIONS: (CONTRACT_VERSION_V1,),
        KIND_REMAINING_RELEASE_IMPACT: (CONTRACT_VERSION_V1,),
        KIND_SYNC_PROVIDER_UNIT: (CONTRACT_VERSION_V1,),
        KIND_TEAM_AUTOIMPORT: (CONTRACT_VERSION_V1,),
        KIND_TEAM_REPO_OWNERSHIP_DERIVATION: (CONTRACT_VERSION_V1,),
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
        required_queues = raw.get("required_queues")
        if (
            not isinstance(required_queues, list)
            or not required_queues
            or not all(isinstance(queue, str) and queue for queue in required_queues)
        ):
            raise ContractDecodeError("required_queues is invalid")
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
                required_queues=tuple(required_queues),
                route=route,
            )
        )
    if len({job.kind for job in jobs}) != len(jobs):
        raise ContractDecodeError("migration state contains duplicate kinds")
    return tuple(jobs)


def capabilities_for_queues(
    registry: Registry, queues: tuple[str, ...] | list[str]
) -> CapabilityReport:
    if (
        not isinstance(queues, (list, tuple))
        or not queues
        or not all(isinstance(queue, str) and queue for queue in queues)
    ):
        raise ContractDecodeError("queues are invalid")
    queue_names = _sorted_unique_strings(queues)
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
        if contract.queue in queue_names
    )
    if not contracts:
        raise ContractDecodeError("queues have no registered contracts")
    return CapabilityReport(contracts=contracts, queues=queue_names)


def check_rollout_capabilities(
    jobs: tuple[MigrationJob, ...],
    reports: tuple[CapabilityReport, ...],
    expected_reports: tuple[CapabilityReport, ...],
) -> None:
    """Fail if any live report for a required queue lacks producer support."""

    by_queue: dict[str, list[CapabilityReport]] = {}
    for report in reports:
        for queue in report.queues:
            by_queue.setdefault(queue, []).append(report)
    expected_by_queue: dict[str, CapabilityReport] = {}
    for report in expected_reports:
        for queue in report.queues:
            expected_by_queue.setdefault(queue, report)
    for job in jobs:
        for queue in job.required_queues:
            expected = expected_by_queue.get(queue)
            if expected is None:
                raise ContractDecodeError("required queue has no expected capability")
            expected_digest = _find_digest(expected, job.kind, job.producer_version)
            if expected_digest is None:
                raise ContractDecodeError("expected capability lacks producer support")
            queue_reports = by_queue.get(queue, [])
            if not queue_reports:
                raise ContractDecodeError("required queue has no capability report")
            for report in queue_reports:
                if (
                    _find_digest(report, job.kind, job.producer_version)
                    != expected_digest
                ):
                    raise ContractDecodeError("live queue lacks producer support")


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


def _sorted_unique_strings(
    values: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(sorted({value for value in values if value}))


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
