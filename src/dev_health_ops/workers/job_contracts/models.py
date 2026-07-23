"""Typed, language-neutral job contract models for the Python transition."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, TypeAlias

CONTRACT_VERSION_V1 = 1
KIND_HEARTBEAT = "system.heartbeat"
KIND_RETENTION_CLEANUP = "system.retention_cleanup"
RETENTION_WORKER_TERMINAL = "worker_job_terminal"
MAX_ENVELOPE_BYTES = 16 * 1024


class ContractPayload(Protocol):
    """Minimum adapter surface required by a transitional Python producer."""

    KIND: ClassVar[str]
    CONTRACT_VERSION: ClassVar[int]
    DOMAIN_TYPE: ClassVar[str]


@dataclass(frozen=True, slots=True)
class DomainLink:
    """Reference to authoritative product or schedule state."""

    type: str
    id: str


@dataclass(frozen=True, slots=True)
class HeartbeatPayload:
    """Version 1 payload for the unique heartbeat occurrence."""

    KIND: ClassVar[str] = KIND_HEARTBEAT
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "schedule_occurrence"

    scheduled_for: str


@dataclass(frozen=True, slots=True)
class RetentionCleanupPayload:
    """Version 1 bounded terminal-job retention request."""

    KIND: ClassVar[str] = KIND_RETENTION_CLEANUP
    CONTRACT_VERSION: ClassVar[int] = CONTRACT_VERSION_V1
    DOMAIN_TYPE: ClassVar[str] = "maintenance_run"

    batch_size: int
    delete_before: str
    retention_policy: str


JobPayload: TypeAlias = HeartbeatPayload | RetentionCleanupPayload


@dataclass(frozen=True, slots=True)
class Envelope:
    """Strict common envelope carried inside River ``encoded_args``."""

    contract_version: int
    organization_id: str | None
    correlation_id: str
    idempotency_key: str
    domain: DomainLink
    payload: JobPayload
