from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class BatchJobStatus(str, Enum):
    CREATED = "created"
    SUBMITTING = "submitting"
    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class BatchItemStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PROVIDER_SUCCEEDED = "provider_succeeded"
    PROVIDER_FAILED = "provider_failed"
    VALIDATED = "validated"
    REPAIRING = "repairing"
    REPAIRED = "repaired"
    FALLBACK = "fallback"
    REUSED = "reused"
    FAILED = "failed"


class BatchProviderFeature(str, Enum):
    SUBMIT = "submit"
    POLL = "poll"
    FETCH_RESULTS = "fetch_results"
    CANCEL = "cancel"


@dataclass(frozen=True)
class BatchCapability:
    provider: str
    model: str
    supported: bool
    features: frozenset[BatchProviderFeature] = frozenset()
    reason: str = ""


@dataclass(frozen=True)
class BatchItemRequest:
    custom_id: str
    prompt: str
    response_format: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        custom_id = self.custom_id.strip()
        if not custom_id:
            raise ValueError("Batch item custom_id is required")
        if any(char.isspace() for char in custom_id):
            raise ValueError("Batch item custom_id must not contain whitespace")
        if not self.prompt:
            raise ValueError("Batch item prompt is required")
        object.__setattr__(self, "custom_id", custom_id)


@dataclass(frozen=True)
class BatchJobSubmission:
    provider_job_id: str
    provider: str
    model: str
    item_count: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.provider_job_id.strip():
            raise ValueError("provider_job_id is required")
        if self.item_count < 0:
            raise ValueError("item_count must be non-negative")


@dataclass(frozen=True)
class BatchJobState:
    provider_job_id: str
    status: BatchJobStatus
    total_count: int
    completed_count: int = 0
    failed_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.total_count < 0:
            raise ValueError("total_count must be non-negative")
        if self.completed_count < 0 or self.failed_count < 0:
            raise ValueError("completed_count and failed_count must be non-negative")


@dataclass(frozen=True)
class BatchItemResult:
    custom_id: str
    raw_response: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    provider_metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.error_code is None and self.raw_response is not None


@runtime_checkable
class BatchProvider(Protocol):
    def batch_capability(self, model: str | None = None) -> BatchCapability:
        raise NotImplementedError

    async def submit_batch(self, items: list[BatchItemRequest]) -> BatchJobSubmission:
        raise NotImplementedError

    async def poll_batch(self, provider_job_id: str) -> BatchJobState:
        raise NotImplementedError

    async def fetch_batch_results(self, provider_job_id: str) -> list[BatchItemResult]:
        raise NotImplementedError

    async def cancel_batch(self, provider_job_id: str) -> None:
        raise NotImplementedError


def unsupported_batch_capability(
    provider: str, model: str | None = None, *, reason: str = "unsupported"
) -> BatchCapability:
    return BatchCapability(
        provider=provider,
        model=model or "",
        supported=False,
        features=frozenset(),
        reason=reason,
    )


def batch_capability_for(provider: object, model: str | None = None) -> BatchCapability:
    capability = getattr(provider, "batch_capability", None)
    if callable(capability):
        result = capability(model)
        if isinstance(result, BatchCapability):
            return result
    provider_name = str(getattr(provider, "provider_name", type(provider).__name__))
    return unsupported_batch_capability(provider_name, model)
