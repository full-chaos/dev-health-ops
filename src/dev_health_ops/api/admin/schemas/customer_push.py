"""Pydantic schemas for the customer-push admin API (CHAOS-2696).

snake_case field naming, matching every other admin schema module (see
Design Decision 15 in docs/architecture/customer-push-authz.md) -- this is a
deliberate divergence from the camelCase data-plane batch envelope.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from dev_health_ops.models.ingest_auth import IngestTokenScope, IngestWebhookMode

_ALL_WEBHOOK_MODES = {mode.value for mode in IngestWebhookMode}
_VALID_SCOPES = {scope.value for scope in IngestTokenScope}


def _validate_webhook_mode(value: str | None) -> str | None:
    """Schema/type-layer check ONLY -- accepts the full 3-value enum.

    adr-004's must-not-foreclose contract is a deliberate two-layer design:
    the Pydantic type here must accept ``fullchaos_hosted`` (no 422) so the
    field never needs a breaking schema change to add it back later; the
    admin router's business-logic layer (``_reject_fullchaos_hosted_webhook_mode``
    in ``api/admin/routers/customer_push.py``) is what actually 400s that
    value before it's persisted or acted on. Do not narrow this to the
    2-value v1-supported subset.
    """
    if value is None:
        return value
    if value not in _ALL_WEBHOOK_MODES:
        raise ValueError(f"webhook_mode must be one of {sorted(_ALL_WEBHOOK_MODES)}")
    return value


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


class IngestSourceCreate(BaseModel):
    system: str = Field(..., min_length=1)
    instance: str = Field(..., min_length=1)
    entity_family: str = Field(default="legacy", min_length=1, max_length=255)
    display_name: str | None = None
    mode: str = "customer_push"
    webhook_mode: str = "disabled"

    @field_validator("instance")
    @classmethod
    def _check_instance(cls, value: str) -> str:
        # Trim before storage/matching -- an un-normalized "acme/api " would
        # both create a distinct (org_id, system, instance) row AND silently
        # bypass the CC5 ownership match against the trimmed managed-source
        # value, defeating the one-active-owner 409.
        normalized = value.strip()
        if not normalized:
            raise ValueError("instance must not be blank")
        return normalized

    @field_validator("webhook_mode")
    @classmethod
    def _check_webhook_mode(cls, value: str) -> str:
        result = _validate_webhook_mode(value)
        assert result is not None
        return result


class IngestSourceResponse(BaseModel):
    id: str
    org_id: str
    system: str
    instance: str
    entity_family: str
    display_name: str | None
    mode: str
    enabled: bool
    webhook_mode: str
    matched_integration_source_id: str | None
    created_at: datetime
    updated_at: datetime
    # Decision 8 non-blocking managed-sync-conflict warning; empty on plain reads.
    warnings: list[str] = []

    model_config = ConfigDict(from_attributes=True)


class IngestSourcePatch(BaseModel):
    display_name: str | None = None
    mode: str | None = None
    enabled: bool | None = None
    webhook_mode: str | None = None

    @field_validator("webhook_mode")
    @classmethod
    def _check_webhook_mode(cls, value: str | None) -> str | None:
        return _validate_webhook_mode(value)


# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------


class IngestTokenCreate(BaseModel):
    name: str = Field(..., min_length=1)
    scopes: list[str] = Field(..., min_length=1)
    expires_at: datetime | None = None

    @field_validator("scopes")
    @classmethod
    def _check_scopes(cls, value: list[str]) -> list[str]:
        unknown = sorted(set(value) - _VALID_SCOPES)
        if unknown:
            raise ValueError(
                f"Unknown scope(s) {unknown}; valid scopes are {sorted(_VALID_SCOPES)}"
            )
        return value


class IngestTokenCreateResponse(BaseModel):
    id: str
    org_id: str
    source_id: str | None
    name: str
    token: str  # PLAINTEXT -- present only in this one response, never again
    token_prefix: str
    scopes: list[str]
    expires_at: datetime | None
    created_at: datetime


class IngestTokenResponse(BaseModel):
    """List/detail view -- deliberately has no ``token`` field."""

    id: str
    org_id: str
    source_id: str | None
    name: str
    token_prefix: str
    scopes: list[str]
    expires_at: datetime | None
    revoked_at: datetime | None
    last_used_at: datetime | None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# ---------------------------------------------------------------------------
# Batches (CHAOS-2694) -- admin-plane read proxies over the same status.py
# store the data-plane GET /api/v1/external-ingest/batches* endpoints use.
# snake_case (admin convention), NOT the data-plane's camelCase. Deliberately
# does NOT surface recompute_status in v1 (CHAOS-2699 adds that in wave 3,
# master-spec CC21).
# ---------------------------------------------------------------------------


class AdminRejectedRecordResponse(BaseModel):
    index: int
    kind: str
    external_id: str | None
    code: str
    message: str
    path: str | None


class AdminBatchResponse(BaseModel):
    ingestion_id: str
    org_id: str
    status: str
    attempts: int
    source_system: str
    source_instance: str
    producer: str | None
    producer_version: str | None
    schema_version: str
    window_started_at: datetime | None
    window_ended_at: datetime | None
    items_received: int
    items_accepted: int
    items_rejected: int
    record_counts: dict | None
    error_summary: dict | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    rejected_records: list[AdminRejectedRecordResponse]
    rejected_records_total: int
    rejected_records_limit: int
    rejected_records_offset: int


class AdminBatchListItemResponse(BaseModel):
    ingestion_id: str
    status: str
    source_system: str
    source_instance: str
    producer: str | None
    items_received: int
    items_accepted: int
    items_rejected: int
    created_at: datetime
    completed_at: datetime | None


class AdminBatchListResponse(BaseModel):
    items: list[AdminBatchListItemResponse]
    total: int
    limit: int
    offset: int


class AdminValidateResponse(BaseModel):
    """POST .../sources/{id}/validate (CHAOS-2695, master-spec CC25).

    snake_case to match the admin-plane convention and the web client's
    ``CustomerPushValidateResponse`` (dev-health-web ``lib/admin/types.ts``)
    -- the data-plane ``ValidationResponse`` is the camelCase twin.
    Envelope-level failures (malformed JSON/envelope, wrong schemaVersion,
    oversized batch) are ALSO reported through this 200 shape (``valid:
    false`` + synthetic error rows), never as 4xx: the console panel renders
    these rows as results, and the web mock contract
    (dev-health-web tests/mocks/handlers.ts) pinned that behavior before
    this endpoint landed.
    """

    valid: bool
    items_accepted: int
    items_rejected: int
    errors: list[AdminRejectedRecordResponse]
