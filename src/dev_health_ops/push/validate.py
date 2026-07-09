"""Local, offline batch validation for `dev-hops push` (CHAOS-2700 decision 3).

Imports the server's own Pydantic models (``schemas.BatchEnvelope``) and its
deep per-record validator (``dev_health_ops.external_ingest.validate.
validate_records``, CHAOS-2691, master-spec CC17 -- single owner, imported
UNCHANGED) rather than re-implementing validation. This is genuinely
offline (Pydantic validation has no I/O) and can never drift from what
``POST /api/v1/external-ingest/validate`` enforces server-side, since both
call the exact same function.

Two entry points, matching the two places the CLI needs "is this batch OK":

* ``validate_payload`` -- full deep validation for `push validate`,
  mirroring ``POST /validate`` exactly: envelope parse -> schema-version
  check -> batch-size check -> per-record ``RECORD_KIND_MODELS[kind]``
  validation (CC29's "Offline validate = envelope parse + per-record
  validation" pin).
* ``check_envelope_shape`` -- the SHALLOWER pre-flight `push batch` runs
  before a network call, mirroring what ``POST /batches`` itself checks
  (envelope parse, schema version, batch size, unknown-kind) and
  deliberately NOT per-record field validation -- a batch with some
  invalid records is still legitimately submittable in v1 (the worker
  reports those as a ``partial`` status with per-record rejections;
  hard-blocking it locally would prevent customers from ever exercising
  that normal partial-success path).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from dev_health_ops.api.external_ingest.schemas import (
    RECORD_KIND_MODELS,
    SCHEMA_VERSION,
    BatchEnvelope,
)
from dev_health_ops.external_ingest.validate import validate_records

from .limits import DEFAULT_LIMITS, BatchLimits


class PayloadParseError(Exception):
    """Raised for a failure with no per-record structure to report against:
    malformed JSON, or a batch envelope that doesn't even parse (missing
    required top-level fields, wrong types, unknown top-level keys)."""

    def __init__(
        self, message: str, *, errors: list[dict[str, Any]] | None = None
    ) -> None:
        super().__init__(message)
        self.errors = errors or []


@dataclass(frozen=True)
class ValidationOutcome:
    valid: bool
    items_accepted: int
    items_rejected: int
    errors: list[dict[str, Any]]


def _load_json(raw: bytes) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PayloadParseError(f"Malformed JSON: {exc}") from exc


def _envelope_validation_error_items(exc: ValidationError) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for err in exc.errors():
        path = ".".join(str(part) for part in err["loc"])
        items.append(
            {
                "index": -1,
                "kind": None,
                "code": "invalid_envelope",
                "message": err["msg"],
                "path": path or None,
            }
        )
    return items


def parse_envelope(raw: bytes) -> BatchEnvelope:
    """Parse+validate the batch envelope shape only (no per-record deep
    validation). Raises ``PayloadParseError`` with CC16-shaped diagnostic
    items on any failure."""
    data = _load_json(raw)
    try:
        return BatchEnvelope.model_validate(data)
    except ValidationError as exc:
        raise PayloadParseError(
            "Malformed batch envelope", errors=_envelope_validation_error_items(exc)
        ) from exc


def validate_payload(raw: bytes) -> ValidationOutcome:
    """Full offline validation, mirroring ``POST /validate`` (CC29)."""
    envelope = parse_envelope(raw)

    if envelope.schema_version != SCHEMA_VERSION:
        return ValidationOutcome(
            valid=False,
            items_accepted=0,
            items_rejected=len(envelope.records),
            errors=[
                {
                    "index": -1,
                    "kind": None,
                    "code": "unsupported_schema_version",
                    "message": f"Unsupported schemaVersion: {envelope.schema_version!r}",
                    "path": "schemaVersion",
                }
            ],
        )

    if len(envelope.records) > DEFAULT_LIMITS.max_records_per_batch:
        return ValidationOutcome(
            valid=False,
            items_accepted=0,
            items_rejected=len(envelope.records),
            errors=[
                {
                    "index": -1,
                    "kind": None,
                    "code": "batch_too_large",
                    "message": (
                        f"Batch has {len(envelope.records)} records; max is "
                        f"{DEFAULT_LIMITS.max_records_per_batch}"
                    ),
                    "path": "records",
                }
            ],
        )

    record_errors = validate_records(envelope.records)
    rejected_indices = {item.index for item in record_errors}
    return ValidationOutcome(
        valid=not record_errors,
        items_accepted=len(envelope.records) - len(rejected_indices),
        items_rejected=len(rejected_indices),
        errors=[item.model_dump() for item in record_errors],
    )


def check_envelope_shape(
    raw: bytes, *, limits: BatchLimits = DEFAULT_LIMITS
) -> tuple[BatchEnvelope, None] | tuple[None, list[dict[str, Any]]]:
    """Shallow pre-flight for `push batch`, mirroring ``POST /batches``'s own
    checks (parse -> schema version -> batch size -> unknown kind) -- NOT
    per-record field validation (see module docstring). Returns
    ``(envelope, None)`` on success or ``(None, errors)`` on failure."""
    try:
        envelope = parse_envelope(raw)
    except PayloadParseError as exc:
        return None, exc.errors or [
            {
                "index": -1,
                "kind": None,
                "code": "invalid_envelope",
                "message": str(exc),
                "path": None,
            }
        ]

    if envelope.schema_version != SCHEMA_VERSION:
        return None, [
            {
                "index": -1,
                "kind": None,
                "code": "unsupported_schema_version",
                "message": f"Unsupported schemaVersion: {envelope.schema_version!r}",
                "path": "schemaVersion",
            }
        ]

    if len(envelope.records) > limits.max_records_per_batch:
        return None, [
            {
                "index": -1,
                "kind": None,
                "code": "batch_too_large",
                "message": (
                    f"Batch has {len(envelope.records)} records; max is "
                    f"{limits.max_records_per_batch}"
                ),
                "path": "records",
            }
        ]

    if len(raw) > limits.max_body_bytes:
        return None, [
            {
                "index": -1,
                "kind": None,
                "code": "payload_too_large",
                "message": f"Payload is {len(raw)} bytes; max is {limits.max_body_bytes}",
                "path": None,
            }
        ]

    unknown_kind_errors = [
        {
            "index": index,
            "kind": record.kind,
            "code": "unknown_record_kind",
            "message": f"Unknown record kind at index {index}: {record.kind!r}",
            "path": f"records[{index}].kind",
        }
        for index, record in enumerate(envelope.records)
        if record.kind not in RECORD_KIND_MODELS
    ]
    if unknown_kind_errors:
        return None, unknown_kind_errors

    return envelope, None


__all__ = [
    "PayloadParseError",
    "ValidationOutcome",
    "parse_envelope",
    "validate_payload",
    "check_envelope_shape",
]
