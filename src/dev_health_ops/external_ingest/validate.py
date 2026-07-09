"""Deep per-record validation over external-ingest wire schemas (CHAOS-2691).

Single owner (master-spec CC17): created COMPLETE here in wave 1, powers
``POST /api/v1/external-ingest/validate``. CHAOS-2697's worker imports
``validate_records`` UNCHANGED in wave 4 so the endpoint and the durable
worker path can never diverge on what "valid" means.

Shape-only: does not resolve cross-batch references (``repositoryExternalId``,
``sourceExternalKey``/``targetExternalKey``, etc.) against ClickHouse — that
requires a DB round-trip the request-validation path deliberately avoids.
Kind×system-matrix / source-instance-scope enforcement (master-spec CC6) is
also out of scope here — it needs org/source context this module doesn't
have and is added by CHAOS-2695/2697's worker-side validation extension.
"""

from __future__ import annotations

from pydantic import ValidationError

from dev_health_ops.api.external_ingest.schemas import (
    RECORD_KIND_MODELS,
    RecordEnvelope,
    ValidationErrorItem,
)

#: Pydantic error "type" -> our stable per-record rejection code vocabulary
#: (master-spec CC16 lists missing_required_field/invalid_literal/unknown_kind
#: as the canonical examples; anything else collapses to invalid_field).
_MISSING_ERROR_TYPES = {"missing"}
_LITERAL_ERROR_TYPES = {"literal_error", "enum"}


def _error_code_for(pydantic_error_type: str) -> str:
    if pydantic_error_type in _MISSING_ERROR_TYPES:
        return "missing_required_field"
    if pydantic_error_type in _LITERAL_ERROR_TYPES:
        return "invalid_literal"
    return "invalid_field"


def _error_path(index: int, loc: tuple) -> str:
    base = f"records[{index}].payload"
    if not loc:
        return base
    return base + "." + ".".join(str(part) for part in loc)


def validate_records(records: list[RecordEnvelope]) -> list[ValidationErrorItem]:
    """Validate each record's payload against its kind's Pydantic model.

    Returns one ``ValidationErrorItem`` per Pydantic field error found
    (a single malformed record can produce multiple items). An unknown
    ``kind`` produces exactly one ``unknown_kind`` item and skips payload
    validation for that record (there is no model to validate against).
    """
    errors: list[ValidationErrorItem] = []
    for index, record in enumerate(records):
        model = RECORD_KIND_MODELS.get(record.kind)
        if model is None:
            errors.append(
                ValidationErrorItem(
                    index=index,
                    kind=record.kind,
                    code="unknown_kind",
                    message=f"Unknown record kind: {record.kind!r}",
                    path=f"records[{index}].kind",
                )
            )
            continue
        try:
            model.model_validate(record.payload)
        except ValidationError as exc:
            for err in exc.errors():
                errors.append(
                    ValidationErrorItem(
                        index=index,
                        kind=record.kind,
                        code=_error_code_for(err["type"]),
                        message=err["msg"],
                        path=_error_path(index, err["loc"]),
                    )
                )
    return errors


__all__ = ["validate_records"]
