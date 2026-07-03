"""Pure worker-side normalization for external-ingest batches (CHAOS-2697).

Turns a parsed batch envelope's ``records`` into the :class:`NormalizedBatch`
shape ``sinks.write_batch()`` consumes, plus per-record rejections. Pure — no
I/O, no ClickHouse/Postgres calls — so it is unit-testable without services
and safe to re-run on redelivered stream entries.

Validation layering (master-spec CC6/CC17):

- Shape validation delegates to ``external_ingest.validate.validate_records``
  UNCHANGED (CC17: single owner, created complete by CHAOS-2691) so the
  ``POST /validate`` endpoint and this worker can never diverge on what a
  structurally valid record is.
- The kind x system matrix and the git-family instance-scope rule (CC6) are
  enforced HERE, as per-record rejections — ``validate.py`` deliberately
  excludes them (they need source context), and ``sinks.py`` only
  asserts-but-never-rejects (its ``record_outside_source_instance`` is a
  write-time warning). This module is the reject decision point.

The instance-scope comparison is case-INSENSITIVE (``casefold`` both sides),
deliberately looser than ``sinks._check_instance_scope``'s exact-match
warning: GitHub/GitLab full names and Jira/Linear keys are case-insensitive
identifiers on their providers (the same finding that made CHAOS-2695's
ownership matching case-insensitive), and ``derive_repo_uuid`` lower-cases
its seed — a case-variant identifier derives the SAME repo UUID, so
rejecting it would refuse data that cannot fork repo identity. The sink's
exact-match warning may still fire for such records; that is diagnostic
noise, not a contract violation.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field

from pydantic import BaseModel, ValidationError

from dev_health_ops.api.external_ingest.schemas import (
    RECORD_KIND_MODELS,
    RecordEnvelope,
)
from dev_health_ops.external_ingest.types import NormalizedBatch
from dev_health_ops.external_ingest.validate import validate_records

logger = logging.getLogger(__name__)

_GIT_FAMILY_KINDS = frozenset(
    {"repository.v1", "pull_request.v1", "review.v1", "commit.v1"}
)
_WORK_ITEM_FAMILY_KINDS = frozenset(
    {"work_item.v1", "work_item_transition.v1", "work_item_dependency.v1"}
)
_ORG_SCOPED_KINDS = frozenset({"team.v1", "identity.v1"})

#: Master-spec CC6 kind x system matrix. ``custom`` excludes the work_item
#: family in v1 (would widen the ``WorkItem.provider`` Literal vocabulary).
ALLOWED_KINDS_BY_SYSTEM: dict[str, frozenset[str]] = {
    "github": _GIT_FAMILY_KINDS | _WORK_ITEM_FAMILY_KINDS | _ORG_SCOPED_KINDS,
    "gitlab": _GIT_FAMILY_KINDS | _WORK_ITEM_FAMILY_KINDS | _ORG_SCOPED_KINDS,
    "jira": _WORK_ITEM_FAMILY_KINDS | _ORG_SCOPED_KINDS,
    "linear": _WORK_ITEM_FAMILY_KINDS | _ORG_SCOPED_KINDS,
    "custom": _GIT_FAMILY_KINDS | _ORG_SCOPED_KINDS,
}

#: CC6: systems whose git-family records must reference the batch's own
#: source.instance (jira/linear have no git-family kinds to scope).
_INSTANCE_SCOPED_SYSTEMS = frozenset({"github", "gitlab", "custom"})

#: kind -> (NormalizedBatch list attribute, record_index_by_kind key,
#: emit dicts?). Dict kinds are ``model_dump()``ed to the snake_case field
#: names ``sinks.py``'s row builders read; work-item-family kinds pass the
#: validated pydantic instances through (sinks duck-types attribute access).
_KIND_DISPATCH: dict[str, tuple[str, str, bool]] = {
    "repository.v1": ("repositories", "repository", True),
    "identity.v1": ("identities", "identity", True),
    "team.v1": ("teams", "team", True),
    "pull_request.v1": ("pull_requests", "pull_request", True),
    "review.v1": ("reviews", "review", True),
    "commit.v1": ("commits", "commit", True),
    "work_item.v1": ("work_items", "work_item", False),
    "work_item_transition.v1": ("work_item_transitions", "work_item_transition", False),
    "work_item_dependency.v1": (
        "work_item_dependencies",
        "work_item_dependency",
        False,
    ),
}


@dataclass(frozen=True)
class RecordRejection:
    """One rejected record. Exactly one per rejected index — the status
    store's ``(ingestion_id, record_index)`` unique constraint allows a
    single persisted diagnostic per record, so multi-field shape failures
    collapse to the first error (``validate_records`` emits them in field
    order)."""

    index: int
    kind: str
    external_id: str | None
    code: str
    message: str
    path: str | None


@dataclass
class NormalizationResult:
    batch: NormalizedBatch
    rejections: list[RecordRejection] = field(default_factory=list)
    #: Accepted records per FULL kind name (e.g. ``{"pull_request.v1": 12}``)
    #: — the vocabulary both the batch row's ``record_counts`` column and
    #: CHAOS-2699's recompute planner expect (``plan_recompute`` intersects
    #: against ``.v1``-suffixed kind sets; the sink scope's bare-kind names
    #: would silently never trigger recompute).
    record_counts: dict[str, int] = field(default_factory=dict)
    items_received: int = 0

    @property
    def items_rejected(self) -> int:
        return len(self.rejections)

    @property
    def items_accepted(self) -> int:
        return self.items_received - len(self.rejections)


def _git_family_repo_identifier(kind: str, payload_model: BaseModel) -> str:
    """The repo identifier CC6 scopes to source.instance: ``repository.v1``
    carries it as its own ``external_id``; the other git-family kinds
    reference it as ``repository_external_id``."""
    if kind == "repository.v1":
        return str(getattr(payload_model, "external_id", "") or "")
    return str(getattr(payload_model, "repository_external_id", "") or "")


def normalize_batch(
    *,
    org_id: str,
    source_id: uuid.UUID,
    source_system: str,
    source_instance: str,
    ingestion_id: uuid.UUID,
    records: list[RecordEnvelope],
) -> NormalizationResult:
    """Validate + normalize one batch's records. Never raises for record
    content — every per-record problem becomes a :class:`RecordRejection`;
    callers decide batch-level outcomes from the counts."""
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system=source_system,
        source_instance=source_instance,
        ingestion_id=ingestion_id,
    )
    result = NormalizationResult(batch=batch, items_received=len(records))

    # CC17: shape validation is validate.py's verdict, imported unchanged.
    # First error per index wins the (single) persisted rejection slot.
    shape_error_by_index: dict[int, tuple[str, str, str | None]] = {}
    for item in validate_records(records):
        shape_error_by_index.setdefault(
            item.index, (item.code, item.message, item.path)
        )

    allowed_kinds = ALLOWED_KINDS_BY_SYSTEM.get(source_system, frozenset())
    instance_folded = source_instance.casefold()

    for index, record in enumerate(records):
        shape_error = shape_error_by_index.get(index)
        if shape_error is not None:
            code, message, path = shape_error
            result.rejections.append(
                RecordRejection(
                    index=index,
                    kind=record.kind,
                    external_id=record.external_id,
                    code=code,
                    message=message,
                    path=path,
                )
            )
            continue

        if record.kind not in allowed_kinds:
            result.rejections.append(
                RecordRejection(
                    index=index,
                    kind=record.kind,
                    external_id=record.external_id,
                    code="unsupported_kind_for_system",
                    message=(
                        f"record kind {record.kind!r} is not accepted for "
                        f"source system {source_system!r} (master-spec CC6)"
                    ),
                    path=f"records[{index}].kind",
                )
            )
            continue

        model_cls = RECORD_KIND_MODELS[record.kind]
        try:
            payload_model = model_cls.model_validate(record.payload)
        except ValidationError:  # pragma: no cover - validate_records already
            # passed this exact payload against this exact model; reaching
            # here would mean the two diverged (a CC17 violation), so fail
            # the record loudly rather than crash the batch.
            logger.exception(
                "normalize_batch: model_validate diverged from validate_records "
                "for kind=%s index=%d",
                record.kind,
                index,
            )
            result.rejections.append(
                RecordRejection(
                    index=index,
                    kind=record.kind,
                    external_id=record.external_id,
                    code="invalid_field",
                    message="payload failed validation during normalization",
                    path=f"records[{index}].payload",
                )
            )
            continue

        if (
            record.kind in _GIT_FAMILY_KINDS
            and source_system in _INSTANCE_SCOPED_SYSTEMS
        ):
            identifier = _git_family_repo_identifier(record.kind, payload_model)
            if identifier.casefold() != instance_folded:
                result.rejections.append(
                    RecordRejection(
                        index=index,
                        kind=record.kind,
                        external_id=record.external_id,
                        code="record_outside_source_instance",
                        message=(
                            f"repository identifier {identifier!r} does not "
                            f"match this batch's source.instance "
                            f"{source_instance!r} (master-spec CC6)"
                        ),
                        path=f"records[{index}].payload",
                    )
                )
                continue

        attr, index_key, as_dict = _KIND_DISPATCH[record.kind]
        target: list = getattr(batch, attr)
        target.append(payload_model.model_dump() if as_dict else payload_model)
        batch.record_index_by_kind.setdefault(index_key, []).append(index)
        result.record_counts[record.kind] = result.record_counts.get(record.kind, 0) + 1

    return result


__all__ = [
    "ALLOWED_KINDS_BY_SYSTEM",
    "NormalizationResult",
    "RecordRejection",
    "normalize_batch",
]
