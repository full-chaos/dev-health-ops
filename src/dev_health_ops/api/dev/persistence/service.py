"""Tenant-scoped persistence and lifecycle operations for Ask Dev.

The service accepts authenticated organization/user identifiers from its caller;
HTTP payloads and model output must never supply those values. All reads and
writes include both identifiers, including administrator operations where the
target user is explicit.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeAlias

from pydantic import ValidationError as PydanticValidationError
from sqlalchemy import and_, case, event, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dev_health_ops.api.dev.contracts import DevError
from dev_health_ops.api.dev.contracts_v2.base import NarrativeFailureCode
from dev_health_ops.api.dev.contracts_v2.frame import (
    DevAnswerFrame as DevAnswerFrameContract,
)
from dev_health_ops.api.dev.contracts_v2.narrative import (
    DevNarrative as DevNarrativeContract,
)
from dev_health_ops.api.dev.contracts_v2.subject import (
    DevResolutionEntry as DevResolutionEntryContract,
)
from dev_health_ops.api.dev.org_policy import ASK_DEV_RUN_COST_HARD_MAX_MICROUSD
from dev_health_ops.models.dev_persistence import (
    DEV_RETENTION_DAYS,
    DevAnswerFrame,
    DevConversation,
    DevConversationTombstone,
    DevFeedback,
    DevMessage,
    DevRun,
    DevRunIntent,
    DevRunNarrative,
    DevRunResolution,
    DevRunSourceObservation,
    DevRunStageDiagnostic,
    DevRunSubjectSet,
    DevToolCall,
)
from dev_health_ops.models.users import Organization, User

logger = logging.getLogger(__name__)

AnswerPayloadValidator: TypeAlias = Callable[[Mapping[str, Any]], Mapping[str, Any]]

_TERMINAL_RUN_STATES = frozenset(
    {
        "completed",
        "insufficient_evidence",
        "refused",
        "failed",
        "cancelled",
    }
)
_RUN_STATES = _TERMINAL_RUN_STATES | frozenset(
    {
        "accepted",
        "resolving_scope",
        # CHAOS-3292 preflight phases. Non-terminal by construction, so a run
        # sitting in one correctly yields 409 concurrency_limited on replay
        # rather than being treated as a finished run.
        "interpreting",
        "resolving_subjects",
        "model_decision",
        "tool_validation",
        "tool_execution",
        "answer_validation",
    }
)
_FEEDBACK_REASONS = frozenset(
    {
        "incorrect",
        "missing_evidence",
        "wrong_scope",
        "stale_data",
        "unclear",
        "useful",
    }
)
_FORBIDDEN_METADATA_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "chain_of_thought",
        "credential",
        "credentials",
        "graphql_result",
        "password",
        "private_reasoning",
        "provider_request",
        "provider_response",
        "raw_prompt",
        "raw_provider_payload",
        "secret",
        "source_payload",
        "sql_result",
        "system_prompt",
    }
)
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")

# -- Wave 3.1 (CHAOS-3299) closed vocabularies -------------------------------
# Mirrors the CHECK constraints on the corresponding tables (defense in
# depth, matching the existing double-check pattern in append_tool_call /
# update_run: the DB CHECK is the hard backstop, this is the friendly error).
_QUESTION_INTENT_IDS = frozenset(
    {
        "entity_status",
        "portfolio_status",
        "remaining_work",
        "observed_change",
        "registered_statistics",
        "metric_comparison",
        "data_trust",
        "project_health",
        "team_health",
        "team_workload_balance",
        "operational_deficiency_inventory",
        "bounded_investigation",
    }
)
_CARDINALITIES = frozenset({"singular", "plural_cohort", "organization_wide"})
_RESOLUTION_OUTCOMES = frozenset(
    {
        "exact_match",
        "ambiguous_candidates",
        "no_authorized_match",
        "catalog_unavailable",
        "unsupported_kind",
    }
)
_ENTITY_KINDS = frozenset(
    {"repository", "project", "work_unit", "issue", "pull_request", "team"}
)
#: CHAOS-3337: this frozenset is a SEPARATE, hand-maintained mirror of
#: ``contracts_v2.base.SourceClass`` -- that enum being closed only proves a
#: plan document's own ``source_requirements`` are internally consistent,
#: never that THIS allowlist was updated to match. A registered plan whose
#: steps emit a SourceClass this table does not carry crashes the first
#: live run that reaches ``append_source_observation`` below with
#: ``DevPersistenceValidationError('invalid source_class')`` -- exactly
#: what happened for ``health_profile``/``deficiency_inventory``
#: (CHAOS-3297 stack #3, merged #1387) before this fix. Reconciliation is
#: now also checked at ``investigation_plans.wave_3_1_plans`` import time
#: (``_source_classes_missing_from_persistence_allowlist``), so the next
#: SourceClass a registered plan's steps emit fails at import, not live.
_SOURCE_CLASSES = frozenset(
    {
        "status_change",
        "work_item",
        "work_graph",
        "pull_request",
        "code_change",
        "review",
        "ci_run",
        "test_report",
        "deployment",
        "incident",
        "operational_control",
        "source_health",
        "health_profile",
        "deficiency_inventory",
    }
)
_REQUIREMENT_LEVELS = frozenset(
    {"mandatory", "conditional", "optional", "not_applicable"}
)
_SOURCE_OBSERVED_STATES = frozenset(
    {
        "available_current",
        "available_stale",
        "available_unknown",
        "unconfigured",
        "unavailable",
        "unauthorized_or_not_visible",
        "not_applicable",
        "truncated",
    }
)
_DATA_SEMANTICS = frozenset({"measured_zero", "no_data", "not_measured"})
_PUBLIC_OUTCOMES = frozenset(
    {
        "answered",
        "answered_with_gaps",
        "needs_clarification",
        "not_found",
        "temporarily_unavailable",
        "unsupported",
        "denied",
        "failed",
    }
)
_NARRATIVE_MODES = frozenset({"provider", "deterministic_fallback"})
# CHAOS-3297 codex NO-SHIP finding round 1, MEDIUM #3: the Python-side half
# of NarrativeFailureCode's enforcement (contracts_v2.base owns the
# vocabulary itself; migration 0083's CHECK constraint is the other half).
_NARRATIVE_FAILURE_CODES = frozenset(code.value for code in NarrativeFailureCode)
_STAGE_IDS = frozenset(
    {
        "interpreting",
        "resolving_subjects",
        "planning",
        "collecting",
        "synthesizing_frame",
        "narrating_optional",
        "projecting_answer",
    }
)
_STAGE_STATUSES = frozenset({"started", "completed", "failed", "skipped"})

# Bounds on the opaque JSONB payload each Wave 3.1 artifact carries. These are
# defense-in-depth (SQLite test targets have no DB-level byte-length CHECK);
# the contract's own bounded collection max_lengths are the primary bound.
_INTENT_PAYLOAD_MAX_BYTES = 16 * 1024
_RESOLUTION_PAYLOAD_MAX_BYTES = 8 * 1024
_SUBJECT_SET_PAYLOAD_MAX_BYTES = 16 * 1024
# Codex finding (HIGH, 2026-08-01): a dense-but-valid `status.entity.v2`
# observation (status_facts + up to 100 required_children + pull_requests +
# ci_checks + deployments + incidents, each fact carrying its own
# evidence_ref_ids/observed_at/display_label) observed ~19.6KB serialized --
# comfortably past the prior 16KB bound, which raised
# DevPersistenceValidationError at persist time and turned a real, already-
# validated answer into an internal_error. The content-bearing collections
# are already hard-capped at the contract layer (DevSourceContent's own
# max_length=25/100 per field, contracts_v2/result.py) and every string
# field is itself bounded (OpaqueID/Label/ShortText/EvidenceHandle), so
# raising this envelope does not make the payload unbounded -- it aligns the
# persistence-layer defense-in-depth check with what the contract already
# allows to be legitimately dense, rather than truncating real evidence-
# linked facts (which would either fabricate a shorter-than-real answer or
# require a second, undisclosed truncation notion on top of the contract's
# own TRUNCATED state). 64KB keeps meaningful headroom above the observed
# dense case while staying well under the 128KB frame-payload envelope this
# same module already trusts for the strictly larger synthesized answer.
_SOURCE_OBSERVATION_PAYLOAD_MAX_BYTES = 64 * 1024
_PLAN_STEP_PARTITION_MAX_BYTES = 8 * 1024
_PLAN_STEP_LIST_MAX_ITEMS = 25
_FRAME_PAYLOAD_MAX_BYTES = 128 * 1024
_NARRATIVE_TEXT_MAX_BYTES = 8 * 1024
_NARRATIVE_PAYLOAD_MAX_BYTES = 16 * 1024


# CHAOS-3297 Codex review round 3 CLASS A: the acceptance predicate for
# terminal_error_payload is contract validity itself (DevError.model_validate
# succeeds -> persist) -- not a byte cap. Three prior byte-cap incarnations
# on this branch alone (16 KiB, 64 KiB, 4 KiB) each rejected some legal
# DevError instance a later review round could still construct; a fourth
# hand-computed "worst case" constant (144 KiB) was *itself* wrong by 13
# bytes (retryable=False serializes one byte longer than True; some legal
# AwareDatetime renderings are longer than a no-fraction UTC instant) --
# proof that hand-assembling one "worst" instance is the wrong shape of
# argument, not just an error to correct again. This constant is now only
# a loose, cheap anti-runaway backstop with no aspiration to be tight; the
# real ceiling is enforced by DevError's own pydantic validation before
# this is ever consulted. tests/api/dev/test_persistence_v2.py::
# test_dev_error_worst_case_is_within_the_terminal_error_backstop proves
# this backstop can never legitimately reject a valid dev_error.v1: it
# derives the worst case by walking DevError.model_fields (not a
# hand-built example) and asserts the sum stays under this constant, so
# the proof -- not a guess -- is what has to keep holding as the contract
# evolves.
_TERMINAL_ERROR_PAYLOAD_BACKSTOP_BYTES = 1024 * 1024


class DevPersistenceNotFound(LookupError):
    """The requested tenant-owned resource is absent or unauthorized."""


class DevPersistenceConflict(RuntimeError):
    """An idempotency key exists without its required durable companion row."""


class DevPersistenceValidationError(ValueError):
    """Input is outside the approved bounded persistence contract."""


class DevConcurrencyLimitExceeded(RuntimeError):
    """The exact per-user or per-organization active-run limit was reached."""


class DevRateLimitExceeded(RuntimeError):
    """The exact per-user or per-organization request window was reached."""


class DevMonthlyRequestLimitExceeded(RuntimeError):
    """The platform-managed organization request allowance was exhausted."""

    def __init__(self, reset_at: datetime) -> None:
        self.reset_at = reset_at
        super().__init__("Ask Dev monthly platform request allowance reached")


class DevMonthlyCostLimitExceeded(RuntimeError):
    """The platform-managed organization monetary allowance was exhausted."""

    def __init__(self, reset_at: datetime) -> None:
        self.reset_at = reset_at
        super().__init__("Ask Dev monthly platform cost allowance reached")


@dataclass(frozen=True, slots=True)
class MessageRunResult:
    message: DevMessage
    run: DevRun
    created: bool


@dataclass(frozen=True, slots=True)
class CleanupResult:
    reason: str
    selected: int
    purged: int


@dataclass(frozen=True, slots=True)
class ConversationRecord:
    conversation: DevConversation
    message_count: int
    latest_answer_id: uuid.UUID | None


@dataclass(frozen=True, slots=True)
class TranscriptRecord:
    message: DevMessage
    run: DevRun


@dataclass(frozen=True, slots=True)
class TranscriptPage:
    records: Sequence[TranscriptRecord]
    has_more: bool


@dataclass(frozen=True, slots=True)
class DevAdmissionLimits:
    active_runs_per_user: int = 1
    active_runs_per_org: int = 5
    requests_per_user_per_15_minutes: int = 20
    requests_per_org_per_hour: int = 100

    def __post_init__(self) -> None:
        if (
            self.active_runs_per_user < 1
            or self.active_runs_per_user > 1
            or self.active_runs_per_org < 1
            or self.active_runs_per_org > 5
            or self.requests_per_user_per_15_minutes < 1
            or self.requests_per_user_per_15_minutes > 20
            or self.requests_per_org_per_hour < 1
            or self.requests_per_org_per_hour > 100
        ):
            raise ValueError("Ask Dev admission limits may only be configured downward")


@dataclass(frozen=True, slots=True)
class DevPlatformAllowance:
    monthly_request_limit: int
    monthly_cost_limit_microusd: int
    per_run_reservation_microusd: int = ASK_DEV_RUN_COST_HARD_MAX_MICROUSD

    def __post_init__(self) -> None:
        if (
            self.monthly_request_limit < 1
            or self.monthly_cost_limit_microusd < 1
            or self.per_run_reservation_microusd < 1
            or self.per_run_reservation_microusd > self.monthly_cost_limit_microusd
        ):
            raise ValueError("Ask Dev platform allowance must be positive and bounded")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _json_copy(value: Any, *, field: str, forbid_sensitive_keys: bool = True) -> Any:
    if forbid_sensitive_keys:
        _reject_sensitive_keys(value, field=field)
    try:
        encoded = json.dumps(value, separators=(",", ":"), sort_keys=True)
        return json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise DevPersistenceValidationError(
            f"{field} must be JSON serializable"
        ) from exc


def _reject_sensitive_keys(value: Any, *, field: str) -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).strip().lower()
            if key in _FORBIDDEN_METADATA_KEYS:
                raise DevPersistenceValidationError(
                    f"{field} contains prohibited metadata key"
                )
            _reject_sensitive_keys(child, field=field)
    elif isinstance(value, list | tuple):
        for child in value:
            _reject_sensitive_keys(child, field=field)


def _bounded_text(value: str | None, *, field: str, max_bytes: int) -> str | None:
    if value is None:
        return None
    if len(value.encode("utf-8")) > max_bytes:
        raise DevPersistenceValidationError(f"{field} exceeds {max_bytes} UTF-8 bytes")
    return value


def _digest(value: str | None, *, field: str) -> str | None:
    if value is None:
        return None
    if len(value) != 71 or not value.startswith("sha256:"):
        raise DevPersistenceValidationError(f"{field} must be a sha256 digest")
    try:
        int(value.removeprefix("sha256:"), 16)
    except ValueError as exc:
        raise DevPersistenceValidationError(f"{field} must be a sha256 digest") from exc
    return value


def _sha256_digest(value: str) -> str:
    """Compute a ``sha256:``-prefixed digest, matching
    ``orchestrator_persistence._digest`` exactly -- unlike ``_digest``
    above (which only *validates* a value is already in that shape), this
    *produces* one, for cross-checking a caller-supplied pre-hashed
    fingerprint against the raw value a validated payload carries.
    """

    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def _safe_token(value: str | None, *, field: str, max_bytes: int) -> str | None:
    value = _bounded_text(value, field=field, max_bytes=max_bytes)
    if value is not None and _SAFE_TOKEN.fullmatch(value) is None:
        raise DevPersistenceValidationError(f"{field} must be a safe identifier")
    return value


def _bounded_json(
    value: Mapping[str, Any], *, field: str, max_bytes: int
) -> dict[str, Any]:
    """A sensitive-key-checked, byte-bounded JSONB payload copy.

    Used for every Wave 3.1 (CHAOS-3299) opaque ``payload`` column: the
    contract's own pydantic validators already bound each collection's
    ``max_length``, but persistence re-checks byte size and forbidden keys
    independently rather than trusting the contract validator alone (the
    established double-check posture, e.g. ``append_tool_call``).
    """

    copied = _json_copy(dict(value), field=field)
    encoded = json.dumps(copied, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) > max_bytes:
        raise DevPersistenceValidationError(f"{field} exceeds {max_bytes} bytes")
    return copied


def _bounded_step_list(values: Sequence[str], *, field: str) -> list[str]:
    """Bound one ``plan_step_partition`` list: at most 25 non-empty entries.

    Mirrors the contract's own ``ShortText`` tuple ``max_length=25`` bound
    (``DevInvestigationResult.completed_steps`` etc.) as a service-layer
    double-check, the same posture as every other Wave 3.1 closed-vocabulary
    re-check.
    """

    if len(values) > _PLAN_STEP_LIST_MAX_ITEMS:
        raise DevPersistenceValidationError(
            f"{field} exceeds {_PLAN_STEP_LIST_MAX_ITEMS} entries"
        )
    bounded: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item:
            raise DevPersistenceValidationError(
                f"{field} entries must be non-empty text"
            )
        bounded.append(item)
    return bounded


def _safe_count_summary(value: Mapping[str, Any], *, field: str) -> dict[str, Any]:
    copied = _json_copy(dict(value), field=field)
    for key, item in copied.items():
        if _SAFE_TOKEN.fullmatch(key) is None:
            raise DevPersistenceValidationError(f"{field} contains an unsafe key")
        if item is not None and not isinstance(item, bool | int | float):
            raise DevPersistenceValidationError(
                f"{field} may contain only counts, booleans, or null"
            )
        if isinstance(item, int | float) and not isinstance(item, bool) and item < 0:
            raise DevPersistenceValidationError(f"{field} counts must be nonnegative")
    return copied


# -- ORM-boundary payload validation (CHAOS-3297 Codex review round 7
# MEDIUM, demoted to fast-fail UX by round 9). The AST scanner
# (test_persistence_v2.py) is a syntax-level tripwire -- it can only ever
# recognize the write *shapes* it was taught to look for, and it only sees
# this module's own source. This session-level layer (mapper events below,
# plus the do_orm_execute hook further down) is broader -- it fires for
# every write issued *through this Session*, mapped-instance or Core DML
# alike -- but it is still bounded by "through this Session": a Core-table
# UPDATE against a bare Table has no `bind_mapper` to look up, and nothing
# here can see the SET clause of an `INSERT ... ON CONFLICT DO UPDATE`'s
# conflict resolution. Both are real, Codex-confirmed bypasses of this
# layer specifically.
#
# The LOAD-BEARING, total guard is the DB trigger on each payload-bearing
# table (see the round-9 comment block below `_PAYLOAD_MODEL_VALIDATORS`,
# and `models/dev_persistence.py`) -- it validates the row on its way into
# the table however it got there, ORM or Core or upsert or raw connection,
# with no statement shape it does not see. This layer's job now is purely
# fast-fail UX: a caller going through the ORM gets a clean
# DevPersistenceValidationError before ever reaching the database, instead
# of an IntegrityError surfacing from a trigger three layers down the
# stack. Every check here is intentionally a strict subset of what the
# trigger enforces (contract validity is a superset the trigger does not
# attempt) -- keeping both correct never requires them to diverge, only
# this layer to occasionally have less to say. -----------------------


class _KnownUnvalidatedPayloadGap:
    """Sentinel marking a payload-bearing model whose ``payload`` is not
    yet validated against its wire contract, at either the service-method
    layer (see ``_KNOWN_UNVALIDATED_PAYLOAD_SINKS``) or this ORM
    boundary -- the same explicit, filed gap, not a silent pass.
    CHAOS-3330 tracks closing both together, model by model."""


_KNOWN_UNVALIDATED_PAYLOAD_GAP = _KnownUnvalidatedPayloadGap()

#: Sentinel distinguishing "this column is part of the write, and its
#: value is None" from "this column is not part of the write at all" --
#: only the bulk-DML shim (``_BulkDmlPayloadTarget``) can ever produce
#: this; a real, fully-formed ORM instance always has every mapped
#: column, so ``getattr(target, name, _MISSING)`` never returns it there
#: (CHAOS-3297 Codex review round 8 HIGH: cross-checks must refuse to
#: validate against a column a bulk statement never touches, not treat
#: its absence as ``None``).
_MISSING = object()


def _require_field_matches(
    target: Any, *, column: str, expected: str, contract_field: str
) -> None:
    """Binds a validated payload to the ROW it is being written to, not
    merely to itself (CHAOS-3297 Codex review round 8 HIGH: a fully
    self-consistent, contract-valid payload for a DIFFERENT run/frame/
    narrative is not a valid write to THIS row). Mirrors the cross-checks
    ``record_frame``/``record_narrative`` already make between the
    validated payload and the service method's own arguments -- here,
    between the validated payload and the OTHER columns this same write
    sets ``column`` to.

    Refuses outright (deny by default) if ``column`` is not part of this
    write at all -- a bulk statement's WHERE clause can leave an existing
    row's identity columns unstated, and reading the pre-existing row to
    "fill in" a comparison would compare the new payload against a value
    this write is not actually asserting.
    """

    actual = getattr(target, column, _MISSING)
    if actual is _MISSING:
        raise DevPersistenceValidationError(
            f"payload cross-check for `{contract_field}` requires the "
            f"`{column}` column to be part of the same write -- refusing "
            f"to validate a payload write that does not also state the "
            f"row identity columns it must agree with"
        )
    if actual is None or str(actual) != expected:
        raise DevPersistenceValidationError(
            f"payload's `{contract_field}` does not match this write's `{column}`"
        )


def _validate_answer_frame_payload(target: Any) -> None:
    try:
        validated = DevAnswerFrameContract.model_validate(target.payload)
    except PydanticValidationError as exc:
        raise DevPersistenceValidationError(
            f"dev_answer_frames.payload is not a valid dev_answer_frame.v1: {exc}"
        ) from exc
    # Mirrors record_frame's own cross-checks (frame_id/run_id/
    # public_outcome against the caller's own arguments) -- here, against
    # the same columns this write itself sets.
    _require_field_matches(
        target,
        column="frame_id",
        expected=validated.frame_id,
        contract_field="frame_id",
    )
    _require_field_matches(
        target, column="run_id", expected=validated.run_id, contract_field="run_id"
    )
    _require_field_matches(
        target,
        column="public_outcome",
        expected=validated.public_outcome.value,
        contract_field="public_outcome",
    )


def _validate_run_narrative_payload(target: Any) -> None:
    # payload excludes `body` -- narrative_text is the separate
    # authoritative copy (see record_narrative's own docstring); the full
    # contract object must be reconstructed before validating, exactly as
    # record_narrative already does at the service layer. narrative_text
    # must be part of THIS write (not read from a possibly-stale
    # pre-existing row) or there is nothing safe to reconstruct `body`
    # from.
    narrative_text = getattr(target, "narrative_text", _MISSING)
    if narrative_text is _MISSING:
        raise DevPersistenceValidationError(
            "dev_run_narratives payload validation requires "
            "`narrative_text` to be part of the same write -- refusing "
            "to reconstruct `body` from a column this write does not set"
        )
    reconstructed = dict(target.payload)
    reconstructed["body"] = narrative_text
    try:
        validated = DevNarrativeContract.model_validate(reconstructed)
    except PydanticValidationError as exc:
        raise DevPersistenceValidationError(
            f"dev_run_narratives.payload is not a valid dev_narrative.v1: {exc}"
        ) from exc
    # Mirrors record_narrative's own cross-checks (narrative_id/run_id/
    # frame_id/mode/provider_fingerprint against the caller's own
    # arguments) -- here, against the same columns this write itself
    # sets. This is exactly what closes CHAOS-3297 Codex review round 8
    # HIGH: a fully valid dev_narrative.v1 payload for a DIFFERENT
    # run/frame/narrative_id must not silently attach to this row.
    _require_field_matches(
        target,
        column="narrative_id",
        expected=validated.narrative_id,
        contract_field="narrative_id",
    )
    _require_field_matches(
        target, column="run_id", expected=validated.run_id, contract_field="run_id"
    )
    _require_field_matches(
        target,
        column="frame_id",
        expected=validated.frame_id,
        contract_field="frame_id",
    )
    _require_field_matches(
        target, column="mode", expected=validated.mode, contract_field="mode"
    )
    expected_provider_fingerprint = (
        _sha256_digest(validated.provider_metadata.model_fingerprint)
        if validated.provider_metadata is not None
        else None
    )
    actual_provider_fingerprint: Any = getattr(target, "provider_fingerprint", _MISSING)
    if actual_provider_fingerprint is _MISSING:
        raise DevPersistenceValidationError(
            "dev_run_narratives payload validation requires "
            "`provider_fingerprint` to be part of the same write"
        )
    # The column already stores the pre-hashed shape record_narrative
    # writes; _digest validates that shape (or passes None through), the
    # same way record_narrative's own cross-check does.
    safe_actual_provider_fingerprint = _digest(
        actual_provider_fingerprint, field="provider_fingerprint"
    )
    if safe_actual_provider_fingerprint != expected_provider_fingerprint:
        raise DevPersistenceValidationError(
            "payload's provider_metadata does not match this write's "
            "provider_fingerprint"
        )


#: Total registry: every payload-bearing ORM model (see
#: ``test_persistence_v2.py::_payload_bearing_orm_model_names``, which
#: discovers the set from live column metadata, never a hand-typed list)
#: must have an entry here -- either a validator function enforcing its
#: wire contract, or explicitly ``_KNOWN_UNVALIDATED_PAYLOAD_GAP``
#: (CHAOS-3330's four sinks, the same honest gap
#: ``_KNOWN_UNVALIDATED_PAYLOAD_SINKS`` already names at the
#: service-method level). Totality against the live model set is asserted
#: by ``test_orm_boundary_payload_validator_registry_matches_the_live_schema``
#: in ``test_persistence_v2.py``.
_PAYLOAD_MODEL_VALIDATORS: dict[type, Any] = {
    DevAnswerFrame: _validate_answer_frame_payload,
    DevRunNarrative: _validate_run_narrative_payload,
    DevRunIntent: _KNOWN_UNVALIDATED_PAYLOAD_GAP,
    DevRunResolution: _KNOWN_UNVALIDATED_PAYLOAD_GAP,
    DevRunSubjectSet: _KNOWN_UNVALIDATED_PAYLOAD_GAP,
    DevRunSourceObservation: _KNOWN_UNVALIDATED_PAYLOAD_GAP,
}


# -- CHAOS-3297 Codex review round 8: partition of session-mediated write
# paths this layer covers, verified empirically against SQLAlchemy 2.0.49
# (probed directly, not assumed from docs). Round 9 found two more real
# rows this layer could not see at all -- a Core-table UPDATE issued
# through the Session (`bind_mapper` is None for a bare `Table`, not a
# mapped class) and an `INSERT ... ON CONFLICT DO UPDATE`'s conflict SET
# clause -- and moved the LOAD-BEARING guarantee to a DB trigger instead
# (see the round-9 comment block below `_PAYLOAD_MODEL_VALIDATORS`). What
# follows is still correct and still runs on every write it describes; it
# is fast-fail UX layered in front of the trigger, not the total guard
# anymore:
#
#   a) unit-of-work flush of ORM instances (attribute assignment, kwargs
#      construction, merge, cascades) -> before_insert/before_update
#      mapper events. Covered below.
#   b) ORM-enabled DML, single .values(...) -> do_orm_execute reading the
#      statement's own `_values`. Covered below.
#   c) ORM-enabled DML, multi-row .values([...]) -> do_orm_execute
#      reading `_multi_values` (confirmed: `_values` is None here, the
#      row dicts live in `_multi_values` instead). Covered below.
#   d) executemany-style parameter lists -- `session.execute(insert(M),
#      [params, ...])` and ORM bulk-update-by-primary-key
#      (`session.execute(update(M), [{"id": ..., ...}, ...])`) ->
#      do_orm_execute reading `orm_execute_state.parameters` (confirmed:
#      both `_values` and `_multi_values` are empty here, the row dicts
#      arrive as `.parameters` instead, keyed by column NAME not
#      `Column` object). Covered below.
#   e) legacy bulk APIs (`Session.bulk_save_objects`/
#      `bulk_insert_mappings`/`bulk_update_mappings`) -- confirmed
#      empirically to fire NEITHER `before_insert`/`before_update` NOR
#      `do_orm_execute` at all (a probe listener on both never fires).
#      `AsyncSession` does not expose these directly, but they remain
#      reachable via `await session.run_sync(lambda s:
#      s.bulk_save_objects(...))`, which is exactly how this was
#      reproduced. No event covers this path in SQLAlchemy 2.x -- it is
#      structurally prohibited below instead (Session methods wrapped to
#      raise for a payload-bearing model), proven by a mutation test that
#      the prohibition itself fires.
#   f) raw Core execution against a Connection obtained via
#      `session.connection()`, entirely outside the Session -- confirmed
#      empirically that `do_orm_execute` never fires there either. Out of
#      scope by construction (this is not a "session-mediated" write path
#      at all -- there is no Session-level event surface to hook), and
#      documented as the legitimate escape hatch
#      test_chaos_3297_frame_reachability.py uses to simulate genuinely
#      out-of-band data. Kept as a documented, tested boundary, not
#      silently assumed safe.
#
# (c) and (d) are both read through `_bulk_dml_row_dicts` below, which
# returns `None` -- not an empty list -- when a DML statement against a
# payload-bearing model carries its row data in NONE of `_values`/
# `_multi_values`/`parameters`: an unrecognized shape this module was not
# written against. The caller treats that as deny-by-default, not
# "nothing to validate" -- see `_enforce_payload_contract_on_bulk_dml`.


def _enforce_payload_contract_at_flush(
    _mapper: Any, _connection: Any, target: Any
) -> None:
    validator = _PAYLOAD_MODEL_VALIDATORS.get(type(target))
    if validator is None or validator is _KNOWN_UNVALIDATED_PAYLOAD_GAP:
        return
    validator(target)


for _payload_model in _PAYLOAD_MODEL_VALIDATORS:
    event.listen(_payload_model, "before_insert", _enforce_payload_contract_at_flush)
    event.listen(_payload_model, "before_update", _enforce_payload_contract_at_flush)
del _payload_model


def _decode_column_keyed_row(values: Mapping[Any, Any]) -> dict[str, Any]:
    """Column name -> Python value for one row's worth of a Core-style
    statement's own column-keyed value mapping (`_values`, or one row
    group of `_multi_values`), unwrapping a bound parameter to its
    literal value where present."""

    decoded: dict[str, Any] = {}
    for column, value in values.items():
        name = column.name if hasattr(column, "name") else str(column)
        decoded[name] = value.value if hasattr(value, "value") else value
    return decoded


def _bulk_dml_row_dicts(orm_execute_state: Any) -> list[dict[str, Any]] | None:
    """Every row's own column-name -> value dict a Core-style ORM DML
    statement could be carrying its data in, across all three shapes (b),
    (c), and (d) above. Returns `None` -- not `[]` -- if the statement
    uses none of the three: an unrecognized internal representation this
    boundary does not know how to inspect, which the caller must treat
    as deny-by-default rather than silently allow through."""

    statement = orm_execute_state.statement
    values = getattr(statement, "_values", None)
    multi_values = getattr(statement, "_multi_values", None)
    parameters = orm_execute_state.parameters
    if not values and not multi_values and not parameters:
        return None

    rows: list[dict[str, Any]] = []
    if values:
        rows.append(_decode_column_keyed_row(values))
    if multi_values:
        for row_group in multi_values:
            for row in row_group:
                rows.append(_decode_column_keyed_row(row))
    if parameters:
        parameter_rows = parameters if isinstance(parameters, list) else [parameters]
        for row in parameter_rows:
            rows.append(dict(row))
    return rows


class _BulkDmlPayloadTarget:
    """Throwaway shim exposing one bulk-DML row's own column values with
    the same attribute-access shape a real ORM instance has, so a bulk
    statement's raw row dict can be validated with the exact same
    validator functions a real ORM instance is -- and so that a column
    genuinely absent from this specific write (as opposed to present and
    ``None``) reads as ``_MISSING``, not ``None``."""

    def __init__(self, row: Mapping[str, Any]) -> None:
        self._row = dict(row)

    def __getattr__(self, name: str) -> Any:
        try:
            return self._row[name]
        except KeyError:
            raise AttributeError(name) from None


def _enforce_payload_contract_on_bulk_dml(orm_execute_state: Any) -> None:
    """Catches every write form ``before_insert``/``before_update``
    mapper events structurally cannot see at all: any Core-style ORM DML
    statement executed via ``session.execute(...)`` runs as a bulk
    statement, never through the ORM unit-of-work flush those events fire
    from (confirmed empirically -- a probe ``before_update`` listener
    never fires for any of shapes (b)/(c)/(d) above against a real
    engine).
    """

    if not (orm_execute_state.is_update or orm_execute_state.is_insert):
        return
    mapper = orm_execute_state.bind_mapper
    if mapper is None:
        return
    validator = _PAYLOAD_MODEL_VALIDATORS.get(mapper.class_)
    if validator is None or validator is _KNOWN_UNVALIDATED_PAYLOAD_GAP:
        return
    rows = _bulk_dml_row_dicts(orm_execute_state)
    if rows is None:
        raise DevPersistenceValidationError(
            f"unrecognized bulk DML write shape against payload-bearing "
            f"model {mapper.class_.__name__} -- this statement carries "
            f"no `_values`, `_multi_values`, or `parameters` this "
            f"boundary knows how to inspect; deny by default rather than "
            f"risk an unvalidated payload write"
        )
    for row in rows:
        if "payload" not in row:
            continue
        validator(_BulkDmlPayloadTarget(row))


event.listen(Session, "do_orm_execute", _enforce_payload_contract_on_bulk_dml)


# -- CHAOS-3297 Codex review round 9: two more real bypasses of
# everything above, both against a live database, both committing an
# invalid or row-mismatched payload:
#
#   * `session.execute(update(DevAnswerFrame.__table__)...)` -- a
#     Core-table UPDATE issued through the Session, not through the
#     mapped class. `orm_execute_state.bind_mapper` is `None` for this
#     statement (there is no ORM mapper to resolve a bare `Table`
#     against), so `_enforce_payload_contract_on_bulk_dml` above returns
#     at its very first `if mapper is None: return` -- correctly, by its
#     own logic, since it has nothing to look up `_PAYLOAD_MODEL_VALIDATORS`
#     with. This is not a bug in that function; it is the edge of what a
#     mapper-keyed registry can see at all.
#   * `insert(...).on_conflict_do_update(..., set_={"payload": ...})` --
#     the INSERT's own values are whatever this layer validates, but the
#     conflict resolution's SET clause is a separate part of the compiled
#     statement nothing here inspects; an UPDATE that only takes effect
#     on a conflict rides through completely unvalidated.
#
# Both are still "session-mediated" by any reasonable definition, and
# both defeated this layer anyway -- the standing conclusion after three
# rounds of extending a parser over SQLAlchemy's statement shapes: stop.
# The load-bearing, TOTAL guarantee is now a BEFORE INSERT/UPDATE trigger
# on each payload-bearing table (`dev_answer_frames`, `dev_run_narratives`
# -- see `models/dev_persistence.py` for the full DDL and design
# rationale, and Alembic migration 0080 for the Postgres arm). A trigger
# validates the row the database is about to accept, independent of
# which SQLAlchemy construct -- or non-SQLAlchemy client -- produced the
# SQL that got it there. Neither bypass above, nor any bypass this module
# has not yet been taught about, can write an invalid or row-mismatched
# payload past it.
#
# Everything below this point (the legacy-bulk-API prohibition) and
# everything above it (mapper events, `do_orm_execute`) remain exactly as
# correct as they were -- they still catch what they catch, before the
# database round-trip, with a clean `DevPersistenceValidationError`
# instead of an `IntegrityError` surfacing from three layers down. They
# are kept for that reason: fast-fail UX, not the load-bearing guarantee
# anymore.


# -- (e) above: legacy bulk APIs structurally prohibited for
# payload-bearing models. Confirmed empirically that
# `Session.bulk_save_objects`/`bulk_insert_mappings`/`bulk_update_mappings`
# fire neither the mapper events nor `do_orm_execute` -- there is no event
# hook in SQLAlchemy 2.x that sees these at all, so unlike (a)-(d) this
# cannot be validated by an event, only refused up front. `AsyncSession`
# does not expose these methods directly (only the underlying sync
# `Session` does, reachable via `session.run_sync(...)`), but wrapping the
# sync `Session` class methods themselves covers that path too, and every
# other way these might be reached in-process. (The DB trigger would also
# reject an invalid payload written this way -- these SQL statements are
# not exempt from it -- but the prohibition still gives a clean Python
# exception before ever reaching the database, same as the rest of this
# layer.)

_ORIGINAL_BULK_SAVE_OBJECTS = Session.bulk_save_objects
_ORIGINAL_BULK_INSERT_MAPPINGS = Session.bulk_insert_mappings
_ORIGINAL_BULK_UPDATE_MAPPINGS = Session.bulk_update_mappings


def _mapped_class(mapper_or_class: Any) -> Any:
    return getattr(mapper_or_class, "class_", mapper_or_class)


def _guarded_bulk_save_objects(
    self: Session, objects: Any, *args: Any, **kwargs: Any
) -> Any:
    materialized = list(objects)
    for obj in materialized:
        if type(obj) in _PAYLOAD_MODEL_VALIDATORS:
            raise DevPersistenceValidationError(
                f"bulk_save_objects is prohibited for payload-bearing "
                f"model {type(obj).__name__} (CHAOS-3297 Codex review "
                f"round 8) -- it bypasses the ORM payload-contract "
                f"boundary entirely; use the normal "
                f"Session.add()/flush() path instead"
            )
    return _ORIGINAL_BULK_SAVE_OBJECTS(self, materialized, *args, **kwargs)


def _guarded_bulk_insert_mappings(
    self: Session, mapper: Any, mappings: Any, *args: Any, **kwargs: Any
) -> Any:
    if _mapped_class(mapper) in _PAYLOAD_MODEL_VALIDATORS:
        raise DevPersistenceValidationError(
            f"bulk_insert_mappings is prohibited for payload-bearing "
            f"model {_mapped_class(mapper).__name__} (CHAOS-3297 Codex "
            f"review round 8) -- it bypasses the ORM payload-contract "
            f"boundary entirely; use the normal "
            f"Session.add()/flush() path instead"
        )
    return _ORIGINAL_BULK_INSERT_MAPPINGS(self, mapper, mappings, *args, **kwargs)


def _guarded_bulk_update_mappings(
    self: Session, mapper: Any, mappings: Any, *args: Any, **kwargs: Any
) -> Any:
    if _mapped_class(mapper) in _PAYLOAD_MODEL_VALIDATORS:
        raise DevPersistenceValidationError(
            f"bulk_update_mappings is prohibited for payload-bearing "
            f"model {_mapped_class(mapper).__name__} (CHAOS-3297 Codex "
            f"review round 8) -- it bypasses the ORM payload-contract "
            f"boundary entirely; use the normal "
            f"Session.add()/flush() path instead"
        )
    return _ORIGINAL_BULK_UPDATE_MAPPINGS(self, mapper, mappings, *args, **kwargs)


Session.bulk_save_objects = _guarded_bulk_save_objects  # type: ignore[method-assign]
Session.bulk_insert_mappings = _guarded_bulk_insert_mappings  # type: ignore[method-assign]
Session.bulk_update_mappings = _guarded_bulk_update_mappings  # type: ignore[method-assign]


async def _authorize_clarification_candidates(
    session: AsyncSession,
    *,
    run_id: uuid.UUID,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    validated: DevAnswerFrameContract,
) -> None:
    """CHAOS-3325 Codex review (NO-SHIP, confirmed medium): a schema-valid
    ``clarification_candidates`` entry only proves *shape*, not provenance --
    a frame naming an entity the resolution ledger never authorized (e.g.
    another org's repository, ``entity_id="other-org-secret-repo"``) would
    otherwise persist as canonical v2 state and be served verbatim by any
    general ``project_answer_v2_to_v1`` caller or future CHAOS-3298 consumer.

    A separate, independently monkeypatchable function -- the same posture
    ``contracts_v2.validators`` uses for its guardrails -- so a RED/GREEN
    test pair can prove this specific check, not the whole of
    ``record_frame``, is what rejects an unauthorized candidate.

    The run's ``ambiguous_candidates`` resolution-ledger entry is **always**
    fetched, regardless of whether the frame's own
    ``clarification_candidates`` is empty (Codex review round 2, confirmed
    medium: the round-1 early return on an empty frame tuple let an internal
    caller silently *downgrade* canonical state -- persist ``needs_
    clarification`` with zero candidates for a run whose ledger genuinely
    recorded several, hiding the real choices the resolver offered from
    every future reader of this "canonical" v2 row):

    * **No ledger entry recorded at all** (e.g. the question could not be
      interpreted -- ``build_preflight_answer`` never calls
      ``append_resolution`` for that case): the frame's candidates must be
      empty too. Non-empty here means the frame is claiming candidates
      nothing authorized, and is rejected exactly like a mismatch below.
    * **A ledger entry exists**: the frame's candidates must equal it
      exactly, in the same order, element for element -- including
      non-emptiness. An empty frame against a non-empty ledger entry is now
      rejected (the round-2 fix): under-disclosure is no longer assumed
      safe once a real ledger row exists to compare against, because that
      row is exactly the canonical-state-downgrade signal above.

    ``orchestrator.run`` persists the ledger entry via ``append_resolution``
    immediately before calling ``record_frame`` for this outcome (see
    ``SubjectPreflightResult.terminating_resolution_entry``), so both fetch
    outcomes above correspond to a real caller state, never an artifact of
    query timing.

    Residual risk (CHAOS-3325 Codex review round 2 finding 1, deferred by
    ruling -- not fixed in this branch): a caller *inside* the trust
    boundary that forges both a schema-valid ledger row (via
    ``append_resolution``, itself unvalidated against the actual scope
    catalog today) and a frame whose candidates happen to equal it defeats
    this equality check -- it only proves the two objects agree with each
    other, not that either was honestly produced by ``subject_preflight``'s
    real resolution. Closing that "double-forge" seam requires validating
    ``append_resolution``'s own payload against the authorized catalog,
    which is CHAOS-3330's scope, not a persistence-layer-to-resolver-catalog
    coupling here. See
    ``test_record_frame_double_forged_ledger_and_frame_defeats_the_equality_check``
    (``xfail(strict=True)``, flips loudly once CHAOS-3330 lands).
    """

    ledger_row = await session.scalar(
        select(DevRunResolution)
        .where(
            DevRunResolution.run_id == run_id,
            DevRunResolution.org_id == org_id,
            DevRunResolution.user_id == user_id,
            DevRunResolution.outcome == "ambiguous_candidates",
        )
        .order_by(DevRunResolution.entry_ordinal.desc())
    )
    if ledger_row is None:
        if validated.clarification_candidates:
            raise DevPersistenceValidationError(
                "frame_payload.clarification_candidates is non-empty but this "
                "run has no recorded ambiguous_candidates resolution ledger "
                "entry to authorize it"
            )
        return
    try:
        ledger_entry = DevResolutionEntryContract.model_validate(ledger_row.payload)
    except PydanticValidationError as exc:
        raise DevPersistenceValidationError(
            "the run's recorded resolution ledger entry is not a valid "
            f"dev_resolution_entry: {exc}"
        ) from exc
    if tuple(validated.clarification_candidates) != tuple(ledger_entry.candidates):
        raise DevPersistenceValidationError(
            "frame_payload.clarification_candidates does not match the "
            "run's recorded ambiguous_candidates resolution ledger entry"
        )


class DevPersistenceService:
    """Persistence seam used by the future Ask Dev API and orchestrator."""

    def __init__(
        self, session: AsyncSession, *, now: Callable[[], datetime] = _utc_now
    ):
        self.session = session
        self._now = now

    async def create_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        current_scope: Mapping[str, Any],
        retention_days: int = 30,
        title: str | None = None,
    ) -> DevConversation:
        if retention_days not in DEV_RETENTION_DAYS:
            raise DevPersistenceValidationError(
                "Ask Dev retention must be exactly 0 or 30 days"
            )
        title = _bounded_text(title, field="title", max_bytes=200)
        scope = _json_copy(dict(current_scope), field="current_scope")
        now = self._now()
        conversation = DevConversation(
            org_id=org_id,
            user_id=user_id,
            title=title,
            current_scope=scope,
            retention_days=retention_days,
            created_at=now,
            updated_at=now,
            expires_at=(now + timedelta(days=30)) if retention_days == 30 else None,
        )
        self.session.add(conversation)
        await self.session.flush()
        return conversation

    async def list_conversation_records(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        limit: int = 50,
        before: datetime | None = None,
        before_id: uuid.UUID | None = None,
    ) -> Sequence[ConversationRecord]:
        if limit < 1 or limit > 100:
            raise DevPersistenceValidationError("limit must be between 1 and 100")
        if (before is None) != (before_id is None):
            raise DevPersistenceValidationError(
                "before and before_id must be provided together"
            )
        now = self._now()
        conditions = [
            DevConversation.org_id == org_id,
            DevConversation.user_id == user_id,
            DevConversation.deleted_at.is_(None),
            (DevConversation.expires_at.is_(None) | (DevConversation.expires_at > now)),
        ]
        if before is not None and before_id is not None:
            conditions.append(
                or_(
                    DevConversation.updated_at < before,
                    and_(
                        DevConversation.updated_at == before,
                        DevConversation.id < before_id,
                    ),
                )
            )
        latest_answer_id = (
            select(DevMessage.answer_id)
            .where(
                DevMessage.conversation_id == DevConversation.id,
                DevMessage.org_id == DevConversation.org_id,
                DevMessage.user_id == DevConversation.user_id,
                DevMessage.role == "assistant",
                DevMessage.answer_id.is_not(None),
            )
            .order_by(DevMessage.created_at.desc(), DevMessage.id.desc())
            .limit(1)
            .scalar_subquery()
        )
        message_count = (
            select(func.count(DevMessage.id))
            .where(
                DevMessage.conversation_id == DevConversation.id,
                DevMessage.org_id == DevConversation.org_id,
                DevMessage.user_id == DevConversation.user_id,
            )
            .scalar_subquery()
        )
        rows = await self.session.execute(
            select(
                DevConversation,
                message_count.label("message_count"),
                latest_answer_id.label("latest_answer_id"),
            )
            .where(and_(*conditions))
            .order_by(DevConversation.updated_at.desc(), DevConversation.id.desc())
            .limit(limit)
        )
        return [
            ConversationRecord(
                conversation=row[0],
                message_count=int(row[1] or 0),
                latest_answer_id=row[2],
            )
            for row in rows.all()
        ]

    async def list_conversations(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        limit: int = 50,
        before: datetime | None = None,
        before_id: uuid.UUID | None = None,
    ) -> Sequence[DevConversation]:
        records = await self.list_conversation_records(
            org_id=org_id,
            user_id=user_id,
            limit=limit,
            before=before,
            before_id=before_id,
        )
        return [record.conversation for record in records]

    async def get_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> DevConversation:
        conversation = await self._owned_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            include_expired=False,
        )
        if conversation is None:
            raise DevPersistenceNotFound("conversation not found")
        return conversation

    async def get_conversation_record(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> ConversationRecord:
        conversation = await self.get_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conversation_id
        )
        message_count = await self.session.scalar(
            select(func.count(DevMessage.id)).where(
                DevMessage.conversation_id == conversation.id,
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
            )
        )
        latest_answer_id = await self.session.scalar(
            select(DevMessage.answer_id)
            .where(
                DevMessage.conversation_id == conversation.id,
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
                DevMessage.role == "assistant",
                DevMessage.answer_id.is_not(None),
            )
            .order_by(DevMessage.created_at.desc(), DevMessage.id.desc())
            .limit(1)
        )
        return ConversationRecord(
            conversation=conversation,
            message_count=int(message_count or 0),
            latest_answer_id=latest_answer_id,
        )

    async def list_transcript_records(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        limit: int = 50,
        after: datetime | None = None,
        after_id: uuid.UUID | None = None,
    ) -> TranscriptPage:
        """Return only safe persisted user questions and validated answers."""

        if limit < 1 or limit > 100:
            raise DevPersistenceValidationError("limit must be between 1 and 100")
        if (after is None) != (after_id is None):
            raise DevPersistenceValidationError(
                "after and after_id must be provided together"
            )
        conversation = await self.get_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
        )
        # Retention-zero conversations deliberately have no readable history,
        # including while their accepted run is still in flight.
        if conversation.retention_days == 0:
            raise DevPersistenceNotFound("conversation transcript not retained")

        conditions = [
            DevMessage.conversation_id == conversation_id,
            DevMessage.org_id == org_id,
            DevMessage.user_id == user_id,
        ]
        if after is not None and after_id is not None:
            conditions.append(
                or_(
                    DevMessage.created_at > after,
                    and_(
                        DevMessage.created_at == after,
                        DevMessage.id > after_id,
                    ),
                )
            )
        messages = list(
            (
                await self.session.scalars(
                    select(DevMessage)
                    .where(and_(*conditions))
                    .order_by(DevMessage.created_at, DevMessage.id)
                    .limit(limit + 1)
                )
            ).all()
        )
        has_more = len(messages) > limit
        messages = messages[:limit]
        if not messages:
            return TranscriptPage(records=(), has_more=False)

        user_message_ids = [
            message.id for message in messages if message.role == "user"
        ]
        answer_ids = [
            message.answer_id
            for message in messages
            if message.role == "assistant" and message.answer_id is not None
        ]
        runs = list(
            (
                await self.session.scalars(
                    select(DevRun).where(
                        DevRun.org_id == org_id,
                        DevRun.user_id == user_id,
                        DevRun.conversation_id == conversation_id,
                        or_(
                            DevRun.user_message_id.in_(user_message_ids),
                            DevRun.answer_id.in_(answer_ids),
                        ),
                    )
                )
            ).all()
        )
        by_user_message = {
            run.user_message_id: run for run in runs if run.user_message_id is not None
        }
        by_answer = {run.answer_id: run for run in runs if run.answer_id is not None}
        records: list[TranscriptRecord] = []
        for message in messages:
            if message.role == "user":
                run = by_user_message.get(message.id)
            elif message.answer_id is not None:
                run = by_answer.get(message.answer_id)
            else:
                run = None
            if run is None:
                raise DevPersistenceConflict(
                    "transcript message has no tenant-owned run"
                )
            records.append(TranscriptRecord(message=message, run=run))
        return TranscriptPage(records=records, has_more=has_more)

    async def get_message_run_by_client_id(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        client_message_id: uuid.UUID,
    ) -> MessageRunResult:
        result = await self._message_run_by_client_id(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            client_message_id=client_message_id,
        )
        if result is None:
            raise DevPersistenceNotFound("message run not found")
        return result

    async def get_answer_frame(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
    ) -> DevAnswerFrame | None:
        """Return the tenant-owned answer frame for one run, if any.

        Used by the replay path (``router._replayed_result``) to reconstruct
        a no-answer-payload outcome (``needs_clarification``/``not_found``/
        etc.) from the stored frame rather than a generic error, without
        rerunning any provider or tool (TRD v2 Section 12).
        """

        return await self.session.scalar(
            select(DevAnswerFrame).where(
                DevAnswerFrame.run_id == run_id,
                DevAnswerFrame.org_id == org_id,
                DevAnswerFrame.user_id == user_id,
            )
        )

    async def get_run_narrative(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
    ) -> DevRunNarrative | None:
        """Return the tenant-owned narrative for one run, if any (CHAOS-3297).

        Symmetric to ``get_answer_frame``: replay needs to reconstruct a
        content-bearing answer's narrative without a second provider call,
        exactly as it already reconstructs a no-answer frame. 0..1 rows per
        run (``uq_dev_run_narratives_run``).
        """

        return await self.session.scalar(
            select(DevRunNarrative).where(
                DevRunNarrative.run_id == run_id,
                DevRunNarrative.org_id == org_id,
                DevRunNarrative.user_id == user_id,
            )
        )

    async def get_answer_message(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        answer_id: uuid.UUID,
    ) -> DevMessage:
        answer = await self.session.scalar(
            select(DevMessage).where(
                DevMessage.answer_id == answer_id,
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
                DevMessage.role == "assistant",
            )
        )
        if answer is None:
            raise DevPersistenceNotFound("answer not found")
        return answer

    async def list_prompt_history_messages(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        exclude_message_id: uuid.UUID,
        limit: int,
    ) -> Sequence[DevMessage]:
        """Return a bounded chronological suffix for safe prompt projection."""

        if limit < 1 or limit > 100:
            raise DevPersistenceValidationError(
                "history limit must be between 1 and 100"
            )
        await self.get_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
        )
        messages = list(
            (
                await self.session.scalars(
                    select(DevMessage)
                    .where(
                        DevMessage.conversation_id == conversation_id,
                        DevMessage.org_id == org_id,
                        DevMessage.user_id == user_id,
                        DevMessage.id != exclude_message_id,
                    )
                    .order_by(DevMessage.created_at.desc(), DevMessage.id.desc())
                    .limit(limit)
                )
            ).all()
        )
        messages.reverse()
        return messages

    async def rename_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        title: str | None,
    ) -> DevConversation:
        conversation = await self.get_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conversation_id
        )
        conversation.title = _bounded_text(title, field="title", max_bytes=200)
        self._touch(conversation)
        await self.session.flush()
        return conversation

    async def append_user_message_and_run(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        client_message_id: uuid.UUID,
        question: str,
        scope_snapshot: Mapping[str, Any],
        request_id: uuid.UUID | None = None,
        retry_of_run_id: uuid.UUID | None = None,
        admission_limits: DevAdmissionLimits | None = None,
        provider_source: str | None = None,
        platform_allowance: DevPlatformAllowance | None = None,
    ) -> MessageRunResult:
        question = _bounded_text(question, field="question", max_bytes=8192) or ""
        if not question.strip():
            raise DevPersistenceValidationError("question must not be empty")
        scope = _json_copy(dict(scope_snapshot), field="scope_snapshot")
        conversation = await self.get_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conversation_id
        )

        existing = await self._message_run_by_client_id(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            client_message_id=client_message_id,
        )
        if existing is not None:
            return existing

        if retry_of_run_id is not None:
            retry_target = await self._owned_run(
                org_id=org_id,
                user_id=user_id,
                run_id=retry_of_run_id,
            )
            if retry_target is None or retry_target.conversation_id != conversation_id:
                raise DevPersistenceNotFound("retry run not found")
            if retry_target.state not in _TERMINAL_RUN_STATES:
                raise DevPersistenceValidationError("retry run is not terminal")

        if admission_limits is not None or platform_allowance is not None:
            await self._lock_admission_scope(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
            )
            existing = await self._message_run_by_client_id(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                client_message_id=client_message_id,
            )
            if existing is not None:
                return existing
            if admission_limits is not None:
                await self._enforce_admission_limits(
                    org_id=org_id,
                    user_id=user_id,
                    limits=admission_limits,
                )
            if platform_allowance is not None:
                if provider_source != "platform":
                    raise DevPersistenceValidationError(
                        "platform allowance requires a platform provider source"
                    )
                await self._enforce_platform_allowance(
                    org_id=org_id,
                    allowance=platform_allowance,
                )

        message = DevMessage(
            conversation_id=conversation_id,
            org_id=org_id,
            user_id=user_id,
            client_message_id=client_message_id,
            role="user",
            content=question,
            scope_snapshot=scope,
            created_at=self._now(),
        )
        run = DevRun(
            request_id=request_id or uuid.uuid4(),
            conversation_id=conversation_id,
            org_id=org_id,
            user_id=user_id,
            state="accepted",
            provider_source=provider_source,
            retry_of_run_id=retry_of_run_id,
            started_at=self._now(),
            created_at=self._now(),
        )
        try:
            async with self.session.begin_nested():
                self.session.add(message)
                await self.session.flush()
                run.user_message_id = message.id
                self.session.add(run)
                await self.session.flush()
        except IntegrityError:
            existing = await self._message_run_by_client_id(
                org_id=org_id,
                user_id=user_id,
                conversation_id=conversation_id,
                client_message_id=client_message_id,
            )
            if existing is None:
                raise
            return existing

        conversation.current_scope = scope
        self._touch(conversation)
        return MessageRunResult(message=message, run=run, created=True)

    async def append_assistant_answer(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        answer_payload: Mapping[str, Any],
        validator: AnswerPayloadValidator,
        scope_snapshot: Mapping[str, Any],
        rendered_content: str | None = None,
    ) -> DevMessage:
        conversation = await self.get_conversation(
            org_id=org_id, user_id=user_id, conversation_id=conversation_id
        )
        try:
            validated = validator(answer_payload)
        except Exception as exc:
            raise DevPersistenceValidationError(
                "answer payload validation failed"
            ) from exc
        payload = _json_copy(
            dict(validated),
            field="answer_payload",
        )
        if payload.get("schema_version") not in {"dev_answer.v1", "dev_answer.v2"}:
            raise DevPersistenceValidationError(
                "validated answer payload must use dev_answer.v1 or dev_answer.v2"
            )
        try:
            answer_id = uuid.UUID(str(payload["answer_id"]))
            payload_conversation_id = uuid.UUID(str(payload["conversation_id"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise DevPersistenceValidationError(
                "validated answer payload is missing canonical identifiers"
            ) from exc
        if payload_conversation_id != conversation_id:
            raise DevPersistenceValidationError(
                "validated answer conversation does not match storage target"
            )
        existing = await self.session.scalar(
            select(DevMessage).where(
                DevMessage.answer_id == answer_id,
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
                DevMessage.conversation_id == conversation_id,
                DevMessage.role == "assistant",
            )
        )
        if existing is not None:
            return existing
        content = _bounded_text(
            rendered_content, field="rendered_content", max_bytes=32 * 1024
        )
        message = DevMessage(
            conversation_id=conversation_id,
            org_id=org_id,
            user_id=user_id,
            role="assistant",
            content=content,
            answer_id=answer_id,
            answer_payload=payload,
            scope_snapshot=_json_copy(dict(scope_snapshot), field="scope_snapshot"),
            created_at=self._now(),
        )
        try:
            async with self.session.begin_nested():
                self.session.add(message)
                await self.session.flush()
        except IntegrityError:
            existing = await self.session.scalar(
                select(DevMessage).where(
                    DevMessage.answer_id == answer_id,
                    DevMessage.org_id == org_id,
                    DevMessage.user_id == user_id,
                    DevMessage.conversation_id == conversation_id,
                    DevMessage.role == "assistant",
                )
            )
            if existing is None:
                raise
            return existing
        self._touch(conversation)
        return message

    async def record_run_diagnostics(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        preflight_outcome: str | None = None,
        legacy_guard_reason: str | None = None,
    ) -> DevRun | None:
        """Write the CHAOS-3292 content-free diagnostics on one run row.

        Deliberately **not** part of ``update_run``: that method rewrites every
        field it is given, clearing any it is not, so folding these in would
        make an ordinary state transition wipe them. Both values are validated
        as safe identifier tokens, so neither can carry question text, an
        entity name, or catalog content even if a caller passed one.

        A non-``None`` value never reverts to ``None``: the preflight records
        its outcome once, and the demoted legacy guard may later add its reason
        to the same row without erasing it.
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if preflight_outcome is not None:
            run.preflight_outcome = _safe_token(
                preflight_outcome, field="preflight_outcome", max_bytes=32
            )
        if legacy_guard_reason is not None:
            run.legacy_guard_reason = _safe_token(
                legacy_guard_reason, field="legacy_guard_reason", max_bytes=64
            )
        await self.session.flush()
        return run

    async def update_run(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        state: str,
        answer_id: uuid.UUID | None = None,
        terminal_reason: str | None = None,
        provider_source: str | None = None,
        provider_fingerprint: str | None = None,
        model_fingerprint: str | None = None,
        prompt_version: str | None = None,
        tool_contract_version: str | None = None,
        metric_version: str | None = None,
        query_version: str | None = None,
        latency_ms: int | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        estimated_cost_microusd: int | None = None,
        tool_call_count: int | None = None,
        citation_count: int | None = None,
        metric_count: int | None = None,
        grounding_validation_status: str | None = None,
        safe_error_code: str | None = None,
        terminal_error_payload: Mapping[str, Any] | None = None,
        narrative_mode: str | None = None,
        narrative_failure_code: str | None = None,
    ) -> DevRun | None:
        if state not in _RUN_STATES:
            raise DevPersistenceValidationError("invalid run state")
        if provider_source not in {None, "platform", "byo"}:
            raise DevPersistenceValidationError("invalid provider source")
        # CHAOS-3297 stack #4: dev_runs.narrative_mode mirrors
        # dev_run_narratives.mode's own closed vocabulary
        # (_NARRATIVE_MODES) -- the run-level column is a fast-fail UX/
        # replay-idempotency signal, not a second source of truth, so it
        # cannot legally diverge from what a DevRunNarrative row (if any)
        # would say. narrative_failure_code's closed vocabulary is
        # contracts_v2.base.NarrativeFailureCode (CHAOS-3297 codex NO-SHIP
        # finding round 1, MEDIUM #3): Python enforces it here as fast-fail
        # UX, and a DB CHECK constraint (migration 0083,
        # ck_dev_runs_narrative_failure_code) enforces it at the one
        # boundary every write path -- this method, an ORM write that
        # bypasses it, a bulk write, or a raw connection -- must cross to
        # become a row, the same posture migration 0080 established for
        # dev_answer_frames/dev_run_narratives.payload. Unlike that
        # payload's nested-key cross-checks, a scalar column-vs-fixed-list
        # comparison needs no trigger function, so a plain CHECK suffices
        # here.
        if narrative_mode is not None and narrative_mode not in _NARRATIVE_MODES:
            raise DevPersistenceValidationError("invalid narrative mode")
        if (
            narrative_failure_code is not None
            and narrative_failure_code not in _NARRATIVE_FAILURE_CODES
        ):
            raise DevPersistenceValidationError("invalid narrative failure code")
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if run.state in _TERMINAL_RUN_STATES:
            if run.state == state:
                return run
            raise DevPersistenceConflict("terminal run state cannot be changed")
        for value, field in (
            (latency_ms, "latency_ms"),
            (input_tokens, "input_tokens"),
            (output_tokens, "output_tokens"),
            (estimated_cost_microusd, "estimated_cost_microusd"),
            (tool_call_count, "tool_call_count"),
            (citation_count, "citation_count"),
            (metric_count, "metric_count"),
        ):
            if value is not None and value < 0:
                raise DevPersistenceValidationError(f"{field} must be nonnegative")

        # CHAOS-3297 Codex review round 2 MEDIUM: every bound/validate call
        # below must run to completion *before* a single `run.*` attribute
        # is touched. Validating in the middle of a sequence of mutations
        # (the previous shape) meant a legal-but-large terminal_error_payload
        # raising here left `run.state`/`run.answer_id`/etc already mutated
        # on the still-open session; the SSE layer catches this exception
        # and never re-raises it to the request's session dependency, whose
        # teardown then commits those partial mutations anyway -- silently
        # persisting a "terminal" v2 run with terminal_error_payload=NULL,
        # which replay can no longer serve verbatim. Computing every derived
        # value into a local first makes this function atomic: either every
        # value validates and every mutation happens, or the first failure
        # raises with `run` completely untouched.
        safe_terminal_reason = _safe_token(
            terminal_reason, field="terminal_reason", max_bytes=64
        )
        safe_provider_fingerprint = _digest(
            provider_fingerprint, field="provider_fingerprint"
        )
        safe_model_fingerprint = _digest(model_fingerprint, field="model_fingerprint")
        safe_prompt_version = _safe_token(
            prompt_version, field="prompt_version", max_bytes=128
        )
        safe_tool_contract_version = _safe_token(
            tool_contract_version, field="tool_contract_version", max_bytes=128
        )
        safe_metric_version = _safe_token(
            metric_version, field="metric_version", max_bytes=128
        )
        safe_query_version = _safe_token(
            query_version, field="query_version", max_bytes=128
        )
        safe_grounding_validation_status = _safe_token(
            grounding_validation_status,
            field="grounding_validation_status",
            max_bytes=32,
        )
        safe_error_code_value = _safe_token(
            safe_error_code, field="safe_error_code", max_bytes=64
        )
        # Enum membership was already checked above (raises before any
        # mutation on an invalid value); this only re-applies the same
        # shape/size bound every other safe token gets.
        safe_narrative_failure_code = _safe_token(
            narrative_failure_code, field="narrative_failure_code", max_bytes=64
        )
        # CHAOS-3297 (0079): the exact terminal v1 DevError, so idempotent
        # replay can reuse it verbatim instead of reconstructing an
        # approximation from the frame (Codex review HIGH #1).
        # CHAOS-3297 Codex review round 3 CLASS A: the acceptance predicate
        # is contract validity itself -- DevError.model_validate succeeding
        # is what admits a payload, not a byte cap (see
        # _TERMINAL_ERROR_PAYLOAD_BACKSTOP_BYTES's own comment for why a
        # byte cap was the wrong shape of check here). The canonical
        # round-tripped dump is what gets persisted, not the caller's raw
        # dict, so a validated-but-differently-key-ordered payload can never
        # diverge from what DevError itself considers this object to be.
        # _TERMINAL_ERROR_PAYLOAD_BACKSTOP_BYTES remains only as a loose
        # anti-runaway guard that a valid dev_error.v1 can never trip.
        bounded_terminal_error_payload: dict[str, Any] | None = None
        if terminal_error_payload is not None:
            try:
                validated_terminal_error = DevError.model_validate(
                    terminal_error_payload
                )
            except PydanticValidationError as exc:
                raise DevPersistenceValidationError(
                    f"terminal_error_payload is not a valid dev_error.v1: {exc}"
                ) from exc
            canonical_terminal_error_payload = validated_terminal_error.model_dump(
                mode="json"
            )
            encoded_terminal_error_bytes = len(
                json.dumps(
                    canonical_terminal_error_payload,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            )
            if encoded_terminal_error_bytes > _TERMINAL_ERROR_PAYLOAD_BACKSTOP_BYTES:
                raise DevPersistenceValidationError(
                    "terminal_error_payload exceeds the anti-runaway backstop"
                )
            bounded_terminal_error_payload = canonical_terminal_error_payload

        # Every value above validated cleanly -- now mutate.
        run.state = state
        run.answer_id = answer_id
        run.terminal_reason = safe_terminal_reason
        run.provider_source = provider_source
        run.provider_fingerprint = safe_provider_fingerprint
        run.model_fingerprint = safe_model_fingerprint
        run.prompt_version = safe_prompt_version
        run.tool_contract_version = safe_tool_contract_version
        run.metric_version = safe_metric_version
        run.query_version = safe_query_version
        run.latency_ms = latency_ms
        run.input_tokens = input_tokens
        run.output_tokens = output_tokens
        run.estimated_cost_microusd = estimated_cost_microusd
        if tool_call_count is not None:
            run.tool_call_count = tool_call_count
        if citation_count is not None:
            run.citation_count = citation_count
        if metric_count is not None:
            run.metric_count = metric_count
        run.grounding_validation_status = safe_grounding_validation_status
        run.safe_error_code = safe_error_code_value
        run.terminal_error_payload = bounded_terminal_error_payload
        run.narrative_mode = narrative_mode
        run.narrative_failure_code = safe_narrative_failure_code
        if state in _TERMINAL_RUN_STATES:
            run.ended_at = self._now()
        await self.session.flush()

        conversation = await self._owned_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=run.conversation_id,
            include_expired=True,
        )
        if (
            state in _TERMINAL_RUN_STATES
            and conversation is not None
            and conversation.retention_days == 0
        ):
            await self._purge_conversation(
                conversation=conversation,
                reason="ephemeral_completed",
                actor_user_id=user_id,
            )
            return None
        return run

    async def force_terminal_fallback(
        self, *, org_id: uuid.UUID, user_id: uuid.UUID, run_id: uuid.UUID
    ) -> None:
        """Last-resort durability guarantee (CHAOS-3297 Codex review round 3
        Finding 2).

        ``finish()`` (``orchestrator.py``) can flush an answer and/or a
        frame, then have its own ``terminal()``/``update_run`` write fail
        for a reason unrelated to input validity (a dropped connection, a
        constraint violation, any database-layer failure) -- on a session
        that failure may have left rollback-only. The SSE layer
        (``streaming.stream_orchestrator``) catches that exception and
        converts it to a generic ``internal_error`` event without
        re-raising to the request's own session-dependency teardown, which
        then commits whatever was already flushed -- artifacts and a
        ``contract_generation = 'v2'`` tag on a run stuck in a
        *non-terminal* ``state`` forever, since ``update_run`` never ran to
        completion.

        Callers invoke this from a **fresh session** bound to a new
        connection (never ``self.session``, which the original failure may
        have poisoned) after that failure propagates, forcing the run into
        a safe terminal state unconditionally. Idempotent: a no-op if the
        run is already terminal (``finish()`` actually completed and
        something else, unrelated, raised afterward) or does not exist.
        Deliberately minimal -- only the columns needed to leave the run in
        a coherent terminal shape for replay; every other field it already
        wrote (or didn't) is left as-is.
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None or run.state in _TERMINAL_RUN_STATES:
            return
        run.state = "failed"
        run.safe_error_code = "internal_error"
        run.terminal_error_payload = None
        run.ended_at = self._now()
        await self.session.commit()

    async def recover_stale_non_terminal_run(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        stale_after: timedelta,
    ) -> DevRun | None:
        """Recovery at the point of manifestation for the idempotent-replay
        path (CHAOS-3297 Codex review round 5 HIGH closure).

        ``force_terminal_fallback`` is the last-resort write from
        ``run_with_events``'s own except-block; if it ALSO fails on the
        same DB incident (a dropped connection outlives the request), a
        run can be committed non-terminal forever. Every subsequent
        duplicate ``client_message_id`` POST replays only TERMINAL runs
        and 409s otherwise -- with no fallback attempt left to run, that
        409 never stops. This is the second-chance recovery for exactly
        that scenario, invoked from the replay path itself, at the moment
        a caller actually asks for this run again, rather than a
        background sweep discovering it "eventually".

        Takes a row lock (``SELECT ... FOR UPDATE``) on the run before
        looking at its state or age, so a genuinely still-running request
        racing this call cannot have its own terminal write clobbered
        out from under it -- this call and the real completion cannot
        both proceed against the same row at once.

        The lock alone is not enough, though: if ``self.session`` already
        has this run's identity mapped from an EARLIER load in the same
        session (the request's own ``service.session`` typically has --
        ``append_user_message_and_run`` loaded it earlier in this same
        request), SQLAlchemy's default loader behavior returns that SAME
        cached Python object without refreshing its attributes from the
        row this query just locked (CHAOS-3297 Codex review round 7
        HIGH). Without ``populate_existing=True`` below, a run that
        genuinely completed between the caller's stale read and this
        call would have its real outcome silently overwritten with
        ``failed``/``internal_error`` -- the user saw success on the live
        stream, and every replay after that would serve
        ``internal_error`` forever.

        Returns:

        * ``None`` if the run does not exist, or is non-terminal and
          younger than ``stale_after`` -- a real in-flight run. The
          caller must still 409.
        * the run, forced terminal (``failed`` / ``internal_error``, the
          same shape ``force_terminal_fallback`` uses) and committed, if
          it was non-terminal and at least ``stale_after`` old.
        * the run as-is, already terminal, if it completed (via this
          call on an earlier replay, the original request after all, or
          a genuinely concurrent request) between the caller's own read
          and this lock -- always the freshly locked row, never a stale
          cached instance.
        """

        run = await self.session.scalar(
            select(DevRun)
            .where(
                DevRun.id == run_id,
                DevRun.org_id == org_id,
                DevRun.user_id == user_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if run is None:
            return None
        if run.state in _TERMINAL_RUN_STATES:
            return run
        # SQLite (test harness only -- Postgres always round-trips
        # DateTime(timezone=True) as aware) reads a naive datetime back
        # even though it was written aware; normalize before comparing.
        started_at = run.started_at
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        if self._now() - started_at < stale_after:
            return None
        run.state = "failed"
        run.safe_error_code = "internal_error"
        run.terminal_error_payload = None
        run.ended_at = self._now()
        await self.session.commit()
        return run

    async def append_tool_call(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        ordinal: int,
        tool_id: str,
        tool_version: str,
        canonical_input_hash: str,
        safe_scope_summary: Mapping[str, Any],
        status: str,
        result_digest: str | None = None,
        evidence_ref_ids: Sequence[str] = (),
        latency_ms: int | None = None,
        row_count: int | None = None,
        item_count: int | None = None,
        byte_count: int | None = None,
        safe_error_code: str | None = None,
    ) -> DevToolCall:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if ordinal < 0 or ordinal > 32767:
            raise DevPersistenceValidationError("tool ordinal is out of range")
        if status not in {"pending", "running", "completed", "failed", "cancelled"}:
            raise DevPersistenceValidationError("invalid tool-call status")
        for value, field in (
            (latency_ms, "latency_ms"),
            (row_count, "row_count"),
            (item_count, "item_count"),
            (byte_count, "byte_count"),
        ):
            if value is not None and value < 0:
                raise DevPersistenceValidationError(f"{field} must be nonnegative")
        references = [
            _safe_token(str(value), field="evidence_ref_id", max_bytes=128) or ""
            for value in evidence_ref_ids
        ]
        if len(references) > 25:
            raise DevPersistenceValidationError("too many evidence references")
        tool_call = DevToolCall(
            run_id=run.id,
            conversation_id=run.conversation_id,
            org_id=org_id,
            user_id=user_id,
            ordinal=ordinal,
            tool_id=_safe_token(tool_id, field="tool_id", max_bytes=96) or "",
            tool_version=_safe_token(tool_version, field="tool_version", max_bytes=64)
            or "",
            canonical_input_hash=_digest(
                canonical_input_hash, field="canonical_input_hash"
            )
            or "",
            safe_scope_summary=_safe_count_summary(
                safe_scope_summary, field="safe_scope_summary"
            ),
            status=status,
            result_digest=_digest(result_digest, field="result_digest"),
            evidence_ref_ids=references,
            latency_ms=latency_ms,
            row_count=row_count,
            item_count=item_count,
            byte_count=byte_count,
            safe_error_code=_safe_token(
                safe_error_code, field="safe_error_code", max_bytes=64
            ),
            created_at=self._now(),
            completed_at=self._now()
            if status in {"completed", "failed", "cancelled"}
            else None,
        )
        self.session.add(tool_call)
        await self.session.flush()
        return tool_call

    # -- Wave 3.1 (CHAOS-3299) recorder methods ------------------------------
    # Each of these persists one bounded, validated artifact of the server-
    # owned intent -> resolution -> plan -> investigation -> frame ->
    # narrative lifecycle. All are called from PersistenceRunRecorder on the
    # live (non-replay) run branch only, before the terminal update_run
    # transition -- the same ordering `append_tool_call` already relies on
    # for 0-day ephemeral retention to cascade through fully-written rows
    # rather than orphaning a write after the conversation is already purged.

    async def record_intent(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        intent_id: str,
        cardinality: str,
        requires_clarification: bool,
        interpreter_version: str,
        payload: Mapping[str, Any],
    ) -> DevRunIntent:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if intent_id not in _QUESTION_INTENT_IDS:
            raise DevPersistenceValidationError("invalid intent_id")
        if cardinality not in _CARDINALITIES:
            raise DevPersistenceValidationError("invalid cardinality")
        record = DevRunIntent(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            intent_id=intent_id,
            cardinality=cardinality,
            requires_clarification=requires_clarification,
            interpreter_version=_safe_token(
                interpreter_version, field="interpreter_version", max_bytes=128
            )
            or "",
            payload=_bounded_json(
                payload, field="intent_payload", max_bytes=_INTENT_PAYLOAD_MAX_BYTES
            ),
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def append_resolution(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        entry_ordinal: int,
        mention_id: uuid.UUID,
        outcome: str,
        resolved_at: datetime,
        payload: Mapping[str, Any],
    ) -> DevRunResolution:
        """Append one immutable entity-resolution ledger entry.

        INSERT-only by construction: no update method for an existing
        ``(run_id, entry_ordinal)`` row is exposed, and a second insert for
        an already-used ordinal fails via the unique constraint rather than
        upserting.
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if entry_ordinal < 0 or entry_ordinal > 99:
            raise DevPersistenceValidationError("resolution entry_ordinal out of range")
        if outcome not in _RESOLUTION_OUTCOMES:
            raise DevPersistenceValidationError("invalid resolution outcome")
        record = DevRunResolution(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            entry_ordinal=entry_ordinal,
            mention_id=mention_id,
            outcome=outcome,
            payload=_bounded_json(
                payload,
                field="resolution_payload",
                max_bytes=_RESOLUTION_PAYLOAD_MAX_BYTES,
            ),
            resolved_at=resolved_at,
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def record_subject_set(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        set_id: uuid.UUID,
        entity_kind: str,
        cohort_complete: bool,
        fingerprint: str,
        payload: Mapping[str, Any],
    ) -> DevRunSubjectSet:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if entity_kind not in _ENTITY_KINDS:
            raise DevPersistenceValidationError("invalid entity_kind")
        record = DevRunSubjectSet(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            set_id=set_id,
            entity_kind=entity_kind,
            cohort_complete=cohort_complete,
            fingerprint=_safe_token(fingerprint, field="fingerprint", max_bytes=128)
            or "",
            payload=_bounded_json(
                payload,
                field="subject_set_payload",
                max_bytes=_SUBJECT_SET_PAYLOAD_MAX_BYTES,
            ),
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def append_source_observation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        ordinal: int,
        observation_id: uuid.UUID,
        source_class: str,
        requirement_level: str,
        observed_state: str,
        data_semantics: str,
        usable_fact_count: int,
        sample_count: int | None,
        subject_coverage: float,
        observed_at: datetime,
        payload: Mapping[str, Any],
    ) -> DevRunSourceObservation:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if ordinal < 0 or ordinal > 24:
            raise DevPersistenceValidationError(
                "source observation ordinal out of range"
            )
        if source_class not in _SOURCE_CLASSES:
            raise DevPersistenceValidationError("invalid source_class")
        if requirement_level not in _REQUIREMENT_LEVELS:
            raise DevPersistenceValidationError("invalid requirement_level")
        if observed_state not in _SOURCE_OBSERVED_STATES:
            raise DevPersistenceValidationError("invalid observed_state")
        if data_semantics not in _DATA_SEMANTICS:
            raise DevPersistenceValidationError("invalid data_semantics")
        if usable_fact_count < 0:
            raise DevPersistenceValidationError("usable_fact_count must be nonnegative")
        if sample_count is not None and sample_count < 0:
            raise DevPersistenceValidationError("sample_count must be nonnegative")
        if not (0.0 <= subject_coverage <= 1.0):
            raise DevPersistenceValidationError("subject_coverage must be in [0, 1]")
        record = DevRunSourceObservation(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            ordinal=ordinal,
            observation_id=observation_id,
            source_class=source_class,
            requirement_level=requirement_level,
            observed_state=observed_state,
            data_semantics=data_semantics,
            usable_fact_count=usable_fact_count,
            sample_count=sample_count,
            subject_coverage=subject_coverage,
            payload=_bounded_json(
                payload,
                field="source_observation_payload",
                max_bytes=_SOURCE_OBSERVATION_PAYLOAD_MAX_BYTES,
            ),
            observed_at=observed_at,
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def record_investigation_result(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        completed_steps: Sequence[str],
        skipped_steps: Sequence[str],
        failed_steps: Sequence[str],
        relationship_closure_verified: bool,
    ) -> DevRun:
        """Persist the ``dev_investigation_result.v1`` step partition + closure bit.

        Folded (orchestrator decision, CHAOS-3299): an earlier revision of
        this branch modeled this as a dedicated
        ``dev_run_investigation_results`` table wrapping
        (result_id, relationship_closure_verified, payload, completed_at).
        Observations are already persisted 1:N via
        ``append_source_observation``, and ``dev_answer_frames`` is the
        replay source of truth -- the only post-terminal facts nothing else
        reconstructs are which plan steps ran (a step can be skipped without
        ever producing an observation) and the closure bit, so those two
        facts are set directly on ``dev_runs`` instead of a ninth table.
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        completed = _bounded_step_list(completed_steps, field="completed_steps")
        skipped = _bounded_step_list(skipped_steps, field="skipped_steps")
        failed = _bounded_step_list(failed_steps, field="failed_steps")
        if (
            (set(completed) & set(skipped))
            or (set(completed) & set(failed))
            or (set(skipped) & set(failed))
        ):
            raise DevPersistenceValidationError(
                "a plan step cannot be in more than one of completed/skipped/failed"
            )
        run.plan_step_partition = _bounded_json(
            {"completed": completed, "skipped": skipped, "failed": failed},
            field="plan_step_partition",
            max_bytes=_PLAN_STEP_PARTITION_MAX_BYTES,
        )
        run.relationship_closure_verified = relationship_closure_verified
        await self.session.flush()
        return run

    async def _construct_validated_payload_row(
        self,
        *,
        model_cls: Any,
        payload_dict: Mapping[str, Any],
        field_name: str,
        max_bytes: int,
        **extra_columns: Any,
    ) -> Any:
        """The **one** place ``DevPersistenceService`` constructs a
        payload-bearing ORM row (CHAOS-3297 Codex review round 5 MEDIUM
        closure).

        Every validated sink (``record_frame``, ``record_narrative``)
        routes its already-validated, already-cross-checked canonical
        payload dict through here rather than touching a payload-bearing
        model's ``payload`` column anywhere in its own body. The totality
        scanner
        (``test_every_payload_field_reference_is_confined_to_the_audited_helper``)
        denies by default: it scans every OTHER method on this class for
        *any* reference to the literal column name ``"payload"`` --
        a keyword argument, an attribute-assignment target, or a
        dict-literal key -- which covers direct construction
        (``Model(payload=x)``), ``**kwargs``-splat construction
        (``Model(**{"payload": x})``), ORM ``update(Model).values(payload=x)``
        calls, and direct attribute assignment (``row.payload = x``) alike,
        with no write-form taxonomy to keep in sync as new bypass shapes
        are discovered. A method outside this helper (and the explicitly
        filed ``_KNOWN_UNVALIDATED_PAYLOAD_SINKS`` gap) that mentions
        ``"payload"`` in any of those shapes fails that test at collection
        time, before it ever runs against a database.
        """

        bounded = _bounded_json(payload_dict, field=field_name, max_bytes=max_bytes)
        return model_cls(payload=bounded, **extra_columns)

    async def record_frame(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        frame_id: uuid.UUID,
        public_outcome: str,
        payload: Mapping[str, Any],
    ) -> DevAnswerFrame:
        """Persist the canonical ``dev_answer_frame.v1`` for one run.

        One row per terminal run regardless of public outcome -- including
        ``needs_clarification``/``not_found``/``unsupported``/``denied``,
        confirmed by the landed no-answer field policy (see the frame model
        docstring and ``DevRun.plan_step_partition``).

        Atomically tags the owned run ``contract_generation = 'v2'`` and
        ``public_outcome`` in the same flush as the frame write. Writing a
        frame is the definitive, structural signal that a run is v2 --
        recording it without tagging the run left ``contract_generation``
        stuck at its 'v1' default forever, which made
        ``router._replayed_result``'s ``contract_generation == "v2"`` replay
        gate permanently unreachable for every real v2 run (a Codex review
        finding on this branch). This closure also structurally narrows the
        window for CHAOS-3299's downgrade guard (0074) to see folded
        ``dev_runs`` columns on a run still tagged 'v1' -- the guard still
        sweeps those columns directly as defense in depth, since a run can
        carry ``plan_step_partition``/``relationship_closure_verified`` from
        ``record_investigation_result`` without ever reaching this method
        (e.g. a failure between investigation and frame synthesis).

        CHAOS-3297 Codex review round 2 MEDIUM #3: ``payload`` used to be an
        opaque ``Mapping`` bounded only by byte size and a bare
        ``public_outcome`` vocabulary check -- ``{"schema_version":
        "dev_answer_frame.v1"}`` alone would pass both checks, get stored
        verbatim, and tag the run ``contract_generation = 'v2'``, silently
        bypassing every frame-level invariant the contract enforces
        (structural closure, no-answer projection, plan-registry
        membership, ...). ``payload`` is now validated as a real
        ``DevAnswerFrame`` before anything is written, and its own
        ``frame_id``/``run_id``/``public_outcome`` must agree with this
        call's own arguments -- a caller passing a frame for a different
        run, or claiming a different outcome than it argues for, is a
        caller bug, not data to persist quietly.

        CHAOS-3325 Codex review (NO-SHIP, confirmed medium): shape validation
        alone did not prove *provenance* for ``clarification_candidates`` --
        a schema-valid frame could name an entity the resolution ledger
        never authorized. Extends the same cross-check posture one field
        further: see ``_authorize_clarification_candidates``.

        CHAOS-3325 confirmation review (MEDIUM): the row is built from
        ``validated.model_dump(mode="json")`` -- the authorized snapshot --
        never from ``payload``. Every check above runs against ``validated``,
        an immutable contract object, while ``payload`` is the caller's
        mapping and stays mutable throughout; building the row from it meant
        anything that changed it after authorization returned was persisted
        unchecked, which review demonstrated by mutating in that window.
        Persisting what was actually authorized, rather than re-reading the
        input, is the same row-binding lesson as CHAOS-3297 s1's payload-vs-
        row identity closure.
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if public_outcome not in _PUBLIC_OUTCOMES:
            raise DevPersistenceValidationError("invalid public_outcome")
        try:
            validated = DevAnswerFrameContract.model_validate(payload)
        except PydanticValidationError as exc:
            raise DevPersistenceValidationError(
                f"frame_payload is not a valid dev_answer_frame.v1: {exc}"
            ) from exc
        if validated.frame_id != str(frame_id):
            raise DevPersistenceValidationError(
                "frame_payload.frame_id does not match the frame_id argument"
            )
        if validated.run_id != str(run_id):
            raise DevPersistenceValidationError(
                "frame_payload.run_id does not match the run_id argument"
            )
        if validated.public_outcome.value != public_outcome:
            raise DevPersistenceValidationError(
                "frame_payload.public_outcome does not match the "
                "public_outcome argument"
            )
        # CHAOS-3325 Codex review (NO-SHIP, confirmed): the contract only
        # enforces wire shape, not provenance -- see
        # _authorize_clarification_candidates's own docstring. Runs before
        # the row is constructed, so an unauthorized candidate list never
        # reaches the payload-bearing row at all.
        await _authorize_clarification_candidates(
            self.session,
            run_id=run_id,
            org_id=org_id,
            user_id=user_id,
            validated=validated,
        )
        record = await self._construct_validated_payload_row(
            model_cls=DevAnswerFrame,
            payload_dict=validated.model_dump(mode="json"),
            field_name="frame_payload",
            max_bytes=_FRAME_PAYLOAD_MAX_BYTES,
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            frame_id=frame_id,
            public_outcome=public_outcome,
            created_at=self._now(),
        )
        self.session.add(record)
        run.contract_generation = "v2"
        run.public_outcome = public_outcome
        await self.session.flush()
        return record

    async def record_narrative(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        narrative_id: uuid.UUID,
        frame_id: uuid.UUID,
        mode: str,
        provider_fingerprint: str | None,
        narrative_text: str,
        payload: Mapping[str, Any],
    ) -> DevRunNarrative:
        """Persist the canonical ``dev_narrative.v1`` for one run.

        CHAOS-3297 Codex review round 3 CLASS B (record_frame's round 2 fix,
        repeated here): ``payload`` used to be an opaque ``Mapping``,
        bounded only by byte size and forbidden-key scanning -- never
        validated as a real ``DevNarrative``, so a caller could tag a run
        with a narrative row that does not actually satisfy the contract
        (e.g. a ``provider`` mode narrative with no ``provider_metadata``).
        ``payload`` is the narrative dump with ``body`` excluded
        (``narrative_text`` is the separate authoritative copy --
        ``PersistenceRunRecorder.record_narrative``), so the full contract
        object is reconstructed (``body`` reinserted from
        ``narrative_text``) before validating -- validation must exercise
        every field the wire contract enforces, not the subset that happens
        to already be in ``payload``. The validated object's own
        ``narrative_id``/``run_id``/``frame_id``/``mode``/
        ``provider_metadata`` must agree with this call's own arguments;
        any mismatch rejects, no write.
        """

        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if mode not in _NARRATIVE_MODES:
            raise DevPersistenceValidationError("invalid narrative mode")
        frame = await self.session.scalar(
            select(DevAnswerFrame).where(
                DevAnswerFrame.run_id == run.id,
                DevAnswerFrame.org_id == org_id,
                DevAnswerFrame.user_id == user_id,
            )
        )
        if frame is None or frame.frame_id != frame_id:
            raise DevPersistenceValidationError(
                "narrative frame_id must match the run's recorded answer frame"
            )
        reconstructed = dict(payload)
        reconstructed["body"] = narrative_text
        try:
            validated = DevNarrativeContract.model_validate(reconstructed)
        except PydanticValidationError as exc:
            raise DevPersistenceValidationError(
                f"narrative payload is not a valid dev_narrative.v1: {exc}"
            ) from exc
        if validated.narrative_id != str(narrative_id):
            raise DevPersistenceValidationError(
                "narrative payload's narrative_id does not match the "
                "narrative_id argument"
            )
        if validated.run_id != str(run_id):
            raise DevPersistenceValidationError(
                "narrative payload's run_id does not match the run_id argument"
            )
        if validated.frame_id != str(frame_id):
            raise DevPersistenceValidationError(
                "narrative payload's frame_id does not match the frame_id argument"
            )
        if validated.mode != mode:
            raise DevPersistenceValidationError(
                "narrative payload's mode does not match the mode argument"
            )
        # provider_fingerprint arrives already hashed (the caller -- see
        # orchestrator_persistence.PersistenceRunRecorder.record_narrative --
        # digests narrative.provider_metadata.model_fingerprint before this
        # call); _digest here only validates that shape. To cross-check it
        # against the *payload's own* raw model_fingerprint, that raw value
        # must be hashed the identical way, not passed through the
        # shape-only validator.
        expected_provider_fingerprint = (
            _sha256_digest(validated.provider_metadata.model_fingerprint)
            if validated.provider_metadata is not None
            else None
        )
        safe_provider_fingerprint = _digest(
            provider_fingerprint, field="provider_fingerprint"
        )
        if expected_provider_fingerprint != safe_provider_fingerprint:
            raise DevPersistenceValidationError(
                "narrative payload's provider_metadata does not match the "
                "provider_fingerprint argument"
            )
        text = _bounded_text(
            validated.body, field="narrative_text", max_bytes=_NARRATIVE_TEXT_MAX_BYTES
        )
        if not text:
            raise DevPersistenceValidationError("narrative_text must not be empty")
        record = await self._construct_validated_payload_row(
            model_cls=DevRunNarrative,
            payload_dict=validated.model_dump(mode="json", exclude={"body"}),
            field_name="narrative_payload",
            max_bytes=_NARRATIVE_PAYLOAD_MAX_BYTES,
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            narrative_id=narrative_id,
            frame_id=frame_id,
            mode=mode,
            provider_fingerprint=safe_provider_fingerprint,
            narrative_text=text,
            created_at=self._now(),
        )
        # codex NO-SHIP finding round 1 (HIGH #2b): the narrative row is an
        # *optional* write on a session that may already carry a
        # successfully-flushed frame/answer earlier in the same request
        # (orchestrator.finish() calls this after record_frame). A flush
        # failure here (a CHECK-constraint violation, a byte-bound
        # rejection) previously poisoned the WHOLE session -- the
        # terminal() write that follows would then raise
        # PendingRollbackError, and recovery would roll back the
        # already-flushed frame/answer along with it, downgrading an
        # otherwise-valid run. A SAVEPOINT isolates this one flush: on
        # failure, only the savepoint rolls back (automatic, on exception
        # exit from this block) -- the outer transaction, and everything
        # already flushed on it, stays intact and the session stays usable
        # for the terminal() write that follows.
        async with self.session.begin_nested():
            self.session.add(record)
            await self.session.flush()
        return record

    async def append_stage_diagnostic(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        run_id: uuid.UUID,
        ordinal: int,
        stage_id: str,
        status: str,
        latency_ms: int | None,
        counts: Mapping[str, Any],
    ) -> DevRunStageDiagnostic:
        run = await self._owned_run(org_id=org_id, user_id=user_id, run_id=run_id)
        if run is None:
            raise DevPersistenceNotFound("run not found")
        if ordinal < 0 or ordinal > 9:
            raise DevPersistenceValidationError("stage diagnostic ordinal out of range")
        if stage_id not in _STAGE_IDS:
            raise DevPersistenceValidationError("invalid stage_id")
        if status not in _STAGE_STATUSES:
            raise DevPersistenceValidationError("invalid stage status")
        if latency_ms is not None and latency_ms < 0:
            raise DevPersistenceValidationError("latency_ms must be nonnegative")
        record = DevRunStageDiagnostic(
            run_id=run.id,
            org_id=org_id,
            user_id=user_id,
            ordinal=ordinal,
            stage_id=stage_id,
            status=status,
            latency_ms=latency_ms,
            counts=_safe_count_summary(counts, field="stage_diagnostic_counts"),
            created_at=self._now(),
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def record_feedback(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        answer_id: uuid.UUID,
        rating: str,
        reasons: Sequence[str],
        comment: str | None = None,
    ) -> DevFeedback:
        if rating not in {"helpful", "not_helpful"}:
            raise DevPersistenceValidationError("invalid feedback rating")
        normalized_reasons = sorted(set(reasons))
        if not set(normalized_reasons).issubset(_FEEDBACK_REASONS):
            raise DevPersistenceValidationError("invalid feedback reason")
        comment = _bounded_text(comment, field="feedback comment", max_bytes=2048)
        answer = await self.session.scalar(
            select(DevMessage).where(
                DevMessage.answer_id == answer_id,
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
                DevMessage.role == "assistant",
            )
        )
        if answer is None:
            raise DevPersistenceNotFound("answer not found")
        feedback = await self.session.scalar(
            select(DevFeedback).where(
                DevFeedback.answer_id == answer_id,
                DevFeedback.org_id == org_id,
                DevFeedback.user_id == user_id,
            )
        )
        now = self._now()
        if feedback is None:
            feedback = DevFeedback(
                answer_id=answer_id,
                org_id=org_id,
                user_id=user_id,
                rating=rating,
                reasons=normalized_reasons,
                comment=comment,
                created_at=now,
                updated_at=now,
            )
            self.session.add(feedback)
        else:
            feedback.rating = rating
            feedback.reasons = normalized_reasons
            feedback.comment = comment
            feedback.updated_at = now
        await self.session.flush()
        return feedback

    async def delete_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> bool:
        conversation = await self._owned_conversation(
            org_id=org_id,
            user_id=user_id,
            conversation_id=conversation_id,
            include_expired=True,
        )
        if conversation is None:
            return False
        await self._purge_conversation(
            conversation=conversation,
            reason="user_deleted",
            actor_user_id=user_id,
        )
        return True

    async def admin_purge_conversation(
        self,
        *,
        org_id: uuid.UUID,
        target_user_id: uuid.UUID,
        actor_user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> bool:
        conversation = await self._owned_conversation(
            org_id=org_id,
            user_id=target_user_id,
            conversation_id=conversation_id,
            include_expired=True,
        )
        if conversation is None:
            return False
        await self._purge_conversation(
            conversation=conversation,
            reason="admin_purged",
            actor_user_id=actor_user_id,
        )
        return True

    async def purge_user_content(
        self,
        *,
        org_id: uuid.UUID,
        target_user_id: uuid.UUID,
        actor_user_id: uuid.UUID | None,
        admin: bool,
        limit: int = 500,
    ) -> CleanupResult:
        if limit < 1 or limit > 500:
            raise DevPersistenceValidationError("purge limit must be between 1 and 500")
        conversations = (
            (
                await self.session.execute(
                    select(DevConversation)
                    .where(
                        DevConversation.org_id == org_id,
                        DevConversation.user_id == target_user_id,
                    )
                    .order_by(DevConversation.created_at, DevConversation.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        reason = "admin_purged" if admin else "user_deleted"
        for conversation in conversations:
            await self._purge_conversation(
                conversation=conversation,
                reason=reason,
                actor_user_id=actor_user_id,
            )
        return CleanupResult(
            reason=reason,
            selected=len(conversations),
            purged=len(conversations),
        )

    async def cleanup_expired(self, *, limit: int = 500) -> CleanupResult:
        """Purge one retry-safe bounded batch selected by persisted expiry."""

        if limit < 1 or limit > 500:
            raise DevPersistenceValidationError(
                "cleanup limit must be between 1 and 500"
            )
        now = self._now()
        conversations = (
            (
                await self.session.execute(
                    select(DevConversation)
                    .where(
                        DevConversation.expires_at.is_not(None),
                        DevConversation.expires_at <= now,
                    )
                    .order_by(DevConversation.expires_at, DevConversation.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        for conversation in conversations:
            reason = (
                "ephemeral_completed"
                if conversation.retention_days == 0
                else "retention_expired"
            )
            await self._purge_conversation(
                conversation=conversation,
                reason=reason,
                actor_user_id=None,
            )
        logger.info(
            "ask_dev_retention_cleanup_completed",
            extra={"selected": len(conversations), "purged": len(conversations)},
        )
        return CleanupResult(
            reason="retention_expired",
            selected=len(conversations),
            purged=len(conversations),
        )

    async def _owned_conversation(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        include_expired: bool,
    ) -> DevConversation | None:
        conditions = [
            DevConversation.id == conversation_id,
            DevConversation.org_id == org_id,
            DevConversation.user_id == user_id,
            DevConversation.deleted_at.is_(None),
        ]
        if not include_expired:
            conditions.append(
                DevConversation.expires_at.is_(None)
                | (DevConversation.expires_at > self._now())
            )
        return await self.session.scalar(select(DevConversation).where(*conditions))

    async def _owned_run(
        self, *, org_id: uuid.UUID, user_id: uuid.UUID, run_id: uuid.UUID
    ) -> DevRun | None:
        return await self.session.scalar(
            select(DevRun).where(
                DevRun.id == run_id,
                DevRun.org_id == org_id,
                DevRun.user_id == user_id,
            )
        )

    async def _lock_admission_scope(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
    ) -> None:
        """Serialize admission in one fixed organization/user/conversation order."""

        locked_org = await self.session.scalar(
            select(Organization.id).where(Organization.id == org_id).with_for_update()
        )
        locked_user = await self.session.scalar(
            select(User.id).where(User.id == user_id).with_for_update()
        )
        locked_conversation = await self.session.scalar(
            select(DevConversation.id)
            .where(
                DevConversation.id == conversation_id,
                DevConversation.org_id == org_id,
                DevConversation.user_id == user_id,
                DevConversation.deleted_at.is_(None),
            )
            .with_for_update()
        )
        if locked_org is None or locked_user is None or locked_conversation is None:
            raise DevPersistenceNotFound("admission scope not found")

    async def _enforce_admission_limits(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        limits: DevAdmissionLimits,
    ) -> None:
        now = self._now()
        active = DevRun.state.not_in(_TERMINAL_RUN_STATES)
        active_user = await self.session.scalar(
            select(func.count(DevRun.id)).where(
                DevRun.org_id == org_id,
                DevRun.user_id == user_id,
                active,
            )
        )
        active_org = await self.session.scalar(
            select(func.count(DevRun.id)).where(
                DevRun.org_id == org_id,
                active,
            )
        )
        if (
            int(active_user or 0) >= limits.active_runs_per_user
            or int(active_org or 0) >= limits.active_runs_per_org
        ):
            raise DevConcurrencyLimitExceeded("Ask Dev active-run limit reached")

        user_window = await self.session.scalar(
            select(func.count(DevRun.id)).where(
                DevRun.org_id == org_id,
                DevRun.user_id == user_id,
                DevRun.started_at >= now - timedelta(minutes=15),
            )
        )
        org_window = await self.session.scalar(
            select(func.count(DevRun.id)).where(
                DevRun.org_id == org_id,
                DevRun.started_at >= now - timedelta(hours=1),
            )
        )
        if (
            int(user_window or 0) >= limits.requests_per_user_per_15_minutes
            or int(org_window or 0) >= limits.requests_per_org_per_hour
        ):
            raise DevRateLimitExceeded("Ask Dev request-rate limit reached")

    async def _enforce_platform_allowance(
        self,
        *,
        org_id: uuid.UUID,
        allowance: DevPlatformAllowance,
    ) -> None:
        now = self._now()
        window_start = datetime(now.year, now.month, 1, tzinfo=UTC)
        if now.month == 12:
            reset_at = datetime(now.year + 1, 1, 1, tzinfo=UTC)
        else:
            reset_at = datetime(now.year, now.month + 1, 1, tzinfo=UTC)
        terminal_with_cost = and_(
            DevRun.state.in_(_TERMINAL_RUN_STATES),
            DevRun.estimated_cost_microusd.is_not(None),
        )
        charged_cost = case(
            (terminal_with_cost, DevRun.estimated_cost_microusd),
            else_=allowance.per_run_reservation_microusd,
        )
        statement = select(
            func.count(DevRun.id), func.coalesce(func.sum(charged_cost), 0)
        ).where(
            DevRun.org_id == org_id,
            DevRun.provider_source == "platform",
            DevRun.started_at >= window_start,
            DevRun.started_at < reset_at,
        )
        request_count, charged_microusd = (await self.session.execute(statement)).one()
        if int(request_count or 0) >= allowance.monthly_request_limit:
            raise DevMonthlyRequestLimitExceeded(reset_at)
        if (
            int(charged_microusd or 0) + allowance.per_run_reservation_microusd
            > allowance.monthly_cost_limit_microusd
        ):
            raise DevMonthlyCostLimitExceeded(reset_at)

    async def _message_run_by_client_id(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        client_message_id: uuid.UUID,
    ) -> MessageRunResult | None:
        message = await self.session.scalar(
            select(DevMessage).where(
                DevMessage.org_id == org_id,
                DevMessage.user_id == user_id,
                DevMessage.conversation_id == conversation_id,
                DevMessage.client_message_id == client_message_id,
            )
        )
        if message is None:
            return None
        run = await self.session.scalar(
            select(DevRun).where(
                DevRun.org_id == org_id,
                DevRun.user_id == user_id,
                DevRun.user_message_id == message.id,
            )
        )
        if run is None:
            raise DevPersistenceConflict(
                "client message exists without its idempotent run"
            )
        return MessageRunResult(message=message, run=run, created=False)

    def _touch(self, conversation: DevConversation) -> None:
        now = self._now()
        conversation.updated_at = now
        if conversation.retention_days == 30:
            conversation.expires_at = now + timedelta(days=30)

    async def _purge_conversation(
        self,
        *,
        conversation: DevConversation,
        reason: str,
        actor_user_id: uuid.UUID | None,
    ) -> None:
        existing = await self.session.scalar(
            select(DevConversationTombstone).where(
                DevConversationTombstone.conversation_id == conversation.id
            )
        )
        if existing is None:
            self.session.add(
                DevConversationTombstone(
                    conversation_id=conversation.id,
                    org_id=conversation.org_id,
                    user_id=conversation.user_id,
                    actor_user_id=actor_user_id,
                    reason=reason,
                    retention_days=conversation.retention_days,
                    conversation_created_at=conversation.created_at,
                    deleted_at=self._now(),
                )
            )
            await self.session.flush()
        await self.session.delete(conversation)
        await self.session.flush()
