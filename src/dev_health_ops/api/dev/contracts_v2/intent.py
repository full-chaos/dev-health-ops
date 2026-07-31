"""``dev_question_intent.v1`` and ``dev_message_request.v2``.

Amendment TRD v2 §4.1: the interpreter is server-owned. A model may assist
extraction only behind a typed contract and deterministic post-validation;
it cannot choose an unsupported intent or bypass explicit subject mentions.
The browser no longer supplies an authoritative ``question_class`` — v1's
required field becomes, at most, a non-authoritative hint the server must
ignore for planning and record a deprecation diagnostic for.
"""

from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import (
    AwareDatetime,
    Field,
    FiniteFloat,
    StringConstraints,
    model_validator,
)

from dev_health_ops.api.dev.contracts import MetricID, QuestionClass

from .base import (
    Cardinality,
    ContractModelV2,
    EntityKind,
    IdempotencyKey,
    OpaqueID,
    QuestionIntentID,
    ServerHandle,
    ShortText,
)
from .embedded import DevScopeV2

__all__ = ["DevMessageRequestV2", "DevQuestionIntent"]

_QuestionText = Annotated[str, StringConstraints(min_length=1, max_length=8_192)]


class DevMessageRequestV2(ContractModelV2):
    """Request amendment: no authoritative client-supplied ``question_class``.

    ``question_class_hint`` retains the v1 field for migration-window
    compatibility, but it is documented and enforced (by
    ``DevQuestionIntent.client_hint_deprecation_warning``) as untrusted
    input the interpreter must ignore for planning.
    """

    schema_version: Literal["dev_message_request.v2"]
    request_id: ServerHandle
    client_message_id: ServerHandle
    conversation_id: ServerHandle | None = None
    idempotency_key: IdempotencyKey
    retry_of_run_id: ServerHandle | None = None
    question: _QuestionText = Field(json_schema_extra={"x-max-utf8-bytes": 8_192})
    scope: DevScopeV2
    requested_metric_ids: tuple[MetricID, ...] = Field(
        default_factory=tuple, max_length=8
    )
    question_class_hint: QuestionClass | None = None

    @model_validator(mode="after")
    def enforce_utf8_question_bound(self) -> Self:
        if len(self.question.encode("utf-8")) > 8_192:
            raise ValueError("question exceeds 8 KiB UTF-8")
        return self


class DevQuestionIntent(ContractModelV2):
    """Server-owned, authoritative interpretation of one request.

    Every field here is produced by the deterministic interpreter (with
    optional model-assisted extraction behind post-validation); none of it
    is copied verbatim from client input except the client's own
    (non-authoritative) hint, which is preserved only for audit/diagnostic
    purposes.
    """

    schema_version: Literal["dev_question_intent.v1"]
    intent_id: QuestionIntentID
    interpreter_version: Annotated[str, StringConstraints(min_length=1, max_length=128)]
    cardinality: Cardinality
    subject_kinds: tuple[EntityKind, ...] = Field(default_factory=tuple, max_length=5)
    mention_ordinals: tuple[int, ...] = Field(default_factory=tuple, max_length=25)
    requested_dimensions: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=20
    )
    requested_metric_ids: tuple[MetricID, ...] = Field(
        default_factory=tuple, max_length=8
    )
    comparison_mode: Literal[
        "none", "period_over_period", "cohort_relative", "own_history"
    ] = "none"
    ranking_requested: bool = False
    confidence: FiniteFloat = Field(ge=0, le=1)
    interpretation_reasons: tuple[ShortText, ...] = Field(min_length=1, max_length=10)
    requires_clarification: bool
    clarification_reason: ShortText | None = None
    client_question_class_hint: QuestionClass | None = None
    client_hint_deprecation_warning: ShortText | None = None
    generated_at: AwareDatetime

    @model_validator(mode="after")
    def validate_intent_invariants(self) -> Self:
        if len(set(self.mention_ordinals)) != len(self.mention_ordinals):
            raise ValueError("mention ordinals must be unique")
        if self.mention_ordinals and sorted(self.mention_ordinals) != list(
            range(len(self.mention_ordinals))
        ):
            raise ValueError("mention ordinals must be contiguous starting at zero")
        if self.requires_clarification and self.clarification_reason is None:
            raise ValueError("clarification requirement requires a reason")
        if not self.requires_clarification and self.clarification_reason is not None:
            raise ValueError("only a clarification requirement may carry a reason")
        if (
            self.cardinality is Cardinality.PLURAL_COHORT
            and len(self.mention_ordinals) < 2
        ):
            raise ValueError("plural cohort intent requires two or more mentions")
        if self.cardinality is Cardinality.ORGANIZATION_WIDE and self.mention_ordinals:
            raise ValueError("organization-wide intent cannot carry subject mentions")
        # The client-supplied question_class is a non-authoritative hint per
        # the Wave 3.1 request amendment: whenever it is present, the server
        # must record a content-free deprecation diagnostic rather than use
        # it for planning.
        if self.client_question_class_hint is not None and (
            self.client_hint_deprecation_warning is None
        ):
            raise ValueError(
                "a client question_class hint requires a deprecation warning"
            )
        if self.client_question_class_hint is None and (
            self.client_hint_deprecation_warning is not None
        ):
            raise ValueError(
                "a deprecation warning requires a client question_class hint"
            )
        return self
