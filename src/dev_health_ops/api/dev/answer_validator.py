"""Grounding validator for model-produced ``dev_answer.v1`` candidates."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from .contracts import (
    DevAnswer,
    DevContractVersions,
    DevEvidenceRef,
    DevMetricRef,
    DevModelMetadata,
    DevScopeResolution,
    DevToolResult,
)

_NUMBER = re.compile(r"(?<![A-Za-z_])[-+]?\d+(?:[.,]\d+)*(?:%|\b)")
_NON_REPAIRABLE_VALIDATION_MARKERS = (
    "unknown evidence",
    "unknown metric",
    "observed claims require",
    "recommendations require",
    "inferred claims cannot",
)
# "complete answer requires all required sources fresh and available"
# (DevAnswer.validate_answer_invariants) is deliberately repairable, not
# listed above: server-derived coverage overwrites whatever the model
# proposed, so the model can genuinely be one bounded correction away from
# a valid answer -- e.g. it claimed "complete" while a tool's own warning
# said a required source was unavailable. Giving it one repair attempt
# (schema_repairs) lets it reissue the same grounded data under an accurate
# status instead of hard-failing a run that has usable evidence (CHAOS-3257).


_MAX_REPAIR_DETAIL_CHARS = 200


def _bounded_detail(messages: tuple[str, ...], *, limit: int) -> str:
    """Join messages up to `limit` chars without cutting one mid-sentence.

    A naive join-then-slice can end on a fragment like "Extra inputs are
    not" (from a 30-forbidden-field answer), which the repair prompt would
    then present as if it were the complete reason. Keep only whole
    messages that fit and note how many were dropped instead.
    """
    non_empty = [part for part in messages if part]
    kept: list[str] = []
    total = 0
    for index, msg in enumerate(non_empty):
        addition = msg if not kept else f"; {msg}"
        if total + len(addition) > limit:
            if not kept:
                # Even the first whole message doesn't fit: better a
                # visibly-truncated fragment than an empty detail string.
                return msg[:limit]
            omitted = len(non_empty) - index
            return "; ".join(kept) + f" (+{omitted} more)"
        kept.append(msg)
        total += len(addition)
    return "; ".join(kept)


class AnswerValidationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        repairable: bool,
        detail: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.code = code
        self.repairable = repairable
        # Short, safe, model-facing description of exactly what failed, used
        # to build an actionable schema-repair prompt turn (CHAOS-3288)
        # instead of a generic "fix your JSON" instruction the model cannot
        # act on. Bounded and built only from our own raised messages / the
        # `msg` field of pydantic error dicts -- never raw echoed input
        # values, which pydantic keeps in a separate `input`/`ctx` field we
        # do not touch. (One documented exception: pydantic's
        # `iteration_error` embeds the underlying iterator exception's own
        # text in `msg`; DevAnswer has no field whose validation iterates a
        # caller-supplied generator today, so this is not currently
        # reachable, but `msg` is not categorically safe for every possible
        # future field type -- see CHAOS-3288 review notes.)
        self.detail = _bounded_detail(detail, limit=_MAX_REPAIR_DETAIL_CHARS) or message


@dataclass(frozen=True, slots=True)
class AnswerValidationContext:
    conversation_id: str
    answer_id: str
    scope_resolution: DevScopeResolution
    versions: DevContractVersions
    model: DevModelMetadata
    tool_results: tuple[DevToolResult, ...]


def _canonical_objects(
    results: tuple[DevToolResult, ...],
) -> tuple[dict[str, DevEvidenceRef], dict[str, DevMetricRef]]:
    evidence: dict[str, DevEvidenceRef] = {}
    metrics: dict[str, DevMetricRef] = {}
    for result in results:
        for evidence_item in result.evidence:
            existing_evidence = evidence.setdefault(
                evidence_item.evidence_ref_id, evidence_item
            )
            if existing_evidence != evidence_item:
                raise AnswerValidationError(
                    "tool results disagree about an evidence reference",
                    code="answer_validation_failed",
                    repairable=False,
                )
        for metric_item in result.metrics:
            existing_metric = metrics.setdefault(metric_item.metric_ref_id, metric_item)
            if existing_metric != metric_item:
                raise AnswerValidationError(
                    "tool results disagree about a metric reference",
                    code="answer_validation_failed",
                    repairable=False,
                )
    return evidence, metrics


def validate_answer_candidate(
    payload: DevAnswer | dict[str, Any], context: AnswerValidationContext
) -> DevAnswer:
    """Validate schema plus server-issued identity, scope, and grounding invariants."""

    try:
        answer = (
            payload
            if isinstance(payload, DevAnswer)
            else DevAnswer.model_validate(payload)
        )
    except ValidationError as exc:
        # `error["msg"]` is the clean "Value error, <our message>" or
        # "Field required" text; pydantic keeps the echoed input value in a
        # separate `input`/`ctx` key we deliberately never read here, so
        # this cannot leak tool/evidence payload content into the prompt.
        # For a missing required field, `loc` is one of our own fixed
        # dev_answer.v1 field names (never model-echoed content), so naming
        # it makes an otherwise-generic "Field required" actionable; for
        # every other error type we keep `msg` alone rather than risk
        # echoing a model-controlled key (e.g. an unexpected extra field).
        messages = tuple(
            f"{'.'.join(str(part) for part in error['loc'])}: {error.get('msg', '')}"
            if error.get("type") == "missing" and error.get("loc")
            else str(error.get("msg", ""))
            for error in exc.errors()
        )
        # Classify repairability from the same safe messages, never from
        # `str(exc)` -- pydantic's rendered exception text also includes
        # `input_value=...`, so a coincidental echoed input (e.g. the model
        # sending status="unknown metric") could otherwise match one of the
        # markers below and wrongly mark an unrelated error non-repairable.
        details = " ".join(messages).casefold()
        repairable = not any(
            marker in details for marker in _NON_REPAIRABLE_VALIDATION_MARKERS
        )
        raise AnswerValidationError(
            "answer does not conform to dev_answer.v1",
            code="answer_validation_failed",
            repairable=repairable,
            detail=messages,
        ) from exc

    if (
        answer.conversation_id != context.conversation_id
        or answer.answer_id != context.answer_id
    ):
        raise AnswerValidationError(
            "answer identifiers are not server issued",
            code="answer_validation_failed",
            repairable=False,
        )
    if answer.resolved_scope != context.scope_resolution:
        raise AnswerValidationError(
            "answer scope differs from the server-authorized scope",
            code="answer_validation_failed",
            repairable=False,
        )
    if answer.versions != context.versions or answer.model != context.model:
        raise AnswerValidationError(
            "answer runtime metadata differs from server-owned metadata",
            code="answer_validation_failed",
            repairable=False,
        )

    canonical_evidence, canonical_metrics = _canonical_objects(context.tool_results)
    for evidence_item in answer.evidence:
        if canonical_evidence.get(evidence_item.evidence_ref_id) != evidence_item:
            raise AnswerValidationError(
                "answer contains unknown or mutated evidence",
                code="answer_validation_failed",
                repairable=False,
            )
    for metric_item in answer.metrics:
        if canonical_metrics.get(metric_item.metric_ref_id) != metric_item:
            raise AnswerValidationError(
                "answer contains unknown or mutated metrics",
                code="answer_validation_failed",
                repairable=False,
            )

    resolved_scope = context.scope_resolution.resolved_scope
    if resolved_scope is None and answer.claims:
        raise AnswerValidationError(
            "unresolved scope cannot carry factual claims",
            code="answer_validation_failed",
            repairable=False,
        )
    for claim in answer.claims:
        if resolved_scope is not None and claim.validity_scope != resolved_scope:
            raise AnswerValidationError(
                "claim validity scope differs from the authorized scope",
                code="answer_validation_failed",
                repairable=False,
            )
        if _NUMBER.search(claim.text) and not (
            claim.metric_ref_ids or claim.evidence_ref_ids
        ):
            raise AnswerValidationError(
                "numeric claim lacks a metric or source reference",
                code="answer_validation_failed",
                repairable=False,
            )
    return answer


__all__ = [
    "AnswerValidationContext",
    "AnswerValidationError",
    "validate_answer_candidate",
]
