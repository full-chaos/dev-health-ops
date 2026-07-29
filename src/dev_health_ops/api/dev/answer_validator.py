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
    "complete answer requires",
    "recommendations require",
    "inferred claims cannot",
)


class AnswerValidationError(ValueError):
    def __init__(self, message: str, *, code: str, repairable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.repairable = repairable


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
        details = str(exc).casefold()
        repairable = not any(
            marker in details for marker in _NON_REPAIRABLE_VALIDATION_MARKERS
        )
        raise AnswerValidationError(
            "answer does not conform to dev_answer.v1",
            code="answer_validation_failed",
            repairable=repairable,
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
