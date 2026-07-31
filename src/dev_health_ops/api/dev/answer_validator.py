"""Grounding validator for model-produced ``dev_answer.v1`` candidates."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from .contracts import (
    AnswerStatus,
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

# CHAOS-3290: a complete (or substantively-worded partial) answer must never
# be an empty shell. A degenerate reasoning-exhausted run can still satisfy
# every other invariant above (identity, scope, canonical evidence/metric
# equality all vacuously pass over empty lists) while carrying zero claims,
# zero metrics, and zero evidence -- a silent non-answer presented as
# success. PRD §8 forbids a material status claim without evidence; this is
# the degenerate case where there is no material claim *or* evidence at all.
#
# The floor cannot simply be "claims/metrics/evidence must be non-empty"
# unconditionally: a metric-catalog question (list_metrics.v1 only) has no
# tool output that is representable as a claim, metric, or evidence ref at
# all (DevMetricDefinition has no value to ground a DevMetricRef with, and
# mints no DevEvidenceRef), so a genuinely thorough catalog answer is
# legitimately empty across all three arrays -- the only signal left is
# whether the free-text summary actually reflects retrieved data or is a
# generic stub. Length alone is not that signal: a deterministic catalog
# summary can legitimately be short ("8 Ask Dev metrics are available in
# this scope."), while the real CHAOS-3290 stub ("Available Ask Dev metrics
# and their definitions.") is a *similar* length but names no concrete
# retrieved fact. Requiring at least one number (a count, an id suffix like
# ".v1", anything reflecting the catalog actually being read) plus a small
# absolute floor catches the stub without penalizing a terse-but-real one.
_MIN_CATALOG_SUMMARY_CHARS = 20
_MIN_UNGROUNDED_SUMMARY_CHARS = 150
_GROUNDABLE_TOOL_RESULT_FIELDS = (
    "metrics",
    "evidence",
    "status_facts",
    "pull_requests",
    "ci_checks",
    "deployments",
    "incidents",
)


def _tool_results_offer_groundable_material(
    tool_results: tuple[DevToolResult, ...],
) -> bool:
    """Whether any executed tool in this run returned something an answer
    could have cited as a claim, metric, or evidence reference.

    False only for tool calls whose only possible output is definitional/
    catalog data (currently: list_metrics.v1's ``metric_definitions``),
    which has no representation anywhere in ``dev_answer.v1``.
    """
    return any(
        getattr(result, field)
        for result in tool_results
        for field in _GROUNDABLE_TOOL_RESULT_FIELDS
    )


def _bounded_detail(messages: tuple[str, ...], *, limit: int) -> str:
    """Join messages up to `limit` chars without cutting one mid-sentence.

    A naive join-then-slice can end on a fragment like "Extra inputs are
    not" (from a 30-forbidden-field answer), which the repair prompt would
    then present as if it were the complete reason. Keep only whole
    messages that fit and note how many were dropped instead.
    """
    non_empty = [part for part in messages if part]
    # Reserve room for the worst-case " (+N more)" suffix up front so
    # appending it can never itself push the result past `limit`.
    suffix_reserve = len(f" (+{len(non_empty)} more)")
    kept: list[str] = []
    total = 0
    for index, msg in enumerate(non_empty):
        addition = msg if not kept else f"; {msg}"
        if total + len(addition) > limit - suffix_reserve:
            if not kept:
                # Even the first whole message doesn't fit: better a
                # visibly-truncated fragment than an empty detail string.
                return msg[:limit]
            omitted = len(non_empty) - index
            return ("; ".join(kept) + f" (+{omitted} more)")[:limit]
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

    # CHAOS-3290 grounding floor -- see the constants above for the full
    # reasoning. Only reachable once every other invariant already passed,
    # so this never overrides a real rejection; it only catches the shape
    # those checks vacuously allow through: nothing to check because there
    # is nothing there.
    if not (answer.claims or answer.metrics or answer.evidence):
        summary = answer.direct_summary.strip()
        if answer.status is AnswerStatus.COMPLETE:
            if _tool_results_offer_groundable_material(context.tool_results):
                # Real tool material existed (metrics, evidence, status
                # facts, ...) and none of it made it into the answer at
                # all -- structurally impossible under PRD §8 regardless of
                # what the prose says.
                raise AnswerValidationError(
                    "complete answer carries no claim, metric, or evidence "
                    "grounding despite groundable tool results",
                    code="answer_grounding_floor_not_met",
                    repairable=False,
                )
            if len(summary) < _MIN_CATALOG_SUMMARY_CHARS or not _NUMBER.search(summary):
                # No groundable material was available at all (e.g. a
                # metric-catalog question) -- the only remaining honest
                # signal is whether the summary reflects real retrieved
                # content instead of restating the question.
                raise AnswerValidationError(
                    "complete answer has no structured grounding and its "
                    "summary does not reflect retrieved data",
                    code="answer_grounding_floor_not_met",
                    repairable=False,
                )
        elif (
            answer.status is AnswerStatus.PARTIAL
            and len(summary) >= _MIN_UNGROUNDED_SUMMARY_CHARS
        ):
            # A partial answer presenting long, confident-sounding prose
            # with zero structured grounding is exactly as untrustworthy as
            # a complete one -- "partial" alone does not excuse an
            # unsupported narrative (CHAOS-3290). An honestly short/modest
            # partial ("no data available yet") is not gated: it never
            # claimed to be a substantive answer in the first place.
            raise AnswerValidationError(
                "substantive partial answer carries no claim, metric, or "
                "evidence grounding",
                code="answer_grounding_floor_not_met",
                repairable=False,
            )
    return answer


__all__ = [
    "AnswerValidationContext",
    "AnswerValidationError",
    "validate_answer_candidate",
]
