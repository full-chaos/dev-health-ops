"""Grounding validator for model-produced ``dev_answer.v1`` candidates."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from .contracts import (
    AnswerStatus,
    DevAnswer,
    DevClaim,
    DevContractVersions,
    DevCoverage,
    DevEvidenceRef,
    DevMetricRef,
    DevModelMetadata,
    DevScopeResolution,
    DevToolResult,
)
from .status_completion_copy import (
    INCOMPLETE_DENOMINATOR_DISCLOSURE,
    any_tool_result_withheld_its_completion_denominator,
    translate_reason_codes,
)
from .status_completion_copy import (
    has_incomplete_denominator_disclosure as _has_incomplete_denominator_disclosure,
)
from .status_completion_copy import (
    is_completion_assessment_untrustworthy as _completion_assessment_is_untrustworthy,
)

_NUMBER = re.compile(r"(?<![A-Za-z_])[-+]?\d+(?:[.,]\d+)*(?:%|\b)")
# CHAOS-3297 s2 round 3 (codex HIGH): the numeric-claim check just below
# (a claim with a number needs *some* metric/evidence citation) is
# necessary but not sufficient for completion language specifically -- it
# never verifies the citation actually grounds the number it accompanies,
# so a model can cite an unrelated evidence ref to "unlock" a fabricated
# "100% complete" / "3 of 5 done" even when status_snapshot.v1 explicitly
# withheld the denominator (ActualCompletion.required_child_total /
# required_child_complete are ``None`` whenever the required-child source
# itself was truncated -- see status_change_service.py). direct_summary
# carries no citation requirement at all, so the same language needs the
# identical guard independently of any claim.
#
# Rounds 5, 6, and 7 (codex HIGH, three times) all tried to detect
# FABRICATED completion language by vocabulary -- digit/fraction shapes,
# then totalizing words, then bare unhedged predicates, then hedge-word
# rescue. Each round's fix was defeated by the next round's fresh
# synonym or a whole-text hedge-token bypassing an unequivocal clause
# elsewhere in the same sentence ("The work appears fully complete --
# and it is."). That is structural, not a vocabulary gap: natural-
# language completion semantics is an open set, and absence-of-bad-
# phrasing can never be closed by enumerating more phrasings -- there is
# always one more synonym, and hedge-rescue at whole-text granularity
# can always be defeated by a second, unequivocal clause.
#
# Round 8 (closure, ratified on the ticket): invert the obligation from
# ABSENCE (no fabricated language) to PRESENCE (a required, exact,
# server-specified disclosure). This is not a vocabulary sweep -- it is
# a two-cell partition of the entire text domain: a string either
# contains INCOMPLETE_DENOMINATOR_DISCLOSURE (verbatim, case-
# insensitive) or it does not, with no third case and nothing left to
# enumerate. The repair-turn prompt already echoes ``exc.detail``
# (see orchestrator.py), which is exactly this message -- the model is
# told the precise sentence to include, not asked to guess at "more
# hedging". The residual risk this accepts (ratified, bounded, not an
# open bypass): a text CAN still contain both the disclosure and an
# unequivocal completion clause ("It's 100% done. Note: the
# required-work completion total could not be fully verified.") -- the
# reader always sees the caveat verbatim alongside whatever else the
# text says, which is a materially different risk than the caveat being
# silently absent.
#
# CHAOS-3377: this constant and the two predicates below (``_completion_
# assessment_is_untrustworthy``/``any_tool_result_withheld_its_completion_
# denominator``) now live in ``status_completion_copy.py`` -- imported above
# -- so ``status_answer_render.py``'s deterministic §10 renderer can use the
# identical disclosure obligation without importing this module (which now
# also imports THAT module's reason-code translation table below, for
# ``completion_truncation_detail``; the shared module exists specifically to
# break that cycle).

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

# CHAOS-3290: a complete (or substantive-partial) answer must never be an
# empty shell. A degenerate reasoning-exhausted run can still satisfy every
# other invariant above (identity, scope, canonical evidence/metric equality
# all vacuously pass over empty lists) while carrying zero *material*
# grounding -- a silent non-answer presented as success. PRD §8 forbids a
# material status claim without evidence; this is the degenerate case where
# there is no material claim or evidence at all.
#
# "Zero grounding" is NOT "claims/metrics/evidence are all empty" (adversarial
# review, CHAOS-3290 follow-up): a single unsupported ``inferred`` claim
# (confidence < 1) needs no metric_ref_ids/evidence_ref_ids at all under
# DevClaim's own schema, so a model can add one fabricated inferred claim
# ("Delivery performance improved substantially across every team.") and
# make ``answer.claims`` non-empty without grounding anything. The floor must
# check whether *any* claim actually references a metric or evidence ID, not
# merely whether the claims array is non-empty.
#
# The floor also cannot simply require claims/metrics/evidence to be
# non-empty unconditionally: a metric-catalog question (list_metrics.v1
# only) has no tool output representable as a claim, metric, or evidence ref
# at all (DevMetricDefinition has no value to ground a DevMetricRef with,
# and mints no DevEvidenceRef), so a genuinely thorough catalog answer is
# legitimately empty across all three arrays -- the only signal left is
# whether the free-text summary actually reflects the retrieved catalog.
# Neither "contains a number" nor "the summary states the retrieved count"
# is safe there (both live-reproduced and adversarially reproduced): "cycle
# time p50 ... over a 30-day window" matches a bare digit check without
# ever reading the catalog, and "I have 1 unresolved limitation..." matches
# a bare count check purely by coincidence when only one definition exists.
# Require covering at least half of the retrieved definitions' machine
# identifiers (a snake_case metric_id or its "<id>.v<n>" definition_version)
# instead -- a paraphrase of the question, an echoed user-supplied
# identifier, or an incidental number cannot coincidentally satisfy that at
# scale, and a single coincidental match is never enough once more than one
# definition exists.
#
# For a substantive-but-ungrounded partial, prose length is not a safe
# distinguishing signal either (adversarial review): a *short*, confident,
# fabricated narrative ("Delivery performance improved substantially across
# every team.") is exactly as untrustworthy as a long one. Use
# ``answer.coverage`` instead -- it is server-computed and fully
# overwritten by the orchestrator before validation (never model-authored),
# so a model cannot forge it. A partial with zero grounding is honest only
# if the server's own coverage accounting shows a real gap (a required
# source unavailable, stale, or short of the required count); if coverage
# reports everything available, an ungrounded partial is just as
# structurally impossible as an ungrounded complete.
_GROUNDABLE_TOOL_RESULT_FIELDS = (
    "metrics",
    "evidence",
    "status_facts",
    "pull_requests",
    "ci_checks",
    "deployments",
    "incidents",
)


def _claim_has_grounding_reference(claim: DevClaim) -> bool:
    return bool(claim.metric_ref_ids or claim.evidence_ref_ids)


# CHAOS-3297 s2 round 9 (codex CONFIRMED): status_change_service._assess
# nulls required_child_total/required_child_complete ONLY when the
# required-child source itself was truncated (children/membership --
# see children_source_truncated there). Every OTHER assessment category
# -- blockers, pull_requests, ci, deployments, incidents -- sets the
# general "assessment_source_limit_reached" reason code while leaving
# the denominator non-None (it genuinely counted every required child it
# saw; it's the REST of the evidence that was cut off). Gating the
# disclosure obligation on required_child_total is None alone left those
# five categories completely unguarded: codex drove the real service
# with each category truncated in turn and got total=0/complete=0 with
# the flag set, and a markerless "All required work is complete." passed
# every time. The trigger must fire on EITHER signal -- a withheld
# denominator makes the count itself unknown; the general reason code
# makes the REST of the evidence (blockers/PRs/CI/deployments/incidents)
# behind that "complete" claim unknown, which is exactly as untrustworthy.
# See ``status_completion_copy.py`` for ``_completion_assessment_is_
# untrustworthy``/``any_tool_result_withheld_its_completion_denominator``,
# imported above.


def completion_truncation_detail(tool_results: tuple[DevToolResult, ...]) -> str:
    """A safe, user-facing description of why a completion total was
    withheld (CHAOS-3297 s2 round 5, codex MEDIUM): rejecting a fabricated
    completion claim without saying why leaves the user with nothing --
    the failure path must still surface the reason codes and how many
    required items WERE displayed, not just a generic "validation
    failed". Built only from ``ActualCompletion`` reason codes and a
    count -- never raw evidence/child content, so it stays safe to show
    verbatim in a terminal error message. Selection matches the trigger
    predicate above (round 9): either signal, not just a null total.

    CHAOS-3377 defect 2: the reason codes are translated through the same
    closed-vocabulary table ``status_answer_render.py`` uses, not rendered
    raw -- this message is a ``DevError.safe_message``, which
    ``no_match_terminal.user_visible_strings`` scans exactly like answer
    prose, and the widened internal-token denylist (CHAOS-3377) would
    otherwise fail this terminal closed on its own error message.
    """
    for result in tool_results:
        actual = result.actual_completion
        if actual is not None and _completion_assessment_is_untrustworthy(actual):
            reasons = "; ".join(translate_reason_codes(actual.reason_codes)) or (
                "an unknown reason"
            )
            displayed = len(actual.required_children)
            return (
                f"The required-work assessment could not verify a "
                f"complete total ({reasons}); {displayed} required "
                "item(s) were displayed, but that count may be "
                "incomplete."
            )
    return "The required-work assessment could not verify a complete total."


def _answer_has_material_grounding(answer: DevAnswer) -> bool:
    """Whether the answer carries anything an evidence-linked reader could
    actually check: a metric, an evidence entry, or a claim that references
    at least one of either. A claim with no reference at all (an
    unsupported ``inferred`` assertion) does not count -- see the
    module-level comment above.
    """
    return (
        bool(answer.metrics)
        or bool(answer.evidence)
        or any(_claim_has_grounding_reference(claim) for claim in answer.claims)
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


def _summary_covers_retrieved_catalog(
    summary: str, tool_results: tuple[DevToolResult, ...]
) -> bool:
    """Whether `summary` demonstrably reflects the retrieved metric catalog
    -- the one signal a model cannot produce without having actually read
    the tool result it was given (see the module-level comment above).
    Requires naming the machine identifier (metric_id or definition_version)
    of at least half of the definitions actually retrieved; anything looser
    is gameable (see the review notes above).
    """
    folded = summary.casefold()
    definitions = [
        definition
        for result in tool_results
        for definition in result.metric_definitions
    ]
    if not definitions:
        return False

    def _named(definition) -> bool:
        return any(
            identifier and identifier.casefold() in folded
            for identifier in (definition.metric_id, definition.definition_version)
        )

    covered = sum(1 for definition in definitions if _named(definition))
    return covered >= math.ceil(len(definitions) / 2)


def _coverage_reports_a_gap(coverage: DevCoverage) -> bool:
    """Whether the server's own (non-model-authored) coverage accounting
    shows a genuine reason data could be missing.
    """
    return bool(
        coverage.available_source_count < coverage.required_source_count
        or coverage.unavailable_required_sources
        or coverage.stale_required_sources
        or coverage.degraded_required_sources
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
    completion_denominator_withheld = (
        any_tool_result_withheld_its_completion_denominator(context.tool_results)
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
        # CHAOS-3297 s2 round 8 (closure): a citation existing (checked
        # above) does not mean it grounds THIS number, and no vocabulary
        # sweep can close an open set of fabricated-completion phrasings
        # (see the module-level comment on INCOMPLETE_DENOMINATOR_
        # DISCLOSURE for the round 5/6/7 history) -- so this is now a
        # POSITIVE obligation, not a vocabulary check: whenever the
        # server withheld the completion denominator, every claim must
        # carry the disclosure verbatim, independent of what else it
        # cites or says. repairable=True (round 5, still true): a
        # missing disclosure is a phrasing omission the model can
        # correct in one bounded pass, not a trust breach with no
        # honest alternative -- see completion_truncation_detail and its
        # caller for what happens if that pass also fails.
        if (
            completion_denominator_withheld
            and not _has_incomplete_denominator_disclosure(claim.text)
        ):
            raise AnswerValidationError(
                "claim omits the required disclosure for a withheld "
                f'completion total -- include the exact sentence "'
                f'{INCOMPLETE_DENOMINATOR_DISCLOSURE}" (verbatim, '
                "case-insensitive)",
                code="completion_denominator_withheld",
                repairable=True,
            )

    if completion_denominator_withheld and not _has_incomplete_denominator_disclosure(
        answer.direct_summary
    ):
        # direct_summary carries no citation requirement at all, so it
        # needs the identical positive obligation independently of the
        # claims loop.
        raise AnswerValidationError(
            "direct summary omits the required disclosure for a withheld "
            f'completion total -- include the exact sentence "'
            f'{INCOMPLETE_DENOMINATOR_DISCLOSURE}" (verbatim, '
            "case-insensitive)",
            code="completion_denominator_withheld",
            repairable=True,
        )

    # CHAOS-3377 defect 1 (refusal ceiling): the mirror image of the
    # CHAOS-3290 floor below. A model can self-declare
    # ``AnswerStatus.REFUSED`` in the same free-form JSON it authors
    # ``direct_summary``/``claims`` in, and nothing previously stopped it
    # from doing so alongside real claim/metric/evidence grounding -- the
    # live defect this ticket reports (a "Refused" chip over a fully
    # substantive body). PRD §12's "a result with content is not a refusal"
    # applies here exactly as it does to CHAOS-3367's no-match path: a
    # refusal that carries material grounding is not an honest refusal, it
    # is a mislabeled answer. ``repairable=True`` (not one of
    # ``_NON_REPAIRABLE_VALIDATION_MARKERS``): this is a one-field labeling
    # mistake the model can correct in the same bounded repair pass
    # CHAOS-3257 already gives a status/coverage mismatch, by reissuing the
    # same grounded content under an accurate status.
    if answer.status is AnswerStatus.REFUSED and _answer_has_material_grounding(answer):
        raise AnswerValidationError(
            "a refused answer cannot carry claim, metric, or evidence "
            "grounding -- reissue with an accurate status (complete, "
            "partial, degraded, or insufficient_evidence) for the evidence "
            "already retrieved",
            code="refused_with_material_grounding",
            repairable=True,
        )

    # CHAOS-3290 grounding floor -- see the constants above for the full
    # reasoning. Only reachable once every other invariant already passed,
    # so this never overrides a real rejection; it only catches the shape
    # those checks vacuously allow through: nothing to check because there
    # is nothing there.
    if not _answer_has_material_grounding(answer):
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
            if not _summary_covers_retrieved_catalog(summary, context.tool_results):
                # No groundable material was available at all (e.g. a
                # metric-catalog question) -- the only remaining honest
                # signal is whether the summary demonstrably covers the
                # retrieved catalog instead of just restating the question.
                raise AnswerValidationError(
                    "complete answer has no structured grounding and its "
                    "summary does not reflect retrieved data",
                    code="answer_grounding_floor_not_met",
                    repairable=False,
                )
        elif answer.status is AnswerStatus.PARTIAL and not _coverage_reports_a_gap(
            answer.coverage
        ):
            # A partial answer with zero grounding is only honest if the
            # server's own coverage accounting explains why -- a required
            # source unavailable, stale, or short of the required count.
            # If coverage reports everything available, an ungrounded
            # partial is exactly as untrustworthy as an ungrounded
            # complete, regardless of how short or modest its prose reads
            # (CHAOS-3290 review: prose length is not a safe signal here).
            raise AnswerValidationError(
                "partial answer carries no claim, metric, or evidence "
                "grounding and reports no coverage gap to explain why",
                code="answer_grounding_floor_not_met",
                repairable=False,
            )
    return answer


__all__ = [
    "INCOMPLETE_DENOMINATOR_DISCLOSURE",
    "AnswerValidationContext",
    "AnswerValidationError",
    "any_tool_result_withheld_its_completion_denominator",
    "completion_truncation_detail",
    "validate_answer_candidate",
]
