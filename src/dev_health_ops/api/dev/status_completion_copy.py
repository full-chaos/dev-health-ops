"""Shared completion-assessment copy: the CHAOS-3297 s2 disclosure obligation
and the CHAOS-3377 reason-code translation table.

Split out of ``answer_validator.py`` (which originated the disclosure
obligation) and ``status_answer_render.py`` (which originated the
translation table) into their own dependency-free module. Both of those
modules need pieces the OTHER one owns -- the validator's
``completion_truncation_detail`` must stop rendering ``ActualCompletion``'s
raw reason codes into a user-visible ``DevError.safe_message`` (CHAOS-3377
defect 2: those codes are exactly what the widened internal-token denylist in
``no_match_terminal.py`` now forbids everywhere), and the deterministic
renderer needs the validator's disclosure constant and withheld-denominator
predicate. Neither module may import the other without a cycle, so both
import this one instead.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from .contracts import DevActualCompletion, DevToolResult
from .status_change_service import STATUS_REASON_CODES

__all__ = [
    "INCOMPLETE_DENOMINATOR_DISCLOSURE",
    "any_tool_result_withheld_its_completion_denominator",
    "has_incomplete_denominator_disclosure",
    "is_completion_assessment_untrustworthy",
    "translate_reason_code",
]

# CHAOS-3297 s2 round 8 (closure, ratified on the ticket): a positive
# disclosure obligation, not a vocabulary sweep against fabricated completion
# language -- see answer_validator.py's original module-level comment
# (rounds 5-8) for the full history of why an absence-of-bad-phrasing check
# cannot work here. A string either contains this sentence, verbatim and
# case-insensitive, or it does not.
INCOMPLETE_DENOMINATOR_DISCLOSURE = (
    "the required-work completion total could not be fully verified"
)


def has_incomplete_denominator_disclosure(text: str) -> bool:
    return INCOMPLETE_DENOMINATOR_DISCLOSURE.casefold() in text.casefold()


# CHAOS-3297 s2 round 9 (codex CONFIRMED): status_change_service._assess
# nulls required_child_total/required_child_complete ONLY when the
# required-child source itself was truncated; every OTHER assessment
# category sets this general reason code while leaving the denominator
# non-None. The disclosure obligation must fire on EITHER signal.
_ASSESSMENT_SOURCE_LIMIT_REACHED = "assessment_source_limit_reached"


def is_completion_assessment_untrustworthy(actual: DevActualCompletion) -> bool:
    return (
        actual.required_child_total is None
        or _ASSESSMENT_SOURCE_LIMIT_REACHED in actual.reason_codes
    )


def any_tool_result_withheld_its_completion_denominator(
    tool_results: tuple[DevToolResult, ...],
) -> bool:
    """Whether any executed tool in this run reported a completion
    assessment that cannot be trusted to ground a completion claim.
    """

    return any(
        result.actual_completion is not None
        and is_completion_assessment_untrustworthy(result.actual_completion)
        for result in tool_results
    )


# --- CHAOS-3377 defect 2: closed-vocabulary reason-code translation --------

#: Closed-vocabulary translation for every reason code
#: ``status_change_service._assess`` can emit (``STATUS_REASON_CODES``).
#: Fail-closed: a code this table has never seen renders as
#: ``_DEFAULT_REASON_COPY``, never the raw token.
_REASON_CODE_COPY: Mapping[str, str] = {
    "child_requirement_unknown": "it is unknown whether some sub-items are required",
    "declared_status_missing": "no declared status was recorded",
    "required_source_not_fresh": "a required data source is stale or unavailable",
    "assessment_source_limit_reached": (
        "the assessment hit a display limit, so this total may be incomplete"
    ),
    "required_release_evidence_missing": "required release evidence is missing",
    "required_child_incomplete": "one or more required sub-items are not complete",
    "open_blocker": "one or more blocking items are still open",
    "required_pull_request_unmerged": "a required pull request has not merged",
    "required_review_unresolved": "a required review has not been completed",
    "review_changes_requested": "a reviewer has requested changes",
    "ci_requirement_unknown": "it is unknown whether some CI checks are required",
    "required_ci_skip_state_unknown": (
        "it is unknown whether a required CI check was skipped"
    ),
    "required_ci_work_skipped": "required CI work was skipped",
    "required_ci_not_passing": "a required CI check is not passing",
    "required_deployment_not_succeeded": "a required deployment has not succeeded",
    "active_blocking_incident": "an active incident is blocking this work",
}
_DEFAULT_REASON_COPY = "an unresolved requirement"

# Import-time totality: every code this module can translate must be one
# _assess can actually emit, and vice versa.
_missing_from_copy_table = STATUS_REASON_CODES - set(_REASON_CODE_COPY)
_missing_from_reason_codes = set(_REASON_CODE_COPY) - STATUS_REASON_CODES
if _missing_from_copy_table or _missing_from_reason_codes:  # pragma: no cover
    raise RuntimeError(
        "status_completion_copy._REASON_CODE_COPY has diverged from "
        "status_change_service.STATUS_REASON_CODES: "
        f"missing translation(s)={sorted(_missing_from_copy_table)}, "
        f"stale translation(s)={sorted(_missing_from_reason_codes)}"
    )


def translate_reason_code(code: str) -> str:
    """The safe, closed-vocabulary clause for one raw ``ActualCompletion``
    reason code. Fail-closed: an unrecognized code renders as
    ``_DEFAULT_REASON_COPY``, never the raw token.
    """

    return _REASON_CODE_COPY.get(code, _DEFAULT_REASON_COPY)


def translate_reason_codes(codes: Sequence[str]) -> list[str]:
    """``translate_reason_code`` over a sequence, de-duplicated in order."""

    seen: set[str] = set()
    translated: list[str] = []
    for code in codes:
        text = translate_reason_code(code)
        if text not in seen:
            seen.add(text)
            translated.append(text)
    return translated
