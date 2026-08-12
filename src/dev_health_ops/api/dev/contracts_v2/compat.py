"""The one backend v2-to-v1 compatibility projector (CHAOS-3294 deliverable).

Amendment TRD v2 §12 / CHAOS-3294 guardrails: "One backend projector owns
v2-to-v1 compatibility; web code must not implement a second mapping."
This module is that single projector. It is a pure function: given a
``DevAnswerV2`` (plus the small amount of request-scope context v1's shape
requires but v2's subject-centric frame does not carry — organization id
and the effective time window), it returns either a v1 ``DevAnswer`` or a
v1 ``DevError``, both already-validated wire objects.

Fidelity notes (documented rather than silently glossed over, since a v1
client can only ever see an approximation of the richer v2 frame):

* CHAOS-3301 gave v1's ``DirectScope``/``EntityType`` a real ``team`` value
  — but only for the *subject preflight's* own construction
  (``scope_service.committed_resolution_for``), which never routes through
  this projector at all: ``build_preflight_answer`` (``preflight_outcomes``)
  never sets ``frame.subject_ref``, so a preflight-committed team subject
  cannot reach ``_build_resolved_scope`` here. This projector's team
  interception is therefore kept deliberately unchanged pending CHAOS-3297
  (real v2 answer frames, which *would* set ``subject_ref`` and could exercise
  this path): a team-subject v2 answer still has no *frame-projection* path
  wired, so the projector still returns a safe ``DevError``
  (``feature_not_enabled``) rather than silently mislabeling a team answer as
  organization- or repository-scoped, which would violate the "no team
  attribution without disclosed scope" guardrail (Amendment PRD v2 §10) at
  the compatibility boundary. Wiring ``_build_resolved_scope``'s team branch
  is CHAOS-3297's job, alongside whatever it does for ``subject_set_ref``
  below.
* Symmetrically (Codex adversarial-review hardening, CHAOS-3294): a
  ``subject_set_ref`` answer (a cohort/plural-subject frame) also has no
  faithful v1 representation. ``dev_answer_frame.v1.subject_set_ref`` is
  only an *opaque pointer* to a ``dev_subject_set.v1`` — the frame does not
  embed the committed entity list itself — so there is no way to build a v1
  ``DevScope`` that actually names the committed cohort. The old code fell
  through ``_build_resolved_scope``'s "no ``subject_ref``" branch and
  silently widened every cohort answer to organization-wide scope
  (``DirectScope.ORGANIZATION`` / ``ScopeResolutionOutcome.ORGANIZATION_FALLBACK``),
  misrepresenting cohort-specific facts as org-wide. The projector now
  intercepts ``subject_set_ref`` before scope-building, exactly like the
  team-subject case, and returns the same safe ``feature_not_enabled``
  ``DevError`` rather than a mislabeled answer.
* v1's ``DevAnswer.status == COMPLETE`` requires full source coverage
  (``validate_answer_invariants``). The projector never claims ``COMPLETE``
  unless the frame's own coverage actually satisfies that invariant, even
  for a v2 ``answered`` outcome — it downgrades to ``PARTIAL`` instead of
  fabricating completeness.
* v2 facts of kind ``recommendation`` carry no per-fact rule version (only
  frame-level ``versions.rule_version``); when neither is present the
  projector downgrades the mapped v1 claim kind to ``inferred`` rather
  than emit an invalid v1 claim.
* A no-answer outcome projects to a ``DevError`` built entirely from
  server-owned tables (``CANONICAL_NO_ANSWER_COPY``, ``dev_error_remediation``
  and ``CANONICAL_NO_ANSWER_REMEDIATION``). No text is carried across from
  the frame — see ``_project_error``.
"""

from __future__ import annotations

from datetime import datetime
from typing import TypeVar

from pydantic import BaseModel

from dev_health_ops.api.dev.contracts import (
    AnswerStatus,
    ClaimKind,
    DevAnswer,
    DevClaim,
    DevClaimFlags,
    DevConflict,
    DevContractVersions,
    DevCoverage,
    DevDisambiguationCandidate,
    DevEntityRef,
    DevError,
    DevEvidenceRef,
    DevMetricRef,
    DevModelMetadata,
    DevScope,
    DevScopeResolution,
    DevTimeRange,
    DirectScope,
    EntityType,
    ScopeResolutionOutcome,
    dev_error_remediation,
)

from .answer import DevAnswerV2
from .base import EntityKind, FactDisclosure, OpaqueID, PublicOutcome
from .frame import DevAnswerFrame, DevFrameVersions
from .subject import DevResolutionCandidate
from .validators import CANONICAL_NO_ANSWER_COPY, CANONICAL_NO_ANSWER_REMEDIATION

__all__ = [
    "no_answer_error_projection",
    "project_answer_v2_to_v1",
    "scope_resolution_from_frame",
]

_V1Model = TypeVar("_V1Model", bound=BaseModel)

# Import-time totality (P2): `FactDisclosure` (contracts_v2/base.py) and v1
# `DevClaimFlags.model_fields` (contracts.py:615-619) must name exactly the
# same set of flags, or a claim's flag could round-trip through
# `wrap_legacy_answer_as_frame` / `_project_answered` into the wrong bit, or
# silently drop one. This is a name-level bijection only, not a semantic
# one -- see `FactDisclosure`'s docstring for why derivation from
# `DevEvidenceFlags` would be wrong even though the names would still match.
_missing_from_fact_disclosure = set(DevClaimFlags.model_fields) - {
    member.value for member in FactDisclosure
}
_missing_from_claim_flags = {member.value for member in FactDisclosure} - set(
    DevClaimFlags.model_fields
)
if _missing_from_fact_disclosure or _missing_from_claim_flags:
    raise RuntimeError(
        "FactDisclosure (contracts_v2/base.py) and DevClaimFlags "
        "(contracts.py) have diverged: DevClaimFlags field(s) missing from "
        f"FactDisclosure={sorted(_missing_from_fact_disclosure)}, "
        f"FactDisclosure value(s) missing from DevClaimFlags="
        f"{sorted(_missing_from_claim_flags)}"
    )


def _as_v1(model_cls: type[_V1Model], value: BaseModel) -> _V1Model:
    """Rebuild one ``embedded.py`` mirror as its plain v1 original.

    A mirror ``isinstance``-passes as its v1 type, so pydantic would accept it
    into a v1-typed field untouched — and then ask v1's ``list`` serializer to
    emit the mirror's ``tuple``. Round-tripping through validation is what
    keeps the v1 wire output exactly v1-shaped. See ``embedded.py``.
    """

    return model_cls.model_validate(value.model_dump())


def _require_versions(
    versions: DevFrameVersions | None, outcome: PublicOutcome
) -> DevFrameVersions:
    """``versions`` is optional only for no-answer outcomes, which never reach here.

    ``validators.validate_versions_presence`` already rejects a content-bearing
    frame without provenance, so this is unreachable in a validated frame; it
    exists so the projector's dependence on that invariant is stated rather
    than assumed.
    """

    if versions is None:
        raise ValueError(
            f"cannot project a {outcome.value!r} answer whose frame carries no "
            "versions provenance block"
        )
    return versions


_DETERMINISTIC_VERSION_PLACEHOLDER = "deterministic_frame.v1"

_KIND_TO_DIRECT_SCOPE: dict[EntityKind, DirectScope] = {
    EntityKind.REPOSITORY: DirectScope.REPOSITORY,
    EntityKind.PROJECT: DirectScope.PROJECT,
    EntityKind.WORK_UNIT: DirectScope.WORK_UNIT,
    EntityKind.ISSUE: DirectScope.ISSUE,
    EntityKind.PULL_REQUEST: DirectScope.PULL_REQUEST,
}
_KIND_TO_ENTITY_TYPE: dict[EntityKind, EntityType] = {
    EntityKind.REPOSITORY: EntityType.REPOSITORY,
    EntityKind.PROJECT: EntityType.PROJECT,
    EntityKind.WORK_UNIT: EntityType.WORK_UNIT,
    EntityKind.ISSUE: EntityType.ISSUE,
    EntityKind.PULL_REQUEST: EntityType.PULL_REQUEST,
}

#: Full ``EntityKind`` -> v1 ``EntityType`` mapping for a clarification
#: candidate (CHAOS-3325), unlike ``_KIND_TO_ENTITY_TYPE`` above: that map
#: intentionally omits ``TEAM`` because it backs ``_build_resolved_scope``,
#: which never reaches a team kind at all (the team-subject guard in
#: ``project_answer_v2_to_v1`` intercepts it first — v1 has no team
#: *direct-scope* representation). A clarification candidate is never a
#: resolved scope, only a named option in a list, and v1's own
#: ``EntityType`` already has a ``TEAM`` member (``contracts.py``) — so a
#: team candidate has a faithful v1 shape and this map is total over
#: ``EntityKind`` rather than reusing the scope-building map's narrower one.
_CANDIDATE_ENTITY_TYPE: dict[EntityKind, EntityType] = {
    **_KIND_TO_ENTITY_TYPE,
    EntityKind.TEAM: EntityType.TEAM,
}


def _project_clarification_candidate(
    candidate: DevResolutionCandidate,
) -> DevDisambiguationCandidate:
    """One real, authorized ledger candidate -> its v1 wire shape.

    Never invented: ``entity_ref``/``reason`` are carried straight across
    from the ``DevResolutionCandidate`` the resolution ledger recorded (see
    ``subject_preflight.SubjectPreflight._entry``) — the same object
    ``build_preflight_answer`` places on ``frame.clarification_candidates``.
    """

    entity = candidate.entity_ref
    return DevDisambiguationCandidate(
        entity_ref=DevEntityRef(
            entity_type=_CANDIDATE_ENTITY_TYPE[entity.entity_kind],
            entity_id=entity.entity_id,
            display_label=entity.display_label,
            repository_id=entity.repository_id,
        ),
        repository_id=entity.repository_id,
        reason=candidate.reason,
    )


def scope_resolution_from_frame(
    frame: DevAnswerFrame,
    *,
    requested_scope: DevScope,
    resolved_at: datetime,
) -> DevScopeResolution:
    """The v1 scope resolution a preflight-terminated run actually reached.

    Three cases, in priority order (CHAOS-3325's own rules, unchanged --
    this function is the extraction of what ``_project_needs_clarification``
    already did inline, so there is exactly ONE producer of this shape):

    1. ``frame.clarification_candidates`` is non-empty (the preflight
       ambiguous-mention case): every real, authorized ledger candidate is
       projected and the outcome is ``AMBIGUOUS``, matching v1's own
       "ambiguous carries candidates" invariant.
    2. ``frame.subject_ref`` is set instead: one derived candidate from the
       one real committed entity.
    3. Neither: ``UNRESOLVED``, carrying nothing. An honest "not resolved,
       nothing to offer" rather than a fabricated option.

    CHAOS-3497 made this callable from ``orchestrator.run()``'s preflight
    TERMINATE branch. Until then that branch published the run's *original*
    top-level resolve on the wire -- which by construction can only be
    ``exact``/``filtered``/``inherited``/``organization_fallback``, since
    every unhealthy outcome already returned earlier. A reader therefore saw
    "scope resolved: exact" one frame before an error saying the named
    subject could not be found: the exact juxtaposition
    ``no_match_terminal``'s module docstring says the PRD prohibits, and it
    also left ``no_unauthorized_candidate_surfaces`` scanning a list that
    STRUCTURALLY cannot hold candidates (v1 permits them only on
    candidate-bearing outcomes), turning a loud "not measured" failure into
    a silent vacuous pass over the wrong object.
    """

    if frame.clarification_candidates:
        outcome = ScopeResolutionOutcome.AMBIGUOUS
        candidates = [
            _project_clarification_candidate(candidate)
            for candidate in frame.clarification_candidates
        ]
    elif frame.subject_ref is not None:
        outcome = ScopeResolutionOutcome.AMBIGUOUS
        candidates = [
            DevDisambiguationCandidate(
                entity_ref=DevEntityRef(
                    entity_type=_KIND_TO_ENTITY_TYPE.get(
                        frame.subject_ref.entity_kind, EntityType.REPOSITORY
                    ),
                    entity_id=frame.subject_ref.entity_id,
                    display_label=frame.subject_ref.display_label,
                    repository_id=frame.subject_ref.repository_id,
                ),
                reason="Clarification requested before continuing.",
            )
        ]
    else:
        outcome = ScopeResolutionOutcome.UNRESOLVED
        candidates = []
    return DevScopeResolution(
        schema_version="dev_scope_resolution.v1",
        requested_scope=requested_scope,
        resolved_scope=None,
        outcome=outcome,
        authorized_repository_ids=[],
        authorized_entity_ids=[],
        candidates=candidates,
        fallbacks=[],
        warnings=[],
        resolved_at=resolved_at,
    )


_ERROR_OUTCOME_CODES: dict[PublicOutcome, tuple[str, bool]] = {
    PublicOutcome.NOT_FOUND: ("scope_not_found", False),
    PublicOutcome.TEMPORARILY_UNAVAILABLE: ("source_unavailable", True),
    PublicOutcome.UNSUPPORTED: ("feature_not_enabled", False),
    PublicOutcome.DENIED: ("forbidden", False),
    PublicOutcome.FAILED: ("internal_error", False),
    # CHAOS-3541: matches orchestrator.py's own live wire code for this
    # outcome exactly ("refused") -- never "insufficient_evidence", which
    # would mislabel a categorical refusal as a resolvable evidence gap on
    # replay too.
    PublicOutcome.REFUSED: ("refused", False),
}


def no_answer_error_projection(outcome: str) -> tuple[str, str, bool, list[str]]:
    """``(safe_message, code, retryable, remediation)`` for one no-answer
    outcome -- EXACTLY the composition ``_project_error`` uses to build a
    v1 ``DevError``, factored out as the single function both the live
    projector and the CHAOS-3471 contract-artifact exporter call.

    Codex round-1 finding 1: an earlier version of the exporter published
    ``CANONICAL_NO_ANSWER_REMEDIATION`` directly, which is only the
    FALLBACK half of what a v1 client actually receives --
    ``_project_error`` tries ``dev_error_remediation(code)`` first. A
    ``dev_error_remediation`` entry added for a no-answer code (e.g.
    ``"forbidden"``, ``denied``'s code) would silently change live output
    while that artifact and its own content test stayed green. Composing
    here, in the one function both sides call, makes that class of drift
    structurally impossible rather than merely tested against.

    Codex round-1 finding 2: an earlier version also published a
    module-level snapshot (``NO_ANSWER_ERROR_CODES``) taken from
    ``_ERROR_OUTCOME_CODES`` once at import time, while ``_project_error``
    read ``_ERROR_OUTCOME_CODES`` directly on every call -- a runtime
    mutation of the private table would reach the live path immediately
    but never the stale snapshot. Reading ``_ERROR_OUTCOME_CODES`` fresh
    on every call here closes that gap too.
    """
    code, retryable = _ERROR_OUTCOME_CODES[PublicOutcome(outcome)]
    remediation = list(
        dev_error_remediation(code) or CANONICAL_NO_ANSWER_REMEDIATION[outcome]
    )
    return CANONICAL_NO_ANSWER_COPY[outcome], code, retryable, remediation


def _build_resolved_scope(
    answer: DevAnswerV2, organization_id: OpaqueID, time_range: DevTimeRange
) -> DevScope:
    frame = answer.frame
    subject = frame.subject_ref
    if subject is None:
        return DevScope(
            schema_version="dev_scope.v1",
            organization_id=organization_id,
            direct_scope=DirectScope.ORGANIZATION,
            repositories=[],
            entity_refs=[],
            team_ids=[],
            time_range=time_range,
        )
    direct_scope = _KIND_TO_DIRECT_SCOPE[subject.entity_kind]
    if direct_scope is DirectScope.REPOSITORY:
        return DevScope(
            schema_version="dev_scope.v1",
            organization_id=organization_id,
            direct_scope=DirectScope.REPOSITORY,
            repositories=[subject.entity_id],
            entity_refs=[],
            team_ids=[],
            time_range=time_range,
        )
    entity_ref = DevEntityRef(
        entity_type=_KIND_TO_ENTITY_TYPE[subject.entity_kind],
        entity_id=subject.entity_id,
        display_label=subject.display_label,
        repository_id=subject.repository_id,
    )
    return DevScope(
        schema_version="dev_scope.v1",
        organization_id=organization_id,
        direct_scope=direct_scope,
        repositories=[],
        entity_refs=[entity_ref],
        team_ids=[],
        time_range=time_range,
    )


def _map_claim_kind(fact_kind: str, has_rule_version: bool) -> ClaimKind:
    if fact_kind == "observed":
        return ClaimKind.OBSERVED
    if fact_kind == "recommendation" and has_rule_version:
        return ClaimKind.RECOMMENDATION
    # Either genuinely inferred, or a recommendation with no rule version to
    # carry — downgrade rather than emit an invalid v1 claim.
    return ClaimKind.INFERRED


def _project_answered(
    answer: DevAnswerV2, organization_id: OpaqueID, time_range: DevTimeRange
) -> DevAnswer:
    frame = answer.frame
    versions = _require_versions(frame.versions, answer.public_outcome)
    rule_version = versions.rule_version
    claims: list[DevClaim] = []
    for fact in frame.facts:
        confidence = fact.confidence
        kind = _map_claim_kind(fact.kind, rule_version is not None)
        if kind is ClaimKind.INFERRED and confidence >= 1:
            confidence = 0.999999
        claims.append(
            DevClaim(
                schema_version="dev_claim.v1",
                claim_id=fact.fact_id,
                kind=kind,
                text=fact.text,
                confidence=confidence,
                evidence_ref_ids=list(fact.evidence_ref_ids),
                metric_ref_ids=[],
                validity_scope=_build_resolved_scope(
                    answer, organization_id, time_range
                ),
                flags=DevClaimFlags(
                    **{disclosure.value: True for disclosure in fact.disclosures}
                ),
                recommendation_rule_version=(
                    rule_version if kind is ClaimKind.RECOMMENDATION else None
                ),
            )
        )
    coverage = frame.coverage
    fully_covered = (
        coverage.available_source_count == coverage.required_source_count
        and not coverage.unavailable_required_sources
        and not coverage.stale_required_sources
        and not coverage.degraded_required_sources
    )
    status = (
        AnswerStatus.COMPLETE
        if answer.public_outcome is PublicOutcome.ANSWERED and fully_covered
        else AnswerStatus.PARTIAL
    )
    resolved = _build_resolved_scope(answer, organization_id, time_range)
    resolution = DevScopeResolution(
        schema_version="dev_scope_resolution.v1",
        requested_scope=resolved,
        resolved_scope=resolved,
        outcome=(
            ScopeResolutionOutcome.EXACT
            if frame.subject_ref is not None
            else ScopeResolutionOutcome.ORGANIZATION_FALLBACK
        ),
        authorized_repository_ids=list(resolved.repositories),
        authorized_entity_ids=[ref.entity_id for ref in resolved.entity_refs],
        candidates=[],
        fallbacks=[],
        warnings=[],
        resolved_at=answer.generated_at,
    )
    model_metadata = (
        answer.narrative.provider_metadata
        if answer.narrative is not None
        and answer.narrative.provider_metadata is not None
        else DevModelMetadata(
            provider_source="platform",
            provider_family="deterministic",
            model_fingerprint=_DETERMINISTIC_VERSION_PLACEHOLDER,
        )
    )
    return DevAnswer(
        schema_version="dev_answer.v1",
        answer_id=answer.answer_id,
        conversation_id=answer.conversation_id,
        generated_at=answer.generated_at,
        resolved_scope=resolution,
        as_of=answer.generated_at,
        status=status,
        direct_summary=frame.direct_answer,
        claims=claims,
        metrics=[_as_v1(DevMetricRef, metric) for metric in frame.metrics],
        evidence=[_as_v1(DevEvidenceRef, item) for item in frame.evidence],
        conflicts=[
            DevConflict(summary=c.summary, evidence_ref_ids=list(c.evidence_ref_ids))
            for c in frame.conflicts
        ],
        coverage=_as_v1(DevCoverage, coverage),
        warnings=list(frame.limitations),
        suggested_follow_up_questions=list(frame.safe_follow_up_questions),
        versions=DevContractVersions(
            prompt_version=versions.prompt_version
            or _DETERMINISTIC_VERSION_PLACEHOLDER,
            tool_contract_version=versions.tool_contract_version,
            metric_definition_version=versions.metric_definition_version,
            query_version=versions.query_version,
        ),
        model=model_metadata,
    )


def _project_needs_clarification(
    answer: DevAnswerV2, organization_id: OpaqueID, time_range: DevTimeRange
) -> DevAnswer:
    """Project a ``needs_clarification`` v2 answer to a v1 ``DevAnswer``.

    CHAOS-3325: candidates are now projected from ``frame.clarification_
    candidates`` — the resolution ledger's own real, authorized entries — and
    never fabricated. Three cases, in priority order:

    1. ``frame.clarification_candidates`` is non-empty (the preflight
       ambiguous-mention case): project each real candidate and report
       ``ScopeResolutionOutcome.AMBIGUOUS``, matching v1's own invariant that
       an ``AMBIGUOUS`` resolution carries candidates
       (``DevScopeResolution.validate_outcome_payload``).
    2. ``frame.subject_ref`` is set instead (pre-CHAOS-3325 behavior,
       unchanged): a committed subject that itself needs narrowing on
       something else — never set by the preflight ambiguity path, which
       commits nothing (see ``build_preflight_answer``) — so this remains a
       single derived candidate from the one real, committed entity.
    3. Neither is present (e.g. the question could not be interpreted at all,
       before any mention was ever resolved — no real candidate list exists
       to report). The old code invented a placeholder entity
       (``entity_id="clarification_required"``) here to satisfy v1's
       ``AMBIGUOUS``-requires-candidates invariant; this reports
       ``ScopeResolutionOutcome.UNRESOLVED`` instead, which carries no
       candidates by the same invariant's other half
       (``candidates are allowed only for ambiguous outcomes``) — an honest
       "not resolved, nothing to offer" rather than a fabricated option.
    """

    frame = answer.frame
    versions = _require_versions(frame.versions, answer.public_outcome)
    resolved = _build_resolved_scope(answer, organization_id, time_range)
    resolution = scope_resolution_from_frame(
        frame, requested_scope=resolved, resolved_at=answer.generated_at
    )
    return DevAnswer(
        schema_version="dev_answer.v1",
        answer_id=answer.answer_id,
        conversation_id=answer.conversation_id,
        generated_at=answer.generated_at,
        resolved_scope=resolution,
        as_of=answer.generated_at,
        status=AnswerStatus.INSUFFICIENT_EVIDENCE,
        direct_summary=frame.direct_answer,
        claims=[],
        metrics=[],
        evidence=[],
        conflicts=[],
        coverage=_as_v1(DevCoverage, frame.coverage),
        warnings=list(frame.limitations),
        suggested_follow_up_questions=list(frame.safe_follow_up_questions),
        versions=DevContractVersions(
            prompt_version=versions.prompt_version
            or _DETERMINISTIC_VERSION_PLACEHOLDER,
            tool_contract_version=versions.tool_contract_version,
            metric_definition_version=versions.metric_definition_version,
            query_version=versions.query_version,
        ),
        model=DevModelMetadata(
            provider_source="platform",
            provider_family="deterministic",
            model_fingerprint=_DETERMINISTIC_VERSION_PLACEHOLDER,
        ),
    )


def _project_error(answer: DevAnswerV2) -> DevError:
    # Team-subject answers are intercepted earlier, in
    # `project_answer_v2_to_v1`, before this helper ever runs.
    #
    # Nothing is read off the frame here. A no-answer frame is already
    # constrained to canonical server copy by
    # `validators.validate_no_answer_projection`, but the projector building
    # its own copy from the same table (rather than copying the frame's
    # field through) means the v1 boundary cannot become a second reuse
    # channel if that constraint is ever relaxed: adversarial review's
    # `denied` counterexample reached a v1 client precisely because
    # `DevError.safe_message` was `frame.direct_answer` verbatim.
    safe_message, code, retryable, remediation = no_answer_error_projection(
        answer.public_outcome.value
    )
    return DevError(
        schema_version="dev_error.v1",
        request_id=answer.run_id,
        code=code,
        safe_message=safe_message,
        retryable=retryable,
        remediation=remediation,
    )


def project_answer_v2_to_v1(
    answer: DevAnswerV2,
    *,
    organization_id: OpaqueID,
    time_range: DevTimeRange,
) -> DevAnswer | DevError:
    """Project one ``dev_answer.v2`` to the retained v1 vocabulary.

    Returns a v1 ``DevAnswer`` for outcomes that mean "here is content"
    (``answered``, ``answered_with_gaps``, ``needs_clarification`` — the
    last mapped to v1's ``insufficient_evidence`` status, its closest
    existing analog), and a v1 ``DevError`` for every outcome that means "no
    content" (``not_found``, ``temporarily_unavailable``, ``unsupported``,
    ``denied``, ``failed``). ``organization_id``/``time_range`` are required
    because v1's ``DevScope`` needs them and v2's subject-centric frame does
    not carry them by design.
    """

    frame = answer.frame
    if (
        frame.subject_ref is not None
        and frame.subject_ref.entity_kind is EntityKind.TEAM
    ):
        # v1 has no team direct-scope representation at all (see module
        # docstring) — never mislabel a team answer as org/repo scoped.
        return DevError(
            schema_version="dev_error.v1",
            request_id=answer.run_id,
            code="feature_not_enabled",
            safe_message="Team-scoped Ask Dev answers require a newer client.",
            retryable=False,
            remediation=["Upgrade to a client that supports team-scoped answers."],
        )
    if frame.subject_set_ref is not None:
        # A subject-set (cohort) frame has no faithful v1 representation
        # either — see module docstring's Fidelity notes. Never fall through
        # to `_build_resolved_scope`'s "no subject_ref" branch, which would
        # silently widen the cohort to organization-wide scope.
        return DevError(
            schema_version="dev_error.v1",
            request_id=answer.run_id,
            code="feature_not_enabled",
            safe_message="Cohort-scoped Ask Dev answers require a newer client.",
            retryable=False,
            remediation=["Upgrade to a client that supports cohort-scoped answers."],
        )
    if answer.public_outcome in (
        PublicOutcome.ANSWERED,
        PublicOutcome.ANSWERED_WITH_GAPS,
    ):
        return _project_answered(answer, organization_id, time_range)
    if answer.public_outcome is PublicOutcome.NEEDS_CLARIFICATION:
        return _project_needs_clarification(answer, organization_id, time_range)
    return _project_error(answer)
