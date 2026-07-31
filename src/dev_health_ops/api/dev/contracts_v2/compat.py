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

* v1's ``DirectScope``/``EntityType`` have no ``team`` value (Wave 3.1 adds
  ``TEAM`` only at the v2 contract layer — see ``base.EntityKind``
  docstring for why it was not retrofitted onto v1). A team-subject v2
  answer therefore has no valid v1 representation; the projector returns a
  safe ``DevError`` (``feature_not_enabled``) rather than silently
  mislabeling a team answer as organization- or repository-scoped, which
  would violate the "no team attribution without disclosed scope"
  guardrail (Amendment PRD v2 §10) at the compatibility boundary.
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

from dev_health_ops.api.dev.contracts import (
    AnswerStatus,
    ClaimKind,
    DevAnswer,
    DevClaim,
    DevClaimFlags,
    DevConflict,
    DevContractVersions,
    DevDisambiguationCandidate,
    DevEntityRef,
    DevError,
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
from .base import EntityKind, OpaqueID, PublicOutcome
from .validators import CANONICAL_NO_ANSWER_COPY, CANONICAL_NO_ANSWER_REMEDIATION

__all__ = ["project_answer_v2_to_v1"]

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

_ERROR_OUTCOME_CODES: dict[PublicOutcome, tuple[str, bool]] = {
    PublicOutcome.NOT_FOUND: ("scope_not_found", False),
    PublicOutcome.TEMPORARILY_UNAVAILABLE: ("source_unavailable", True),
    PublicOutcome.UNSUPPORTED: ("feature_not_enabled", False),
    PublicOutcome.DENIED: ("forbidden", False),
    PublicOutcome.FAILED: ("internal_error", False),
}


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
    rule_version = frame.versions.rule_version
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
                flags=DevClaimFlags(),
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
        metrics=list(frame.metrics),
        evidence=list(frame.evidence),
        conflicts=[
            DevConflict(summary=c.summary, evidence_ref_ids=list(c.evidence_ref_ids))
            for c in frame.conflicts
        ],
        coverage=coverage,
        warnings=list(frame.limitations),
        suggested_follow_up_questions=list(frame.safe_follow_up_questions),
        versions=DevContractVersions(
            prompt_version=frame.versions.prompt_version
            or _DETERMINISTIC_VERSION_PLACEHOLDER,
            tool_contract_version=frame.versions.tool_contract_version,
            metric_definition_version=frame.versions.metric_definition_version,
            query_version=frame.versions.query_version,
        ),
        model=model_metadata,
    )


def _project_needs_clarification(
    answer: DevAnswerV2, organization_id: OpaqueID, time_range: DevTimeRange
) -> DevAnswer:
    frame = answer.frame
    resolved = _build_resolved_scope(answer, organization_id, time_range)
    resolution = DevScopeResolution(
        schema_version="dev_scope_resolution.v1",
        requested_scope=resolved,
        resolved_scope=None,
        outcome=ScopeResolutionOutcome.AMBIGUOUS,
        authorized_repository_ids=[],
        authorized_entity_ids=[],
        candidates=[
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
        if frame.subject_ref is not None
        else [
            DevDisambiguationCandidate(
                entity_ref=DevEntityRef(
                    entity_type=EntityType.REPOSITORY,
                    entity_id="clarification_required",
                    display_label="Clarification required",
                ),
                reason="The question requires clarification before it can be answered.",
            )
        ],
        fallbacks=[],
        warnings=[],
        resolved_at=answer.generated_at,
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
        coverage=frame.coverage,
        warnings=list(frame.limitations),
        suggested_follow_up_questions=list(frame.safe_follow_up_questions),
        versions=DevContractVersions(
            prompt_version=frame.versions.prompt_version
            or _DETERMINISTIC_VERSION_PLACEHOLDER,
            tool_contract_version=frame.versions.tool_contract_version,
            metric_definition_version=frame.versions.metric_definition_version,
            query_version=frame.versions.query_version,
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
    code, retryable = _ERROR_OUTCOME_CODES[answer.public_outcome]
    return DevError(
        schema_version="dev_error.v1",
        request_id=answer.run_id,
        code=code,
        safe_message=CANONICAL_NO_ANSWER_COPY[answer.public_outcome.value],
        retryable=retryable,
        remediation=(
            dev_error_remediation(code)
            or list(CANONICAL_NO_ANSWER_REMEDIATION[answer.public_outcome.value])
        ),
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
