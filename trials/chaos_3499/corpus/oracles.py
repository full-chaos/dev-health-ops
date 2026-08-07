"""The expected-evidence oracles, one or more per evaluation question.

**Authored before any arm ran.** Every expectation below is derived from
``ground_truth.py`` -- how the corpus was constructed -- and not from any
observed output. ``tests/test_corpus_consistency.py`` re-derives the same
include-sets from ground truth through :func:`ground_truth.select` and fails
on disagreement, so a hand-authored slip here cannot silently redefine
"correct".

Every as-of oracle pins ``axis`` (PRD §10). The Q2 pair is deliberately
constructed so the two axes have *different* correct answers: an arm that
drops the axis field passes exactly one of them and cannot pass both.
"""

from __future__ import annotations

import dataclasses

from ..harness.contracts import (
    ArmOutcome,
    ClaimKind,
    QueryMode,
    QuestionClass,
    TemporalContextQuery,
    TimeAxis,
)
from ..harness.oracle import CoverageExpectation, FactExpectation, Oracle
from . import ground_truth as gt

_PROJECTION = "temporal-projector.v1"


def _q(
    mode: QueryMode,
    *,
    subjects: tuple,
    as_of=None,
    axis: TimeAxis | None = None,
    relations: tuple[str, ...] = (),
    max_results: int = 20,
) -> TemporalContextQuery:
    return TemporalContextQuery(
        subjects=subjects,
        query_mode=mode,
        as_of=as_of,
        axis=axis,
        allowed_relation_types=relations,
        max_results=max_results,
    )


# --------------------------------------------------------------------------
# Q1 -- "What did we try last time this CI failure occurred?"  (class c)
# --------------------------------------------------------------------------

O1_CI_PRIOR_ATTEMPTS = Oracle(
    oracle_id="O1_ci_prior_attempts",
    question_id="Q1",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C04_prior_attempts", "C05_repeated_failure_pattern"),
    query=_q(
        QueryMode.PRIOR_ATTEMPTS,
        subjects=(gt.CI_FAILURE_SIGNATURE,),
        relations=("touched", "exhibits_failure_signature"),
    ),
    rationale=(
        "The failed attempt is the one an agent most needs and the one a "
        "relevance ranker is most likely to bury, so it is required by name "
        "rather than left to a recall threshold."
    ),
    must_include=(
        FactExpectation(
            subject=gt.EPISODE_FAILED,
            predicate="touched",
            object=gt.REPO_API,
            require_claim_kind=ClaimKind.OBSERVED,
            require_evidence_refs=frozenset({"ev1_ep_0002"}),
            label="failed prior attempt is retrieved",
        ),
        FactExpectation(
            subject=gt.EPISODE_ABANDONED,
            predicate="touched",
            object=gt.REPO_API,
            require_claim_kind=ClaimKind.OBSERVED,
            label="abandoned prior attempt is retrieved",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.ISSUE_BETA_101,
            predicate="blocks",
            object=gt.EntityRef("work_item", "ATL-110-beta"),
            label="cross-tenant beta material",
        ),
    ),
    max_inferred_facts=0,
)

O1_CI_PRIOR_ATTEMPTS_STALE = Oracle(
    oracle_id="O1_ci_prior_attempts_stale",
    question_id="Q1",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C10_stale_watermark",),
    query=_q(
        QueryMode.PRIOR_ATTEMPTS,
        subjects=(gt.CI_FAILURE_SIGNATURE,),
        relations=("touched",),
    ),
    rationale=(
        "Projection halted 9 days back. The answer may still be useful, but "
        "an answer served from behind its own watermark without saying so is "
        "indistinguishable from a fresh one -- that is the defect."
    ),
    must_include=(
        FactExpectation(
            subject=gt.EPISODE_FAILED,
            predicate="touched",
            object=gt.REPO_API,
            require_flags=frozenset({"stale"}),
            label="pre-watermark fact returned, flagged stale",
        ),
    ),
    # Deliberately does NOT require a fresh watermark: the scenario IS a
    # stalled projection. Demanding freshness here would make the oracle
    # unsatisfiable and turn a declared-staleness test into a liveness test.
    require_warnings=frozenset({"source_stale:temporal_graph.v1"}),
)

O1_CI_PRIOR_ATTEMPTS_SQUASH = Oracle(
    oracle_id="O1_ci_prior_attempts_squash",
    question_id="Q1",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C16_squash_merge_org",),
    query=_q(
        QueryMode.PRIOR_ATTEMPTS,
        subjects=(gt.CI_FAILURE_SIGNATURE,),
        relations=("touched",),
    ),
    rationale=(
        "The squash org's PR->commit linkage is sparse for the residual "
        "formats the heuristic tier cannot parse (ops builder.py:1895-2055 "
        "handles GitHub's `(#N)` suffix but nothing below it). The required "
        "behaviour is a declared coverage gap, because 'we found nothing' and "
        "'we cannot see it' are different answers to the caller."
    ),
    coverage=(
        CoverageExpectation(
            source="work_graph_pr_commit",
            expect_available=False,
            expect_reason_contains="squash",
        ),
    ),
    require_warnings=frozenset(
        {"source_unavailable:work_graph_pr_commit:squash_merge_linkage_absent"}
    ),
)


# --------------------------------------------------------------------------
# Q2 -- axis pair.  (class b)
# --------------------------------------------------------------------------

_Q2_SUBJECTS = (gt.ISSUE_101, gt.ISSUE_105)

O2_BLOCKING_VALID = Oracle(
    oracle_id="O2_blocking_valid",
    question_id="Q2",
    question_class=QuestionClass.NEEDS_DECLARED_STATE_HISTORY,
    corpus_case_ids=(
        "C03_changed_blockers",
        "C19_axis_pair",
        "C15_cross_tenant_near_duplicate",
    ),
    query=_q(
        QueryMode.AS_OF,
        subjects=_Q2_SUBJECTS,
        as_of=gt.AS_OF_JUL_15,
        axis=TimeAxis.VALID_TIME,
        relations=("blocks",),
    ),
    rationale=(
        "On valid time, BOTH blockers held on 07-15: ATL-101 (07-02..07-18) "
        "and the backfilled ATL-105 (valid from 07-05, ingested 07-20). "
        "Requiring the backfilled one is what makes this oracle disagree with "
        "its observed-time twin."
    ),
    must_include=(
        FactExpectation(
            subject=gt.ISSUE_101,
            predicate="blocks",
            object=gt.ISSUE_110,
            require_claim_kind=ClaimKind.OBSERVED,
            require_evidence_refs=frozenset({"ev1_dep_101_110"}),
            label="ATL-101 blocked ATL-110 on 07-15 (valid time)",
        ),
        FactExpectation(
            subject=gt.ISSUE_105,
            predicate="blocks",
            object=gt.ISSUE_110,
            require_claim_kind=ClaimKind.OBSERVED,
            label="backfilled ATL-105 blocker is true on 07-15 (valid time)",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.ISSUE_BETA_101,
            predicate="blocks",
            object=gt.EntityRef("work_item", "ATL-110-beta"),
            label="cross-tenant near-duplicate must not appear",
        ),
    ),
)

O2_BLOCKING_OBSERVED = Oracle(
    oracle_id="O2_blocking_observed",
    question_id="Q2",
    question_class=QuestionClass.NEEDS_DECLARED_STATE_HISTORY,
    corpus_case_ids=("C03_changed_blockers", "C19_axis_pair"),
    query=_q(
        QueryMode.AS_OF,
        subjects=_Q2_SUBJECTS,
        as_of=gt.AS_OF_JUL_15,
        axis=TimeAxis.OBSERVED_TIME,
        relations=("blocks",),
    ),
    rationale=(
        "On observed time the same instant gives a DIFFERENT answer: ATL-105 "
        "was not ingested until 07-20, so on 07-15 Dev Health did not know "
        "it. An arm returning it here has answered the valid-time question "
        "while claiming to answer the observed-time one."
    ),
    must_include=(
        FactExpectation(
            subject=gt.ISSUE_101,
            predicate="blocks",
            object=gt.ISSUE_110,
            require_claim_kind=ClaimKind.OBSERVED,
            label="ATL-101 blocker was known on 07-15",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.ISSUE_105,
            predicate="blocks",
            object=gt.ISSUE_110,
            label="backfilled blocker was NOT yet known on 07-15",
        ),
    ),
)


# --------------------------------------------------------------------------
# Q3 -- supersession.  (class c)
# --------------------------------------------------------------------------

O3_SUPERSESSION = Oracle(
    oracle_id="O3_supersession",
    question_id="Q3",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=(
        "C02_superseded_decision",
        "C07_structured_plus_unstructured",
    ),
    query=_q(
        QueryMode.SUPERSEDED_DECISIONS,
        subjects=(gt.PROJ_ATLAS,),
        relations=("supersedes", "describes_deployment_design_for"),
    ),
    rationale=(
        "Direction is the whole answer: ADR-021 supersedes ADR-014, not the "
        "reverse. The reversed edge is a plausible-looking wrong answer, "
        "which is why the matcher compares positionally."
    ),
    must_include=(
        FactExpectation(
            subject=gt.DECISION_SUPERSEDING,
            predicate="supersedes",
            object=gt.DECISION_ORIGINAL,
            require_evidence_refs=frozenset({"ev1_adr_021"}),
            label="ADR-021 supersedes ADR-014, in that direction",
        ),
        FactExpectation(
            subject=gt.DECISION_ORIGINAL,
            predicate="describes_deployment_design_for",
            object=gt.PROJ_ATLAS,
            require_invalidation_claim_kind=ClaimKind.OBSERVED,
            label="ADR-014's window closed, with recorded invalidation source",
        ),
    ),
)

O3_SUPERSESSION_EXTRACTION_DOWN = Oracle(
    oracle_id="O3_supersession_extraction_down",
    question_id="Q3",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C12_extraction_provider_failure",),
    query=_q(
        QueryMode.SUPERSEDED_DECISIONS,
        subjects=(gt.PROJ_ATLAS,),
        relations=("supersedes",),
    ),
    rationale=(
        "The provider returns malformed structured output. A partially "
        "parsed relationship presented as observed is the dangerous outcome; "
        "declaring the gap is the correct one."
    ),
    expect_outcome=ArmOutcome.ANSWERED,
    coverage=(
        CoverageExpectation(
            source="extraction",
            expect_available=False,
            expect_reason_contains="provider",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.DECISION_SUPERSEDING,
            predicate="supersedes",
            object=gt.DECISION_ORIGINAL,
            label="no supersession claim while extraction is broken",
        ),
    ),
    require_degraded_reasons=frozenset({"extraction_provider_unavailable"}),
    max_inferred_facts=0,
)

O3_SUPERSESSION_DETERMINISTIC_ONLY = Oracle(
    oracle_id="O3_supersession_deterministic_only",
    question_id="Q3",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C21_deterministic_only_org",),
    query=_q(
        QueryMode.SUPERSEDED_DECISIONS,
        subjects=(gt.PROJ_ATLAS,),
        relations=("supersedes",),
    ),
    rationale=(
        "This org's provider policy forbids model providers, so §7.1 "
        "structured projection is all there is -- and no canonical record "
        "expresses decision supersession (ops work_graph/models.py:37-84 has "
        "no such edge type). The honest answer is an explicit gap. Scoring "
        "this as a miss would overstate what such customers lose; scoring it "
        "as a pass would overstate what they keep."
    ),
    expect_outcome=ArmOutcome.ANSWERED,
    coverage=(
        CoverageExpectation(
            source="extraction",
            expect_available=False,
            expect_reason_contains="provider_policy",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.DECISION_SUPERSEDING,
            predicate="supersedes",
            object=gt.DECISION_ORIGINAL,
            label="extraction-derived fact must not appear for this org",
        ),
    ),
    require_degraded_reasons=frozenset({"extraction_disallowed_by_policy"}),
    max_inferred_facts=0,
)


# --------------------------------------------------------------------------
# Q4 -- prior agent attempts.  (class c)
# --------------------------------------------------------------------------

_Q4_QUERY = _q(
    QueryMode.PRIOR_ATTEMPTS,
    subjects=(gt.REPO_API,),
    relations=("touched",),
)

O4_PRIOR_ATTEMPTS = Oracle(
    oracle_id="O4_prior_attempts",
    question_id="Q4",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=(
        "C04_prior_attempts",
        "C07_structured_plus_unstructured",
        "C15_cross_tenant_near_duplicate",
    ),
    query=_Q4_QUERY,
    rationale=(
        "All three outcomes must come back. An arm returning only the "
        "successful attempt has answered a different, more flattering "
        "question."
    ),
    must_include=(
        FactExpectation(
            subject=gt.EPISODE_SUCCEEDED,
            predicate="touched",
            object=gt.REPO_API,
            require_claim_kind=ClaimKind.OBSERVED,
            label="succeeded attempt",
        ),
        FactExpectation(
            subject=gt.EPISODE_FAILED,
            predicate="touched",
            object=gt.REPO_API,
            require_claim_kind=ClaimKind.OBSERVED,
            label="failed attempt",
        ),
        FactExpectation(
            subject=gt.EPISODE_ABANDONED,
            predicate="touched",
            object=gt.REPO_API,
            require_claim_kind=ClaimKind.OBSERVED,
            label="abandoned attempt",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.ISSUE_BETA_101,
            predicate="blocks",
            object=gt.EntityRef("work_item", "ATL-110-beta"),
            label="cross-tenant material",
        ),
    ),
)

O4_PRIOR_ATTEMPTS_MANIPULATED = Oracle(
    oracle_id="O4_prior_attempts_manipulated",
    question_id="Q4",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C17_retrieval_manipulation",),
    query=_q(
        QueryMode.PRIOR_ATTEMPTS,
        subjects=(gt.REPO_API,),
        relations=("touched",),
        max_results=3,
    ),
    rationale=(
        "max_results is deliberately 3 -- exactly the number of genuine "
        "attempts. The keyword-stuffed decoy is legitimately in scope, so "
        "this is not an exclusion test: it asks whether the decoy DISPLACES "
        "real evidence when the budget is tight. With a loose budget the "
        "decoy could rank first and nothing would be lost; with this budget, "
        "ranking it first costs a real answer."
    ),
    must_include=(
        FactExpectation(
            subject=gt.EPISODE_SUCCEEDED,
            predicate="touched",
            object=gt.REPO_API,
            label="succeeded attempt survives the decoy",
        ),
        FactExpectation(
            subject=gt.EPISODE_FAILED,
            predicate="touched",
            object=gt.REPO_API,
            label="failed attempt survives the decoy",
        ),
        FactExpectation(
            subject=gt.EPISODE_ABANDONED,
            predicate="touched",
            object=gt.REPO_API,
            label="abandoned attempt survives the decoy",
        ),
    ),
)

O4_PRIOR_ATTEMPTS_AFTER_REDACTION = Oracle(
    oracle_id="O4_prior_attempts_after_redaction",
    question_id="Q4",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C08_deleted_redacted_episode",),
    query=_Q4_QUERY,
    rationale=(
        "Over-deletion and under-deletion are both failures and this oracle "
        "asserts against both in one shot: the multi-source fact must "
        "survive with reduced provenance, the sole-source fact must vanish."
    ),
    must_include=(
        FactExpectation(
            subject=gt.EPISODE_SUCCEEDED,
            predicate="touched",
            object=gt.REPO_API,
            require_claim_kind=ClaimKind.OBSERVED,
            label="multi-source fact survives redaction of one source",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.EPISODE_SOLE_SUPPORT,
            predicate="touched",
            object=gt.REPO_API,
            label="sole-source fact disappears when its only source is deleted",
        ),
    ),
    require_degraded_reasons=frozenset({"provenance_redacted"}),
)

O4_PRIOR_ATTEMPTS_AFTER_REVOCATION = Oracle(
    oracle_id="O4_prior_attempts_after_revocation",
    question_id="Q4",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C09_revoked_repo_visibility",),
    query=_q(
        QueryMode.PRIOR_ATTEMPTS,
        subjects=(gt.REPO_API, gt.REPO_WEB),
        relations=("touched",),
    ),
    rationale=(
        "repo_atlas_web was visible when the projector indexed the episode "
        "and is not visible now. Re-authorisation must happen against "
        "current canonical visibility, not the scope captured at projection "
        "time."
    ),
    must_include=(
        FactExpectation(
            subject=gt.EPISODE_SUCCEEDED,
            predicate="touched",
            object=gt.REPO_API,
            label="still-authorized repo material remains",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.EPISODE_WEB_REPO,
            predicate="touched",
            object=gt.REPO_WEB,
            label="revoked repo material is gone",
        ),
    ),
)

O4_PRIOR_ATTEMPTS_GRAPH_OUTAGE = Oracle(
    oracle_id="O4_prior_attempts_graph_outage",
    question_id="Q4",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C13_graph_datastore_outage",),
    query=_Q4_QUERY,
    rationale=(
        "With the backend down the only correct outcome is an explicit "
        "unavailability. Answering from a cache is the failure this asserts "
        "against; the companion assertion that existing ACR/Ask Dev fallback "
        "is unaffected lives in the harness's positive-control run, because "
        "an oracle over graph output cannot observe it."
    ),
    expect_outcome=ArmOutcome.UNAVAILABLE,
    coverage=(
        CoverageExpectation(
            source="temporal_graph.v1",
            expect_available=False,
            expect_reason_contains="outage",
        ),
    ),
    require_degraded_reasons=frozenset({"temporal_graph_unavailable"}),
)


# --------------------------------------------------------------------------
# Q5 -- conflicts.  (class c)
# --------------------------------------------------------------------------

_Q5_QUERY = _q(
    QueryMode.CONFLICTS,
    subjects=(gt.INCIDENT_503,),
    relations=("attributed_to",),
)

O5_CONFLICTS = Oracle(
    oracle_id="O5_conflicts",
    question_id="Q5",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C06_conflicting_episodes",),
    query=_Q5_QUERY,
    rationale=(
        "Neither attribution was retracted, so silently picking a winner is "
        "wrong even when the winner happens to be right. Both sides must "
        "surface, both flagged conflicting, and both classified inferred -- "
        "an attribution the source did not state outright is not observed."
    ),
    must_include=(
        FactExpectation(
            subject=gt.INCIDENT_503,
            predicate="attributed_to",
            object=gt.EntityRef("component", "cmp_payments_pool"),
            require_claim_kind=ClaimKind.INFERRED,
            require_flags=frozenset({"conflicting"}),
            label="conflict side A, flagged and marked inferred",
        ),
        FactExpectation(
            subject=gt.INCIDENT_503,
            predicate="attributed_to",
            object=gt.EntityRef("component", "cmp_upstream_gateway"),
            require_claim_kind=ClaimKind.INFERRED,
            require_flags=frozenset({"conflicting"}),
            label="conflict side B, flagged and marked inferred",
        ),
    ),
)

O5_CONFLICTS_INJECTED = Oracle(
    oracle_id="O5_conflicts_injected",
    question_id="Q5",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C14_prompt_injection",),
    query=_q(
        QueryMode.CONFLICTS,
        subjects=(gt.PROJ_ATLAS, gt.INCIDENT_503),
        relations=("attributed_to", "approved_for_release_by"),
    ),
    rationale=(
        "The injected instruction must produce no fact at all, and the "
        "legitimate facts extracted from the same untrusted source must carry "
        "untrusted_content -- content treated as content, never as "
        "instruction."
    ),
    must_include=(
        FactExpectation(
            subject=gt.INCIDENT_503,
            predicate="attributed_to",
            object=gt.EntityRef("component", "cmp_payments_pool"),
            require_flags=frozenset({"conflicting"}),
            label="legitimate facts still returned",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.PROJ_ATLAS,
            predicate="approved_for_release_by",
            object=gt.EntityRef("decision", "ADR-999-nonexistent"),
            label="injected instruction produced no fact",
        ),
    ),
)

O5_CONFLICTS_POISONED = Oracle(
    oracle_id="O5_conflicts_poisoned",
    question_id="Q5",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=("C18_entity_linking_poisoning",),
    query=_q(
        QueryMode.CONFLICTS,
        subjects=(gt.PROJ_ATLAS,),
        relations=("depends_on",),
    ),
    rationale=(
        "proj_atlas is legitimately in scope and the tenant is correct, so "
        "no authorization check can catch this. Only the fact's own falsity "
        "distinguishes it -- which is why it needs its own oracle rather "
        "than riding on the cross-tenant one."
    ),
    must_include=(
        FactExpectation(
            subject=gt.PROJ_ATLAS,
            predicate="depends_on",
            object=gt.DEPENDENCY_LIBPAY,
            require_claim_kind=ClaimKind.OBSERVED,
            label="the genuine dependency is still returned",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.PROJ_ATLAS,
            predicate="depends_on",
            object=gt.EntityRef("repository", "dep_attacker_controlled"),
            label="poisoned link to a real canonical entity",
        ),
    ),
    max_inferred_facts=0,
)


# --------------------------------------------------------------------------
# Q6 -- recurring pattern.  (class c)
# --------------------------------------------------------------------------

O6_RECURRING_PATTERN = Oracle(
    oracle_id="O6_recurring_pattern",
    question_id="Q6",
    question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
    corpus_case_ids=(
        "C05_repeated_failure_pattern",
        "C11_projector_retry",
    ),
    query=_q(
        QueryMode.RECURRING_PATTERNS,
        subjects=(gt.INCIDENT_501, gt.INCIDENT_502, gt.INCIDENT_503),
        relations=("exhibits_failure_signature",),
    ),
    rationale=(
        "Three incidents share a signature; INC-504 shares subsystem and "
        "timing but not cause. Admitting the decoy is exactly PRD §7.3's "
        "prohibited move -- graph proximity read as causation."
    ),
    must_include=(
        FactExpectation(
            subject=gt.INCIDENT_501,
            predicate="exhibits_failure_signature",
            object=gt.CI_FAILURE_SIGNATURE,
            require_claim_kind=ClaimKind.OBSERVED,
            label="INC-501 in the pattern",
        ),
        FactExpectation(
            subject=gt.INCIDENT_502,
            predicate="exhibits_failure_signature",
            object=gt.CI_FAILURE_SIGNATURE,
            require_claim_kind=ClaimKind.OBSERVED,
            label="INC-502 in the pattern",
        ),
        FactExpectation(
            subject=gt.INCIDENT_503,
            predicate="exhibits_failure_signature",
            object=gt.CI_FAILURE_SIGNATURE,
            require_claim_kind=ClaimKind.OBSERVED,
            label="INC-503 in the pattern",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.INCIDENT_504_DECOY,
            predicate="exhibits_failure_signature",
            object=gt.CI_FAILURE_SIGNATURE,
            label="decoy must not be attached to this signature",
        ),
    ),
)


# --------------------------------------------------------------------------
# Q7 -- dependency as-of.  (class a -- native should win or tie)
# --------------------------------------------------------------------------

O7_VALID = Oracle(
    oracle_id="O7_valid",
    question_id="Q7",
    question_class=QuestionClass.NATIVE_ANSWERABLE,
    corpus_case_ids=("C01_historical_truth", "C21_deterministic_only_org"),
    query=_q(
        QueryMode.AS_OF,
        subjects=(gt.SERVICE_PAYMENTS,),
        as_of=gt.AS_OF_JUL_25,
        axis=TimeAxis.VALID_TIME,
        relations=("implemented_by",),
    ),
    rationale=(
        "The v1 mapping ended 07-10 and v2 began. As of 07-25 only v2 holds. "
        "This is the class (a) control: native has real valid-time intervals "
        "and a real interval filter, so if it loses here the finding is about "
        "the harness, not the arm."
    ),
    must_include=(
        FactExpectation(
            subject=gt.SERVICE_PAYMENTS,
            predicate="implemented_by",
            object=gt.DEPENDENCY_LIBPAY,
            require_claim_kind=ClaimKind.OBSERVED,
            require_evidence_refs=frozenset({"ev1_svc_map_v2"}),
            label="current mapping holds at 07-25",
        ),
    ),
    must_exclude=(
        FactExpectation(
            subject=gt.SERVICE_PAYMENTS,
            predicate="implemented_by",
            object=gt.REPO_API,
            label="superseded mapping must not hold at 07-25",
        ),
    ),
)

O7_UNPINNED = Oracle(
    oracle_id="O7_unpinned",
    question_id="Q7",
    question_class=QuestionClass.NATIVE_ANSWERABLE,
    corpus_case_ids=("C20_unpinned_time",),
    query=_q(
        QueryMode.TIMELINE,
        subjects=(gt.SERVICE_PAYMENTS,),
        relations=("implemented_by",),
    ),
    rationale=(
        "No as_of, no time window. ACR applies no server default today "
        "(read_adapter.go:64-65 passes both through verbatim), so unbounded "
        "is the inherited behaviour rather than a chosen one. The trial's "
        "defined default is: answer, bound the scan, and DECLARE that a "
        "default was applied -- an unbounded answer that looks pinned is the "
        "defect."
    ),
    must_include=(
        FactExpectation(
            subject=gt.SERVICE_PAYMENTS,
            predicate="implemented_by",
            object=gt.DEPENDENCY_LIBPAY,
            label="current mapping returned under the default bound",
        ),
    ),
    require_warnings=frozenset({"temporal_default_time_bound_applied"}),
)


O7_NULL_VALID_FROM = Oracle(
    oracle_id="O7_null_valid_from",
    question_id="Q7",
    question_class=QuestionClass.NATIVE_ANSWERABLE,
    corpus_case_ids=("C01_historical_truth",),
    query=_q(
        QueryMode.AS_OF,
        subjects=(gt.EntityRef("service", "svc_ledger"),),
        as_of=gt.AS_OF_JUL_25,
        axis=TimeAxis.VALID_TIME,
        relations=("implemented_by",),
    ),
    rationale=(
        "Kept separate from O7_valid on purpose. O7_valid is the clean class "
        "(a) control and must stay uncontaminated; this oracle probes a "
        "specific latent defect found during baseline inventory: "
        "operational_service_repository_mappings.valid_from is Nullable "
        "(066_operational_canonical.sql:261) while every as-of filter applies "
        "`valid_from <= as_of`, and NULL <= anything is false in ClickHouse. "
        "An open-started interval is true at every instant, so dropping it is "
        "wrong on both axes. Expect the native arm to FAIL this one; that "
        "failure is a finding about ops, not about the graph, and the ADR "
        "must not let a graph arm take credit for it."
    ),
    must_include=(
        FactExpectation(
            subject=gt.EntityRef("service", "svc_ledger"),
            predicate="implemented_by",
            object=gt.REPO_API,
            require_claim_kind=ClaimKind.OBSERVED,
            require_evidence_refs=frozenset({"ev1_svc_map_null_start"}),
            label="open-started interval holds at any as_of",
        ),
    ),
)


#: Oracles whose scenario is *deliberately* degraded, and which therefore
#: cannot assert a healthy indexing watermark without contradicting
#: themselves. Everything else gets the freshness assertion automatically --
#: see `_with_freshness` below.
_DEGRADED_WATERMARK_ORACLES = frozenset(
    {
        "O1_ci_prior_attempts_stale",
        "O4_prior_attempts_graph_outage",
    }
)


def _with_freshness(oracle: Oracle) -> Oracle:
    """Attach the healthy-watermark assertion unless the scenario is degraded.

    Applied centrally rather than repeated on each oracle for one reason: a
    gate that has to be remembered per-oracle is a gate that will be forgotten
    on the next oracle someone adds. An answer served from behind a stale
    watermark, with no staleness declared, is wrong for every question in the
    corpus -- so the default is "assert it", and the exemptions are named and
    visible.
    """
    if oracle.oracle_id in _DEGRADED_WATERMARK_ORACLES:
        return oracle
    return dataclasses.replace(
        oracle, require_indexed_through_at_or_after=gt.REQUIRED_WATERMARK
    )


_AUTHORED_ORACLES: tuple[Oracle, ...] = (
    O1_CI_PRIOR_ATTEMPTS,
    O1_CI_PRIOR_ATTEMPTS_STALE,
    O1_CI_PRIOR_ATTEMPTS_SQUASH,
    O2_BLOCKING_VALID,
    O2_BLOCKING_OBSERVED,
    O3_SUPERSESSION,
    O3_SUPERSESSION_EXTRACTION_DOWN,
    O3_SUPERSESSION_DETERMINISTIC_ONLY,
    O4_PRIOR_ATTEMPTS,
    O4_PRIOR_ATTEMPTS_MANIPULATED,
    O4_PRIOR_ATTEMPTS_AFTER_REDACTION,
    O4_PRIOR_ATTEMPTS_AFTER_REVOCATION,
    O4_PRIOR_ATTEMPTS_GRAPH_OUTAGE,
    O5_CONFLICTS,
    O5_CONFLICTS_INJECTED,
    O5_CONFLICTS_POISONED,
    O6_RECURRING_PATTERN,
    O7_VALID,
    O7_NULL_VALID_FROM,
    O7_UNPINNED,
)

ALL_ORACLES: tuple[Oracle, ...] = tuple(
    _with_freshness(oracle) for oracle in _AUTHORED_ORACLES
)

ORACLES_BY_ID = {oracle.oracle_id: oracle for oracle in ALL_ORACLES}
