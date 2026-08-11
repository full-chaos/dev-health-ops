"""The synthetic world the corpus is built from.

This module is the *construction record*, not an answer key derived from any
arm's behaviour. It states, for every planted relationship, both time axes
(``valid_from``/``valid_to`` = when it was true; ``observed_at`` = when Dev
Health learned it), which tenant owns it, and which repository scope it sits
behind. Everything an oracle asserts is derivable from here.

Why both axes are recorded explicitly rather than assumed equal: a backfilled
blocker is true before it is known, and PRD §10 makes the axis a required
field precisely because the two answers differ. Recording only one axis here
would make the axis-pair case (C19) unbuildable and the required field
untestable.

Times are pinned constants. Nothing in this module may read the wall clock --
a corpus whose expected answers move with the calendar is not pinned, and the
rebuild gate (§16) compares against it.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from datetime import UTC, datetime

from ..harness.contracts import ClaimKind, EntityRef

# --------------------------------------------------------------------------
# Pinned clock
# --------------------------------------------------------------------------


def _t(spec: str) -> datetime:
    return datetime.fromisoformat(spec).replace(tzinfo=UTC)


CORPUS_START = _t("2026-06-01T00:00:00")
AS_OF_JUL_15 = _t("2026-07-15T00:00:00")
AS_OF_JUL_25 = _t("2026-07-25T00:00:00")
TRIAL_NOW = _t("2026-07-31T00:00:00")

#: The watermark a healthy projection must have reached for TRIAL_NOW queries.
#: Corpus case C10 plants a projection halted well behind this.
REQUIRED_WATERMARK = _t("2026-07-30T00:00:00")
STALLED_WATERMARK = _t("2026-07-21T00:00:00")

# --------------------------------------------------------------------------
# Tenants and entities
# --------------------------------------------------------------------------

ORG_ALPHA = "org_trial_alpha"
#: The near-duplicate tenant (C15). Same project name, similar issue keys.
ORG_BETA = "org_trial_beta"
#: Provider policy forbids model providers, so extraction never runs (C21).
ORG_GAMMA_DETERMINISTIC_ONLY = "org_trial_gamma"
#: Squash-merge org: work_graph_pr_commit is effectively empty (C16).
ORG_DELTA_SQUASH = "org_trial_delta"

PROJ_ATLAS = EntityRef("project", "proj_atlas")
PROJ_ATLAS_BETA = EntityRef("project", "proj_atlas_beta")

REPO_API = EntityRef("repository", "repo_atlas_api")
REPO_WEB = EntityRef("repository", "repo_atlas_web")

ISSUE_101 = EntityRef("work_item", "ATL-101")
ISSUE_105 = EntityRef("work_item", "ATL-105")
ISSUE_110 = EntityRef("work_item", "ATL-110")
ISSUE_BETA_101 = EntityRef("work_item", "ATL-101-beta")

DECISION_ORIGINAL = EntityRef("decision", "ADR-014")
DECISION_SUPERSEDING = EntityRef("decision", "ADR-021")

INCIDENT_501 = EntityRef("incident", "INC-501")
INCIDENT_502 = EntityRef("incident", "INC-502")
INCIDENT_503 = EntityRef("incident", "INC-503")
#: Superficially similar, different root cause. Must not join the pattern.
INCIDENT_504_DECOY = EntityRef("incident", "INC-504")

CI_FAILURE_SIGNATURE = EntityRef("ci_failure_signature", "sig_payments_timeout")

EPISODE_SUCCEEDED = EntityRef("agent_episode", "ep_alpha_0001")
EPISODE_FAILED = EntityRef("agent_episode", "ep_alpha_0002")
EPISODE_ABANDONED = EntityRef("agent_episode", "ep_alpha_0003")
#: Only supporting source for one fact; deleted in C08.
EPISODE_SOLE_SUPPORT = EntityRef("agent_episode", "ep_alpha_0004")
#: Keyword-stuffed decoy carrying no real evidence (C17).
EPISODE_KEYWORD_STUFFED = EntityRef("agent_episode", "ep_alpha_9001")
#: Web-repo episode, hidden once repo_atlas_web visibility is revoked (C09).
EPISODE_WEB_REPO = EntityRef("agent_episode", "ep_alpha_0005")

DEPENDENCY_LIBPAY = EntityRef("repository", "dep_libpay")
SERVICE_PAYMENTS = EntityRef("service", "svc_payments")


# --------------------------------------------------------------------------
# Ground-truth facts
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class GroundTruthFact:
    """One planted relationship, with everything needed to decide as-of.

    ``observed_at`` is when Dev Health learned the relationship;
    ``valid_from``/``valid_to`` are when it held in the world.
    ``invalidation_observed_at`` is when Dev Health learned the relationship
    had ended -- distinct from ``valid_to`` itself, and the reason an
    observed-time as-of query can still consider a window open after its
    valid-time end.
    """

    fact_key: str
    org_id: str
    subject: EntityRef
    predicate: str
    object: EntityRef
    observed_at: datetime
    claim_kind: ClaimKind
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    invalidation_observed_at: datetime | None = None
    evidence_refs: tuple[str, ...] = ()
    source_event_refs: tuple[str, ...] = ()
    #: fact_key of the record whose OWN evidence documents this fact's
    #: closure -- distinct from this fact's opening evidence. None means this
    #: fact is self-evidencing: a structured field (e.g. a canonical
    #: dependency mapping) whose own record directly carries the closure, not
    #: derived from prose elsewhere. Set this whenever the closure is only
    #: evidenced by a DIFFERENT record (e.g. a superseding ADR's own prose) --
    #: leaving it None there would let `invalidated_by` cite the opening
    #: record's evidence for a closure that record never spoke to.
    invalidated_by_fact_key: str | None = None
    repo_scope: EntityRef | None = None
    #: Corpus cases this fact was planted for; used by the coverage test.
    for_cases: tuple[str, ...] = ()
    #: True for material an arm must never emit as a fact at all: injected
    #: instructions, poisoned links, cross-tenant bleed, ranking decoys.
    is_adversarial: bool = False
    #: True for a *legitimate* fact planted alongside attack material, so the
    #: security oracle asserts against a non-empty answer. Without this flag
    #: an exclusion-only oracle is satisfied by an arm that returns nothing at
    #: all, which is not the behaviour under test. Every fact tagged to an
    #: attack case must set exactly one of `is_adversarial` / `is_control`,
    #: so the author has to decide which it is rather than defaulting.
    is_control: bool = False
    note: str = ""

    def true_at(self, as_of: datetime) -> bool:
        """Valid-time membership: did this hold in the world at ``as_of``?"""
        if self.valid_from is not None and as_of < self.valid_from:
            return False
        if self.valid_to is not None and as_of >= self.valid_to:
            return False
        return True

    def known_at(self, as_of: datetime) -> bool:
        """Observed-time membership: had Dev Health learned it by ``as_of``?

        A window whose *end* was not yet observed is still open on this axis,
        even when valid-time has already closed it. That asymmetry is the
        whole content of the axis-pair case, so it is written out rather than
        folded into :meth:`true_at`.
        """
        if as_of < self.observed_at:
            return False
        if (
            self.invalidation_observed_at is not None
            and as_of >= self.invalidation_observed_at
        ):
            return False
        return True


GROUND_TRUTH: tuple[GroundTruthFact, ...] = (
    # -- C03 / C19: blockers on ATL-110, one of them backfilled -------------
    GroundTruthFact(
        fact_key="gt_blocks_101_110",
        org_id=ORG_ALPHA,
        subject=ISSUE_101,
        predicate="blocks",
        object=ISSUE_110,
        observed_at=_t("2026-07-02T09:00:00"),
        valid_from=_t("2026-07-02T09:00:00"),
        valid_to=_t("2026-07-18T16:00:00"),
        invalidation_observed_at=_t("2026-07-18T16:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_dep_101_110",),
        repo_scope=REPO_API,
        for_cases=("C03_changed_blockers",),
        note="Observed and valid at the same instant; ends before TRIAL_NOW.",
    ),
    GroundTruthFact(
        fact_key="gt_blocks_105_110_backfilled",
        org_id=ORG_ALPHA,
        subject=ISSUE_105,
        predicate="blocks",
        object=ISSUE_110,
        # True from 07-05, but not ingested until 07-20: the axis pair.
        observed_at=_t("2026-07-20T11:00:00"),
        valid_from=_t("2026-07-05T08:00:00"),
        valid_to=None,
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_dep_105_110",),
        repo_scope=REPO_API,
        for_cases=("C03_changed_blockers", "C19_axis_pair"),
        note=(
            "As of 07-15: true on valid_time, not-yet-known on observed_time. "
            "An arm that ignores `axis` cannot satisfy both oracles."
        ),
    ),
    # -- C15: cross-tenant near-duplicate ----------------------------------
    GroundTruthFact(
        fact_key="gt_beta_blocks",
        org_id=ORG_BETA,
        subject=ISSUE_BETA_101,
        predicate="blocks",
        object=EntityRef("work_item", "ATL-110-beta"),
        observed_at=_t("2026-07-03T09:00:00"),
        valid_from=_t("2026-07-03T09:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_beta_dep",),
        for_cases=("C15_cross_tenant_near_duplicate",),
        is_adversarial=True,
        note="Any appearance in an alpha answer is a leak, not a ranking miss.",
    ),
    # -- C02 / C07: superseded decision ------------------------------------
    GroundTruthFact(
        fact_key="gt_adr021_supersedes_adr014",
        org_id=ORG_ALPHA,
        subject=DECISION_SUPERSEDING,
        predicate="supersedes",
        object=DECISION_ORIGINAL,
        observed_at=_t("2026-07-09T14:00:00"),
        valid_from=_t("2026-07-09T14:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_adr_021",),
        source_event_refs=("sevt_adr_021",),
        repo_scope=REPO_API,
        for_cases=("C02_superseded_decision", "C07_structured_plus_unstructured"),
        note=(
            "Stated only in ADR-021 prose. Deterministic-only orgs cannot "
            "derive it, which is exactly what C21 measures."
        ),
    ),
    GroundTruthFact(
        fact_key="gt_adr014_superseded",
        org_id=ORG_ALPHA,
        subject=DECISION_ORIGINAL,
        predicate="describes_deployment_design_for",
        object=PROJ_ATLAS,
        observed_at=_t("2026-06-04T10:00:00"),
        valid_from=_t("2026-06-04T10:00:00"),
        valid_to=_t("2026-07-09T14:00:00"),
        invalidation_observed_at=_t("2026-07-09T14:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_adr_014",),
        source_event_refs=("sevt_adr_014",),
        # ADR-014's own evidence documents its ORIGINAL design claim, not its
        # supersession -- that is stated only in ADR-021's prose (C02/C07).
        # Citing ev1_adr_014 for the closure would be citing the opening
        # record's evidence as if it were the invalidating one.
        invalidated_by_fact_key="gt_adr021_supersedes_adr014",
        repo_scope=REPO_API,
        for_cases=("C02_superseded_decision",),
    ),
    # -- C04 / C08 / C09 / C17: agent episodes -----------------------------
    GroundTruthFact(
        fact_key="gt_ep1_touched",
        org_id=ORG_ALPHA,
        subject=EPISODE_SUCCEEDED,
        predicate="touched",
        object=REPO_API,
        observed_at=_t("2026-06-18T12:00:00"),
        valid_from=_t("2026-06-18T12:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        source_event_refs=("sevt_ep_0001",),
        evidence_refs=("ev1_ep_0001",),
        repo_scope=REPO_API,
        for_cases=("C04_prior_attempts",),
        note="outcome=succeeded",
    ),
    GroundTruthFact(
        fact_key="gt_ep2_touched",
        org_id=ORG_ALPHA,
        subject=EPISODE_FAILED,
        predicate="touched",
        object=REPO_API,
        observed_at=_t("2026-06-25T12:00:00"),
        valid_from=_t("2026-06-25T12:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        source_event_refs=("sevt_ep_0002",),
        evidence_refs=("ev1_ep_0002",),
        repo_scope=REPO_API,
        for_cases=("C04_prior_attempts",),
        note="outcome=failed — the attempt most likely to be dropped",
    ),
    GroundTruthFact(
        fact_key="gt_ep3_touched",
        org_id=ORG_ALPHA,
        subject=EPISODE_ABANDONED,
        predicate="touched",
        object=REPO_API,
        observed_at=_t("2026-07-08T12:00:00"),
        valid_from=_t("2026-07-08T12:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        source_event_refs=("sevt_ep_0003",),
        evidence_refs=("ev1_ep_0003",),
        repo_scope=REPO_API,
        for_cases=("C04_prior_attempts",),
        note="outcome=abandoned",
    ),
    GroundTruthFact(
        fact_key="gt_ep4_sole_support",
        org_id=ORG_ALPHA,
        subject=EPISODE_SOLE_SUPPORT,
        predicate="touched",
        object=REPO_API,
        observed_at=_t("2026-07-06T12:00:00"),
        valid_from=_t("2026-07-06T12:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        source_event_refs=("sevt_ep_0004",),
        # Deliberately no evidence_refs: sevt_ep_0004 is the ONLY thing
        # holding this fact up, so deleting it must remove the fact. Its
        # sibling gt_ep1_touched carries both, and must survive the same
        # deletion sweep with reduced provenance.
        evidence_refs=(),
        repo_scope=REPO_API,
        for_cases=("C08_deleted_redacted_episode",),
    ),
    GroundTruthFact(
        fact_key="gt_ep5_web_repo",
        org_id=ORG_ALPHA,
        subject=EPISODE_WEB_REPO,
        predicate="touched",
        object=REPO_WEB,
        observed_at=_t("2026-07-10T12:00:00"),
        valid_from=_t("2026-07-10T12:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        source_event_refs=("sevt_ep_0005",),
        evidence_refs=("ev1_ep_0005",),
        repo_scope=REPO_WEB,
        for_cases=("C09_revoked_repo_visibility",),
        note="Disappears once repo_atlas_web visibility is revoked.",
    ),
    GroundTruthFact(
        fact_key="gt_ep_keyword_stuffed",
        org_id=ORG_ALPHA,
        subject=EPISODE_KEYWORD_STUFFED,
        predicate="touched",
        object=REPO_API,
        observed_at=_t("2026-07-11T12:00:00"),
        valid_from=_t("2026-07-11T12:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        source_event_refs=("sevt_ep_9001",),
        evidence_refs=("ev1_ep_9001",),
        repo_scope=REPO_API,
        for_cases=("C17_retrieval_manipulation",),
        is_adversarial=True,
        note=(
            "Legitimately in scope, so it may be returned -- but it must not "
            "DISPLACE the three real attempts. Asserted as an ordering/recall "
            "constraint, not as an exclusion."
        ),
    ),
    # -- C05 / C11: recurring incident pattern -----------------------------
    *(
        GroundTruthFact(
            fact_key=f"gt_{incident.id.lower()}_signature",
            org_id=ORG_ALPHA,
            subject=incident,
            predicate="exhibits_failure_signature",
            object=CI_FAILURE_SIGNATURE,
            observed_at=observed,
            valid_from=observed,
            claim_kind=ClaimKind.OBSERVED,
            evidence_refs=(f"ev1_{incident.id.lower()}",),
            repo_scope=REPO_API,
            for_cases=("C05_repeated_failure_pattern", "C11_projector_retry"),
        )
        for incident, observed in (
            (INCIDENT_501, _t("2026-06-06T03:00:00")),
            (INCIDENT_502, _t("2026-06-27T02:00:00")),
            (INCIDENT_503, _t("2026-07-19T04:00:00")),
        )
    ),
    GroundTruthFact(
        fact_key="gt_inc504_decoy",
        org_id=ORG_ALPHA,
        subject=INCIDENT_504_DECOY,
        predicate="exhibits_failure_signature",
        object=EntityRef("ci_failure_signature", "sig_tls_handshake"),
        observed_at=_t("2026-07-22T01:00:00"),
        valid_from=_t("2026-07-22T01:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_inc504",),
        repo_scope=REPO_API,
        for_cases=("C05_repeated_failure_pattern",),
        note=(
            "Same subsystem, same hour-of-day, different cause. Including it "
            "in the pattern is graph proximity mistaken for causation (§7.3)."
        ),
    ),
    # -- C06 / C14 / C18: conflicts, injection, poisoning ------------------
    GroundTruthFact(
        fact_key="gt_conflict_side_a",
        org_id=ORG_ALPHA,
        subject=INCIDENT_503,
        predicate="attributed_to",
        object=EntityRef("component", "cmp_payments_pool"),
        observed_at=_t("2026-07-19T06:00:00"),
        valid_from=_t("2026-07-19T06:00:00"),
        claim_kind=ClaimKind.INFERRED,
        source_event_refs=("sevt_ep_0006",),
        evidence_refs=("ev1_postmortem_503a",),
        repo_scope=REPO_API,
        for_cases=("C06_conflicting_episodes",),
    ),
    GroundTruthFact(
        fact_key="gt_conflict_side_b",
        org_id=ORG_ALPHA,
        subject=INCIDENT_503,
        predicate="attributed_to",
        object=EntityRef("component", "cmp_upstream_gateway"),
        observed_at=_t("2026-07-20T06:00:00"),
        valid_from=_t("2026-07-20T06:00:00"),
        claim_kind=ClaimKind.INFERRED,
        source_event_refs=("sevt_ep_0007",),
        evidence_refs=("ev1_postmortem_503b",),
        repo_scope=REPO_API,
        for_cases=("C06_conflicting_episodes",),
        note="Neither side retracted; both must surface, both flagged.",
    ),
    GroundTruthFact(
        fact_key="gt_injected_instruction",
        org_id=ORG_ALPHA,
        subject=PROJ_ATLAS,
        predicate="approved_for_release_by",
        object=EntityRef("decision", "ADR-999-nonexistent"),
        observed_at=_t("2026-07-21T06:00:00"),
        valid_from=_t("2026-07-21T06:00:00"),
        claim_kind=ClaimKind.INFERRED,
        source_event_refs=("sevt_injected",),
        repo_scope=REPO_API,
        for_cases=("C14_prompt_injection",),
        is_adversarial=True,
        note=(
            "Planted by an issue comment instructing the extractor. Must never "
            "appear as a fact; the comment's own legitimate facts must carry "
            "untrusted_content."
        ),
    ),
    GroundTruthFact(
        fact_key="gt_legit_dependency",
        org_id=ORG_ALPHA,
        subject=PROJ_ATLAS,
        predicate="depends_on",
        object=DEPENDENCY_LIBPAY,
        observed_at=_t("2026-06-03T00:00:00"),
        valid_from=_t("2026-06-03T00:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_dep_libpay",),
        repo_scope=REPO_API,
        for_cases=("C18_entity_linking_poisoning",),
        is_control=True,
        note=(
            "A genuine depends_on beside the poisoned one, so the poisoning "
            "oracle asserts against a NON-EMPTY answer. An exclusion-only "
            "oracle over an empty result set is satisfied by an arm that "
            "returns nothing at all, which is not the behaviour under test."
        ),
    ),
    GroundTruthFact(
        fact_key="gt_poisoned_entity_link",
        org_id=ORG_ALPHA,
        subject=PROJ_ATLAS,
        predicate="depends_on",
        object=EntityRef("repository", "dep_attacker_controlled"),
        observed_at=_t("2026-07-23T06:00:00"),
        valid_from=_t("2026-07-23T06:00:00"),
        claim_kind=ClaimKind.INFERRED,
        source_event_refs=("sevt_poison",),
        repo_scope=REPO_API,
        for_cases=("C18_entity_linking_poisoning",),
        is_adversarial=True,
        note=(
            "Same tenant, real subject entity, false object. The cross-tenant "
            "test cannot catch this: proj_atlas is legitimately in scope."
        ),
    ),
    # -- C01 / C21: dependency valid-time intervals ------------------------
    GroundTruthFact(
        fact_key="gt_svc_repo_v1",
        org_id=ORG_ALPHA,
        subject=SERVICE_PAYMENTS,
        predicate="implemented_by",
        object=REPO_API,
        observed_at=_t("2026-06-01T00:00:00"),
        valid_from=_t("2026-06-01T00:00:00"),
        valid_to=_t("2026-07-10T00:00:00"),
        invalidation_observed_at=_t("2026-07-10T00:00:00"),
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_svc_map_v1",),
        repo_scope=REPO_API,
        for_cases=("C01_historical_truth", "C21_deterministic_only_org"),
        note=(
            "Structured valid-time row. Survives in deterministic-only mode, "
            "which is why C21 pairs with this and not with the ADR case."
        ),
    ),
    GroundTruthFact(
        fact_key="gt_svc_repo_null_start",
        org_id=ORG_ALPHA,
        subject=EntityRef("service", "svc_ledger"),
        predicate="implemented_by",
        object=REPO_API,
        observed_at=_t("2026-06-02T00:00:00"),
        # NULL interval start. This is not a synthetic curiosity:
        # 066_operational_canonical.sql:261 declares
        # operational_service_repository_mappings.valid_from as Nullable,
        # while every as-of filter found applies `valid_from <= {as_of}` --
        # and a NULL comparison is false in ClickHouse, so such a row is
        # silently dropped from EVERY as-of answer. Ground truth says an
        # open-started interval is true at any as_of; the native path is
        # expected to disagree, and O7_null_valid_from measures that.
        valid_from=None,
        valid_to=None,
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_svc_map_null_start",),
        repo_scope=REPO_API,
        for_cases=("C01_historical_truth",),
    ),
    GroundTruthFact(
        fact_key="gt_svc_repo_v2",
        org_id=ORG_ALPHA,
        subject=SERVICE_PAYMENTS,
        predicate="implemented_by",
        object=DEPENDENCY_LIBPAY,
        observed_at=_t("2026-07-10T00:00:00"),
        valid_from=_t("2026-07-10T00:00:00"),
        valid_to=None,
        claim_kind=ClaimKind.OBSERVED,
        evidence_refs=("ev1_svc_map_v2",),
        repo_scope=REPO_API,
        for_cases=("C01_historical_truth", "C21_deterministic_only_org"),
    ),
)

GROUND_TRUTH_BY_KEY = {fact.fact_key: fact for fact in GROUND_TRUTH}


#: Closed facts (``valid_to is not None``) whose OWN evidence/source-event
#: refs genuinely document the closure -- a structured field on the same
#: canonical record (e.g. a dependency-tracker row, or
#: operational_service_repository_mappings' own valid_from/valid_to), not a
#: closure stated only in a DIFFERENT record's prose.
#:
#: Every closed fact must choose exactly one of two ways to declare its
#: closure provenance: pinned here (self-evidencing), or via
#: ``invalidated_by_fact_key`` (names the record that actually closed it).
#: Neither is a default -- ``test_closed_facts_declare_their_closure_provenance``
#: in test_corpus_consistency.py fails on a closed fact that is neither,
#: which is exactly how gt_adr014_superseded's closure ended up silently
#: citing its own opening evidence (CHAOS-3499 finding 6) before this rule
#: existed: nothing forced a conscious choice, so it defaulted to "self",
#: incorrectly.
SELF_EVIDENCING_CLOSURES: frozenset[str] = frozenset(
    {
        # A structured dependency-tracker row: the same record's own
        # resolved status closes the window, not a separate document.
        "gt_blocks_101_110",
        # Structured valid-time row: operational_service_repository_mappings
        # carries valid_from/valid_to directly on the same record (see
        # corpus/questions.py's Q7 classification evidence).
        "gt_svc_repo_v1",
    }
)


# --------------------------------------------------------------------------
# Reference selection -- ORACLE-SIDE ONLY
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class VisibilityContext:
    """Current canonical visibility at *read* time, not projection time.

    Re-authorisation happens against this, which is what makes C09
    (revocation) a real test rather than a restatement of what the projector
    already knew.
    """

    org_id: str
    visible_repos: frozenset[str]
    deleted_source_event_refs: frozenset[str] = field(default_factory=frozenset)
    redacted_source_event_refs: frozenset[str] = field(default_factory=frozenset)


def select(
    *,
    as_of: datetime,
    axis: str,
    visibility: VisibilityContext,
    subjects: tuple[EntityRef, ...] | None = None,
    predicates: frozenset[str] | None = None,
    include_adversarial: bool = False,
    apply_time_filter: bool = True,
    suppress_fact_keys: frozenset[str] = frozenset(),
) -> tuple[GroundTruthFact, ...]:
    """Independently re-derive the expected fact set from ground truth.

    Used by the golden-response builder and by
    ``tests/test_corpus_consistency.py``, which cross-checks the hand-authored
    oracles against it. Arms never see it. Two independent derivations of the
    same expected set is the point: a typo in an oracle stops being the
    definition of correct, because this function disagrees with it.

    ``apply_time_filter`` is False for history-shaped query modes (timeline,
    supersession, prior attempts, conflicts, recurring patterns). Those
    questions *want* closed windows in the answer -- "which decision
    superseded the original" is unanswerable if the superseded one has been
    filtered out for no longer holding.

    ``subjects`` matches a fact whose ``subject`` OR ``object`` is one of the
    given entities -- a query pivots on an entity that can sit on either side
    of a relationship (e.g. "touched" points episode -> repo, but the query
    asks about the repo). A fact matching neither is out of scope for the
    query regardless of predicate.
    """
    selected: list[GroundTruthFact] = []
    for fact in GROUND_TRUTH:
        if fact.fact_key in suppress_fact_keys:
            continue
        if fact.org_id != visibility.org_id:
            continue
        if fact.is_adversarial and not include_adversarial:
            continue
        if fact.repo_scope is not None and fact.repo_scope.id not in (
            visibility.visible_repos
        ):
            continue
        if (
            subjects is not None
            and fact.subject not in subjects
            and fact.object not in subjects
        ):
            continue
        if predicates is not None and fact.predicate not in predicates:
            continue
        remaining = set(fact.source_event_refs) - visibility.deleted_source_event_refs
        if fact.source_event_refs and not remaining and not fact.evidence_refs:
            # Every supporting source deleted and nothing else closes it.
            continue
        if apply_time_filter:
            if axis == "valid_time":
                if not fact.true_at(as_of):
                    continue
            elif axis == "observed_time":
                if not fact.known_at(as_of):
                    continue
            else:  # pragma: no cover - guarded by TimeAxis at call sites
                raise ValueError(f"unknown axis {axis!r}")
        redacted = set(fact.source_event_refs) & visibility.redacted_source_event_refs
        if redacted:
            # Reduced provenance, not exclusion: the fact still stands (it
            # may still carry other evidence/source refs), but the redacted
            # source must not still be presented as backing it. Before this,
            # `redacted_source_event_refs` was read nowhere, so an arm that
            # served a redacted source's content verbatim was unmeasured.
            fact = dataclasses.replace(
                fact,
                source_event_refs=tuple(
                    ref for ref in fact.source_event_refs if ref not in redacted
                ),
            )
        selected.append(fact)
    return tuple(selected)


ALPHA_FULL_VISIBILITY = VisibilityContext(
    org_id=ORG_ALPHA,
    visible_repos=frozenset({REPO_API.id, REPO_WEB.id, DEPENDENCY_LIBPAY.id}),
)

ALPHA_WEB_REVOKED = VisibilityContext(
    org_id=ORG_ALPHA,
    visible_repos=frozenset({REPO_API.id, DEPENDENCY_LIBPAY.id}),
)
