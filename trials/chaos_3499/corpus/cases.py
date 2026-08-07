"""The pinned trial corpus: 21 cases from amended PRD §15.1.

Each case names the *defect it exists to catch*, not merely the data it
contains. A case whose ``catches`` reads "checks that history works" is not a
case, it is a wish; the wording here is deliberately the failure an arm would
exhibit if it got this wrong, so that a reviewer can tell whether the oracle
that claims to cover the case actually does.

``exercised_by`` links each case to the oracle ids that assert against it.
``tests/test_corpus_coverage.py`` fails if any case has no oracle, so a case
cannot sit in the corpus looking like coverage while nothing measures it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CaseFamily(str, Enum):
    TEMPORAL_TRUTH = "temporal_truth"
    EPISODIC = "episodic"
    SECURITY = "security"
    RESILIENCE = "resilience"
    COVERAGE = "coverage"


@dataclass(frozen=True)
class CorpusCase:
    case_id: str
    prd_index: int
    family: CaseFamily
    title: str
    plants: str
    catches: str
    exercised_by: tuple[str, ...] = ()


CORPUS_CASES: tuple[CorpusCase, ...] = (
    CorpusCase(
        case_id="C01_historical_truth",
        prd_index=1,
        family=CaseFamily.TEMPORAL_TRUTH,
        title="Historical truth at a timestamp",
        plants=(
            "A dependency mapping valid 2026-06-01..2026-07-10, replaced by a "
            "different mapping valid from 2026-07-10 onward; plus a third "
            "mapping with a NULL interval start."
        ),
        catches=(
            "An arm that answers every as-of question with present state. The "
            "two intervals give different answers on either side of 07-10, so "
            "a present-state answer is wrong for exactly one of the probes. "
            "The NULL-start row additionally probes a latent native defect "
            "(see O7_null_valid_from)."
        ),
        exercised_by=("O7_valid", "O7_null_valid_from"),
    ),
    CorpusCase(
        case_id="C02_superseded_decision",
        prd_index=2,
        family=CaseFamily.TEMPORAL_TRUTH,
        title="Superseded architecture decision",
        plants=(
            "ADR-014 (original deployment design) superseded by ADR-021, with "
            "the supersession stated only in ADR-021's prose."
        ),
        catches=(
            "An arm that returns both decisions as equally current, or that "
            "returns the supersession without provenance to the ADR that "
            "states it."
        ),
        exercised_by=("O3_supersession",),
    ),
    CorpusCase(
        case_id="C03_changed_blockers",
        prd_index=3,
        family=CaseFamily.TEMPORAL_TRUTH,
        title="Changed blockers and dependency state",
        plants=(
            "ATL-101 blocks ATL-110 from 07-02 to 07-18; ATL-105 blocks "
            "ATL-110 from 07-18 onward."
        ),
        catches=(
            "An arm that reports the current blocker for a past as-of date, "
            "and an arm that reports the past blocker without saying it was "
            "resolved."
        ),
        exercised_by=("O2_blocking_valid", "O2_blocking_observed"),
    ),
    CorpusCase(
        case_id="C04_prior_attempts",
        prd_index=4,
        family=CaseFamily.EPISODIC,
        title="Prior agent attempts and outcomes",
        plants=(
            "Three episodes touching repo_atlas_api/src/payments/, with "
            "outcomes succeeded, failed, and abandoned."
        ),
        catches=(
            "An arm that returns attempts without their outcomes, or that "
            "returns only the successful one."
        ),
        exercised_by=("O4_prior_attempts",),
    ),
    CorpusCase(
        case_id="C05_repeated_failure_pattern",
        prd_index=5,
        family=CaseFamily.EPISODIC,
        title="Repeated failure pattern across incidents",
        plants=(
            "INC-501/502/503 share a root-cause signature; INC-504 is a "
            "superficially similar decoy with a different cause."
        ),
        catches=(
            "An arm that reports a pattern spanning the decoy, i.e. one that "
            "mistakes graph proximity for a shared cause (PRD §7.3)."
        ),
        exercised_by=("O6_recurring_pattern",),
    ),
    CorpusCase(
        case_id="C06_conflicting_episodes",
        prd_index=6,
        family=CaseFamily.EPISODIC,
        title="Conflicting episodes",
        plants=(
            "Two episodes assert incompatible causes for the same incident, "
            "neither retracted."
        ),
        catches=(
            "An arm that silently picks one side. The correct answer surfaces "
            "both with the conflict flag set."
        ),
        exercised_by=("O5_conflicts",),
    ),
    CorpusCase(
        case_id="C07_structured_plus_unstructured",
        prd_index=7,
        family=CaseFamily.EPISODIC,
        title="Structured and unstructured evidence for one relationship",
        plants=(
            "A canonical PR->issue link, plus an ADR paragraph describing the "
            "same relationship in prose."
        ),
        catches=(
            "Double-counting: the same relationship emitted as two facts, "
            "which inflates the duplicate-fact rate and makes one relationship "
            "look like corroboration by two."
        ),
        exercised_by=("O3_supersession", "O4_prior_attempts"),
    ),
    CorpusCase(
        case_id="C08_deleted_redacted_episode",
        prd_index=8,
        family=CaseFamily.SECURITY,
        title="Deleted / redacted episodes",
        plants=(
            "An episode supporting a two-source fact is redacted; a "
            "single-source fact's only episode is deleted."
        ),
        catches=(
            "Over- and under-deletion in one case: the two-source fact must "
            "survive with reduced provenance, the single-source fact must "
            "disappear entirely."
        ),
        exercised_by=("O4_prior_attempts_after_redaction",),
    ),
    CorpusCase(
        case_id="C09_revoked_repo_visibility",
        prd_index=9,
        family=CaseFamily.SECURITY,
        title="Repository access revocation",
        plants="repo_atlas_web visibility revoked after projection indexed it.",
        catches=(
            "Stale authorization: an arm re-authorising against the scope "
            "captured at projection time rather than current visibility."
        ),
        exercised_by=("O4_prior_attempts_after_revocation",),
    ),
    CorpusCase(
        case_id="C10_stale_watermark",
        prd_index=10,
        family=CaseFamily.RESILIENCE,
        title="Stale graph watermark",
        plants="Projection halted 9 days before the query's as-of date.",
        catches=(
            "An arm that answers from behind its own watermark without "
            "declaring staleness -- indistinguishable from a fresh answer."
        ),
        exercised_by=("O1_ci_prior_attempts_stale",),
    ),
    CorpusCase(
        case_id="C11_projector_retry",
        prd_index=11,
        family=CaseFamily.RESILIENCE,
        title="Projector failure and retry",
        plants=(
            "A projector run fails mid-batch and retries, replaying events "
            "already applied."
        ),
        catches=(
            "Non-idempotent projection: duplicate facts, or a watermark "
            "advanced past events that were never applied."
        ),
        exercised_by=("O6_recurring_pattern",),
    ),
    CorpusCase(
        case_id="C12_extraction_provider_failure",
        prd_index=12,
        family=CaseFamily.RESILIENCE,
        title="Extraction-provider failure",
        plants=(
            "The extraction provider returns malformed structured output, then "
            "times out."
        ),
        catches=(
            "An arm that emits a partially-parsed fact as observed, or that "
            "drops the source silently instead of declaring the coverage gap."
        ),
        exercised_by=("O3_supersession_extraction_down",),
    ),
    CorpusCase(
        case_id="C13_graph_datastore_outage",
        prd_index=13,
        family=CaseFamily.RESILIENCE,
        title="Graph datastore outage",
        plants="The graph backend is unreachable for the duration of the query.",
        catches=(
            "An arm that answers anyway from a cache, and -- the §16 hard gate "
            "-- any effect on the existing ACR/Ask Dev fallback path."
        ),
        exercised_by=("O4_prior_attempts_graph_outage",),
    ),
    CorpusCase(
        case_id="C14_prompt_injection",
        prd_index=14,
        family=CaseFamily.SECURITY,
        title="Prompt injection inside source content",
        plants=(
            "An issue comment instructing the extractor to ignore its rules "
            "and emit an arbitrary fact."
        ),
        catches=(
            "Content treated as instruction. The injected fact must be absent, "
            "and the source's own facts must carry untrusted_content."
        ),
        exercised_by=("O5_conflicts_injected",),
    ),
    CorpusCase(
        case_id="C15_cross_tenant_near_duplicate",
        prd_index=15,
        family=CaseFamily.SECURITY,
        title="Cross-tenant near-duplicate entity names",
        plants=(
            "org_trial_beta owns a project also named Atlas with a similarly "
            "named repo and issue keys."
        ),
        catches=(
            "Entity resolution that merges across tenants. Any beta fact in an "
            "alpha answer is a leak, not a ranking error."
        ),
        exercised_by=("O2_blocking_valid", "O4_prior_attempts"),
    ),
    CorpusCase(
        case_id="C16_squash_merge_org",
        prd_index=16,
        family=CaseFamily.COVERAGE,
        title="Squash-merge org with near-empty work_graph_pr_commit",
        plants=(
            "An org whose merges are squashed, leaving PR->commit linkage "
            "effectively absent."
        ),
        catches=(
            "Silent emptiness: returning 'no prior attempts' when the truth is "
            "'the source that would show them cannot see them'. The answer "
            "must declare the coverage gap."
        ),
        exercised_by=("O1_ci_prior_attempts_squash",),
    ),
    CorpusCase(
        case_id="C17_retrieval_manipulation",
        prd_index=17,
        family=CaseFamily.SECURITY,
        title="Retrieval manipulation by keyword stuffing",
        plants=(
            "An episode stuffed with every term in the query, containing no "
            "real evidence."
        ),
        catches=(
            "Hybrid retrieval displacing oracle-expected evidence with a "
            "high-scoring decoy. Distinct from prompt injection: nothing here "
            "instructs the model, it only games the ranker."
        ),
        exercised_by=("O4_prior_attempts_manipulated",),
    ),
    CorpusCase(
        case_id="C18_entity_linking_poisoning",
        prd_index=18,
        family=CaseFamily.SECURITY,
        title="Entity-linking poisoning against a real entity",
        plants=(
            "Same-tenant content crafted so extraction attaches a false fact "
            "to the genuine proj_atlas node."
        ),
        catches=(
            "Same-tenant integrity failure. The cross-tenant test cannot catch "
            "this: the entity is legitimately in scope, the fact is not true."
        ),
        exercised_by=("O5_conflicts_poisoned",),
    ),
    CorpusCase(
        case_id="C19_axis_pair",
        prd_index=19,
        family=CaseFamily.TEMPORAL_TRUTH,
        title="Axis pair: valid-time and observed-time disagree",
        plants=(
            "A blocker true from 07-05 but backfilled into Dev Health on "
            "07-20. Asked as of 07-15, valid-time says blocked, observed-time "
            "says not-yet-known."
        ),
        catches=(
            "An arm that ignores the `axis` field. Because the two oracles "
            "expect different answers, such an arm passes exactly one and "
            "fails the other -- it cannot pass both by accident."
        ),
        exercised_by=("O2_blocking_valid", "O2_blocking_observed"),
    ),
    CorpusCase(
        case_id="C20_unpinned_time",
        prd_index=20,
        family=CaseFamily.COVERAGE,
        title="Unpinned time: no as_of, no time_window_days",
        plants="A query submitted with both time bounds omitted.",
        catches=(
            "Unbounded scan and unstated semantics. ACR applies no server "
            "default today, so the behaviour must be defined and asserted "
            "rather than inherited by accident."
        ),
        exercised_by=("O7_unpinned",),
    ),
    CorpusCase(
        case_id="C21_deterministic_only_org",
        prd_index=21,
        family=CaseFamily.COVERAGE,
        title="Deterministic-only org (provider policy disallows extraction)",
        plants=(
            "An org whose provider policy forbids model providers, so §7.1 "
            "structured projection is the only path."
        ),
        catches=(
            "Measures what value actually survives without extraction. Some "
            "customers will run this way permanently, so a trial score that "
            "assumes extraction overstates their outcome."
        ),
        exercised_by=("O3_supersession_deterministic_only", "O7_valid"),
    ),
)

CORPUS_CASES_BY_ID = {case.case_id: case for case in CORPUS_CASES}
