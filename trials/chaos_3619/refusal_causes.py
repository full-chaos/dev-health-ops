"""Why the graph arm refused, decomposed by cause it actually recorded.

The sweep records ONE refusal string for every unresolved case -- "no
authorized subject resolved from the question" -- because that is the single
branch the leg takes when ``seeds_from`` comes back empty. That string is
true and it is not enough: it cannot distinguish an arm that was *not allowed*
to see a match from one that found nothing to match, and those are opposite
facts about the same count. A reader deciding whether refusals are a
capability gap needs them separated.

**This module does not re-interpret the records; it re-derives the cause from
the same pure functions the sweep called, and then checks itself against the
records.** The arm has two entry modes, and each case's recorded
``comparison_shape`` says which one the sweep took -- this is the SAME
signal ``trials.chaos_3619.sweep`` branches on (see ``_run_graph``), so
recomputation follows the identical fork rather than guessing from a
disposition:

* the SEEDED mode (:func:`~.graph_leg.discover_subjects`) for every shape
  except ``discovered_cohort``: pure over (question, projection, grant), all
  three of which are frozen, so recomputing it reproduces exactly what the
  sweep saw.
* the SUBJECTLESS COHORT mode (:func:`~.graph_leg.discover_cohort_for`,
  CHAOS-3645) for ``discovered_cohort``: pure over (question family,
  projection, grant), and deliberately blind to the question text -- a
  cohort question never carries a subject phrase to extract by design, so
  recomputing THIS shape through the seeded path would always find empty
  seeds regardless of what the live mechanism does. That mismatch, unledgered,
  is exactly the CHAOS-3656 defect: 13 current-tip cohort cases score without
  a single extracted mention, and a decomposition that only knows the seeded
  mode reads that as drift.

Both modes feed the same self-check. :func:`decompose` asserts that: every
case whose recomputed seed set is empty must be a case the records show as
refused, and vice versa. A mismatch means the recomputation has drifted from
the run and the numbers must not be published -- so it raises rather than
reporting. New mechanisms extend the fork above and the mechanism-specific
claim check in the ledger-admission branch below; they never make the
seeded path recompute for a shape it no longer owns.

The categories are the ones a reader has to tell apart:

``authorization`` -- the arm matched entities and the grant withheld them.
    This is the arm's authorization working, and on this corpus it is a
    planted case. It is CORRECT BEHAVIOUR, not a gap, and counting it with
    the misses would understate the arm. Shared by both entry modes.
``no_mention_extracted`` -- production ``extract_mentions`` (plus the untyped
    backstop) returned no subject phrase at all, so discovery had nothing to
    search for. The graph arm shares this extractor with the native
    interpreter, which is what makes this category a COMMON-MODE limit rather
    than an arm difference. SEEDED mode only: a cohort question extracts no
    mention by design, so this category would misdescribe why it refused.
``no_authorized_match`` -- phrases were extracted and none matched an entity
    the principal may see. SEEDED mode only, for the same reason.
``no_cohort_family_support`` -- the question family has no subjectless entry
    at all (:data:`~dev_health_ops.context_fabric.graph_arm.cohort_discovery.FAMILY_CANDIDATE_KINDS`
    does not name it), so ``discover_cohort_for`` raised
    ``UnsupportedCohortFamilyError`` before any traversal was attempted. A
    named capability boundary, not an extraction gap.
``no_cohort_members_enumerated`` -- the family IS supported, the universe was
    read, and nothing survived authorization and the shared-basis rule. The
    cohort mode's analogue of ``no_authorized_match``.

Deliberately NOT a category: "shape unsupported" as a synonym for
``no_mention_extracted``. Every ``discovered_cohort`` refusal used to read
that way before CHAOS-3645 existed, because the only mode available was the
seeded one; the four categories above are what replaced that conflation once
the arm had a second entry mode to actually be unsupported *in*.

The divergence ledger
---------------------

The self-check above compares a **live recomputation** against a **pinned
record**, which means any legitimate improvement to the shared code the sweep
called will make the two disagree. That is the guard working, not failing --
but "the guard fired, so we turned it down" is how a differential oracle stops
being worth having.

So a divergence is admitted only by *naming itself*. :data:`DIVERGENCE_LEDGER`
is an append-only list of dated entries; each names the ticket and pull request
that moved the code, the exact case ids affected, and the exact direction of
the move. :func:`decompose` accepts precisely those cases moving precisely that
way and **raises on everything else, in either direction**. A case that
regresses, a case nobody ledgered, or a ledgered case that stops diverging all
still stop the run.

Three properties are load-bearing, and each has a test:

* **Additive only.** An entry is never edited or removed once landed. The
  historical fact ("the pinned sweep recorded these as absent-extraction
  refusals") and the current fact ("the current extractor resolves them") are
  both true, and the ledger is what holds them together with a citation. A
  reader who deletes an entry to make a number tidy destroys the link.
* **Directional.** Each entry states ``from_category`` -> ``to_category``. A
  recall *gain* (the pin refused, the live code resolves) is what CHAOS-3648
  produced; a recall *loss* in the same case is a regression and must still
  raise, so acceptance checks the direction rather than the case id alone.
* **Claim-checked, not asserted.** An entry claiming the extractor now finds a
  phrase is honoured only if the live recomputation actually produces one. An
  entry whose stated cause stopped being true is drift like any other. A
  cohort-mode entry's claim -- that ``discover_cohort_for`` now resolves a
  non-empty cohort -- is checked the same way, against that mechanism's own
  recomputation, never against the seeded path's.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from dev_health_ops.api.dev.investigation_corpus.cases import CorpusCase
    from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection

__all__ = [
    "AUTHORIZATION",
    "COHORT_RESOLVED_POST_3645",
    "DIVERGENCE_LEDGER",
    "NO_COHORT_FAMILY_SUPPORT",
    "NO_COHORT_MEMBERS",
    "NO_MATCH",
    "NO_MENTION",
    "PHRASE_EXTRACTED_POST_3648",
    "LedgeredDivergence",
    "RefusalCause",
    "counts",
    "decompose",
]

AUTHORIZATION = "authorization"
NO_MENTION = "no_mention_extracted"
NO_MATCH = "no_authorized_match"

#: SEEDED-mode categories above; SUBJECTLESS-COHORT-mode categories below.
#: ``authorization`` is shared by both modes -- see :func:`decompose`.

#: The family has no subjectless entry at all
#: (:data:`~dev_health_ops.context_fabric.graph_arm.cohort_discovery.FAMILY_CANDIDATE_KINDS`
#: does not name it), so ``discover_cohort_for`` raised
#: ``UnsupportedCohortFamilyError`` before any traversal was attempted. A
#: named capability boundary the arm decided, not an extraction gap -- the
#: cohort mode's analogue of a refusal the arm KNOWS it has.
NO_COHORT_FAMILY_SUPPORT = "no_cohort_family_support"

#: The family IS supported, the universe was read, and nothing survived
#: authorization and the shared-basis rule. The cohort mode's analogue of
#: ``no_authorized_match``.
NO_COHORT_MEMBERS = "no_cohort_members_enumerated"

#: The cause a CHAOS-3648 case carries *now*: the pinned sweep recorded it as
#: an absent-extraction refusal, and the current extractor resolves a subject
#: for it. Held as its own category rather than folded into a recomputed
#: ``no_authorized_match`` so a reader can never mistake the ledgered rows for
#: rows the pinned sweep measured that way.
PHRASE_EXTRACTED_POST_3648 = "phrase_extracted_post_3648"

#: The cause a CHAOS-3645 cohort case carries *now*, in a records file frozen
#: before the subjectless cohort mode existed: the pinned sweep could only
#: take the seeded path, extracted no mention (cohort questions never carry
#: one, by design) and refused; the current sweep takes the cohort path and
#: resolves a non-empty cohort for the same question. Held as its own
#: category, never folded into ``no_mention_extracted``, for the same reason
#: :data:`PHRASE_EXTRACTED_POST_3648` is not folded into it either: a reader
#: comparing categories across records files must be able to tell "the pin
#: measured this as an extraction gap" apart from "this decomposition is
#: crediting a mechanism the pin never ran."
COHORT_RESOLVED_POST_3645 = "cohort_resolved_post_3645"


@dataclass(frozen=True, slots=True)
class LedgeredDivergence:
    """One dated, cited reason the live code no longer reproduces the pin."""

    ticket: str
    pull_request: str
    #: ISO date the divergence landed. A ledger without dates cannot be read
    #: as a history, only as a list of excuses.
    landed_on: str
    from_category: str
    to_category: str
    case_ids: frozenset[str]
    rationale: str


#: APPEND ONLY. Never edit or delete a landed entry -- see the module docstring.
#: Ordered by landing time: CHAOS-3645 (13:16:52) merged before CHAOS-3648
#: (13:33:32) on the same date.
DIVERGENCE_LEDGER: tuple[LedgeredDivergence, ...] = (
    LedgeredDivergence(
        ticket="CHAOS-3645",
        pull_request="#1623",
        landed_on="2026-08-09",
        from_category=NO_MENTION,
        to_category=COHORT_RESOLVED_POST_3645,
        case_ids=frozenset(
            {
                "T01_clearly_struggling_team",
                "T02_high_wip_without_struggle",
                "T03_operational_displaces_feature",
                "T04_review_pressure_across_projects",
                "T05_stale_source_data",
                "T06_healthy_despite_noisy_metric",
                "P01_demand_exceeds_capacity",
                "P02_critical_path_few_contributors",
                "P03_lightly_loaded_project",
                "S03_shared_dependency_portfolio_risk",
                "S06_declared_complete_without_delivery_evidence",
                "A03_false_relationship_on_real_entity",
                "A08_stale_and_truncated_state",
            }
        ),
        rationale=(
            "CHAOS-3645 (#1623) gave the graph arm a second entry mode --"
            "subjectless cohort discovery, driven by the analytical job's "
            "question family and the grant alone, with no question text "
            "read at all. Before it landed, every discovered_cohort case "
            "could only take the seeded path, which extracts no mention from "
            "a cohort question by design (there is no subject phrase in "
            "'which teams are struggling') and therefore always refused. "
            "trial-results.records.json (the CHAOS-3619 frozen pin) and "
            "post-3648-remeasure.records.json were both measured before "
            "CHAOS-3645 landed and record these 13 cases as arm_refused; "
            "cohort-families-trial-results.records.json and every "
            "consolidated-post-wave* artifact were measured after, and "
            "record them as scored. Neither number is wrong -- each is what "
            "its tree could do -- and this entry is the citation that keeps "
            "the pinned records decomposable without crediting them with a "
            "mechanism they never ran. T07_going_sideways_open_question is "
            "the corpus's 14th discovered_cohort case and is NOT ledgered "
            "here: its family (clarification_and_no_match) has no "
            "subjectless entry in FAMILY_CANDIDATE_KINDS by design, so it "
            "refuses under both mechanisms and carries "
            "no_cohort_family_support rather than a ledgered divergence."
        ),
    ),
    LedgeredDivergence(
        ticket="CHAOS-3648",
        pull_request="#1622",
        landed_on="2026-08-09",
        from_category=NO_MENTION,
        to_category=PHRASE_EXTRACTED_POST_3648,
        case_ids=frozenset(
            {
                "S07_renamed_and_superseded_project",
                "S08_split_evidence_symptom",
                "H03_the_auth_work",
                "H07_unresolved_needs_candidates",
            }
        ),
        rationale=(
            "CHAOS-3648 taught the shared production extractor to read a "
            "definite description of a body of work ('the payroll migration') "
            "as a naming act, so these four colloquial singular-subject "
            "questions now yield a subject phrase where the pinned sweep "
            "recorded none. The records are NOT regenerated: the pinned run "
            "remains the measurement CHAOS-3619 published, and this entry is "
            "the citation that separates it from what the current code does."
        ),
    ),
)


def _validate_ledger() -> None:
    """Structural checks at import, so a malformed entry cannot ship quietly."""

    seen: set[str] = set()
    for entry in DIVERGENCE_LEDGER:
        missing = [
            field
            for field in ("ticket", "pull_request", "landed_on", "rationale")
            if not getattr(entry, field).strip()
        ]
        if missing:
            raise ValueError(
                f"divergence ledger entry for {sorted(entry.case_ids)} is "
                f"missing {missing}; an uncited divergence is an excuse, not a "
                "record"
            )
        if not entry.case_ids:
            raise ValueError(
                f"{entry.ticket} ledgers no case ids, so it would license "
                "nothing while reading as an allowance"
            )
        overlap = seen & entry.case_ids
        if overlap:
            raise ValueError(
                f"{entry.ticket} re-ledgers {sorted(overlap)}; a case may "
                "diverge once, or the later entry is silently rewriting the "
                "earlier one's history"
            )
        seen |= entry.case_ids


_validate_ledger()


def _ledger_entry_for(case_id: str) -> LedgeredDivergence | None:
    for entry in DIVERGENCE_LEDGER:
        if case_id in entry.case_ids:
            return entry
    return None


@dataclass(frozen=True, slots=True)
class RefusalCause:
    """One refused case, and which of the three causes produced it."""

    case_id: str
    comparison_shape: str
    corpus_family: str
    category: str
    mentions_extracted: int
    authorization_filtered_count: int


@dataclass(frozen=True, slots=True)
class _Recomputation:
    """What recomputing ONE case's discovery, through its own mechanism, found.

    ``mentions`` is always empty for a cohort-mode case: the mechanism never
    extracts one, and leaving the field populated from the seeded path (as a
    stray default) would let ``no_mention_extracted`` categorize a cohort
    refusal by an artifact of a check that never ran for it.
    """

    empty: bool
    mentions: tuple[str, ...]
    authorization_filtered_count: int
    has_candidates: bool
    unsupported_family: bool = False


def _recompute_seeded(
    case: CorpusCase, projection: GraphProjection, grant: frozenset[str]
) -> _Recomputation:
    """The mode every shape but ``discovered_cohort`` takes."""

    from . import graph_leg

    discovery = graph_leg.discover_subjects(
        question=case.question,
        projection=projection,
        authorized_entity_ids=grant,
    )
    mentions = graph_leg.mention_texts(case.question)
    return _Recomputation(
        empty=not graph_leg.seeds_from(discovery),
        mentions=mentions,
        authorization_filtered_count=discovery.authorization_filtered_count,
        has_candidates=bool(discovery.candidates),
    )


def _recompute_cohort(
    case: CorpusCase, projection: GraphProjection, grant: frozenset[str]
) -> _Recomputation:
    """The mode ``discovered_cohort`` takes (CHAOS-3645), mirroring the exact
    branch ``trials.chaos_3619.sweep._run_graph`` selects on comparison shape.

    No mention is ever extracted here -- the mechanism reads the question
    family and the grant, never the question text -- so this recomputation
    can never contribute a ``no_mention_extracted`` or ``no_authorized_match``
    category; those describe a check this mode does not run.
    """

    from dev_health_ops.api.dev.investigation_corpus import world
    from dev_health_ops.context_fabric.graph_arm.cohort_discovery import (
        UnsupportedCohortFamilyError,
    )

    from . import graph_leg

    try:
        cohort = graph_leg.discover_cohort_for(
            question_family=case.question_family,
            projection=projection,
            authorized_entity_ids=grant,
            as_of=world.WINDOW_END,
        )
    except UnsupportedCohortFamilyError:
        # A named capability boundary: this family has no subjectless entry
        # at all, so no traversal was attempted. Reported as empty (refused)
        # but flagged so the category below is never confused with an
        # extraction gap.
        return _Recomputation(
            empty=True,
            mentions=(),
            authorization_filtered_count=0,
            has_candidates=False,
            unsupported_family=True,
        )
    return _Recomputation(
        empty=not graph_leg.cohort_seeds_from(cohort),
        mentions=(),
        authorization_filtered_count=cohort.authorization_filtered_count,
        has_candidates=bool(cohort.proposal.members),
    )


def decompose(records_path: Path, leg: str) -> tuple[RefusalCause, ...]:
    """Every refusal in ``leg``, with its cause, verified against the records.

    Raises when the recomputation and the records disagree: a decomposition
    that has drifted from the run it describes is worse than none, because it
    reads as evidence about a sweep it no longer matches.

    Which recomputation a case gets is decided by its OWN comparison shape --
    the same signal ``trials.chaos_3619.sweep`` forks on -- never by which
    records file is being checked. A records file frozen before a mechanism
    existed is expected to disagree with what that mechanism finds today;
    that disagreement is not drift, it is exactly what a divergence-ledger
    entry exists to admit.

    The single exception is a divergence named in :data:`DIVERGENCE_LEDGER`,
    moving in the direction that entry states, whose stated cause the live
    recomputation still bears out. Such a case keeps its place in the refusal
    set the records measured -- the pinned sweep really did refuse it -- and
    carries the ledgered category instead of a recomputed one.
    """

    from dev_health_ops.api.dev.investigation_contract import ComparisonShape
    from dev_health_ops.api.dev.investigation_corpus import world
    from dev_health_ops.api.dev.investigation_corpus.cases import authored_cases
    from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
    from dev_health_ops.context_fabric.graph_arm.projection import build_projection

    from .sweep import GRAPH_ARM_ID

    payload = json.loads(records_path.read_text())
    recorded: dict[str, tuple[str, str, str]] = {}
    for case in payload["cases"]:
        if case["leg"] != leg:
            continue
        for arm in case["arms"]:
            if arm["arm_id"] == GRAPH_ARM_ID:
                recorded[case["case_id"]] = (
                    arm["disposition"],
                    case["comparison_shape"],
                    case["corpus_family"],
                )

    projection = build_projection(adapter.corpus_batch(world.ORG_HELIO))
    grant = frozenset(adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST))

    causes: list[RefusalCause] = []
    drift: list[str] = []
    for case in authored_cases():
        disposition, shape, family = recorded[case.case_id]
        is_cohort = case.comparison_shape is ComparisonShape.DISCOVERED_COHORT
        recomputed = (
            _recompute_cohort(case, projection, grant)
            if is_cohort
            else _recompute_seeded(case, projection, grant)
        )
        empty = recomputed.empty
        mentions = recomputed.mentions
        refused = disposition == "arm_refused"
        if empty != refused:
            entry = _ledger_entry_for(case.case_id)
            # Admitted only in the ledgered direction (the pin refused, the
            # live code now resolves) AND only while the entry's stated cause
            # still holds, checked against the SAME mechanism that produced
            # ``empty`` above. The mirror image -- a case the pin scored that
            # the live code now refuses -- is a recall regression and must
            # stop the run even for a ledgered case, which is why this tests
            # the direction rather than membership alone.
            claim_holds = entry is not None and (
                refused
                and not empty
                and entry.from_category == NO_MENTION
                and (
                    (entry.to_category == PHRASE_EXTRACTED_POST_3648 and mentions)
                    or (entry.to_category == COHORT_RESOLVED_POST_3645 and is_cohort)
                )
            )
            if claim_holds and entry is not None:
                causes.append(
                    RefusalCause(
                        case_id=case.case_id,
                        comparison_shape=shape,
                        corpus_family=family,
                        category=entry.to_category,
                        mentions_extracted=len(mentions),
                        authorization_filtered_count=(
                            recomputed.authorization_filtered_count
                        ),
                    )
                )
                continue
            drift.append(
                f"{case.case_id}: recomputed empty_seeds={empty} but the "
                f"records say disposition={disposition!r}"
                + (
                    ""
                    if entry is None
                    else f" (ledgered by {entry.ticket} {entry.pull_request} "
                    f"as {entry.from_category} -> {entry.to_category}, which "
                    "this divergence does not match)"
                )
            )
            continue
        if not refused:
            continue
        if recomputed.unsupported_family:
            category = NO_COHORT_FAMILY_SUPPORT
        elif (
            recomputed.authorization_filtered_count > 0
            and not recomputed.has_candidates
        ):
            category = AUTHORIZATION
        elif is_cohort:
            category = NO_COHORT_MEMBERS
        elif not mentions:
            category = NO_MENTION
        else:
            category = NO_MATCH
        causes.append(
            RefusalCause(
                case_id=case.case_id,
                comparison_shape=shape,
                corpus_family=family,
                category=category,
                mentions_extracted=len(mentions),
                authorization_filtered_count=recomputed.authorization_filtered_count,
            )
        )
    if drift:
        raise RuntimeError(
            "the refusal decomposition no longer reproduces the recorded "
            "sweep, so its categories describe a run that did not happen:\n  "
            + "\n  ".join(drift)
        )
    return tuple(causes)


def counts(causes: tuple[RefusalCause, ...]) -> dict[str, int]:
    return dict(Counter(cause.category for cause in causes))
