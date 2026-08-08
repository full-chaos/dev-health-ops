"""Episode-readback adapter: a plain agent_episodes list, zero graph infra.

A **baseline component** (harness-design.md §3.1), and the single most
important measurement decision in the trial: ``EpisodeArtifacts.FilesTouched``
(``acr/internal/contracts/v1/types.go:336-340``) is already structured, so if
plain readback answers Q4, the graph's margin there is **zero**. Scoring
readback as a candidate peer would let Graphiti be compared against native
alone and take credit for value ordinary readback already delivers.

**Read path status today: none.** ``acr/internal/storage/interfaces.go:85-91``
confirms ``EpisodeStore`` has no list/query method; the openapi contract
defines no GET (``contracts/openapi/acr-v1.yaml:132-170``). CHAOS-3564 owns
building it. This adapter answers as if that thin read wrapper already
existed over data ACR already writes and stores today (``AgentEpisodeCreate``,
``types.go:304-321``) -- a forward simulation of a real, scoped, already-
approved increment, not an invented capability. Step 5 of the bring-up plan
("Arms N and E measured") is where this gets re-run against the real,
landed read path; step 1 proves the adapter shape and the value hypothesis
ahead of that landing.

**What it can genuinely answer: Q4** ("show prior agent attempts touching
this subsystem"; ``PRIOR_ATTEMPTS`` mode, subject is the repo). Lists
episodes by repo/subsystem overlap, which ``FilesTouched`` answers directly
with no extraction. **Q1** ("what did we try last time this CI failure
occurred") is the same query mode but a different subject shape -- the
signature, not the repo -- and plain episode data has no edge from a CI
failure signature to anything at all, so it is declared unanswerable
explicitly rather than silently returning empty via subject-filter fallout.

**Deliberately unsophisticated, on purpose.** No relevance ranking -- just
recency order -- so a keyword-stuffed decoy that happens to be more recently
observed than a real attempt DOES displace it under a tight budget (C17's
premise, corpus/cases.py). No redaction, deletion, or revocation awareness:
those need real state a static snapshot cannot carry (see
:mod:`harness.arms`'s module docstring).
"""

from __future__ import annotations

from ...corpus import ground_truth as gt
from ..contracts import ArmOutcome, ArmResponse, TemporalFact
from ..oracle import Oracle

ARM_NAME = "episode_readback"
_PROJECTION_VERSION = "episode_readback.v1"

#: agent_episodes-shaped rows this adapter can see. Same fact_keys, same
#: evidence_refs as ground_truth -- the same planted world, read back
#: through episode storage rather than a temporal graph.
_EPISODE_FACT_KEYS = (
    "gt_ep1_touched",
    "gt_ep2_touched",
    "gt_ep3_touched",
    "gt_ep4_sole_support",
    "gt_ep5_web_repo",
    "gt_ep_keyword_stuffed",
)


def _episode_rows() -> list[gt.GroundTruthFact]:
    return [gt.GROUND_TRUTH_BY_KEY[key] for key in _EPISODE_FACT_KEYS]


def _currently_visible(row: gt.GroundTruthFact) -> bool:
    """The one static snapshot this adapter can see: current full
    visibility, nothing revoked. See :mod:`harness.arms`'s module docstring
    for why a per-scenario revocation state cannot be represented here.
    """
    if row.repo_scope is None:
        return True
    return row.repo_scope.id in gt.ALPHA_FULL_VISIBILITY.visible_repos


def _to_fact(row: gt.GroundTruthFact) -> TemporalFact:
    return TemporalFact(
        fact_id=f"tf_episode_readback_{row.fact_key}",
        subject_ref=row.subject,
        predicate=row.predicate,
        object_ref=row.object,
        observed_at=row.observed_at,
        claim_kind=row.claim_kind,
        projection_version=_PROJECTION_VERSION,
        valid_from=row.valid_from,
        valid_to=row.valid_to,
        reference_time=row.valid_from,
        evidence_refs=row.evidence_refs,
        source_event_refs=row.source_event_refs,
    )


def _degraded(
    oracle: Oracle,
    reason: str = "episode_readback_answers_only_prior_attempts_questions",
) -> ArmResponse:
    return ArmResponse(
        arm=ARM_NAME,
        outcome=ArmOutcome.ANSWERED,
        query=oracle.query,
        facts=(),
        degraded_reasons=(reason,),
        indexed_through=gt.TRIAL_NOW,
        versions={"episode_readback": _PROJECTION_VERSION},
    )


def _answer_prior_attempts(oracle: Oracle) -> ArmResponse:
    query = oracle.query
    subjects = frozenset(query.subjects) if query.subjects else None
    predicates = (
        frozenset(query.allowed_relation_types)
        if query.allowed_relation_types
        else None
    )
    rows = [
        row
        for row in _episode_rows()
        if row.org_id == gt.ORG_ALPHA
        and _currently_visible(row)
        and (subjects is None or row.subject in subjects or row.object in subjects)
        and (predicates is None or row.predicate in predicates)
    ]
    # No ranking sophistication -- most-recently-observed first, a plain
    # "recent activity" list. Stable, so ties keep their GROUND_TRUTH
    # declaration order.
    rows.sort(key=lambda row: row.observed_at, reverse=True)
    truncated = len(rows) > query.max_results
    facts = tuple(_to_fact(row) for row in rows)[: query.max_results]
    return ArmResponse(
        arm=ARM_NAME,
        outcome=ArmOutcome.ANSWERED,
        query=query,
        facts=facts,
        indexed_through=gt.TRIAL_NOW,
        truncated=truncated,
        versions={"episode_readback": _PROJECTION_VERSION},
    )


def answer(oracle: Oracle) -> ArmResponse:
    """The response a plain, already-approved episode list-by-repo endpoint
    would give.

    Registered as a baseline component (``ArmRole.BASELINE_COMPONENT``), not
    a candidate -- readback beating a class means the graph's margin there is
    zero, which is exactly the measurement amended §14 exists to make.
    """
    if oracle.question_id == "Q4":
        return _answer_prior_attempts(oracle)
    if oracle.question_id == "Q1":
        # Q1's subject is a CI failure signature, not a repo or episode --
        # plain episode data has no edge from a signature to anything at
        # all, so this is a structural gap, not "genuinely no results for
        # this repo". Declared explicitly rather than falling through
        # _answer_prior_attempts to an unexplained empty list (C16's
        # silent-emptiness failure mode).
        return _degraded(oracle, "no_signature_to_episode_association")
    return _degraded(oracle)
