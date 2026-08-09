"""Why the graph arm refused, decomposed by cause it actually recorded.

The sweep records ONE refusal string for every unresolved case -- "no
authorized subject resolved from the question" -- because that is the single
branch the leg takes when ``seeds_from`` comes back empty. That string is
true and it is not enough: it cannot distinguish an arm that was *not allowed*
to see a match from one that found nothing to match, and those are opposite
facts about the same count. A reader deciding whether refusals are a
capability gap needs them separated.

**This module does not re-interpret the records; it re-derives the cause from
the same pure function the sweep called, and then checks itself against the
records.** ``discover_subjects`` is pure over (question, projection, grant),
all three of which are frozen, so recomputing it reproduces exactly what the
sweep saw. :func:`decompose` asserts that: every case whose recomputed seed
set is empty must be a case the records show as refused, and vice versa. A
mismatch means the recomputation has drifted from the run and the numbers
must not be published -- so it raises rather than reporting.

The categories are the ones a reader has to tell apart:

``authorization`` -- the arm matched entities and the grant withheld them.
    This is the arm's authorization working, and on this corpus it is a
    planted case. It is CORRECT BEHAVIOUR, not a gap, and counting it with
    the misses would understate the arm.
``no_mention_extracted`` -- production ``extract_mentions`` (plus the untyped
    backstop) returned no subject phrase at all, so discovery had nothing to
    search for. The graph arm shares this extractor with the native
    interpreter, which is what makes this category a COMMON-MODE limit rather
    than an arm difference.
``no_authorized_match`` -- phrases were extracted and none matched an entity
    the principal may see.

Deliberately NOT a category: "shape unsupported". No refusal records one, and
inventing it would attribute to the arm's coverage something the records
attribute to subject resolution.

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
  entry whose stated cause stopped being true is drift like any other.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

__all__ = [
    "DIVERGENCE_LEDGER",
    "LedgeredDivergence",
    "RefusalCause",
    "decompose",
]

AUTHORIZATION = "authorization"
NO_MENTION = "no_mention_extracted"
NO_MATCH = "no_authorized_match"

#: The cause a CHAOS-3648 case carries *now*: the pinned sweep recorded it as
#: an absent-extraction refusal, and the current extractor resolves a subject
#: for it. Held as its own category rather than folded into a recomputed
#: ``no_authorized_match`` so a reader can never mistake the ledgered rows for
#: rows the pinned sweep measured that way.
PHRASE_EXTRACTED_POST_3648 = "phrase_extracted_post_3648"


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
DIVERGENCE_LEDGER: tuple[LedgeredDivergence, ...] = (
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


def decompose(records_path: Path, leg: str) -> tuple[RefusalCause, ...]:
    """Every refusal in ``leg``, with its cause, verified against the records.

    Raises when the recomputation and the records disagree: a decomposition
    that has drifted from the run it describes is worse than none, because it
    reads as evidence about a sweep it no longer matches.

    The single exception is a divergence named in :data:`DIVERGENCE_LEDGER`,
    moving in the direction that entry states, whose stated cause the live
    recomputation still bears out. Such a case keeps its place in the refusal
    set the records measured -- the pinned sweep really did refuse it -- and
    carries the ledgered category instead of a recomputed one.
    """

    from dev_health_ops.api.dev.investigation_corpus import world
    from dev_health_ops.api.dev.investigation_corpus.cases import authored_cases
    from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
    from dev_health_ops.context_fabric.graph_arm.projection import build_projection

    from . import graph_leg
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
        discovery = graph_leg.discover_subjects(
            question=case.question,
            projection=projection,
            authorized_entity_ids=grant,
        )
        mentions = graph_leg.mention_texts(case.question)
        empty = not graph_leg.seeds_from(discovery)
        refused = disposition == "arm_refused"
        if empty != refused:
            entry = _ledger_entry_for(case.case_id)
            # Admitted only in the ledgered direction (the pin refused, the
            # live code now resolves) AND only while the entry's stated cause
            # still holds. The mirror image -- a case the pin scored that the
            # live code now refuses -- is a recall regression and must stop
            # the run even for a ledgered case, which is why this tests the
            # direction rather than membership alone.
            if (
                entry is not None
                and refused
                and not empty
                and entry.from_category == NO_MENTION
                and mentions
            ):
                causes.append(
                    RefusalCause(
                        case_id=case.case_id,
                        comparison_shape=shape,
                        corpus_family=family,
                        category=entry.to_category,
                        mentions_extracted=len(mentions),
                        authorization_filtered_count=(
                            discovery.authorization_filtered_count
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
        if discovery.authorization_filtered_count > 0 and not discovery.candidates:
            category = AUTHORIZATION
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
                authorization_filtered_count=(discovery.authorization_filtered_count),
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
