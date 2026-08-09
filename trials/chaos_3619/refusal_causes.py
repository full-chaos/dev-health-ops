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
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

__all__ = ["RefusalCause", "decompose"]

AUTHORIZATION = "authorization"
NO_MENTION = "no_mention_extracted"
NO_MATCH = "no_authorized_match"


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
            drift.append(
                f"{case.case_id}: recomputed empty_seeds={empty} but the "
                f"records say disposition={disposition!r}"
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
