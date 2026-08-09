"""The measured artifact. This file's output IS the result.

Same rule as ``trials.chaos_3619.records``: the raw machine-readable record
is the source of truth and anything human-readable is derived from it, so a
summary cannot quietly describe a run that did not happen.

Two fields here exist because of what the delta means without them.

``embedded_text_surface`` records **what the vectors were actually built
from**. A node's vector is the embedding of its ``name``, which the arm sets
to the display label; aliases, acronyms and previous names live in node
attributes and are embedded by nothing. A reader comparing a semantic leg to
an alias-registry leg without that fact will attribute the result to
"embeddings are weak" when the honest statement is "these embeddings never
saw the alias". It is derived from the projection at run time rather than
typed in.

``delta.classification`` is a closed enum rather than prose. The question
CHAOS-3647 was opened to answer is "does semantic retrieval add discovery
value", and the answer is a distribution over these classes. A free-text
verdict per case invites a summary nobody can recount.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from . import SEMANTIC_TRIAL_SCHEMA_VERSION

__all__ = [
    "CaseRecord",
    "Delta",
    "DeltaRecord",
    "EmbeddedTextSurface",
    "LegRecord",
    "SemanticTrialRecords",
    "write_records",
]


class Delta(StrEnum):
    """What changed between the pinned baseline and the semantic leg.

    Named from the *baseline's* point of view, so the class that would be
    cited as evidence for adopting retrieval — ``SEMANTIC_ONLY_CORRECT`` —
    has to be earned rather than inferred from a case simply moving.
    """

    #: Both legs resolved the case's subject correctly.
    BOTH_CORRECT = "both_correct"
    #: The pinned deterministic baseline was right and the semantic leg was
    #: not. A regression, and the reason the baseline is not replaced.
    DETERMINISTIC_ONLY_CORRECT = "deterministic_only_correct"
    #: The claim CHAOS-3647 exists to test: the semantic leg resolved
    #: correctly where the baseline could not.
    SEMANTIC_ONLY_CORRECT = "semantic_only_correct"
    #: Neither resolved correctly, and the semantic leg ranked nothing —
    #: so it at least did not manufacture confidence.
    NEITHER_CORRECT_NEITHER_RANKED = "neither_correct_neither_ranked"
    #: Neither resolved correctly, and the semantic leg produced a ranked
    #: answer anyway where the baseline refused. The most expensive class:
    #: a wrong answer costs more than a refusal, and this is the one a
    #: headline "graph reaches more answers" figure would hide.
    NEITHER_CORRECT_SEMANTIC_RANKED_ANYWAY = "neither_correct_semantic_ranked_anyway"
    #: Neither resolved correctly and both ranked something.
    NEITHER_CORRECT_BOTH_RANKED = "neither_correct_both_ranked"


@dataclass(frozen=True)
class EmbeddedTextSurface:
    """What the store's vectors were built from, measured off the projection."""

    #: The node field the embedder consumed. Read from the arm rather than
    #: asserted: ``backend.to_graphiti_nodes`` sets ``EntityNode.name``.
    node_embedded_field: str
    edge_embedded_field: str
    #: Fields present on the projection that NOTHING embedded. The list a
    #: reader needs before interpreting an alias-resolution failure.
    not_embedded: tuple[str, ...]
    #: Aliases in the corpus world, none of which reach a vector.
    alias_count: int
    node_count: int
    edge_count: int
    #: The full-text index's own field list, read from Graphiti's query
    #: builder rather than assumed, because BM25's reach is half of a hybrid
    #: leg's result and an assumed index is an unmeasured one.
    fulltext_indexed_fields: tuple[str, ...]


@dataclass(frozen=True)
class LegRecord:
    """One leg's resolution and score for one case."""

    leg: str
    query: str
    mentions: tuple[str, ...]
    subjects: tuple[dict[str, Any], ...]
    authorization_filtered_count: int
    withheld_canonical_ids: tuple[str, ...]
    bm25_order: tuple[str, ...]
    cosine_order: tuple[str, ...]
    observation_hits: tuple[str, ...]
    score: dict[str, Any]
    #: CHAOS-3635 oracle v2 applied to this leg's ranked output — the
    #: display labels a packet's ``candidates[].display_label`` and
    #: ``match_signals[].matched_text`` would be built from. Non-empty means
    #: a restricted NAME reached the consumer surface even if no restricted
    #: id did, which is the disclosure a canonical-id audit cannot see.
    prose_disclosures: tuple[str, ...]


@dataclass(frozen=True)
class DeltaRecord:
    """The semantic-versus-deterministic comparison for one case."""

    classification: str
    baseline_correct: bool
    semantic_correct: bool
    baseline_ranked: int
    semantic_ranked: int
    detail: str


@dataclass(frozen=True)
class CaseRecord:
    """One corpus case, every leg, and the delta between two of them."""

    case_id: str
    question: str
    question_family: str
    expected_answer: str
    committed_subject_id: str | None
    permitted_candidate_ids: tuple[str, ...]
    forbidden_subject_ids: tuple[str, ...]
    principal_id: str
    legs: tuple[LegRecord, ...]
    delta: DeltaRecord
    #: The pinned CHAOS-3619 artifact's disposition for this case on the
    #: interpreter-lifted leg, copied verbatim. Carried so a reader can check
    #: that the re-run baseline agrees with the pinned one without opening a
    #: second file.
    pinned_baseline_disposition: str
    pinned_baseline_detail: str


@dataclass(frozen=True)
class SemanticTrialRecords:
    """The whole measured run."""

    schema_version: str = SEMANTIC_TRIAL_SCHEMA_VERSION
    binding: dict[str, Any] = field(default_factory=dict)
    embedded_text_surface: dict[str, Any] = field(default_factory=dict)
    cases: tuple[CaseRecord, ...] = ()
    authorization_probes: tuple[dict[str, Any], ...] = ()
    summary: dict[str, Any] = field(default_factory=dict)
    #: Statements a reader would otherwise have to infer, written before the
    #: numbers were seen. Kept in the artifact rather than only in a PR body,
    #: because the artifact outlives the PR.
    scope_notes: tuple[str, ...] = ()


def write_records(records: SemanticTrialRecords, path: Path) -> None:
    """Write the artifact. Sorted keys, trailing newline, stable ordering."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(asdict(records), indent=1, sort_keys=True) + "\n",
        encoding="utf-8",
    )
