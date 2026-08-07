"""The seven evaluation questions and their §15.2 class assignments.

Each classification carries the file:line evidence that decided it, because
"native-answerable" is a claim about this codebase at this commit, not a
judgement call. The evidence is restated in
``trials/chaos-3499/docs/baseline-inventory.md``; if the two ever disagree,
the code wins and both are wrong until reconciled.

A result worth stating up front, before any arm runs: **six of the seven
questions are not natively answerable today, and five of those are class
(c)**. The question set is therefore biased toward the arm under evaluation.
That is not a reason to change the questions -- they are the questions the
product wants answered -- but it *is* a reason the ADR must report per class
and must never quote an aggregate alone, because an aggregate over this set
would flatter any extraction-capable arm regardless of merit.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..harness.contracts import QuestionClass


@dataclass(frozen=True)
class EvaluationQuestion:
    question_id: str
    text: str
    question_class: QuestionClass
    classification_evidence: tuple[str, ...]
    rationale: str
    #: Set when a question straddles classes. The ADR reports the assigned
    #: class, but hiding the split would misrepresent what the arm improved.
    split_note: str = ""


EVALUATION_QUESTIONS: tuple[EvaluationQuestion, ...] = (
    EvaluationQuestion(
        question_id="Q1",
        text="What did we try last time this CI failure occurred?",
        question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
        classification_evidence=(
            "acr/internal/storage/interfaces.go:85-91 -- EpisodeStore exposes "
            "PreflightIdempotency/CreateIdempotent/GetByClientEpisodeID/Redact/"
            "PurgeExpired; no list-by-repo/task/file-overlap method exists.",
            "acr/internal/api/app.go:77 -- episodes route is POST-only; "
            "acr/contracts/openapi/acr-v1.yaml:132-170 defines no GET.",
            "acr/internal/contextpacket/ -- no episode reference in packet "
            "assembly; the 17-source catalog does not include episodes.",
        ),
        rationale=(
            "'What we tried' lives only in agent episodes, which have no read "
            "path at all today, and associating those episodes with a CI "
            "failure signature is cross-episode association by definition."
        ),
        split_note=(
            "ci_pipeline_runs.v1 can establish that the failure recurred "
            "(acr source_queries.go:56), so the recurrence half is native; "
            "the attempted-remedy half is not."
        ),
    ),
    EvaluationQuestion(
        question_id="Q2",
        text="What was blocking Project X on July 15, and what changed since?",
        question_class=QuestionClass.NEEDS_DECLARED_STATE_HISTORY,
        classification_evidence=(
            "ops/src/dev_health_ops/api/dev/native_status_change.py:369-379 -- "
            "'there is no history left to read here ... an as-of snapshot of a "
            "since-changed declared state is simply unavailable'.",
            "ops/src/dev_health_ops/migrations/clickhouse/014_work_graph.sql:"
            "5-15 -- projects is ReplacingMergeTree(updated_at) with ORDER BY "
            "(org_id, provider, id); updated_at is NOT in the sorting key, so "
            "FINAL collapses every prior declared state.",
            "ops/src/dev_health_ops/migrations/clickhouse/014_work_graph.sql:"
            "6-22 -- work_graph_edges is ReplacingMergeTree(last_synced) keyed "
            "on (org_id, source_type, source_id, edge_type, target_type, "
            "target_id) with discovered_at but NO valid_to column.",
            "ops/src/dev_health_ops/api/dev/native_status_change.py:456-499 -- "
            "_BLOCKERS_SQL reads work_graph_edges where edge_type='blocks'.",
        ),
        rationale=(
            "The as-of half needs history the storage engine discards. Note "
            "this question exposes a SECOND gap beyond the one the PRD "
            "documents: blocker edges have discovered_at but no valid_to, so "
            "the interval during which a blocker held is not representable "
            "either. CHAOS-3563 as scoped covers declared state; the edge "
            "gap needs its own disposition in the ADR."
        ),
        split_note=(
            "'What changed since' is class (a): work_item_transitions keeps "
            "occurred_at in its sorting key "
            "(009_raw_work_items.sql:30-42, 027_add_org_id_to_sorting_keys.py:"
            "70), so status history is genuinely append-only and readable."
        ),
    ),
    EvaluationQuestion(
        question_id="Q3",
        text="Which decision superseded the original deployment design?",
        question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
        classification_evidence=(
            "ops/src/dev_health_ops/work_graph/models.py:37-84 -- EdgeType is "
            "a closed 30-member enum with no 'supersedes' member.",
            "No canonical record links one architecture decision to another; "
            "the relationship exists only in ADR prose.",
        ),
        rationale=(
            "Supersession between decisions is not a structured relationship "
            "anywhere in the canonical model, so §7.1 deterministic "
            "projection cannot produce it."
        ),
    ),
    EvaluationQuestion(
        question_id="Q4",
        text="Show prior agent attempts touching this subsystem and outcomes.",
        question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
        classification_evidence=(
            "acr/internal/storage/interfaces.go:85-91 -- no episode list/query method.",
            "acr/internal/contracts/v1/types.go:336-340 -- EpisodeArtifacts "
            "carries FilesTouched/ArtifactURIs/TestsRun, i.e. the subsystem "
            "overlap signal EXISTS in the stored payload and needs no "
            "extraction to use.",
            "acr/internal/sidecar/config.go:194 -- ACR_ENABLE_WRITEBACK "
            "defaults to false, so episode volume is ~zero today.",
        ),
        rationale=(
            "Classified (c) only because no read path exists. The signal "
            "itself is structured. This is the question where the "
            "episode-readback arm matters most: if plain readback answers it, "
            "the graph's margin here is zero and an aggregate that credits "
            "the graph for it is measuring the wrong thing."
        ),
        split_note=(
            "Becomes class (a) once CHAOS-3564 lands the read path. The ADR "
            "must state which side of that landing the trial measured."
        ),
    ),
    EvaluationQuestion(
        question_id="Q5",
        text="Which current project facts conflict with earlier evidence?",
        question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
        classification_evidence=(
            "ops/src/dev_health_ops/migrations/clickhouse/014_work_graph.sql:"
            "6-22 -- work_graph_edges has confidence/provenance/evidence but "
            "the ReplacingMergeTree key omits discovered_at, so a changed "
            "confidence or provenance overwrites the prior value with no "
            "retained history to conflict against.",
        ),
        rationale=(
            "Contradiction detection needs two versions of a fact "
            "simultaneously. The canonical store keeps one."
        ),
    ),
    EvaluationQuestion(
        question_id="Q6",
        text="What recurring failure pattern is supported across incidents?",
        question_class=QuestionClass.NEEDS_EXTRACTION_OR_ASSOCIATION,
        classification_evidence=(
            "ops/src/dev_health_ops/migrations/clickhouse/"
            "066_operational_canonical.sql:69-79 -- "
            "operational_incident_timeline_events exists with occurred_at and "
            "source_event_at, so the per-incident timeline IS native.",
            "ops/src/dev_health_ops/work_graph/models.py:37-84 -- no edge type "
            "expresses 'shares a root cause with'.",
        ),
        rationale=(
            "The raw incident history is native; the cross-incident "
            "generalisation is not. This is the cleanest test of PRD §7.3: "
            "the corpus decoy (C05) shares subsystem and timing but not "
            "cause, so an arm that groups by proximity fails here."
        ),
        split_note=(
            "Retrieving the incidents is class (a); asserting a shared "
            "pattern across them is class (c)."
        ),
    ),
    EvaluationQuestion(
        question_id="Q7",
        text="What was true about this dependency as of date Y?",
        question_class=QuestionClass.NATIVE_ANSWERABLE,
        classification_evidence=(
            "ops/src/dev_health_ops/migrations/clickhouse/"
            "066_operational_canonical.sql:243-264 -- "
            "operational_service_repository_mappings carries valid_from/"
            "valid_to.",
            "ops/src/dev_health_ops/work_graph/operational_edges.py:44-48 -- "
            "the interval filter valid_from <= now AND (valid_to IS NULL OR "
            "valid_to > now).",
            "acr/internal/contextpacket/source_queries.go:62 -- incidents.v1 "
            "applies the same interval bound against scope.as_of.",
            "ops/src/dev_health_ops/metrics/loaders/clickhouse.py:392-499 -- "
            "three further as-of interval joins over team_* tables.",
        ),
        rationale=(
            "Genuine valid-time intervals, genuinely interval-filtered. Per "
            "PRD §15.2 the native arm should win or tie here; if it does not, "
            "the harness is broken and the finding is about the harness."
        ),
        split_note=(
            "Carries a live defect the trial must not paper over: "
            "066_operational_canonical.sql:261 declares valid_from as "
            "Nullable, and a NULL fails `valid_from <= as_of` in ClickHouse, "
            "so a null-start mapping is silently dropped by every as-of "
            "filter found. Corpus case C01 plants exactly such a row."
        ),
    ),
)

QUESTIONS_BY_ID = {q.question_id: q for q in EVALUATION_QUESTIONS}


def questions_in_class(
    question_class: QuestionClass,
) -> tuple[EvaluationQuestion, ...]:
    return tuple(q for q in EVALUATION_QUESTIONS if q.question_class is question_class)
