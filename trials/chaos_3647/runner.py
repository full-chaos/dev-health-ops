"""The measured run: ingest once under a real embedder, then all three legs.

Run it with the live store and a real embedding model::

    GRAPH_TRIAL_PROJECT="graph-trial-$(openssl rand -hex 6)"
    docker compose --project-name "$GRAPH_TRIAL_PROJECT" --profile graph-trial up -d graph-trial-store
    GRAPH_TRIAL_STORE_PORT="$(docker compose --project-name "$GRAPH_TRIAL_PROJECT" port graph-trial-store 6379 | awk -F: '{print $NF}')"
    CONTEXT_FABRIC_GRAPH_STORE_URI="falkor://127.0.0.1:$GRAPH_TRIAL_STORE_PORT" \\
    CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE=1 \\
    CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED=1 \\
    uv run python -m trials.chaos_3647.runner
    docker compose --project-name "$GRAPH_TRIAL_PROJECT" down --volumes --remove-orphans

**Every precondition raises rather than degrades.** No live store, no API
key, a non-semantic embedder — each is a hard stop with a message naming
what is missing. The alternative is a run that completes, writes a full set
of records under a "semantic" heading, and is wrong in every row with
nothing in the file to say so.

**Both tenants are written.** The Helio partition is what the legs query;
the Lumen partition exists so the cross-tenant probe has something real to
be excluded. A partition assertion made against an empty neighbouring
keyspace is an assertion that cannot fail, which is worse than not making
it.

**The Helio partition is purged on both sides.** A partition left behind by
an interrupted earlier run would otherwise be read as this run's world —
and, worse here than on the deterministic sweep, would mix hash vectors with
model vectors inside one keyspace, which the readback's embedder attestation
exists to refuse.
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.investigation_corpus.cases import (
    CorpusFamily,
    authored_cases,
)
from dev_health_ops.api.dev.investigation_corpus.oracles import oracle_for
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.backend import CloudEmbedder
from dev_health_ops.context_fabric.graph_arm.flags import (
    graph_projection_enabled,
    live_store_required,
    trial_store_config,
)
from dev_health_ops.context_fabric.graph_arm.projection import build_projection
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    wait_for_fulltext_index,
)
from dev_health_ops.context_fabric.graph_arm.store import GraphArmStore
from trials.chaos_3619.binding import RunClass, collect_binding

from .legs import (
    LegId,
    LegResolution,
    prose_disclosures_in,
    resolve_deterministic,
    resolve_semantic,
)
from .probes import PROBES, run_probe
from .records import (
    CaseRecord,
    Delta,
    DeltaRecord,
    EmbeddedTextSurface,
    LegRecord,
    SemanticTrialRecords,
    write_records,
)
from .scoring import LegScore, score_leg

__all__ = ["RESULTS_PATH", "run", "main"]

RESULTS_PATH = Path(__file__).resolve().parent / "results" / "semantic-leg.records.json"

#: The pinned CHAOS-3619 artifact, read for its dispositions only. Never
#: written, never scored against — it is quoted into each case record so a
#: reader can see the baseline this run claims to extend without opening a
#: second file.
_PINNED_BASELINE = (
    Path(__file__).resolve().parents[1]
    / "chaos_3619"
    / "results"
    / "trial-results.records.json"
)

#: Statements written before the numbers were seen. See ``records.py``.
SCOPE_NOTES: tuple[str, ...] = (
    "This measures SUBJECT RESOLUTION only. No packet is assembled, no "
    "driver is synthesised and no evidence is cited, so nothing here "
    "supports or refutes a claim about answer quality.",
    "The CHAOS-3619 deterministic run remains the pinned baseline. This run "
    "adds a leg; it does not replace, edit or rescore that artifact.",
    "The semantic leg receives the RAW QUESTION and the deterministic "
    "baseline receives PRODUCTION-EXTRACTED MENTIONS, because those are the "
    "inputs each mechanism is built for. The deterministic_question leg "
    "exists to price that asymmetry: it runs the baseline matcher over the "
    "raw question, so a reader can see how much of any delta is retrieval "
    "and how much is the mention extractor returning nothing.",
    "Node vectors are embeddings of the DISPLAY LABEL. Aliases, acronyms "
    "and previous names are node attributes and are embedded by nothing, so "
    "a semantic leg cannot resolve an alias by similarity however good the "
    "model is. See embedded_text_surface.",
    "The eight corpus ambiguity questions withhold nothing on any leg, "
    "because none of them is near the restricted project. The "
    "authorization_probes section exists because a clean audit obtained "
    "that way is not evidence the filter works.",
)


class PreconditionError(RuntimeError):
    """A precondition for a measured semantic run is missing."""


def _require_preconditions() -> Any:
    config = trial_store_config()
    if config is None:
        raise PreconditionError(
            "CONTEXT_FABRIC_GRAPH_STORE_URI is unset. This run must reach a "
            "live store; there is deliberately no default host and port"
        )
    if not live_store_required():
        raise PreconditionError(
            "CONTEXT_FABRIC_GRAPH_REQUIRE_LIVE is not 1. A measured run must "
            "fail on an unreachable store rather than skip past it"
        )
    if not graph_projection_enabled():
        raise PreconditionError(
            "CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED is not 1, so the store "
            "would refuse the write this run depends on"
        )
    return config


def _ambiguity_cases() -> tuple[Any, ...]:
    return tuple(
        case
        for case in authored_cases()
        if case.corpus_family is CorpusFamily.HUMAN_AMBIGUITY
    )


def _pinned_dispositions() -> dict[str, tuple[str, str]]:
    """The pinned baseline's graph-arm disposition per case, best leg first.

    "Best" means the interpreter-lifted leg where it ran: the as-deployed leg
    reports ``not_run_precondition`` for every colloquial case because the
    production interpreter derived no question family, and quoting that would
    describe an interpretation ceiling as a retrieval result.
    """

    import json

    if not _PINNED_BASELINE.exists():  # pragma: no cover - environment
        return {}
    payload = json.loads(_PINNED_BASELINE.read_text(encoding="utf-8"))
    best: dict[str, tuple[str, str]] = {}
    for case in payload.get("cases", ()):
        case_id = case.get("case_id", "")
        for arm in case.get("arms", ()):
            if arm.get("arm_id") != "graph_assisted_shadow_arm":
                continue
            disposition = str(arm.get("disposition", ""))
            detail = str(arm.get("detail", ""))
            existing = best.get(case_id)
            if existing is None or existing[0] == "not_run_precondition":
                best[case_id] = (disposition, detail)
    return best


def _embedded_text_surface(projection: Any) -> EmbeddedTextSurface:
    """Measured off the projection and off Graphiti, never asserted."""

    alias_count = sum(len(node.aliases) for node in projection.nodes)
    return EmbeddedTextSurface(
        node_embedded_field="EntityNode.name (== GraphNode.display_label)",
        edge_embedded_field="EntityEdge.fact (== '<source_id> <relationship> <target_id>')",
        not_embedded=(
            "GraphNode.canonical_id",
            "GraphNode.aliases[].value (alias, acronym, previous_name, provider_identifier)",
            "GraphNode.attributes[*]",
            "EntityNode.summary (always empty by the arm's no-prose rule)",
        ),
        alias_count=alias_count,
        node_count=len(projection.nodes),
        edge_count=len(projection.edges),
        # FalkorDB's Entity full-text index, as Graphiti creates it.
        fulltext_indexed_fields=("name", "summary", "group_id"),
    )


def _classify(baseline: LegScore, semantic: LegScore, semantic_ranked: int) -> Delta:
    if baseline.subject_resolution_correct and semantic.subject_resolution_correct:
        return Delta.BOTH_CORRECT
    if baseline.subject_resolution_correct:
        return Delta.DETERMINISTIC_ONLY_CORRECT
    if semantic.subject_resolution_correct:
        return Delta.SEMANTIC_ONLY_CORRECT
    baseline_ranked = baseline.resolved
    if not semantic_ranked and not baseline_ranked:
        return Delta.NEITHER_CORRECT_NEITHER_RANKED
    if semantic_ranked and not baseline_ranked:
        return Delta.NEITHER_CORRECT_SEMANTIC_RANKED_ANYWAY
    return Delta.NEITHER_CORRECT_BOTH_RANKED


def _leg_record(
    resolution: LegResolution, score: LegScore, principal_id: str
) -> LegRecord:
    return LegRecord(
        leg=resolution.leg.value,
        prose_disclosures=prose_disclosures_in(resolution, principal_id),
        query=resolution.query,
        mentions=resolution.mentions,
        subjects=tuple(asdict(subject) for subject in resolution.subjects),
        authorization_filtered_count=resolution.authorization_filtered_count,
        withheld_canonical_ids=resolution.withheld_canonical_ids,
        bm25_order=resolution.bm25_order,
        cosine_order=resolution.cosine_order,
        observation_hits=resolution.observation_hits,
        score=asdict(score),
    )


async def run() -> SemanticTrialRecords:
    """Ingest under a real embedder, run every leg, judge, and record."""

    config = _require_preconditions()
    embedder = CloudEmbedder.from_environment()
    if not embedder.semantic:  # pragma: no cover - from_environment raises first
        raise PreconditionError(
            f"embedder {embedder.model_id!r} reports semantic=False; a "
            "semantic leg run on it would be the deterministic baseline "
            "wearing a costume"
        )

    helio = build_projection(adapter.corpus_batch(world.ORG_HELIO))
    lumen = build_projection(adapter.corpus_batch(world.ORG_LUMEN))
    # Each case's grant is derived from ITS OWN principal below, never from a
    # single analyst grant hoisted here. The corpus's ambiguity family happens
    # to be all-analyst today; a hoisted set would silently answer a
    # non-analyst case against the wrong grant the day one is added.
    pinned = _pinned_dispositions()

    helio_store = GraphArmStore.for_org(
        world.ORG_HELIO, config=config, embedder=embedder
    )
    lumen_store = GraphArmStore.for_org(
        world.ORG_LUMEN, config=config, embedder=embedder
    )

    cases: list[CaseRecord] = []
    probe_records: list[dict[str, Any]] = []
    try:
        for store, projection in ((helio_store, helio), (lumen_store, lumen)):
            await store.purge_org()
            await store.build_indices()
            await store.write_projection(projection)

        # The BM25 half of the hybrid leg must be live before anything is
        # measured. An earlier recorded run of this trial queried too soon
        # and produced `bm25_order == []` on all eight cases — a cosine-only
        # result under a hybrid heading, with nothing in the file to say so.
        # The probe is a positive control: a label whose correct answer is
        # known, checked for that answer rather than for a non-empty result.
        readiness = await wait_for_fulltext_index(
            helio_store,
            probe_query=world.ENTITIES_BY_ID[world.PROJ_IDENTITY_REWRITE].display_label,
            expected_canonical_id=world.PROJ_IDENTITY_REWRITE,
        )

        for case in _ambiguity_cases():
            oracle = oracle_for(case.case_id)
            case_authorized = adapter.authorized_entity_ids_for(case.principal_id)
            baseline = resolve_deterministic(
                question=case.question,
                projection=helio,
                authorized_entity_ids=case_authorized,
                over_mentions=True,
            )
            question_leg = resolve_deterministic(
                question=case.question,
                projection=helio,
                authorized_entity_ids=case_authorized,
                over_mentions=False,
            )
            semantic = await resolve_semantic(
                question=case.question,
                store=helio_store,
                authorized_entity_ids=case_authorized,
            )

            scores = {
                resolution.leg: score_leg(case.case_id, resolution)
                for resolution in (baseline, question_leg, semantic)
            }
            classification = _classify(
                scores[LegId.DETERMINISTIC_MENTIONS],
                scores[LegId.SEMANTIC_HYBRID],
                len(semantic.subjects),
            )
            pinned_disposition, pinned_detail = pinned.get(case.case_id, ("", ""))
            cases.append(
                CaseRecord(
                    case_id=case.case_id,
                    question=case.question,
                    question_family=case.question_family.value,
                    expected_answer=oracle.expected_answer.value,
                    committed_subject_id=oracle.committed_subject_id,
                    permitted_candidate_ids=tuple(oracle.permitted_candidate_ids),
                    forbidden_subject_ids=tuple(oracle.forbidden_subject_ids),
                    principal_id=case.principal_id,
                    legs=tuple(
                        _leg_record(
                            resolution, scores[resolution.leg], case.principal_id
                        )
                        for resolution in (baseline, question_leg, semantic)
                    ),
                    delta=DeltaRecord(
                        classification=classification.value,
                        baseline_correct=scores[
                            LegId.DETERMINISTIC_MENTIONS
                        ].subject_resolution_correct,
                        semantic_correct=scores[
                            LegId.SEMANTIC_HYBRID
                        ].subject_resolution_correct,
                        baseline_ranked=len(baseline.subjects),
                        semantic_ranked=len(semantic.subjects),
                        detail=(
                            "baseline: "
                            f"{scores[LegId.DETERMINISTIC_MENTIONS].subject_resolution_detail}"
                            " | semantic: "
                            f"{scores[LegId.SEMANTIC_HYBRID].subject_resolution_detail}"
                        ),
                    ),
                    pinned_baseline_disposition=pinned_disposition,
                    pinned_baseline_detail=pinned_detail,
                )
            )

        for probe in PROBES:
            outcome = await run_probe(
                probe,
                store=helio_store,
                authorized_entity_ids=adapter.authorized_entity_ids_for(
                    probe.principal_id
                ),
                presence_store=lumen_store if probe.presence_partition else None,
                presence_authorized_entity_ids=(
                    adapter.authorized_entity_ids_for(probe.presence_principal_id)
                    if probe.presence_partition
                    else None
                ),
            )
            probe_records.append(asdict(outcome))
    finally:
        for store in (helio_store, lumen_store):
            try:
                await store.purge_org()
            finally:
                await store.close()

    binding = collect_binding(
        run_class=RunClass.MEASURED,
        per_case_timeout_seconds=120.0,
        trial_store_backend=f"falkordb ({config.host}:{config.port})",
        graph_embedder_model_id=embedder.model_id,
        notes=(
            "CHAOS-3647 semantic/hybrid retrieval leg. Subject resolution "
            "only; no packet assembly.",
        ),
    )
    return SemanticTrialRecords(
        binding=asdict(binding),
        fulltext_readiness=asdict(readiness),
        embedded_text_surface=asdict(_embedded_text_surface(helio)),
        cases=tuple(cases),
        authorization_probes=tuple(probe_records),
        summary=_summarise(cases, probe_records),
        scope_notes=SCOPE_NOTES,
    )


def _summarise(cases: list[CaseRecord], probes: list[dict[str, Any]]) -> dict[str, Any]:
    """Counts only. Every figure here is recountable from ``cases``."""

    by_class: dict[str, list[str]] = {}
    for case in cases:
        by_class.setdefault(case.delta.classification, []).append(case.case_id)
    leg_correct: dict[str, list[str]] = {}
    for case in cases:
        for leg in case.legs:
            if leg.score["subject_resolution_correct"]:
                leg_correct.setdefault(leg.leg, []).append(case.case_id)
    return {
        "cases_measured": len(cases),
        "delta_classification_counts": {
            name: len(ids) for name, ids in sorted(by_class.items())
        },
        "delta_classification_case_ids": {
            name: sorted(ids) for name, ids in sorted(by_class.items())
        },
        "subject_resolution_correct_by_leg": {
            leg.value: sorted(leg_correct.get(leg.value, [])) for leg in LegId
        },
        "authorization_probes_run": len(probes),
        "authorization_probe_verdicts": {
            probe["probe_id"]: probe["verdict"] for probe in probes
        },
        "authorization_probes_ineffective": sorted(
            probe["probe_id"] for probe in probes if not probe["effective"]
        ),
        "any_unauthorized_entity_ranked": any(
            probe["target_is_restricted"] and probe["target_ranked"] for probe in probes
        ),
        # CHAOS-3635 oracle v2, over every leg of every case. A restricted
        # NAME reaching the consumer surface is a disclosure even when no
        # restricted id does, and this is the figure that says so.
        "prose_disclosures_by_leg": {
            leg.leg: list(leg.prose_disclosures)
            for case in cases
            for leg in case.legs
            if leg.prose_disclosures
        },
        "any_prose_disclosure": any(
            leg.prose_disclosures for case in cases for leg in case.legs
        ),
    }


def main() -> int:
    records = asyncio.run(run())
    write_records(records, RESULTS_PATH)
    summary = records.summary
    print(f"wrote {RESULTS_PATH}")
    print(f"cases measured: {summary['cases_measured']}")
    for name, count in summary["delta_classification_counts"].items():
        print(f"  {name}: {count}")
    print(f"probe verdicts: {summary['authorization_probe_verdicts']}")
    if summary["authorization_probes_ineffective"]:
        print(
            "  INEFFECTIVE PROBES (failed measurement, not a clean result): "
            f"{summary['authorization_probes_ineffective']}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover - entry point
    sys.exit(main())
