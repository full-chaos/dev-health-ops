"""Generate or verify the checked-in corpus artifacts.

The corpus writes to its **own** root, ``contracts/ask-dev-investigation-
corpus/v1``, and deliberately not into ``contracts/ask-dev-investigation/v1``.
That tree belongs to the frozen CHAOS-3615 contract, and its drift gate
(``investigation_contract/export.py``, ``check_artifacts``) compares the full
path set: any unexpected file there is a failure. Adding corpus output to it
would put every CHAOS-3616 iteration on the critical path of the contract's
own freeze, which is exactly the coupling the freeze exists to prevent.

What is exported:

* ``manifest.json`` — pinned constants, counts, and a sha256 for every file;
* ``world/source_manifest.json`` — the per-source-class feed state;
* ``world/entities.json`` / ``relationships.json`` / ``evidence.json`` — the
  construction record a non-Python consumer can read;
* ``registries/cases.json``, ``oracles.json``, ``coverage_matrix.json``,
  ``dispositions.json``;
* ``examples/reference/*.json`` — three witness packets in full, and
  ``reference_digests.json`` with a sha256 for all of them.

Only three witness packets are written in full. The rest are covered by their
digests, which detects drift without adding forty near-identical files to the
tree. The digest list is not a substitute for the packets themselves: it is
reproducible from ``reference_packet(case_id)``, and the reproduction command
is in the architecture document.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from .cases import ALL_CASE_IDS, CASE_REGISTRY, REQUIRED_CORPUS_TOPICS, authored_cases
from .coverage import coverage_matrix, dispositions_table
from .oracles import CASE_ORACLES
from .reference import reference_packet
from .world import (
    CORPUS_VERSION,
    PRINCIPALS,
    SOURCE_MANIFEST,
    TRIAL_NOW,
    WINDOW_END,
    WINDOW_START,
    WORLD_ENTITIES,
    WORLD_EPISODES,
    WORLD_EPOCH,
    WORLD_EVIDENCE,
    WORLD_MEASUREMENTS,
    WORLD_RELATIONSHIPS,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[5]
ARTIFACT_ROOT = REPOSITORY_ROOT / "contracts" / "ask-dev-investigation-corpus" / "v1"

#: Witness packets written out in full: one supported singular-subject case,
#: one discovered-cohort portfolio case, one clarification case. Chosen to
#: cover the three structurally different packet shapes rather than to be
#: representative of the corpus's content.
FULL_WITNESS_CASE_IDS = (
    "S04_symptom_versus_driver",
    "S03_shared_dependency_portfolio_risk",
    "H05_the_other_project_we_discussed",
)

__all__ = [
    "ARTIFACT_ROOT",
    "check_artifacts",
    "expected_artifacts",
    "main",
    "write_artifacts",
]


def _json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def _sha256(contents: str) -> str:
    return hashlib.sha256(contents.encode("utf-8")).hexdigest()


def _iso(value: Any) -> str:
    return str(value).replace("+00:00", "Z")


def _world_artifacts() -> dict[str, Any]:
    return {
        "world/source_manifest.json": {
            "schema_version": "ask_dev_corpus_source_manifest.v1",
            "corpus_version": CORPUS_VERSION,
            "feeds": [
                {
                    "source_class": source_class.value,
                    "state": feed.state.value,
                    "watermark": _iso(feed.watermark) if feed.watermark else None,
                    "note": feed.note,
                    "degraded_entity_ids": list(feed.degraded_entity_ids),
                }
                for source_class, feed in sorted(
                    SOURCE_MANIFEST.items(), key=lambda item: item[0].value
                )
            ],
        },
        "world/entities.json": {
            "schema_version": "ask_dev_corpus_entities.v1",
            "entities": [
                {
                    "entity_id": entity.entity_id,
                    "kind": entity.kind.value,
                    "display_label": entity.display_label,
                    "tenant_id": entity.tenant_id,
                    "state": entity.state.value,
                    "declared_status": entity.declared_status,
                    "superseded_by": entity.superseded_by,
                    "aliases": [
                        {"text": alias.text, "signal": alias.signal.value}
                        for alias in entity.aliases
                    ],
                    "note": entity.note,
                }
                for entity in WORLD_ENTITIES
            ],
        },
        "world/relationships.json": {
            "schema_version": "ask_dev_corpus_relationships.v1",
            "relationships": [
                {
                    "relationship_key": edge.relationship_key,
                    "tenant_id": edge.tenant_id,
                    "source_entity_id": edge.source_entity_id,
                    "relationship": edge.relationship.value,
                    "target_entity_id": edge.target_entity_id,
                    "observed_at": _iso(edge.observed_at),
                    "valid_from": _iso(edge.valid_from) if edge.valid_from else None,
                    "valid_to": _iso(edge.valid_to) if edge.valid_to else None,
                    "evidence_slugs": list(edge.evidence_slugs),
                    "is_false_claim": edge.is_false_claim,
                    "relevance_at_trial_now": edge.relevance_at(TRIAL_NOW).value,
                    "note": edge.note,
                }
                for edge in WORLD_RELATIONSHIPS
            ],
        },
        "world/evidence.json": {
            "schema_version": "ask_dev_corpus_evidence.v1",
            "note": (
                "The world is the sole mint for evidence handles. An oracle "
                "may only require a handle listed here, which is the "
                "CHAOS-3612 recurrence guard."
            ),
            "evidence": [
                {
                    "slug": record.slug,
                    "handle": record.handle,
                    "tenant_id": record.tenant_id,
                    "source_class": record.source_class.value,
                    "entity_id": record.entity_id,
                    "display_label": record.display_label,
                    "observed_at": _iso(record.observed_at),
                    "state": record.state.value,
                    "trust": record.trust.value,
                    "is_adversarial": record.is_adversarial,
                    "control_entity_id": record.control_entity_id,
                    "is_citable": record.is_citable,
                }
                for record in WORLD_EVIDENCE
            ],
        },
        "world/principals.json": {
            "schema_version": "ask_dev_corpus_principals.v1",
            "note": (
                "The world's TRUE authorization grants. The packet contract "
                "can only check a packet against its own declared "
                "authorized_entity_ids; these are what make that declaration "
                "falsifiable."
            ),
            "principals": [
                {
                    "principal_id": principal.principal_id,
                    "tenant_id": principal.tenant_id,
                    "display_label": principal.display_label,
                    "visible_entity_ids": sorted(principal.visible_entity_ids),
                    "note": principal.note,
                }
                for principal in sorted(
                    PRINCIPALS.values(), key=lambda item: item.principal_id
                )
            ],
        },
    }


def _registry_artifacts() -> dict[str, Any]:
    cases = {
        "schema_version": "ask_dev_corpus_case_registry.v1",
        "required_topics": dict(sorted(REQUIRED_CORPUS_TOPICS.items())),
        "cases": [
            {
                "case_id": case.case_id,
                "corpus_family": case.corpus_family.value,
                "question_family": case.question_family.value,
                "title": case.title,
                "question": case.question,
                "variant_kind": case.variant_kind.value,
                "catches": case.catches,
                "topics": list(case.topics),
                "scoring_dimension_ids": [
                    item.value for item in case.scoring_dimension_ids
                ],
                "expected_answer": case.expected_answer.value,
                "comparison_shape": case.comparison_shape.value,
                "analytical_slice": case.analytical_slice.value,
                "principal_id": case.principal_id,
                "follows_case_id": case.follows_case_id,
                "disposition": case.disposition.value,
                "disposition_reason": case.disposition_reason,
            }
            for case in CASE_REGISTRY.values()
        ],
    }
    oracles = {
        "schema_version": "ask_dev_corpus_oracles.v1",
        "note": (
            "Expected results identify the required subject, cohort, lineage, "
            "drivers and evidence. No oracle names one exact prose answer."
        ),
        "oracles": [
            {
                "case_id": oracle.case_id,
                "permitted_candidate_ids": list(oracle.permitted_candidate_ids),
                "committed_subject_id": oracle.committed_subject_id,
                "forbidden_subject_ids": list(oracle.forbidden_subject_ids),
                "required_match_signals": [
                    item.value for item in oracle.required_match_signals
                ],
                "required_cohort_ids": list(oracle.required_cohort_ids),
                "forbidden_cohort_ids": list(oracle.forbidden_cohort_ids),
                "required_exclusion_ids": list(oracle.required_exclusion_ids),
                "required_entity_ids": list(oracle.required_entity_ids),
                "forbidden_entity_ids": list(oracle.forbidden_entity_ids),
                "required_paths": [item.key for item in oracle.required_paths],
                "forbidden_paths": [item.key for item in oracle.forbidden_paths],
                "expected_principal_drivers": [
                    {
                        "driver_key": driver.driver_key,
                        "category": driver.category.value,
                        "role": driver.role.value,
                        "standing": driver.standing.value,
                        "affected_entity_ids": list(driver.affected_entity_ids),
                        "supporting_evidence_slugs": list(
                            driver.supporting_evidence_slugs
                        ),
                        "supporting_paths": [
                            item.key for item in driver.supporting_paths
                        ],
                        "relevance": driver.relevance.value,
                        "rationale": driver.rationale,
                    }
                    for driver in oracle.expected_principal_drivers
                ],
                "expected_non_drivers": [
                    {
                        "driver_key": driver.driver_key,
                        "category": driver.category.value,
                        "role": driver.role.value,
                        "standing": driver.standing.value,
                        "affected_entity_ids": list(driver.affected_entity_ids),
                        "supporting_evidence_slugs": list(
                            driver.supporting_evidence_slugs
                        ),
                        "relevance": driver.relevance.value,
                        "rationale": driver.rationale,
                    }
                    for driver in oracle.expected_non_drivers
                ],
                "required_evidence_slugs": list(oracle.required_evidence_slugs),
                "forbidden_evidence": [
                    {"slug": item.slug, "reason": item.reason.value}
                    for item in oracle.forbidden_evidence
                ],
                "required_source_classes": [
                    item.value for item in oracle.required_source_classes
                ],
                "required_relevance": {
                    entity_id: state.value
                    for entity_id, state in sorted(oracle.required_relevance.items())
                },
                "required_limitation_kinds": [
                    item.value for item in oracle.required_limitation_kinds
                ],
                "confidence_ceiling": oracle.confidence_ceiling.value,
                "expected_answer": oracle.expected_answer.value,
                "permitted_outcomes": sorted(
                    item.value for item in oracle.permitted_outcomes
                ),
                "rationale": oracle.rationale,
            }
            for oracle in CASE_ORACLES.values()
        ],
    }
    matrix = coverage_matrix()
    coverage = {
        "schema_version": "ask_dev_corpus_coverage_matrix.v1",
        "reporting_shape": "per_question_family_x_per_dimension",
        "aggregate_score_prohibited": True,
        "cells": [
            {
                "question_family": family_id.value,
                "dimension": dimension_id.value,
                "status": cell.status.value,
                "authored_case_ids": list(cell.authored_case_ids),
                "skipped_case_ids": list(cell.skipped_case_ids),
            }
            for (family_id, dimension_id), cell in sorted(
                matrix.items(), key=lambda item: (item[0][0].value, item[0][1].value)
            )
        ],
    }
    dispositions = {
        "schema_version": "ask_dev_corpus_dispositions.v1",
        "note": (
            "A skipped case is not a failure and is not coverage. Every entry "
            "here states why it is not authored, and none of them claims a "
            "required corpus topic."
        ),
        "dispositions": [
            {
                "case_id": case.case_id,
                "disposition": case.disposition.value,
                "reason": case.disposition_reason,
                "note": case.note,
            }
            for case in dispositions_table()
        ],
    }
    return {
        "registries/cases.json": cases,
        "registries/oracles.json": oracles,
        "registries/coverage_matrix.json": coverage,
        "registries/dispositions.json": dispositions,
    }


def _witness_artifacts() -> dict[str, Any]:
    artifacts: dict[str, Any] = {}
    digests: list[dict[str, str]] = []
    for case in authored_cases():
        payload = reference_packet(case.case_id)
        contents = _json(payload)
        digests.append({"case_id": case.case_id, "sha256": _sha256(contents)})
        if case.case_id in FULL_WITNESS_CASE_IDS:
            artifacts[f"examples/reference/{case.case_id}.json"] = payload
    missing = sorted(set(FULL_WITNESS_CASE_IDS) - set(ALL_CASE_IDS))
    if missing:
        raise RuntimeError(f"full-witness case ids that do not exist: {missing}")
    artifacts["examples/reference_digests.json"] = {
        "schema_version": "ask_dev_corpus_reference_digests.v1",
        "note": (
            'Reproduce with: python -c "import json; from '
            "dev_health_ops.api.dev.investigation_corpus.reference import "
            "reference_packet; print(json.dumps(reference_packet(CASE), "
            'indent=2, sort_keys=True))". These are satisfiability witnesses, '
            "not arm output."
        ),
        "digests": digests,
    }
    return artifacts


def expected_artifacts() -> dict[str, str]:
    artifacts: dict[str, str] = {}
    for path, payload in {
        **_world_artifacts(),
        **_registry_artifacts(),
        **_witness_artifacts(),
    }.items():
        artifacts[path] = _json(payload)

    manifest = {
        "schema_version": "ask_dev_investigation_corpus_manifest.v1",
        "corpus_version": CORPUS_VERSION,
        "compatibility": "internal-trial-artifact-not-client-served",
        "artifact_root_note": (
            "A sibling of contracts/ask-dev-investigation/v1 rather than a "
            "member of it. That tree is the frozen CHAOS-3615 contract and its "
            "drift gate rejects any unexpected file, so corpus output there "
            "would couple every corpus iteration to the contract's freeze."
        ),
        "pinned_clock": {
            "world_epoch": _iso(WORLD_EPOCH),
            "window_start": _iso(WINDOW_START),
            "window_end": _iso(WINDOW_END),
            "trial_now": _iso(TRIAL_NOW),
        },
        "counts": {
            "entities": len(WORLD_ENTITIES),
            "relationships": len(WORLD_RELATIONSHIPS),
            "evidence_records": len(WORLD_EVIDENCE),
            "measurements": len(WORLD_MEASUREMENTS),
            "episodes": len(WORLD_EPISODES),
            "source_feeds": len(SOURCE_MANIFEST),
            "cases_total": len(ALL_CASE_IDS),
            "cases_authored": len(authored_cases()),
            "oracles": len(CASE_ORACLES),
            "required_topics": len(REQUIRED_CORPUS_TOPICS),
        },
        "validation_policy": {
            "canonical_validator": (
                "dev_health_ops.api.dev.investigation_contract.packet"
                ".AskDevInvestigationPacket"
            ),
            "schema_only_validation_is_sufficient": False,
            "note": (
                "The evaluation layer runs the canonical Pydantic validator on "
                "every packet before scoring it, per the contract manifest's "
                "own validation_policy. A consumer that schema-validates and "
                "stops has checked none of the cross-field rules."
            ),
        },
        "authorization_policy": {
            "producer_declaration_is_not_evidence": True,
            "note": (
                "ZERO_UNAUTHORIZED_RESULTS is scored against world/"
                "principals.json, not against the packet's own "
                "authorized_entity_ids. The contract proves internal "
                "consistency with that declaration; only the world can prove "
                "the declaration is true."
            ),
        },
        "files": [
            {"path": path, "sha256": _sha256(contents)}
            for path, contents in sorted(artifacts.items())
        ],
    }
    artifacts["manifest.json"] = _json(manifest)
    return artifacts


def _current_artifact_paths() -> set[str]:
    if not ARTIFACT_ROOT.exists():
        return set()
    return {
        str(path.relative_to(ARTIFACT_ROOT))
        for path in ARTIFACT_ROOT.rglob("*")
        if path.is_file()
    }


def write_artifacts(artifacts: dict[str, str]) -> None:
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    for relative_path, contents in artifacts.items():
        destination = ARTIFACT_ROOT / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(contents, encoding="utf-8")
    for stale in _current_artifact_paths() - set(artifacts):
        (ARTIFACT_ROOT / stale).unlink()


def check_artifacts(artifacts: dict[str, str]) -> None:
    actual_paths = _current_artifact_paths()
    expected_paths = set(artifacts)
    if actual_paths != expected_paths:
        missing = sorted(expected_paths - actual_paths)
        stale = sorted(actual_paths - expected_paths)
        raise RuntimeError(
            f"corpus artifact set drifted; missing={missing}, stale={stale}"
        )
    drifted = [
        relative_path
        for relative_path, expected in artifacts.items()
        if (ARTIFACT_ROOT / relative_path).read_text(encoding="utf-8") != expected
    ]
    if drifted:
        raise RuntimeError(f"corpus artifacts drifted: {sorted(drifted)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("write", "check"))
    args = parser.parse_args()
    artifacts = expected_artifacts()
    if args.mode == "write":
        write_artifacts(artifacts)
        print(f"wrote {len(artifacts)} Ask Dev investigation corpus artifacts")
    else:
        check_artifacts(artifacts)
        print(f"verified {len(artifacts)} Ask Dev investigation corpus artifacts")


if __name__ == "__main__":
    main()
