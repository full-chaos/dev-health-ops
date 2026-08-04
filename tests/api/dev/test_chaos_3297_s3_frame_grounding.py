"""Tests for CHAOS-3297 stack #3's F10 grounding floor
(``contracts_v2.validators.validate_frame_grounding``, ratified 2026-08-02):
every frame fact carries signer-minted evidence or an explicit
no-evidence classification (``fact.disclosures``).

The mutation-isolation coverage (disabling this ONE guard flips exactly its
own fixture, "fact_missing_grounding") lives in test_contracts_v2.py's
shared ``_FRAME_VALIDATOR_CASES`` harness. This file covers the finer
per-fact combinations that harness does not enumerate.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contracts_v2.embedded import DevCoverageV2, DevEvidenceRefV2
from dev_health_ops.api.dev.contracts_v2.frame import (
    DevAnswerFact,
    DevAnswerFrame,
    DevAnswerSection,
    DevFrameVersions,
)

_NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
_REAL_EVIDENCE_HANDLE = "ev1_" + ("a1b2c3d4e5" * 4)


def _evidence() -> DevEvidenceRefV2:
    return DevEvidenceRefV2(
        schema_version="dev_evidence_ref.v1",
        evidence_ref_id=_REAL_EVIDENCE_HANDLE,
        source_system="work_graph",
        source_version="work_graph.v1",
        entity_type="work_item",
        entity_id="item_01",
        display_label="Status snapshot",
        link={"internal_path": "/work/items/item_01", "source_url": None},
        observed_at=_NOW,
        freshness="fresh",
        provenance="Canonical work graph projection",
        confidence=1.0,
        citation_text="Repository status observed directly.",
        repository_ids=(),
        valid_entity_ids=(),
        flags={
            "stale": False,
            "unavailable": False,
            "redacted": False,
            "deleted": False,
            "uncertain": False,
            "conflicting": False,
            "untrusted_content": True,
        },
    )


def _fact(**overrides: object) -> DevAnswerFact:
    base: dict[str, object] = dict(
        fact_id="fact_01",
        text="One dimension is at risk.",
        kind="observed",
        evidence_ref_ids=(),
        relationship_path_ids=(),
        confidence=1.0,
        disclosures=(),
    )
    base.update(overrides)
    return DevAnswerFact(**base)


def _frame(*, facts: tuple[DevAnswerFact, ...]) -> DevAnswerFrame:
    cites_evidence = any(fact.evidence_ref_ids for fact in facts)
    return DevAnswerFrame(
        schema_version="dev_answer_frame.v1",
        frame_id="00000000-0000-0000-0000-0000000000f1",
        run_id="00000000-0000-0000-0000-0000000000f2",
        generated_at=_NOW,
        public_outcome="answered_with_gaps",
        direct_answer="Repo dev-health has one at-risk dimension.",
        completion=None,
        sections=(
            DevAnswerSection(
                section_id="summary",
                title="Summary",
                fact_ids=tuple(fact.fact_id for fact in facts),
            ),
        ),
        facts=facts,
        evidence=(_evidence(),) if cites_evidence else (),
        coverage=DevCoverageV2(
            required_source_count=1,
            available_source_count=1,
            unavailable_required_sources=(),
            stale_required_sources=(),
            as_of=_NOW,
        ),
        limitations=("Health rules are still provisional.",),
        versions=DevFrameVersions(
            interpreter_version="intent_interpreter.v1",
            plan_id="health.project.v1",
            plan_version="health.project.v1.0",
            tool_contract_version="ask_dev_tools.v1",
            metric_definition_version="ask_dev_metrics.v1",
            query_version="ask_dev_queries.v1",
        ),
    )


def test_fact_with_evidence_and_no_disclosure_passes() -> None:
    frame = _frame(facts=(_fact(evidence_ref_ids=(_REAL_EVIDENCE_HANDLE,)),))
    assert frame.facts[0].evidence_ref_ids == (_REAL_EVIDENCE_HANDLE,)


def test_fact_with_disclosure_and_no_evidence_passes() -> None:
    frame = _frame(facts=(_fact(disclosures=("untrusted_source",)),))
    assert frame.facts[0].disclosures == ("untrusted_source",)


def test_fact_with_both_evidence_and_disclosure_passes() -> None:
    frame = _frame(
        facts=(
            _fact(
                evidence_ref_ids=(_REAL_EVIDENCE_HANDLE,),
                disclosures=("stale",),
            ),
        )
    )
    assert frame.facts[0].evidence_ref_ids
    assert frame.facts[0].disclosures


def test_fact_with_neither_evidence_nor_disclosure_is_rejected() -> None:
    with pytest.raises(ValidationError, match="F10"):
        _frame(facts=(_fact(),))


def test_one_ungrounded_fact_among_several_grounded_ones_is_still_rejected() -> None:
    """A single bad fact must not be masked by its well-formed siblings."""

    with pytest.raises(ValidationError, match=r"F10.*fact_02"):
        _frame(
            facts=(
                _fact(fact_id="fact_01", evidence_ref_ids=(_REAL_EVIDENCE_HANDLE,)),
                _fact(fact_id="fact_02"),
                _fact(fact_id="fact_03", disclosures=("uncertain",)),
            )
        )
