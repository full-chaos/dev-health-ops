"""CHAOS-3502 increment 2: the candidate-extraction leg of the packet->frame
bridge (``graph_evidence_admission.extract_evidence_candidates``).
"""

from __future__ import annotations

from copy import deepcopy

from dev_health_ops.api.dev.evidence_service import EvidenceCandidate
from dev_health_ops.api.dev.graph_evidence_admission import (
    extract_evidence_candidates,
)
from dev_health_ops.api.dev.investigation_contract import AskDevInvestigationPacket
from dev_health_ops.api.dev.investigation_contract.fixtures import positive_fixtures


def _packet() -> AskDevInvestigationPacket:
    payload = deepcopy(positive_fixtures()["ask_dev_investigation_packet.v1"])
    return AskDevInvestigationPacket.model_validate(payload)


def test_one_candidate_per_evidence_index_entry_in_order() -> None:
    packet = _packet()
    candidates = extract_evidence_candidates(packet)
    assert len(candidates) == len(packet.evidence_coverage.evidence_index)
    for candidate, entry in zip(
        candidates, packet.evidence_coverage.evidence_index, strict=True
    ):
        assert isinstance(candidate, EvidenceCandidate)
        assert candidate.source_system == entry.evidence.source_system
        assert candidate.entity_type == entry.evidence.entity_type
        assert candidate.entity_id == entry.evidence.entity_id
        assert candidate.repository_ids == tuple(entry.evidence.repository_ids)


def test_a_missing_record_locator_becomes_an_empty_string_not_entity_id() -> None:
    """CHAOS-3633 guardrail: an absent locator must never be coerced to
    ``entity_id`` (that would silently re-collapse two same-kind records
    about one entity back onto each other) and must never be dropped
    (that would silently narrow the candidate set). It becomes an empty
    string -- a locator no real resolver record will match, so admission
    refuses it through its ordinary "no record at this address" path
    rather than a fabricated success or a raised exception.
    """

    packet = _packet()
    assert all(
        entry.evidence.record_locator is None
        for entry in packet.evidence_coverage.evidence_index
    ), "fixture assumption changed -- this test needs a record_locator=None entry"

    candidates = extract_evidence_candidates(packet)
    assert candidates
    for candidate, entry in zip(
        candidates, packet.evidence_coverage.evidence_index, strict=True
    ):
        assert candidate.locator == ""
        assert candidate.locator != entry.evidence.entity_id


def test_a_present_record_locator_passes_through_unchanged() -> None:
    payload = deepcopy(positive_fixtures()["ask_dev_investigation_packet.v1"])
    payload["evidence_coverage"]["evidence_index"][0]["evidence"]["record_locator"] = (
        "review_platform:pr_4821:review_2"
    )
    packet = AskDevInvestigationPacket.model_validate(payload)

    candidates = extract_evidence_candidates(packet)
    assert candidates[0].locator == "review_platform:pr_4821:review_2"


def test_extraction_is_purely_derived_and_mints_nothing() -> None:
    """The candidate carries no evidence_ref_id, display_label, citation_text,
    observed_at, confidence, provenance, or freshness -- exactly the CHAOS-3646
    guarantee (see EvidenceCandidate's own docstring): the graph proposes, it
    never mints authority, and the dataclass has nowhere to put a handle even
    if this function tried to smuggle one through.
    """

    packet = _packet()
    candidates = extract_evidence_candidates(packet)
    for candidate in candidates:
        assert not hasattr(candidate, "evidence_ref_id")
        assert not hasattr(candidate, "display_label")
        assert not hasattr(candidate, "confidence")
