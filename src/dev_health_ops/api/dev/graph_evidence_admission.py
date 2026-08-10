"""CHAOS-3502 increment 2: the candidate-extraction leg of the packet->frame bridge.

Ruled on CHAOS-3660 (orchestrator, integration-seam decision): "Lane B
delivers: the assembler -- extract candidates from AskDevInvestigationPacket,
call C's admission service, assemble contracts_v2 DevInvestigationResult/
DevAnswerFrame from admitted results + enrichment." This module is the first
half of that: extraction only. It does not call
:meth:`~.evidence_service.EvidenceService.admit` itself (that is an
orchestrator-owned, request-scoped call needing ``org_id``/
``permission_fingerprint``/``scope_request``) -- it only turns a packet's
proposed evidence into the exact input shape ``admit()`` needs.

**Why this reuses the real ``EvidenceCandidate``/``EvidenceService`` types
rather than a fake.** Unlike the graph query seam (Lane A's CHAOS-3500,
genuinely not built yet) or the canonical-admission-result contract (Lane
C's CHAOS-3664, still landing), ``evidence_service.EvidenceCandidate`` and
``EvidenceService.admit()`` are *already* real, stable, production contracts
(CHAOS-3646, merged). Building a parallel interface here would be exactly
the "duplicate hand-maintained schema" the handoff's §8 forbids. What is
still incomplete is *behind* ``EvidenceService`` (``candidate_resolvers`` for
graph-sourced candidates specifically -- Lane C's CHAOS-3664/3675), which
this module does not need to see: an unconfigured resolver already produces
an honest ``EvidenceAvailability.UNCONFIGURED`` refusal per candidate, not a
crash or a fabricated success.

**Why every candidate needs ``record_locator`` (CHAOS-3633).** A packet's
``InvestigationEvidenceEntry.evidence: DevEvidenceRefV2`` was minted by the
graph arm's own (pre-authorization) handle service when the packet was
built -- a *proposed* identity, not an admitted one
(``investigation_contract``'s own docstring: "the graph proposes; canonical
services verify"). ``EvidenceCandidate.locator`` is the field
``EvidenceService.admit()`` re-resolves against, distinct from
``entity_id`` -- the exact distinction CHAOS-3633 exists to preserve: two
same-kind records about one entity are two records, not one just because
they share an ``entity_id``. A packet entry with no ``record_locator`` (pre-
CHAOS-3633 graph-arm code, or a source the arm cannot address a specific
record within) produces a candidate with an EMPTY locator string --
deliberately not coerced to ``entity_id`` or silently dropped, either of
which would reintroduce the exact collision CHAOS-3633 closed. An empty
locator is not a special case here: it is simply a locator no configured
resolver's records will match, so ``admit()`` refuses it through its
ordinary "no record at this address" path.
"""

from __future__ import annotations

from .evidence_service import EvidenceCandidate
from .investigation_contract import AskDevInvestigationPacket

__all__ = ["extract_evidence_candidates"]


def extract_evidence_candidates(
    packet: AskDevInvestigationPacket,
) -> tuple[EvidenceCandidate, ...]:
    """One :class:`EvidenceCandidate` per packet evidence-index entry, in
    the packet's own order.

    Deliberately 1:1 and order-preserving -- the caller (the increment-2
    assembler, not yet built) must be able to line up
    ``EvidenceAdmissionResult.admissions`` against
    ``packet.evidence_coverage.evidence_index`` positionally to know which
    refused candidate corresponds to which packet claim, exactly as
    :class:`~.evidence_service.EvidenceAdmissionResult`'s own docstring
    requires ("one entry per candidate, always").
    """

    return tuple(
        EvidenceCandidate(
            source_system=entry.evidence.source_system,
            entity_type=entry.evidence.entity_type,
            entity_id=entry.evidence.entity_id,
            locator=entry.evidence.record_locator or "",
            repository_ids=tuple(entry.evidence.repository_ids),
        )
        for entry in packet.evidence_coverage.evidence_index
    )
