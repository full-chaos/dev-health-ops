"""The real authorization oracle for ``ZERO_UNAUTHORIZED_RESULTS``.

CHAOS-3615 left this residual stated in its own source rather than glossed.
``RelatedContext.validate_paths_stay_inside_authorized_set`` and
``AskDevInvestigationPacket.validate_every_entity_is_authorized``
(``packet.py:843-878``, ``packet.py:1397-1444``) check the packet's contents
against ``related_context.authorized_entity_ids`` — **a field the producer
fills in**. They prove the traversal was consistent with the arm's own claim.
They cannot prove the claim is true, and both docstrings say so: an arm that
listed the whole organization as authorized passes every contract check.

This module holds the other half. :data:`world.PRINCIPALS` records what each
caller may actually see, so :func:`audit_authorization` can catch four things
the contract structurally cannot:

1. **A false authorization claim.** An id in ``authorized_entity_ids`` that
   the principal cannot see. This is the fault the contract is blind to, and
   it is the reason this module exists.
2. **An unauthorized disclosure.** An id reaching a consumer anywhere in the
   packet that the principal cannot see — including through a path that
   merely routes across it.
3. **A fabricated entity.** An id that exists nowhere in the world. The
   contract's closure checks are internal, so a packet that invents an
   entity, declares it authorized and cites it consistently validates
   cleanly.
4. **A fabricated or unauthorized evidence handle.** A handle the world never
   minted, or one whose subject the principal cannot see. Handle *grammar* is
   pinned by the contract; handle *existence* is not, and cannot be — the
   contract's own docstring notes that dereferencing is a runtime,
   org-scoped question.

The corpus world plants a restricted project **inside the caller's own
tenant** precisely so this oracle has to be more than a tenant comparison: no
organization check catches ``proj_quarry`` leaking to the analyst principal.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ..investigation_contract import AskDevInvestigationPacket
from . import world

__all__ = [
    "AuthorizationAudit",
    "EntitySighting",
    "audit_authorization",
    "entity_sightings",
]


@dataclass(frozen=True)
class EntitySighting:
    """One entity id, and every place in the packet it appears."""

    entity_id: str
    locations: tuple[str, ...]


@dataclass(frozen=True)
class AuthorizationAudit:
    """What a packet disclosed, measured against the world's true grants."""

    case_id: str
    principal_id: str
    #: Ids the packet *declared* authorized that the principal cannot see.
    #: The fault the frozen contract is structurally blind to.
    false_authorization_claims: tuple[str, ...] = ()
    #: Ids that reached a consumer and are outside the true grant.
    unauthorized_disclosures: tuple[EntitySighting, ...] = ()
    #: Ids that exist nowhere in the world.
    fabricated_entities: tuple[EntitySighting, ...] = ()
    #: Evidence handles the world never minted.
    fabricated_evidence_handles: tuple[str, ...] = ()
    #: Evidence handles about entities the principal cannot see.
    unauthorized_evidence_handles: tuple[str, ...] = ()
    #: Cited handles whose world record is revoked, redacted or deleted.
    withdrawn_evidence_handles: tuple[str, ...] = ()
    #: Set when the packet claims an organization the principal does not
    #: belong to. Adversarial review round 1: a clean Helio witness relabelled
    #: ``org_lumen`` stayed contract-valid, authorization-clean and
    #: dimension-clean, because the audit compared entity ids and never the
    #: tenant those ids were attributed to.
    tenant_mismatch: str = ""

    @property
    def is_clean(self) -> bool:
        return not (
            self.tenant_mismatch
            or self.false_authorization_claims
            or self.unauthorized_disclosures
            or self.fabricated_entities
            or self.fabricated_evidence_handles
            or self.unauthorized_evidence_handles
            or self.withdrawn_evidence_handles
        )

    def summary(self) -> str:
        if self.is_clean:
            return "clean"
        parts: list[str] = []
        if self.false_authorization_claims:
            parts.append(
                f"declared-but-not-authorized={sorted(self.false_authorization_claims)}"
            )
        if self.unauthorized_disclosures:
            parts.append(
                "disclosed-but-not-authorized="
                f"{sorted(item.entity_id for item in self.unauthorized_disclosures)}"
            )
        if self.fabricated_entities:
            parts.append(
                "entities-not-in-world="
                f"{sorted(item.entity_id for item in self.fabricated_entities)}"
            )
        if self.fabricated_evidence_handles:
            parts.append(
                f"handles-never-minted={sorted(self.fabricated_evidence_handles)}"
            )
        if self.unauthorized_evidence_handles:
            parts.append(
                f"handles-not-authorized={sorted(self.unauthorized_evidence_handles)}"
            )
        if self.withdrawn_evidence_handles:
            parts.append(f"handles-withdrawn={sorted(self.withdrawn_evidence_handles)}")
        if self.tenant_mismatch:
            parts.append(f"tenant-mismatch={self.tenant_mismatch}")
        return "; ".join(parts)


def entity_sightings(packet: AskDevInvestigationPacket) -> Mapping[str, set[str]]:
    """Every entity id in the packet, mapped to where it appears.

    Deliberately exhaustive rather than scoped to the sections a leak is
    "likely" to use. Adversarial review of the frozen contract found exactly
    that mistake — a guard that checked lineage only, while a cohort member,
    a driver's affected subject or an indexed evidence item could name
    anything. Each of those is an identifier reaching a consumer, which is
    the leak.
    """

    sightings: dict[str, set[str]] = {}

    def see(location: str, entity_id: str | None) -> None:
        if entity_id is None:
            return
        sightings.setdefault(entity_id, set()).add(location)

    # ``candidate_id`` is the packet's own local handle for a candidate, not a
    # canonical entity id. Resolving through the candidate table rather than
    # treating the two as interchangeable matters: the naive reading reports
    # every clarification packet as disclosing entities that do not exist,
    # which would bury real leaks under noise.
    canonical_by_candidate = {
        candidate.candidate_id: candidate.canonical_id
        for candidate in packet.subject_discovery.candidates
    }

    for candidate in packet.subject_discovery.candidates:
        see("subject_discovery.candidates", candidate.canonical_id)
    for subject_id in packet.subject_discovery.committed_subject_ids:
        see("subject_discovery.committed_subject_ids", subject_id)
    for mention in packet.subject_discovery.unresolved_mentions:
        for candidate_id in mention.candidate_ids:
            see(
                "subject_discovery.unresolved_mentions",
                canonical_by_candidate.get(candidate_id),
            )

    for member in packet.comparison_cohort.members:
        see("comparison_cohort.members", member.canonical_id)
    for exclusion in packet.comparison_cohort.exclusions:
        see("comparison_cohort.exclusions", exclusion.canonical_id)

    for entity in packet.related_context.entities:
        see("related_context.entities", entity.entity_id)
    for path in packet.related_context.paths:
        see("related_context.paths.origin", path.origin_entity_id)
        see("related_context.paths.terminal", path.terminal_entity_id)
        for hop in path.hops:
            see("related_context.paths.hops", hop.source_entity_id)
            see("related_context.paths.hops", hop.target_entity_id)

    for driver in packet.driver_analysis.candidates:
        for subject_id in driver.affected_subject_ids:
            see("driver_analysis.affected_subject_ids", subject_id)

    for entry in packet.evidence_coverage.evidence_index:
        see("evidence_coverage.evidence_index", entry.evidence.entity_id)
        for entity_id in entry.supports_entity_ids:
            see("evidence_coverage.supports_entity_ids", entity_id)
        for subject_id in entry.supports_subject_ids:
            see("evidence_coverage.supports_subject_ids", subject_id)

    for need in packet.evidence_coverage.clarification_needs:
        for candidate_id in need.candidate_ids:
            # A clarification may name a candidate handle or, for a candidate
            # the packet never ranked, a canonical id directly. Both are
            # resolved; an id that is neither is reported as-is, because an
            # unresolvable identifier reaching a user is itself a disclosure.
            resolved = canonical_by_candidate.get(candidate_id, candidate_id)
            see("evidence_coverage.clarification_needs", resolved)

    for ref in packet.analytical_job.surface_context_refs:
        see("analytical_job.surface_context_refs", ref.entity_id)

    return sightings


def _cited_handles(packet: AskDevInvestigationPacket) -> set[str]:
    handles: set[str] = set()
    for entry in packet.evidence_coverage.evidence_index:
        handles.add(entry.evidence.evidence_ref_id)
    for candidate in packet.subject_discovery.candidates:
        for signal in candidate.match_signals:
            handles.update(signal.evidence_ref_ids)
    for member in packet.comparison_cohort.members:
        handles.update(member.inclusion_evidence_ids)
    for path in packet.related_context.paths:
        handles.update(path.evidence_ref_ids)
    for driver in packet.driver_analysis.candidates:
        handles.update(driver.supporting_evidence_ids)
        handles.update(driver.conflicting_evidence_ids)
    for conflict in packet.evidence_coverage.conflicts:
        handles.update(conflict.evidence_ref_ids)
    return handles


def audit_authorization(
    packet: AskDevInvestigationPacket,
    principal_id: str,
    *,
    case_id: str = "",
) -> AuthorizationAudit:
    """Measure a packet against what ``principal_id`` may actually see.

    Raises ``KeyError`` for an unknown principal rather than defaulting.
    The tempting default here is "sees everything", which would turn this
    oracle into a no-op on precisely the inputs it exists to judge.
    """

    principal = world.PRINCIPALS[principal_id]
    visible = principal.visible_entity_ids
    known = set(world.ENTITIES_BY_ID)

    tenant_mismatch = ""
    if packet.organization_id != principal.tenant_id:
        tenant_mismatch = (
            f"packet claims {packet.organization_id}; principal "
            f"{principal_id} belongs to {principal.tenant_id}"
        )

    declared = set(packet.related_context.authorized_entity_ids)
    false_claims = tuple(sorted(declared - visible))

    sightings = entity_sightings(packet)
    unauthorized: list[EntitySighting] = []
    fabricated: list[EntitySighting] = []
    for entity_id in sorted(sightings):
        locations = tuple(sorted(sightings[entity_id]))
        if entity_id not in known:
            fabricated.append(EntitySighting(entity_id, locations))
            continue
        if entity_id not in visible:
            unauthorized.append(EntitySighting(entity_id, locations))

    fabricated_handles: list[str] = []
    unauthorized_handles: list[str] = []
    withdrawn_handles: list[str] = []
    for handle in sorted(_cited_handles(packet)):
        record = world.EVIDENCE_BY_HANDLE.get(handle)
        if record is None:
            fabricated_handles.append(handle)
            continue
        if record.entity_id not in visible:
            unauthorized_handles.append(handle)
        if record.state is not world.EvidenceState.ACTIVE:
            withdrawn_handles.append(handle)

    return AuthorizationAudit(
        case_id=case_id,
        principal_id=principal_id,
        tenant_mismatch=tenant_mismatch,
        false_authorization_claims=false_claims,
        unauthorized_disclosures=tuple(unauthorized),
        fabricated_entities=tuple(fabricated),
        fabricated_evidence_handles=tuple(fabricated_handles),
        unauthorized_evidence_handles=tuple(unauthorized_handles),
        withdrawn_evidence_handles=tuple(withdrawn_handles),
    )


#: Kept as a module-level constant so a reader of the coverage matrix can see
#: which dimension this module is the *independent* oracle for, rather than
#: inferring it from the docstring.
SCORES_DIMENSION = "zero_unauthorized_results"
