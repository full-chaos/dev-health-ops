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

import re
from collections.abc import Mapping
from dataclasses import dataclass

from ..investigation_contract import AskDevInvestigationPacket
from . import world

__all__ = [
    "AuthorizationAudit",
    "EntitySighting",
    "ProseSighting",
    "RestrictedVocabulary",
    "audit_authorization",
    "entity_sightings",
    "prose_sightings",
    "prose_sightings_in_text",
    "restricted_vocabulary",
]


@dataclass(frozen=True)
class EntitySighting:
    """One entity id, and every place in the packet it appears."""

    entity_id: str
    locations: tuple[str, ...]


@dataclass(frozen=True)
class ProseSighting:
    """One restricted token found in the packet's rendered form.

    CHAOS-3635 oracle v2. ``channel`` says what kind of thing leaked — an
    entity id, an evidence slug, a human-readable label — because the fix
    differs completely: an id in a prose field is a producer bug, a label in
    a ``display_label`` is a disclosure the producer thinks is legitimate.
    """

    channel: str
    token: str


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
    #: CHAOS-3635 oracle v2. Restricted ids, evidence slugs and human-readable
    #: names found anywhere in the packet's rendered form — including the
    #: prose channels (``display_label``, driver summaries, inclusion
    #: reasons, ``match_signals[].matched_text``, limitation details) and the
    #: id-bearing paths :func:`entity_sightings`' field list does not walk.
    #:
    #: **Additive.** Every field above is computed exactly as it was before
    #: this channel existed, so a packet that failed the v1 audit still fails
    #: it for the same reason. This can only turn a clean verdict unclean,
    #: which is the strengthening-only rule CHAOS-3635 was unfrozen under.
    prose_disclosures: tuple[ProseSighting, ...] = ()
    #: Names that WOULD be restricted but also occur in material the caller
    #: may legitimately see, so matching them would report a disclosure that
    #: is not one. Carried on the audit rather than dropped silently: this is
    #: the walker's own residual blind spot, and an unstated blind spot in a
    #: safety oracle is worse than a stated one.
    ambiguous_labels_not_matched: tuple[str, ...] = ()
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
            # CHAOS-3635 oracle v2, appended rather than woven in: every term
            # above is untouched, so this predicate can only move a verdict
            # from clean to unclean and never the reverse.
            or self.prose_disclosures
        )

    @property
    def is_clean_v1_channels_only(self) -> bool:
        """The pre-CHAOS-3635 verdict, kept so the change is auditable.

        Exists to make the strengthening-only claim checkable rather than
        asserted: ``is_clean`` implies this, never the other way round, and
        ``test_chaos_3635_oracle_v2`` proves the implication over the whole
        corpus plus the 3620 suite's planted counterexamples. Without it, a
        reader has to take "additive" on trust.
        """

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
        if self.prose_disclosures:
            parts.append(
                "prose-disclosures="
                f"{sorted(f'{item.channel}:{item.token}' for item in self.prose_disclosures)}"
            )
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


# ---------------------------------------------------------------------------
# CHAOS-3635 oracle v2: prose and label channels
# ---------------------------------------------------------------------------
#
# Ported from the CHAOS-3620 suite's own disclosure walker, which the ticket
# names as the reference implementation. It lives here now because this is the
# *owning* instrument: a safety property proved by a test module beside the
# arm under test is proved by something the next arm will not inherit.
#
# The 3620 module keeps its copy. That is deliberate rather than an oversight
# to tidy up later: it is that suite's acceptance set for this code, and a
# reference implementation that has been replaced by the thing it certifies
# is no longer a reference. ``test_chaos_3635_oracle_v2`` runs both over the
# same inputs and requires them to agree.


@dataclass(frozen=True)
class RestrictedVocabulary:
    """Everything a principal must not see, in every form it can appear as."""

    principal_id: str
    entity_ids: frozenset[str]
    evidence_slugs: frozenset[str]
    #: Names safe to match: restricted, and occurring nowhere in material the
    #: caller may legitimately read.
    labels: frozenset[str]
    #: Names that are restricted AND occur in permitted material, so matching
    #: them would report a disclosure that is not one. Reported, not dropped.
    ambiguous_labels: frozenset[str]


#: Identifier characters. A token counts only when it appears WHOLE.
#:
#: Two false positives this rules out, both found by running the CHAOS-3620
#: sweep rather than by reasoning about it: ``proj_quarry`` inside a
#: hypothetical ``proj_quarry_archive`` is a different entity, and the label
#: ``Ember`` — a team the Lumen principal cannot see — occurs inside the JSON
#: key ``"members"`` in every packet, so a plain substring search reported a
#: disclosure on four clean packets. A check that cries wolf on its own
#: serialization format is one people learn to ignore.
_TOKEN_BOUNDARY = re.compile(r"[A-Za-z0-9_-]")


def _contains_token(haystack: str, token: str) -> bool:
    """Whether ``token`` occurs in ``haystack`` whole, case-insensitively."""

    folded_haystack = haystack.casefold()
    folded_token = token.casefold()
    start = 0
    while True:
        index = folded_haystack.find(folded_token, start)
        if index < 0:
            return False
        after_index = index + len(folded_token)
        before = folded_haystack[index - 1] if index else ""
        after = (
            folded_haystack[after_index] if after_index < len(folded_haystack) else ""
        )
        if not _TOKEN_BOUNDARY.match(before or " ") and not _TOKEN_BOUNDARY.match(
            after or " "
        ):
            return True
        start = index + 1


def restricted_vocabulary(principal_id: str) -> RestrictedVocabulary:
    """What ``principal_id`` must not see, derived from the world's grants.

    Raises ``KeyError`` for an unknown principal, like
    :func:`audit_authorization`: the tempting default is "sees everything",
    which empties the restricted set and turns this into a no-op on exactly
    the inputs it exists to judge.
    """

    visible = world.PRINCIPALS[principal_id].visible_entity_ids

    entity_ids = frozenset(
        entity_id for entity_id in world.ENTITIES_BY_ID if entity_id not in visible
    )
    evidence_slugs = frozenset(
        slug
        for slug, record in world.EVIDENCE_BY_SLUG.items()
        if record.entity_id not in visible
    )

    # Everything the caller may legitimately read, as one corpus. A restricted
    # name occurring anywhere in here cannot be told apart from a permitted
    # mention of a permitted thing.
    permitted_text = "\n".join(
        text
        for text in (
            *(world.ENTITIES_BY_ID[entity_id].display_label for entity_id in visible),
            *(
                alias.text
                for entity_id in visible
                for alias in world.ENTITIES_BY_ID[entity_id].aliases
            ),
            *(
                record.display_label
                for slug, record in world.EVIDENCE_BY_SLUG.items()
                if slug not in evidence_slugs
            ),
        )
        if text
    )

    labels: set[str] = set()
    ambiguous: set[str] = set()
    for label in {
        *(world.ENTITIES_BY_ID[entity_id].display_label for entity_id in entity_ids),
        *(world.EVIDENCE_BY_SLUG[slug].display_label for slug in evidence_slugs),
    }:
        if not label:
            continue
        # Classified by the SAME whole-token predicate the matcher uses.
        # Classifying by substring while matching by token creates a blind
        # spot out of the exclusion rule itself: a label excluded for a
        # collision the matcher would never have made.
        (ambiguous if _contains_token(permitted_text, label) else labels).add(label)

    return RestrictedVocabulary(
        principal_id=principal_id,
        entity_ids=entity_ids,
        evidence_slugs=evidence_slugs,
        labels=frozenset(labels),
        ambiguous_labels=frozenset(ambiguous),
    )


def prose_sightings_in_text(
    rendered: str, principal_id: str, *, include_evidence_slugs: bool = True
) -> tuple[ProseSighting, ...]:
    """The same scan, over any text a consumer would receive.

    Split out from :func:`prose_sightings` so a producer that has not
    assembled a packet yet can still be measured — the CHAOS-3647 retrieval
    leg ranks subjects and never builds one, and "we could not check because
    there was no packet" is how a channel goes unmeasured until it leaks.

    ``include_evidence_slugs`` exists for callers whose text legitimately
    contains no evidence at all; leaving it on for them costs nothing but a
    reader's confusion about why an evidence channel is reported as clean on
    a surface that has none.
    """

    vocabulary = restricted_vocabulary(principal_id)
    found: list[ProseSighting] = [
        ProseSighting("entity_id", entity_id)
        for entity_id in sorted(vocabulary.entity_ids)
        if _contains_token(rendered, entity_id)
    ]
    if include_evidence_slugs:
        found += [
            ProseSighting("evidence_slug", slug)
            for slug in sorted(vocabulary.evidence_slugs)
            if _contains_token(rendered, slug)
        ]
    found += [
        ProseSighting("label", label)
        for label in sorted(vocabulary.labels)
        if _contains_token(rendered, label)
    ]
    return tuple(found)


def prose_sightings(
    packet: AskDevInvestigationPacket, principal_id: str
) -> tuple[ProseSighting, ...]:
    """Every restricted token anywhere in the packet a consumer receives.

    Searched over the packet's **own JSON rendering** rather than over a
    chosen list of fields. A field list is how a disclosure walker silently
    stops covering the field that later leaks — which is precisely what
    CHAOS-3635 was raised about: :func:`entity_sightings` walks the sections
    it knows, and seven id-bearing paths plus every prose channel were
    outside them.

    Ids and evidence slugs are matched as well as labels. They are not
    redundant with :func:`entity_sightings`: that function walks *structured
    id fields*, so a restricted id that arrives inside a driver summary or a
    truncation message is invisible to it and visible here.
    """

    return prose_sightings_in_text(packet.model_dump_json(), principal_id)


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

    vocabulary = restricted_vocabulary(principal_id)

    return AuthorizationAudit(
        case_id=case_id,
        principal_id=principal_id,
        prose_disclosures=prose_sightings(packet, principal_id),
        ambiguous_labels_not_matched=tuple(sorted(vocabulary.ambiguous_labels)),
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
