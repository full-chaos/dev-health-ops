"""CHAOS-3617 PR2: read the CHAOS-3616 corpus world into the arm.

Two jobs, and the boundary between them is the point of the module.

**Ingestion.** ``world``'s entities, relationships, evidence and documents
become an :class:`~.records.IngestionBatch` the arm already knows how to
project. Nothing here is arm-specific cleverness: the corpus already models
the same shapes the arm's record types do, because both were built against
the same frozen contract.

**Authorization.** :func:`authorized_entity_ids_for` derives the caller's
visible set from ``world.PRINCIPALS[...].visible_entity_ids`` — the true
per-principal grant — rather than from tenancy. That distinction is the
whole reason the corpus has a *same-tenant restricted project*: an arm that
authorizes by tenant returns ``proj_quarry`` to the analyst principal, and no
tenant-level check anywhere catches it. This function is what
``readback.derive_authorized_entity_ids`` was a placeholder for, scoped to
the trial world.

**What this module deliberately does not read.** ``world`` only. Not
``oracles``, not ``cases``, not ``evaluate``, not ``reference``. The arm must
not be able to see what it is scored against, and the cheapest way to
guarantee that is for the import to be absent —
``test_chaos_3617_corpus_adapter.py`` asserts it stays absent.

**Evidence handles come from the world.** Never rebuilt here, never minted by
the arm's own signer: ``world.evidence_handle(slug)`` is the corpus's sole
mint, and a handle this adapter constructed itself would be a handle no
oracle could match.

CHAOS-3627 made that sentence true. It was written when this adapter was
authored and it described an intention, not the code: the handle was read
nowhere, so the packet builder re-signed one of its own for every
observation and the frozen oracle reported all 31 as fabricated. Each
corpus-originated observation now carries the source-issued handle in
:data:`SOURCE_EVIDENCE_HANDLE_ATTRIBUTE`, alongside
:data:`SOURCE_EVIDENCE_ID_ATTRIBUTE` -- the canonical id the source issued
that handle **for**. The pair travels through the projection and both
readers as declared readback attributes, and the builder cites what it was
issued.

The second half of the pair is what makes a *measurement* citable. A
canonical measurement is not itself an evidence record; the world names the
record that evidences it (``WorldMeasurement.evidence_slug``), so the
measurement carries that record's handle and that record's id. Two
observations may therefore carry the same handle -- which is the truth about
them: one source record, projected twice.
"""

from __future__ import annotations

from dev_health_ops.api.dev.investigation_corpus import world

from .records import (
    AliasRecord,
    CanonicalRef,
    EntityRecord,
    IngestionBatch,
    ObservationRecord,
    RelationshipRecord,
    UnstructuredDocumentRecord,
)
from .vocabulary import (
    SOURCE_EVIDENCE_ENTITY_ATTRIBUTE,
    SOURCE_EVIDENCE_HANDLE_ATTRIBUTE,
    SOURCE_EVIDENCE_ID_ATTRIBUTE,
    AliasKind,
    GraphEntityKind,
    GraphObservationKind,
)

__all__ = [
    "CORPUS_VERSION",
    "authorized_entity_ids_for",
    "corpus_batch",
    "seed_ids_for_tenant",
]

#: Re-exported so a caller records the corpus version on the packet without
#: importing ``world`` itself.
CORPUS_VERSION = world.CORPUS_VERSION

#: How the corpus spells an alias signal -> how the arm spells an alias kind.
#: Total over the signals the world actually plants; an unmapped signal is a
#: hard failure rather than a silent drop, because a lost alias is a subject
#: the arm then cannot resolve and nobody can see why.
_ALIAS_KIND = {
    "alias": AliasKind.ALIAS,
    "acronym": AliasKind.ACRONYM,
    "previous_name": AliasKind.PREVIOUS_NAME,
    "provider_identifier": AliasKind.PROVIDER_IDENTIFIER,
}


def authorized_entity_ids_for(principal_id: str) -> frozenset[str]:
    """The entities ``principal_id`` may actually see, per the world's grants.

    Derived from the principal, never from the tenant. The corpus plants a
    restricted project inside the caller's own tenant precisely so that a
    tenant-derived set looks correct and is not: it would return
    ``proj_quarry`` to the analyst, and the only check that catches that is
    one that knows the true grant.

    Raises ``KeyError`` for an unknown principal — the same refusal
    ``world.authorized_entity_ids`` makes, and for the same reason: the
    tempting default is "sees everything", which turns an authorization
    boundary into a no-op on exactly the inputs it exists to bound.
    """

    return world.authorized_entity_ids(principal_id)


def seed_ids_for_tenant(tenant_id: str) -> tuple[str, ...]:
    """Every canonical entity id in one tenant, sorted.

    A convenience for tests and reproductions that need a starting set. It
    is deliberately *not* an authorization set — it is every entity in the
    tenant, including ones no principal may see.
    """

    return tuple(
        sorted(
            entity_id
            for entity_id, entity in world.ENTITIES_BY_ID.items()
            if entity.tenant_id == tenant_id
        )
    )


def _alias_records(entity: world.WorldEntity) -> tuple[AliasRecord, ...]:
    records: list[AliasRecord] = []
    for alias in entity.aliases:
        signal = str(alias.signal)
        kind = _ALIAS_KIND.get(signal)
        if kind is None:
            # Not a drop. An alias the arm cannot represent is a subject it
            # will fail to resolve, and a silent skip here would surface as
            # an unexplained recall miss much later.
            raise ValueError(
                f"corpus alias {alias.text!r} on {entity.entity_id} carries "
                f"signal {signal!r}, which the arm has no alias kind for. Add "
                "one to the vocabulary rather than dropping the alias"
            )
        records.append(AliasRecord(kind=kind, value=alias.text))
    return tuple(records)


def corpus_batch(tenant_id: str) -> IngestionBatch:
    """Everything in one tenant of the corpus world, as an ingestion batch.

    Tenant-scoped because :class:`IngestionBatch` is org-homogeneous by
    construction — and because ingesting both tenants into one batch would
    be the cross-tenant mistake the world exists to detect, made by the
    adapter rather than by the graph.

    **False-claim edges and adversarial evidence are ingested, not filtered.**
    The world plants them precisely so a correct arm can be seen *not* citing
    them; an adapter that dropped them at the door would make every
    poisoned-linkage expectation pass without the arm doing anything. They
    are carried with their world flags so a later capability can decide, and
    so the decision is visible.
    """

    entities: list[EntityRecord] = []
    for entity_id, entity in sorted(world.ENTITIES_BY_ID.items()):
        if entity.tenant_id != tenant_id:
            continue
        attributes: dict[str, str | int | float | bool | None] = {
            "corpus_state": str(entity.state),
        }
        if entity.declared_status is not None:
            attributes["declared_status"] = entity.declared_status
        if entity.superseded_by is not None:
            attributes["superseded_by"] = entity.superseded_by
        entities.append(
            EntityRecord(
                org_id=tenant_id,
                kind=GraphEntityKind(str(entity.kind)),
                canonical_id=entity_id,
                display_label=entity.display_label,
                source_class=world.SourceClass.WORK_GRAPH,
                observed_at=entity.observed_at,
                aliases=_alias_records(entity),
                attributes=attributes,
                valid_from=entity.valid_from,
                valid_to=entity.valid_to,
            )
        )

    known = {record.canonical_id for record in entities}

    relationships: list[RelationshipRecord] = []
    for edge in sorted(
        world.RELATIONSHIPS_BY_KEY.values(), key=lambda item: item.relationship_key
    ):
        if edge.tenant_id != tenant_id:
            continue
        if edge.source_entity_id not in known or edge.target_entity_id not in known:
            continue
        relationships.append(
            RelationshipRecord(
                org_id=tenant_id,
                source=CanonicalRef(
                    kind=GraphEntityKind(
                        str(world.ENTITIES_BY_ID[edge.source_entity_id].kind)
                    ),
                    canonical_id=edge.source_entity_id,
                ),
                relationship=edge.relationship,
                target=CanonicalRef(
                    kind=GraphEntityKind(
                        str(world.ENTITIES_BY_ID[edge.target_entity_id].kind)
                    ),
                    canonical_id=edge.target_entity_id,
                ),
                source_class=world.SourceClass.WORK_GRAPH,
                observed_at=edge.observed_at,
                valid_from=edge.valid_from,
                valid_to=edge.valid_to,
                observation_ids=tuple(sorted(edge.evidence_slugs)),
            )
        )

    observations: list[ObservationRecord] = []
    for slug, evidence in sorted(world.EVIDENCE_BY_SLUG.items()):
        if evidence.tenant_id != tenant_id or evidence.entity_id not in known:
            continue
        subject = world.ENTITIES_BY_ID[evidence.entity_id]
        observations.append(
            ObservationRecord(
                org_id=tenant_id,
                kind=_observation_kind(evidence.source_class),
                canonical_id=slug,
                title=evidence.display_label,
                source_class=evidence.source_class,
                observed_at=evidence.observed_at,
                subjects=(
                    CanonicalRef(
                        kind=GraphEntityKind(str(subject.kind)),
                        canonical_id=evidence.entity_id,
                    ),
                ),
                outcome=str(evidence.state),
                attributes={
                    "corpus_trust": str(evidence.trust),
                    "corpus_is_adversarial": evidence.is_adversarial,
                    # The world's own mint, carried verbatim. This record IS
                    # the thing the handle was issued for, so the id half of
                    # the pair is its own slug.
                    SOURCE_EVIDENCE_HANDLE_ATTRIBUTE: evidence.handle,
                    SOURCE_EVIDENCE_ID_ATTRIBUTE: slug,
                    SOURCE_EVIDENCE_ENTITY_ATTRIBUTE: evidence.entity_id,
                },
            )
        )

    # ---- canonical measurements ------------------------------------------
    #
    # Ingested as observations so the number lives INSIDE the trial
    # partition: authorized like everything else, deletable with the
    # keyspace, and citable through the same evidence handle. A measurement
    # read from outside the graph at query time would escape all three.
    #
    # Every field is carried VERBATIM. The arm's job with a canonical
    # service's number is to cite it, and a value this adapter rounded,
    # scaled or summarised would be the arm measuring rather than citing --
    # the exact fault the correction exists to prevent.
    for measurement in sorted(
        world.WORLD_MEASUREMENTS, key=lambda item: item.measurement_key
    ):
        if measurement.tenant_id != tenant_id or measurement.entity_id not in known:
            continue
        subject = world.ENTITIES_BY_ID[measurement.entity_id]
        # A measurement is not itself an evidence record; the world names the
        # record that evidences the number. Refuse rather than default: a
        # measurement whose named record does not exist is a number with no
        # citable source, and minting a handle for it here would be the arm
        # asserting a provenance the corpus never issued.
        backing = world.EVIDENCE_BY_SLUG.get(measurement.evidence_slug)
        if backing is None:
            raise ValueError(
                f"corpus measurement {measurement.measurement_key} names "
                f"evidence {measurement.evidence_slug!r}, which the world "
                "never minted. A canonical number the arm cannot attribute "
                "to a source record must not be ingested as citable evidence"
            )
        measured: dict[str, str | int | float | bool | None] = {
            "measurement_metric": measurement.metric,
            "measurement_value": measurement.value,
            "measurement_unit": measurement.unit,
            "measurement_basis": str(measurement.basis),
            "measurement_evidence_slug": measurement.evidence_slug,
            # The handle belongs to the record, not to the number. Carrying
            # the id alongside it is what lets the builder tell "this
            # observation is the record" from "this observation cites it" --
            # and the world's record may be about a DIFFERENT entity than the
            # measurement (an incident on a service backing a team's incident
            # load), which is exactly the distinction the oracle reads.
            SOURCE_EVIDENCE_HANDLE_ATTRIBUTE: backing.handle,
            SOURCE_EVIDENCE_ID_ATTRIBUTE: measurement.evidence_slug,
            # The entity the RECORD is about, which is routinely NOT the
            # entity the measurement is about: an incident on a service backs
            # a team's incident load. Describing the entry with the
            # measurement's subject while carrying the record's handle is the
            # mis-attribution both reviewers measured.
            SOURCE_EVIDENCE_ENTITY_ATTRIBUTE: backing.entity_id,
        }
        if measurement.cohort_median is not None:
            measured["measurement_cohort_median"] = measurement.cohort_median
        observations.append(
            ObservationRecord(
                org_id=tenant_id,
                kind=GraphObservationKind.MEASUREMENT,
                canonical_id=measurement.measurement_key,
                title=measurement.metric,
                source_class=measurement.source_class,
                observed_at=measurement.window_end,
                subjects=(
                    CanonicalRef(
                        kind=GraphEntityKind(str(subject.kind)),
                        canonical_id=measurement.entity_id,
                    ),
                ),
                attributes={
                    **measured,
                    # A measurement inherits the trust of the service that
                    # produced it. Stated rather than defaulted, because the
                    # driver module refuses to attribute on a record whose
                    # trust it cannot read.
                    "corpus_trust": (
                        "canonical"
                        if str(measurement.basis) == "canonical_service"
                        else "provider_asserted"
                    ),
                },
            )
        )

    documents: list[UnstructuredDocumentRecord] = []
    for document in sorted(world.WORLD_DOCUMENTS, key=lambda item: item.document_id):
        if document.tenant_id != tenant_id or document.about_entity_id not in known:
            continue
        subject = world.ENTITIES_BY_ID[document.about_entity_id]
        documents.append(
            UnstructuredDocumentRecord(
                org_id=tenant_id,
                canonical_id=document.document_id,
                title=document.title,
                body=document.body,
                source_class=world.SourceClass.WORK_GRAPH,
                observed_at=document.observed_at,
                subjects=(
                    CanonicalRef(
                        kind=GraphEntityKind(str(subject.kind)),
                        canonical_id=document.about_entity_id,
                    ),
                ),
                approved=_document_is_approved(document),
            )
        )

    return IngestionBatch(
        org_id=tenant_id,
        entities=tuple(entities),
        relationships=tuple(relationships),
        observations=tuple(observations),
        documents=tuple(documents),
    )


def _document_is_approved(document: world.WorldDocument) -> bool:
    """Whether model extraction may be pointed at this document.

    The corpus has no ``is_approved`` field — it models the same idea as
    ``trust`` plus ``contains_injection``, so the mapping is written out
    rather than guessed. The first draft of this adapter used
    ``getattr(document, "is_approved", False)``, which would have marked
    every corpus document unapproved *by accident* and made every
    extraction-path test pass without extraction ever being attempted. A
    default that silently absorbs a modelling mismatch is the exact failure
    this whole lane keeps finding.

    Today the answer is always ``False``: every corpus document is
    ``UNTRUSTED_CONTENT``, which is precisely what "not approved for
    extraction" means. That is written as a mapping rather than a constant so
    that a corpus which later plants a trusted document starts flowing
    through the approved path without anyone remembering to come back here —
    and so an unknown trust level raises instead of defaulting.
    """

    if document.contains_injection:
        # Belt and braces: injected content is never approved regardless of
        # how it is labelled, because approval is what points a model at it.
        return False
    trust = str(document.trust)
    approved_by_trust = {
        "canonical": True,
        "provider_asserted": True,
        "untrusted_content": False,
    }
    if trust not in approved_by_trust:
        raise ValueError(
            f"corpus document {document.document_id} carries trust {trust!r}, "
            "which the arm has no approval rule for. Decide it explicitly: a "
            "default here silently decides whether a model reads this text"
        )
    return approved_by_trust[trust]


#: Source class -> the observation kind the arm files it under. Explicit
#: rather than inferred: an unmapped source class raises, because filing
#: evidence under the wrong kind is invisible in the packet (both render as
#: an evidence entry) and only shows up as a capability that cannot find it.
_OBSERVATION_KIND = {
    "status_change": GraphObservationKind.STATUS_CHANGE,
    "work_item": GraphObservationKind.AGENT_TASK,
    "work_graph": GraphObservationKind.DECISION,
    "pull_request": GraphObservationKind.REVIEW,
    "code_change": GraphObservationKind.COMMIT,
    "review": GraphObservationKind.REVIEW,
    "ci_run": GraphObservationKind.CI_RUN,
    "test_report": GraphObservationKind.TEST_REPORT,
    "deployment": GraphObservationKind.DEPLOYMENT,
    "incident": GraphObservationKind.INCIDENT,
    "operational_control": GraphObservationKind.INCIDENT,
    "source_health": GraphObservationKind.STATUS_CHANGE,
    "cognitive_load": GraphObservationKind.AGENT_OUTCOME,
    "investment_allocation": GraphObservationKind.AGENT_OUTCOME,
    "health_profile": GraphObservationKind.AGENT_OUTCOME,
    "deficiency_inventory": GraphObservationKind.AGENT_ARTIFACT,
}


def _observation_kind(source_class: object) -> GraphObservationKind:
    kind = _OBSERVATION_KIND.get(str(source_class))
    if kind is None:
        raise ValueError(
            f"corpus source class {source_class!r} has no observation kind in "
            "the arm. Filing it under a wrong-but-valid kind would be "
            "invisible in the packet and would surface only as a capability "
            "that cannot find its own evidence"
        )
    return kind
