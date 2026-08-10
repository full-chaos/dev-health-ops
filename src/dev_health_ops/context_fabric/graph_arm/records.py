"""CHAOS-3617: the canonical structured records the graph arm ingests.

These are **not** wire contracts and deliberately do not derive from
``ContractModelV2``. They are the internal shape a reader hands the
projection: one frozen dataclass per ingestible thing, carrying canonical
Dev Health / ACR identifiers and nothing a model authored.

The rule this module exists to make structural, from the issue and repeated
verbatim in the corrective plan's anti-drift list:

    Structured provider and ACR episode records are ingested as structured
    episodes/facts. Do not convert structured records into hand-authored
    prose.

Every field below is a canonical identifier, a closed-vocabulary token, a
timestamp, or a count. There is no free-text ``narrative`` field, no
``summary`` an adapter would compose, and no field whose intended value is a
sentence about the record. ``display_label`` carries the source system's own
label and ``title`` carries the source system's own title — both copied, not
written. Unstructured material (documents, comments) is a *separate* path
with its own record type, :class:`UnstructuredDocumentRecord`, which is the
only record that carries a body and the only record model extraction may
ever see.

Identity: ``canonical_id`` is the Dev Health / ACR canonical identifier and
is the entity's identity everywhere downstream. The graph mints a UUID for
storage (``identity.node_uuid``) but that UUID is a *storage address*
derived from the canonical id, never a competing product identity — see
that module's docstring.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import RelationshipType

from .vocabulary import AliasKind, GraphEntityKind, GraphObservationKind

__all__ = [
    "AliasRecord",
    "CanonicalRef",
    "EntityRecord",
    "IngestionBatch",
    "ObservationRecord",
    "RelationshipRecord",
    "UnstructuredDocumentRecord",
]


@dataclass(frozen=True, slots=True)
class CanonicalRef:
    """A canonical entity reference: what kind of thing, and which one."""

    kind: GraphEntityKind
    canonical_id: str


@dataclass(frozen=True, slots=True)
class AliasRecord:
    """One alternative name a subject search may legitimately match on.

    ``value`` is the alternative name as the source system holds it. Aliases
    are what let "the auth work", "ACR" and a project's previous name resolve
    to a canonical subject without the arm falling back to fuzzy label
    matching — which the frozen contract refuses to accept as the sole basis
    for a commitment (``SubjectDiscovery.validate_commitment_is_evidenced``).
    """

    kind: AliasKind
    value: str
    #: The provider that asserts this alias, for provider identifiers and for
    #: provenance on the emitted match signal. ``None`` for aliases the
    #: platform itself holds.
    provider: str | None = None
    observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class EntityRecord:
    """A canonical entity: an organization, team, project, repo, work item…

    ``attributes`` is a mapping of closed-vocabulary keys to scalar values
    (str/int/float/bool/None) copied from the source record — declared
    status, provider key, archived flag. It is not an escape hatch for
    prose: :func:`projection.build_projection` rejects any attribute value
    that is a string longer than :data:`MAX_ATTRIBUTE_CHARS`, so an adapter
    cannot smuggle a paragraph through it.
    """

    org_id: str
    kind: GraphEntityKind
    canonical_id: str
    display_label: str
    source_class: SourceClass
    observed_at: datetime
    aliases: tuple[AliasRecord, ...] = ()
    attributes: Mapping[str, str | int | float | bool | None] = field(
        default_factory=dict
    )
    #: Repositories this entity is scoped to, when the platform scopes it that
    #: way. Carried through to the evidence ref's ``repository_ids`` so a
    #: repository-scoped authorization decision has something to bind to.
    repository_ids: tuple[str, ...] = ()
    valid_from: datetime | None = None
    valid_to: datetime | None = None


@dataclass(frozen=True, slots=True)
class RelationshipRecord:
    """One structured relationship between two canonical entities.

    ``relationship`` is a member of the frozen
    ``investigation_contract.RelationshipType`` allowlist, and ``source`` /
    ``target`` are stated in that type's **canonical** orientation — "the
    owned entity is the source; the owning team is the target". The
    projection refuses any record whose endpoint kinds contradict the
    allowlist's declared orientation, so a reversed relationship fails at
    ingestion rather than surfacing as a reversed hop the reader has to
    catch.

    ``contributor_count`` is how contribution and membership are expressed.
    It is an aggregate on the *association*, not a set of people: there is no
    person entity kind and adding one would be a contract change (see
    ``vocabulary``).
    """

    org_id: str
    source: CanonicalRef
    relationship: RelationshipType
    target: CanonicalRef
    source_class: SourceClass
    observed_at: datetime
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    #: Aggregate contributor count behind a contribution/membership edge.
    #: ``None`` when the edge is not a contribution edge or the count is
    #: unknown; ``0`` is a meaningful value and is not the same as ``None``.
    contributor_count: int | None = None
    #: Canonical ids of observations that evidence this relationship.
    observation_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ObservationRecord:
    """A structured event or artifact observed about one or more entities.

    Reviews, CI outcomes, deployments, releases, incidents, decisions, and
    the ACR agent episode/task/artifact/outcome family all arrive here. An
    observation is evidence, never a relationship endpoint.

    ``supersedes`` carries decision supersession: a decision record that
    replaces an earlier one names it here, which is what makes "which
    decision is current" answerable without a temporal query the trial
    datastore may not support.

    ``prior_attempt_ids`` carries the ACR prior-attempt chain for the same
    reason — an agent outcome that follows earlier attempts names them, so
    prior-attempt retrieval is a graph read rather than a heuristic over
    timestamps.
    """

    org_id: str
    kind: GraphObservationKind
    canonical_id: str
    title: str
    source_class: SourceClass
    observed_at: datetime
    #: Entities this observation is about. At least one; enforced by the
    #: projection, because an observation attached to nothing is exactly the
    #: unattached evidence ``InvestigationEvidenceEntry`` refuses to index.
    subjects: tuple[CanonicalRef, ...] = ()
    outcome: str | None = None
    attributes: Mapping[str, str | int | float | bool | None] = field(
        default_factory=dict
    )
    repository_ids: tuple[str, ...] = ()
    supersedes: tuple[str, ...] = ()
    prior_attempt_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class UnstructuredDocumentRecord:
    """An approved unstructured document or comment.

    The **only** record carrying a body, and therefore the only record model
    extraction may ever be pointed at. ``approved`` is not a courtesy field:
    the projection refuses to hand an unapproved document to extraction, so
    "model extraction is used only for approved unstructured material" is
    enforced rather than documented.

    Extraction itself is out of PR1's scope; this type exists now so the
    boundary is visible in the ingestion contract from the start and so the
    structured path can be proven not to touch it.

    ``attributes`` is the same closed-vocabulary, generic mapping
    :class:`EntityRecord`/:class:`ObservationRecord` already carry (CHAOS-3632)
    -- deliberately not a dedicated trust/evidence field. Trust and
    provenance signals a document needs (``corpus_trust``, ``corpus_state``,
    ...) already exist in :data:`~.projection.READBACK_ATTRIBUTE_KEYS`;
    adding a parallel, document-specific vocabulary for the same concepts
    would be the second hand-maintained implementation of one idea this
    project's own corrective plan forbids.
    """

    org_id: str
    canonical_id: str
    title: str
    body: str
    source_class: SourceClass
    observed_at: datetime
    subjects: tuple[CanonicalRef, ...] = ()
    approved: bool = False
    repository_ids: tuple[str, ...] = ()
    attributes: Mapping[str, str | int | float | bool | None] = field(
        default_factory=dict
    )


@dataclass(frozen=True, slots=True)
class IngestionBatch:
    """Everything one projection run was handed, for one organization.

    A batch is org-homogeneous by construction: :func:`validate_batch_org`
    rejects any member whose ``org_id`` differs from the batch's, which is
    the first of the three places a cross-tenant record is stopped (the
    others are partition derivation in ``identity`` and the authorized-set
    filter at read time).
    """

    org_id: str
    entities: tuple[EntityRecord, ...] = ()
    relationships: tuple[RelationshipRecord, ...] = ()
    observations: tuple[ObservationRecord, ...] = ()
    documents: tuple[UnstructuredDocumentRecord, ...] = ()

    def record_count(self) -> int:
        return (
            len(self.entities)
            + len(self.relationships)
            + len(self.observations)
            + len(self.documents)
        )


def _foreign(items: Sequence[object], org_id: str) -> list[str]:
    foreign: list[str] = []
    for item in items:
        item_org = getattr(item, "org_id", None)
        if item_org != org_id:
            label = getattr(item, "canonical_id", None) or repr(item)
            foreign.append(f"{label}(org={item_org!r})")
    return foreign


def validate_batch_org(batch: IngestionBatch) -> None:
    """Raise if any record in the batch belongs to another organization.

    Checked here, before anything is written, because a record that reaches
    the store carrying a foreign ``org_id`` has already crossed the tenant
    boundary — filtering it at read time would be closing the door after the
    fact, and the read filter's own guard is producer-declared.
    """

    foreign = (
        _foreign(batch.entities, batch.org_id)
        + _foreign(batch.relationships, batch.org_id)
        + _foreign(batch.observations, batch.org_id)
        + _foreign(batch.documents, batch.org_id)
    )
    if foreign:
        raise ValueError(
            f"ingestion batch for org {batch.org_id!r} carries records "
            f"belonging to other organizations: {sorted(foreign)}"
        )
