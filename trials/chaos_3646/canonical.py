"""CHAOS-3646: the canonical evidence service, over the corpus world.

The admission path in ``api.dev.evidence_service`` is source-agnostic by
construction: it resolves a candidate through whatever resolver is registered
for its ``source_system``. This module supplies the one the measurement needs
-- a resolver that reads the **CHAOS-3616 world**, which is the same world the
graph arm's projection was built from, but read through the canonical
service's own path rather than through the arm's copy of it.

Three substitutions are made and every one of them is a boundary on what the
measurement can claim. They are named here rather than in a footnote:

1. **The resolver reads the world, not ClickHouse.** No ``native_evidence``
   adapter is exercised, so nothing measured says how a real source would
   resolve a locator.
2. **The mint is the world's.** ``world.evidence_handle(slug)`` is the
   corpus's sole mint (``world.py:158``, which documents exactly why the
   corpus cannot key the platform HMAC), and the FROZEN authorization oracle
   audits cited handles against it. A platform-HMAC handle would be scored as
   a handle the world never minted, so the trial's canonical service signs
   with the world's mint. This is a property of the corpus, not of the
   admission path.
3. **The scope resolution is built from the world's own grant.** The real
   ``ScopeResolutionService`` needs a tenant catalog this trial has no
   business standing up. The grant comes from ``world.PRINCIPALS`` -- the
   same source ``corpus_adapter.authorized_entity_ids_for`` reads for the
   arm, so neither side is handed a grant the other cannot see.

What is NOT substituted: ``EvidenceService.admit`` itself, its entitlement
call, its per-candidate re-resolution, ``_to_ref``, and
``_authorize_expansion``. The decision under measurement is the real one.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC

from dev_health_ops.api.dev.contracts import FreshnessState
from dev_health_ops.api.dev.evidence_service import (
    EvidenceCandidate,
    EvidenceCandidateResolver,
    EvidenceRecord,
    EvidenceReferenceSigner,
    EvidenceService,
    SignedIdentity,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ResolvedTimeRange,
    ScopeResolution,
    ScopeResolutionOutcome,
    ScopeResolveRequest,
)
from dev_health_ops.context_fabric.graph_arm.admission import ARM_SOURCE_SYSTEM

__all__ = [
    "CORPUS_SOURCE_VERSION",
    "CorpusCandidateResolver",
    "CorpusEvidenceSigner",
    "CorpusScopeAuthorizer",
    "corpus_evidence_service",
]

#: Stamped on every admitted ref. Names the world and its version, so a
#: reader can tell an admitted corpus record from a production one at a
#: glance and cannot mistake this artifact for a production measurement.
CORPUS_SOURCE_VERSION = f"investigation-corpus/{world.CORPUS_VERSION}"

#: Kinds the world uses that the scope vocabulary has no member for. They are
#: still granted and still admissible -- the CHAOS-3619 trial recorded the
#: same nine-entity vocabulary gap on the native arm -- so they are mapped to
#: PROJECT for the purpose of the resolution's entity list rather than
#: dropped. Dropping them would silently narrow the grant and make every
#: refusal about them read as an authorization decision.
_KIND_FALLBACK = EntityKind.PROJECT

_KIND_BY_WORLD_KIND = {
    "project": EntityKind.PROJECT,
    "team": EntityKind.TEAM,
    "work_unit": EntityKind.WORK_UNIT,
    "issue": EntityKind.ISSUE,
    "pull_request": EntityKind.PULL_REQUEST,
}


class CorpusEvidenceSigner(EvidenceReferenceSigner):
    """The world's mint, wearing the platform signer's interface.

    ``issue`` returns ``world.evidence_handle(slug)`` for a record the corpus
    resolver produced, and remembers the identity it issued over. ``verify``
    then answers the question the HMAC answers -- "was this handle issued for
    exactly this identity, in this org" -- by comparing the remembered
    payload rather than by recomputing a key it cannot hold.

    Not a weakening of verification: the payload compared is
    ``EvidenceReferenceSigner._payload``'s own bytes, so a ref whose identity
    fields were altered after minting fails here exactly as it would fail an
    HMAC check. What is lost is only the ability to verify a handle this
    process did not issue, which no admission round needs.
    """

    def __init__(self, secret: str | bytes) -> None:
        super().__init__(secret)
        self._issued: dict[str, bytes] = {}

    def issue(self, org_id: str, record: EvidenceRecord) -> str:
        slug = record.internal_path
        if not slug:
            raise ValueError(
                "the corpus mint identifies a record by its evidence slug, "
                "which the resolver must carry on EvidenceRecord.internal_path; "
                f"got {record!r}"
            )
        handle = world.evidence_handle(slug)
        self._issued[handle] = self._payload(org_id, record)
        return handle

    def verify(self, org_id: str, evidence: SignedIdentity) -> bool:
        expected = self._issued.get(evidence.evidence_ref_id)
        return expected is not None and expected == self._payload(org_id, evidence)


@dataclass(frozen=True)
class CorpusScopeAuthorizer:
    """Resolves scope from the world's per-principal grant.

    One principal, resolved the same way for every candidate -- the service
    calls this once per candidate on purpose, and a stateless authorizer
    honours that rather than pretending to.
    """

    principal_id: str

    async def resolve(
        self,
        org_id: str,
        permission_fingerprint: str,
        request: ScopeResolveRequest,
    ) -> ScopeResolution:
        principal = world.PRINCIPALS[self.principal_id]
        if principal.tenant_id != org_id:
            # A cross-tenant request resolves to nothing authorized. Stated
            # as an outcome rather than an exception, because the service's
            # own branch for "not authorized" is what must be exercised.
            return ScopeResolution(
                outcome=ScopeResolutionOutcome.UNRESOLVED,
                entities=(),
                team_filters=(),
                candidates=(),
                time_range=_time_range(),
            )
        entities = tuple(
            AuthorizedEntity(
                kind=_scope_kind(entity_id),
                canonical_id=entity_id,
                label=_label(entity_id),
            )
            for entity_id in sorted(principal.visible_entity_ids)
        )
        return ScopeResolution(
            outcome=ScopeResolutionOutcome.EXACT,
            entities=entities,
            team_filters=(),
            candidates=(),
            time_range=_time_range(),
        )


@dataclass(frozen=True)
class CorpusCandidateResolver:
    """Resolves a graph-discovered locator against the WORLD's own records.

    The locator is an evidence slug. ``None`` for anything the world does not
    hold, for anything belonging to another tenant, and for anything whose
    state is no longer ACTIVE -- revoked, redacted or deleted. That last
    refusal mirrors what ``expand`` already does with a withdrawn record and
    what the arm's own CHAOS-3628 filter does upstream.

    **Adversarial records are ADMITTED, and that is deliberate.** The world
    plants keyword-stuffed filler, a prompt injection inside a real document,
    and a false dependency claim -- all authentic records, present in the
    caller's tenant, about entities the caller may see. A canonical evidence
    service is not a content-trust filter: refusing them here would make the
    admission path quietly do the arm's job, and every downstream dimension
    that scores whether an arm resists them (``unsupported_attribution_rate``,
    the injection cases) would pass because the material never arrived. What
    the service does instead is carry the world's own trust level onto the
    ref's flags, which is the disclosure the frame expects.

    The consequence was measured rather than assumed: the first version DID
    refuse non-citable records, and the run failed loudly, because a driver
    cited ``doc_false_dependency_claim`` as CONFLICTING evidence -- the arm
    correctly recording a false claim as contradicted. Refusing it took away
    the arm's ability to say so. A filter that removes counter-evidence is not
    a safety feature.
    """

    org_id: str
    source_system: str = ARM_SOURCE_SYSTEM

    async def resolve(
        self,
        *,
        org_id: str,
        scope: ScopeResolution,
        candidate: EvidenceCandidate,
    ) -> EvidenceRecord | None:
        evidence = world.EVIDENCE_BY_SLUG.get(candidate.locator)
        if evidence is None:
            return None
        if evidence.tenant_id != org_id:
            return None
        if evidence.state is not world.EvidenceState.ACTIVE:
            return None
        return EvidenceRecord(
            source_system=self.source_system,
            source_version=CORPUS_SOURCE_VERSION,
            # Every field below comes from the WORLD's record, never from the
            # candidate. That is the whole point of resolution: the arm said
            # where to look, and the source said what is there.
            entity_type=evidence.source_class.value,
            entity_id=evidence.entity_id,
            display_label=evidence.display_label,
            observed_at=evidence.observed_at.astimezone(UTC),
            freshness=FreshnessState.FRESH,
            provenance=f"canonical record admitted from {evidence.source_class.value}",
            confidence=1.0,
            repository_ids=(),
            raw_excerpt=evidence.citation_text,
            # The world's own trust level, carried as a flag rather than as a
            # refusal. ``_to_ref`` already stamps ``untrusted_content`` on
            # every ref; this says the SOURCE also considers the content
            # unverified, which is a different claim.
            uncertain=evidence.trust is not world.TrustLevel.CANONICAL,
            # Carries the slug to the mint. Not emitted on the ref:
            # ``_safe_link`` ignores a path that does not start with "/".
            internal_path=evidence.slug,
        )


class _AlwaysEntitled:
    """The corpus has no feature flags. Named so the absence is visible."""

    async def require(self, org_id: str) -> None:
        return None


def corpus_evidence_service(
    *, org_id: str, principal_id: str, secret: str
) -> EvidenceService:
    """The real ``EvidenceService``, wired to the corpus world."""

    return EvidenceService(
        entitlement=_AlwaysEntitled(),
        authorizer=CorpusScopeAuthorizer(principal_id=principal_id),
        signer=CorpusEvidenceSigner(secret),
        native_adapters=(),
        candidate_resolvers=_resolvers(org_id),
    )


def _resolvers(org_id: str) -> tuple[EvidenceCandidateResolver, ...]:
    return (CorpusCandidateResolver(org_id=org_id),)


def _scope_kind(entity_id: str) -> EntityKind:
    entity = world.ENTITIES_BY_ID.get(entity_id)
    if entity is None:
        return _KIND_FALLBACK
    return _KIND_BY_WORLD_KIND.get(entity.kind.value, _KIND_FALLBACK)


def _label(entity_id: str) -> str:
    entity = world.ENTITIES_BY_ID.get(entity_id)
    return entity.display_label if entity is not None else entity_id


def _time_range() -> ResolvedTimeRange:
    start = world.WINDOW_START.astimezone(UTC)
    end = world.WINDOW_END.astimezone(UTC)
    return ResolvedTimeRange(
        timezone="UTC",
        utc_start=start,
        utc_end=end,
        local_start=start.isoformat(),
        local_end=end.isoformat(),
        comparison_utc_start=start,
        comparison_utc_end=end,
        comparison_local_start=start.isoformat(),
        comparison_local_end=end.isoformat(),
    )
