"""CHAOS-3620: the one end-to-end path every safety proof in this lane attacks.

Every earlier lane tested a *stage*. CHAOS-3617's suite reads back over
synthetic ``alpha``/``beta`` fixtures whose authorized set is a hand-written
tuple (``graph_arm/fixtures.py:509``); CHAOS-3616's oracles score a witness
packet the corpus itself builds (``investigation_corpus/reference.py:250``).
Neither composition has ever been run: the **real** graph arm, over the
**real** corpus world, under a **real** per-principal grant, emitting a
**real** packet, measured by the **independent** authorization oracle.

That composition is what CHAOS-3620 has to prove is safe, so it is built once
here and imported by every module in the lane. Three properties of it are
load-bearing and none of them is incidental:

* **The grant is the world's, not the arm's.** ``authorized_entity_ids_for``
  (``graph_arm/corpus_adapter.py:70``) reads
  ``world.PRINCIPALS[...].visible_entity_ids``. The corpus plants a
  restricted project *inside the caller's own tenant*
  (``world.py:2861-2871``), so a tenant-derived set looks correct and is not.
  Every authorization claim in this lane rests on that distinction.

* **The graph contains what the grant excludes.** ``corpus_batch`` is
  tenant-scoped (``corpus_adapter.py:123``), so ``proj_quarry`` is ingested,
  is a node, and has a real edge to an authorized team. Its absence from a
  packet is therefore a *filtering* result and never an absence of data —
  which is the only version of the claim worth making.

* **Nothing here is a mock.** ``ProjectionGraphReader`` is the arm's own
  in-memory reader, ``build_packet`` is the arm's own emitter, and
  ``audit_authorization`` is the corpus's own oracle. A safety proof over
  test doubles proves the doubles are safe.

Nothing in this module writes to the frozen corpus or the frozen contract. It
reads ``world`` through the arm's adapter exactly as the arm does.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from functools import cache

from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
from dev_health_ops.api.dev.investigation_contract import (
    AskDevInvestigationPacket,
    ComparisonShape,
    QuestionFamilyID,
)
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import build_projection
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.budgets import (
    DEFAULT_BUDGETS,
    TrialBudgets,
)
from dev_health_ops.context_fabric.graph_arm.drivers import (
    DriverFinding,
    discover_drivers,
)
from dev_health_ops.context_fabric.graph_arm.packet_builder import (
    JobContext,
    TrialContext,
    build_packet,
)
from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection
from dev_health_ops.context_fabric.graph_arm.readback import (
    InvestigationReadout,
    ProjectionGraphReader,
)
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

__all__ = [
    "PRODUCED_AT",
    "RestrictedMaterial",
    "disclosures",
    "findings_for",
    "restricted_material",
    "RUN_ID",
    "SIGNING_SECRET",
    "Investigation",
    "current_watermark",
    "helio_projection",
    "investigate",
    "lumen_projection",
    "packet_from",
    "readout_for",
    "signer",
]

#: A fixed, obviously-fake signing secret. ``EvidenceReferenceSigner``
#: requires >= 32 bytes and refuses to operate without one, which is what
#: makes every handle in this lane verifiable rather than decorative.
SIGNING_SECRET = "chaos-3620-safety-proof-signing-secret-not-a-real-key"

#: Pinned so two runs of the same proof produce byte-identical packets. A
#: wall-clock here would make every packet-identity assertion in the lane
#: quietly time-dependent.
PRODUCED_AT = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
RUN_ID = "3620a5af-0000-4000-8000-000000003620"


def signer() -> EvidenceReferenceSigner:
    return EvidenceReferenceSigner(SIGNING_SECRET)


@cache
def helio_projection() -> GraphProjection:
    """The primary tenant, projected once.

    Cached because projection is pure and the corpus is frozen; a per-test
    rebuild would cost seconds across the lane and could not differ.
    """

    return build_projection(adapter.corpus_batch(world.ORG_HELIO))


@cache
def lumen_projection() -> GraphProjection:
    """The neighbouring tenant. Present so cross-tenant claims are symmetric."""

    return build_projection(adapter.corpus_batch(world.ORG_LUMEN))


def current_watermark(records_indexed: int = 48) -> IndexWatermark:
    """A watermark that is current for the corpus's own bounded window.

    Stated rather than defaulted: a watermark is a ``build_packet`` argument
    with no cross-check against the readout
    (``graph_arm/packet_builder.py:642``), so every proof in this lane that
    is *not* about staleness has to pin freshness deliberately, or a future
    change to the default would silently turn an authorization proof into a
    staleness proof.
    """

    return IndexWatermark(
        indexed_through=world.WINDOW_END,
        projected_at=world.WINDOW_END,
        records_indexed=records_indexed,
    )


def readout_for(
    seeds: tuple[str, ...] | list[str],
    *,
    principal: str = world.PRINCIPAL_ANALYST,
    projection: GraphProjection | None = None,
    max_hops: int = 2,
    budgets: TrialBudgets = DEFAULT_BUDGETS,
    authorized_entity_ids: frozenset[str] | tuple[str, ...] | None = None,
) -> InvestigationReadout:
    """One bounded, authorized traversal of the corpus world.

    ``authorized_entity_ids`` overrides the principal's true grant and exists
    for exactly one purpose: planting the *widened-grant* fault shape. A
    proof that a correct grant excludes the restricted project says nothing
    until the same traversal under a widened grant is observed including it.
    """

    graph = projection if projection is not None else helio_projection()
    grant = (
        adapter.authorized_entity_ids_for(principal)
        if authorized_entity_ids is None
        else frozenset(authorized_entity_ids)
    )
    return asyncio.run(
        ProjectionGraphReader(graph).neighbourhood(
            org_id=graph.org_id,
            seed_canonical_ids=list(seeds),
            authorized_entity_ids=sorted(grant),
            max_hops=max_hops,
            budgets=budgets,
        )
    )


def packet_from(
    readout: InvestigationReadout,
    *,
    job_id: str = "job_3620_status",
    question_family: str = "project_status_drivers",
    job_statement: str = "What is the current status, and what is driving it?",
    comparison_shape: ComparisonShape = ComparisonShape.SINGULAR_SUBJECT,
    watermark: IndexWatermark | None = None,
    **overrides: object,
) -> AskDevInvestigationPacket:
    """Emit the frozen contract from one readout, through the arm's builder."""

    job = JobContext(
        job_id=job_id,
        question_family=QuestionFamilyID(question_family),
        job_statement=job_statement,
        comparison_shape=comparison_shape,
        window_start=world.WINDOW_START,
        window_end=world.WINDOW_END,
    )
    return build_packet(
        readout=readout,
        job=job,
        watermark=watermark if watermark is not None else current_watermark(),
        signer=signer(),
        trial=TrialContext(run_id=RUN_ID, corpus_version=adapter.CORPUS_VERSION),
        produced_at=PRODUCED_AT,
        **overrides,  # type: ignore[arg-type]
    )


@dataclass(frozen=True)
class Investigation:
    """One complete run, with every intermediate kept.

    The intermediates are not convenience. A leak has to be attributable to a
    stage — traversal, emission, or the grant itself — and a helper that
    returned only the packet would make every failure in this lane say
    "something, somewhere, disclosed it".
    """

    principal: str
    seeds: tuple[str, ...]
    grant: frozenset[str]
    readout: InvestigationReadout
    packet: AskDevInvestigationPacket
    #: What ``discover_drivers`` produced, when the run asked for drivers.
    #: Empty is a real answer and is distinguishable from "not asked for"
    #: only by ``drivers_requested`` — a distinction that matters because a
    #: provenance proof over a packet with no drivers proves nothing about
    #: driver provenance.
    findings: tuple[DriverFinding, ...] = ()
    drivers_requested: bool = False


def investigate(
    *seeds: str,
    principal: str = world.PRINCIPAL_ANALYST,
    projection: GraphProjection | None = None,
    max_hops: int = 2,
    authorized_entity_ids: frozenset[str] | tuple[str, ...] | None = None,
    with_drivers: bool = False,
    as_of: datetime | None = None,
    **packet_overrides: object,
) -> Investigation:
    """Grant -> traversal -> (drivers) -> packet, with nothing stubbed.

    ``with_drivers`` runs the arm's own :func:`discover_drivers` against the
    first seed and hands the findings to the emitter, which is the only way
    the packet's driver section is populated at all. It is opt-in rather than
    always-on because most authorization claims are about identifiers
    reaching a consumer, and a driver pass would add cost to every one of
    them without changing the answer.
    """

    grant = (
        adapter.authorized_entity_ids_for(principal)
        if authorized_entity_ids is None
        else frozenset(authorized_entity_ids)
    )
    readout = readout_for(
        seeds,
        principal=principal,
        projection=projection,
        max_hops=max_hops,
        authorized_entity_ids=grant,
    )
    findings: tuple[DriverFinding, ...] = ()
    if with_drivers:
        if not seeds:
            raise ValueError(
                "with_drivers needs a seed to discover drivers for; a driver "
                "pass over no subject would return nothing and read as a "
                "proof that nothing was asserted"
            )
        discovered, _truncated = discover_drivers(
            readout, seeds[0], as_of=as_of if as_of is not None else world.TRIAL_NOW
        )
        findings = tuple(discovered)
        packet_overrides.setdefault("drivers", findings)
    return Investigation(
        principal=principal,
        seeds=tuple(seeds),
        grant=grant,
        readout=readout,
        packet=packet_from(readout, **packet_overrides),  # type: ignore[arg-type]
        findings=findings,
        drivers_requested=with_drivers,
    )


@dataclass(frozen=True)
class RestrictedMaterial:
    """Everything a principal must not see, in every form it could appear as.

    The frozen oracle's ``entity_sightings`` walks *canonical entity ids* in
    the packet sections it knows about. That is the right scope for a
    contract-neutral oracle and it is not the whole disclosure surface. Two
    channels sit outside it, and adversarial review proved both reachable:

    * **observation identifiers.** ``evidence.entity_id`` on an indexed item
      carries an evidence slug, not an entity id. A restricted slug —
      ``wi_quarry_redacted``, whose subject the analyst cannot see — reaches
      the packet, appears in ``entity_sightings``, and is invisible to any
      check that filters by "is this a known entity id".
    * **prose.** ``display_label``, driver summaries, inclusion reasons,
      limitation details and matched text all carry human-readable names. A
      packet that never names ``proj_quarry`` but does say "Quarry
      Compliance" has disclosed it.

    Ids are matched exactly; labels are matched as substrings, and a label
    that also occurs inside anything the caller may legitimately see is
    **excluded and recorded** rather than matched. Without that exclusion the
    check false-positives immediately: the corpus's cross-tenant
    near-duplicate gives ``lumen_proj_acr`` the label "Agent Context
    Runtime", identical to the Helio project the analyst is entitled to read
    about.
    """

    principal_id: str
    #: Canonical entity ids the principal cannot see.
    entity_ids: frozenset[str]
    #: Evidence slugs whose subject the principal cannot see.
    evidence_slugs: frozenset[str]
    #: Human-readable names that are safe to substring-match.
    labels: frozenset[str]
    #: Names that WOULD be restricted but also occur in material the caller
    #: may see, so matching them would report a false disclosure. Carried so
    #: the exclusion is auditable instead of silent.
    ambiguous_labels: frozenset[str]


def restricted_material(principal_id: str) -> RestrictedMaterial:
    """What ``principal_id`` must not see, derived from the world's grants."""

    visible = world.PRINCIPALS[principal_id].visible_entity_ids

    entity_ids = frozenset(
        entity_id for entity_id in world.ENTITIES_BY_ID if entity_id not in visible
    )
    evidence_slugs = frozenset(
        slug
        for slug, record in world.EVIDENCE_BY_SLUG.items()
        if record.entity_id not in visible
    )

    # Everything the caller may legitimately read, as one casefolded corpus.
    # A restricted name occurring anywhere in here cannot be distinguished
    # from a permitted mention of a permitted thing.
    permitted_text = "\n".join(
        text.casefold()
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

    candidates = {
        *(world.ENTITIES_BY_ID[entity_id].display_label for entity_id in entity_ids),
        *(world.EVIDENCE_BY_SLUG[slug].display_label for slug in evidence_slugs),
    }
    labels: set[str] = set()
    ambiguous: set[str] = set()
    for label in candidates:
        if not label:
            continue
        # Classified by the SAME whole-token predicate the matcher uses.
        # Adversarial review found these two disagreeing: classification by
        # substring, matching by token. A label excluded for a substring
        # collision the matcher would never have made is a blind spot created
        # by the exclusion rule itself.
        #
        # Measured consequence, recorded because it corrects the finding's
        # stated cause: making them consistent recovers NOTHING on this
        # corpus. ``Core`` is excluded either way, because the analyst can
        # legitimately see ``platform core`` (team_atlas's previous name),
        # which contains ``core`` as a whole token. The asymmetry was real
        # and was not what created the blind spot — genuine ambiguity is.
        # Both are fixed: the rule is now consistent, and the residual it
        # leaves is asserted rather than left in a field nobody reads.
        (ambiguous if _contains_token(permitted_text, label) else labels).add(label)

    return RestrictedMaterial(
        principal_id=principal_id,
        entity_ids=entity_ids,
        evidence_slugs=evidence_slugs,
        labels=frozenset(labels),
        ambiguous_labels=frozenset(ambiguous),
    )


#: Identifier characters. A token is a disclosure only when it appears
#: WHOLE. Two false positives this rules out, both found by running the
#: sweep rather than by reasoning about it:
#:
#: * ``proj_quarry`` inside a hypothetical ``proj_quarry_archive`` is a
#:   different entity;
#: * the label ``Ember`` — a team the Lumen principal cannot see — occurs
#:   inside the JSON key ``"members"`` in every packet, so a plain substring
#:   search reported a disclosure on four clean Lumen packets.
#:
#: The second one is why labels get the same treatment as ids. A check that
#: cries wolf on its own serialization format is a check people learn to
#: ignore, which is worse than not having it.
_TOKEN_BOUNDARY = re.compile(r"[A-Za-z0-9_-]")


def _contains_token(haystack: str, token: str) -> bool:
    """Whether ``token`` occurs in ``haystack`` as a whole token, case-insensitively."""

    folded_haystack = haystack.casefold()
    folded_token = token.casefold()
    start = 0
    while True:
        index = folded_haystack.find(folded_token, start)
        if index < 0:
            return False
        before = folded_haystack[index - 1] if index else ""
        after_index = index + len(folded_token)
        after = (
            folded_haystack[after_index] if after_index < len(folded_haystack) else ""
        )
        if not _TOKEN_BOUNDARY.match(before or " ") and not _TOKEN_BOUNDARY.match(
            after or " "
        ):
            return True
        start = index + 1


def disclosures(packet, principal_id: str = world.PRINCIPAL_ANALYST) -> list[str]:
    """Every restricted identifier or name anywhere in the emitted packet.

    Searched over the packet's own JSON rendering — which is what a consumer
    receives — rather than over a chosen list of fields. A field list is how
    a disclosure walker silently stops covering the field that later leaks;
    adversarial review found exactly that in this suite's first version,
    where the check filtered to canonical entity ids and a restricted
    evidence slug rode through untouched.

    Returns sorted ``"<channel>:<token>"`` strings so a failure names both
    what leaked and how it was carried.
    """

    material = restricted_material(principal_id)
    rendered = packet.model_dump_json()

    found = [
        f"entity_id:{entity_id}"
        for entity_id in sorted(material.entity_ids)
        if _contains_token(rendered, entity_id)
    ]
    found += [
        f"evidence_slug:{slug}"
        for slug in sorted(material.evidence_slugs)
        if _contains_token(rendered, slug)
    ]
    found += [
        f"label:{label}"
        for label in sorted(material.labels)
        if _contains_token(rendered, label)
    ]
    return sorted(found)


@cache
def findings_for(
    seed: str,
    principal: str = world.PRINCIPAL_ANALYST,
) -> tuple[DriverFinding, ...]:
    """Driver findings for one subject, without emitting a packet.

    Separated from :func:`investigate` because the two can disagree: driver
    *discovery* succeeds for every corpus subject, while packet *emission*
    is refused for at least one (a capacity driver carrying no staffing
    qualification is rejected by the frozen contract — CHAOS-3634). A
    semantic claim about what the arm is willing to assert must not lose a
    subject to a downstream emission refusal, or the sweep quietly shrinks
    on exactly the subject that produced the awkward finding.
    """

    readout = readout_for((seed,), principal=principal)
    discovered, _truncated = discover_drivers(readout, seed, as_of=world.TRIAL_NOW)
    return tuple(discovered)


def lineage_path_for(path):
    """One discovered path, converted by the arm's own emitter helper.

    Exposed so a test can hand the *contract* exactly the lineage the arm
    would have emitted, without going through ``build_packet`` — which is the
    only way to exercise the contract's own authorization validator
    independently of the emitter's earlier check. Hand-building an equivalent
    ``LineagePath`` here would test a hand-built shape rather than the arm's.
    """

    from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState
    from dev_health_ops.context_fabric.graph_arm.packet_builder import _lineage_path

    return _lineage_path(path, SourceRequirementState.AVAILABLE_CURRENT)


def with_grant(
    readout: InvestigationReadout, authorized: frozenset[str] | tuple[str, ...]
) -> InvestigationReadout:
    """A readout relabelled with a different declared grant.

    The plant for "the arm's own claim is not evidence": every contract-level
    authorization check compares the packet against
    ``related_context.authorized_entity_ids``, which the producer fills in
    (``investigation_contract/packet.py:842-878``). Relabelling produces a
    packet that is internally consistent and externally false — the exact
    input the independent oracle exists to judge.
    """

    return replace(readout, authorized_entity_ids=tuple(sorted(authorized)))
