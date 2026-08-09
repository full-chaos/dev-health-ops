"""The graph arm, driven from a question and nothing that reveals the answer.

**The fairness rule this module exists to make structural.** Review condition
3(a): both arms receive the identical question and conversational context and
*nothing else*; the graph arm must resolve subjects through its own candidate
discovery and must never be handed the case's seed or committed subject. If
it were, the orchestrator-interpreted native leg would be doing ambiguity work
the graph leg skipped, and every figure in the ambiguity family would be
unearned for the graph arm.

That rule is enforced by construction rather than by discipline, in two ways
a reviewer can check quickly:

* :func:`discover_subjects` takes a **question string**. There is no
  parameter anywhere in this module through which an expected subject, a
  seed id, or a case id could arrive -- so passing one is not something a
  caller can do carelessly, it is something that does not typecheck;
* this module imports ``investigation_corpus.world`` for tenancy and grants
  and **never** ``cases``, ``oracles``, ``evaluate`` or ``reference``. That
  is the same discipline CHAOS-3617 already enforces on ``corpus_adapter``
  (whose test asserts the imports stay absent), and
  ``test_chaos_3619_graph_leg_fairness.py`` asserts it here for the same
  reason: the arm must not be able to see what it is scored against, and the
  cheapest way to guarantee that is for the import to be absent.

**Seeds come from what the arm found, not from what the corpus knows.** The
traversal is seeded with the canonical ids of the candidates discovery
returned, in its own ranked order. A question that matches nothing yields no
seeds and therefore an empty neighbourhood -- which is the correct, and
scoreable, behaviour for the corpus's no-match and clarification cases. It is
emphatically not an error to be worked around by falling back to a seed the
trial happens to know.

**Discovery reads the projection, not the store.** ``search_candidates``
matches over stored node text (canonical ids, display labels, aliases,
acronyms, previous names) and takes ``GraphNode``s, so it runs against the
same projection that was written to the live store rather than issuing a
query. Named here because it is a real mechanism difference from the
neighbourhood read, and it belongs in the fairness table rather than in a
reader's assumptions: subject discovery is an exact/alias lookup over stored
text either way, and no semantic retrieval is involved -- the deterministic
embedder carries no similarity at all, and the emission guard refuses a
semantic match signal under it.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import corpus_adapter
from dev_health_ops.context_fabric.graph_arm.corpus_adapter import CORPUS_VERSION
from dev_health_ops.context_fabric.graph_arm.discovery import (
    CandidateMatch,
    search_candidates,
)
from dev_health_ops.context_fabric.graph_arm.projection import GraphProjection

if TYPE_CHECKING:  # pragma: no cover - typing only
    from datetime import datetime

    from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
    from dev_health_ops.api.dev.investigation_contract import (
        ComparisonShape,
        QuestionFamilyID,
    )
    from dev_health_ops.context_fabric.graph_arm.cohort_discovery import (
        CohortDiscovery,
    )
    from dev_health_ops.context_fabric.graph_arm.readback import (
        InvestigationReadout,
    )
    from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind
    from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

#: Mention ids are correlation handles the trial never reads; a fixed
#: namespace keeps a sweep reproducible rather than seeding uuid4 per run.
_MINT_NAMESPACE = uuid.UUID("3619a11e-0000-4000-8000-000000000001")
_mint_counter = iter(range(1_000_000))


def _mint() -> str:
    return str(uuid.uuid5(_MINT_NAMESPACE, str(next(_mint_counter))))


__all__ = [
    "MAX_COHORT_SEEDS",
    "MAX_SEEDS",
    "GraphPacketOutcome",
    "SubjectDiscovery",
    "assemble_cohort_packet",
    "assemble_packet",
    "authorized_ids_for",
    "cohort_seeds_from",
    "discover_cohort_for",
    "discover_subjects",
    "seeds_from",
]

#: How many discovered candidates seed the traversal.
#:
#: Bounded because a neighbourhood read from every candidate in a 25-entry
#: list is not an investigation, it is a sweep of the tenant -- and a sweep
#: presented as a bounded investigation is the organization-widening the
#: contract refuses. Three is the contract's own top-3 subject horizon, so
#: the traversal covers exactly the candidates the packet may commit to.
MAX_SEEDS = 3


@dataclass(frozen=True, slots=True)
class SubjectDiscovery:
    """What the arm found for a question, and what it was not allowed to see.

    ``authorization_filtered_count`` is a real disclosure rather than a
    constant: it counts distinct entities that matched the query and were
    withheld, which tells a packet reader the answer was narrowed without
    telling them what was removed.
    """

    candidates: tuple[CandidateMatch, ...]
    authorization_filtered_count: int

    @property
    def resolved(self) -> bool:
        return bool(self.candidates)


def authorized_ids_for(principal_id: str) -> frozenset[str]:
    """The principal's true grant, from the world's own per-principal map.

    Derived from the principal and never from tenancy. The corpus plants a
    restricted project inside the caller's own tenant precisely so a
    tenant-derived set looks correct and is not.
    """

    return corpus_adapter.authorized_entity_ids_for(principal_id)


def mention_texts(question: str) -> tuple[str, ...]:
    """The candidate subject phrases in a question, via PRODUCTION extraction.

    ``search_candidates`` matches a *mention* against stored text -- exact
    canonical id, exact display name, alias family, or whole-token
    containment. It is not a question parser, and handing it a whole
    sentence matches nothing: "is solstice billing understaffed" is not a
    token-subset of "Solstice Billing". So a mention step is required, and
    the honest one is the interpreter's own.

    Using ``extract_mentions`` (plus the untyped bare-name backstop) rather
    than anything written here is the point. It is the same extraction the
    native leg's interpreter runs, so both arms start from the identical
    phrases and the comparison isolates what each does NEXT: the native leg
    resolves through the catalog scope service, the graph leg through graph
    discovery. That difference is the variable under test. A bespoke
    extractor tuned until the graph arm resolved more would be tuning the
    arm to the corpus, which the correction forbids.
    """

    from dev_health_ops.api.dev.question_interpreter import (
        extract_mentions,
        untyped_name_candidates,
    )

    texts: list[str] = []
    for mention in extract_mentions(question, context_refs=[], mint_id=_mint):
        # ``normalized_lookup_text`` is the field the contract provides FOR
        # lookup; ``original_text_span`` is the raw span a human typed. Both
        # are tried because a mention whose normalization emptied it would
        # otherwise contribute nothing and silently narrow discovery.
        #
        # This loop was written against a field name that does not exist
        # (``mention_text``) and every test passed, because none of the
        # corpus questions produce a TYPED mention -- the bare-name backstop
        # below supplies all of them, so the body never ran. mypy caught it;
        # no test could have. ``test_the_typed_mention_path_is_exercised``
        # now closes that blind spot.
        for value in (mention.normalized_lookup_text, mention.original_text_span):
            if value and value not in texts:
                texts.append(value)
    for candidate in untyped_name_candidates(question) or ():
        text = candidate if isinstance(candidate, str) else str(candidate)
        if text not in texts:
            texts.append(text)
    return tuple(texts)


def discover_subjects(
    *,
    question: str,
    projection: GraphProjection,
    authorized_entity_ids: frozenset[str],
    limit: int = 25,
) -> SubjectDiscovery:
    """Resolve a human question to authorized candidate subjects.

    The whole 3(a) surface. Note what is absent from the signature: there is
    no expected subject, no seed, no case id and no oracle. The only things
    deciding what this returns are the question and what the principal may
    see.

    Candidates from every extracted mention are merged and re-ranked by the
    arm's own total order (signal strength, then canonical id), so a question
    naming two subjects contributes both rather than whichever mention was
    extracted first.

    The withheld count is the **maximum** across mentions, not the sum.
    ``search_candidates`` returns a count and not identities, so summing
    would double-count a single restricted entity that matched two mentions
    and overstate the disclosure -- and this number reaches the packet as
    ``authorization_filtered_count``, where an inflated value is a false
    claim about how much the answer was narrowed. The maximum understates
    when two *different* entities are withheld by different mentions; that is
    the safe direction and it is why this is not a sum.
    """

    merged: dict[str, CandidateMatch] = {}
    withheld: set[str] = set()
    for text in mention_texts(question):
        candidates, filtered = search_candidates(
            text, projection.nodes, authorized_entity_ids, limit=limit
        )
        for match in candidates:
            existing = merged.get(match.canonical_id)
            if existing is None or match.rank_key < existing.rank_key:
                merged[match.canonical_id] = match
        if filtered:
            # ``search_candidates`` returns a COUNT, not identities, so the
            # only honest merge across mentions is the maximum rather than a
            # sum: adding them would double-count one entity withheld from
            # two mentions and overstate the disclosure.
            withheld.add(f"mention:{text}:{filtered}")

    ranked = tuple(sorted(merged.values(), key=lambda item: item.rank_key))
    filtered_total = max(
        (int(entry.rsplit(":", 1)[1]) for entry in withheld), default=0
    )
    return SubjectDiscovery(
        candidates=ranked[:limit], authorization_filtered_count=filtered_total
    )


def seeds_from(discovery: SubjectDiscovery, *, limit: int = MAX_SEEDS) -> list[str]:
    """The traversal seeds, in the arm's own ranked order.

    Returns ``[]`` when nothing matched, and that empty list is a result
    rather than a failure: the corpus's no-match and clarification cases are
    *supposed* to produce an empty neighbourhood, and substituting any seed
    here would convert the trial's hardest safety cases into easy ones.
    """

    return [match.canonical_id for match in discovery.candidates[:limit]]


def tenant_of(principal_id: str) -> str:
    """The organization a principal asks inside.

    Read from the world's principal map rather than passed in, so a caller
    cannot ask a Helio principal's question against Lumen's partition and
    have the arm answer it.
    """

    return world.PRINCIPALS[principal_id].tenant_id


# ---------------------------------------------------------------------------
# Packet assembly
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GraphPacketOutcome:
    """One graph-arm attempt: a packet, a named refusal, or a fault.

    Three outcomes rather than "packet or None", because the trial's
    dispositions distinguish them and the arm genuinely does too. A refusal
    is a capability boundary the arm KNOWS it has and names
    (``UnsupportedComparisonShapeError``, ``IncomparableCohortError``); a
    fault is something it does not model. Reporting a fault as a boundary
    would publish a defect as an honest limitation.
    """

    payload: Mapping[str, object] | None
    refusal: str = ""
    fault: str = ""
    seeds: tuple[str, ...] = ()
    authorization_filtered_count: int = 0

    @property
    def emitted(self) -> bool:
        return self.payload is not None


#: The graph arm's own named capability boundaries. Anything raised that is
#: NOT in here is a fault, not a limit -- see :class:`GraphPacketOutcome`.
_NAMED_REFUSALS: tuple[str, ...] = (
    "UnsupportedComparisonShapeError",
    "IncomparableCohortError",
    "UnsupportedMatchMechanismError",
    "EmbedderProvenanceMismatchError",
    "PacketTooLargeError",
)


def _entity_labels(
    projection: GraphProjection,
) -> dict[str, tuple[GraphEntityKind, str]]:
    """The ``canonical_id -> (kind, label)`` map ``build_cohort`` takes.

    Excludes the organization partition root: it is an entity node with no
    emittable subject kind, and a cohort that could contain it would be a
    cohort containing the tenant.
    """

    from dev_health_ops.context_fabric.graph_arm.vocabulary import (
        GraphEntityKind as _Kind,
    )

    return {
        node.canonical_id: (kind, node.display_label)
        for node in projection.nodes
        if node.is_entity
        and (kind := node.entity_kind) is not None
        and kind is not _Kind.ORGANIZATION
    }


def assemble_packet(
    *,
    readout: InvestigationReadout,
    projection: GraphProjection,
    discovery: SubjectDiscovery,
    question_family: QuestionFamilyID,
    comparison_shape: ComparisonShape,
    job_statement: str,
    window_start: datetime,
    window_end: datetime,
    run_id: str,
    watermark: IndexWatermark,
    signer: EvidenceReferenceSigner,
    produced_at: datetime,
    as_of: datetime,
    authorized_entity_ids: frozenset[str],
) -> GraphPacketOutcome:
    """Turn one authorized traversal into a packet, or a named refusal.

    ``question_family`` and ``comparison_shape`` are ARGUMENTS rather than
    anything this module derives, and that is what lets the same code serve
    both legs: Leg A passes what the production interpreter produced, Leg B
    passes the corpus's declared values through the two-field channel. The
    arm cannot tell the legs apart, which is the point -- a leg-aware arm
    would be an arm tuned to the trial.

    Cohort and drivers are derived from the readout the arm produced, never
    supplied. Drivers are attempted only when the readout declares it can say
    what a record is about; on a readout that cannot, ``discover_drivers``
    refuses to attribute and the packet says so rather than silently
    attributing nothing.
    """

    from dev_health_ops.api.dev.investigation_contract import ComparisonShape
    from dev_health_ops.context_fabric.graph_arm.cohort import build_cohort
    from dev_health_ops.context_fabric.graph_arm.drivers import (
        DriverFinding,
        discover_drivers,
    )
    from dev_health_ops.context_fabric.graph_arm.packet_builder import (
        JobContext,
        TrialContext,
        build_packet,
    )

    seeds = tuple(seeds_from(discovery))
    if not seeds:
        # No subject resolved. NOT a refusal and not a fault: the corpus's
        # no-match and clarification cases are supposed to land here, and
        # the arm declining to investigate an unresolved reference is the
        # correct, scoreable behaviour.
        return GraphPacketOutcome(
            payload=None,
            refusal="no authorized subject resolved from the question",
            seeds=(),
            authorization_filtered_count=discovery.authorization_filtered_count,
        )

    subject_id = seeds[0]
    cohort = None
    if comparison_shape is not ComparisonShape.SINGULAR_SUBJECT:
        cohort = build_cohort(
            subject_id,
            projection.edges,
            _entity_labels(projection),
            authorized_entity_ids,
        )

    drivers: tuple[DriverFinding, ...] = ()
    truncated = False
    if readout.observation_attachment_available:
        drivers, truncated = discover_drivers(readout, subject_id, as_of=as_of)

    try:
        packet = build_packet(
            readout=readout,
            job=JobContext(
                job_id=f"job_{subject_id}",
                question_family=question_family,
                job_statement=job_statement,
                comparison_shape=comparison_shape,
                window_start=window_start,
                window_end=window_end,
            ),
            watermark=watermark,
            signer=signer,
            trial=TrialContext(run_id=run_id, corpus_version=CORPUS_VERSION),
            produced_at=produced_at,
            cohort=cohort,
            drivers=drivers,
            drivers_truncated=truncated,
        )
    except Exception as raised:  # noqa: BLE001 - classified, not swallowed
        name = type(raised).__name__
        if name in _NAMED_REFUSALS:
            return GraphPacketOutcome(
                payload=None,
                refusal=f"{name}: {raised}",
                seeds=seeds,
                authorization_filtered_count=discovery.authorization_filtered_count,
            )
        return GraphPacketOutcome(
            payload=None,
            fault=f"{name}: {raised}",
            seeds=seeds,
            authorization_filtered_count=discovery.authorization_filtered_count,
        )

    import json

    return GraphPacketOutcome(
        payload=json.loads(packet.model_dump_json()),
        seeds=seeds,
        authorization_filtered_count=discovery.authorization_filtered_count,
    )


# ---------------------------------------------------------------------------
# CHAOS-3645: the subjectless cohort entry mode
# ---------------------------------------------------------------------------


#: How many cohort members seed the traversal.
#:
#: A different bound from :data:`MAX_SEEDS`, and for a different reason.
#: ``MAX_SEEDS`` is the contract's top-3 SUBJECT horizon -- the packet may
#: commit to at most three candidates, so reading further would be reading
#: about entities the packet cannot speak for. A scope-enumerated cohort has
#: no committed subject and every member is equally the answer, so the
#: traversal has to reach all of them or the packet's evidence describes an
#: arbitrary subset of its own cohort. Bounded regardless, because "read the
#: neighbourhood of every entity in the tenant" is the sweep the contract
#: refuses; a cohort larger than this is read partially and says so.
MAX_COHORT_SEEDS = 12


def cohort_seeds_from(
    discovery: CohortDiscovery, *, limit: int = MAX_COHORT_SEEDS
) -> list[str]:
    """The traversal seeds for an enumerated cohort, in canonical-id order.

    Ordered by canonical id rather than by any notion of strength. There is
    no ranking here to express: the members are peers by construction, and an
    order derived from measurement values would be the arm computing a
    comparison the canonical services own.
    """

    return [member.canonical_id for member in discovery.proposal.members][:limit]


def discover_cohort_for(
    *,
    question_family: QuestionFamilyID,
    projection: GraphProjection,
    authorized_entity_ids: frozenset[str],
    as_of: datetime,
) -> CohortDiscovery:
    """The second entry mode, driven by the analytical job and the grant alone.

    The 3(a) fairness surface again, and the same absence proves it: there is
    no question, no mention, no seed and no case id in this signature. A
    cohort question's text contains no subject to be fair about, so what has
    to be guaranteed here is the other half -- that the arm is not handed the
    answer's shape from outside. It is handed a family and a grant.
    """

    from dev_health_ops.context_fabric.graph_arm.cohort_discovery import (
        discover_cohort,
    )

    return discover_cohort(
        question_family=question_family,
        nodes=projection.nodes,
        edges=projection.edges,
        authorized_entity_ids=authorized_entity_ids,
        as_of=as_of,
    )


def assemble_cohort_packet(
    *,
    readout: InvestigationReadout,
    cohort: CohortDiscovery,
    question_family: QuestionFamilyID,
    comparison_shape: ComparisonShape,
    job_statement: str,
    window_start: datetime,
    window_end: datetime,
    run_id: str,
    watermark: IndexWatermark,
    signer: EvidenceReferenceSigner,
    produced_at: datetime,
) -> GraphPacketOutcome:
    """Emit a packet for an enumerated cohort, or a named refusal.

    Deliberately the SAME ``build_packet`` the seeded path calls, handed the
    same kind of proposal. A private assembly path for the second entry mode
    is how the two modes would drift into producing differently-shaped
    packets that a reader would compare as if they were alike -- and the
    trial's whole value is that the two columns are comparable.

    **This mode attributes no drivers, and that is a scope boundary rather
    than an oversight.** ``discover_drivers`` answers "what is causing THIS
    subject's state", and a scope-enumerated cohort has no subject; the
    per-member generalisation is its own capability, not a loop around this
    one. Two things were measured while establishing that, and both belong in
    the artifact rather than in a commit message:

    * running it per member DOES produce findings, so the absence here is a
      decision and not an inability;
    * the first of those findings the contract sees is refused --
      ``drv_metric_atlas_load`` is a capacity/staffing driver and the graph
      arm sets no ``staffing_qualification`` on any driver it emits, which
      the frozen contract rejects outright. That is a latent defect in the
      SEEDED path too, reachable there the moment a scored case's subject
      yields a capacity driver; it is filed rather than fixed here
      (CHAOS-3651), because a fix smuggled into a measurement run is a change
      to the thing being measured.

    A packet with no drivers already carries the contract limitation saying
    so, so a reader cannot mistake "attempted none" for "found no causes".
    """

    from dev_health_ops.context_fabric.graph_arm.packet_builder import (
        JobContext,
        TrialContext,
        build_packet,
    )

    seeds = tuple(cohort_seeds_from(cohort))

    try:
        packet = build_packet(
            readout=readout,
            job=JobContext(
                # Named for the cohort rather than for a subject, because
                # there is no subject. A job id borrowed from one member
                # would read as "this investigation was about that member".
                job_id=f"job_cohort_{question_family.value}",
                question_family=question_family,
                job_statement=job_statement,
                comparison_shape=comparison_shape,
                window_start=window_start,
                window_end=window_end,
            ),
            watermark=watermark,
            signer=signer,
            trial=TrialContext(run_id=run_id, corpus_version=CORPUS_VERSION),
            produced_at=produced_at,
            cohort=cohort.proposal,
        )
    except Exception as raised:  # noqa: BLE001 - classified, not swallowed
        name = type(raised).__name__
        outcome_kwargs = {
            "payload": None,
            "seeds": seeds,
            "authorization_filtered_count": cohort.authorization_filtered_count,
        }
        if name in _NAMED_REFUSALS:
            return GraphPacketOutcome(refusal=f"{name}: {raised}", **outcome_kwargs)  # type: ignore[arg-type]
        return GraphPacketOutcome(fault=f"{name}: {raised}", **outcome_kwargs)  # type: ignore[arg-type]

    import json

    return GraphPacketOutcome(
        payload=json.loads(packet.model_dump_json()),
        seeds=seeds,
        authorization_filtered_count=cohort.authorization_filtered_count,
    )
