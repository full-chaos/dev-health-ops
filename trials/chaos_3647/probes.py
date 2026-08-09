"""Planted probes that make the authorization verdict mean something.

The eight corpus ambiguity questions withheld **nothing** on the semantic
leg: no query happened to be near the restricted project, so
``authorization_filtered_count`` was zero on every row. A clean audit
produced that way is not evidence the filter works — it is evidence the
filter was never asked. Reporting it as "authorization held on the semantic
path" would be the exact inaccurate-coverage claim that stops a reader
checking.

So the filter is asked, deliberately, with queries chosen to hit it:

``restricted_exact_label``
    The restricted project's own display label, typed by a principal who
    cannot see it. The corpus plants ``proj_quarry`` **inside the analyst's
    own tenant** precisely so no tenant comparison catches it, and a
    similarity search has no notion of a grant at all.

``restricted_topical``
    A colloquial phrase near the restricted project rather than its name.
    Separate from the exact-label probe because they fail differently: a
    filter keyed on the query text would pass the second and fail the first.

``cross_tenant_near_duplicate``
    The label ``lumen_proj_acr`` shares verbatim with ``proj_acr`` in another
    tenant. Both partitions are written before this runs — a cross-partition
    assertion against a store where the other partition is empty is an
    assertion that cannot fail.

``unrestricted_control``
    A label the principal may see. The control: a probe suite that reports a
    withholding on every query is measuring its own eagerness, not the
    boundary.

**A probe that does not reach the filter is a FAILED MEASUREMENT, not a
passed check.** :attr:`ProbeOutcome.effective` is false when retrieval never
surfaced the restricted entity at all, and the runner reports that as loudly
as a leak: it means the probe needs a better query, not that the arm is safe.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm.identity import partition_for_org
from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
    retrieve_candidates,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from dev_health_ops.context_fabric.graph_arm.semantic_retrieval import (
        RetrievalStore,
    )

__all__ = [
    "PROBES",
    "AuthorizationProbe",
    "ProbeOutcome",
    "run_probe",
]


@dataclass(frozen=True, slots=True)
class AuthorizationProbe:
    """One query aimed at the authorization boundary, and what must happen."""

    probe_id: str
    query: str
    principal_id: str
    #: The entity this probe exists to try to pull through the filter. Empty
    #: for the control, which is aimed at nothing.
    target_entity_id: str
    #: True when the target is expected to be OUTSIDE the principal's grant,
    #: so surfacing-then-withholding is the correct behaviour.
    target_is_restricted: bool
    rationale: str
    #: Partition the target actually lives in, when that is not the queried
    #: one. Set for the cross-tenant probe and load-bearing there: the group
    #: filter excludes the other tenant's node inside the Cypher query, so it
    #: never reaches the authorization step and "nothing was withheld" is
    #: indistinguishable from "nothing exists". Running the identical query
    #: against the partition the target lives in, with the principal who may
    #: see it, converts that into a differential: present and ranked THERE,
    #: absent HERE, therefore the partition filter is what removed it.
    presence_partition: str = ""
    presence_principal_id: str = ""


PROBES: tuple[AuthorizationProbe, ...] = (
    AuthorizationProbe(
        probe_id="restricted_exact_label",
        query="Quarry Compliance",
        principal_id=world.PRINCIPAL_ANALYST,
        target_entity_id=world.PROJ_QUARRY,
        target_is_restricted=True,
        rationale=(
            "The restricted project's verbatim display label, which is also "
            "the text its node vector was built from. If any query retrieves "
            "it, this one does."
        ),
    ),
    AuthorizationProbe(
        probe_id="restricted_topical",
        query="how is the compliance work going",
        principal_id=world.PRINCIPAL_ANALYST,
        target_entity_id=world.PROJ_QUARRY,
        target_is_restricted=True,
        rationale=(
            "A colloquial route to the same entity. A filter keyed on query "
            "text rather than on the grant passes the exact-label probe and "
            "fails this one."
        ),
    ),
    AuthorizationProbe(
        probe_id="cross_tenant_near_duplicate",
        query="Agent Context Runtime",
        principal_id=world.PRINCIPAL_ANALYST,
        target_entity_id=world.LUMEN_PROJ_ACR,
        target_is_restricted=True,
        rationale=(
            "A label shared verbatim across two tenants. The Lumen partition "
            "is written before this runs, so the partition filter has "
            "something real to exclude — and the presence check proves the "
            "same query does retrieve the Lumen node from the Lumen "
            "partition, which is what makes its absence here a measurement."
        ),
        presence_partition=partition_for_org(world.ORG_LUMEN),
        presence_principal_id=world.PRINCIPAL_LUMEN,
    ),
    AuthorizationProbe(
        probe_id="unrestricted_control",
        query="Identity Platform Rewrite",
        principal_id=world.PRINCIPAL_ANALYST,
        target_entity_id=world.PROJ_IDENTITY_REWRITE,
        target_is_restricted=False,
        rationale=(
            "The control. A probe suite that withholds on every query is "
            "measuring its own eagerness rather than the boundary."
        ),
    ),
)


@dataclass(frozen=True, slots=True)
class ProbeOutcome:
    """What one probe measured."""

    probe_id: str
    query: str
    principal_id: str
    target_entity_id: str
    target_is_restricted: bool
    #: Did retrieval surface the target at all, before authorization?
    target_retrieved: bool
    #: Did the target reach a rank in the authorized result?
    target_ranked: bool
    authorization_filtered_count: int
    withheld_canonical_ids: tuple[str, ...]
    ranked_canonical_ids: tuple[str, ...]
    #: Whether this probe actually exercised the thing it exists to
    #: exercise. False is a measurement failure, never a clean result.
    effective: bool
    verdict: str
    detail: str
    #: Cross-tenant probe only: did the identical query rank the target in
    #: the partition the target lives in? ``None`` where no presence check
    #: applies. ``False`` means the probe proved nothing — the query cannot
    #: retrieve the target anywhere, so its absence here is not evidence the
    #: partition filter did anything.
    target_present_in_home_partition: bool | None = None


async def run_probe(
    probe: AuthorizationProbe,
    *,
    store: RetrievalStore,
    authorized_entity_ids: frozenset[str],
    presence_store: RetrievalStore | None = None,
    presence_authorized_entity_ids: frozenset[str] | None = None,
) -> ProbeOutcome:
    """Run one probe and judge it. Never returns a bare boolean.

    ``presence_store`` runs the *identical* query against the tenant the
    target actually belongs to. Only the cross-tenant probe needs it, and it
    needs it badly: Graphiti filters ``group_id`` inside the Cypher, so the
    other tenant's node never reaches the authorization step and an absence
    here proves nothing on its own.
    """

    partition = store.partition
    retrieval = await retrieve_candidates(
        probe.query,
        store=store,
        authorized_entity_ids=authorized_entity_ids,
    )

    present_at_home: bool | None = None
    if probe.presence_partition:
        if presence_store is None or presence_authorized_entity_ids is None:
            raise ValueError(
                f"probe {probe.probe_id!r} declares a presence partition but "
                "no presence store was supplied; without it the probe "
                "cannot distinguish 'the filter excluded it' from 'it was "
                "never there', which is the only thing it measures"
            )
        if presence_store.partition != probe.presence_partition:
            raise ValueError(
                f"probe {probe.probe_id!r} expects presence partition "
                f"{probe.presence_partition!r} but the supplied store is open "
                f"for {presence_store.partition!r}; a presence check against "
                "the wrong tenant would report the differential backwards"
            )
        home = await retrieve_candidates(
            probe.query,
            store=presence_store,
            authorized_entity_ids=presence_authorized_entity_ids,
        )
        present_at_home = probe.target_entity_id in {
            candidate.canonical_id for candidate in home.candidates
        }
    ranked = tuple(candidate.canonical_id for candidate in retrieval.candidates)
    surfaced = set(retrieval.retrieved_before_authorization) | set(
        retrieval.withheld_canonical_ids
    )
    target_retrieved = probe.target_entity_id in surfaced
    target_ranked = probe.target_entity_id in ranked

    if not probe.target_is_restricted:
        effective = True
        if target_ranked and retrieval.authorization_filtered_count == 0:
            verdict, detail = (
                "pass",
                "an authorized target was ranked and nothing was withheld, "
                "so the filter is not simply withholding everything",
            )
        elif not target_ranked:
            verdict, detail = (
                "fail",
                f"the control target {probe.target_entity_id} was not ranked; "
                "retrieval cannot reach an entity the principal may see, so "
                "no withholding result from this suite is interpretable",
            )
        else:
            verdict, detail = (
                "fail",
                f"the control withheld {retrieval.authorization_filtered_count} "
                "entities on a query aimed at nothing restricted",
            )
        return ProbeOutcome(
            probe_id=probe.probe_id,
            query=probe.query,
            principal_id=probe.principal_id,
            target_entity_id=probe.target_entity_id,
            target_is_restricted=probe.target_is_restricted,
            target_retrieved=target_retrieved,
            target_ranked=target_ranked,
            authorization_filtered_count=retrieval.authorization_filtered_count,
            withheld_canonical_ids=retrieval.withheld_canonical_ids,
            ranked_canonical_ids=ranked,
            effective=effective,
            verdict=verdict,
            detail=detail,
            target_present_in_home_partition=present_at_home,
        )

    if target_ranked:
        verdict = "fail"
        detail = (
            f"{probe.target_entity_id} reached rank "
            f"{ranked.index(probe.target_entity_id)} for a principal who "
            "cannot see it; the filter did not hold"
        )
        effective = True
    elif target_retrieved:
        verdict = "pass"
        detail = (
            f"retrieval surfaced {probe.target_entity_id} and authorization "
            "removed it before ranking; the filter was exercised and held"
        )
        effective = True
    elif present_at_home:
        # Excluded inside the query rather than at the authorization step.
        # That is the CORRECT mechanism for a cross-partition exclusion, and
        # it is a measurement only because the presence check proved the
        # same query ranks the target in the partition it lives in.
        verdict = "pass"
        detail = (
            f"the identical query ranked {probe.target_entity_id} in its own "
            f"partition {probe.presence_partition} and returned it nowhere in "
            f"{partition}; the group filter is what removed it, and the "
            "presence check is what makes that a measurement rather than an "
            "empty result"
        )
        effective = True
    else:
        # The dangerous case: nothing leaked, and nothing was tested.
        verdict = "not_measured"
        detail = (
            f"retrieval never surfaced {probe.target_entity_id}, so nothing "
            "excluded it and nothing was measured"
            + (
                f"; the presence check could not rank it in "
                f"{probe.presence_partition} either, so this query cannot "
                "retrieve the target anywhere and the probe needs a better one"
                if probe.presence_partition
                else ". This is a failed measurement, not a clean result: the "
                "probe needs a query that retrieves the target"
            )
        )
        effective = False

    return ProbeOutcome(
        probe_id=probe.probe_id,
        query=probe.query,
        principal_id=probe.principal_id,
        target_entity_id=probe.target_entity_id,
        target_is_restricted=probe.target_is_restricted,
        target_retrieved=target_retrieved,
        target_ranked=target_ranked,
        authorization_filtered_count=retrieval.authorization_filtered_count,
        withheld_canonical_ids=retrieval.withheld_canonical_ids,
        ranked_canonical_ids=ranked,
        effective=effective,
        verdict=verdict,
        detail=detail,
        target_present_in_home_partition=present_at_home,
    )
