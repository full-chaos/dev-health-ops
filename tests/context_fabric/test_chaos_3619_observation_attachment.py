"""CHAOS-3619 (H3): the live reader must recover what an observation is about.

Why this is a trial precondition rather than a nice-to-have, stated as the
measurement that forced it. Driving the CHAOS-3616 corpus into the live
FalkorDB trial store and reading one neighbourhood back (``org_helio``, seed
``proj_vertex``, analyst grant, three hops) produced:

    reference reader : 17 entities, 27 paths, 39 observations, 39 attached
    live reader      : 17 entities, 27 paths,  0 observations

Not "attached to nothing" -- *absent*. ``_traverse`` keeps an observation only
when one of its subjects is in the visited set, so an empty subject list drops
every record. A comparative trial run against that reader would score the
graph arm as emitting packets with no evidence, no cross-source association
and no drivers, and that number would be a fact about an unimplemented
readback rather than about graph assistance.

Three mechanisms are asserted here, separately, because they fail
independently and a single end-to-end assertion would let any two of them be
broken while the third carried the test:

1. the write records the attachment (``to_graphiti_nodes``);
2. the read recovers it (``_OBSERVATION_QUERY`` + ``LiveGraphReader``);
3. the *declaration* tracks the attestation rather than being a constant.

(3) is the one that matters most and is the easiest to fake. The previous
revision declared ``observation_attachment_available = False`` as a literal,
and flipping a literal to ``True`` would turn every assertion below green
while recovering nothing. So the capability is derived from what the
partition itself attests, and the negative control writes a partition under a
different encoding and requires the reader to say ``False`` again.

The separator case is not incidental. Attachment makes a canonical id a
member of a joined multi-valued property for the first time, and
``projection.py`` checked canonical ids for control characters only. A
comma-bearing canonical id would split into two subjects on read -- an
attachment no source supplied -- which is exactly what refuse-don't-sanitize
exists to stop.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace

import pytest
import pytest_asyncio

from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.context_fabric.graph_arm import corpus_adapter as adapter
from dev_health_ops.context_fabric.graph_arm.backend import (
    ATTACHMENT_ENCODING,
    ATTACHMENT_ENCODING_ATTRIBUTE,
    OBSERVATION_SUBJECTS_ATTRIBUTE,
    attachment_encoding_supported,
    to_graphiti_nodes,
)
from dev_health_ops.context_fabric.graph_arm.projection import (
    GraphProjection,
    ProjectionError,
    build_projection,
)
from dev_health_ops.context_fabric.graph_arm.readback import (
    LiveGraphReader,
    ProjectionGraphReader,
)
from dev_health_ops.context_fabric.graph_arm.records import (
    CanonicalRef,
    EntityRecord,
    IngestionBatch,
    ObservationRecord,
)
from dev_health_ops.context_fabric.graph_arm.store import GraphArmStore
from dev_health_ops.context_fabric.graph_arm.vocabulary import (
    GraphEntityKind,
    GraphObservationKind,
)
from tests.context_fabric import live_gate

#: The corpus subject the spike measured, and a hop budget that reaches
#: observations. A seed with no observations in range would make every
#: assertion below pass while recovering nothing.
_SEED = "proj_vertex"
_MAX_HOPS = 3


def _corpus_projection(org_id: str = world.ORG_HELIO) -> GraphProjection:
    return build_projection(_reorg(adapter.corpus_batch(world.ORG_HELIO), org_id))


def _grant() -> list[str]:
    return sorted(adapter.authorized_entity_ids_for(world.PRINCIPAL_ANALYST))


def _unique_org(prefix: str) -> str:
    """A throwaway tenant id, so live runs never share a partition.

    Mirrors ``test_chaos_3617_live_store._unique_org``. Not hygiene theatre:
    the trial store is one container shared by every lane on this machine,
    and two suites writing ``cf_trial_org_helio`` concurrently would each see
    the other's nodes -- which reads as a graph difference, not as a
    collision, and would be debugged as one.
    """

    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _reorg(batch: IngestionBatch, org_id: str) -> IngestionBatch:
    """The corpus batch, rebound to a throwaway tenant.

    Only ``org_id`` changes. Canonical ids, kinds, attachments and the
    per-principal grant are all tenant-independent, so the projection under
    test is the corpus's own -- this is an addressing change, not a fixture.
    """

    def rebind(record: object) -> object:
        return replace(record, org_id=org_id)  # type: ignore[type-var]

    return replace(
        batch,
        org_id=org_id,
        entities=tuple(rebind(item) for item in batch.entities),  # type: ignore[misc]
        relationships=tuple(rebind(item) for item in batch.relationships),  # type: ignore[misc]
        observations=tuple(rebind(item) for item in batch.observations),  # type: ignore[misc]
        documents=tuple(rebind(item) for item in batch.documents),  # type: ignore[misc]
    )


# ---------------------------------------------------------------------------
# 1. The write records the attachment
# ---------------------------------------------------------------------------


class TestTheWriteRecordsTheAttachment:
    def test_an_attached_observation_carries_its_subject_canonical_ids(self) -> None:
        """The property the live read depends on is actually written.

        Asserted against the projection's own attachment map rather than a
        hand-written expectation, so this cannot drift into agreeing with a
        writer that dropped half the subjects.
        """

        live_gate.require_graphiti_extra()
        from dev_health_ops.context_fabric.graph_arm.backend import (
            DeterministicEmbedder,
        )

        projection = _corpus_projection()
        assert projection.observation_attachments, (
            "the corpus produced no attachments at all; every assertion below "
            "would pass while measuring nothing"
        )
        canonical_by_uuid = {node.uuid: node.canonical_id for node in projection.nodes}
        nodes = to_graphiti_nodes(projection, DeterministicEmbedder())
        by_uuid = {node.uuid: node for node in nodes}

        checked = 0
        for (
            observation_uuid,
            subject_uuids,
        ) in projection.observation_attachments.items():
            expected = sorted(
                canonical_by_uuid[uuid]
                for uuid in subject_uuids
                if uuid in canonical_by_uuid
            )
            if not expected:
                continue
            stored = by_uuid[observation_uuid].attributes.get(
                OBSERVATION_SUBJECTS_ATTRIBUTE
            )
            assert stored is not None, (
                f"observation {observation_uuid} is attached to {expected} in "
                "the projection and stores no subject property; the live "
                "reader cannot recover an attachment that was never written"
            )
            assert str(stored).split(",") == expected
            checked += 1
        assert checked, "no attached observation was checked; vacuous"

    def test_every_node_attests_the_attachment_encoding(self) -> None:
        """The attestation is per partition, so it must be on every node.

        A subset would make the reader's DISTINCT read return the encoding
        alongside ``None``, which is the mixed-partition case -- and the
        reader would correctly refuse a partition the writer thought it had
        fully covered.
        """

        live_gate.require_graphiti_extra()
        from dev_health_ops.context_fabric.graph_arm.backend import (
            DeterministicEmbedder,
        )

        nodes = to_graphiti_nodes(_corpus_projection(), DeterministicEmbedder())
        assert nodes, "no nodes were produced; vacuous"
        attested = {
            node.attributes.get(ATTACHMENT_ENCODING_ATTRIBUTE) for node in nodes
        }
        assert attested == {ATTACHMENT_ENCODING}


# ---------------------------------------------------------------------------
# 3. The declaration tracks the attestation (pure, so it is always measured)
# ---------------------------------------------------------------------------


class TestTheDeclarationTracksTheAttestation:
    """The mechanism, tested without a server so it cannot be skipped.

    ``observation_attachment_available`` used to be a literal. These four
    cases are what makes the replacement a derivation: each one changes only
    what the partition attests, and each one must change the answer.
    """

    def test_an_unattested_partition_is_not_available(self) -> None:
        assert attachment_encoding_supported(set()) is False, (
            "GUARD attachment_encoding_unattested -- a partition that attests "
            "no encoding was read as attachment-capable; that is every "
            "partition written before the encoding existed"
        )

    def test_the_understood_encoding_is_available(self) -> None:
        assert attachment_encoding_supported({ATTACHMENT_ENCODING}) is True, (
            "GUARD attachment_encoding_understood -- the reader rejects the "
            "encoding it writes itself, so no partition would ever be usable"
        )

    def test_another_encoding_is_not_available(self) -> None:
        """A future or foreign writer is refused, not assumed compatible."""

        assert attachment_encoding_supported({"canonical_ids.v2"}) is False, (
            "GUARD attachment_encoding_unknown -- an encoding this revision "
            "does not understand was accepted, so a newer writer's layout "
            "would be misparsed rather than declined"
        )

    def test_a_mixed_partition_is_not_available(self) -> None:
        """Half a partition's worth of attachment is not an attachment."""

        assert (
            attachment_encoding_supported({ATTACHMENT_ENCODING, "canonical_ids.v2"})
            is False
        ), (
            "GUARD attachment_encoding_mixed -- a partition written by two "
            "writers was accepted, so drivers would be attributed from "
            "whichever half happened to carry attachment"
        )


# ---------------------------------------------------------------------------
# The separator promotion attachment forces
# ---------------------------------------------------------------------------


def _batch_with_canonical_id(
    entity_id: str = "proj_clean", observation_id: str = "obs_clean"
) -> IngestionBatch:
    """One entity and one observation about it, under chosen ids."""

    return IngestionBatch(
        org_id="org_sep",
        entities=(
            EntityRecord(
                org_id="org_sep",
                kind=GraphEntityKind.PROJECT,
                canonical_id=entity_id,
                display_label="Separator probe",
                source_class=SourceClass.STATUS_CHANGE,
                observed_at=world.TRIAL_NOW,
            ),
        ),
        observations=(
            ObservationRecord(
                org_id="org_sep",
                kind=GraphObservationKind.STATUS_CHANGE,
                canonical_id=observation_id,
                title="Separator probe observation",
                source_class=SourceClass.STATUS_CHANGE,
                observed_at=world.TRIAL_NOW,
                subjects=(
                    CanonicalRef(kind=GraphEntityKind.PROJECT, canonical_id=entity_id),
                ),
            ),
        ),
    )


class TestACanonicalIdMayNotCarryAJoinByte:
    """Attachment makes a canonical id a list member for the first time.

    Before attachment, a canonical id was only ever stored as a single-valued
    property, so a comma in one was harmless. Joining subjects on a comma
    makes ``proj,other`` read back as two subjects -- an attachment no source
    supplied. Refused at projection time rather than escaped on write, which
    is the rule the alias and repository-id fields already follow.

    **Only the comma discriminates, and saying so is the point.** The unit
    separator (0x1f) is also a C0 control character, so the pre-H3
    ``_reject_control_characters`` call already refused it -- a 0x1f case
    passes with the promotion and without it, and presenting it as evidence
    for the promotion would be an inaccurate coverage claim. Verified by
    planting the pre-H3 check: exactly one case flipped, the comma one. The
    0x1f case is kept as a regression pin on behaviour that already existed,
    labelled as such.
    """

    @pytest.mark.parametrize(
        ("separator", "discriminates"),
        [(",", True), ("\x1f", False)],
        ids=["comma_new_in_h3", "unit_separator_already_refused"],
    )
    def test_an_entity_canonical_id_carrying_a_join_byte_is_refused(
        self, separator: str, discriminates: bool
    ) -> None:
        with pytest.raises(ProjectionError) as raised:
            build_projection(
                _batch_with_canonical_id(entity_id=f"proj{separator}injected")
            )
        assert "canonical_id" in str(raised.value)
        if discriminates:
            # The comma message must name the join mechanism, not the control
            # -character one -- otherwise the promotion could be "passing"
            # via a check that was always there.
            assert "comma" in str(raised.value)

    def test_an_observation_canonical_id_carrying_a_comma_is_refused(self) -> None:
        """The second promoted site, which the entity case does not cover.

        ``_observation_node`` got the same promotion and would otherwise be
        untested: every parametrised case above varies the ENTITY id and
        leaves the observation id clean, so a promotion applied to only one
        of the two call sites would still show all green.
        """

        with pytest.raises(ProjectionError) as raised:
            build_projection(_batch_with_canonical_id(observation_id="obs,injected"))
        assert "comma" in str(raised.value)

    def test_the_same_batch_without_the_join_byte_projects(self) -> None:
        """The control every refusal above needs.

        Without it, a batch that could never project for an unrelated reason
        would make the refusals look like the separator check firing.
        """

        projection = build_projection(_batch_with_canonical_id())
        assert projection.nodes


# ---------------------------------------------------------------------------
# 2. The read recovers it -- against the live store
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def corpus_store(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[GraphArmStore, GraphProjection]]:
    """The CHAOS-3616 corpus, in the real trial store.

    The first fixture in the repository to join corpus ingestion to the live
    backend; every prior live test drove the synthetic ``alpha_batch``.
    """

    config = live_gate.require_live_store()
    monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
    live_gate.require_flag_state()
    org_id = _unique_org("orgh3")
    projection = _corpus_projection(org_id)
    store = GraphArmStore.for_org(org_id, config=config)
    try:
        await store.build_indices()
        await store.write_projection(projection)
        yield store, projection
    finally:
        try:
            await store.purge_org()
        finally:
            await store.close()


@pytest.mark.graphiti
@pytest.mark.asyncio
class TestTheLiveReadRecoversTheAttachment:
    async def test_the_live_reader_returns_attached_observations(
        self, corpus_store: tuple[GraphArmStore, GraphProjection]
    ) -> None:
        """The measurement that made H3 a precondition, inverted.

        Asserts the *state the system exists to reach* -- observations, with
        subjects -- not that the query ran.
        """

        store, _ = corpus_store
        live = await LiveGraphReader(store).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=[_SEED],
            authorized_entity_ids=_grant(),
            max_hops=_MAX_HOPS,
        )
        assert live.observation_attachment_available is True
        assert live.observations, (
            "the live reader recovered no observations at all; this is the "
            "exact pre-H3 state the spike measured"
        )
        assert all(item.subject_canonical_ids for item in live.observations), (
            "the reader declares attachment and returned an unattached record"
        )

    async def test_the_live_attachment_equals_the_reference_attachment(
        self, corpus_store: tuple[GraphArmStore, GraphProjection]
    ) -> None:
        """A differential, because 'non-empty' is a weak claim.

        The reference reader has the attachment map outright. Recovering
        *some* subjects would satisfy the test above while dropping half of
        them, and a dropped subject silently narrows every driver the arm can
        attribute.
        """

        store, projection = corpus_store
        grant = _grant()
        live = await LiveGraphReader(store).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=[_SEED],
            authorized_entity_ids=grant,
            max_hops=_MAX_HOPS,
        )
        reference = await ProjectionGraphReader(projection).neighbourhood(
            org_id=store.org_id,
            seed_canonical_ids=[_SEED],
            authorized_entity_ids=grant,
            max_hops=_MAX_HOPS,
        )

        def attachment(readout: object) -> dict[str, tuple[str, ...]]:
            return {
                item.canonical_id: tuple(sorted(item.subject_canonical_ids))
                for item in readout.observations  # type: ignore[attr-defined]
            }

        assert reference.observations, "the reference returned none; vacuous"
        assert attachment(live) == attachment(reference)

    async def test_an_unrecognised_encoding_makes_the_reader_decline_again(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The negative control that keeps the declaration honest.

        Flipping a literal ``False`` to ``True`` would pass every test above.
        This writes a partition under an encoding the reader does not
        understand and requires it to decline -- so the declaration is
        observed to be a derivation from what the partition attests, not a
        constant that happens to be right today.

        **The patch is undone before the read, and that is the whole test.**
        The first version left it in place, which moved the writer AND the
        reader's notion of "understood" together: the partition attested
        ``...v9``, the reader asked whether the encoding equalled ``...v9``,
        they matched, and the control passed while proving nothing. That is
        the exact vacuity shape this file guards against elsewhere, and it
        appeared in the guard itself first. Patching only the WRITE is what
        makes this a genuine version skew.
        """

        from dev_health_ops.context_fabric.graph_arm import backend

        config = live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_PROJECTION_ENABLED", "1")
        org_id = _unique_org("orgh3skew")
        projection = _corpus_projection(org_id)
        store = GraphArmStore.for_org(org_id, config=config)
        try:
            await store.build_indices()
            monkeypatch.setattr(
                backend, "ATTACHMENT_ENCODING", "canonical_ids.from_the_future_v9"
            )
            await store.write_projection(projection)
            monkeypatch.setattr(backend, "ATTACHMENT_ENCODING", ATTACHMENT_ENCODING)
            # Anti-vacuity: the reader must be back on its own encoding, or
            # "declined" would only mean "the patch is still active".
            assert backend.ATTACHMENT_ENCODING == ATTACHMENT_ENCODING

            live = await LiveGraphReader(store).neighbourhood(
                org_id=store.org_id,
                seed_canonical_ids=[_SEED],
                authorized_entity_ids=_grant(),
                max_hops=_MAX_HOPS,
            )
            assert live.observation_attachment_available is False, (
                "the reader accepted an encoding it does not understand; the "
                "declaration is a constant, not a derivation"
            )
            # And the consequence must be the pre-H3 behaviour rather than a
            # half state: an attachment the reader may not trust must not
            # reach a caller that would attribute a driver to it.
            assert all(not item.subject_canonical_ids for item in live.observations)
        finally:
            try:
                await store.purge_org()
            finally:
                await store.close()


def test_the_seed_used_by_the_live_tests_reaches_observations() -> None:
    """Anti-vacuity for the whole live class, run without a server.

    Every live assertion above rests on this seed reaching observations in
    the reference reader. If a corpus edit ever moves them out of range, the
    live tests would pass by finding nothing on both sides; this fails
    instead, and it fails in the standard suite rather than only in the live
    lane.
    """

    reference = asyncio.run(
        ProjectionGraphReader(_corpus_projection()).neighbourhood(
            org_id=world.ORG_HELIO,
            seed_canonical_ids=[_SEED],
            authorized_entity_ids=_grant(),
            max_hops=_MAX_HOPS,
        )
    )
    assert reference.observations, (
        f"seed {_SEED!r} reaches no observations within {_MAX_HOPS} hops, so "
        "the live attachment tests would compare two empty sets"
    )
    assert all(item.subject_canonical_ids for item in reference.observations)
