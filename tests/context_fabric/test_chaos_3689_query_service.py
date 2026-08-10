"""CHAOS-3689: ``SUBJECTLESS_COHORT_DISCOVERY``'s real COMPLETED path.

Two halves:

* :data:`~.query_service._COHORT_DISCOVERY_QUESTION_FAMILY` -- the closed
  family table, tested for exhaustiveness against
  ``CohortDiscoveryFamily.__members__`` directly (never a hand-copied list
  that could drift) and for landing only on families
  ``cohort_discovery.discover_cohort`` actually supports.
* ``ProductionGraphInvestigationQuery.investigate``'s
  ``SUBJECTLESS_COHORT_DISCOVERY`` dispatch -- what this file proves is the
  WIRING (family lookup -> live snapshot -> ``discover_cohort`` -> packet
  assembly), not ``discover_cohort``'s own comparability logic, which has
  its own extensive direct coverage
  (``test_chaos_3645_cohort_discovery.py``, ``test_chaos_3667_cohort_
  ranking.py``) and its own live-corroboration thresholds this file does
  not attempt to reverse-engineer through a hand-built fixture. The one
  positive COMPLETED-path test therefore mocks ``discover_cohort`` at the
  ``query_service`` module boundary -- exactly the "already covered
  elsewhere" boundary ``TestSeededExplicitCohortCompletes`` in
  ``test_chaos_3678_query_service.py`` draws for ``cohort.build_cohort``.
  The refusal path is exercised for real, unmocked, against an empty live
  snapshot -- no reason to fake a result that trivially reproduces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest

from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    QuestionIntentID,
    SourceClass,
)
from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
from dev_health_ops.api.dev.graph_investigation_query import (
    CohortDiscoveryFamily,
    GraphInvestigationRequest,
    GraphQueryOutcome,
)
from dev_health_ops.api.dev.investigation_contract import (
    CohortInclusionBasis,
    ComparisonDimension,
)
from dev_health_ops.context_fabric.graph_arm import (
    query_service as query_service_module,
)
from dev_health_ops.context_fabric.graph_arm.cohort import (
    CohortCandidate,
    CohortEntryMode,
    CohortProposal,
)
from dev_health_ops.context_fabric.graph_arm.cohort_discovery import (
    FAMILY_CANDIDATE_KINDS,
    CohortDiscovery,
)
from dev_health_ops.context_fabric.graph_arm.query_service import (
    _COHORT_DISCOVERY_QUESTION_FAMILY,
    _MAX_COHORT_SEEDS,
    ProductionGraphInvestigationQuery,
)
from dev_health_ops.context_fabric.graph_arm.readback import InvestigationReadout
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark

_TEST_SIGNING_SECRET = "chaos-3689-query-service-test-signing-secret-not-real"
_RUN_UUID = "5c9a3f9e-1111-4222-8333-444455556666"


def _soon() -> datetime:
    return datetime.now(UTC) + timedelta(seconds=30)


def _fresh_watermark() -> IndexWatermark:
    return IndexWatermark(indexed_through=datetime.now(UTC), records_indexed=1)


def _request(
    *,
    cohort_discovery_family: CohortDiscoveryFamily = CohortDiscoveryFamily.PROJECT_CAPACITY,
    org_id: str = "org_query_service_test",
    run_id: str = _RUN_UUID,
    authorized_entity_ids: frozenset[str] = frozenset({"proj_a", "proj_b", "team_x"}),
) -> GraphInvestigationRequest:
    return GraphInvestigationRequest(
        org_id=org_id,
        run_id=run_id,
        intent_id=QuestionIntentID.DISCOVERED_COHORT,
        cardinality=Cardinality.ORGANIZATION_WIDE,
        mentions=(),
        question_text="Which projects are capacity-constrained right now?",
        authorized_entity_ids=authorized_entity_ids,
        window_start=datetime(2026, 5, 12, tzinfo=UTC),
        window_end=datetime(2026, 8, 9, tzinfo=UTC),
        cohort_discovery_family=cohort_discovery_family,
        deadline=_soon(),
    )


class TestCohortDiscoveryFamilyTable:
    def test_every_family_is_mapped(self) -> None:
        """No default branch: every member of the wire enum must appear as
        a literal key, checked against ``__members__`` directly rather than
        a hand-copied list that could silently stop tracking the enum.
        """

        assert set(_COHORT_DISCOVERY_QUESTION_FAMILY) == set(
            CohortDiscoveryFamily.__members__.values()
        )

    def test_the_table_is_not_accidentally_empty(self) -> None:
        """Anti-vacuity: the exhaustiveness check above would pass
        trivially if both sides were empty.
        """

        assert _COHORT_DISCOVERY_QUESTION_FAMILY

    def test_every_mapped_family_is_one_discover_cohort_actually_supports(
        self,
    ) -> None:
        """A family this table maps to but ``FAMILY_CANDIDATE_KINDS`` does
        not cover would raise ``UnsupportedCohortFamilyError`` on every
        real call -- a table that type-checks but always refuses.
        """

        unsupported = {
            family: question_family
            for family, question_family in _COHORT_DISCOVERY_QUESTION_FAMILY.items()
            if question_family not in FAMILY_CANDIDATE_KINDS
        }
        assert not unsupported, unsupported


@dataclass
class _FakeDriver:
    """Dispatches on which of the three declared queries was issued --
    ``_live_graph_snapshot`` calls all three (entity/observation/edge)
    unconditionally, unlike ``SEEDED_EXPLICIT_COHORT``'s narrower
    ``_live_entities``/``_live_cohort_edges``, which only ever issue two.
    """

    entity_rows: list[dict] = field(default_factory=list)
    observation_rows: list[dict] = field(default_factory=list)
    edge_rows: list[dict] = field(default_factory=list)
    queries_seen: list[str] = field(default_factory=list)

    async def execute_query(self, query: str, **params: object) -> tuple:
        self.queries_seen.append(query)
        if "RELATES_TO" in query:
            return (self.edge_rows, None, None)
        if "cf_is_entity = true" in query:
            return (self.entity_rows, None, None)
        return (self.observation_rows, None, None)


def _entity_row(
    canonical_id: str,
    display_label: str,
    *,
    kind: str = GraphEntityKind.PROJECT.value,
) -> dict:
    return {
        "canonical_id": canonical_id,
        "entity_kind": kind,
        "display_label": display_label,
        "source_class": SourceClass.WORK_GRAPH.value,
        "observed_at": "2026-05-01T00:00:00+00:00",
    }


@dataclass
class _FakeStore:
    watermark: IndexWatermark | None = None
    partition: str = "cf_query_service_test"
    _driver: object = None

    async def read_watermark(self) -> IndexWatermark:
        assert self.watermark is not None
        return self.watermark

    async def close(self) -> None:
        return None


def _factory(store: _FakeStore):
    def _build(_org_id: str) -> _FakeStore:
        return store

    return _build


@dataclass
class _FakeReader:
    readout: InvestigationReadout
    calls: list[dict] = field(default_factory=list)

    async def neighbourhood(
        self,
        *,
        org_id: str,
        seed_canonical_ids,
        authorized_entity_ids,
        max_hops: int = 3,
        budgets=None,
    ) -> InvestigationReadout:
        self.calls.append(
            {
                "org_id": org_id,
                "seed_canonical_ids": list(seed_canonical_ids),
                "authorized_entity_ids": list(authorized_entity_ids),
            }
        )
        return self.readout


def _service(
    *, driver: _FakeDriver, reader: _FakeReader
) -> ProductionGraphInvestigationQuery:
    store = _FakeStore(watermark=_fresh_watermark(), _driver=driver)
    return ProductionGraphInvestigationQuery(
        store_factory=_factory(store),
        reader_factory=lambda _store: reader,
        signer_factory=lambda: EvidenceReferenceSigner(_TEST_SIGNING_SECRET),
    )


def _empty_readout() -> InvestigationReadout:
    return InvestigationReadout(
        org_id="org_query_service_test",
        partition="cf_query_service_test",
        seed_canonical_ids=(),
        authorized_entity_ids=(),
    )


class TestSubjectlessCohortDiscoveryRefuses:
    """The real (unmocked) path: an empty live snapshot -> an empty,
    incomparable discovery -> PROVIDER_FAILURE naming the mechanism.
    Exercises the actual wiring end to end -- family lookup,
    ``_live_graph_snapshot``, ``discover_cohort`` -- without needing a
    hand-built corroborated-pressure fixture to reach a refusal.
    """

    @pytest.mark.asyncio
    async def test_no_candidates_reaches_provider_failure_naming_the_mechanism(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        driver = _FakeDriver()
        reader = _FakeReader(readout=_empty_readout())
        service = _service(driver=driver, reader=reader)

        result = await service.investigate(_request())

        assert result.outcome is GraphQueryOutcome.PROVIDER_FAILURE
        assert result.packet is None
        assert result.diagnostic is not None
        assert "subjectless_cohort_discovery" in result.diagnostic
        # Proves the wiring actually reached discover_cohort rather than
        # failing earlier (a store/driver error would ALSO produce
        # PROVIDER_FAILURE naming the mechanism -- see the module's broad
        # except -- so this distinguishes "reached and refused" from
        # "crashed before reaching").
        assert not reader.calls, (
            "no comparable cohort means no seeds, which means neighbourhood() "
            "must never have been called at all"
        )

    @pytest.mark.asyncio
    async def test_the_live_snapshot_is_fetched_from_this_requests_own_partition(
        self, monkeypatch
    ) -> None:
        """A targeted proof of the adapter wiring itself: all three
        declared queries are actually issued against the real driver, not
        skipped or short-circuited.
        """

        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        driver = _FakeDriver()
        reader = _FakeReader(readout=_empty_readout())
        service = _service(driver=driver, reader=reader)

        await service.investigate(_request())

        assert any("RELATES_TO" in query for query in driver.queries_seen)
        assert any("cf_is_entity = true" in query for query in driver.queries_seen)
        assert any("cf_is_entity = false" in query for query in driver.queries_seen)


def _canned_discovery(*, members: tuple[str, ...]) -> CohortDiscovery:
    """A comparable ``CohortDiscovery`` for the mocked positive path --
    not a claim about what real corroborated pressure data would look
    like, only a valid shape ``build_production_packet`` accepts. Real
    corroboration thresholds are ``discover_cohort``'s own tested
    responsibility, not this wiring test's.
    """

    candidates = tuple(
        CohortCandidate(
            canonical_id=canonical_id,
            kind=GraphEntityKind.PROJECT,
            display_label=canonical_id,
            basis_anchors=((CohortInclusionBasis.SHARED_TEAM_OWNERSHIP, ("team_x",)),),
        )
        for canonical_id in members
    )
    return CohortDiscovery(
        proposal=CohortProposal(
            subject_id="",
            members=candidates,
            exclusions=(),
            dimensions=(ComparisonDimension.DELIVERY_THROUGHPUT,),
            truncated=False,
            truncated_count=0,
            authorization_filtered_count=0,
            entry_mode=CohortEntryMode.SCOPE_ENUMERATED,
        ),
        candidate_kinds=(GraphEntityKind.PROJECT,),
        universe_size=len(members),
        authorization_filtered_count=0,
    )


class TestSubjectlessCohortDiscoveryCompletes:
    """The mocked positive path -- see the module docstring for why
    ``discover_cohort`` itself is mocked here rather than fed a hand-built
    corroborated fixture.
    """

    @pytest.mark.asyncio
    async def test_a_comparable_cohort_completes_with_a_real_packet(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        driver = _FakeDriver(
            entity_rows=[
                _entity_row("proj_a", "Project A"),
                _entity_row("proj_b", "Project B"),
            ]
        )
        readout = InvestigationReadout(
            org_id="org_query_service_test",
            partition="cf_query_service_test",
            seed_canonical_ids=("proj_a", "proj_b"),
            authorized_entity_ids=("proj_a", "proj_b", "team_x"),
        )
        reader = _FakeReader(readout=readout)
        service = _service(driver=driver, reader=reader)

        discovery = _canned_discovery(members=("proj_a", "proj_b"))
        monkeypatch.setattr(
            query_service_module, "discover_cohort", lambda **_kwargs: discovery
        )

        result = await service.investigate(_request())

        assert result.outcome is GraphQueryOutcome.COMPLETED
        assert result.packet is not None
        assert result.packet.comparison_cohort.comparison_shape.value == (
            "discovered_cohort"
        )
        assert sorted(
            member.canonical_id for member in result.packet.comparison_cohort.members
        ) == ["proj_a", "proj_b"]
        # No drivers attributed -- the same scope boundary
        # SEEDED_EXPLICIT_COHORT already draws (no subject to explain).
        assert not result.packet.driver_analysis.candidates
        assert len(reader.calls) == 1
        assert sorted(reader.calls[0]["seed_canonical_ids"]) == ["proj_a", "proj_b"]

    @pytest.mark.asyncio
    async def test_seeds_are_capped_at_max_cohort_seeds_in_canonical_id_order(
        self, monkeypatch
    ) -> None:
        """Never a strength/relevance ranking this arm does not own --
        canonical-id order, capped, mirroring the trial's own
        ``cohort_seeds_from`` exactly.
        """

        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        many_members = tuple(f"proj_{i:02d}" for i in range(_MAX_COHORT_SEEDS + 5))
        driver = _FakeDriver(entity_rows=[_entity_row(m, m) for m in many_members])
        readout = InvestigationReadout(
            org_id="org_query_service_test",
            partition="cf_query_service_test",
            seed_canonical_ids=many_members[:_MAX_COHORT_SEEDS],
            authorized_entity_ids=many_members,
        )
        reader = _FakeReader(readout=readout)
        service = _service(driver=driver, reader=reader)

        # Shuffle the discovery's own member order to prove the CAP+ORDER
        # comes from this wiring's own slicing, not from an
        # already-sorted discover_cohort output it happens to inherit.
        shuffled = tuple(reversed(many_members))
        discovery = _canned_discovery(members=shuffled)
        monkeypatch.setattr(
            query_service_module, "discover_cohort", lambda **_kwargs: discovery
        )

        result = await service.investigate(
            _request(authorized_entity_ids=frozenset(many_members))
        )

        assert result.outcome is GraphQueryOutcome.COMPLETED
        assert len(reader.calls) == 1
        seeds = reader.calls[0]["seed_canonical_ids"]
        assert len(seeds) == _MAX_COHORT_SEEDS
        assert seeds == sorted(many_members)[:_MAX_COHORT_SEEDS]
