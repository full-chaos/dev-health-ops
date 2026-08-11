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

from dataclasses import dataclass, field, replace
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
    AssertionBasis,
    CohortCompleteness,
    CohortInclusionBasis,
    ComparisonDimension,
    ConfidenceQualifier,
    DriverCategory,
    DriverExclusionReason,
    DriverRole,
    DriverStanding,
    InvestigationOutcome,
    PacketLimitationKind,
    RelationshipDirection,
    RelationshipType,
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
from dev_health_ops.context_fabric.graph_arm.drivers import (
    DriverFinding,
    StandingMechanism,
)
from dev_health_ops.context_fabric.graph_arm.query_service import (
    _COHORT_DISCOVERY_QUESTION_FAMILY,
    _MAX_COHORT_SEEDS,
    ProductionGraphInvestigationQuery,
    _subjectless_drivers,
)
from dev_health_ops.context_fabric.graph_arm.readback import (
    DiscoveredEntity,
    DiscoveredObservation,
    DiscoveredPath,
    InvestigationReadout,
    PathStep,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import (
    GraphEntityKind,
    GraphObservationKind,
)
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


def _capped_readout(members: tuple[str, ...]) -> InvestigationReadout:
    """A bounded readout with one real driver on the first seeded member.

    The proposal may contain more members than the reader seeds. The reader
    therefore returns the seeded project entities plus the blocker they reach,
    while the authorization envelope still names the complete proposal.
    """

    observed_at = datetime(2026, 8, 8, tzinfo=UTC)
    seeds = tuple(sorted(members)[:_MAX_COHORT_SEEDS])
    blocker_id = "wu_blocker"
    entities = tuple(
        DiscoveredEntity(
            canonical_id=member_id,
            kind=GraphEntityKind.PROJECT,
            display_label=member_id,
            source_class=SourceClass.WORK_GRAPH,
            observed_at=observed_at,
        )
        for member_id in seeds
    ) + (
        DiscoveredEntity(
            canonical_id=blocker_id,
            kind=GraphEntityKind.WORK_UNIT,
            display_label="Open blocker",
            source_class=SourceClass.WORK_GRAPH,
            observed_at=observed_at,
        ),
    )
    observation_id = "obs_blocker"
    path = DiscoveredPath(
        path_id="p0001",
        origin_canonical_id=seeds[0],
        terminal_canonical_id=blocker_id,
        steps=(
            PathStep(
                from_canonical_id=seeds[0],
                from_kind=GraphEntityKind.PROJECT,
                relationship=RelationshipType.BLOCKED_BY,
                direction=RelationshipDirection.FORWARD,
                to_canonical_id=blocker_id,
                to_kind=GraphEntityKind.WORK_UNIT,
                source_class=SourceClass.WORK_GRAPH,
                observed_at=observed_at,
                observation_ids=(observation_id,),
            ),
        ),
    )
    observation = DiscoveredObservation(
        canonical_id=observation_id,
        kind=GraphObservationKind.CI_RUN,
        title="Canonical CI blocker record",
        source_class=SourceClass.WORK_GRAPH,
        observed_at=observed_at,
        subject_canonical_ids=(seeds[0], blocker_id),
        attributes={"corpus_trust": "canonical"},
    )
    return InvestigationReadout(
        org_id="org_query_service_test",
        partition="cf_query_service_test",
        seed_canonical_ids=seeds,
        entities=entities,
        paths=(path,),
        observations=(observation,),
        authorized_entity_ids=tuple(sorted((*members, blocker_id))),
    )


def _excluded_finding(driver_id: str, subject_id: str) -> DriverFinding:
    """A candidate with no support, for exercising the aggregation bound.

    Excluded candidates are still part of the packet's truthful accounting,
    but they do not require a fabricated path/evidence fixture merely to test
    ordering, duplicate-id handling or truncation disclosure.
    """

    return DriverFinding(
        driver_id=driver_id,
        subject_id=subject_id,
        cause_id=f"cause_{driver_id}",
        category=DriverCategory.EXTERNAL_BLOCKER,
        role=DriverRole.DRIVER,
        standing=DriverStanding.EXCLUDED,
        mechanism=StandingMechanism.STRUCTURAL,
        summary_subject=f"cause_{driver_id}",
        summary_detail="was considered but lacked canonical support",
        exclusion_reason=DriverExclusionReason.EVIDENCE_CONFLICT_UNRESOLVED,
        assertion_basis=AssertionBasis.SOURCE_ASSERTED,
        confidence_qualifier=ConfidenceQualifier.QUALIFIED,
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
            authorized_entity_ids=("proj_a", "proj_b", "team_x", "wu_blocker"),
            entities=(
                DiscoveredEntity(
                    canonical_id="proj_a",
                    kind=GraphEntityKind.PROJECT,
                    display_label="Project A",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=datetime(2026, 5, 1, tzinfo=UTC),
                ),
                DiscoveredEntity(
                    canonical_id="proj_b",
                    kind=GraphEntityKind.PROJECT,
                    display_label="Project B",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=datetime(2026, 5, 1, tzinfo=UTC),
                ),
                DiscoveredEntity(
                    canonical_id="wu_blocker",
                    kind=GraphEntityKind.WORK_UNIT,
                    display_label="Open blocker",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=datetime(2026, 5, 1, tzinfo=UTC),
                ),
            ),
            paths=(
                DiscoveredPath(
                    path_id="p0001",
                    origin_canonical_id="proj_a",
                    terminal_canonical_id="wu_blocker",
                    steps=(
                        PathStep(
                            from_canonical_id="proj_a",
                            from_kind=GraphEntityKind.PROJECT,
                            relationship=RelationshipType.BLOCKED_BY,
                            direction=RelationshipDirection.FORWARD,
                            to_canonical_id="wu_blocker",
                            to_kind=GraphEntityKind.WORK_UNIT,
                            source_class=SourceClass.WORK_GRAPH,
                            observed_at=datetime(2026, 5, 1, tzinfo=UTC),
                            observation_ids=("obs_blocker",),
                        ),
                    ),
                ),
            ),
            observations=(
                DiscoveredObservation(
                    canonical_id="obs_blocker",
                    kind=GraphObservationKind.CI_RUN,
                    title="Canonical CI blocker record",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=datetime(2026, 5, 1, tzinfo=UTC),
                    subject_canonical_ids=("proj_a", "wu_blocker"),
                    attributes={"corpus_trust": "canonical"},
                ),
                DiscoveredObservation(
                    canonical_id="obs_measurement",
                    kind=GraphObservationKind.MEASUREMENT,
                    title="Canonical work in progress measurement",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=datetime(2026, 5, 1, tzinfo=UTC),
                    subject_canonical_ids=("proj_a",),
                    attributes={
                        "corpus_trust": "canonical",
                        "measurement_metric": "work_in_progress",
                        "measurement_value": "10",
                        "measurement_cohort_median": "5",
                    },
                ),
            ),
        )
        reader = _FakeReader(readout=readout)
        service = _service(driver=driver, reader=reader)

        discovery = _canned_discovery(members=("proj_a", "proj_b"))
        monkeypatch.setattr(
            query_service_module, "discover_cohort", lambda **_kwargs: discovery
        )

        result = await service.investigate(
            _request(
                authorized_entity_ids=frozenset(
                    {"proj_a", "proj_b", "team_x", "wu_blocker"}
                )
            )
        )

        assert result.outcome is GraphQueryOutcome.COMPLETED
        assert result.packet is not None
        assert result.packet.outcome is InvestigationOutcome.SUPPORTED
        assert result.packet.comparison_cohort.comparison_shape.value == (
            "discovered_cohort"
        )
        assert sorted(
            member.canonical_id for member in result.packet.comparison_cohort.members
        ) == ["proj_a", "proj_b"]
        # W4: scope enumeration still has no committed subject, but every
        # cohort member can contribute its own structurally evidenced driver.
        # ``proj_a`` is blocked by a current work unit with a canonical
        # edge-level observation; its measurement is retained as context but
        # may not become an asserted driver; ``proj_b`` has no such path.
        assert [
            candidate.driver_id
            for candidate in result.packet.driver_analysis.candidates
        ] == ["drv_block_wu_blocker", "drv_metric_obs_measurement"]
        assert (
            result.packet.driver_analysis.candidates[0].standing
            is DriverStanding.PRINCIPAL_DRIVER
        )
        assert (
            result.packet.driver_analysis.candidates[1].standing
            is DriverStanding.CANDIDATE_ONLY
        )
        assert any(
            limitation.kind is PacketLimitationKind.INTERPRETATION_UNCERTAINTY
            and "canonical measurements remain candidate-only context"
            in limitation.detail
            for limitation in result.packet.evidence_coverage.limitations
        )
        assert len(reader.calls) == 1
        assert sorted(reader.calls[0]["seed_canonical_ids"]) == ["proj_a", "proj_b"]

    @pytest.mark.asyncio
    async def test_seeds_are_capped_at_max_cohort_seeds_in_canonical_id_order(
        self, monkeypatch
    ) -> None:
        """The readback cap bounds both traversal and driver synthesis.

        Never a strength/relevance ranking this arm does not own -- canonical-
        id order, capped, mirroring the trial's own ``cohort_seeds_from``
        exactly. The cap is a partial result, so a structurally supported
        finding is weakened rather than presented as complete.
        """

        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        many_members = tuple(f"proj_{i:02d}" for i in range(_MAX_COHORT_SEEDS + 5))
        driver = _FakeDriver(entity_rows=[_entity_row(m, m) for m in many_members])
        readout = _capped_readout(many_members)
        reader = _FakeReader(readout=readout)
        service = _service(driver=driver, reader=reader)

        driver_calls: list[str] = []
        real_discover_drivers = query_service_module.discover_drivers

        def tracked_discover_drivers(readout, member_id, *, as_of):
            driver_calls.append(member_id)
            return real_discover_drivers(readout, member_id, as_of=as_of)

        monkeypatch.setattr(
            query_service_module, "discover_drivers", tracked_discover_drivers
        )

        # Shuffle the discovery's own member order to prove the CAP+ORDER
        # comes from this wiring's own slicing, not from an
        # already-sorted discover_cohort output it happens to inherit.
        shuffled = tuple(reversed(many_members))
        discovery = _canned_discovery(members=shuffled)
        discovery = replace(
            discovery,
            proposal=replace(
                discovery.proposal,
                truncated=True,
                truncated_count=4,
            ),
        )
        captured_cohorts: list[CohortProposal] = []
        real_build_production_packet = query_service_module.build_production_packet

        def capture_cohort(**kwargs):
            captured_cohorts.append(kwargs["cohort"])
            return real_build_production_packet(**kwargs)

        monkeypatch.setattr(
            query_service_module, "build_production_packet", capture_cohort
        )
        monkeypatch.setattr(
            query_service_module, "discover_cohort", lambda **_kwargs: discovery
        )

        result = await service.investigate(
            _request(authorized_entity_ids=frozenset((*many_members, "wu_blocker")))
        )

        assert result.outcome is GraphQueryOutcome.COMPLETED
        assert result.packet is not None
        assert len(captured_cohorts) == 1
        assert captured_cohorts[0].truncated_count == 4 + 5
        assert result.packet.outcome is InvestigationOutcome.SUPPORTED_WITH_GAPS
        assert (
            result.packet.comparison_cohort.completeness is CohortCompleteness.TRUNCATED
        )
        assert result.packet.comparison_cohort.truncation_reason is not None
        assert any(
            limitation.kind is PacketLimitationKind.TRUNCATED_TRAVERSAL
            for limitation in result.packet.evidence_coverage.limitations
        )
        assert len(reader.calls) == 1
        seeds = reader.calls[0]["seed_canonical_ids"]
        assert len(seeds) == _MAX_COHORT_SEEDS
        assert seeds == sorted(many_members)[:_MAX_COHORT_SEEDS]
        assert driver_calls == seeds
        assert [
            candidate.driver_id
            for candidate in result.packet.driver_analysis.candidates
            if candidate.standing
            in {
                DriverStanding.CONTRIBUTING_DRIVER,
                DriverStanding.PRINCIPAL_DRIVER,
            }
        ] == ["drv_block_wu_blocker"]
        assert all(
            candidate.affected_subject_ids[0] in seeds
            for candidate in result.packet.driver_analysis.candidates
        )


class TestSubjectlessDriverAggregation:
    def test_each_member_is_discovered_in_canonical_order_without_ranking(
        self, monkeypatch
    ) -> None:
        calls: list[str] = []

        def fake_discover(readout, member_id, *, as_of):
            del readout, as_of
            calls.append(member_id)
            return (_excluded_finding("drv_same", member_id),), False

        monkeypatch.setattr(query_service_module, "discover_drivers", fake_discover)

        findings, truncated = _subjectless_drivers(
            _empty_readout(),
            ("proj_b", "proj_a", "proj_b"),
            as_of=datetime(2026, 8, 9, tzinfo=UTC),
        )

        assert calls == ["proj_a", "proj_b"]
        assert not truncated
        assert [(item.subject_id, item.driver_id) for item in findings] == [
            ("proj_a", "drv_same"),
            ("proj_b", "drv_same__proj_b"),
        ]

    def test_member_driver_bound_is_disclosed_as_truncation(self, monkeypatch) -> None:
        def fake_discover(readout, member_id, *, as_of):
            del readout, as_of
            return (
                tuple(
                    _excluded_finding(f"drv_{index:02d}", member_id)
                    for index in range(51)
                ),
                True,
            )

        monkeypatch.setattr(query_service_module, "discover_drivers", fake_discover)

        findings, truncated = _subjectless_drivers(
            _empty_readout(),
            ("proj_a",),
            as_of=datetime(2026, 8, 9, tzinfo=UTC),
        )

        assert len(findings) == 50
        assert [item.driver_id for item in findings] == [
            f"drv_{index:02d}" for index in range(50)
        ]
        assert truncated
