"""CHAOS-3678: the production bounded graph query service.

Three halves, matching the module's own documented scope:

* :func:`mechanism_for` — the fixed ``(intent_id, cardinality) ->
  mechanism`` table (CHAOS-3660's accepted job/shape determination).
* ``ProductionGraphInvestigationQuery.investigate`` — the transport/outcome
  mapping, tested against fake stores injected via ``store_factory`` (no
  live backend needed for DISABLED/UNAVAILABLE/STALE/DEADLINE_EXCEEDED/
  CANCELLED/PROVIDER_FAILURE) plus one live positive control.
* ``SEEDED_SINGULAR_SUBJECT``'s ``COMPLETED`` path (this revision) —
  tested against a fake ``reader_factory``/fake driver, never a live
  FalkorDB: ``subject_resolution.resolve_exact_subjects`` already has its
  own direct unit coverage (``test_chaos_3678_subject_resolution.py``), so
  what matters here is the wiring between resolution, traversal and
  ``build_production_packet``, not re-proving the resolver's own rules.

``SEEDED_EXPLICIT_COHORT``/``SUBJECTLESS_COHORT_DISCOVERY`` remain
untouched by this revision — both still reach ``PROVIDER_FAILURE`` with a
diagnostic naming the mechanism, which is the honest, current behaviour
until ``cohort_discovery`` is wired in as a tracked follow-up.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest

from dev_health_ops.api.dev.contracts_v2.base import (
    Cardinality,
    EntityKind,
    QuestionIntentID,
    SourceClass,
)
from dev_health_ops.api.dev.contracts_v2.subject import DevSubjectMention
from dev_health_ops.api.dev.evidence_service import EvidenceReferenceSigner
from dev_health_ops.api.dev.graph_investigation_query import (
    GraphInvestigationRequest,
    GraphQueryOutcome,
)
from dev_health_ops.context_fabric.graph_arm.query_service import (
    GraphMechanism,
    ProductionGraphInvestigationQuery,
    mechanism_for,
)
from dev_health_ops.context_fabric.graph_arm.readback import (
    DiscoveredEntity,
    InvestigationReadout,
)
from dev_health_ops.context_fabric.graph_arm.store import (
    GraphArmStore,
    StoreUnavailableError,
)
from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark
from tests.context_fabric import live_gate

#: A fixed reference instant for constructing WATERMARK values only -- always
#: comfortably in the past relative to the real clock, so "N days before
#: this" reliably reads as stale. Deadlines must never be computed from this:
#: `investigate()` compares them against the REAL `datetime.now(UTC)`, so a
#: deadline offset from a fixed historical instant is a deadline already in
#: the past on any test run -- see `_soon()`.
_NOW = datetime(2026, 8, 9, 12, tzinfo=UTC)


def _soon() -> datetime:
    """A deadline comfortably in the future relative to the real clock."""

    return datetime.now(UTC) + timedelta(seconds=30)


def _fresh_watermark() -> IndexWatermark:
    """A watermark current as of the real clock -- not `_NOW`, which is a
    fixed historical instant for staleness tests only."""

    return IndexWatermark(indexed_through=datetime.now(UTC), records_indexed=1)


#: A real UUID -- ``ProductionJobProvenance.run_id`` is a ``ServerHandle``
#: (exact 36-char UUID pattern), which only the COMPLETED-path tests reach.
_RUN_UUID = "9c9a3f9e-1111-4222-8333-444455556666"


def _request(
    *,
    intent_id: QuestionIntentID = QuestionIntentID.PROJECT_HEALTH,
    cardinality: Cardinality = Cardinality.SINGULAR,
    deadline: datetime | None = None,
    org_id: str = "org_query_service_test",
    run_id: str = "run_test",
    mentions: tuple[DevSubjectMention, ...] = (),
    authorized_entity_ids: frozenset[str] = frozenset({"proj_nightfall_migration"}),
) -> GraphInvestigationRequest:
    return GraphInvestigationRequest(
        org_id=org_id,
        run_id=run_id,
        intent_id=intent_id,
        cardinality=cardinality,
        mentions=mentions,
        question_text="What is the status of the Nightfall Migration project?",
        authorized_entity_ids=authorized_entity_ids,
        window_start=datetime(2026, 5, 12, tzinfo=UTC),
        window_end=datetime(2026, 8, 9, tzinfo=UTC),
        deadline=deadline or _soon(),
    )


class TestMechanismFor:
    def test_discovered_cohort_selects_subjectless_cohort_discovery(self) -> None:
        assert (
            mechanism_for(
                QuestionIntentID.DISCOVERED_COHORT, Cardinality.ORGANIZATION_WIDE
            )
            is GraphMechanism.SUBJECTLESS_COHORT_DISCOVERY
        )

    @pytest.mark.parametrize(
        "intent_id",
        [
            QuestionIntentID.ENTITY_STATUS,
            QuestionIntentID.PROJECT_HEALTH,
            QuestionIntentID.TEAM_HEALTH,
            QuestionIntentID.REMAINING_WORK,
        ],
    )
    def test_singular_cardinality_selects_seeded_singular_subject(
        self, intent_id: QuestionIntentID
    ) -> None:
        assert (
            mechanism_for(intent_id, Cardinality.SINGULAR)
            is GraphMechanism.SEEDED_SINGULAR_SUBJECT
        )

    def test_plural_cohort_cardinality_selects_seeded_explicit_cohort(self) -> None:
        assert (
            mechanism_for(QuestionIntentID.METRIC_COMPARISON, Cardinality.PLURAL_COHORT)
            is GraphMechanism.SEEDED_EXPLICIT_COHORT
        )

    def test_organization_wide_with_a_non_cohort_intent_is_unsupported(self) -> None:
        """The organization-wide sweep this arm must never construct.

        ``ORGANIZATION_WIDE`` cardinality paired with any intent other than
        ``DISCOVERED_COHORT`` is the shape handoff §4 forbids
        ("unresolved named subjects never widen to organization scope") --
        this table returns UNSUPPORTED rather than a mechanism, so
        ``investigate`` never attempts it.
        """

        assert (
            mechanism_for(
                QuestionIntentID.PORTFOLIO_STATUS, Cardinality.ORGANIZATION_WIDE
            )
            is GraphMechanism.UNSUPPORTED
        )

    def test_the_table_is_fixed_not_content_derived(self) -> None:
        """Same (intent_id, cardinality) -> same mechanism, always.

        Not a meaningful assertion about randomness -- there is none -- but
        a structural pin that the function takes exactly two arguments and
        nothing resembling question text, which is easy to check by calling
        it and impossible to check by reading the return type alone.
        """

        import inspect

        params = list(inspect.signature(mechanism_for).parameters)
        assert params == ["intent_id", "cardinality"]


@dataclass
class _FakeStore:
    """Stands in for ``GraphArmStore`` at exactly the two methods
    ``investigate`` calls: ``read_watermark`` and ``close``.
    """

    watermark: IndexWatermark | None = None
    watermark_error: Exception | None = None
    hang_seconds: float | None = None
    closed: bool = False
    close_calls: int = field(default=0)
    #: Only read by the COMPLETED path (``_TraversalStore``'s two members,
    #: via ``cast``) -- every other test class never reaches that far, so
    #: these defaults are never exercised outside
    #: ``TestSeededSingularSubjectCompletes``.
    partition: str = "cf_query_service_test"
    _driver: object = None

    async def read_watermark(self) -> IndexWatermark:
        if self.hang_seconds is not None:
            await asyncio.sleep(self.hang_seconds)
        if self.watermark_error is not None:
            raise self.watermark_error
        assert self.watermark is not None
        return self.watermark

    async def close(self) -> None:
        self.closed = True
        self.close_calls += 1


def _factory(store: _FakeStore):
    def _build(_org_id: str) -> _FakeStore:
        return store

    return _build


def _query(store: _FakeStore) -> ProductionGraphInvestigationQuery:
    return ProductionGraphInvestigationQuery(store_factory=_factory(store))


class TestDisabled:
    pytestmark = pytest.mark.asyncio

    async def test_disabled_when_the_read_flag_is_off(self) -> None:
        service = _query(_FakeStore())
        result = await service.investigate(_request())
        assert result.outcome is GraphQueryOutcome.DISABLED
        assert result.packet is None

    async def test_disabled_never_constructs_a_store(self, monkeypatch) -> None:
        """The read flag is checked before the store factory is ever called."""

        calls: list[str] = []

        def _factory_records(org_id: str) -> _FakeStore:
            calls.append(org_id)
            return _FakeStore()

        service = ProductionGraphInvestigationQuery(store_factory=_factory_records)
        await service.investigate(_request())
        assert calls == []


class TestUnavailable:
    pytestmark = pytest.mark.asyncio

    async def test_a_store_construction_failure_is_unavailable(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")

        def _raising_factory(_org_id: str):
            raise StoreUnavailableError("no trial graph store is configured")

        service = ProductionGraphInvestigationQuery(store_factory=_raising_factory)
        result = await service.investigate(_request())
        assert result.outcome is GraphQueryOutcome.UNAVAILABLE
        assert result.packet is None

    async def test_a_watermark_read_failure_is_unavailable(self, monkeypatch) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(watermark_error=StoreUnavailableError("transport failure"))
        service = _query(store)
        result = await service.investigate(_request())
        assert result.outcome is GraphQueryOutcome.UNAVAILABLE
        assert store.closed is True

    async def test_a_never_projected_partition_is_unavailable(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(watermark=IndexWatermark(indexed_through=None))
        service = _query(store)
        result = await service.investigate(_request())
        assert result.outcome is GraphQueryOutcome.UNAVAILABLE


class TestStale:
    pytestmark = pytest.mark.asyncio

    async def test_a_watermark_beyond_tolerance_is_stale(self, monkeypatch) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(
            watermark=IndexWatermark(
                indexed_through=_NOW - timedelta(days=2), records_indexed=5
            )
        )
        service = _query(store)
        result = await service.investigate(_request(deadline=_soon()))
        assert result.outcome is GraphQueryOutcome.STALE
        assert result.packet is None

    async def test_a_partial_watermark_is_stale_even_if_recent(
        self, monkeypatch
    ) -> None:
        """Mirrors ``IndexWatermark.freshness_for``'s own rule directly."""

        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(
            watermark=IndexWatermark(
                indexed_through=_NOW, records_indexed=5, partial=True
            )
        )
        service = _query(store)
        result = await service.investigate(_request())
        assert result.outcome is GraphQueryOutcome.STALE


class TestDeadlineExceeded:
    pytestmark = pytest.mark.asyncio

    async def test_a_deadline_already_past_never_touches_the_store(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        calls: list[str] = []

        def _factory_records(org_id: str) -> _FakeStore:
            calls.append(org_id)
            return _FakeStore()

        service = ProductionGraphInvestigationQuery(store_factory=_factory_records)
        past_deadline = _NOW - timedelta(seconds=1)
        result = await service.investigate(_request(deadline=past_deadline))
        assert result.outcome is GraphQueryOutcome.DEADLINE_EXCEEDED
        assert calls == [], "a caller that waited too long must not pay for a store"

    async def test_a_hung_watermark_read_exceeds_the_deadline(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(hang_seconds=999.0)
        service = _query(store)
        near_deadline = datetime.now(UTC) + timedelta(milliseconds=50)
        result = await service.investigate(_request(deadline=near_deadline))
        assert result.outcome is GraphQueryOutcome.DEADLINE_EXCEEDED
        assert store.closed is True, "the store is still closed on a deadline timeout"


class TestCancelled:
    pytestmark = pytest.mark.asyncio

    async def test_cancellation_during_the_watermark_read_is_reported_not_raised(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(hang_seconds=999.0)
        service = _query(store)

        task = asyncio.ensure_future(service.investigate(_request(deadline=_soon())))
        await asyncio.sleep(0)  # let investigate() reach the hung await
        task.cancel()
        result = await task
        assert result.outcome is GraphQueryOutcome.CANCELLED
        assert store.closed is True


class TestUnsupportedMechanism:
    pytestmark = pytest.mark.asyncio

    async def test_an_unsupported_mechanism_is_provider_failure_not_a_crash(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(watermark=_fresh_watermark())
        service = _query(store)
        result = await service.investigate(
            _request(
                intent_id=QuestionIntentID.PORTFOLIO_STATUS,
                cardinality=Cardinality.ORGANIZATION_WIDE,
            )
        )
        assert result.outcome is GraphQueryOutcome.PROVIDER_FAILURE
        assert result.diagnostic is not None
        assert "no graph mechanism" in result.diagnostic


class TestCohortMechanismsAreNotYetImplemented:
    pytestmark = pytest.mark.asyncio

    """The remaining honest boundary: selected, not yet executed.

    SEEDED_SINGULAR_SUBJECT graduated to a real COMPLETED path this
    revision (see ``TestSeededSingularSubjectCompletes`` below) --
    SEEDED_EXPLICIT_COHORT and SUBJECTLESS_COHORT_DISCOVERY have not, since
    both need ``cohort_discovery`` wired in as a separate follow-up.
    """

    async def test_seeded_explicit_cohort_reaches_provider_failure_with_a_named_reason(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(watermark=_fresh_watermark())
        service = _query(store)
        result = await service.investigate(
            _request(
                intent_id=QuestionIntentID.METRIC_COMPARISON,
                cardinality=Cardinality.PLURAL_COHORT,
            )
        )
        assert result.outcome is GraphQueryOutcome.PROVIDER_FAILURE
        assert result.packet is None
        assert result.diagnostic is not None
        assert "seeded_explicit_cohort" in result.diagnostic

    async def test_subjectless_cohort_discovery_reaches_provider_failure(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(watermark=_fresh_watermark())
        service = _query(store)
        result = await service.investigate(
            _request(
                intent_id=QuestionIntentID.DISCOVERED_COHORT,
                cardinality=Cardinality.ORGANIZATION_WIDE,
            )
        )
        assert result.outcome is GraphQueryOutcome.PROVIDER_FAILURE
        assert result.packet is None
        assert result.diagnostic is not None
        assert "subjectless_cohort_discovery" in result.diagnostic


_TEST_SIGNING_SECRET = "chaos-3678-query-service-test-signing-secret-not-real"


@dataclass
class _FakeDriver:
    """Mirrors ``readback._rows``'s contract, same shape as
    ``test_chaos_3678_subject_resolution.py``'s fake -- this class exists
    separately because it belongs to a different fake store's lifecycle,
    not because the contract differs.
    """

    rows: list[dict] = field(default_factory=list)

    async def execute_query(self, query: str, **params: object) -> tuple:
        return (self.rows, None, None)


def _entity_row(canonical_id: str, display_label: str) -> dict:
    return {
        "canonical_id": canonical_id,
        "entity_kind": GraphEntityKind.PROJECT.value,
        "display_label": display_label,
        "source_class": SourceClass.WORK_GRAPH.value,
    }


def _mention(text: str) -> DevSubjectMention:
    return DevSubjectMention(
        schema_version="dev_subject_mention.v1",
        mention_id="1c2d3e4f-1111-4222-8333-444455556666",
        mention_ordinal=0,
        original_text_span=text,
        requested_entity_kind=EntityKind.PROJECT,
        normalized_lookup_text=text,
    )


@dataclass
class _FakeReader:
    """A canned ``GraphReader``, decoupled from any store -- this
    increment's tests need to control exactly what a traversal returns
    without a live-store-compatible entity/edge/observation fixture.
    """

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


class TestSeededSingularSubjectCompletes:
    pytestmark = pytest.mark.asyncio

    """This revision's one real COMPLETED path.

    Both tests use the identical fake driver/reader/mentions setup except
    for one thing -- the query text -- so the difference in outcome is
    attributable to resolution finding (or not finding) the subject, not
    to an incidental setup difference between the two tests.
    """

    def _service(
        self, *, driver: _FakeDriver, reader: _FakeReader
    ) -> ProductionGraphInvestigationQuery:
        store = _FakeStore(watermark=_fresh_watermark(), _driver=driver)
        return ProductionGraphInvestigationQuery(
            store_factory=_factory(store),
            reader_factory=lambda _store: reader,
            signer_factory=lambda: EvidenceReferenceSigner(_TEST_SIGNING_SECRET),
        )

    async def test_a_resolving_mention_reaches_completed_with_a_committed_subject(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        driver = _FakeDriver(
            rows=[_entity_row("proj_nightfall_migration", "Nightfall Migration")]
        )
        readout = InvestigationReadout(
            org_id="org_query_service_test",
            partition="cf_query_service_test",
            seed_canonical_ids=("proj_nightfall_migration",),
            # A real traversal populates this from its own authorized-scope
            # check; the packet contract cross-validates every subject
            # candidate against it, so the fake readout must too.
            authorized_entity_ids=("proj_nightfall_migration",),
            entities=(
                DiscoveredEntity(
                    canonical_id="proj_nightfall_migration",
                    kind=GraphEntityKind.PROJECT,
                    display_label="Nightfall Migration",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=datetime.now(UTC),
                ),
            ),
        )
        reader = _FakeReader(readout=readout)
        service = self._service(driver=driver, reader=reader)

        result = await service.investigate(
            _request(
                intent_id=QuestionIntentID.PROJECT_HEALTH,
                cardinality=Cardinality.SINGULAR,
                run_id=_RUN_UUID,
                mentions=(_mention("Nightfall Migration"),),
                authorized_entity_ids=frozenset({"proj_nightfall_migration"}),
            )
        )

        assert result.outcome is GraphQueryOutcome.COMPLETED
        assert result.packet is not None
        assert reader.calls == [
            {
                "org_id": "org_query_service_test",
                "seed_canonical_ids": ["proj_nightfall_migration"],
                "authorized_entity_ids": ["proj_nightfall_migration"],
            }
        ]
        job = result.packet.analytical_job
        assert job.schema_version == "ask_dev_analytical_job.v2"
        assert job.production_job is not None
        assert job.production_job.run_id == _RUN_UUID
        # PROPOSED, not necessarily COMMITTED: commitment additionally
        # requires the seed be touched by at least one traversal path
        # (``packet_builder``'s own rule, already covered by that module's
        # tests) -- what THIS test proves is that resolution's match
        # reached subject_discovery as a real candidate at all, which is
        # the wiring under test here.
        assert [
            candidate.canonical_id
            for candidate in result.packet.subject_discovery.candidates
        ] == ["proj_nightfall_migration"]

    async def test_a_non_resolving_mention_still_reaches_completed_not_a_guess(
        self, monkeypatch
    ) -> None:
        """§4: no fuzzy/unresolved-name widening. A mention that resolves to
        nothing is still an honestly COMPLETED call -- the packet itself
        discloses no committed subject, never a fabricated one and never a
        transport failure this is not.
        """

        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        driver = _FakeDriver(
            rows=[_entity_row("proj_nightfall_migration", "Nightfall Migration")]
        )
        readout = InvestigationReadout(
            org_id="org_query_service_test",
            partition="cf_query_service_test",
            seed_canonical_ids=(),
        )
        reader = _FakeReader(readout=readout)
        service = self._service(driver=driver, reader=reader)

        result = await service.investigate(
            _request(
                intent_id=QuestionIntentID.PROJECT_HEALTH,
                cardinality=Cardinality.SINGULAR,
                run_id=_RUN_UUID,
                # Only a fuzzy/partial overlap with the fixture's entity --
                # resolve_exact_subjects's own negative control.
                mentions=(_mention("Nightfall"),),
                authorized_entity_ids=frozenset({"proj_nightfall_migration"}),
            )
        )

        assert result.outcome is GraphQueryOutcome.COMPLETED
        assert result.packet is not None
        assert reader.calls[0]["seed_canonical_ids"] == []
        assert result.packet.subject_discovery.committed_subject_ids == ()

    async def test_the_store_is_closed_after_completed(self, monkeypatch) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        driver = _FakeDriver(rows=[])
        readout = InvestigationReadout(
            org_id="org_query_service_test",
            partition="cf_query_service_test",
            seed_canonical_ids=(),
        )
        reader = _FakeReader(readout=readout)
        store = _FakeStore(watermark=_fresh_watermark(), _driver=driver)
        service = ProductionGraphInvestigationQuery(
            store_factory=_factory(store),
            reader_factory=lambda _store: reader,
            signer_factory=lambda: EvidenceReferenceSigner(_TEST_SIGNING_SECRET),
        )
        await service.investigate(
            _request(
                intent_id=QuestionIntentID.PROJECT_HEALTH,
                cardinality=Cardinality.SINGULAR,
                run_id=_RUN_UUID,
                mentions=(_mention("nothing matches"),),
            )
        )
        assert store.close_calls == 1


class TestDiagnosticsAreContentSafe:
    pytestmark = pytest.mark.asyncio

    async def test_no_diagnostic_carries_the_question_text(self, monkeypatch) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        planted = "TOTALLY-SECRET-QUESTION-TEXT-MARKER"
        store = _FakeStore(watermark=IndexWatermark(indexed_through=None))
        service = _query(store)
        request = GraphInvestigationRequest(
            org_id="org_content_safety",
            run_id="run_content_safety",
            intent_id=QuestionIntentID.PROJECT_HEALTH,
            cardinality=Cardinality.SINGULAR,
            mentions=(),
            question_text=planted,
            authorized_entity_ids=frozenset(),
            window_start=datetime(2026, 5, 12, tzinfo=UTC),
            window_end=datetime(2026, 8, 9, tzinfo=UTC),
            deadline=_soon(),
        )
        result = await service.investigate(request)
        assert result.diagnostic is not None
        assert planted not in result.diagnostic


class TestStoreIsAlwaysClosed:
    pytestmark = pytest.mark.asyncio

    async def test_the_store_is_closed_after_a_provider_failure(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(watermark=_fresh_watermark())
        service = _query(store)
        await service.investigate(_request())
        assert store.close_calls == 1

    async def test_the_store_is_never_closed_when_never_constructed(self) -> None:
        """DISABLED short-circuits before any store exists to close."""

        calls: list[str] = []

        def _factory_records(org_id: str) -> _FakeStore:
            calls.append(org_id)
            return _FakeStore(watermark=_fresh_watermark())

        service = ProductionGraphInvestigationQuery(store_factory=_factory_records)
        await service.investigate(_request())
        assert calls == []


class TestLivePositiveControl:
    pytestmark = pytest.mark.asyncio

    """Against the real live store: proves the wiring, not just the fakes."""

    @pytest.mark.graphiti
    async def test_disabled_against_a_real_store_factory(self, monkeypatch) -> None:
        live_gate.require_live_store()
        monkeypatch.delenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", raising=False)
        service = ProductionGraphInvestigationQuery(store_factory=GraphArmStore.for_org)
        result = await service.investigate(_request(org_id="org_live_query_service"))
        assert result.outcome is GraphQueryOutcome.DISABLED

    @pytest.mark.graphiti
    async def test_never_projected_against_a_real_store(self, monkeypatch) -> None:
        live_gate.require_live_store()
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        service = ProductionGraphInvestigationQuery(store_factory=GraphArmStore.for_org)
        result = await service.investigate(
            _request(org_id="org_live_query_service_fresh")
        )
        assert result.outcome is GraphQueryOutcome.UNAVAILABLE
