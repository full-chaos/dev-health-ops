"""CHAOS-3678: the production bounded graph query service.

Two halves, matching the module's own documented scope:

* :func:`mechanism_for` — the fixed ``(intent_id, cardinality) ->
  mechanism`` table (CHAOS-3660's accepted job/shape determination).
* ``ProductionGraphInvestigationQuery.investigate`` — the transport/outcome
  mapping, tested against fake stores injected via ``store_factory`` (no
  live backend needed for DISABLED/UNAVAILABLE/STALE/DEADLINE_EXCEEDED/
  CANCELLED/PROVIDER_FAILURE) plus one live positive control.

The ``COMPLETED`` path is not implemented in this increment (see the module
docstring) and is not tested here — every currently-SUPPORTED mechanism is
asserted to reach ``PROVIDER_FAILURE`` with a diagnostic naming the pending
dependency, which is the honest, current behaviour.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest

from dev_health_ops.api.dev.contracts_v2.base import Cardinality, QuestionIntentID
from dev_health_ops.api.dev.graph_investigation_query import (
    GraphInvestigationRequest,
    GraphQueryOutcome,
)
from dev_health_ops.context_fabric.graph_arm.query_service import (
    GraphMechanism,
    ProductionGraphInvestigationQuery,
    mechanism_for,
)
from dev_health_ops.context_fabric.graph_arm.store import (
    GraphArmStore,
    StoreUnavailableError,
)
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


def _request(
    *,
    intent_id: QuestionIntentID = QuestionIntentID.PROJECT_HEALTH,
    cardinality: Cardinality = Cardinality.SINGULAR,
    deadline: datetime | None = None,
    org_id: str = "org_query_service_test",
) -> GraphInvestigationRequest:
    return GraphInvestigationRequest(
        org_id=org_id,
        run_id="run_test",
        intent_id=intent_id,
        cardinality=cardinality,
        mentions=(),
        question_text="What is the status of the Nightfall Migration project?",
        authorized_entity_ids=frozenset({"proj_nightfall_migration"}),
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


class TestSupportedMechanismIsNotYetImplemented:
    pytestmark = pytest.mark.asyncio

    """Increment 1's honest boundary: selected, not yet executed."""

    async def test_a_supported_mechanism_reaches_provider_failure_with_a_named_reason(
        self, monkeypatch
    ) -> None:
        monkeypatch.setenv("CONTEXT_FABRIC_GRAPH_READ_ENABLED", "1")
        store = _FakeStore(watermark=_fresh_watermark())
        service = _query(store)
        result = await service.investigate(
            _request(
                intent_id=QuestionIntentID.PROJECT_HEALTH,
                cardinality=Cardinality.SINGULAR,
            )
        )
        assert result.outcome is GraphQueryOutcome.PROVIDER_FAILURE
        assert result.packet is None
        assert result.diagnostic is not None
        assert "seeded_singular_subject" in result.diagnostic
        assert "CHAOS-3660" in result.diagnostic


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
