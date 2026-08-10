"""CHAOS-3676: bounded retry, failure classification around write_projection.

``GraphArmStore.write_projection`` had no retry and no failure
classification: a transient failure (an unreachable/timed-out backend, per
CHAOS-3631) and a permanent one (a malformed batch, an over-budget embedding
run, a wrong-organization projection) both just propagated on the first
attempt. ``project_with_retry`` is the driver that sits between a future
worker task and the store: it retries the transient class up to a bounded
attempt count with backoff, never retries the permanent class, and returns a
structured :class:`ProjectionOutcome` so a caller can decide retry-via-outbox
vs. dead-letter without inspecting exception types itself.

Idempotency at the write layer is NOT re-proven here -- it is established at
the Graphiti/FalkorDB layer (Cypher ``MERGE`` keyed on a deterministic uuid)
and cited on the issue. This module tests the retry/classification driver
added on top of an already-idempotent write.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.context_fabric.graph_arm import fixtures
from dev_health_ops.context_fabric.graph_arm.projector import (
    DEFAULT_MAX_ATTEMPTS,
    ProjectionFailureClass,
    ProjectionOutcome,
    project_with_retry,
)
from dev_health_ops.context_fabric.graph_arm.store import (
    EmbeddingBudgetExceededError,
    GraphOperationTimeoutError,
    ProjectionDisabledError,
)
from dev_health_ops.context_fabric.graph_arm.watermark import IndexWatermark


@dataclass
class _FailNTimesThenSucceedStore:
    """Raises a transient error the first ``fail_count`` calls, then succeeds."""

    fail_count: int
    calls: int = 0
    sleeps: list[float] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self.sleeps = []

    async def write_projection(self, projection: Any, *, budgets: Any = None) -> Any:
        self.calls += 1
        if self.calls <= self.fail_count:
            raise GraphOperationTimeoutError(
                f"graph operation 'write_projection' did not complete within "
                f"15.0s (org 'orgtest' partition 'cf_orgtest'); attempt {self.calls}"
            )
        return _fake_write_result()


@dataclass
class _AlwaysFailsTransientlyStore:
    calls: int = 0

    async def write_projection(self, projection: Any, *, budgets: Any = None) -> Any:
        self.calls += 1
        raise GraphOperationTimeoutError("graph operation 'write_projection' timed out")


@dataclass
class _AlwaysRaisesStore:
    """Raises whatever exception instance it is constructed with."""

    exc: Exception
    calls: int = 0

    async def write_projection(self, projection: Any, *, budgets: Any = None) -> Any:
        self.calls += 1
        raise self.exc


def _fake_write_result() -> Any:
    from dev_health_ops.context_fabric.graph_arm.store import WriteResult

    return WriteResult(
        nodes_written=3,
        edges_written=1,
        watermark=IndexWatermark(
            indexed_through=datetime(2026, 8, 8, tzinfo=UTC),
            projected_at=datetime(2026, 8, 8, tzinfo=UTC),
            records_indexed=4,
        ),
    )


def _batch():
    return fixtures.alpha_batch()


class TestSuccessAfterTransientFailures:
    pytestmark = pytest.mark.asyncio

    async def test_retries_and_succeeds_within_the_bound(self) -> None:
        store = _FailNTimesThenSucceedStore(fail_count=2)
        outcome = await project_with_retry(
            store, _batch(), max_attempts=3, backoff_s=0.0
        )
        assert outcome.success is True
        assert outcome.attempts == 3
        assert store.calls == 3
        assert outcome.write_result is not None
        assert outcome.failure_class is None
        assert outcome.failure_detail is None

    async def test_a_call_that_succeeds_first_try_is_not_retried(self) -> None:
        store = _FailNTimesThenSucceedStore(fail_count=0)
        outcome = await project_with_retry(store, _batch(), max_attempts=5)
        assert outcome.success is True
        assert outcome.attempts == 1
        assert store.calls == 1


class TestTransientExhaustion:
    pytestmark = pytest.mark.asyncio

    async def test_stops_at_the_configured_bound_and_reports_transient_exhausted(
        self,
    ) -> None:
        store = _AlwaysFailsTransientlyStore()
        outcome = await project_with_retry(
            store, _batch(), max_attempts=4, backoff_s=0.0
        )
        assert outcome.success is False
        assert store.calls == 4, (
            "a driver with no real bound would keep calling past the "
            "configured max_attempts -- this asserts the count, not just "
            "that failure was eventually reported"
        )
        assert outcome.attempts == 4
        assert outcome.failure_class is ProjectionFailureClass.TRANSIENT_EXHAUSTED
        assert outcome.write_result is None

    async def test_the_default_bound_is_the_documented_constant(self) -> None:
        store = _AlwaysFailsTransientlyStore()
        outcome = await project_with_retry(store, _batch(), backoff_s=0.0)
        assert store.calls == DEFAULT_MAX_ATTEMPTS
        assert outcome.attempts == DEFAULT_MAX_ATTEMPTS

    async def test_backoff_is_bounded_and_does_not_amplify_a_hung_backend(self) -> None:
        """Total sleep across an exhausted retry run stays under a small ceiling.

        Driven by a fake sleep that records durations rather than a real
        ``asyncio.sleep`` -- a timing test against the wall clock would be a
        flake, and a flaky test that is quietly retried is a bound nobody is
        actually measuring.
        """

        store = _AlwaysFailsTransientlyStore()
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        outcome = await project_with_retry(
            store, _batch(), max_attempts=5, backoff_s=0.5, sleep=fake_sleep
        )
        assert outcome.attempts == 5
        # One sleep between each pair of attempts, never after the last.
        assert len(sleeps) == 4
        assert sum(sleeps) < 30.0, (
            "backoff grew unbounded -- CHAOS-3631 requires retries that do "
            "not amplify load against an already-struggling backend"
        )


class TestPermanentFailuresAreNeverRetried:
    pytestmark = pytest.mark.asyncio

    async def test_a_malformed_batch_is_refused_before_any_write_attempt(self) -> None:
        """``build_projection`` itself raises -- the store is never called.

        Forced with a batch whose one observation names no subject entity --
        ``build_projection`` refuses that deterministically (see
        ``projection.py``'s "unattached evidence" check), which is a real
        ``ProjectionError`` rather than a hand-authored one.
        """

        from dev_health_ops.api.dev.contracts_v2.base import SourceClass
        from dev_health_ops.context_fabric.graph_arm.records import (
            IngestionBatch,
            ObservationRecord,
        )
        from dev_health_ops.context_fabric.graph_arm.vocabulary import (
            GraphObservationKind,
        )

        orphan_batch = IngestionBatch(
            org_id="org_alpha",
            entities=(),
            relationships=(),
            observations=(
                ObservationRecord(
                    org_id="org_alpha",
                    kind=GraphObservationKind.MEASUREMENT,
                    canonical_id="obs_orphan",
                    source_class=SourceClass.WORK_GRAPH,
                    title="orphan",
                    subjects=(),
                    observed_at=datetime(2026, 8, 8, tzinfo=UTC),
                ),
            ),
        )
        store = _AlwaysFailsTransientlyStore()
        outcome = await project_with_retry(store, orphan_batch, max_attempts=5)
        assert outcome.success is False
        assert outcome.failure_class is ProjectionFailureClass.PERMANENT
        assert store.calls == 0, (
            "the store must never be called for a batch build_projection refused"
        )

    async def test_an_over_budget_embedding_run_is_not_retried(self) -> None:
        store = _AlwaysRaisesStore(
            exc=EmbeddingBudgetExceededError("would spend more embedding calls")
        )
        outcome = await project_with_retry(store, _batch(), max_attempts=5)
        assert outcome.success is False
        assert outcome.failure_class is ProjectionFailureClass.PERMANENT
        assert store.calls == 1

    async def test_a_wrong_organization_projection_is_not_retried(self) -> None:
        store = _AlwaysRaisesStore(
            exc=PermissionError("projection belongs to a different organization")
        )
        outcome = await project_with_retry(store, _batch(), max_attempts=5)
        assert outcome.success is False
        assert outcome.failure_class is ProjectionFailureClass.PERMANENT
        assert store.calls == 1

    async def test_the_projection_flag_being_off_is_not_retried(self) -> None:
        store = _AlwaysRaisesStore(
            exc=ProjectionDisabledError("graph projection is disabled")
        )
        outcome = await project_with_retry(store, _batch(), max_attempts=5)
        assert outcome.success is False
        assert outcome.failure_class is ProjectionFailureClass.PERMANENT
        assert store.calls == 1


class TestOutcomeShape:
    def test_a_success_outcome_cannot_also_carry_failure_detail(self) -> None:
        with pytest.raises(ValueError, match="successful outcome"):
            ProjectionOutcome(
                success=True,
                attempts=1,
                write_result=_fake_write_result(),
                failure_class=ProjectionFailureClass.PERMANENT,
            )

    def test_a_success_outcome_must_carry_a_write_result(self) -> None:
        with pytest.raises(ValueError, match="successful outcome"):
            ProjectionOutcome(success=True, attempts=1, write_result=None)

    def test_a_failure_outcome_cannot_carry_a_write_result(self) -> None:
        with pytest.raises(ValueError, match="failed outcome"):
            ProjectionOutcome(
                success=False,
                attempts=1,
                write_result=_fake_write_result(),
                failure_class=ProjectionFailureClass.PERMANENT,
                failure_detail="x",
            )

    def test_a_failure_outcome_must_carry_a_class_and_detail(self) -> None:
        with pytest.raises(ValueError, match="failed outcome"):
            ProjectionOutcome(success=False, attempts=1)


class TestFailureDetailIsContentSafe:
    pytestmark = pytest.mark.asyncio

    async def test_a_permanent_build_failure_never_leaks_entity_labels(self) -> None:
        """Plant a distinctive label in the batch that triggers ProjectionError.

        ``build_projection`` can raise ``ProjectionError`` with a message that
        embeds a source-supplied display label (e.g. two conflicting labels
        for the same canonical id). The outcome's ``failure_detail`` must
        never carry that text -- only a fixed template naming the operation,
        attempt count and exception TYPE, matching the content-safety bar
        CHAOS-3631 set for timeout messages.
        """

        from dev_health_ops.api.dev.contracts_v2.base import SourceClass
        from dev_health_ops.context_fabric.graph_arm.records import (
            EntityRecord,
            IngestionBatch,
        )
        from dev_health_ops.context_fabric.graph_arm.vocabulary import GraphEntityKind

        planted_label = "TOTALLY-SECRET-DISPLAY-LABEL-MARKER"
        now = datetime(2026, 8, 8, tzinfo=UTC)
        conflicting_batch = IngestionBatch(
            org_id="org_alpha",
            entities=(
                EntityRecord(
                    org_id="org_alpha",
                    kind=GraphEntityKind.PROJECT,
                    canonical_id="proj_dup",
                    display_label="Original Label",
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=now,
                ),
                EntityRecord(
                    org_id="org_alpha",
                    kind=GraphEntityKind.PROJECT,
                    canonical_id="proj_dup",
                    display_label=planted_label,
                    source_class=SourceClass.WORK_GRAPH,
                    observed_at=now,
                ),
            ),
        )
        store = _AlwaysFailsTransientlyStore()
        outcome = await project_with_retry(store, conflicting_batch, max_attempts=3)
        assert outcome.success is False
        assert outcome.failure_class is ProjectionFailureClass.PERMANENT
        assert outcome.failure_detail is not None
        assert planted_label not in outcome.failure_detail
        assert "Original Label" not in outcome.failure_detail
        assert "ProjectionError" in outcome.failure_detail

    async def test_a_transient_exhaustion_detail_names_the_exception_type_only(
        self,
    ) -> None:
        store = _AlwaysFailsTransientlyStore()
        outcome = await project_with_retry(
            store, _batch(), max_attempts=2, backoff_s=0.0
        )
        assert outcome.failure_detail is not None
        assert "GraphOperationTimeoutError" in outcome.failure_detail
        assert "write_projection" in outcome.failure_detail
