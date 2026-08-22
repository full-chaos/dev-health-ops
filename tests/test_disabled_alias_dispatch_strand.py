"""Unroutable dataset aliases must never strand in ``dispatching``.

CHAOS-3990. CHAOS-3941 taught the dispatch loop to terminalize a unit no
runtime can execute (``_terminalize_unroutable_units``, ``feature_disabled``).
That fix lives INSIDE the per-unit loop, so it only ever sees units that
``_claim_units`` actually returned. This module pins the hole underneath it:
a unit that the concurrency cap excludes from the claim never enters the loop,
so it is never re-decided and never terminalized.

The exclusion is deterministic, not probabilistic, which is why the operator's
00:48 UTC manual drain did not hold:

  * ``DispatchGuard.authorize_run`` (``sync/guard.py``) treats a STALE
    ``dispatching`` unit as an ordinary candidate, so the concurrency cap can
    fall on it just like brand-new PLANNED work.
  * ``_claim_units`` then subtracts every capped id from its stale-reclaim
    ``WHERE`` clause.

A capped reclaim is therefore never re-claimed, never re-decided and never
terminalized. Unit ids are stable and the bucket stays saturated, so the
same unit is capped on every subsequent pass: it holds a concurrency slot it
can never release, with no lease to expire, no outbox row, no River job and
zero attempts -- unreachable by every recovery mechanism in the system.

The fix is the invariant that the cap governs admitting NEW work only: a unit
already occupying the bucket is always re-decided, because re-deciding it is
the only thing that can ever free the slot. Reclaims still count against the
cap for admitting planned work, so total in-flight stays bounded.

CHAOS-4054 step 4 deleted the Celery dispatch plane, so the lever these tests
pull is now one-dimensional. There is no consumer probe and no durable
``sync.provider_unit`` transport row to pin: River is the only runtime, and
``routes_to_river(provider, dataset)`` -- the checked-in capability matrix
marking a pair BOTH ``route_ready`` and ``plannable`` -- is the whole question
both the dispatcher and the reconciler's sweep ask. A unit is forced down the
terminalize branch by choosing a pair the matrix does not route (an alias
identity such as ``github/tests``), not by scaling a broker to zero.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    SyncRunUnit,
    SyncRunUnitStatus,
    WorkerJobOutbox,
    WorkerJobRoute,
)
from dev_health_ops.sync.canonical_incident_gate import FEATURE_DISABLED_ERROR_CATEGORY

from ._helpers import seed_sync_dispatch_transport_routes
from .test_sync_units import (
    _patch_db_session,
    _patch_worker_enqueues,
    _seed_run,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        seed_sync_dispatch_transport_routes(session)
        yield session
    engine.dispose()


#: The three github aliases disabled at the 2026-08-19 20:00 UTC cutover --
#: originally by their per-pair env switches, now unconditionally by the
#: checked-in capability matrix (``route_ready: true``, ``plannable: false``,
#: so ``routes_to_river`` is False; CHAOS-4054 deleted the switch plane and
#: then the Celery fallthrough) -- and the exact set found stranded in
#: production.
DISABLED_ALIASES = ("tests", "pr-reviews", "pr-comments")

#: Sorts after every filler id below, so the cap always reaches it.
_STRANDED_ID = uuid.UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")


def _filler_id(index: int) -> uuid.UUID:
    return uuid.UUID(f"00000000-0000-4000-8000-{index:012d}")


def _seed_capped_bucket(db_session, monkeypatch, *, dataset_key: str):
    """Production shape: one stale disabled-alias unit behind a full bucket.

    Returns the run and the unit that must not be allowed to strand.
    """

    run, filler = _seed_run(db_session, provider="github", dataset_key="commits")
    filler.id = _filler_id(0)

    # Fill every remaining concurrency slot with PLANNED units in the SAME
    # (org, provider, cost_class) bucket, all sorting before the stale unit.
    for index in range(1, 8):
        db_session.add(
            SyncRunUnit(
                id=_filler_id(index),
                org_id=run.org_id,
                sync_run_id=run.id,
                integration_id=filler.integration_id,
                source_id=filler.source_id,
                provider="github",
                dataset_key="commits",
                cost_class="medium",
                mode=filler.mode,
                status=SyncRunUnitStatus.PLANNED.value,
                attempts=0,
                processor_flags={"sync_git": True},
            )
        )

    # The production casualty: claimed by an earlier pass, flipped to
    # DISPATCHING, never published, now stale. No lease, no heartbeat, no
    # attempts -- exactly the forensic signature from the ticket.
    stranded = SyncRunUnit(
        id=_STRANDED_ID,
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=filler.integration_id,
        source_id=filler.source_id,
        provider="github",
        dataset_key=dataset_key,
        cost_class="medium",
        mode=filler.mode,
        status=SyncRunUnitStatus.DISPATCHING.value,
        attempts=0,
        lease_owner=None,
        lease_expires_at=None,
        last_heartbeat_at=None,
        processor_flags={"sync_git": True},
        created_at=datetime.now(timezone.utc) - timedelta(hours=16),
        updated_at=datetime.now(timezone.utc) - timedelta(minutes=30),
    )
    db_session.add(stranded)
    run.total_units = 9
    db_session.flush()

    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
    _patch_db_session(monkeypatch, db_session)
    db_session.commit()
    return run, stranded


@pytest.mark.parametrize("dataset_key", DISABLED_ALIASES)
def test_capped_disabled_alias_unit_is_terminalized_not_stranded(
    db_session, monkeypatch, dataset_key: str
) -> None:
    """A concurrency-capped, runtime-disabled unit must reach a terminal state.

    Pre-fix this asserts the production defect: the unit stays ``dispatching``
    forever with no error, no outbox row and no lease, holding a slot nothing
    can reclaim.
    """

    from dev_health_ops.workers import sync_units

    run, stranded = _seed_capped_bucket(
        db_session, monkeypatch, dataset_key=dataset_key
    )
    _patch_worker_enqueues(monkeypatch)

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(stranded)

    # It must not be published -- no runtime can execute it.
    assert (
        db_session.query(WorkerJobOutbox)
        .filter(WorkerJobOutbox.dedupe_key == f"sync.provider_unit:{stranded.id}")
        .one_or_none()
        is None
    )
    # And it must not survive as a slot-holding zombie.
    assert stranded.status == SyncRunUnitStatus.FAILED.value
    assert stranded.result is not None
    # The machine-readable category downstream readers key on is unchanged --
    # this is the same terminal idiom the 00:48 UTC operator drain used.
    assert stranded.result["error_category"] == FEATURE_DISABLED_ERROR_CATEGORY
    assert stranded.result["provider"] == "github"
    assert stranded.result["dataset_key"] == dataset_key
    assert stranded.lease_owner is None
    assert stranded.lease_expires_at is None
    assert stranded.available_at is None
    # The durable reason an operator queries -- the ticket's observability gap.
    assert stranded.error


@pytest.mark.parametrize("dataset_key", DISABLED_ALIASES)
def test_capped_disabled_alias_unit_stamps_a_durable_reason(
    db_session, monkeypatch, dataset_key: str
) -> None:
    """The refusal must be readable from the row, not only from an INFO log."""

    from dev_health_ops.workers import sync_units

    run, stranded = _seed_capped_bucket(
        db_session, monkeypatch, dataset_key=dataset_key
    )
    _patch_worker_enqueues(monkeypatch)

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(stranded)
    assert stranded.error is not None
    assert stranded.last_retry_reason is not None


@pytest.mark.parametrize("dataset_key", DISABLED_ALIASES)
def test_capped_disabled_alias_unit_cannot_survive_repeated_passes(
    db_session, monkeypatch, dataset_key: str
) -> None:
    """Input symmetry over TIME: the strand must not merely be delayed.

    Unit ids are stable, so pre-fix the cap fell on the same unit every pass
    and no number of redispatches recovered it -- which is why the operator's
    manual drain refilled within hours. Driving several passes pins that the
    unit is gone after the first one and stays gone.
    """

    from dev_health_ops.workers import sync_units

    run, stranded = _seed_capped_bucket(
        db_session, monkeypatch, dataset_key=dataset_key
    )
    _patch_worker_enqueues(monkeypatch)

    for _ in range(3):
        sync_units.dispatch_sync_run(str(run.id))
        db_session.refresh(stranded)
        assert stranded.status == SyncRunUnitStatus.FAILED.value


def test_capped_enabled_alias_unit_still_publishes_normally(
    db_session, monkeypatch
) -> None:
    """Input symmetry: the control that must NOT be terminalized.

    Same capped-bucket shape, but the stale unit's pair (``repo-metadata``) is
    an ordinary route-ready, plannable identity -- capability is always on in
    the binary now, so there is no switch to flip. A live runtime can execute
    it, so the fix must reclaim it and publish it -- proving the change frees
    stuck capacity rather than simply failing whatever the cap happened to
    reach.
    """

    from dev_health_ops.workers import sync_units

    run, stranded = _seed_capped_bucket(
        db_session, monkeypatch, dataset_key="repo-metadata"
    )
    _patch_worker_enqueues(monkeypatch)

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(stranded)
    # Published to River, and therefore still in flight -- NOT terminalized.
    assert stranded.status == SyncRunUnitStatus.DISPATCHING.value
    assert stranded.error is None
    assert (
        db_session.query(WorkerJobOutbox)
        .filter(WorkerJobOutbox.dedupe_key == f"sync.provider_unit:{stranded.id}")
        .one_or_none()
        is not None
    )


def _saturate_bucket_from_sibling_run(db_session, run, filler, *, count=8):
    """Hold the whole bucket with ANOTHER run's fresh DISPATCHING units.

    ``active_count`` is measured across ALL runs in the bucket, which is the
    real production shape: the ticket shows five runs wedged at once, each
    holding slots the others needed. This drives ``allowed_slots`` to zero, so
    ordering alone cannot save a stale unit -- only refusing to cap a reclaim
    can.
    """

    from dev_health_ops.models import SyncRun, SyncRunStatus

    sibling = SyncRun(
        org_id=run.org_id,
        integration_id=run.integration_id,
        triggered_by="schedule",
        mode=run.mode,
        status=SyncRunStatus.DISPATCHING.value,
        total_units=count,
        completed_units=0,
        failed_units=0,
    )
    db_session.add(sibling)
    db_session.flush()
    for index in range(count):
        db_session.add(
            SyncRunUnit(
                id=uuid.UUID(f"aaaaaaaa-0000-4000-8000-{index:012d}"),
                org_id=run.org_id,
                sync_run_id=sibling.id,
                integration_id=filler.integration_id,
                source_id=filler.source_id,
                provider="github",
                dataset_key="commits",
                cost_class="medium",
                mode=run.mode,
                status=SyncRunUnitStatus.DISPATCHING.value,
                attempts=0,
                processor_flags={"sync_git": True},
                updated_at=datetime.now(timezone.utc),
            )
        )
    db_session.flush()
    return sibling


@pytest.mark.parametrize("dataset_key", DISABLED_ALIASES)
def test_run_with_disabled_alias_units_finalizes_partial_failed(
    db_session, monkeypatch, dataset_key: str
) -> None:
    """The interaction that matters operationally: the run must not wedge.

    A run mixing succeeded work with runtime-disabled units has to reach a
    terminal aggregate. Pre-fix the disabled units sat in ``dispatching``
    forever, so finalize could never aggregate and the whole org sync hung.
    """

    from dev_health_ops.models import SyncRun, SyncRunStatus
    from dev_health_ops.workers import sync_units

    run, stranded = _seed_capped_bucket(
        db_session, monkeypatch, dataset_key=dataset_key
    )
    filler = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == _filler_id(0)).one()
    # Everything that is not the disabled alias already succeeded, so the ONLY
    # thing standing between this run and a terminal aggregate is the stranded
    # unit.
    db_session.query(SyncRunUnit).filter(
        SyncRunUnit.sync_run_id == run.id,
        SyncRunUnit.id != stranded.id,
    ).update(
        {SyncRunUnit.status: SyncRunUnitStatus.SUCCESS.value},
        synchronize_session=False,
    )
    # Another run holds every slot in the bucket, so this run's cap allowance
    # is zero -- the production shape, and the case ordering alone cannot fix.
    _saturate_bucket_from_sibling_run(db_session, run, filler)
    db_session.commit()
    _patch_worker_enqueues(monkeypatch)

    sync_units.dispatch_sync_run(str(run.id))

    # The bucket is genuinely full, so the cap legitimately withholds the
    # reclaim this pass -- the dispatcher must NOT exceed the cap to drain it.
    # The reconciler's unreclaimable sweep is what reaches a strand the cap
    # keeps withholding, which is precisely why fix-2 is not optional.
    from dev_health_ops.workers import sync_reconciler

    db_session.expire_all()
    monkeypatch.setenv("SYNC_UNIT_UNRECLAIMABLE_SECONDS", "60")
    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    stranded = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == stranded.id).one()
    assert stranded.status == SyncRunUnitStatus.FAILED.value

    sync_units.finalize_sync_run(str(run.id))

    finalized = db_session.query(SyncRun).filter(SyncRun.id == run.id).one()
    assert finalized.status == SyncRunStatus.PARTIAL_FAILED.value
    assert finalized.completed_at is not None


@pytest.mark.parametrize(
    ("provider", "dataset_key"),
    (
        ("github", "tests"),
        ("github", "pr-reviews"),
        ("github", "pr-comments"),
    ),
)
def test_reason_names_the_matrix_for_a_non_family_alias(
    provider: str, dataset_key: str
) -> None:
    """CHAOS-4054: the durable reason never names an env var an operator
    could flip -- capability is always on in the binary, so a non-family
    alias identity is refused because the capability matrix does not mark it
    both route-ready and plannable, full stop."""

    from dev_health_ops.workers.sync_units import _unroutable_reason

    reason = _unroutable_reason(provider, dataset_key)
    assert "route-ready and plannable" in reason
    assert "WORKER_" not in reason
    assert "_ENABLED" not in reason


@pytest.mark.parametrize(
    ("provider", "dataset_key"),
    (
        ("github", "work-item-labels"),
        ("github", "work-item-history"),
    ),
)
def test_reason_names_the_atomic_family_for_a_work_item_alias(
    provider: str, dataset_key: str
) -> None:
    """A work-item-family alias is refused because it is a non-canonical
    member of an atomic provider family, not because of any switch."""

    from dev_health_ops.workers.sync_units import _unroutable_reason

    reason = _unroutable_reason(provider, dataset_key)
    assert "atomic provider family" in reason
    assert "canonical work-items claim" in reason
    assert "WORKER_" not in reason
    assert "_ENABLED" not in reason


def test_reason_never_blames_a_missing_celery_consumer() -> None:
    """CHAOS-4054 step 4: River is the only runtime, so the reason must not
    send an operator looking for a fallback queue or its consumers.

    The pre-step-4 reason ended with ", and the Celery fallback queue has no
    consumer" -- advice that now points at a plane that does not exist. Both
    surviving branches are asserted here so neither can quietly grow it back.
    """

    from dev_health_ops.workers.sync_units import _unroutable_reason

    for provider, dataset_key in (
        ("github", "tests"),
        ("github", "work-item-labels"),
    ):
        reason = _unroutable_reason(provider, dataset_key)
        assert "Celery" not in reason
        assert "celery" not in reason
        assert "consumer" not in reason
        assert "fallback" not in reason


def test_reclaims_do_not_raise_total_in_flight_above_the_cap(
    db_session, monkeypatch
) -> None:
    """The cap must still bound the bucket once reclaims are uncappable.

    Refusing to cap a reclaim reserves its slot instead of handing it to new
    work (``cap - active_count - len(reclaims)``), so admitting planned units
    can never push the bucket past ``concurrency_cap``. This pins that
    arithmetic against the shape that would break a naive fix: a bucket whose
    stale reclaims alone already meet the cap must admit ZERO planned units.
    """

    from dev_health_ops.sync.guard import DispatchGuard

    run, filler = _seed_run(db_session, provider="github", dataset_key="commits")
    filler.id = _filler_id(0)
    filler.status = SyncRunUnitStatus.DISPATCHING.value
    filler.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)

    stale = datetime.now(timezone.utc) - timedelta(minutes=30)
    # Four stale reclaims total, against a cap of four: no room for new work.
    for index in range(1, 4):
        db_session.add(
            SyncRunUnit(
                id=_filler_id(index),
                org_id=run.org_id,
                sync_run_id=run.id,
                integration_id=filler.integration_id,
                source_id=filler.source_id,
                provider="github",
                dataset_key="commits",
                cost_class="medium",
                mode=filler.mode,
                status=SyncRunUnitStatus.DISPATCHING.value,
                attempts=0,
                processor_flags={"sync_git": True},
                updated_at=stale,
            )
        )
    planned_ids = []
    for index in range(10, 14):
        planned_id = _filler_id(index)
        planned_ids.append(str(planned_id))
        db_session.add(
            SyncRunUnit(
                id=planned_id,
                org_id=run.org_id,
                sync_run_id=run.id,
                integration_id=filler.integration_id,
                source_id=filler.source_id,
                provider="github",
                dataset_key="commits",
                cost_class="medium",
                mode=filler.mode,
                status=SyncRunUnitStatus.PLANNED.value,
                attempts=0,
                processor_flags={"sync_git": True},
            )
        )
    run.total_units = 8
    db_session.flush()
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "4")

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    assert decision.allowed
    # Every reclaim survives -- that is the fix.
    assert not set(decision.capped_unit_ids) & {str(_filler_id(i)) for i in range(4)}
    # And every planned unit is capped, so 4 reclaims + 0 new == the cap.
    assert set(decision.capped_unit_ids) == set(planned_ids)


def _add_unit(
    db_session, run, filler, *, unit_id, status, updated_at=None, run_id=None
):
    db_session.add(
        SyncRunUnit(
            id=unit_id,
            org_id=run.org_id,
            sync_run_id=run_id or run.id,
            integration_id=filler.integration_id,
            source_id=filler.source_id,
            provider="github",
            dataset_key="commits",
            cost_class="medium",
            mode=filler.mode,
            status=status,
            attempts=0,
            processor_flags={"sync_git": True},
            updated_at=updated_at or datetime.now(timezone.utc),
        )
    )


def test_reclaims_are_capped_when_the_bucket_is_already_full(
    db_session, monkeypatch
) -> None:
    """Reclaims are PRIORITIZED, not exempt (adversarial review finding).

    Exempting reclaims from the cap admits more than ``concurrency_cap``
    whenever ``active_count`` already meets it: another run holding every slot
    with fresh dispatching units, plus one stale reclaim here, would put the
    bucket at cap+1 units actually executing.
    """

    from dev_health_ops.models import SyncRun, SyncRunStatus
    from dev_health_ops.sync.guard import DispatchGuard

    run, filler = _seed_run(db_session, provider="github", dataset_key="commits")
    filler.id = _filler_id(0)
    filler.status = SyncRunUnitStatus.DISPATCHING.value
    filler.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)

    sibling = SyncRun(
        org_id=run.org_id,
        integration_id=run.integration_id,
        triggered_by="schedule",
        mode=run.mode,
        status=SyncRunStatus.DISPATCHING.value,
        total_units=4,
        completed_units=0,
        failed_units=0,
    )
    db_session.add(sibling)
    db_session.flush()
    # The sibling run holds the ENTIRE cap with fresh dispatching units.
    for index in range(4):
        _add_unit(
            db_session,
            run,
            filler,
            unit_id=uuid.UUID(f"aaaaaaaa-0000-4000-8000-{index:012d}"),
            status=SyncRunUnitStatus.DISPATCHING.value,
            run_id=sibling.id,
        )
    run.total_units = 1
    db_session.flush()
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "4")

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    # active_count already == cap, so there is no room for even one reclaim.
    assert str(filler.id) in set(decision.capped_unit_ids)


def test_a_capped_reclaim_is_admitted_on_a_later_pass(db_session, monkeypatch) -> None:
    """Oldest-stale-first guarantees every strand reaches the head.

    Capping reclaims is only safe because the order rotates. Ordering by unit
    id -- a stable UUID -- is what made the SAME unit lose every pass forever;
    ordering by ``updated_at`` means the units that missed out are strictly
    the oldest next time.
    """

    from dev_health_ops.sync.guard import DispatchGuard

    run, filler = _seed_run(db_session, provider="github", dataset_key="commits")
    filler.id = _filler_id(0)
    filler.status = SyncRunUnitStatus.DISPATCHING.value
    # The youngest stale unit, but the LOWEST id -- pre-fix it won every pass.
    filler.updated_at = datetime.now(timezone.utc) - timedelta(minutes=20)

    # The real production casualty: oldest, but the highest id.
    _add_unit(
        db_session,
        run,
        filler,
        unit_id=_STRANDED_ID,
        status=SyncRunUnitStatus.DISPATCHING.value,
        updated_at=datetime.now(timezone.utc) - timedelta(hours=16),
    )
    run.total_units = 2
    db_session.flush()
    # Exactly one slot: the two reclaims must compete.
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")

    decision = DispatchGuard.authorize_run(db_session, str(run.id))

    # The longest-stranded unit takes the slot, not the lowest-id one.
    assert str(_STRANDED_ID) not in set(decision.capped_unit_ids)
    assert str(filler.id) in set(decision.capped_unit_ids)


def _seed_already_stranded_unit(db_session, *, dataset_key: str, age_minutes: int = 90):
    """The exact production casualty, with no cap involved.

    Claimed to ``dispatching`` by some earlier pass, never published: no lease,
    no lease expiry, no heartbeat, zero attempts, and no ``worker_job_outbox``
    row. Nothing in the system selects it -- the dispatcher claims
    planned/retrying/stale-dispatching, and lease repair matches only RUNNING
    units with an expired lease.
    """

    run, unit = _seed_run(db_session, provider="github", dataset_key=dataset_key)
    unit.status = SyncRunUnitStatus.DISPATCHING.value
    unit.attempts = 0
    unit.lease_owner = None
    unit.lease_expires_at = None
    unit.last_heartbeat_at = None
    unit.error = None
    unit.created_at = datetime.now(timezone.utc) - timedelta(
        minutes=max(age_minutes, 16 * 60)
    )
    unit.updated_at = datetime.now(timezone.utc) - timedelta(minutes=age_minutes)
    db_session.flush()
    db_session.commit()
    return run, unit


@pytest.mark.parametrize("dataset_key", DISABLED_ALIASES)
def test_reconciler_drains_an_already_stranded_unit(
    db_session, monkeypatch, dataset_key: str
) -> None:
    """The live-acceptance case: units ALREADY wedged must drain themselves.

    Preventing new strands does not help the ~66 units sitting in production
    right now. This is the sweep that reaches them, and it must not depend on
    the dispatcher selecting them -- it does not.
    """

    from dev_health_ops.workers import sync_reconciler

    run, stranded = _seed_already_stranded_unit(db_session, dataset_key=dataset_key)
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    drained = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == stranded.id).one()
    assert drained.status == SyncRunUnitStatus.FAILED.value
    assert drained.result["error_category"] == FEATURE_DISABLED_ERROR_CATEGORY
    assert drained.lease_owner is None
    assert drained.available_at is None
    # The durable reason an operator reads off the row.
    assert drained.last_retry_reason
    assert "no lease" in drained.last_retry_reason


def test_reconciler_does_not_touch_a_freshly_dispatching_unit(
    db_session, monkeypatch
) -> None:
    """Input symmetry: a unit still inside its window must survive.

    A just-claimed unit legitimately sits in ``dispatching`` with zero
    attempts until the relay hands it to River and a worker starts it. The
    sweep must not destroy live work.
    """

    from dev_health_ops.workers import sync_reconciler

    run, fresh = _seed_already_stranded_unit(
        db_session, dataset_key="tests", age_minutes=0
    )
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    untouched = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == fresh.id).one()
    assert untouched.status == SyncRunUnitStatus.DISPATCHING.value
    assert untouched.error is None


def test_reconciler_does_not_touch_a_unit_that_reached_the_river_relay(
    db_session, monkeypatch
) -> None:
    """Input symmetry: an outbox row means River owns it, not this sweep.

    CHAOS-3951's reclaim covers a delivered-but-stuck River job. Terminalizing
    it here would race that recovery and could fail work that is about to run.
    """

    from dev_health_ops.models import WorkerJobOutbox
    from dev_health_ops.workers import sync_reconciler

    run, published = _seed_already_stranded_unit(db_session, dataset_key="tests")
    db_session.add(
        WorkerJobOutbox(
            dedupe_key=f"sync.provider_unit:{published.id}",
            job_kind="sync.provider_unit",
            contract_version=1,
            args={},
            payload_hash="sha256:" + "0" * 64,
            queue="sync",
            priority=1,
            max_attempts=5,
            scheduled_at=datetime.now(timezone.utc),
            status="pending",
            next_attempt_at=datetime.now(timezone.utc),
            attempt_count=0,
        )
    )
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    untouched = (
        db_session.query(SyncRunUnit).filter(SyncRunUnit.id == published.id).one()
    )
    assert untouched.status == SyncRunUnitStatus.DISPATCHING.value
    assert untouched.error is None


def test_reconciler_does_not_touch_a_leased_running_unit(
    db_session, monkeypatch
) -> None:
    """Input symmetry: a unit a worker actually holds is never swept."""

    from dev_health_ops.workers import sync_reconciler

    run, working = _seed_already_stranded_unit(db_session, dataset_key="tests")
    working.status = SyncRunUnitStatus.RUNNING.value
    working.lease_owner = "worker-1"
    working.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)
    working.last_heartbeat_at = datetime.now(timezone.utc)
    working.attempts = 1
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    untouched = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == working.id).one()
    assert untouched.status == SyncRunUnitStatus.RUNNING.value


# NOTE: the two "does not blame a switch" tests formerly here are superseded
# by ``test_reason_names_the_non_river_route_when_river_does_not_own_units``
# and ``test_reason_names_the_atomic_family_for_a_work_item_alias`` above --
# CHAOS-4054 deleted the switch plane those tests guarded against naming, so
# their successor asserts the (now switch-free) reason text directly rather
# than the absence of one particular deleted variable name.


def test_sweep_still_reaches_a_unit_the_dispatcher_keeps_re_stamping(
    db_session, monkeypatch
) -> None:
    """The sweep's age clock must not be one the reclaim path resets.

    ``_claim_units`` re-stamps ``updated_at`` every time it reclaims a stale
    unit. If the sweep aged off ``updated_at``, a unit caught in a
    reclaim-and-republish loop would reset that clock every stale window and
    could never age into the sweep -- the exact case the sweep exists for.
    Anchoring the age bound to ``created_at`` closes it.
    """

    from dev_health_ops.workers import sync_reconciler

    run, stranded = _seed_already_stranded_unit(db_session, dataset_key="tests")
    # Long-lived, but touched recently enough that an ``updated_at``-based age
    # bound would exempt it -- while still past the ordinary stale window.
    stranded.created_at = datetime.now(timezone.utc) - timedelta(hours=16)
    stranded.updated_at = datetime.now(timezone.utc) - timedelta(seconds=1000)
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    drained = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == stranded.id).one()
    assert drained.status == SyncRunUnitStatus.FAILED.value


def test_sweep_spares_a_long_lived_unit_the_dispatcher_just_published(
    db_session, monkeypatch
) -> None:
    """Input symmetry for the two-clock rule.

    An old unit that was only just dispatched is live work, not a strand: the
    system has not given up on it once yet.
    """

    from dev_health_ops.workers import sync_reconciler

    run, fresh = _seed_already_stranded_unit(db_session, dataset_key="tests")
    fresh.created_at = datetime.now(timezone.utc) - timedelta(hours=16)
    fresh.updated_at = datetime.now(timezone.utc)
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    untouched = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == fresh.id).one()
    assert untouched.status == SyncRunUnitStatus.DISPATCHING.value
    assert untouched.error is None


def test_sweep_yields_to_a_concurrent_dispatcher_touch(db_session, monkeypatch) -> None:
    """The sweep must lose a race with a dispatcher that republishes.

    The outbox check happens during selection, so a dispatcher that reclaims
    and publishes AFTER that read would insert its ``worker_job_outbox`` row
    while the never-leased shape still matched -- and the sweep would
    terminalize a unit that had just acquired a River job. ``_claim_units``
    re-stamps ``updated_at`` in that same transaction, so the write pins the
    exact value it read and yields if anything moved.

    The interleaving here is real: select, then mutate, then write.
    """

    from dev_health_ops.workers import sync_reconciler

    run, stranded = _seed_already_stranded_unit(db_session, dataset_key="tests")
    now = datetime.now(timezone.utc)

    selected = sync_reconciler._select_unreclaimable_dispatching_units(
        db_session, now, 100
    )
    assert [unit.id for unit in selected] == [stranded.id]

    # The dispatcher reclaims and publishes, re-stamping updated_at.
    db_session.execute(
        update(SyncRunUnit)
        .where(SyncRunUnit.id == stranded.id)
        .values(updated_at=datetime.now(timezone.utc) + timedelta(seconds=1))
        .execution_options(synchronize_session=False)
    )
    db_session.flush()

    terminalized, touched = sync_reconciler._terminalize_selected_unreclaimable_units(
        db_session, selected, now
    )

    assert terminalized == 0
    assert touched == set()
    db_session.expire_all()
    survivor = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == stranded.id).one()
    assert survivor.status == SyncRunUnitStatus.DISPATCHING.value
    assert survivor.error is None


def test_sweep_writes_when_nothing_moved(db_session, monkeypatch) -> None:
    """Positive control for the race guard.

    Without this, the guard could pin an impossible predicate and the race
    test above would pass for the wrong reason.
    """

    from dev_health_ops.workers import sync_reconciler

    run, stranded = _seed_already_stranded_unit(db_session, dataset_key="tests")
    now = datetime.now(timezone.utc)

    selected = sync_reconciler._select_unreclaimable_dispatching_units(
        db_session, now, 100
    )
    terminalized, touched = sync_reconciler._terminalize_selected_unreclaimable_units(
        db_session, selected, now
    )

    assert terminalized == 1
    assert touched == {stranded.sync_run_id}


def test_sweep_sees_a_strand_hidden_behind_a_page_of_published_units(
    db_session, monkeypatch
) -> None:
    """Filtering must not happen AFTER the limit.

    A page full of River-published rows would otherwise hide a genuine strand
    behind them on every pass -- the same deterministic-loser failure this
    ticket is about, reintroduced one layer down (review round 2).
    """

    from dev_health_ops.models import WorkerJobOutbox
    from dev_health_ops.workers import sync_reconciler

    run, older = _seed_already_stranded_unit(db_session, dataset_key="tests")
    # Three OLDER units that all reached the River relay, so they fill the
    # first page and are then filtered out.
    for index in range(3):
        published_id = uuid.UUID(f"bbbbbbbb-0000-4000-8000-{index:012d}")
        db_session.add(
            SyncRunUnit(
                id=published_id,
                org_id=run.org_id,
                sync_run_id=run.id,
                integration_id=older.integration_id,
                source_id=older.source_id,
                provider="github",
                dataset_key="commits",
                cost_class="medium",
                mode=older.mode,
                status=SyncRunUnitStatus.DISPATCHING.value,
                attempts=0,
                processor_flags={"sync_git": True},
                created_at=datetime.now(timezone.utc) - timedelta(hours=20),
                updated_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
        )
        db_session.add(
            WorkerJobOutbox(
                dedupe_key=f"sync.provider_unit:{published_id}",
                job_kind="sync.provider_unit",
                contract_version=1,
                args={},
                payload_hash="sha256:" + "0" * 64,
                queue="sync",
                priority=1,
                max_attempts=5,
                scheduled_at=datetime.now(timezone.utc),
                status="pending",
                next_attempt_at=datetime.now(timezone.utc),
                attempt_count=0,
            )
        )
    db_session.commit()

    # A limit of 1: the first page is entirely published rows.
    selected = sync_reconciler._select_unreclaimable_dispatching_units(
        db_session, datetime.now(timezone.utc), 1
    )

    assert [unit.id for unit in selected] == [older.id]


def test_sweep_spares_a_plannable_pair(db_session, monkeypatch) -> None:
    """Routability, not age, is the gate: a plannable pair routes to River.

    Input symmetry for the sweep. The strand-shaped row here is identical to
    the one the sweep drains above in every respect except its pair, so the
    matrix verdict is the only thing that can spare it.
    """

    from dev_health_ops.workers import sync_reconciler

    run, unit = _seed_already_stranded_unit(db_session, dataset_key="repo-metadata")
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    survivor = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == unit.id).one()
    assert survivor.status == SyncRunUnitStatus.DISPATCHING.value
    assert survivor.error is None


@pytest.mark.parametrize("state", ("rolled-back", "paused"))
def test_sweep_declines_when_river_does_not_own_provider_units(
    db_session, monkeypatch, state: str
) -> None:
    """The sweep must not destroy work another owner may still be holding.

    The other half of the gate, and the destructive half. The unit here is the
    SAME non-plannable, strand-shaped row the sweep terminalizes above -- the
    only difference is that the durable ``sync.provider_unit`` route no longer
    selects the River outbox. That alone must spare it.

    This is the twin of the Go sweep's ``riverOwns()`` fence, which declines on
    a paused or non-River route for exactly this reason. CHAOS-4054 step 4
    removed the Celery-presence probe that used to supply the Python half, and
    an adversarial review caught that nothing replaced it: during a rollback,
    the Python sweep was terminalizing aged units its Go counterpart spares.
    A unit waiting on another owner is indistinguishable from a strand when
    viewed through the capability matrix alone, so the matrix must not be
    asked until River ownership is established.
    """

    from dev_health_ops.workers import sync_reconciler

    run, unit = _seed_already_stranded_unit(db_session, dataset_key="pr-comments")
    route_row = db_session.query(WorkerJobRoute).filter(
        WorkerJobRoute.job_kind == "sync.provider_unit"
    )
    if state == "paused":
        route_row.update({WorkerJobRoute.paused: True})
    else:
        route_row.update({WorkerJobRoute.transport: "celery"})
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    survivor = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == unit.id).one()
    assert survivor.status == SyncRunUnitStatus.DISPATCHING.value
    assert survivor.error is None


def test_sweep_logs_a_paused_route_at_warning_not_error(
    db_session, monkeypatch, caplog
) -> None:
    """A deliberate route pause must not read as a Sentry-worthy incident.

    Adversarial review finding: reconcile_sync_dispatch is scheduled every
    60s and can revisit the same aged units on every pass. Logging a
    deliberate operator pause (job_routes.py's "worker job route is paused"
    branch of WorkerJobRouteError) at ERROR -- the same level CHAOS-3957's
    savepoint fix uses for a genuine read failure -- would fire a Sentry
    incident roughly once a minute for as long as the pause lasts, drowning
    out a genuine one. Only "paused" is downgraded to WARNING; every other
    WorkerJobRouteError reason (unavailable store, drift) stays at ERROR,
    covered by test_sweep_declines_when_river_does_not_own_provider_units's
    "rolled-back" case alongside the assertions below.
    """

    from dev_health_ops.workers import sync_reconciler

    run, unit = _seed_already_stranded_unit(db_session, dataset_key="pr-comments")
    db_session.query(WorkerJobRoute).filter(
        WorkerJobRoute.job_kind == "sync.provider_unit"
    ).update({WorkerJobRoute.paused: True})
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    caplog.set_level(logging.WARNING, logger="dev_health_ops.workers.sync_reconciler")

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    paused_records = [
        record
        for record in caplog.records
        if record.message == "reconcile_sync_dispatch.unreclaimable_routability_paused"
    ]
    assert len(paused_records) == 1
    assert paused_records[0].levelno == logging.WARNING
    assert not any(
        record.message
        == "reconcile_sync_dispatch.unreclaimable_routability_unavailable"
        for record in caplog.records
    )


def test_sweep_logs_a_drifted_route_at_error_not_warning(
    db_session, monkeypatch, caplog
) -> None:
    """Drift is a real fault, not an operator's deliberate choice -- stays loud.

    Rolling the route back to ``celery`` (the rollback transport) doesn't
    even reach the except block -- ``resolve_worker_job_route`` returns it
    cleanly, and ``_only_unroutable``'s own "not in PROVIDER_UNIT_OUTBOX_ROUTES"
    check handles it via ordinary control flow (see
    ``test_sweep_declines_when_river_does_not_own_provider_units``'s
    "rolled-back" case). Genuine drift needs a transport that is neither the
    checked-in policy route nor the celery rollback -- ``shadow`` is exactly
    that (job_routes.py's ``_ROUTES``/``resolve_worker_job_route``) -- which
    is what actually raises WorkerJobRouteError("... drifts from checked-in
    policy"). That is a real fault an operator did not choose through this
    route's own pause switch, so it must not be silently downgraded
    alongside a genuine pause.
    """

    from dev_health_ops.workers import sync_reconciler

    run, unit = _seed_already_stranded_unit(db_session, dataset_key="pr-comments")
    db_session.query(WorkerJobRoute).filter(
        WorkerJobRoute.job_kind == "sync.provider_unit"
    ).update({WorkerJobRoute.transport: "shadow"})
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)
    caplog.set_level(logging.WARNING, logger="dev_health_ops.workers.sync_reconciler")

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    unavailable_records = [
        record
        for record in caplog.records
        if record.message
        == "reconcile_sync_dispatch.unreclaimable_routability_unavailable"
    ]
    assert len(unavailable_records) == 1
    assert unavailable_records[0].levelno == logging.ERROR
    assert not any(
        record.message == "reconcile_sync_dispatch.unreclaimable_routability_paused"
        for record in caplog.records
    )


#: The production wedge as of 2026-08-20 ~15:03 UTC: units the Go runtime
#: cannot execute, split across the three disabled aliases and their two cost
#: classes (``tests`` is heavy, the PR-social pair is medium -- from
#: ``contracts/provider-matrix/v1/matrix.json``).
_WEDGE_SHAPE = (
    ("tests", "heavy", 27, 28),
    ("pr-comments", "medium", 21, 22),
    ("pr-reviews", "medium", 18, 19),
)


def _seed_production_wedge(db_session, monkeypatch):
    """Rebuild the real wedge: 66 stranded ``dispatching`` + 69 ``planned``."""

    run, seed = _seed_run(db_session, provider="github", dataset_key="commits")
    seed.status = SyncRunUnitStatus.SUCCESS.value
    created = datetime.now(timezone.utc) - timedelta(hours=16)
    index = 0
    stranded_ids: list[uuid.UUID] = []
    planned_ids: list[uuid.UUID] = []
    for dataset_key, cost_class, stranded_n, planned_n in _WEDGE_SHAPE:
        for _ in range(stranded_n):
            index += 1
            unit_id = uuid.UUID(f"cccccccc-0000-4000-8000-{index:012d}")
            stranded_ids.append(unit_id)
            db_session.add(
                SyncRunUnit(
                    id=unit_id,
                    org_id=run.org_id,
                    sync_run_id=run.id,
                    integration_id=seed.integration_id,
                    source_id=seed.source_id,
                    provider="github",
                    dataset_key=dataset_key,
                    cost_class=cost_class,
                    mode=seed.mode,
                    status=SyncRunUnitStatus.DISPATCHING.value,
                    attempts=0,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_heartbeat_at=None,
                    processor_flags={"sync_git": True},
                    created_at=created,
                    updated_at=created,
                )
            )
        for _ in range(planned_n):
            index += 1
            unit_id = uuid.UUID(f"cccccccc-0000-4000-8000-{index:012d}")
            planned_ids.append(unit_id)
            db_session.add(
                SyncRunUnit(
                    id=unit_id,
                    org_id=run.org_id,
                    sync_run_id=run.id,
                    integration_id=seed.integration_id,
                    source_id=seed.source_id,
                    provider="github",
                    dataset_key=dataset_key,
                    cost_class=cost_class,
                    mode=seed.mode,
                    status=SyncRunUnitStatus.PLANNED.value,
                    attempts=0,
                    processor_flags={"sync_git": True},
                    created_at=created,
                )
            )
    run.total_units = 1 + len(stranded_ids) + len(planned_ids)
    db_session.flush()
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
    _patch_db_session(monkeypatch, db_session)
    db_session.commit()
    return run, stranded_ids, planned_ids


def _wedge_population(db_session, run):
    rows = (
        db_session.query(SyncRunUnit)
        .filter(
            SyncRunUnit.sync_run_id == run.id,
            SyncRunUnit.dataset_key.in_([d for d, _c, _s, _p in _WEDGE_SHAPE]),
        )
        .all()
    )
    return {
        "dispatching": sum(
            1 for r in rows if r.status == SyncRunUnitStatus.DISPATCHING.value
        ),
        "planned": sum(1 for r in rows if r.status == SyncRunUnitStatus.PLANNED.value),
        "failed": sum(1 for r in rows if r.status == SyncRunUnitStatus.FAILED.value),
    }


def test_production_wedge_drains_to_zero_with_predicted_counters(
    db_session, monkeypatch, caplog
) -> None:
    """The live acceptance case, at the real shape, with predicted numbers.

    Seeds the production wedge (66 stranded ``dispatching`` + 69 ``planned``
    disabled-alias units) and drives dispatch/reconcile cycles. Asserts the
    stuck population reaches ZERO with no manual SQL, and records how many
    cycles that takes so prod can be checked against a prediction rather than
    an inference.

    The deadlock signature this replaces is ``guard.reclaimed_stale == 0``
    forever while ``guard.capped_new`` stays high -- the state the five
    production runs were in for sixteen hours.
    """

    import logging

    from dev_health_ops.workers import sync_reconciler, sync_units

    run, stranded_ids, planned_ids = _seed_production_wedge(db_session, monkeypatch)
    _patch_worker_enqueues(monkeypatch)

    start = _wedge_population(db_session, run)
    assert start["dispatching"] == 66
    assert start["planned"] == 69

    cycles = 0
    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.guard"):
        for _ in range(40):
            cycles += 1
            sync_units.dispatch_sync_run(str(run.id))
            sync_reconciler.reconcile_sync_dispatch(limit=100)
            db_session.expire_all()
            population = _wedge_population(db_session, run)
            if population["dispatching"] == 0 and population["planned"] == 0:
                break

    final = _wedge_population(db_session, run)
    assert final["dispatching"] == 0, f"strands survived after {cycles} cycles"
    assert final["planned"] == 0
    assert final["failed"] == 135

    # Every unit carries the durable reason the ticket asked for.
    drained = (
        db_session.query(SyncRunUnit)
        .filter(SyncRunUnit.id.in_(stranded_ids + planned_ids))
        .all()
    )
    assert all(
        unit.result["error_category"] == FEATURE_DISABLED_ERROR_CATEGORY
        for unit in drained
    )
    assert all(unit.last_retry_reason for unit in drained)

    # The counters are emitted and predictable. During the drain reclaims must
    # actually happen -- ``reclaimed_stale > 0`` is precisely the signal that
    # was absent for sixteen hours in production, where it stayed 0 while
    # ``capped_new`` stayed high.
    def _decisions():
        return [
            record
            for record in caplog.records
            if record.getMessage() == "dispatch_guard.bucket_decision"
        ]

    during = _decisions()
    assert during, "guard emitted no bucket decisions"
    assert all(hasattr(record, "guard.reclaimed_stale") for record in during)
    assert max(getattr(r, "guard.reclaimed_stale") for r in during) > 0
    assert max(getattr(r, "guard.capped_new") for r in during) > 0

    # One more quiescent pass: with the wedge gone there is nothing left to
    # reclaim and nothing left to cap. These are the values prod should show
    # once the drain completes.
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.guard"):
        sync_units.dispatch_sync_run(str(run.id))
    quiescent = _decisions()
    assert all(getattr(r, "guard.reclaimed_stale") == 0 for r in quiescent)
    assert all(getattr(r, "guard.capped_new") == 0 for r in quiescent)

    # Recorded so the PR body states a measured number, not an estimate.
    print(f"\nCHAOS-3990 wedge drained in {cycles} dispatch/reconcile cycles")


def test_production_wedge_run_finalizes_partial_failed(db_session, monkeypatch) -> None:
    """After the drain the wedged run must reach a terminal aggregate."""

    from dev_health_ops.models import SyncRun, SyncRunStatus
    from dev_health_ops.workers import sync_reconciler, sync_units

    run, _stranded, _planned = _seed_production_wedge(db_session, monkeypatch)
    _patch_worker_enqueues(monkeypatch)

    for _ in range(40):
        sync_units.dispatch_sync_run(str(run.id))
        sync_reconciler.reconcile_sync_dispatch(limit=100)
        db_session.expire_all()
        if _wedge_population(db_session, run)["dispatching"] == 0:
            if _wedge_population(db_session, run)["planned"] == 0:
                break

    sync_units.finalize_sync_run(str(run.id))
    db_session.expire_all()
    finalized = db_session.query(SyncRun).filter(SyncRun.id == run.id).one()
    assert finalized.status == SyncRunStatus.PARTIAL_FAILED.value
    assert finalized.completed_at is not None


def test_sweep_reaches_a_strand_behind_a_page_of_routable_units(
    db_session, monkeypatch
) -> None:
    """Routability must be applied per page, not to a fixed prefix.

    A page of River-routable units would otherwise mask a genuine strand
    behind them on every pass -- the deterministic-loser failure again, one
    layer down (hunt finding).
    """

    from dev_health_ops.workers import sync_reconciler

    run, strand = _seed_already_stranded_unit(db_session, dataset_key="tests")
    # Three OLDER units on a route-ready, plannable pair: routable, so
    # filtered by _only_unroutable, and old enough to sort ahead of the real
    # strand.
    for index in range(3):
        db_session.add(
            SyncRunUnit(
                id=uuid.UUID(f"dddddddd-0000-4000-8000-{index:012d}"),
                org_id=run.org_id,
                sync_run_id=run.id,
                integration_id=strand.integration_id,
                source_id=strand.source_id,
                provider="github",
                dataset_key="repo-metadata",
                cost_class="light",
                mode=strand.mode,
                status=SyncRunUnitStatus.DISPATCHING.value,
                attempts=0,
                processor_flags={},
                created_at=datetime.now(timezone.utc) - timedelta(hours=20),
                updated_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
        )
    db_session.commit()

    selected = sync_reconciler._select_unreclaimable_dispatching_units(
        db_session, datetime.now(timezone.utc), 1
    )

    assert [unit.id for unit in selected] == [strand.id]


def test_sweep_reaches_a_strand_that_was_ever_budget_deferred(
    db_session, monkeypatch
) -> None:
    """A synthetic heartbeat must not exempt a unit forever.

    ``BudgetGuard`` stamps ``last_heartbeat_at`` on a deferral even though no
    worker ran, and the later claim does not clear it. A strict ``IS NULL``
    predicate would permanently exempt every unit that was ever budget-
    deferred (hunt finding).
    """

    from dev_health_ops.workers import sync_reconciler

    run, strand = _seed_already_stranded_unit(db_session, dataset_key="tests")
    # The leftover of a budget deferral: old, and no worker ever ran. Written
    # via UPDATE so SQLAlchemy's onupdate does not reset ``updated_at`` and
    # quietly make the row fresh (which would pass this test for the wrong
    # reason).
    db_session.execute(
        update(SyncRunUnit)
        .where(SyncRunUnit.id == strand.id)
        .values(
            last_heartbeat_at=datetime.now(timezone.utc) - timedelta(hours=4),
            updated_at=datetime.now(timezone.utc) - timedelta(minutes=90),
        )
        .execution_options(synchronize_session=False)
    )
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    drained = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == strand.id).one()
    assert drained.status == SyncRunUnitStatus.FAILED.value


def test_sweep_spares_a_unit_with_a_live_heartbeat(db_session, monkeypatch) -> None:
    """Input symmetry: something actively heartbeating is live work."""

    from dev_health_ops.workers import sync_reconciler

    run, unit = _seed_already_stranded_unit(db_session, dataset_key="tests")
    # Live heartbeat, but an otherwise strand-shaped (stale updated_at) row --
    # so the heartbeat is the ONLY thing sparing it.
    db_session.execute(
        update(SyncRunUnit)
        .where(SyncRunUnit.id == unit.id)
        .values(
            last_heartbeat_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc) - timedelta(minutes=90),
        )
        .execution_options(synchronize_session=False)
    )
    db_session.commit()
    _patch_db_session(monkeypatch, db_session)

    sync_reconciler.reconcile_sync_dispatch(limit=100)

    db_session.expire_all()
    survivor = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == unit.id).one()
    assert survivor.status == SyncRunUnitStatus.DISPATCHING.value


def test_routability_failure_does_not_abort_the_reconcile_pass(
    db_session, monkeypatch
) -> None:
    """An unreadable contract must degrade to 'sweep nothing', not kill
    reconcile.

    The sweep shares a pass with lease repair and wakeup materialization; an
    exception here would take those down with it (hunt finding). CHAOS-4054
    moved the failing seam -- the sweep no longer resolves a durable route, it
    asks the capability matrix -- but the fail-safe is the same one, and it
    must still spare the unit rather than terminalize on a guess.
    """

    from dev_health_ops.workers import provider_unit_route, sync_reconciler

    run, unit = _seed_already_stranded_unit(db_session, dataset_key="tests")
    _patch_db_session(monkeypatch, db_session)

    def _boom(*_args, **_kwargs):
        raise RuntimeError("capability matrix contract drifted")

    monkeypatch.setattr(provider_unit_route, "routes_to_river", _boom)

    # Must not raise.
    result = sync_reconciler.reconcile_sync_dispatch(limit=100)

    assert result is not None
    db_session.expire_all()
    survivor = db_session.query(SyncRunUnit).filter(SyncRunUnit.id == unit.id).one()
    assert survivor.status == SyncRunUnitStatus.DISPATCHING.value


def test_guard_reports_capped_stale_so_a_wedge_is_not_read_as_idle(
    db_session, monkeypatch, caplog
) -> None:
    """A bucket saturated purely by stale work must not look quiescent.

    Without ``guard.capped_stale`` such a bucket reports
    ``reclaimed_stale=0, capped_new=0`` -- identical to idle, which is the
    wedge reading as healthy (hunt finding).
    """

    import logging

    from dev_health_ops.sync.guard import DispatchGuard

    run, filler = _seed_run(db_session, provider="github", dataset_key="commits")
    filler.id = _filler_id(0)
    filler.status = SyncRunUnitStatus.DISPATCHING.value
    filler.updated_at = datetime.now(timezone.utc) - timedelta(minutes=30)
    for index in range(1, 4):
        _add_unit(
            db_session,
            run,
            filler,
            unit_id=_filler_id(index),
            status=SyncRunUnitStatus.DISPATCHING.value,
            updated_at=datetime.now(timezone.utc) - timedelta(minutes=30),
        )
    run.total_units = 4
    db_session.flush()
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")

    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.guard"):
        DispatchGuard.authorize_run(db_session, str(run.id))

    decisions = [
        record
        for record in caplog.records
        if record.getMessage() == "dispatch_guard.bucket_decision"
    ]
    assert decisions
    record = decisions[-1]
    assert getattr(record, "guard.capped_new") == 0
    # Three stale reclaims could not be taken -- the wedge signal.
    assert getattr(record, "guard.capped_stale") == 3
    assert getattr(record, "guard.reclaimed_stale") == 1
