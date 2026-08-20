"""Planner/dispatcher routable-set parity (CHAOS-3941).

The planner decides which units to CREATE from ``IntegrationDataset.is_enabled``
(per-org Postgres rows). The dispatcher decides where to SEND them from
``ProviderUnitRouteSwitches`` (process ``WORKER_*_ENABLED`` env vars, default
False). Nothing asserted the two agreed, so a dataset the customer enabled whose
River switch was off was planned every cycle and published to a Celery broker
with zero consumers: wedged in ``dispatching``, holding the whole DispatchGuard
concurrency budget, never aged out because each pass re-stamped ``updated_at``.

These tests assert the EFFECT, not the call:

  * ``test_planner_and_dispatcher_agree_on_routable_set`` sweeps every
    ``route_ready`` pair in the real ``contracts/provider-matrix/v1/matrix.json``
    with its real switch field, in both switch states, and asserts that with
    Celery consumers absent the planner creates exactly the units the dispatcher
    can hand to a live runtime -- zero Celery publishes, and no unit left
    non-terminal that no runtime will execute.
  * ``test_dispatch_refuses_to_publish_into_a_consumerless_broker`` asserts the
    dispatch-time refusal on a unit that was planned BEFORE the switch flip.
  * ``test_dispatch_still_uses_celery_when_consumers_are_present`` pins the
    CUT-19 / mixed-run rollback path: a live Celery fallback profile still gets
    its publish.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    Integration,
    IntegrationCredential,
    IntegrationDataset,
    IntegrationSource,
    SyncRun,
    SyncRunMode,
    SyncRunReferenceDiscovery,
    SyncRunStatus,
    SyncRunUnit,
    SyncRunUnitStatus,
    WorkerJobOutbox,
    WorkerJobRoute,
)
from dev_health_ops.models.licensing import FeatureFlag
from dev_health_ops.models.users import Organization
from dev_health_ops.sync import planner as planner_module
from dev_health_ops.sync.canonical_incident_gate import (
    CANONICAL_INCIDENT_FEATURE_KEY,
)
from dev_health_ops.sync.planner import SyncPlanRequest, plan_sync_run
from dev_health_ops.workers import celery_consumers, provider_unit_route
from dev_health_ops.workers.celery_consumers import CeleryConsumerPresence
from dev_health_ops.workers.provider_unit_route import ProviderUnitRouteSwitches

from ._helpers import seed_sync_dispatch_transport_routes

#: Every ``route_ready`` pair in the checked-in capability matrix, paired with
#: the switch field name the dispatcher resolves for it. Derived, never
#: hand-listed: a new matrix row is covered the moment it lands.
MATRIX_PAIRS: tuple[tuple[str, str, str], ...] = tuple(
    (provider, dataset, provider_unit_route._switch_field_name(provider, dataset))
    for provider, dataset in sorted(provider_unit_route._route_ready_pairs())
)

#: ``WORKER_<PROVIDER>_<DATASET>_ENABLED`` for a switch field.
_SWITCH_ENV = {
    field: f"WORKER_{field.upper()}_ENABLED"
    for field in ProviderUnitRouteSwitches.__dataclass_fields__
}

_SOURCE_TYPES = {
    "github": "repo",
    "gitlab": "repo",
    "jira": "project",
    "linear": "project",
    "launchdarkly": "project",
    "pagerduty": "service",
}


@contextmanager
def _fresh_session(*, transport: str = "river_canary"):
    """A private in-memory database seeded with the migration-default routes."""

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    try:
        with Session(engine) as session:
            seed_sync_dispatch_transport_routes(session)
            _set_provider_unit_route(session, transport)
            # Incident datasets sit behind an unrelated org feature gate; leave
            # it registered and on so those matrix pairs actually plan units.
            session.add(
                FeatureFlag(
                    key=CANONICAL_INCIDENT_FEATURE_KEY,
                    name="Canonical Incident Ingestion",
                    category="integrations",
                    min_tier="community",
                    is_enabled=True,
                )
            )
            session.flush()
            yield session
    finally:
        engine.dispose()


@contextmanager
def _session_ctx(session):
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


def _pin_presence(monkeypatch, presence: CeleryConsumerPresence) -> None:
    celery_consumers.reset_celery_consumer_probe_cache()
    monkeypatch.setattr(
        celery_consumers,
        "probe_celery_consumers",
        lambda *_args, **_kwargs: presence,
    )


def _dataset_keys_for(provider: str, dataset: str) -> tuple[str, ...]:
    """Datasets to enable so the plan is a COMPLETE, admissible claim.

    Work-item-family datasets collapse into one composite unit that must carry
    the whole family's ownership flags; enabling a single alias would be
    rejected by the family-claim contract for reasons unrelated to this defect.
    """

    if dataset in planner_module._WORK_ITEM_FAMILY_DATASETS:
        return tuple(planner_module._WORK_ITEM_FAMILY_DATASET_ORDER)
    return (dataset,)


def _seed_integration(session: Session, provider: str, dataset_keys) -> Integration:
    org_uuid = uuid.uuid4()
    org_id = str(org_uuid)
    session.add(
        Organization(
            id=org_uuid,
            slug=f"parity-{org_uuid.hex[:12]}",
            name="Parity Org",
            tier="enterprise",
        )
    )
    session.flush()
    # PagerDuty plans only against an ACTIVE org-scoped credential; every other
    # provider ignores this row.
    credential = IntegrationCredential(
        org_id=org_id,
        provider=provider,
        name="default",
        config={"account_id": "acme", "subdomain": "acme"},
        is_active=True,
    )
    session.add(credential)
    session.flush()
    integration = Integration(
        org_id=org_id,
        provider=provider,
        name=f"{provider}-parity",
        config={},
        is_active=True,
        credential_id=credential.id,
    )
    session.add(integration)
    session.flush()
    session.add(
        IntegrationSource(
            org_id=org_id,
            integration_id=integration.id,
            provider=provider,
            source_type=_SOURCE_TYPES.get(provider, "repo"),
            external_id="full-chaos/dev-health",
            name="dev-health",
            full_name="full-chaos/dev-health",
            metadata_={},
            is_enabled=True,
            discovered_at=datetime.now(timezone.utc),
            last_seen_at=datetime.now(timezone.utc),
        )
    )
    session.add_all(
        IntegrationDataset(
            org_id=org_id,
            integration_id=integration.id,
            dataset_key=key,
            is_enabled=True,
            options={},
        )
        for key in dataset_keys
    )
    session.flush()
    return integration


def _mark_discovery_succeeded(session: Session, sync_run_id: str) -> None:
    ledger = (
        session.query(SyncRunReferenceDiscovery)
        .filter(SyncRunReferenceDiscovery.sync_run_id == uuid.UUID(sync_run_id))
        .one_or_none()
    )
    now = datetime.now(timezone.utc)
    if ledger is None:
        run = session.query(SyncRun).filter(SyncRun.id == uuid.UUID(sync_run_id)).one()
        ledger = SyncRunReferenceDiscovery(
            org_id=run.org_id,
            sync_run_id=run.id,
            status="success",
            attempts=1,
            available_at=now,
            completed_at=now,
        )
        session.add(ledger)
    else:
        ledger.status = "success"
        ledger.attempts = 1
        ledger.completed_at = now
    session.flush()


def _set_provider_unit_route(session: Session, transport: str) -> None:
    session.query(WorkerJobRoute).filter(
        WorkerJobRoute.job_kind == "sync.provider_unit"
    ).update({WorkerJobRoute.transport: transport})
    session.flush()


def _capture_celery_publishes(monkeypatch) -> list[tuple[str, str | None]]:
    from dev_health_ops.workers import sync_units

    published: list[tuple[str, str | None]] = []

    class FakeUnitSignature:
        def __init__(self, unit_id):
            self.unit_id = unit_id
            self.queue = None

        def set(self, *, queue):
            self.queue = queue
            return self

        def apply_async(self):
            published.append((self.unit_id, self.queue))

    monkeypatch.setattr(
        sync_units.run_sync_unit, "s", lambda unit_id: FakeUnitSignature(unit_id)
    )
    monkeypatch.setattr(
        sync_units.dispatch_sync_run,
        "apply_async",
        lambda args=None, queue=None, **kwargs: None,
    )
    monkeypatch.setattr(
        sync_units.finalize_sync_run,
        "apply_async",
        lambda args=None, queue=None: None,
    )
    return published


def _plan_and_dispatch(session, monkeypatch, *, provider: str, dataset: str) -> dict:
    """Plan a run for one pair, dispatch it, and report what each side did."""

    import dev_health_ops.db as db
    from dev_health_ops.workers import sync_units

    integration = _seed_integration(
        session, provider, _dataset_keys_for(provider, dataset)
    )
    plan = plan_sync_run(
        session,
        SyncPlanRequest(
            org_id=str(integration.org_id),
            integration_id=str(integration.id),
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="parity-test",
        ),
    )
    planned = (
        session.query(SyncRunUnit)
        .filter(SyncRunUnit.sync_run_id == uuid.UUID(plan.sync_run_id))
        .all()
    )
    planned_pairs = sorted((str(u.provider), str(u.dataset_key)) for u in planned)
    if not planned:
        return {
            "planned_pairs": [],
            "river_pairs": [],
            "terminal_pairs": [],
            "celery_publishes": [],
            "stranded_units": [],
        }

    _mark_discovery_succeeded(session, plan.sync_run_id)
    published = _capture_celery_publishes(monkeypatch)
    monkeypatch.setattr(db, "get_postgres_session_sync", lambda: _session_ctx(session))
    session.commit()

    sync_units.dispatch_sync_run(plan.sync_run_id)

    unit_by_id = {
        str(unit.id): unit
        for unit in session.query(SyncRunUnit)
        .filter(SyncRunUnit.sync_run_id == uuid.UUID(plan.sync_run_id))
        .all()
    }
    river_unit_ids = {
        row.args["payload"]["unit_id"]
        for row in session.query(WorkerJobOutbox).all()
        if row.job_kind == "sync.provider_unit"
    }
    celery_unit_ids = {unit_id for unit_id, _queue in published}
    river_pairs = sorted(
        (str(unit_by_id[unit_id].provider), str(unit_by_id[unit_id].dataset_key))
        for unit_id in river_unit_ids
    )
    # A unit handed to no runtime must be terminal. Anything else is the
    # production shape: a live-looking unit nothing will ever execute, holding
    # a DispatchGuard slot until a human intervenes.
    stranded = sorted(
        (str(unit.provider), str(unit.dataset_key), str(unit.status))
        for unit_id, unit in unit_by_id.items()
        if unit_id not in river_unit_ids
        and unit_id not in celery_unit_ids
        and unit.status
        not in {
            SyncRunUnitStatus.SUCCESS.value,
            SyncRunUnitStatus.FAILED.value,
        }
    )
    # A denied unit must SAY it was denied. Dropping it silently would let a
    # partially-disabled integration finalize SUCCESS with no coverage record.
    terminal_pairs = sorted(
        (str(unit.provider), str(unit.dataset_key))
        for unit_id, unit in unit_by_id.items()
        if unit_id not in river_unit_ids
        and unit.status == SyncRunUnitStatus.FAILED.value
        and unit.error == "feature_disabled"
    )
    return {
        "planned_pairs": planned_pairs,
        "river_pairs": river_pairs,
        "terminal_pairs": terminal_pairs,
        "celery_publishes": list(published),
        "stranded_units": stranded,
    }


@pytest.mark.parametrize(
    ("provider", "dataset", "switch_field"),
    MATRIX_PAIRS,
    ids=[f"{provider}-{dataset}" for provider, dataset, _ in MATRIX_PAIRS],
)
@pytest.mark.parametrize("switch_on", (True, False), ids=("switch-on", "switch-off"))
def test_planner_and_dispatcher_agree_on_routable_set(
    monkeypatch, provider: str, dataset: str, switch_field: str, switch_on: bool
) -> None:
    """No unit is created that no runtime will execute.

    With Celery scaled to zero, the executable set is exactly the River-routable
    set. The planner must produce that set and nothing else, in BOTH switch
    states -- which is the property that failed in production.
    """

    monkeypatch.setenv(_SWITCH_ENV[switch_field], "true" if switch_on else "false")
    _pin_presence(monkeypatch, CeleryConsumerPresence.ABSENT)

    with _fresh_session() as session:
        outcome = _plan_and_dispatch(
            session, monkeypatch, provider=provider, dataset=dataset
        )

    assert outcome["celery_publishes"] == [], (
        f"{provider}/{dataset} switch_on={switch_on}: a unit was published to a "
        "Celery queue with no consumer"
    )
    assert outcome["stranded_units"] == [], (
        f"{provider}/{dataset} switch_on={switch_on}: units left non-terminal "
        "that no runtime will execute"
    )
    assert outcome["planned_pairs"] == sorted(
        outcome["river_pairs"] + outcome["terminal_pairs"]
    ), (
        f"{provider}/{dataset} switch_on={switch_on}: planner planned "
        f"{outcome['planned_pairs']}, but only {outcome['river_pairs']} reached a "
        f"live runtime and only {outcome['terminal_pairs']} were recorded as "
        "denied -- the remainder went nowhere and said nothing"
    )


def test_matrix_sweep_is_not_vacuous(monkeypatch) -> None:
    """Guard the sweep itself: most pairs must really produce planned units.

    Without this, a change that made ``plan_sync_run`` return zero units for
    every pair would turn the parity sweep green while proving nothing.
    """

    _pin_presence(monkeypatch, CeleryConsumerPresence.PRESENT)
    planning_pairs = 0
    for provider, dataset, _field in MATRIX_PAIRS:
        with _fresh_session() as session:
            outcome = _plan_and_dispatch(
                session, monkeypatch, provider=provider, dataset=dataset
            )
        if outcome["planned_pairs"]:
            planning_pairs += 1
    assert planning_pairs >= 40, (
        f"only {planning_pairs}/{len(MATRIX_PAIRS)} matrix pairs produced planned "
        "units; the parity sweep would be near-vacuous"
    )


def test_dispatch_refuses_to_publish_into_a_consumerless_broker(
    monkeypatch,
) -> None:
    """A unit planned before a switch flip must terminalize, not be published.

    This is the residual case the plan-time gate cannot cover, and it is the
    exact production shape: the unit exists, its pair is not River-routable, and
    the Celery queue has no consumer.
    """

    import dev_health_ops.db as db
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED", "true")
    _pin_presence(monkeypatch, CeleryConsumerPresence.ABSENT)

    with _fresh_session() as session:
        outcome = _plan_and_dispatch(
            session,
            monkeypatch,
            provider="launchdarkly",
            dataset="feature-flags",
        )
        assert outcome["river_pairs"] == [("launchdarkly", "feature-flags")]
        run_id = str(session.query(SyncRun).one().id)

        # The operator now turns the switch off; the planned-and-claimed unit
        # from the next pass has no runtime at all.
        monkeypatch.setenv("WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED", "false")
        stranded = session.query(SyncRunUnit).one()
        stranded.status = SyncRunUnitStatus.PLANNED.value
        stranded.updated_at = datetime.now(timezone.utc)
        session.query(SyncRun).filter(SyncRun.id == uuid.UUID(run_id)).update(
            {SyncRun.status: SyncRunStatus.PLANNED.value}
        )
        session.query(WorkerJobOutbox).delete()
        session.flush()

        published = _capture_celery_publishes(monkeypatch)
        monkeypatch.setattr(
            db, "get_postgres_session_sync", lambda: _session_ctx(session)
        )
        session.commit()

        sync_units.dispatch_sync_run(run_id)

        session.expire_all()
        unit = session.query(SyncRunUnit).one()
        assert published == [], "unit was published to a broker with no consumer"
        assert unit.status == SyncRunUnitStatus.FAILED.value
        assert unit.error == "feature_disabled"
        assert unit.result == {"error_category": "feature_disabled"}


def test_dispatch_still_uses_celery_when_consumers_are_present(
    monkeypatch,
) -> None:
    """CUT-19 / mixed-run rollback: a live Celery fallback still gets its work.

    CHAOS-3091 requires starting the complete Celery fallback profile and running
    a representative job through it. Consumer presence -- not the switch -- is
    what this change gates on, so that rehearsal is unaffected.
    """

    import dev_health_ops.db as db
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED", "false")
    _pin_presence(monkeypatch, CeleryConsumerPresence.PRESENT)

    with _fresh_session() as session:
        integration = _seed_integration(session, "launchdarkly", ("feature-flags",))
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(integration.org_id),
                integration_id=str(integration.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )
        assert plan.total_units == 1, "a live Celery fallback must still be planned"
        _mark_discovery_succeeded(session, plan.sync_run_id)
        published = _capture_celery_publishes(monkeypatch)
        monkeypatch.setattr(
            db, "get_postgres_session_sync", lambda: _session_ctx(session)
        )
        session.commit()

        sync_units.dispatch_sync_run(plan.sync_run_id)

        session.expire_all()
        assert [unit_id for unit_id, _queue in published] == list(plan.unit_ids)
        assert session.query(WorkerJobOutbox).count() == 0
        assert (
            session.query(SyncRunUnit).one().status
            == SyncRunUnitStatus.DISPATCHING.value
        )


# ---------------------------------------------------------------------------
# CHAOS-3941 review finding (caught by CI, not by the local gate): the
# transport gate's "fail open" only fails open if it owns a SAVEPOINT.
# ---------------------------------------------------------------------------


def test_undecidable_consumer_state_neither_publishes_nor_terminalizes(
    monkeypatch,
) -> None:
    """UNKNOWN must not publish and must not destroy (review finding).

    A reachable broker whose pidbox control plane is failing looks like
    "cannot tell". Publishing there re-opens the void (apply_async succeeds
    into an empty queue); terminalizing there fails healthy work. The unit is
    released to PLANNED instead, holding no DispatchGuard slot, and re-claimed
    next pass.
    """

    import dev_health_ops.db as db
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED", "false")
    _pin_presence(monkeypatch, CeleryConsumerPresence.UNKNOWN)

    with _fresh_session() as session:
        integration = _seed_integration(session, "launchdarkly", ("feature-flags",))
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(integration.org_id),
                integration_id=str(integration.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )
        _mark_discovery_succeeded(session, plan.sync_run_id)
        published = _capture_celery_publishes(monkeypatch)
        monkeypatch.setattr(
            db, "get_postgres_session_sync", lambda: _session_ctx(session)
        )
        session.commit()

        sync_units.dispatch_sync_run(plan.sync_run_id)

        session.expire_all()
        unit = session.query(SyncRunUnit).one()
        assert published == [], "published while consumer state was unknown"
        assert session.query(WorkerJobOutbox).count() == 0
        assert unit.status == SyncRunUnitStatus.PLANNED.value, (
            "an undecidable unit must be released, not left holding a "
            "DispatchGuard slot in dispatching"
        )
        assert unit.error is None, "an undecidable unit must not be terminalized"
