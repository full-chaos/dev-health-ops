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

    CHAOS-4047 (owner decision, 2026-08-21): planning now gates on the pair's
    own River switch -- Celery fallback is retired, so a disabled switch
    means "never planned," not "planned and handled by some other runtime."
    Each pair's own switch is set ON here (never a blanket preset) so the
    sweep also proves ``_SWITCH_ENV``/``MATRIX_PAIRS``' derived field names
    actually enable their pair. Without this, a change that made
    ``plan_sync_run`` return zero units for every ENABLED pair would turn the
    parity sweep green while proving nothing.
    """

    _disable_local_all_routes_preset(monkeypatch)
    planning_pairs = 0
    for provider, dataset, field in MATRIX_PAIRS:
        monkeypatch.setenv(_SWITCH_ENV[field], "true")
        with _fresh_session() as session:
            outcome = _plan_and_dispatch(
                session, monkeypatch, provider=provider, dataset=dataset
            )
        monkeypatch.delenv(_SWITCH_ENV[field], raising=False)
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
        # CHAOS-3990 widened this payload with an operator-facing reason and
        # the refused pair. The machine-readable category every downstream
        # reader keys on is unchanged, which is what this test pins.
        assert unit.result["error_category"] == "feature_disabled"
        assert unit.result["provider"] == "launchdarkly"
        assert unit.result["dataset_key"] == "feature-flags"
        assert unit.last_retry_reason


def test_route_disabled_pair_is_never_planned_regardless_of_celery_presence(
    monkeypatch,
) -> None:
    """CHAOS-4047 (owner decision, 2026-08-21): Celery fallback is retired.

    Formerly ``test_dispatch_still_uses_celery_when_consumers_are_present``,
    pinning the CUT-19 mixed-run rollback rehearsal: a disabled switch still
    planned a unit, and a live Celery fallback profile picked it up. The
    owner decommissioned that fallback outright ("why bother saving any
    celery work at all ... I literally made the call to get rid of it") --
    ``plan_sync_run`` now excludes a route-disabled pair at plan time no
    matter what a Celery consumer probe would say, because there is no
    longer any runtime the switch-off pair could reach. The dispatcher's
    CELERY/DEFER machinery is deliberately left in place (a separate ticket
    owns removing it) but this proves it is now unreachable from planning.
    """

    _disable_local_all_routes_preset(monkeypatch)
    monkeypatch.setenv("WORKER_LAUNCHDARKLY_FEATURE_FLAGS_ENABLED", "false")

    with _fresh_session() as session:
        integration = _seed_integration(session, "launchdarkly", ("feature-flags",))
        for presence in (
            CeleryConsumerPresence.PRESENT,
            CeleryConsumerPresence.ABSENT,
            CeleryConsumerPresence.UNKNOWN,
        ):
            _pin_presence(monkeypatch, presence)
            plan = plan_sync_run(
                session,
                SyncPlanRequest(
                    org_id=str(integration.org_id),
                    integration_id=str(integration.id),
                    mode=SyncRunMode.INCREMENTAL.value,
                    triggered_by="parity-test",
                ),
            )
            assert plan.total_units == 0, (
                f"presence={presence}: a route-disabled pair must never be "
                "planned now that the Celery fallback is retired"
            )


# ---------------------------------------------------------------------------
# CHAOS-3941 review finding (caught by CI, not by the local gate): the
# transport gate's "fail open" only fails open if it owns a SAVEPOINT.
#
# Formerly ``test_undecidable_consumer_state_neither_publishes_nor_terminalizes``,
# pinning that a reachable-but-undecidable broker (pidbox control plane
# failing) neither publishes nor terminalizes a planned unit. Retired by
# owner decision, 2026-08-21: Celery is not a transport anymore, so a probe
# of "is Celery listening" no longer has a live broker on the other end to
# be undecidable about. The premise this test exercised (a switch-off unit
# still gets planned, and dispatch alone must sort out its transport) no
# longer holds -- ``test_route_disabled_pair_is_never_planned_regardless_of_celery_presence``
# above already proves presence (including UNKNOWN) has zero bearing on
# planning, which is the property that matters now. The dispatcher's DEFER
# branch itself is untouched and stays wired (a separate ticket owns
# removing the Celery machinery); this just stops asserting a scenario the
# ratified topology no longer produces.
# ---------------------------------------------------------------------------
# CHAOS-4047: the plan-time route-switch gate. Fixtures throughout are real
# ``IntegrationDataset``/``IntegrationSource`` rows via ``_seed_integration``,
# never a hand-authored dataset vocabulary.
# ---------------------------------------------------------------------------


def _disable_local_all_routes_preset(monkeypatch) -> None:
    """Deny the ``GO_PROVIDER_ROUTES=all`` local-only preset explicitly.

    Without this, a developer's own shell (``DEV_HEALTH_ENV=local``,
    ``GO_PROVIDER_ROUTES=all`` -- a common local setup) makes
    ``ProviderUnitRouteSwitches.from_environment()`` treat every alias of an
    explicitly-enabled canonical writer as also enabled
    (``_LOCAL_ALL_ALIAS_CANONICAL``), which defeats exactly the switch-off
    assertions these tests make. Explicit switches otherwise win over the
    preset's *defaults*, but the alias-canonicalization fallback inside
    ``_switch_enabled`` is not a default -- it re-checks the canonical field
    whenever the alias's own explicit value is False. CI has neither var set;
    this makes the tests deterministic regardless of the runner's shell.
    """

    monkeypatch.delenv("GO_PROVIDER_ROUTES", raising=False)
    monkeypatch.delenv("DEV_HEALTH_ENV", raising=False)


def test_route_disabled_alias_with_sibling_enabled_is_never_planned(
    monkeypatch,
) -> None:
    """Regression: the exact production failure shape from CHAOS-4047/4048.

    github pr-comments, pr-reviews, and tests are mutually exclusive alias
    identities of the prs/cicd shared writers (config.go:337/:350 reject
    enabling both). With prs enabled and pr-comments left off -- the actual
    prod switch profile -- a pre-CHAOS-4047 planner still minted a
    pr-comments unit that terminalized instantly as
    ``status=failed, error=feature_disabled, attempts=0`` (200 such units in
    one window), burying real failures in the same run.

    POSITIVE CONTROL (verified manually, not asserted here): reverting the
    ``route_switches`` gate in ``_build_planned_units`` back to
    pre-CHAOS-4047 (drop the ``route_switches`` parameter and its exclusion
    check) makes this test FAIL with ``total_units == 1``.
    """

    _disable_local_all_routes_preset(monkeypatch)
    monkeypatch.setenv("WORKER_GITHUB_PRS_ENABLED", "true")
    monkeypatch.setenv("WORKER_GITHUB_PR_COMMENTS_ENABLED", "false")

    with _fresh_session() as session:
        integration = _seed_integration(session, "github", ("pr-comments",))
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(integration.org_id),
                integration_id=str(integration.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )

    assert plan.total_units == 0, (
        "a disabled alias whose sibling is enabled must never be planned; it "
        "would terminalize instantly as feature_disabled and bury real failures"
    )


def test_route_disabled_pair_with_no_sibling_is_never_planned(monkeypatch) -> None:
    """Input symmetry: exclusion is a general 'switch is off' rule.

    github security has no alias sibling at all -- proving the gate is not
    secretly alias-specific, only "is this pair's own switch on."
    """

    _disable_local_all_routes_preset(monkeypatch)
    monkeypatch.setenv("WORKER_GITHUB_SECURITY_ENABLED", "false")

    with _fresh_session() as session:
        integration = _seed_integration(session, "github", ("security",))
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(integration.org_id),
                integration_id=str(integration.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )

    assert plan.total_units == 0


def test_route_enabled_pair_is_still_planned(monkeypatch) -> None:
    """Input symmetry: the gate excludes only disabled pairs, not everything."""

    _disable_local_all_routes_preset(monkeypatch)
    monkeypatch.setenv("WORKER_GITHUB_SECURITY_ENABLED", "true")

    with _fresh_session() as session:
        integration = _seed_integration(session, "github", ("security",))
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(integration.org_id),
                integration_id=str(integration.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )

    assert plan.total_units == 1


def test_route_switches_are_not_consulted_off_the_river_outbox_route(
    monkeypatch,
) -> None:
    """Input symmetry: a non-River ``sync.provider_unit`` route plans
    unfiltered, exactly like before CHAOS-4047.

    ``PROVIDER_UNIT_OUTBOX_ROUTES`` and the Celery transport machinery stay in
    place by owner decision (a separate ticket owns removing them); this pins
    that the plan-time gate only ever engages when the durable route says
    River owns the kind.
    """

    _disable_local_all_routes_preset(monkeypatch)
    monkeypatch.setenv("WORKER_GITHUB_SECURITY_ENABLED", "false")

    with _fresh_session(transport="celery") as session:
        integration = _seed_integration(session, "github", ("security",))
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(integration.org_id),
                integration_id=str(integration.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )

    assert plan.total_units == 1, (
        "off the River outbox route, every enabled dataset must still be "
        "planned unfiltered -- route switches govern River eligibility only"
    )


def test_route_disabled_work_item_family_canonical_claim_is_never_planned(
    monkeypatch,
) -> None:
    """Codex review finding: family ALIASES are deliberately unchecked (their
    admission is the atomic-family collapse's business), but the CANONICAL
    claim the collapse emits ("work-items") is an ordinary routable pair with
    its own switch (WORKER_GITHUB_WORK_ITEMS_ENABLED) that must still gate
    it -- skipping the whole family branch must not also skip that.

    POSITIVE CONTROL (verified manually, not asserted here): dropping the
    post-collapse ``route_switches`` filter in ``_build_planned_units``
    (the ``family_units = [... for unit in family_units ...]`` block) makes
    this test FAIL with ``total_units == 1``.
    """

    _disable_local_all_routes_preset(monkeypatch)
    monkeypatch.setenv("WORKER_GITHUB_WORK_ITEMS_ENABLED", "false")

    with _fresh_session() as session:
        integration = _seed_integration(session, "github", ("work-items",))
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(integration.org_id),
                integration_id=str(integration.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )

    assert plan.total_units == 0, (
        "the canonical work-items claim's own switch is off; it must never "
        "be planned regardless of the alias-collapse machinery"
    )


def test_route_enabled_work_item_family_canonical_claim_is_still_planned(
    monkeypatch,
) -> None:
    """Input symmetry: the family canonical-claim gate excludes only a
    disabled switch, not everything."""

    _disable_local_all_routes_preset(monkeypatch)
    monkeypatch.setenv("WORKER_GITHUB_WORK_ITEMS_ENABLED", "true")

    with _fresh_session() as session:
        integration = _seed_integration(session, "github", ("work-items",))
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(integration.org_id),
                integration_id=str(integration.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )

    assert plan.total_units == 1


def test_route_unknown_pair_fails_closed_not_open(monkeypatch) -> None:
    """Input symmetry: a pair the checked-in matrix does not recognize at all
    is excluded exactly like an explicit switch-off pair -- fail closed by
    construction (``getattr(..., False)``/matrix membership), never an
    exception or an accidental route.
    """

    switches = ProviderUnitRouteSwitches.from_environment(environment={})
    assert switches.routes_to_river("acme-corp", "widgets") is False
