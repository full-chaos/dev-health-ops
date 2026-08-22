"""Planner/dispatcher routable-set parity (CHAOS-3941, updated for CHAOS-4054).

The planner decides which units to CREATE from ``IntegrationDataset.is_enabled``
(per-org Postgres rows) AND the checked-in capability matrix
(``contracts/provider-matrix/v1/matrix.json``): a unit is only ever minted for
a pair the matrix marks BOTH ``route_ready`` and ``plannable``. The dispatcher
decides where to SEND an already-planned unit, from the durable
``sync.provider_unit`` route (``river_owns_units``) and, only when that route
is not River, a live Celery consumer probe.

CHAOS-4054 deleted the ``WORKER_*_ENABLED`` route-switch environment plane
entirely. Capability is always on in the binary: there is no "switch on" /
"switch off" runtime state left to sweep. What replaces it is a pure
capability-matrix fact -- ``routes_to_river(provider, dataset)`` -- consulted
unconditionally by the planner, regardless of the durable transport route.
These tests assert the EFFECT, not the call:

  * ``test_planner_and_dispatcher_agree_on_routable_set`` sweeps every
    ``route_ready`` pair in the real matrix, in both River-outbox states, and
    asserts that with Celery consumers absent the planner creates exactly the
    units the dispatcher can hand to a live runtime -- zero Celery publishes,
    and no unit left non-terminal that no runtime will execute.
  * ``test_dispatch_refuses_to_publish_into_a_consumerless_broker`` asserts the
    dispatch-time refusal on a unit that was planned before its pair lost
    plannability (the CHAOS-4054 analog of "planned before a switch flip").
  * A capability-matrix alias identity (never plannable) is never planned,
    regardless of celery presence or durable transport -- the direct successor
    to the old "disabled switch is never planned" family of tests.
"""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

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

from ._helpers import seed_sync_dispatch_transport_routes

_MATRIX_CONTRACT = (
    Path(__file__).resolve().parents[1]
    / "contracts"
    / "provider-matrix"
    / "v1"
    / "matrix.json"
)
_MATRIX = json.loads(_MATRIX_CONTRACT.read_text())

#: Every ``route_ready`` pair in the checked-in capability matrix. Derived,
#: never hand-listed: a new matrix row is covered the moment it lands.
MATRIX_PAIRS: tuple[tuple[str, str], ...] = tuple(
    sorted(
        (pair["provider"], pair["dataset"])
        for pair in _MATRIX["pairs"]
        if pair["route_ready"]
    )
)

#: The subset of MATRIX_PAIRS that is a canonical writer identity of its own.
PLANNABLE_PAIRS: frozenset[tuple[str, str]] = frozenset(
    (pair["provider"], pair["dataset"])
    for pair in _MATRIX["pairs"]
    if pair["route_ready"] and pair["plannable"]
)

#: The canonical identity every route-ready pair's intent is served under.
#: An alias maps to the writer it folds onto; a canonical pair maps to itself.
#: Derived from the contract, never hand-listed.
CANONICAL_IDENTITY: dict[tuple[str, str], str] = {
    (pair["provider"], pair["dataset"]): pair["canonical_dataset"]
    for pair in _MATRIX["pairs"]
    if pair["route_ready"]
}

#: Non-family alias identities (route_ready, never plannable, no collapse
#: machinery of their own), used by the "an alias is never planned" tests
#: below. Deliberately NOT a work-item-family alias: requesting one of those
#: in isolation (with no sibling family dataset enabled) hits the family
#: claim-completeness gate instead -- an orthogonal, pre-existing contract
#: unrelated to CHAOS-4054 (see ``_dataset_keys_for``'s docstring).
_GITHUB_PR_ALIAS = ("github", "pr-comments")
_GITLAB_PR_ALIAS = ("gitlab", "pr-comments")

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


def _planned_dataset_keys(session, sync_run_id: str) -> list[str]:
    return sorted(
        str(unit.dataset_key)
        for unit in session.query(SyncRunUnit)
        .filter(SyncRunUnit.sync_run_id == uuid.UUID(sync_run_id))
        .all()
    )


def _plan_and_dispatch(session, monkeypatch, *, provider: str, dataset: str) -> dict:
    """Plan a run for one pair, dispatch it, and report what each side did."""

    import dev_health_ops.db as db
    from dev_health_ops.workers import sync_units

    dataset_keys = _dataset_keys_for(provider, dataset)
    integration = _seed_integration(session, provider, dataset_keys)
    plan = plan_sync_run(
        session,
        SyncPlanRequest(
            org_id=str(integration.org_id),
            integration_id=str(integration.id),
            mode=SyncRunMode.INCREMENTAL.value,
            triggered_by="parity-test",
            # CHAOS-4054: with no runtime switch left to make every other
            # provider dataset opt out on its own, a default-enabled sweep
            # over one pair at a time must scope the request explicitly --
            # PagerDuty's independent identities are ALL always-plannable, so
            # an unfiltered request would plan every pagerduty pair at once
            # regardless of which one this iteration means to exercise.
            dataset_keys=dataset_keys,
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
    ("provider", "dataset"),
    MATRIX_PAIRS,
    ids=[f"{provider}-{dataset}" for provider, dataset in MATRIX_PAIRS],
)
@pytest.mark.parametrize(
    "transport",
    ("river_canary", "celery"),
    ids=("river-owns-units", "celery-owns-units"),
)
def test_planner_and_dispatcher_agree_on_routable_set(
    monkeypatch, provider: str, dataset: str, transport: str
) -> None:
    """No unit is created that no runtime will execute.

    With Celery scaled to zero, the executable set is exactly the River-routable
    set. The planner must produce that set and nothing else, whether or not the
    durable ``sync.provider_unit`` route currently hands units to River -- the
    capability-matrix admission the planner applies is unconditional.
    """

    _pin_presence(monkeypatch, CeleryConsumerPresence.ABSENT)

    with _fresh_session(transport=transport) as session:
        outcome = _plan_and_dispatch(
            session, monkeypatch, provider=provider, dataset=dataset
        )

    assert outcome["celery_publishes"] == [], (
        f"{provider}/{dataset} transport={transport}: a unit was published to a "
        "Celery queue with no consumer"
    )
    assert outcome["stranded_units"] == [], (
        f"{provider}/{dataset} transport={transport}: units left non-terminal "
        "that no runtime will execute"
    )
    assert outcome["planned_pairs"] == sorted(
        outcome["river_pairs"] + outcome["terminal_pairs"]
    ), (
        f"{provider}/{dataset} transport={transport}: planner planned "
        f"{outcome['planned_pairs']}, but only {outcome['river_pairs']} reached a "
        f"live runtime and only {outcome['terminal_pairs']} were recorded as "
        "denied -- the remainder went nowhere and said nothing"
    )
    # The plan-time capability gate is unconditional, and so is the alias
    # fold: every route-ready pair is planned, as one or more windowed units
    # claimed under the CANONICAL identity of its writer family. For a
    # canonical pair that is the pair itself; for an alias it is the writer
    # the alias folds onto (pr-reviews/pr-comments -> prs, tests -> cicd,
    # work-item-* -> work-items). Nothing route-ready plans nothing.
    expected_pair = (provider, CANONICAL_IDENTITY[(provider, dataset)])
    assert outcome["planned_pairs"], f"{provider}/{dataset} must be planned"
    assert set(outcome["planned_pairs"]) == {expected_pair}


def test_matrix_sweep_is_not_vacuous(monkeypatch) -> None:
    """Guard the sweep itself: every matrix pair must really produce units.

    CHAOS-4054: capability is always on in the binary, so every route-ready
    pair is served -- there is no switch left to flip and no identity whose
    intent is silently dropped. A canonical pair plans itself; an alias plans
    the canonical writer it folds onto. This proves both, so a change that
    made ``plan_sync_run`` return zero units for any pair, or that let an
    alias mint a unit of its own, would turn this sweep red.
    """

    planning_pairs = 0
    for provider, dataset in MATRIX_PAIRS:
        with _fresh_session() as session:
            outcome = _plan_and_dispatch(
                session, monkeypatch, provider=provider, dataset=dataset
            )
        if outcome["planned_pairs"]:
            planning_pairs += 1
    assert planning_pairs == len(MATRIX_PAIRS) == 59, (
        f"{planning_pairs}/{len(MATRIX_PAIRS)} matrix pairs produced planned "
        "units; every route-ready pair must be served, directly or through "
        "the canonical writer it folds onto"
    )


def test_dispatch_refuses_to_publish_into_a_consumerless_broker(
    monkeypatch, tmp_path
) -> None:
    """A unit planned before its pair lost plannability must terminalize, not
    be published.

    CHAOS-4054 successor to the switch-flip scenario: there is no runtime
    switch left to flip mid-flight, but the checked-in matrix is still a
    reloadable contract (production reads it once per process; this test
    repoints the seam to simulate a pair that was plannable at plan time and
    is not by dispatch time). This is the residual case the plan-time gate
    cannot cover, and it is the exact production shape: the unit exists, its
    pair is not River-routable, and the Celery queue has no consumer.
    """

    import dev_health_ops.db as db
    from dev_health_ops.workers import sync_units

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

        # The matrix now says this pair is no longer plannable; the
        # planned-and-claimed unit from the next pass has no runtime at all.
        fixture = tmp_path / "matrix.json"
        fixture.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "pairs": [
                        {
                            "provider": "launchdarkly",
                            "dataset": "feature-flags",
                            "route_ready": True,
                            "plannable": False,
                        }
                    ],
                }
            )
        )
        provider_unit_route._MATRIX_CONTRACT_PATH = fixture
        provider_unit_route.clear_matrix_cache()
        try:
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
        finally:
            provider_unit_route._MATRIX_CONTRACT_PATH = (
                provider_unit_route._DEFAULT_MATRIX_CONTRACT_PATH
            )
            provider_unit_route.clear_matrix_cache()


@pytest.mark.parametrize(("provider", "dataset"), (_GITHUB_PR_ALIAS, _GITLAB_PR_ALIAS))
def test_alias_pair_is_never_planned_regardless_of_celery_presence(
    monkeypatch, provider: str, dataset: str
) -> None:
    """CHAOS-4054 successor to the switch-off family of tests: a matrix alias
    identity is never plannable AS ITSELF, so no celery-presence state can
    make it mint a unit under its own identity. Its intent is still served --
    the unit is claimed under the canonical writer it folds onto, because
    there is no runtime that could execute the alias standalone.
    """

    with _fresh_session() as session:
        integration = _seed_integration(session, provider, (dataset,))
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
                    # Scope to the alias under test -- with no switch left to
                    # keep every other provider dataset opted out by default,
                    # an unfiltered request would also plan this provider's
                    # other, unrelated plannable pairs (missing rows default
                    # to enabled) and defeat the assertion below.
                    dataset_keys=(dataset,),
                ),
            )
            assert _planned_dataset_keys(session, plan.sync_run_id) == ["prs"], (
                f"presence={presence}: an alias must fold onto its canonical "
                "writer, never mint a unit under its own identity"
            )


def test_route_enabled_pair_is_still_planned(monkeypatch) -> None:
    """Input symmetry: a plannable pair is planned with no switch to flip."""

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


def test_capability_gate_applies_regardless_of_durable_transport(
    monkeypatch,
) -> None:
    """CHAOS-4054 successor to the old
    ``test_route_switches_are_not_consulted_off_the_river_outbox_route``.

    Pre-CHAOS-4054, a switch-off pair planned unfiltered off the River outbox
    route (switches only governed River eligibility). CHAOS-4054 deleted that
    exception outright: ``_build_planned_units`` now calls
    ``routes_to_river(...)`` unconditionally, so the capability gate applies
    the same way no matter which runtime the durable route currently hands
    units to. This is a deliberate behavior change from the switch era, not
    an oversight -- see ``.remember/chaos-4054-context.md``.
    """

    with _fresh_session(transport="celery") as session:
        plannable = _seed_integration(session, "github", ("security",))
        plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(plannable.org_id),
                integration_id=str(plannable.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )
        assert plan.total_units == 1, (
            "a plannable pair must still be planned off the River outbox route"
        )

        alias = _seed_integration(session, "github", (_GITHUB_PR_ALIAS[1],))
        alias_plan = plan_sync_run(
            session,
            SyncPlanRequest(
                org_id=str(alias.org_id),
                integration_id=str(alias.id),
                mode=SyncRunMode.INCREMENTAL.value,
                triggered_by="parity-test",
            ),
        )
        assert _planned_dataset_keys(session, alias_plan.sync_run_id) == ["prs"], (
            "an alias pair must still fold onto its canonical writer off the "
            "River outbox route"
        )


def test_route_enabled_work_item_family_canonical_claim_is_still_planned(
    monkeypatch,
) -> None:
    """Input symmetry: the canonical work-items claim is always plannable."""

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


def test_route_unknown_pair_fails_closed_not_open() -> None:
    """Input symmetry: a pair the checked-in matrix does not recognize at all
    is excluded exactly like an alias pair -- fail closed by construction
    (matrix membership), never an exception or an accidental route.
    """

    assert provider_unit_route.routes_to_river("acme-corp", "widgets") is False
