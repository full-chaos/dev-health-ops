"""Planner/dispatcher routable-set parity (CHAOS-3941, updated for CHAOS-4054).

The planner decides which units to CREATE from ``IntegrationDataset.is_enabled``
(per-org Postgres rows) AND the checked-in capability matrix
(``contracts/provider-matrix/v1/matrix.json``): a unit is only ever minted for
a pair the matrix marks BOTH ``route_ready`` and ``plannable``. Since
CHAOS-4054 step 4 the dispatcher asks that SAME question and nothing else --
River is the only runtime that executes a provider sync unit, so a claimed
unit is either staged in the ``sync.provider_unit`` outbox or terminalized.

Both planes of the old two-dimensional model are gone. CHAOS-4054 deleted the
``WORKER_*_ENABLED`` route-switch environment plane (capability is always on
in the binary), and step 4 then deleted the Celery dispatch plane: no consumer
probe, no fallback queue, no durable ``sync.provider_unit`` transport read.
What remains is one pure capability-matrix fact,
``routes_to_river(provider, dataset)``, consulted unconditionally by both
sides. These tests assert the EFFECT, not the call:

  * ``test_planner_and_dispatcher_agree_on_routable_set`` sweeps every
    ``route_ready`` pair in the real matrix and asserts the planner creates
    exactly the units the dispatcher can hand to River -- with no unit left
    non-terminal that no runtime will execute.
  * ``test_dispatch_terminalizes_a_unit_whose_pair_lost_plannability`` asserts
    the dispatch-time refusal on a unit that was planned before its pair lost
    plannability (the CHAOS-4054 analog of "planned before a switch flip").
  * A capability-matrix alias identity is never planned AS ITS OWN
    dataset_key, but (CHAOS-4078) an alias-only selection now folds onto its
    canonical writer instead of silently planning nothing -- the exact
    failure shape CHAOS-4125 found in production (pr-reviews/pr-comments/
    tests planned zero units for 36+ hours). ``canonical_identity`` is the
    one authority both the planner and this test's expectations read.
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
from dev_health_ops.workers import provider_unit_route

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

#: The subset of MATRIX_PAIRS the planner will actually mint a unit for.
PLANNABLE_PAIRS: frozenset[tuple[str, str]] = frozenset(
    (pair["provider"], pair["dataset"])
    for pair in _MATRIX["pairs"]
    if pair["route_ready"] and pair["plannable"]
)

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


def _silence_run_scheduling(monkeypatch) -> None:
    """Keep redispatch/finalize scheduling off a real broker.

    Before CHAOS-4054 step 4 this helper also captured per-unit Celery
    publishes by faking ``run_sync_unit.s(...).set(queue=...).apply_async()``.
    That branch no longer exists in ``dispatch_sync_run``: an admitted unit is
    staged in the durable outbox and an unroutable one is terminalized, so
    "did anything reach a Celery queue" is now answered by the outbox/terminal
    accounting below rather than by a signature spy.
    """

    from dev_health_ops.workers import sync_units

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
            "stranded_units": [],
        }

    _mark_discovery_succeeded(session, plan.sync_run_id)
    _silence_run_scheduling(monkeypatch)
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
        "stranded_units": stranded,
    }


@pytest.mark.parametrize(
    ("provider", "dataset"),
    MATRIX_PAIRS,
    ids=[f"{provider}-{dataset}" for provider, dataset in MATRIX_PAIRS],
)
def test_planner_and_dispatcher_agree_on_routable_set(
    monkeypatch, provider: str, dataset: str
) -> None:
    """No unit is created that no runtime will execute.

    River is the only runtime, so the executable set is exactly the
    River-routable set. The planner must produce that set and nothing else.

    This sweep used to run twice per pair, once with the durable
    ``sync.provider_unit`` route on River and once on Celery, to prove the
    plan-time capability gate did not vary with the route. CHAOS-4054 step 4
    deleted that dimension from the dispatcher too -- neither side reads the
    durable route any more -- so the sweep is one case per pair. The residual
    "the durable route row does not move the gate" claim is still pinned once,
    directly, by ``test_capability_gate_applies_regardless_of_durable_transport``.
    """

    with _fresh_session() as session:
        outcome = _plan_and_dispatch(
            session, monkeypatch, provider=provider, dataset=dataset
        )

    assert outcome["stranded_units"] == [], (
        f"{provider}/{dataset}: units left non-terminal that no runtime will execute"
    )
    assert outcome["planned_pairs"] == sorted(
        outcome["river_pairs"] + outcome["terminal_pairs"]
    ), (
        f"{provider}/{dataset}: planner planned "
        f"{outcome['planned_pairs']}, but only {outcome['river_pairs']} reached a "
        f"live runtime and only {outcome['terminal_pairs']} were recorded as "
        "denied -- the remainder went nowhere and said nothing"
    )
    # The plan-time capability gate is unconditional: every route_ready pair
    # is planned under its canonical identity. For a plannable pair that IS
    # its own canonical identity. For an alias (work-item-family,
    # PR-social, or TestOps -- CHAOS-4078 folds the latter two the same way
    # CHAOS-2721 already folded the former), ``canonical_identity`` names the
    # writer the fold collapses onto: work-item-family aliases get there via
    # ``_dataset_keys_for`` enabling the whole family, PR-social/TestOps
    # aliases get there directly (single-alias selection folds on its own,
    # no sibling required). One assertion covers both shapes because
    # ``canonical_identity`` is the SAME authority the planner folds onto.
    expected_canonical = provider_unit_route.canonical_identity(provider, dataset)
    assert expected_canonical is not None, (
        f"{provider}/{dataset} is route_ready but canonical_identity found no "
        "plannable writer for it"
    )
    expected_pair = (provider, expected_canonical)
    assert outcome["planned_pairs"], f"{provider}/{dataset} must be planned"
    assert set(outcome["planned_pairs"]) == {expected_pair}


def test_matrix_sweep_is_not_vacuous(monkeypatch) -> None:
    """Guard the sweep itself: every route_ready pair must really produce a
    planned unit, one way or another.

    CHAOS-4054: capability is always on in the binary, so every plannable
    pair plans a unit unconditionally -- there is no switch left to flip.
    CHAOS-4078 closed the remaining gap: PR-social (prs/pr-reviews/
    pr-comments) and TestOps (cicd/tests) now fold onto their canonical
    writer exactly like the work-item family already did, so EVERY
    route_ready pair -- plannable or alias -- produces a planned unit. A
    change that made ``plan_sync_run`` return zero units for any pair (or
    silently dropped an alias again, the CHAOS-4125 regression shape) would
    turn this sweep red.
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
        "units; expected every route_ready pair to plan (CHAOS-4078: no more "
        "silent alias-only gaps)"
    )


def test_dispatch_terminalizes_a_unit_whose_pair_lost_plannability(
    monkeypatch, tmp_path
) -> None:
    """A unit planned before its pair lost plannability must terminalize, not
    be published.

    CHAOS-4054 successor to the switch-flip scenario: there is no runtime
    switch left to flip mid-flight, but the checked-in matrix is still a
    reloadable contract (production reads it once per process; this test
    repoints the seam to simulate a pair that was plannable at plan time and
    is not by dispatch time). This is the residual case the plan-time gate
    cannot cover, and it is the exact production shape: the unit exists and
    its pair is not River-routable, so there is nowhere left to send it.
    """

    import dev_health_ops.db as db
    from dev_health_ops.workers import sync_units

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

            _silence_run_scheduling(monkeypatch)
            monkeypatch.setattr(
                db, "get_postgres_session_sync", lambda: _session_ctx(session)
            )
            session.commit()

            sync_units.dispatch_sync_run(run_id)

            session.expire_all()
            unit = session.query(SyncRunUnit).one()
            # Refused, not staged: nothing may claim a unit no runtime owns.
            assert session.query(WorkerJobOutbox).count() == 0, (
                "an unroutable unit was staged in the provider-unit outbox"
            )
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
def test_alias_pair_folds_onto_its_canonical_writer(
    provider: str, dataset: str
) -> None:
    """CHAOS-4078 successor to the old "an alias pair is never planned" test:
    a matrix alias identity is never plannable AS ITS OWN dataset_key, but an
    alias-only selection now folds onto its canonical writer instead of
    silently planning nothing (the exact CHAOS-4125 zero-success shape).

    This used to run the same assertion three times, once per
    ``CeleryConsumerPresence``. Step 4 deleted the consumer probe, so that
    dimension has exactly one value and the loop is inlined.
    """

    with _fresh_session() as session:
        integration = _seed_integration(session, provider, (dataset,))
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
        assert plan.total_units == 1, (
            "an alias-only selection must fold onto its canonical writer"
        )
        unit = (
            session.query(SyncRunUnit)
            .filter(SyncRunUnit.sync_run_id == uuid.UUID(plan.sync_run_id))
            .one()
        )
        expected_canonical = provider_unit_route.canonical_identity(provider, dataset)
        assert str(unit.dataset_key) == expected_canonical
        assert unit.dataset_key != dataset, (
            "the alias's own identity must never be the persisted dataset_key"
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
    the same way no matter what the durable ``sync.provider_unit`` route row
    says. This is a deliberate behavior change from the switch era, not an
    oversight -- see ``.remember/chaos-4054-context.md``.

    The row is still seeded by migration and still readable, but since step 4
    nothing on the dispatch path consults it. This is therefore a standing
    guard rather than a live branch: it is the one place that would turn red
    if route-conditional planning were reintroduced.
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
        # CHAOS-4078: the alias folds onto its canonical writer regardless of
        # durable transport, same as every other capability-gate decision --
        # it is still never planned AS ITS OWN identity, which is the
        # invariant this test exists to pin.
        assert alias_plan.total_units == 1, (
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
