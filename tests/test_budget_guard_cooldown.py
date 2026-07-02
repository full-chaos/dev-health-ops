"""Tests for shared cooldown gating at dispatch (CHAOS-2760).

A 429 observed by one unit persists a durable observation row
(``provider_rate_limit_observations``, CHAOS-2758). Before dispatching a
run's remaining candidates, ``BudgetGuard.enforce_run`` consults that store
for an ACTIVE cooldown on the same ``(org_id, provider, integration_id,
route_family)`` -- org-scoped, deliberately excluding
``credential_fingerprint``/``host`` -- and defers (or, on rate-limit-deferral
budget exhaustion, terminally fails) matching sibling units before they burn
a worker slot re-discovering a limit BudgetGuard already knows about.

Covers:
  * a persisted cooldown defers PLANNED siblings of the same
    (provider, integration, route_family).
  * a cooldown on one route family does not defer a different family of the
    same integration.
  * credential rotation between the observation write and the next dispatch
    pass does not bypass the cooldown (the match key excludes
    credential_fingerprint entirely).
  * the ambiguous-attribution fallback: a NULL-family, dimension-tagged
    observation gates on (org_id, provider, integration_id, dimension)
    instead of guessing a family.
  * org isolation: a cooldown recorded under a different org_id never gates,
    even when (provider, integration_id, route_family) coincide.
  * fail-open: an observation-store read failure must never block dispatch.
  * an expired cooldown does not gate.
  * cooldown deferrals count against the existing per-unit
    rate_limit_deferrals/rate_limit_first_seen_at budget, and a chronically
    limited provider terminalizes instead of holding the run open.
  * exactly one query touches provider_rate_limit_observations per dispatch
    pass, regardless of candidate count.
  * BudgetGuardResult.next_deferred_at (from a cooldown deferral) re-arms
    _schedule_redispatch's outbox wakeup.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    ProviderRateLimitObservation,
    SyncDispatchOutbox,
    SyncRunMode,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.dispatch_outbox import OUTBOX_KIND_DISPATCH
from tests.test_sync_units import (
    _aware,
    _patch_db_session,
    _patch_worker_enqueues,
    _seed_run,
)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


def _sibling_unit(
    run,
    template: SyncRunUnit,
    *,
    provider: str = "github",
    dataset_key: str = "commits",
    processor_flags: dict | None = None,
    status: str = SyncRunUnitStatus.PLANNED.value,
    rate_limit_deferrals: int = 0,
    rate_limit_first_seen_at: datetime | None = None,
) -> SyncRunUnit:
    return SyncRunUnit(
        org_id=run.org_id,
        sync_run_id=run.id,
        integration_id=template.integration_id,
        source_id=template.source_id,
        provider=provider,
        dataset_key=dataset_key,
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=status,
        attempts=0,
        processor_flags=processor_flags if processor_flags is not None else {},
        rate_limit_deferrals=rate_limit_deferrals,
        rate_limit_first_seen_at=rate_limit_first_seen_at,
    )


def _observation(
    run,
    unit: SyncRunUnit,
    *,
    route_family: str | None,
    dimension: str | None,
    observed_at: datetime,
    route_family_attribution: str | None = None,
    reset_at: datetime | None = None,
    retry_after_seconds: float | None = None,
    org_id: str | None = None,
    integration_id: uuid.UUID | None = None,
    provider: str = "github",
) -> ProviderRateLimitObservation:
    return ProviderRateLimitObservation(
        org_id=org_id if org_id is not None else run.org_id,
        provider=provider,
        host="api.github.com",
        integration_id=(
            integration_id if integration_id is not None else unit.integration_id
        ),
        sync_run_id=run.id,
        sync_run_unit_id=unit.id,
        route_family=route_family,
        route_family_attribution=route_family_attribution,
        dimension=dimension,
        retry_after_seconds=retry_after_seconds,
        reset_at=reset_at,
        reason="primary",
        request_id=None,
        observed_at=observed_at,
    )


def test_ambiguous_attribution_constant_matches_observation_writer():
    """budget_guard duplicates (does not import) sync_units's ambiguous
    attribution marker to avoid a reverse import cycle -- pin them equal."""
    from dev_health_ops.sync.budget_guard import (
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION as guard_constant,
    )
    from dev_health_ops.workers.sync_units import (
        _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION as writer_constant,
    )

    assert guard_constant == writer_constant == "ambiguous_dimension"


def test_sibling_units_deferred_during_active_cooldown(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)  # provider=github, dataset_key=commits
    first.status = SyncRunUnitStatus.SUCCESS.value  # discovering unit, not a candidate
    second = _sibling_unit(
        run, first, dataset_key="commits", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    now = datetime.now(timezone.utc)
    reset_at = now + timedelta(seconds=180)
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=reset_at,
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert result["status"] == "deferred"
    assert result["queued_units"] == 0
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_deferred"
    assert second.rate_limit_deferrals == 1
    assert second.rate_limit_first_seen_at is not None
    assert second.available_at is not None
    assert abs((_aware(second.available_at) - reset_at).total_seconds()) < 0.5


def test_different_route_family_dispatches_normally(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)  # dataset_key=commits -> route_family "git"
    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(run, first, dataset_key="work-items", processor_flags={})
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    now = datetime.now(timezone.utc)
    # Cooldown on 'prs' (a different route family than second's 'work_items').
    db_session.add(
        _observation(
            run,
            first,
            route_family="prs",
            dimension="rest_core",
            reset_at=now + timedelta(seconds=300),
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert second.status == SyncRunUnitStatus.DISPATCHING.value
    assert second.result is None


def test_credential_rotation_does_not_bypass_cooldown(db_session, monkeypatch):
    import json

    from dev_health_ops.core.encryption import encrypt_value
    from dev_health_ops.models import Integration, IntegrationCredential
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "test-cooldown-credential-rotation")
    run, first = _seed_run(db_session)
    integration = db_session.query(Integration).filter_by(id=first.integration_id).one()

    def _make_credential(name: str, token: str) -> IntegrationCredential:
        credential = IntegrationCredential(
            provider="github",
            name=name,
            org_id=run.org_id,
            credentials_encrypted=encrypt_value(json.dumps({"token": token})),
            config={},
            is_active=True,
        )
        db_session.add(credential)
        db_session.flush()
        return credential

    credential_a = _make_credential("primary", "tok-A")
    integration.credential_id = credential_a.id
    db_session.flush()

    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(
        run, first, dataset_key="commits", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    now = datetime.now(timezone.utc)
    reset_at = now + timedelta(seconds=180)
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=reset_at,
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    # Rotate the credential BETWEEN the observation write and dispatch.
    credential_b = _make_credential("secondary", "tok-B")
    integration.credential_id = credential_b.id
    db_session.flush()
    assert credential_a.id != credential_b.id  # the swap was real

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_deferred"


def test_ambiguous_attribution_falls_back_to_dimension_gating(db_session, monkeypatch):
    """Linear's work-items estimator emits multiple route families (teams,
    issues, cycles, ...) all under graphql_cost -- exactly the case CHAOS-2758
    cannot confidently attribute to one family. The observation writer marks
    those rows route_family=NULL, route_family_attribution='ambiguous_dimension'.
    The gate must fall back to (org_id, provider, integration_id, dimension).
    """
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(
        db_session,
        provider="linear",
        source_type="team",
        external_id="TEAM",
        name="TEAM",
        full_name="TEAM",
        dataset_key="work-items",
        processor_flags={},
    )
    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(
        run,
        first,
        provider="linear",
        dataset_key="work-items",
        processor_flags={},
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    now = datetime.now(timezone.utc)
    reset_at = now + timedelta(seconds=120)
    db_session.add(
        _observation(
            run,
            first,
            provider="linear",
            route_family=None,
            route_family_attribution="ambiguous_dimension",
            dimension="graphql_cost",
            reset_at=reset_at,
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_deferred"
    assert second.available_at is not None
    assert abs((_aware(second.available_at) - reset_at).total_seconds()) < 0.5


def test_cooldown_never_crosses_org_boundary(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(
        run, first, dataset_key="commits", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    now = datetime.now(timezone.utc)
    # Same (provider, integration_id, route_family) as `second`'s candidate
    # key, but a DIFFERENT (foreign) org_id -- must NOT gate.
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=now + timedelta(seconds=300),
            observed_at=now - timedelta(seconds=5),
            org_id=f"org-{uuid.uuid4()}",
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert second.status == SyncRunUnitStatus.DISPATCHING.value


def test_cooldown_read_failure_fails_open(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(
        run, first, dataset_key="commits", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    now = datetime.now(timezone.utc)
    # A REAL active cooldown that would normally gate `second`.
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=now + timedelta(seconds=300),
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    real_query = db_session.query

    def _broken_query(entity, *args, **kwargs):
        if entity is ProviderRateLimitObservation:
            raise RuntimeError("simulated observation-store read failure")
        return real_query(entity, *args, **kwargs)

    monkeypatch.setattr(db_session, "query", _broken_query)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    # Fail-open: the read blew up, so the gate must act as if no cooldown
    # existed rather than blocking (or crashing) dispatch.
    assert result == {"status": "dispatched", "queued_units": 1}
    assert second.status == SyncRunUnitStatus.DISPATCHING.value


def test_expired_cooldown_dispatches_normally(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(
        run, first, dataset_key="commits", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    now = datetime.now(timezone.utc)
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=now - timedelta(seconds=30),  # already expired
            observed_at=now - timedelta(seconds=300),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert second.status == SyncRunUnitStatus.DISPATCHING.value


def test_cooldown_expiry_drains_bounded_by_concurrency_cap(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    first.status = SyncRunUnitStatus.RETRYING.value
    first.available_at = now - timedelta(seconds=5)
    second = _sibling_unit(
        run,
        first,
        dataset_key="commits",
        processor_flags={"sync_git": True},
        status=SyncRunUnitStatus.RETRYING.value,
    )
    second.available_at = now - timedelta(seconds=5)
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    # Expired cooldown -- gate must not re-defer either due-RETRYING sibling;
    # whatever capping happens must come from DispatchGuard's concurrency cap.
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=now - timedelta(seconds=1),
            observed_at=now - timedelta(seconds=120),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(first)
    db_session.refresh(second)
    assert result == {"status": "dispatched", "queued_units": 1}
    statuses = {first.status, second.status}
    assert SyncRunUnitStatus.DISPATCHING.value in statuses
    dispatched = (
        first if first.status == SyncRunUnitStatus.DISPATCHING.value else second
    )
    capped = second if dispatched is first else first
    assert dispatched.status == SyncRunUnitStatus.DISPATCHING.value
    # Concurrency-capped sibling is left untouched by BudgetGuard this pass
    # (DispatchGuard excludes it from the candidate set entirely) -- NOT
    # re-stamped by the (expired) cooldown gate.
    assert capped.status == SyncRunUnitStatus.RETRYING.value
    assert capped.result is None or (
        capped.result.get("error_category") != "rate_limit_cooldown_deferred"
    )


def test_cooldown_deferral_consumes_rate_limit_budget_and_terminalizes(
    db_session, monkeypatch
):
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.rate_limit_defer import RATE_LIMIT_MAX_DEFERRALS

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    now = datetime.now(timezone.utc)
    second = _sibling_unit(
        run,
        first,
        dataset_key="commits",
        processor_flags={"sync_git": True},
        rate_limit_deferrals=RATE_LIMIT_MAX_DEFERRALS,
        rate_limit_first_seen_at=now - timedelta(minutes=5),
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=now + timedelta(seconds=120),
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.FAILED.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_exhausted"


def test_single_observation_query_per_dispatch_pass(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    siblings = []
    for _ in range(3):
        unit = _sibling_unit(
            run, first, dataset_key="commits", processor_flags={"sync_git": True}
        )
        siblings.append(unit)
        db_session.add(unit)
    run.total_units = len(siblings) + 1
    db_session.flush()

    now = datetime.now(timezone.utc)
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=now + timedelta(seconds=180),
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    engine = db_session.get_bind()
    captured: list[str] = []

    def _record(conn, cursor, statement, parameters, context, executemany):  # noqa: ARG001
        if "provider_rate_limit_observations" in statement:
            captured.append(statement)

    event.listen(engine, "before_cursor_execute", _record)
    try:
        sync_units.dispatch_sync_run(str(run.id))
    finally:
        event.remove(engine, "before_cursor_execute", _record)

    select_statements = [
        stmt for stmt in captured if stmt.strip().upper().startswith("SELECT")
    ]
    assert len(select_statements) == 1, (
        "expected exactly one cooldown-observation SELECT per dispatch pass "
        f"regardless of candidate count, got {len(select_statements)}: "
        f"{select_statements}"
    )

    for sibling in siblings:
        db_session.refresh(sibling)
        assert sibling.status == SyncRunUnitStatus.RETRYING.value
        assert sibling.result is not None
        assert sibling.result["error_category"] == "rate_limit_cooldown_deferred"


def test_next_deferred_at_rearms_redispatch(db_session, monkeypatch):
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(
        run, first, dataset_key="commits", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    now = datetime.now(timezone.utc)
    reset_at = now + timedelta(seconds=200)
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=reset_at,
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    sync_units.dispatch_sync_run(str(run.id))

    outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )
    assert abs((_aware(outbox.available_at) - reset_at).total_seconds()) < 0.5
