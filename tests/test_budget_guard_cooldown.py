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
  * TOCTOU closure (review finding): a sibling's 429 committing a brand-new
    observation between enforce_run's snapshot and the atomic claim is still
    caught by the late reconfirm_cooldowns check, immediately before
    _claim_units.
  * available_at respects plan_rate_limit_deferral's wall-clock clamp
    (RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS), not the raw cooldown expiry, and a
    unit whose wall-clock budget is already spent terminalizes rather than
    sleeping past the clamp (review finding).
  * a malformed observation row (non-finite retry_after_seconds) is skipped,
    not fatal to the whole dispatch pass (review finding).
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


# ---------------------------------------------------------------------------
# Codex adversarial review round 1 findings
# ---------------------------------------------------------------------------


def test_concurrent_observation_between_enforce_run_and_claim_still_defers_sibling(
    db_session, monkeypatch
):
    """HIGH finding: BudgetGuard.enforce_run reads
    provider_rate_limit_observations once, early in its pass, then does more
    real DB work of its own (budget admission / active-consumption
    re-estimation) before returning. Under READ COMMITTED, a sibling unit's
    429 can commit a brand-new observation in exactly that window -- one
    enforce_run's snapshot never saw -- and without a second look,
    _claim_units would dispatch straight into it.

    Simulates that race deterministically: NO observation exists when
    enforce_run runs (so its own snapshot is clean), then a fresh row is
    inserted+flushed the instant BudgetGuard.reconfirm_cooldowns is invoked
    (the seam dispatch_sync_run calls immediately before _claim_units) --
    mirroring a concurrent transaction's commit landing in that gap. The
    fresh row must still be caught before the atomic claim.
    """
    from dev_health_ops.sync.budget_guard import BudgetGuard
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(
        run, first, dataset_key="commits", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()
    # Deliberately no observation seeded yet -- enforce_run's own read must
    # see nothing active, proving the race window is real (not just "the
    # gate never ran").

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    reset_at = datetime.now(timezone.utc) + timedelta(seconds=180)
    real_reconfirm = BudgetGuard.reconfirm_cooldowns

    def _reconfirm_after_concurrent_commit(*args, **kwargs):
        # The "concurrent commit" -- lands strictly AFTER enforce_run's own
        # cooldown snapshot, strictly BEFORE the late re-check.
        db_session.add(
            _observation(
                run,
                first,
                route_family="git",
                dimension="rest_core",
                reset_at=reset_at,
                observed_at=datetime.now(timezone.utc) - timedelta(seconds=1),
            )
        )
        db_session.flush()
        return real_reconfirm(*args, **kwargs)

    monkeypatch.setattr(
        BudgetGuard,
        "reconfirm_cooldowns",
        staticmethod(_reconfirm_after_concurrent_commit),
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert result["queued_units"] == 0
    # Left exactly as enforce_run's own pass found it -- reconfirm_cooldowns
    # only EXCLUDES from this pass's claim, it does not stamp RETRYING
    # itself (the next enforce_run pass formally defers it with full
    # bookkeeping).
    assert second.status == SyncRunUnitStatus.PLANNED.value


def test_cooldown_available_at_respects_wall_clock_clamp(db_session, monkeypatch):
    """HIGH finding: _apply_cooldown_deferral must not stamp
    available_at=cooldown_expiry+jitter when cooldown_expiry is beyond the
    remaining RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS wall-clock budget --
    plan_rate_limit_deferral's own not_before clamp must be authoritative,
    or a far-future reset_at parks the unit for hours past the point the
    policy promises terminalization.
    """
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.rate_limit_defer import (
        RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS,
    )

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(
        run, first, dataset_key="commits", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    now = datetime.now(timezone.utc)
    # Reset_at is 5x the wall-clock budget out -- must NOT be honored as-is.
    reset_at = now + timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS * 5)
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

    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.available_at is not None
    clamp_boundary = now + timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS)
    # Lands at the wall-clock clamp boundary, nowhere near the raw reset_at.
    assert abs((_aware(second.available_at) - clamp_boundary).total_seconds()) < 2
    assert (reset_at - _aware(second.available_at)).total_seconds() > 3600


def test_cooldown_wall_clock_budget_exhausted_terminalizes_rather_than_sleeping_past_clamp(  # noqa: E501
    db_session, monkeypatch
):
    """HIGH finding, second half: once the wall-clock deferral budget is
    already spent (simulating "the following pass" via a pre-seeded
    first_seen_at), a unit gated by a still-active cooldown must terminalize
    -- not get re-deferred past the clamp plan_rate_limit_deferral already
    said was the limit.
    """
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.rate_limit_defer import (
        RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS,
    )

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    now = datetime.now(timezone.utc)
    second = _sibling_unit(
        run,
        first,
        dataset_key="commits",
        processor_flags={"sync_git": True},
        rate_limit_deferrals=1,
        rate_limit_first_seen_at=now
        - timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS + 60),
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    # Still an active (future) cooldown per the observation itself --
    # exhaustion must come from the wall-clock budget, not an expired row.
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=now + timedelta(hours=5),
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


def test_cooldown_read_survives_malformed_observation_row(
    db_session, monkeypatch, caplog
):
    """MEDIUM finding: a malformed row (non-finite retry_after_seconds, no
    usable reset_at) must not abort the whole cooldown read and block
    dispatch org-wide -- it is skipped, logged, and treated as no signal.
    """
    import logging

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
    # No reset_at, an infinite retry_after_seconds -- timedelta(seconds=inf)
    # raises OverflowError if unguarded.
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=None,
            retry_after_seconds=float("inf"),
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    with caplog.at_level(logging.WARNING, logger="dev_health_ops.sync.budget_guard"):
        result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert second.status == SyncRunUnitStatus.DISPATCHING.value
    assert any(
        record.getMessage() == "dispatch_sync_run.cooldown_observation_row_malformed"
        for record in caplog.records
    )
