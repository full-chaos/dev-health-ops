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
  * rate_limit_deferrals/rate_limit_first_seen_at are cleared at episode
    boundaries (SUCCESS, and any non-rate-limit RETRYING stamp) so stale
    bookkeeping from an earlier, resolved rate-limit episode can never be
    misread as an ongoing one by the wall-clock-exhaustion check; a defense-
    in-depth error_category gate protects against a missed clear site too
    (review finding).
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
    result: dict | None = None,
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
        result=result,
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
    # reconfirm_cooldowns fully defers the match with the SAME write path
    # enforce_run's own cooldown loop uses (review finding, round 2) -- not
    # a bare PLANNED exclusion, which would livelock the run on a bare ~60s
    # redispatch countdown without ever counting against the shared
    # rate-limit-deferral budget.
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_deferred"
    assert second.rate_limit_deferrals == 1
    assert second.available_at is not None
    assert abs((_aware(second.available_at) - reset_at).total_seconds()) < 10


def test_cooldown_available_at_respects_wall_clock_clamp(db_session, monkeypatch):
    """HIGH finding: _apply_cooldown_deferral must not stamp
    available_at=cooldown_expiry+jitter when cooldown_expiry is beyond the
    remaining RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS wall-clock budget --
    plan_rate_limit_deferral's own not_before clamp must be authoritative,
    or a far-future reset_at parks the unit for hours past the point the
    policy promises terminalization.

    Deliberately uses a NONZERO jitter (review finding, round 2: the
    original version of this test forced jitter=0, which happened to mask
    the follow-on bug where jitter is added AFTER the clamp and can itself
    push available_at past the wall-clock deadline). With not_before
    already sitting at the clamp boundary, jitter added on top must be
    clamped back down, not allowed to overshoot.
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

    jitter_seconds = 120
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", str(jitter_seconds))

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.available_at is not None
    deadline = now + timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS)
    # Never past the wall-clock deadline, even with jitter added on top of
    # an already-clamped not_before -- a small tolerance only for the clock
    # drift between this test's `now` and dispatch_sync_run's own `now`.
    assert _aware(second.available_at) <= deadline + timedelta(seconds=1)
    # And not clamped away to something implausibly early either.
    assert _aware(second.available_at) >= deadline - timedelta(
        seconds=jitter_seconds + 5
    )
    assert (reset_at - _aware(second.available_at)).total_seconds() > 3600

    # next_deferred_at (the redispatch re-arm) inherits the same clamp.
    outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )
    assert _aware(outbox.available_at) <= deadline + timedelta(seconds=1)


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


# ---------------------------------------------------------------------------
# Codex adversarial review round 2 findings
# ---------------------------------------------------------------------------


def test_late_reconfirm_match_short_reset_window_defers_with_full_bookkeeping(
    db_session, monkeypatch
):
    """HIGH finding, round 2: a unit caught ONLY by the late reconfirm pass
    (not enforce_run's own snapshot) with a SHORT, well-within-budget reset
    window must get the SAME full deferral bookkeeping a same-pass match
    would -- available_at, rate_limit_deferrals, error_category, and the
    next_deferred_at re-arm -- not a bare PLANNED exclusion.
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

    reset_at = datetime.now(timezone.utc) + timedelta(seconds=90)
    real_reconfirm = BudgetGuard.reconfirm_cooldowns

    def _reconfirm_after_concurrent_commit(*args, **kwargs):
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
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_deferred"
    assert second.rate_limit_deferrals == 1
    assert second.rate_limit_first_seen_at is not None
    assert second.available_at is not None
    assert abs((_aware(second.available_at) - reset_at).total_seconds()) < 5

    outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )
    assert abs((_aware(outbox.available_at) - reset_at).total_seconds()) < 5


def test_late_reconfirm_match_long_reset_window_clamps_to_wall_clock_deadline(
    db_session, monkeypatch
):
    """HIGH finding, round 2: a unit caught only by the late reconfirm pass
    with a LONG reset window (well beyond the wall-clock deferral budget)
    still gets the SAME clamp-to-deadline treatment a same-pass match
    would: available_at lands at the deadline, not the raw far-future
    reset_at, and the unit is DEFERRED (not yet exhausted) with full
    bookkeeping -- proving the late path reuses the exact same
    _apply_cooldown_deferral clamp logic, not a second, weaker one.
    """
    from dev_health_ops.sync.budget_guard import BudgetGuard
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
    reset_at = now + timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS * 5)
    real_reconfirm = BudgetGuard.reconfirm_cooldowns

    def _reconfirm_after_concurrent_commit(*args, **kwargs):
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
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_deferred"
    assert second.rate_limit_deferrals == 1
    deadline = now + timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS)
    assert second.available_at is not None
    assert abs((_aware(second.available_at) - deadline).total_seconds()) < 5
    assert (reset_at - _aware(second.available_at)).total_seconds() > 3600

    outbox = (
        db_session.query(SyncDispatchOutbox)
        .filter_by(sync_run_id=run.id, kind=OUTBOX_KIND_DISPATCH)
        .one()
    )
    assert abs((_aware(outbox.available_at) - deadline).total_seconds()) < 5


def test_reconfirm_cooldowns_terminalizes_exhausted_match_directly(
    db_session, monkeypatch
):
    """HIGH finding, round 2: reconfirm_cooldowns' own cooldown-match branch
    must terminalize (not just exclude) a unit whose shared rate-limit-
    deferral budget is already spent -- not a bare PLANNED exclusion that
    would livelock the run redispatching every ~60s forever without ever
    counting against the budget.

    Calls BudgetGuard.reconfirm_cooldowns directly rather than going through
    dispatch_sync_run: an already-exhausted unit is ALSO caught by
    enforce_run's own pass (the finding-2a wall-clock-exhaustion check runs
    unconditionally, independent of any observation), so routing this
    through the full dispatch flow would only prove enforce_run's check
    fired first, not that reconfirm_cooldowns' OWN termination branch works.
    This isolates reconfirm_cooldowns' write path specifically.
    """
    from dev_health_ops.sync.budget import estimate_provider_budget
    from dev_health_ops.sync.budget_guard import BudgetGuard
    from dev_health_ops.workers.rate_limit_defer import RATE_LIMIT_MAX_DEFERRALS
    from dev_health_ops.workers.sync_bootstrap import SyncTaskBootstrap

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    second = _sibling_unit(
        run,
        first,
        dataset_key="commits",
        processor_flags={"sync_git": True},
        rate_limit_deferrals=RATE_LIMIT_MAX_DEFERRALS,
        rate_limit_first_seen_at=datetime.now(timezone.utc) - timedelta(minutes=5),
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
            reset_at=now + timedelta(seconds=120),
            observed_at=now - timedelta(seconds=1),
        )
    )
    db_session.flush()

    ctx = SyncTaskBootstrap.load(db_session, str(second.id))
    estimates = estimate_provider_budget(ctx)

    result = BudgetGuard.reconfirm_cooldowns(
        db_session,
        str(run.id),
        units=[second],
        estimates_by_unit={str(second.id): estimates},
        already_excluded_ids=frozenset(),
        jitter_seconds=0,
        now=now,
    )

    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.FAILED.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_exhausted"
    assert str(second.id) in result.excluded_unit_ids
    assert result.next_deferred_at is None


def test_cooldown_observation_aged_past_lookback_terminalizes_from_unit_state(
    db_session, monkeypatch
):
    """MEDIUM finding, round 2, part (a): termination must not depend on
    re-reading the observation. Even when the causing observation is FAR
    older than any plausible lookback window (so _active_cooldowns
    genuinely cannot see it), a due unit whose own
    rate_limit_deferrals/rate_limit_first_seen_at already show the shared
    wall-clock deferral budget spent terminalizes from its own persisted
    state -- it must not just quietly dispatch because the causing row
    happened to fall out of the lookback window.
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
        status=SyncRunUnitStatus.RETRYING.value,
        rate_limit_deferrals=1,
        rate_limit_first_seen_at=now
        - timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS + 30),
        # A genuine, still-ongoing rate-limit episode: the unit's own last
        # recorded cause is the cooldown gate's deferral category -- this is
        # what the round-3 defense-in-depth error_category gate on
        # _rate_limit_deferral_exhausted requires to even consider
        # terminalizing (a unit whose last cause was unrelated, e.g.
        # budget_deferred, must NOT be terminalized off stale columns; see
        # test_stale_rate_limit_state_does_not_terminalize_unrelated_retry).
        result={
            "error_category": "rate_limit_cooldown_deferred",
            "not_before": (now - timedelta(seconds=1)).isoformat(),
            "rate_limit_deferrals": 1,
        },
    )
    second.available_at = now - timedelta(seconds=1)  # due
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    # The causing observation is a full day old -- genuinely invisible to
    # _active_cooldowns under ANY reasonable lookback window.
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=now + timedelta(hours=5),
            observed_at=now - timedelta(days=1),
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


def test_cooldown_lookback_window_has_slack_beyond_wall_clock_budget(
    db_session, monkeypatch
):
    """MEDIUM finding, round 2, part (b): the observation lookback window
    must NOT equal RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS exactly. An observation
    whose age is JUST past the OLD (bare-wall-clock-budget) boundary must
    still be visible under the widened default, deferring a FRESH sibling
    normally -- not letting it silently dispatch just because the row
    happened to be a couple of minutes past that old cliff edge.
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
    # 90s past the OLD (bare wall-clock-budget) lookback boundary -- must
    # still fall within the widened default (budget + jitter_max + a
    # generous skew margin, comfortably more than 90s of slack).
    db_session.add(
        _observation(
            run,
            first,
            route_family="git",
            dimension="rest_core",
            reset_at=now + timedelta(minutes=5),
            observed_at=now - timedelta(seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS + 90),
        )
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    # Deferred normally (still visible, and a FRESH unit so not exhausted)
    # -- NOT dispatched, NOT terminalized.
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_deferred"


# ---------------------------------------------------------------------------
# Codex adversarial review round 3 finding: stale rate-limit state lifecycle
# ---------------------------------------------------------------------------


def test_stale_rate_limit_state_cleared_by_non_rate_limit_retry_then_claimed(
    db_session, monkeypatch
):
    """HIGH finding, round 3, regression (i): a unit carrying STALE
    rate_limit_deferrals/rate_limit_first_seen_at from an earlier, resolved
    rate-limit episode must not be wrongly terminalized just because it
    later goes through an UNRELATED retry (here: budget deferral, not a
    rate limit). The non-rate-limit deferral clears the stale columns (root
    fix); the unit is claimed normally on its next due pass instead of
    being terminalized off ancient data.
    """
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.rate_limit_defer import (
        RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS,
    )

    run, unit = _seed_run(db_session)  # provider=github, dataset_key=commits
    now = datetime.now(timezone.utc)
    # Stale rate-limit history: well past the wall-clock budget, from a
    # rate-limit episode that has nothing to do with what happens next.
    unit.rate_limit_deferrals = 1
    unit.rate_limit_first_seen_at = now - timedelta(
        seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS + 3600
    )
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    # Force a budget deferral (unrelated to rate limits) on this pass.
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 0}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert result["status"] == "deferred"
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.result is not None
    assert unit.result["error_category"] == "budget_deferred"
    # Root fix: cleared by the non-rate-limit (budget) deferral.
    assert unit.rate_limit_deferrals == 0
    assert unit.rate_limit_first_seen_at is None

    # Let the deferral elapse and redispatch -- with clean columns, the unit
    # is claimed normally instead of being wrongly terminalized off the
    # stale, unrelated old data.
    unit.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    monkeypatch.delenv("SYNC_BUDGET_BUCKET_LIMITS", raising=False)
    db_session.flush()

    result2 = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert result2 == {"status": "dispatched", "queued_units": 1}
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value


def test_stale_rate_limit_columns_without_rate_limit_error_category_do_not_terminalize(  # noqa: E501
    db_session, monkeypatch
):
    """HIGH finding, round 3, defense in depth: even if a unit somehow still
    carries stale, budget-exhausted-looking rate_limit_deferrals/
    rate_limit_first_seen_at (simulating a missed clear site),
    _rate_limit_deferral_exhausted refuses to fire unless the unit's own
    last-recorded result.error_category is rate-limit-related. A stale row
    whose last real cause was unrelated (here: worker_lost, as a reconciler
    expired-lease retry would stamp) must dispatch normally, not
    terminalize.
    """
    from dev_health_ops.workers import sync_units
    from dev_health_ops.workers.rate_limit_defer import (
        RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS,
    )

    run, unit = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    unit.status = SyncRunUnitStatus.RETRYING.value
    unit.available_at = now - timedelta(seconds=1)
    unit.rate_limit_deferrals = 1
    unit.rate_limit_first_seen_at = now - timedelta(
        seconds=RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS + 60
    )
    unit.result = {"error_category": "worker_lost", "retry_reason": "expired_lease"}
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value


def test_rate_limit_state_cleared_on_success_starts_fresh_episode_later(
    db_session, monkeypatch
):
    """HIGH finding, round 3, regression (ii): a unit that resolves a
    rate-limit episode by SUCCEEDING has its rate_limit_deferrals/
    rate_limit_first_seen_at cleared. A LATER, unrelated rate-limit episode
    (simulated well past the OLD episode's 2h wall-clock budget) computes
    its OWN fresh clock starting from the new first_seen_at -- it is not
    immediately exhausted against the stale old timestamp, which is exactly
    what would happen if the clear had not fired.
    """
    from dev_health_ops.exceptions import RateLimitException
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.rate_limit_defer import plan_rate_limit_deferral
    from dev_health_ops.workers.sync_units import run_sync_unit
    from tests.test_sync_units import (
        _mark_dispatching,
        _patch_finalize_apply,
        _patch_runtime,
    )

    run, unit = _seed_run(db_session)  # provider=github, dataset_key=commits
    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)

    def rate_limited(ctx, runtime):
        raise RateLimitException("rate limited", retry_after_seconds=1.0)

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", rate_limited)
    result = getattr(run_sync_unit, "run")(str(unit.id))
    assert result["status"] == "rate_limited_deferred"

    db_session.refresh(unit)
    assert unit.rate_limit_deferrals == 1
    assert unit.rate_limit_first_seen_at is not None
    old_first_seen = _aware(unit.rate_limit_first_seen_at)

    # Redispatch -- this time the provider is healthy.
    _mark_dispatching(db_session, unit)

    def succeeds(ctx, runtime):
        return {"ok": True}

    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", succeeds)
    result = getattr(run_sync_unit, "run")(str(unit.id))
    assert result["status"] == "success"

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    # Root fix: cleared on SUCCESS.
    assert unit.rate_limit_deferrals == 0
    assert unit.rate_limit_first_seen_at is None

    # A LATER, unrelated rate-limit episode -- well past the OLD episode's
    # wall-clock budget -- must start its OWN fresh clock, not be treated
    # as a continuation of (and therefore immediately exhausted against)
    # the stale old first_seen_at.
    much_later = old_first_seen + timedelta(hours=3)
    deferral = plan_rate_limit_deferral(
        retry_after_seconds=30.0,
        attempts=unit.rate_limit_deferrals,
        first_seen_at=unit.rate_limit_first_seen_at.isoformat()
        if unit.rate_limit_first_seen_at
        else None,
        now=much_later,
    )
    assert deferral is not None
    fresh_first_seen = datetime.fromisoformat(deferral.first_seen_at)
    assert abs((fresh_first_seen - much_later).total_seconds()) < 1
