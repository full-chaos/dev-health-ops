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
from dev_health_ops.sync.budget_types import (
    BudgetBucketKey,
    BudgetDimension,
    BudgetEstimate,
)
from dev_health_ops.sync.dispatch_outbox import OUTBOX_KIND_DISPATCH
from tests._helpers import seed_sync_dispatch_transport_routes
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
        seed_sync_dispatch_transport_routes(session)
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
        # Producible state (CHAOS-3412 closure): every production stamp that
        # sets a non-zero rate_limit_deferrals co-writes a rate-limit
        # error_category, so counters without one is a state the producer
        # cannot emit. The guard now treats unevidenced counters as a fresh
        # episode, so a fixture that omitted the cause was asserting against
        # an impossible row.
        result={"error_category": "rate_limit_cooldown_deferred"},
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
        # Producible state (CHAOS-3412 closure): every production stamp that
        # sets a non-zero rate_limit_deferrals co-writes a rate-limit
        # error_category, so counters without one is a state the producer
        # cannot emit. The guard now treats unevidenced counters as a fresh
        # episode, so a fixture that omitted the cause was asserting against
        # an impossible row.
        result={"error_category": "rate_limit_cooldown_deferred"},
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
        # Producible state (CHAOS-3412 closure): every production stamp that
        # sets a non-zero rate_limit_deferrals co-writes a rate-limit
        # error_category, so counters without one is a state the producer
        # cannot emit. The guard now treats unevidenced counters as a fresh
        # episode, so a fixture that omitted the cause was asserting against
        # an impossible row.
        result={"error_category": "rate_limit_cooldown_deferred"},
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


# ---------------------------------------------------------------------------
# CHAOS-3412: budget-deferral episode exhaustion
# ---------------------------------------------------------------------------


def _lap_budget_deferrals(db_session, run, unit, *, laps: int) -> list[dict]:
    """Run ``dispatch_sync_run`` ``laps`` times, making the unit due again
    between laps so every lap re-evaluates the SAME budget-deferred unit --
    the loop an operator actually observes (``updated_at`` advancing while
    the unit never leaves ``retrying``)."""
    from dev_health_ops.workers import sync_units

    results = []
    for _ in range(laps):
        results.append(sync_units.dispatch_sync_run(str(run.id)))
        db_session.refresh(unit)
        if unit.status != SyncRunUnitStatus.RETRYING.value:
            break
        unit.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        db_session.flush()
    return results


def test_budget_deferral_exhausts_instead_of_looping_forever(db_session, monkeypatch):
    """NEGATIVE CONTROL (CHAOS-3412), kept as the regression test.

    Before the fix this test's loop ran forever: ``_defer_unit_for_budget``
    re-stamped ``retrying`` with a fresh ``available_at`` every lap while
    ``attempts`` stayed pinned at 0 and no exhaustion predicate existed for
    the budget episode, so the unit never failed, never ran, and never
    surfaced an error to the operator. Run against unmodified code, the
    final assertions below fail with ``'retrying' != 'failed'`` -- that is
    the defect. After the fix the count cap (``SYNC_BUDGET_MAX_DEFERRALS``)
    terminalizes the unit with an actionable error.
    """
    from dev_health_ops.sync.budget_guard import BUDGET_MAX_DEFERRALS_DEFAULT

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    # An estimate this unit can never fit: the guard is doing its job, the
    # unit is permanently oversized for its bucket (the CHAOS-3412 shape).
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 0}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    results = _lap_budget_deferrals(
        db_session, run, unit, laps=BUDGET_MAX_DEFERRALS_DEFAULT + 1
    )

    # Every lap up to the cap deferred; none of them dispatched the unit.
    assert all(result["status"] == "deferred" for result in results[:-1])
    assert all(result["queued_units"] == 0 for result in results)

    db_session.refresh(unit)
    # The loop now has an exit: the unit fails LOUDLY instead of sitting in
    # retrying forever.
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result is not None
    assert unit.result["error_category"] == "budget_deferral_exhausted"
    assert unit.budget_deferrals == BUDGET_MAX_DEFERRALS_DEFAULT
    assert unit.budget_first_deferred_at is not None
    # It never ran: the exhaustion is a configuration error surfaced before
    # a worker slot is burned, so attempts stays 0 (the ticket's observation).
    assert unit.attempts == 0
    # The error text has to be actionable on its own -- it names the bucket,
    # the estimate, the cap it could never fit, the window span that drove
    # the estimate, and the two operator remedies.
    assert unit.error is not None
    assert "github:" in unit.error and "rest_core" in unit.error
    assert "backfill" in unit.error.lower()
    assert "SYNC_BUDGET_BUCKET_LIMITS" in unit.error


def test_budget_deferral_exhaustion_finalizes_the_run_as_failed(
    db_session, monkeypatch
):
    """Reached-state assertion, not just the unit's own row: terminalizing
    the last blocked unit has to move the RUN to a terminal, failed state
    too. A unit that fails while its run stays open forever would swap one
    invisible-nothing-happening for another.
    """
    from dev_health_ops.models import SyncRunStatus
    from dev_health_ops.sync.budget_guard import BUDGET_MAX_DEFERRALS_DEFAULT

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 0}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    _lap_budget_deferrals(db_session, run, unit, laps=BUDGET_MAX_DEFERRALS_DEFAULT + 1)

    db_session.refresh(run)
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert run.status in {
        SyncRunStatus.FAILED.value,
        SyncRunStatus.PARTIAL_FAILED.value,
    }


def test_budget_wall_clock_cap_exhausts_below_the_count_cap(db_session, monkeypatch):
    """The second cap: a unit stuck in ONE continuous budget episode for
    longer than SYNC_BUDGET_DEFERRAL_WALL_CLOCK_SECONDS terminalizes even
    though its deferral COUNT is nowhere near the cap (a long deferral
    interval would otherwise let it loop for weeks).

    The bucket limit is set deliberately (review round 2, F1): exhaustion is
    only ever decided for a unit that STILL does not fit on this pass, so a
    test that left the bucket wide open would assert the unit is killed while
    it is actually admissible -- which is the defect F1 fixed, not the cap
    this test is about.
    """
    from dev_health_ops.providers.github import budget as github_budget
    from dev_health_ops.sync.budget_guard import (
        BUDGET_DEFERRAL_WALL_CLOCK_SECONDS_DEFAULT,
        BUDGET_MAX_DEFERRALS_DEFAULT,
    )
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    unit.status = SyncRunUnitStatus.RETRYING.value
    unit.available_at = now - timedelta(seconds=1)
    unit.budget_deferrals = 2
    assert unit.budget_deferrals < BUDGET_MAX_DEFERRALS_DEFAULT
    unit.budget_first_deferred_at = now - timedelta(
        seconds=BUDGET_DEFERRAL_WALL_CLOCK_SECONDS_DEFAULT + 60
    )
    unit.result = {
        "error_category": "budget_deferred",
        "not_before": (now - timedelta(seconds=1)).isoformat(),
        "budget_guard": [
            {
                "decision": "deferred",
                "budget_key": "github:org:api.github.com:fp:rest_core:git",
                "estimated_units": 360,
                "budget_limit": 100,
            }
        ],
    }
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    # Still oversized on THIS pass -- the wall clock is what ends it, but the
    # unit must genuinely not fit for exhaustion to be considered at all.
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 0}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")
    monkeypatch.setattr(
        github_budget, "_credential_fingerprint", lambda *_a, **_k: "ca360fbc"
    )

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result is not None
    assert unit.result["error_category"] == "budget_deferral_exhausted"
    assert unit.result["budget_deferrals"] == 2
    # The error text is built from THIS pass's live observation, not from the
    # stale result['budget_guard'] payload seeded above (which claims 360
    # units against a cap of 100). Review round 2, F1: the explanation has to
    # come from the same evaluation that made the decision, or an operator is
    # handed numbers from a bucket state that no longer exists.
    #
    # CHAOS-3530: asserted against the NUMERIC evidence field, not a bare
    # "360" substring of the whole message. The credential fingerprint above
    # is pinned to a digest that itself contains "360" (github.com/... run
    # 31160919212 hit an unpinned digest with the same collision) -- a
    # substring check over the full error text fires on that hex noise
    # whether or not the stale count leaked, and used to flake whenever an
    # unpinned random digest happened to contain the same three digits.
    # Checking the field the number actually lives in is immune to what the
    # fingerprint hashes to.
    assert unit.error is not None
    assert unit.result["estimated_units"] == 2, unit.result
    assert unit.result["estimated_units"] != 360, unit.result
    assert "estimates 2 units" in unit.error
    assert "cap is 0" in unit.error
    assert "rest_core" in unit.error
    assert "SYNC_BUDGET_BUCKET_LIMITS" in unit.error


def test_fresh_unit_with_no_budget_history_is_never_exhausted(db_session, monkeypatch):
    """Guard-failing control: exhaustion must require GENUINE prior
    budget-deferral history. A unit with a clean budget pair dispatches
    normally, even on a pass where the guard is evaluated.
    """
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    assert unit.budget_deferrals == 0
    assert unit.budget_first_deferred_at is None
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value


def test_stale_budget_state_does_not_terminalize_unrelated_retry(
    db_session, monkeypatch
):
    """Defence in depth (mirrors the rate-limit predicate's round-3 gate):
    a unit carrying budget-exhausted-LOOKING budget columns whose own last
    recorded cause is something else entirely (here a rate-limit deferral)
    must NOT be terminalized off them. Only a unit whose last cause is the
    budget guard's own deferral category is eligible.
    """
    from dev_health_ops.sync.budget_guard import BUDGET_MAX_DEFERRALS_DEFAULT
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    unit.status = SyncRunUnitStatus.RETRYING.value
    unit.available_at = now - timedelta(seconds=1)
    # Well past BOTH caps -- and still not eligible.
    unit.budget_deferrals = BUDGET_MAX_DEFERRALS_DEFAULT + 5
    unit.budget_first_deferred_at = now - timedelta(days=7)
    unit.result = {"error_category": "rate_limit", "rate_limit_deferrals": 1}
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert unit.status != SyncRunUnitStatus.FAILED.value
    assert result == {"status": "dispatched", "queued_units": 1}


def test_rate_limit_deferral_clears_the_budget_episode_pair(db_session, monkeypatch):
    """Episode symmetry (the converse of the existing budget-deferral-clears-
    the-rate-limit-pair invariant): a rate-limit cooldown deferral is NOT a
    budget episode, so it clears budget_deferrals/budget_first_deferred_at.
    Without this, a unit that budget-deferred a few times and then hit a
    genuine 429 would keep counting toward budget exhaustion off a cause
    that has nothing to do with its budget.
    """
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    now = datetime.now(timezone.utc)
    second = _sibling_unit(
        run, first, dataset_key="commits", processor_flags={"sync_git": True}
    )
    second.budget_deferrals = 4
    second.budget_first_deferred_at = now - timedelta(minutes=30)
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
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_deferred"
    assert second.budget_deferrals == 0
    assert second.budget_first_deferred_at is None


def test_budget_episode_pair_cleared_on_success_starts_fresh_later(
    db_session, monkeypatch
):
    """A unit that resolves its budget episode by SUCCEEDING gets a fresh
    count and a fresh wall clock for any later episode -- it is not
    immediately exhausted against a resolved one. This is what makes the
    Lane 2 window ratchet safe: each narrowed window that succeeds resets
    the episode instead of inheriting the cold-start block's history.
    """
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit
    from tests.test_sync_units import (
        _mark_dispatching,
        _patch_finalize_apply,
        _patch_runtime,
    )

    run, unit = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    unit.budget_deferrals = 7
    unit.budget_first_deferred_at = now - timedelta(hours=4)
    db_session.flush()

    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", lambda ctx, rt: {"ok": 1})

    result = getattr(run_sync_unit, "run")(str(unit.id))
    assert result["status"] == "success"

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.budget_deferrals == 0
    assert unit.budget_first_deferred_at is None


def _stamp_values_calls(source: str):
    """Every ``update(SyncRunUnit).values(...)`` call in a module, as
    {keyword: unparsed value} dicts."""
    import ast

    calls = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr != "values":
            continue
        calls.append(
            {
                keyword.arg: ast.unparse(keyword.value)
                for keyword in node.keywords
                if keyword.arg is not None
            }
        )
    return calls


#: Columns every stamp writes regardless of WHY it fired -- status plumbing,
#: not deferral lifecycle. Listed so that any OTHER column a deferral stamp
#: touches is treated as lifecycle state and must be classified below.
_GENERIC_STAMP_COLUMNS = frozenset(
    {
        "status",
        "available_at",
        "attempts",
        "duration_seconds",
        "error",
        "result",
        "processor_flags",
        "lease_owner",
        "lease_expires_at",
        "last_heartbeat_at",
        "updated_at",
        "expired_lease_retry_count",
        "last_retry_reason",
        "retry_exhausted_at",
    }
)


def test_deferral_lifecycle_columns_are_classified_and_stamped_correctly():
    """CHAOS-3412 closure guard for the defect CLASS, not just this instance.

    Two distinct lifecycles have to stay straight, and getting either wrong
    reintroduces an invisible-forever unit:

    * PER-EPISODE (``rate_limit_*`` / ``budget_*``) -- reset by every
      non-terminal stamp, so each pair measures ONE continuous episode. A
      stamp that forgets a pair lets an exhaustion decision read a resolved
      episode's counters.
    * AGGREGATE (``first_blocked_at``) -- deliberately NOT in the per-episode
      set. It is set-if-null by every DEFERRAL stamp and cleared ONLY by a
      successful dispatch claim or SUCCESS. If a non-terminal stamp reset it
      the way it resets the per-episode pairs, the budget/rate-limit
      alternation would clear the outer bound too and the F2 oscillation
      would be unbounded again.

    The column set is DERIVED from the live model and from what the real
    deferral stamps actually assign -- never listed here -- so a THIRD
    lifecycle column added later fails this test until someone classifies it.
    """
    import ast
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[1]
    read = lambda rel: (repo_root / rel).read_text()  # noqa: E731

    model_columns = {
        statement.target.id
        for node in ast.walk(
            ast.parse(read("src/dev_health_ops/models/integrations.py"))
        )
        if isinstance(node, ast.ClassDef) and node.name == "SyncRunUnit"
        for statement in node.body
        if isinstance(statement, ast.AnnAssign)
        and isinstance(statement.target, ast.Name)
    }
    per_episode = {
        name
        for name in model_columns
        if name.startswith("rate_limit_") or name.startswith("budget_")
    }
    # A derivation that finds nothing must FAIL, not read as "all clean".
    assert len(per_episode) >= 4, per_episode
    assert {"budget_deferrals", "budget_first_deferred_at"} <= per_episode
    # The aggregate column is deliberately outside the per-episode set --
    # pinned so a rename into the prefix space cannot silently enrol it into
    # the "reset at every stamp" rule that would defeat it.
    assert "first_blocked_at" in model_columns
    assert "first_blocked_at" not in per_episode

    # (module, marker keyword, marker value) for the three real DEFERRAL
    # stamps -- the ones that hold a unit back without it ever running.
    deferral_stamps = [
        (
            "src/dev_health_ops/sync/budget_guard.py",
            "error",
            "'deferred by sync budget guard'",
        ),
        (
            "src/dev_health_ops/sync/budget_guard.py",
            "error",
            "'deferred by sync cooldown guard'",
        ),
        (
            "src/dev_health_ops/workers/sync_units.py",
            "rate_limit_first_seen_at",
            "first_seen_at",
        ),
    ]
    lifecycle_columns: set[str] = set()
    for relative_path, marker_key, marker_value in deferral_stamps:
        matches = [
            assigned
            for assigned in _stamp_values_calls(read(relative_path))
            if assigned.get(marker_key) == marker_value
        ]
        assert len(matches) == 1, (
            f"{relative_path}: expected exactly one deferral stamp with "
            f"{marker_key}={marker_value}, found {len(matches)} -- the stamp "
            "this guard measures moved or changed meaning."
        )
        assigned = matches[0]
        lifecycle_columns |= (set(assigned) & model_columns) - _GENERIC_STAMP_COLUMNS
        # Every deferral stamp must START the aggregate clock, set-if-null.
        assert "first_blocked_at" in assigned, (
            f"{relative_path} deferral stamp does not set first_blocked_at; "
            "a blocking reason that never starts the aggregate clock is "
            "invisible to the outer bound."
        )
        assert "coalesce" in assigned["first_blocked_at"].lower(), (
            f"{relative_path} overwrites first_blocked_at instead of "
            "COALESCEing it. An overwrite lets an episode change restart the "
            "outer bound, which is exactly the F2 defect."
        )

    # The classification is exhaustive: every lifecycle column a deferral
    # stamp writes is either per-episode or the aggregate clock.
    assert lifecycle_columns == per_episode | {"first_blocked_at"}, (
        f"unclassified deferral-lifecycle column(s): "
        f"{sorted(lifecycle_columns - (per_episode | {'first_blocked_at'}))}. "
        "Decide whether it resets per episode or tracks the aggregate block, "
        "and extend this guard accordingly."
    )

    # Per-episode rule: every non-terminal stamp assigns EVERY per-episode
    # column. Terminal (failed) stamps are excluded -- a failed unit is never
    # a dispatch candidate, so no predicate reads it.
    sites = [
        ("src/dev_health_ops/workers/sync_units.py", "SUCCESS"),
        ("src/dev_health_ops/workers/sync_units.py", "RETRYING"),
        ("src/dev_health_ops/workers/sync_reconciler.py", "RETRYING"),
        ("src/dev_health_ops/sync/budget_guard.py", "RETRYING"),
    ]
    checked = 0
    for relative_path, status_name in sites:
        marker = f"SyncRunUnitStatus.{status_name}.value"
        for assigned in _stamp_values_calls(read(relative_path)):
            if assigned.get("status") != marker:
                continue
            checked += 1
            missing = per_episode - set(assigned)
            assert not missing, (
                f"{relative_path} {status_name} stamp does not assign "
                f"{sorted(missing)}. Every non-terminal stamp must either "
                "clear an episode pair or set it deliberately; leaving it "
                "untouched is what let CHAOS-3412's budget columns (and, "
                "before it, CHAOS-2760's rate-limit columns) be read from a "
                "resolved episode."
            )
    # An unmeasured path must fail loudly rather than read as covered.
    assert checked >= len(sites), f"only located {checked} stamp sites"

    # Aggregate rule: SUCCESS and the dispatch claim are the ONLY places that
    # clear it, and they clear it outright rather than COALESCEing.
    claim_and_success = [
        assigned
        for assigned in _stamp_values_calls(
            read("src/dev_health_ops/workers/sync_units.py")
        )
        if assigned.get("first_blocked_at") == "None"
    ]
    assert len(claim_and_success) == 3, (
        "expected exactly 3 first_blocked_at clear sites (SUCCESS plus the "
        f"two dispatch-claim UPDATEs), found {len(claim_and_success)}"
    )


# ---------------------------------------------------------------------------
# CHAOS-3412 review round 2: F1 (fit-before-kill) and F2 (aggregate clock)
# ---------------------------------------------------------------------------


def test_unit_at_count_cap_is_admitted_when_capacity_frees_up(db_session, monkeypatch):
    """F1 (HIGH): exhaustion is a statement about NOW, not about history.

    A unit can reach the deferral cap and then have its bucket free up --
    a sibling finishes, an hourly window rolls over. Evaluating exhaustion
    before admission killed it on last pass's evidence, discarding work that
    would have succeeded on this one. Before the fix this test failed with
    'failed' != 'dispatching'.
    """
    from dev_health_ops.sync.budget_guard import BUDGET_MAX_DEFERRALS_DEFAULT
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 0}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    _lap_budget_deferrals(db_session, run, unit, laps=BUDGET_MAX_DEFERRALS_DEFAULT)
    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.budget_deferrals == BUDGET_MAX_DEFERRALS_DEFAULT

    # Capacity is now available. The unit is AT the cap, so under the old
    # ordering this pass would have killed it without ever asking whether it
    # still fits.
    monkeypatch.delenv("SYNC_BUDGET_BUCKET_LIMITS", raising=False)
    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.result is None or (
        unit.result.get("error_category") != "budget_deferral_exhausted"
    )
    # The successful claim also stops the aggregate blocked clock: the unit
    # is no longer going nowhere.
    assert unit.first_blocked_at is None


def test_unit_at_count_cap_still_oversized_is_terminalized(db_session, monkeypatch):
    """F1's other half -- the guard must still fail a unit that reached the
    cap AND genuinely still does not fit. Without this, 'check fit first'
    could be satisfied by never terminalizing at all.
    """
    from dev_health_ops.sync.budget_guard import BUDGET_MAX_DEFERRALS_DEFAULT
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 0}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    _lap_budget_deferrals(db_session, run, unit, laps=BUDGET_MAX_DEFERRALS_DEFAULT)
    db_session.refresh(unit)
    assert unit.budget_deferrals == BUDGET_MAX_DEFERRALS_DEFAULT

    # Bucket is STILL full on this pass.
    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result is not None
    assert unit.result["error_category"] == "budget_deferral_exhausted"


def _alternate_blocking_laps(db_session, run, unit, monkeypatch, *, laps: int):
    """Alternate the unit between a budget block and a rate-limit cooldown
    block, so each stamp clears the OTHER episode's counters. This is the F2
    oscillation: genuine, continuous blocking that no per-episode cap can
    ever measure."""
    from dev_health_ops.workers import sync_units

    observation = None
    for lap in range(laps):
        now = datetime.now(timezone.utc)
        if lap % 2 == 0:
            if observation is not None:
                db_session.delete(observation)
                observation = None
            monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 0}')
        else:
            monkeypatch.delenv("SYNC_BUDGET_BUCKET_LIMITS", raising=False)
            observation = _observation(
                run,
                unit,
                route_family="git",
                dimension="rest_core",
                reset_at=now + timedelta(seconds=90),
                observed_at=now - timedelta(seconds=5),
            )
            db_session.add(observation)
        db_session.flush()

        sync_units.dispatch_sync_run(str(run.id))
        db_session.refresh(unit)
        if unit.status == SyncRunUnitStatus.FAILED.value:
            return lap
        unit.available_at = now - timedelta(seconds=1)
        db_session.flush()
    return None


def test_alternating_episodes_preserve_the_aggregate_blocked_clock(
    db_session, monkeypatch
):
    """F2 (HIGH), first half -- the mechanism that makes the cap reachable.

    Each episode kind clears the other's counters by design, so neither
    per-episode cap can ever accumulate under alternation. first_blocked_at
    is the one thing an episode change must NOT reset; if it did, the
    aggregate cap would be as unreachable as the per-episode ones.
    """
    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    terminalized_at = _alternate_blocking_laps(
        db_session, run, unit, monkeypatch, laps=8
    )
    assert terminalized_at is None, "aggregate cap is 24h; 8 same-second laps"

    db_session.refresh(unit)
    first_blocked_at = unit.first_blocked_at
    # Neither per-episode counter can accumulate -- this is why the
    # per-episode caps are structurally unreachable here.
    assert unit.budget_deferrals <= 1
    assert unit.rate_limit_deferrals <= 1
    # But the aggregate clock survived every episode change.
    assert first_blocked_at is not None

    _alternate_blocking_laps(db_session, run, unit, monkeypatch, laps=4)
    db_session.refresh(unit)
    assert _aware(unit.first_blocked_at) == _aware(first_blocked_at)


def test_alternating_episodes_terminalize_at_the_aggregate_cap(db_session, monkeypatch):
    """F2 (HIGH), second half -- the loop actually ends.

    Once the aggregate clock passes SYNC_DEFERRAL_TOTAL_WALL_CLOCK_SECONDS,
    a unit that is STILL blocked fails with its own category, naming the last
    episode kind and both counters so an operator can read "alternating"
    straight off the error.
    """
    from dev_health_ops.sync.budget_guard import (
        DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT,
    )

    run, unit = _seed_run(db_session)
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "60")
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    # Odd lap count so the LAST completed lap is a budget lap -- the error
    # text names whichever episode most recently blocked the unit, and the
    # assertion below is only meaningful if that is deterministic.
    assert _alternate_blocking_laps(db_session, run, unit, monkeypatch, laps=7) is None
    db_session.refresh(unit)
    assert unit.first_blocked_at is not None
    assert unit.result["error_category"] == "budget_deferred"

    # Simulate the elapsed time the laps above cannot: the unit has now been
    # blocked, continuously and for shifting reasons, past the outer bound.
    unit.first_blocked_at = datetime.now(timezone.utc) - timedelta(
        seconds=DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT + 60
    )
    db_session.flush()

    assert _alternate_blocking_laps(db_session, run, unit, monkeypatch, laps=2) == 0

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.FAILED.value
    assert unit.result is not None
    assert unit.result["error_category"] == "deferral_exhausted"
    assert unit.result["blocked_seconds"] >= DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT
    # Both counters are reported: neither is a usable CAP under alternation,
    # but together they are the diagnosis.
    assert "budget deferrals" in unit.error
    assert "rate-limit deferrals" in unit.error
    # Names the episode that most recently blocked it, resolved from the
    # unit's own persisted category -- never the "unknown" fallback.
    assert "sync budget admission" in unit.error
    assert "unknown" not in unit.error


def test_aggregate_cap_does_not_kill_a_unit_that_now_fits(db_session, monkeypatch):
    """F1's rule applied to the aggregate cap too: a unit blocked for longer
    than the outer bound but ADMISSIBLE on this pass is dispatched, not
    failed. The aggregate predicate is only ever asked of a unit the guard
    has just re-established is blocked.
    """
    from dev_health_ops.sync.budget_guard import (
        DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT,
    )
    from dev_health_ops.workers import sync_units

    run, unit = _seed_run(db_session)
    now = datetime.now(timezone.utc)
    unit.status = SyncRunUnitStatus.RETRYING.value
    unit.available_at = now - timedelta(seconds=1)
    unit.first_blocked_at = now - timedelta(
        seconds=DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT * 3
    )
    unit.result = {"error_category": "budget_deferred"}
    db_session.flush()

    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    # No bucket limit and no cooldown: the unit fits now.
    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert unit.status == SyncRunUnitStatus.DISPATCHING.value
    assert unit.first_blocked_at is None


def test_aggregate_blocked_clock_cleared_on_success(db_session, monkeypatch):
    """A unit that gets through is not going nowhere: SUCCESS stops the
    aggregate clock, so a later, unrelated blocking episode starts its own
    24h rather than inheriting a resolved one's.
    """
    from dev_health_ops.processors import dataset_adapters
    from dev_health_ops.workers.sync_units import run_sync_unit
    from tests.test_sync_units import (
        _mark_dispatching,
        _patch_finalize_apply,
        _patch_runtime,
    )

    run, unit = _seed_run(db_session)
    unit.first_blocked_at = datetime.now(timezone.utc) - timedelta(hours=12)
    db_session.flush()

    _mark_dispatching(db_session, unit)
    _patch_db_session(monkeypatch, db_session)
    _patch_runtime(monkeypatch)
    _patch_finalize_apply(monkeypatch)
    monkeypatch.setattr(dataset_adapters, "run_dataset_unit", lambda ctx, rt: {"ok": 1})

    assert getattr(run_sync_unit, "run")(str(unit.id))["status"] == "success"

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.SUCCESS.value
    assert unit.first_blocked_at is None


# ---------------------------------------------------------------------------
# CHAOS-3412 review round 2: R2-F1 (order-independent terminalization) and
# R2-F2 (episode-specific caps first, on every path)
# ---------------------------------------------------------------------------


def _estimate(unit, units_cost: int) -> tuple[BudgetEstimate, ...]:
    return (
        BudgetEstimate(
            bucket=BudgetBucketKey(
                provider="github",
                org_id=str(unit.org_id),
                host="api.github.com",
                credential_fingerprint="fp",
                dimension=BudgetDimension.REST_CORE,
            ),
            estimated_units=units_cost,
            confidence="high",
            route_family="git",
        ),
    )


def _run_with_order(db_session, monkeypatch, *, order, costs, at_cap_dataset):
    """Two siblings in one bucket with controlled estimates and a controlled
    candidate ORDER (review round 2, R2-F1). `at_cap_dataset` starts at the
    budget deferral cap. Estimates and order are injected because the defect
    IS about ordering: it cannot be exercised without controlling both."""
    from dev_health_ops.sync import budget_guard
    from dev_health_ops.sync.budget_guard import BUDGET_MAX_DEFERRALS_DEFAULT
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.dataset_key = "alpha"
    second = _sibling_unit(
        run, first, dataset_key="beta", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    by_key = {"alpha": first, "beta": second}
    for name, unit in by_key.items():
        if name == at_cap_dataset:
            unit.status = SyncRunUnitStatus.RETRYING.value
            unit.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
            unit.budget_deferrals = BUDGET_MAX_DEFERRALS_DEFAULT
            unit.budget_first_deferred_at = datetime.now(timezone.utc) - timedelta(
                minutes=5
            )
            unit.result = {"error_category": "budget_deferred"}
    db_session.flush()

    monkeypatch.setattr(
        budget_guard,
        "estimate_provider_budget",
        lambda ctx: _estimate(by_key[ctx.dataset_key], costs[ctx.dataset_key]),
    )
    ordered = [by_key[name] for name in order]
    monkeypatch.setattr(
        budget_guard,
        "_dispatch_candidate_units",
        lambda *a, **k: [
            u
            for u in ordered
            if u.status
            in {SyncRunUnitStatus.PLANNED.value, SyncRunUnitStatus.RETRYING.value}
        ],
    )
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 10}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    sync_units.dispatch_sync_run(str(run.id))
    db_session.refresh(by_key[at_cap_dataset])
    return by_key[at_cap_dataset]


@pytest.mark.parametrize("order", [["alpha", "beta"], ["beta", "alpha"]])
def test_at_cap_unit_that_fits_the_baseline_is_never_killed_in_either_order(
    db_session, monkeypatch, order
):
    """R2-F1 (HIGH): a terminal verdict may not depend on candidate order.

    beta costs 2 against a bucket cap of 10 and sits at its deferral cap;
    alpha costs 9. Visiting alpha first consumed 9 of the cap, so beta's live
    estimate became would_defer and beta was terminalized -- with an error
    that literally read "estimates 2 units ... whose cap is 10 ... so it can
    never be admitted". Visiting beta first admitted it. Same unit, same
    world, opposite verdicts.

    Terminalization is now measured against the DURABLE baseline (work
    already dispatching/running), which no candidate ordering can change.
    """
    at_cap = _run_with_order(
        db_session,
        monkeypatch,
        order=order,
        costs={"alpha": 9, "beta": 2},
        at_cap_dataset="beta",
    )
    assert at_cap.status != SyncRunUnitStatus.FAILED.value, (
        f"order {order} terminalized a unit whose own estimate (2) fits the "
        f"bucket cap (10): {at_cap.error}"
    )
    if at_cap.status == SyncRunUnitStatus.RETRYING.value:
        assert at_cap.result is not None
        # Deferred rather than killed -- and the counter still advances, so
        # the aggregate clock remains the backstop if contention never clears.
        assert at_cap.result["error_category"] == "budget_deferred"
        assert at_cap.first_blocked_at is not None


def test_permanently_oversized_unit_at_cap_is_still_terminalized(
    db_session, monkeypatch
):
    """R2-F1's other half: "measure against the baseline" must not degrade
    into "never terminalize". A unit whose OWN estimate exceeds the whole
    bucket cap is unfit no matter what else is running or admitted, and must
    still fail -- with the "can never be admitted" claim, which is true here.
    """
    at_cap = _run_with_order(
        db_session,
        monkeypatch,
        order=["alpha", "beta"],
        costs={"alpha": 1, "beta": 40},
        at_cap_dataset="beta",
    )
    assert at_cap.status == SyncRunUnitStatus.FAILED.value
    assert at_cap.result is not None
    assert at_cap.error is not None
    assert at_cap.result["error_category"] == "budget_deferral_exhausted"
    assert at_cap.result["permanently_oversized"] is True
    assert "can never be admitted" in at_cap.error


def test_contention_blocked_unit_error_never_claims_it_can_never_be_admitted(
    db_session, monkeypatch
):
    """The claim has to be literally true when made. A unit blocked only by
    DURABLE contention (work already running) is genuinely at its cap and is
    terminalized, but it is not permanently oversized -- waiting would let it
    run. The text must say contention, not "never".
    """
    from dev_health_ops.sync import budget_guard
    from dev_health_ops.sync.budget_guard import BUDGET_MAX_DEFERRALS_DEFAULT
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.dataset_key = "alpha"
    # alpha is RUNNING with a live lease: durable consumption, not an
    # optional admission this pass makes.
    first.status = SyncRunUnitStatus.RUNNING.value
    first.lease_owner = "worker-a"
    first.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)
    second = _sibling_unit(
        run, first, dataset_key="beta", processor_flags={"sync_git": True}
    )
    second.status = SyncRunUnitStatus.RETRYING.value
    second.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    second.budget_deferrals = BUDGET_MAX_DEFERRALS_DEFAULT
    second.budget_first_deferred_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    second.result = {"error_category": "budget_deferred"}
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    by_key = {"alpha": first, "beta": second}
    monkeypatch.setattr(
        budget_guard,
        "estimate_provider_budget",
        lambda ctx: _estimate(
            by_key[ctx.dataset_key], {"alpha": 9, "beta": 2}[ctx.dataset_key]
        ),
    )
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 10}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    sync_units.dispatch_sync_run(str(run.id))
    db_session.refresh(second)

    assert second.status == SyncRunUnitStatus.FAILED.value
    assert second.result is not None
    assert second.error is not None
    assert second.result["permanently_oversized"] is False
    assert second.result["durable_units"] == 9
    assert "can never be admitted" not in second.error
    assert "already running" in second.error


def test_sibling_completion_lets_a_previously_contended_unit_run(
    db_session, monkeypatch
):
    """The race codex named: the durable holder finishes between passes, so
    the contention that was blocking the unit is gone and it must run. Under
    the old ordering the unit could already have been killed by then.
    """
    from dev_health_ops.sync import budget_guard
    from dev_health_ops.sync.budget_guard import BUDGET_MAX_DEFERRALS_DEFAULT
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.dataset_key = "alpha"
    first.status = SyncRunUnitStatus.RUNNING.value
    first.lease_owner = "worker-a"
    first.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)
    second = _sibling_unit(
        run, first, dataset_key="beta", processor_flags={"sync_git": True}
    )
    second.status = SyncRunUnitStatus.RETRYING.value
    second.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    second.budget_deferrals = BUDGET_MAX_DEFERRALS_DEFAULT - 1
    second.budget_first_deferred_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    second.result = {"error_category": "budget_deferred"}
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    by_key = {"alpha": first, "beta": second}
    monkeypatch.setattr(
        budget_guard,
        "estimate_provider_budget",
        lambda ctx: _estimate(
            by_key[ctx.dataset_key], {"alpha": 9, "beta": 2}[ctx.dataset_key]
        ),
    )
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 10}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    # Pass 1: alpha still running -> beta contends, defers, hits its cap.
    sync_units.dispatch_sync_run(str(run.id))
    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.RETRYING.value
    assert second.budget_deferrals == BUDGET_MAX_DEFERRALS_DEFAULT

    # Pass 2: alpha finished. The cap is spent, but the unit now fits.
    first.status = SyncRunUnitStatus.SUCCESS.value
    first.lease_owner = None
    first.lease_expires_at = None
    second.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db_session.flush()

    result = sync_units.dispatch_sync_run(str(run.id))
    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.DISPATCHING.value, second.error
    assert result["queued_units"] == 1


def test_single_cause_rate_limit_unit_gets_the_specific_category(
    db_session, monkeypatch
):
    """R2-F2 (MEDIUM): episode-specific caps are evaluated before the
    aggregate backstop on EVERY path. A unit whose only cause was rate
    limiting used to fail as the generic 'deferral_exhausted' carrying "the
    blocking reason kept changing" -- false for a unit that only ever had one
    reason, and it hides the actionable diagnosis.
    """
    from dev_health_ops.sync.budget_guard import (
        DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT,
    )
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
        status=SyncRunUnitStatus.RETRYING.value,
        rate_limit_deferrals=RATE_LIMIT_MAX_DEFERRALS,
        rate_limit_first_seen_at=now - timedelta(hours=3),
        result={"error_category": "rate_limit_cooldown_deferred"},
    )
    second.available_at = now - timedelta(seconds=1)
    second.budget_deferrals = 0  # never had a budget episode
    second.first_blocked_at = now - timedelta(
        seconds=DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT + 60
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
            reset_at=now + timedelta(seconds=300),
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
    # Both caps were spent; the SPECIFIC one wins.
    assert second.result["error_category"] == "rate_limit_cooldown_exhausted"


def test_generic_exhaustion_text_only_claims_alternation_when_it_happened(
    db_session, monkeypatch
):
    """The generic category is also reachable for a SINGLE-cause unit (one
    held back purely by durable budget contention, which the episode cap
    deliberately does not terminalize). Its text must match its own evidence:
    both counters non-zero means alternation, otherwise it must not claim it.
    """
    from dev_health_ops.sync import budget_guard
    from dev_health_ops.sync.budget_guard import (
        DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT,
    )
    from dev_health_ops.workers import sync_units

    run, first = _seed_run(db_session)
    first.dataset_key = "alpha"
    first.status = SyncRunUnitStatus.RUNNING.value
    first.lease_owner = "worker-a"
    first.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)
    second = _sibling_unit(
        run, first, dataset_key="beta", processor_flags={"sync_git": True}
    )
    second.status = SyncRunUnitStatus.RETRYING.value
    second.available_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    # Budget-only history, and past the aggregate bound.
    second.budget_deferrals = 3
    second.budget_first_deferred_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    second.rate_limit_deferrals = 0
    second.first_blocked_at = datetime.now(timezone.utc) - timedelta(
        seconds=DEFERRAL_TOTAL_WALL_CLOCK_SECONDS_DEFAULT + 60
    )
    second.result = {"error_category": "budget_deferred"}
    run.total_units = 2
    db_session.add(second)
    db_session.flush()

    by_key = {"alpha": first, "beta": second}
    monkeypatch.setattr(
        budget_guard,
        "estimate_provider_budget",
        lambda ctx: _estimate(
            by_key[ctx.dataset_key], {"alpha": 9, "beta": 2}[ctx.dataset_key]
        ),
    )
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 10}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)

    sync_units.dispatch_sync_run(str(run.id))
    db_session.refresh(second)

    assert second.status == SyncRunUnitStatus.FAILED.value
    assert second.result is not None
    assert second.error is not None
    assert second.result["error_category"] == "deferral_exhausted"
    # The evidence says one cause only -- the text must agree with it.
    assert second.result["episodes_alternated"] is False
    assert second.result["rate_limit_deferrals"] == 0
    assert "alternated" not in second.error
    assert "kept changing" not in second.error


# ---------------------------------------------------------------------------
# CHAOS-3412 closure: the terminalization chokepoint
# ---------------------------------------------------------------------------


def test_stale_capped_counters_with_cooldown_defer_as_a_fresh_episode(
    db_session, monkeypatch
):
    """Closure instance 5 (codex round 3).

    A unit carrying CAPPED rate-limit counters from an episode that already
    resolved -- its own last cause is 'worker_lost' -- meets an active
    cooldown. ``_plan_cooldown_deferral`` re-read those counters raw, returned
    None, and terminalized the unit as rate_limit_cooldown_exhausted:
    irreversible, and under a category the unit's own state contradicts. The
    pre-existing regression only covered the no-visible-cooldown path.

    Counters are evidence only when the unit's last cause says the episode is
    live, so this is a FRESH episode: the unit defers and its counters restart
    from this genuine block.
    """
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
        status=SyncRunUnitStatus.RETRYING.value,
        rate_limit_deferrals=RATE_LIMIT_MAX_DEFERRALS,
        rate_limit_first_seen_at=now - timedelta(hours=3),
        result={"error_category": "worker_lost", "retry_reason": "expired_lease"},
    )
    second.available_at = now - timedelta(seconds=1)
    run.total_units = 2
    db_session.add(second)
    db_session.flush()
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
    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(second)
    assert second.status == SyncRunUnitStatus.RETRYING.value, second.error
    assert second.result is not None
    assert second.result["error_category"] == "rate_limit_cooldown_deferred"
    # Restarted, not continued: the resolved episode's 10 is not inherited.
    assert second.rate_limit_deferrals == 1


def test_chokepoint_refuses_a_verdict_the_unit_state_does_not_evidence():
    """The chokepoint's own check (a), tested directly rather than only
    through a caller: a verdict naming an episode the unit's last cause does
    not evidence is REFUSED (returns None), not written."""
    from dev_health_ops.sync.budget_guard import (
        TerminalOutcome,
        TerminalVerdict,
        _terminalize_unit,
    )

    class _Session:
        def __init__(self):
            self.executed = []

        def execute(self, statement):
            self.executed.append(statement)
            raise AssertionError("refused verdict must never reach the database")

    unit = SyncRunUnit(
        org_id="org",
        sync_run_id=uuid.uuid4(),
        integration_id=uuid.uuid4(),
        source_id=uuid.uuid4(),
        provider="github",
        dataset_key="commits",
        cost_class="medium",
        mode=SyncRunMode.INCREMENTAL.value,
        status=SyncRunUnitStatus.RETRYING.value,
        rate_limit_deferrals=10,
        result={"error_category": "worker_lost"},
    )
    session = _Session()
    decision = _terminalize_unit(
        session,
        unit,
        verdict=TerminalVerdict(
            error_category="rate_limit_cooldown_exhausted",
            error_text="rate limit cooldown deferral budget exhausted",
            evidence={"rate_limit_deferrals": 10},
            episode="rate_limit",
        ),
        now=datetime.now(timezone.utc),
        log_ctx={},
    )
    # A refusal is its OWN outcome, never conflated with a lost CAS race.
    assert decision.outcome is TerminalOutcome.REFUSED
    assert decision.at is None
    assert session.executed == []
    assert unit.status == SyncRunUnitStatus.RETRYING.value


def test_chokepoint_rejects_unlicensed_claims_and_missing_fitness():
    """Checks (b) and (c) are invariants over code this module writes, so they
    raise rather than silently producing a false diagnosis."""
    from dev_health_ops.sync.budget_guard import (
        TerminalVerdict,
        TerminalVerdictError,
        _assert_verdict_wellformed,
    )

    # (c) an unlicensed "can never be admitted"
    with pytest.raises(TerminalVerdictError, match="can never be admitted"):
        _assert_verdict_wellformed(
            TerminalVerdict(
                error_category="deferral_exhausted",
                error_text="... so it can never be admitted ...",
                evidence={"permanently_oversized": False},
            )
        )
    # (c) an unlicensed alternation claim
    with pytest.raises(TerminalVerdictError, match="alternated"):
        _assert_verdict_wellformed(
            TerminalVerdict(
                error_category="deferral_exhausted",
                error_text="the reason alternated between causes",
                evidence={"episodes_alternated": False},
            )
        )
    # (b) a fitness claim with no durable-world verdict attached
    with pytest.raises(TerminalVerdictError, match="fitness claim"):
        _assert_verdict_wellformed(
            TerminalVerdict(
                error_category="budget_deferral_exhausted",
                error_text="exhausted",
                evidence={},
                episode="budget",
            )
        )
    # An unexplained terminal failure is the state this ticket removes.
    with pytest.raises(TerminalVerdictError, match="no error text"):
        _assert_verdict_wellformed(
            TerminalVerdict(
                error_category="deferral_exhausted", error_text="  ", evidence={}
            )
        )


def test_every_terminal_deferral_stamp_routes_through_the_chokepoint():
    """The property that makes "the class is closed" a claim and not a hope.

    Derives every ``status=FAILED`` stamp in the deferral subsystem and
    asserts each one is lexically inside ``_terminalize_unit``. A sixth
    terminal path either routes through the chokepoint -- and is therefore
    subject to checks (a), (b) and (c) -- or fails here. It cannot be added
    quietly.

    Scope is stated rather than assumed: the deferral-exhaustion categories
    this closure owns. ``worker_lost_retry_exhausted`` (lease-retry
    exhaustion, workers/sync_reconciler.py) is a different subsystem with its
    own decision path and is deliberately NOT claimed here -- claiming it
    without routing it would be exactly the kind of coverage assertion this
    ticket exists to stop making.
    """
    import ast
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[1]
    guard_path = "src/dev_health_ops/sync/budget_guard.py"
    other_paths = [
        "src/dev_health_ops/workers/sync_units.py",
        "src/dev_health_ops/workers/sync_reconciler.py",
    ]

    guard_source = (repo_root / guard_path).read_text()
    guard_tree = ast.parse(guard_source)

    # The registered categories, derived from the module rather than listed.
    owned_categories = {
        node.targets[0].id: ast.literal_eval(node.value)
        for node in ast.walk(guard_tree)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id.endswith("_EXHAUSTED_CATEGORY")
        and isinstance(node.value, ast.Constant)
    }
    assert len(owned_categories) >= 3, owned_categories
    owned = set(owned_categories.values())

    def enclosing_functions(tree):
        """{node -> nearest enclosing function name} for every node."""
        owner: dict[ast.AST, str] = {}
        for func in ast.walk(tree):
            if isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for child in ast.walk(func):
                    owner.setdefault(child, func.name)
        return owner

    def failed_stamps(tree):
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr != "values":
                continue
            assigned = {
                keyword.arg: ast.unparse(keyword.value)
                for keyword in node.keywords
                if keyword.arg is not None
            }
            if assigned.get("status") == "SyncRunUnitStatus.FAILED.value":
                yield node, assigned

    owner = enclosing_functions(guard_tree)
    guard_stamps = list(failed_stamps(guard_tree))
    # A derivation that finds nothing must FAIL, not read as "all routed".
    assert guard_stamps, "no FAILED stamps found in budget_guard.py"
    for node, _assigned in guard_stamps:
        assert owner.get(node) == "_terminalize_unit", (
            f"{guard_path}: a status=FAILED stamp lives in "
            f"{owner.get(node)!r}, outside the terminalization chokepoint. "
            "Every terminal deferral verdict must route through "
            "_terminalize_unit so its evidence, fitness and claims are "
            "checked; adding a second terminal path reopens the defect class "
            "this closure exists to shut."
        )

    # No module outside the guard may stamp one of the owned categories.
    for relative_path in other_paths:
        tree = ast.parse((repo_root / relative_path).read_text())
        for _node, assigned in failed_stamps(tree):
            rendered = assigned.get("result", "")
            leaked = [category for category in owned if category in rendered]
            assert not leaked, (
                f"{relative_path} stamps deferral-exhaustion category "
                f"{leaked!r} outside the chokepoint in budget_guard.py"
            )

    # And every terminalize helper actually delegates to it.
    delegating = {
        func.name
        for func in ast.walk(guard_tree)
        if isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef))
        and func.name.startswith("_terminalize_")
        and func.name != "_terminalize_unit"
        and "_terminalize_unit(" in ast.unparse(func)
    }
    declared = {
        func.name
        for func in ast.walk(guard_tree)
        if isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef))
        and func.name.startswith("_terminalize_")
        and func.name != "_terminalize_unit"
    }
    assert declared, "no _terminalize_* helpers found"
    assert declared == delegating, (
        f"terminalize helper(s) {sorted(declared - delegating)} do not "
        "delegate to _terminalize_unit"
    )


def _cooldown_blocked_stale_counter_unit(db_session, monkeypatch):
    """A unit carrying CAPPED rate-limit counters from a RESOLVED episode
    (last cause 'worker_lost'), due, and gated by an ACTIVE cooldown."""
    from dev_health_ops.workers.rate_limit_defer import RATE_LIMIT_MAX_DEFERRALS

    run, first = _seed_run(db_session)
    first.status = SyncRunUnitStatus.SUCCESS.value
    now = datetime.now(timezone.utc)
    second = _sibling_unit(
        run,
        first,
        dataset_key="commits",
        processor_flags={"sync_git": True},
        status=SyncRunUnitStatus.RETRYING.value,
        rate_limit_deferrals=RATE_LIMIT_MAX_DEFERRALS,
        rate_limit_first_seen_at=now - timedelta(hours=3),
        result={"error_category": "worker_lost", "retry_reason": "expired_lease"},
    )
    second.available_at = now - timedelta(seconds=1)
    second.first_blocked_at = now - timedelta(hours=2)
    run.total_units = 2
    db_session.add(second)
    db_session.flush()
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
    return run, second


def test_stale_counters_under_cooldown_defer_fresh_through_the_outer_caller(
    db_session, monkeypatch
):
    """Driven through the OUTER dispatch path, not the resolver directly.

    Pins the promise end to end: unevidenced counters + an active cooldown
    defer as a FRESH episode, the aggregate clock is untouched, and the unit
    is not claimable while the cooldown holds.
    """
    from dev_health_ops.workers import sync_units

    run, unit = _cooldown_blocked_stale_counter_unit(db_session, monkeypatch)
    before = _aware(unit.first_blocked_at)

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.rate_limit_deferrals == 1
    assert _aware(unit.first_blocked_at) == before
    # Not claimable: it carries a future availability under the live cooldown.
    assert unit.available_at is not None
    assert _aware(unit.available_at) > datetime.now(timezone.utc)


def test_refused_verdict_defers_rather_than_leaving_the_unit_claimable(
    db_session, monkeypatch
):
    """A REFUSED verdict must never read as a lost CAS race.

    Both used to be ``None``, and callers treated ``None`` as "another pass
    owns this unit, skip it" -- so a refusal left a cooldown-blocked unit
    unstamped, ``_claim_units`` dispatched it into the live cooldown, and the
    claim cleared ``first_blocked_at``. A refusal silently resetting the
    aggregate clock violates the invariant this module enforces.

    The refusal path is not reachable through normal state (the episode
    normalizer settles those counters before any verdict is proposed), so the
    condition is injected: an episode whose evidence set licenses nothing.
    Before the fix this test failed with 'dispatching' != 'retrying' and a
    wiped first_blocked_at.
    """
    from dev_health_ops.sync import budget_guard
    from dev_health_ops.workers import sync_units

    run, unit = _cooldown_blocked_stale_counter_unit(db_session, monkeypatch)
    before = _aware(unit.first_blocked_at)

    monkeypatch.setattr(
        budget_guard,
        "_EPISODE_EVIDENCE",
        {**budget_guard._EPISODE_EVIDENCE, "rate_limit": frozenset()},
    )
    monkeypatch.setattr(
        budget_guard, "_rate_limit_deferral_exhausted", lambda unit, *, now: True
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(unit)
    assert result["queued_units"] == 0
    assert unit.status == SyncRunUnitStatus.RETRYING.value
    assert unit.result is not None
    assert unit.result["error_category"] == "rate_limit_cooldown_deferred"
    # Fresh episode: the refused counters are not reused as a starting point.
    assert unit.rate_limit_deferrals == 1
    # The aggregate clock survives a refusal.
    assert _aware(unit.first_blocked_at) == before


def test_terminal_decision_distinguishes_refusal_from_a_lost_race():
    """The type-level half: REFUSED and CAS_LOST are distinct outcomes, and
    only the cooldown path's settler treats them differently. One value with
    two meanings is what let the bug exist."""
    from dev_health_ops.sync.budget_guard import (
        _CARRY_ON,
        TerminalDecision,
        TerminalOutcome,
        _settle_or_skip,
        _settle_terminal_decision,
    )

    now = datetime.now(timezone.utc)
    written = TerminalDecision(TerminalOutcome.TERMINALIZED, now)
    refused = TerminalDecision(TerminalOutcome.REFUSED)
    lost = TerminalDecision(TerminalOutcome.CAS_LOST)

    assert TerminalOutcome.REFUSED is not TerminalOutcome.CAS_LOST
    # Cooldown path: a refusal must be carried on (stamped by the caller),
    # a lost race must not.
    assert _settle_terminal_decision(written) == (now, True)
    assert _settle_terminal_decision(refused) is _CARRY_ON
    assert _settle_terminal_decision(lost) is None
    # Non-cooldown path: both are safely "leave to normal handling".
    assert _settle_or_skip(written) == (now, True)
    assert _settle_or_skip(refused) is None
    assert _settle_or_skip(lost) is None
