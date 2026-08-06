"""In-cycle budget surplus retry (CHAOS-3465).

CHAOS-3412 gave the budget guard an exit for units it can never admit. It did
not give a unit it CAN admit a way back in before its deferral countdown
expires: a unit deferred at the top of a pass sits at ``available_at = now +
SYNC_BUDGET_DEFERRAL_SECONDS``, so the very next pass -- however much of the
bucket has since drained -- does not even consider it. The spare capacity
lapses and the unit waits out a full cycle it did not need to wait.

This module covers the surplus phase that spends that leftover, and -- just as
importantly -- every guard it must NOT spend its way past.

Covers:
  * a not-yet-due budget-deferred unit is admitted from THIS pass's surplus
    and dispatches in the same cycle (the behaviour that did not exist; with
    surplus disabled the same fixture leaves it waiting, which is the
    pre-change behaviour planted as a negative control).
  * counter semantics: a surplus attempt that cannot fit is a total no-op --
    it never advances budget_deferrals, so trying to help a unit can never
    drag its exhaustion caps forward.
  * counter semantics: a surplus attempt that succeeds writes availability
    only, leaving the episode's end to the claim and the SUCCESS stamp that
    already own it (asserted behaviourally AND over the source of the stamp).
  * ordering: the LONGEST-deferred unit wins a surplus that only stretches to
    one, in either candidate order.
  * surplus relaxes budget admission and nothing else: the per-bucket
    concurrency cap, an active provider cooldown, and the terminal outcome of
    budget exhaustion each still bind.
  * fail-closed: without DispatchGuard's slot headroom there is no surplus
    retry at all.
  * the candidate cap is logged, never silently applied.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from dev_health_ops.models import (
    Base,
    SyncRunUnit,
    SyncRunUnitStatus,
)
from dev_health_ops.sync.budget_types import (
    BudgetBucketKey,
    BudgetDimension,
    BudgetEstimate,
)
from tests._helpers import seed_sync_dispatch_transport_routes
from tests.test_budget_guard_cooldown import _observation, _sibling_unit
from tests.test_sync_units import (
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


def _estimate_of(org_id, units_cost: int) -> tuple[BudgetEstimate, ...]:
    return (
        BudgetEstimate(
            bucket=BudgetBucketKey(
                provider="github",
                org_id=str(org_id),
                host="api.github.com",
                credential_fingerprint="fp",
                dimension=BudgetDimension.REST_CORE,
            ),
            estimated_units=units_cost,
            confidence="high",
            route_family="git",
        ),
    )


def _mark_budget_deferred(
    unit: SyncRunUnit,
    *,
    now: datetime,
    deferrals: int = 2,
    deferred_ago: timedelta = timedelta(minutes=5),
    available_in: timedelta = timedelta(seconds=60),
) -> None:
    """Put a unit in the exact state ``_defer_unit_for_budget`` leaves it in:
    RETRYING, counting down, carrying a live budget episode. Producible state
    matters -- a fixture the real deferral path cannot emit would be asserting
    against a row that never occurs."""
    unit.status = SyncRunUnitStatus.RETRYING.value
    unit.available_at = now + available_in
    unit.budget_deferrals = deferrals
    unit.budget_first_deferred_at = now - deferred_ago
    unit.first_blocked_at = now - deferred_ago
    unit.rate_limit_deferrals = 0
    unit.rate_limit_first_seen_at = None
    unit.result = {
        "error_category": "budget_deferred",
        "not_before": (now + available_in).isoformat(),
        "budget_guard": [],
    }


def _two_unit_run(db_session, monkeypatch, *, costs: dict[str, int], limit: int):
    """One PLANNED candidate ('alpha') plus one not-yet-due budget-deferred
    unit ('beta') in the same bucket, with injected estimates so the surplus
    left after admitting alpha is an exact, stated number rather than whatever
    the live estimators happen to produce."""
    from dev_health_ops.sync import budget_guard

    run, alpha = _seed_run(db_session)
    alpha.dataset_key = "alpha"
    beta = _sibling_unit(
        run, alpha, dataset_key="beta", processor_flags={"sync_git": True}
    )
    run.total_units = 2
    db_session.add(beta)
    db_session.flush()

    now = datetime.now(timezone.utc)
    _mark_budget_deferred(beta, now=now)
    db_session.flush()

    monkeypatch.setattr(
        budget_guard,
        "estimate_provider_budget",
        lambda ctx: _estimate_of(run.org_id, costs[ctx.dataset_key]),
    )
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", f'{{"github:rest_core": {limit}}}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")
    _patch_db_session(monkeypatch, db_session)
    _patch_worker_enqueues(monkeypatch)
    return run, alpha, beta


# ---------------------------------------------------------------------------
# The capability itself, and the pre-change behaviour as its negative control
# ---------------------------------------------------------------------------


def test_deferred_unit_is_admitted_from_this_cycles_surplus(db_session, monkeypatch):
    """The reached state, not "the code ran": a unit that was still counting
    down a budget deferral is DISPATCHING at the end of this pass.

    alpha costs 1 of a cap of 10, so 9 units of budget were about to lapse.
    beta costs 1 and was 59 seconds from being reconsidered. Before this
    change beta was not in the candidate set at all -- ``queued_units`` was 1
    and beta was still ``retrying`` -- which is what
    ``test_surplus_disabled_leaves_the_deferred_unit_waiting`` pins as the old
    behaviour.
    """
    from dev_health_ops.workers import sync_units

    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 1, "beta": 1}, limit=10
    )

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(alpha)
    db_session.refresh(beta)
    assert result == {"status": "dispatched", "queued_units": 2}
    assert alpha.status == SyncRunUnitStatus.DISPATCHING.value
    assert beta.status == SyncRunUnitStatus.DISPATCHING.value, (
        "the deferred unit was left to wait out its countdown while the "
        "budget it needed went unspent -- the CHAOS-3465 defect"
    )


def test_surplus_disabled_leaves_the_deferred_unit_waiting(db_session, monkeypatch):
    """NEGATIVE CONTROL: the same fixture, with the surplus phase turned off,
    reproduces the pre-change behaviour exactly -- beta stays ``retrying``
    with its countdown untouched and only alpha dispatches.

    Without this pairing the test above would pass on any code that dispatches
    two units for any reason; with it, the two tests differ in nothing but
    whether surplus retry runs.
    """
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_BUDGET_SURPLUS_MAX_CANDIDATES", "0")
    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 1, "beta": 1}, limit=10
    )
    deferred_until = beta.available_at

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(alpha)
    db_session.refresh(beta)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert alpha.status == SyncRunUnitStatus.DISPATCHING.value
    assert beta.status == SyncRunUnitStatus.RETRYING.value
    assert beta.available_at == deferred_until


# ---------------------------------------------------------------------------
# Counter semantics
# ---------------------------------------------------------------------------


def test_surplus_attempt_that_cannot_fit_is_a_total_no_op(db_session, monkeypatch):
    """A surplus attempt is not a deferral, and must not be counted as one.

    alpha takes 9 of a cap of 10; beta needs 4 and does not fit the leftover.
    Every column the exhaustion predicates read has to come out of this pass
    byte-identical -- if a failed attempt incremented ``budget_deferrals``, a
    unit would burn one deferral per pass merely because the guard looked at
    it, and the units surplus exists to rescue would reach
    SYNC_BUDGET_MAX_DEFERRALS FASTEST.
    """
    from dev_health_ops.workers import sync_units

    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 9, "beta": 4}, limit=10
    )
    before = {
        "available_at": beta.available_at,
        "budget_deferrals": beta.budget_deferrals,
        "budget_first_deferred_at": beta.budget_first_deferred_at,
        "first_blocked_at": beta.first_blocked_at,
        "result": dict(beta.result or {}),
        "status": beta.status,
    }

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(beta)
    assert beta.status == before["status"]
    assert beta.available_at == before["available_at"]
    assert beta.budget_deferrals == before["budget_deferrals"]
    assert beta.budget_first_deferred_at == before["budget_first_deferred_at"]
    assert beta.first_blocked_at == before["first_blocked_at"]
    assert dict(beta.result or {}) == before["result"]


def test_repeated_surplus_misses_never_advance_the_exhaustion_caps(
    db_session, monkeypatch
):
    """The consequence of the rule above, over the laps that would expose it.

    Ten passes in which beta is looked at and rejected leave its deferral
    count exactly where the ONE real deferral put it. A per-attempt increment
    would show up here as 12 and would have terminalized the unit.
    """
    from dev_health_ops.sync.budget_guard import BUDGET_MAX_DEFERRALS_DEFAULT
    from dev_health_ops.workers import sync_units

    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 9, "beta": 4}, limit=10
    )
    beta.available_at = datetime.now(timezone.utc) + timedelta(hours=1)
    db_session.flush()

    for _ in range(BUDGET_MAX_DEFERRALS_DEFAULT):
        sync_units.dispatch_sync_run(str(run.id))
        db_session.refresh(beta)
        # Re-arm alpha so every lap really re-runs admission with the same
        # surplus shortfall rather than short-circuiting on an empty run.
        db_session.refresh(alpha)
        alpha.status = SyncRunUnitStatus.PLANNED.value
        alpha.available_at = None
        db_session.flush()

    db_session.refresh(beta)
    assert beta.budget_deferrals == 2
    assert beta.status == SyncRunUnitStatus.RETRYING.value


def test_surplus_admission_leaves_the_episode_columns_alone(db_session, monkeypatch):
    """A successful surplus admission writes availability and nothing else.

    Asserted at the ``enforce_run`` boundary, before ``_claim_units`` runs, so
    the surplus stamp is measured on its own rather than through the claim
    that clears ``first_blocked_at`` immediately afterwards. Clearing the
    budget pair here would be a second interpretation of "the episode ended"
    competing with the SUCCESS stamp that owns it; rewriting ``result`` would
    disarm ``_budget_deferral_exhausted``'s error-category gate for the unit.
    """
    from dev_health_ops.sync.budget_guard import BudgetGuard

    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 1, "beta": 1}, limit=10
    )
    blocked_at = beta.first_blocked_at
    first_deferred_at = beta.budget_first_deferred_at
    now = datetime.now(timezone.utc)

    result = BudgetGuard.enforce_run(
        db_session,
        str(run.id),
        slot_headroom={(str(run.org_id), "github", "medium"): 4},
        now=now,
    )

    db_session.refresh(beta)
    assert result.surplus_admitted_unit_ids == frozenset({str(beta.id)})
    # Due now -- that is the entire effect of the stamp.
    assert beta.available_at is not None
    assert beta.status == SyncRunUnitStatus.RETRYING.value
    # The episode is untouched: not cleared, not advanced.
    assert beta.budget_deferrals == 2
    assert beta.budget_first_deferred_at == first_deferred_at
    assert beta.first_blocked_at == blocked_at
    assert (beta.result or {})["error_category"] == "budget_deferred"


def test_surplus_admitted_unit_clears_the_aggregate_clock_at_the_claim(
    db_session, monkeypatch
):
    """...and the episode does still end, in the place that already owns it.

    The claim is what stops the aggregate blocked clock, exactly as it does
    for any other dispatched unit. Pinned so "the surplus stamp leaves the
    columns alone" cannot quietly become "a surplus-admitted unit keeps a
    stale first_blocked_at forever".
    """
    from dev_health_ops.workers import sync_units

    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 1, "beta": 1}, limit=10
    )
    assert beta.first_blocked_at is not None

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(beta)
    assert beta.status == SyncRunUnitStatus.DISPATCHING.value
    assert beta.first_blocked_at is None
    # The budget pair is the SUCCESS stamp's to clear, not the claim's, so it
    # is still the history of a real episode at this point.
    assert beta.budget_deferrals == 2


def test_surplus_stamp_writes_no_deferral_lifecycle_column(db_session):
    """Source-derived guard for the counter semantics above.

    The behavioural tests show the columns unchanged for the fixtures they
    run; this shows the stamp CANNOT change them for any fixture. The column
    set is derived from the live model, so a lifecycle column added later is
    covered without anyone remembering to list it here -- and a derivation
    that finds nothing fails rather than reading as clean.
    """
    import ast
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[1]
    module = ast.parse(
        (repo_root / "src/dev_health_ops/sync/budget_guard.py").read_text()
    )
    model_columns = {
        statement.target.id
        for node in ast.walk(
            ast.parse(
                (repo_root / "src/dev_health_ops/models/integrations.py").read_text()
            )
        )
        if isinstance(node, ast.ClassDef) and node.name == "SyncRunUnit"
        for statement in node.body
        if isinstance(statement, ast.AnnAssign)
        and isinstance(statement.target, ast.Name)
    }
    lifecycle_columns = {
        name
        for name in model_columns
        if name.startswith("rate_limit_") or name.startswith("budget_")
    } | {"first_blocked_at"}
    assert len(lifecycle_columns) >= 5, lifecycle_columns

    stamps = [
        node
        for function in ast.walk(module)
        if isinstance(function, ast.FunctionDef)
        and function.name == "_admit_unit_from_surplus"
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "values"
    ]
    assert len(stamps) == 1, (
        f"expected exactly one values(...) stamp in _admit_unit_from_surplus, "
        f"found {len(stamps)} -- the stamp this guard measures moved."
    )
    assigned = {kw.arg for kw in stamps[0].keywords if kw.arg is not None}
    assert assigned == {"available_at", "updated_at"}, assigned
    assert not (assigned & lifecycle_columns), sorted(assigned & lifecycle_columns)
    # No status re-assignment: a surplus admission does not change what the
    # unit IS, only when it is due. (It is also what keeps this stamp out of
    # the per-episode "every RETRYING stamp assigns every pair" rule, whose
    # semantics would be wrong here.)
    assert "status" not in assigned


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("older_first", [True, False])
def test_longest_deferred_unit_wins_a_surplus_that_only_fits_one(
    db_session, monkeypatch, older_first
):
    """Surplus must make the exhaustion path RARER, not merely rearrange who
    reaches it, so the unit closest to its caps is served first.

    Two deferred units cost 3 each against a leftover of 4: exactly one can be
    admitted. Parametrized over which was created (and therefore id-ordered)
    first, so passing cannot be an accident of row order.
    """
    from dev_health_ops.sync import budget_guard
    from dev_health_ops.sync.budget_guard import BudgetGuard

    run, alpha = _seed_run(db_session)
    alpha.dataset_key = "alpha"
    now = datetime.now(timezone.utc)
    names = ["old", "new"] if older_first else ["new", "old"]
    deferred: dict[str, SyncRunUnit] = {}
    for name in names:
        unit = _sibling_unit(
            run, alpha, dataset_key=name, processor_flags={"sync_git": True}
        )
        db_session.add(unit)
        db_session.flush()
        deferred[name] = unit
    run.total_units = 3
    _mark_budget_deferred(deferred["old"], now=now, deferred_ago=timedelta(hours=3))
    _mark_budget_deferred(deferred["new"], now=now, deferred_ago=timedelta(minutes=1))
    db_session.flush()

    costs = {"alpha": 6, "old": 3, "new": 3}
    monkeypatch.setattr(
        budget_guard,
        "estimate_provider_budget",
        lambda ctx: _estimate_of(run.org_id, costs[ctx.dataset_key]),
    )
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 10}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = BudgetGuard.enforce_run(
        db_session,
        str(run.id),
        slot_headroom={(str(run.org_id), "github", "medium"): 4},
        now=now,
    )

    assert result.surplus_admitted_unit_ids == frozenset({str(deferred["old"].id)}), (
        "the surplus went to the more recently deferred unit; the one nearest "
        "its exhaustion caps was passed over"
    )


# ---------------------------------------------------------------------------
# Surplus relaxes budget admission and NOTHING else
# ---------------------------------------------------------------------------


def test_surplus_never_breaches_the_per_bucket_concurrency_cap(db_session, monkeypatch):
    """Budget headroom is not dispatch headroom.

    The bucket has 9 units of budget going spare, and the concurrency cap is
    1 -- consumed by alpha. beta must stay deferred: admitting it would put
    two units in a bucket the concurrency guard sized for one, and the
    concurrency guard never saw beta at all (a not-yet-due RETRYING unit is
    not in its candidate set).
    """
    from dev_health_ops.workers import sync_units

    monkeypatch.setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 1, "beta": 1}, limit=10
    )
    deferred_until = beta.available_at

    result = sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(alpha)
    db_session.refresh(beta)
    assert result == {"status": "dispatched", "queued_units": 1}
    assert alpha.status == SyncRunUnitStatus.DISPATCHING.value
    assert beta.status == SyncRunUnitStatus.RETRYING.value
    assert beta.available_at == deferred_until


def test_surplus_never_admits_into_an_active_provider_cooldown(db_session, monkeypatch):
    """A cooldown is a statement about the provider, not about the budget.

    beta fits the surplus comfortably, but its route family is cooling down
    from a sibling's 429. Spare budget is no reason to shorten that wait --
    the unit stays deferred, and is not re-stamped by the surplus phase
    either.
    """
    from dev_health_ops.workers import sync_units

    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 1, "beta": 1}, limit=10
    )
    now = datetime.now(timezone.utc)
    deferred_until = beta.available_at
    db_session.add(
        _observation(
            run,
            alpha,
            route_family="git",
            dimension="rest_core",
            reset_at=now + timedelta(minutes=10),
            observed_at=now - timedelta(seconds=5),
        )
    )
    db_session.flush()

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(beta)
    assert beta.status == SyncRunUnitStatus.RETRYING.value
    assert beta.available_at == deferred_until
    assert (beta.result or {})["error_category"] == "budget_deferred"


def test_surplus_does_not_revive_a_budget_exhausted_unit(db_session, monkeypatch):
    """Exhaustion is a deliberate, operator-visible verdict that the
    configuration is wrong (CHAOS-3412), not a queue state spare capacity may
    undo. A FAILED unit is not a surplus candidate however much budget is
    free -- reviving it would relax the exhaustion outcome, and surplus
    relaxes budget admission only.

    The end-to-end assertion alone cannot fail: three independent conditions
    keep a FAILED unit out (the candidate query's status filter, the
    error-category filter, and the promotion CAS's own ``status == RETRYING``
    predicate), so removing any one -- or even the first two together --
    leaves it passing. Asserting the SELECTION directly is what actually
    measures the outermost barrier; the end-to-end assertion below then pins
    the outcome an operator sees.
    """
    from dev_health_ops.sync.budget_guard import _surplus_retry_candidates
    from dev_health_ops.workers import sync_units

    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 1, "beta": 1}, limit=10
    )
    beta.status = SyncRunUnitStatus.FAILED.value
    beta.error = "sync budget deferral exhausted after 10 deferrals"
    beta.result = {
        "error_category": "budget_deferral_exhausted",
        "budget_deferrals": 10,
    }
    db_session.flush()

    candidates = _surplus_retry_candidates(
        db_session,
        str(run.id),
        ignored_unit_ids=set(),
        slot_headroom={(str(run.org_id), "github", "medium"): 4},
        now=datetime.now(timezone.utc),
    )
    assert [str(unit.id) for unit in candidates] == [], (
        "a budget-exhausted unit was offered to the surplus phase; exhaustion "
        "is a verdict, not a queue state spare capacity may undo"
    )

    sync_units.dispatch_sync_run(str(run.id))

    db_session.refresh(beta)
    assert beta.status == SyncRunUnitStatus.FAILED.value
    assert (beta.result or {})["error_category"] == "budget_deferral_exhausted"


def test_surplus_is_disabled_without_concurrency_headroom(db_session, monkeypatch):
    """Fail closed. ``slot_headroom`` is the only evidence the surplus phase
    has that admitting a unit will not breach the concurrency cap; with none,
    it admits nothing rather than guessing.
    """
    from dev_health_ops.sync.budget_guard import BudgetGuard

    run, alpha, beta = _two_unit_run(
        db_session, monkeypatch, costs={"alpha": 1, "beta": 1}, limit=10
    )
    deferred_until = beta.available_at

    result = BudgetGuard.enforce_run(db_session, str(run.id))

    db_session.refresh(beta)
    assert result.surplus_admitted_unit_ids == frozenset()
    assert beta.available_at == deferred_until


# ---------------------------------------------------------------------------
# Bounded work, stated out loud
# ---------------------------------------------------------------------------


def test_candidate_cap_is_reported_rather_than_silently_applied(
    db_session, monkeypatch, caplog
):
    """A cap that truncates without saying so reads as "surplus considered
    everything and nothing else fitted", which is a different fact.
    """
    from dev_health_ops.sync import budget_guard
    from dev_health_ops.sync.budget_guard import BudgetGuard

    run, alpha = _seed_run(db_session)
    alpha.dataset_key = "alpha"
    now = datetime.now(timezone.utc)
    for index, name in enumerate(("old", "new")):
        unit = _sibling_unit(
            run, alpha, dataset_key=name, processor_flags={"sync_git": True}
        )
        db_session.add(unit)
        db_session.flush()
        _mark_budget_deferred(unit, now=now, deferred_ago=timedelta(hours=3 - index))
    run.total_units = 3
    db_session.flush()

    costs = {"alpha": 1, "old": 1, "new": 1}
    monkeypatch.setattr(
        budget_guard,
        "estimate_provider_budget",
        lambda ctx: _estimate_of(run.org_id, costs[ctx.dataset_key]),
    )
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 100}')
    monkeypatch.setenv("SYNC_BUDGET_SURPLUS_MAX_CANDIDATES", "1")

    with caplog.at_level(logging.INFO, logger="dev_health_ops.sync.budget_guard"):
        result = BudgetGuard.enforce_run(
            db_session,
            str(run.id),
            slot_headroom={(str(run.org_id), "github", "medium"): 4},
            now=now,
        )

    assert len(result.surplus_admitted_unit_ids) == 1
    truncations = [
        record
        for record in caplog.records
        if record.msg == "dispatch_sync_run.budget_surplus_candidates_truncated"
    ]
    assert len(truncations) == 1, "the cap dropped a candidate without saying so"
    assert truncations[0].deferred_units == 2
    assert truncations[0].considered_units == 1


# ---------------------------------------------------------------------------
# Postgres: the dialect the ordering and JSON filtering actually ship against
# ---------------------------------------------------------------------------

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"


def _require_postgres_test_url():
    """The configured Postgres, forced onto the SYNC driver.

    ``BudgetGuard`` runs inside ``get_postgres_session_sync``; driving it from
    an async driver is what made an earlier PostgreSQL-gated test abort on
    ``MissingGreenlet`` long before it reached the code it claimed to cover
    (CHAOS-3450). A skip is only acceptable locally -- in CI an unconfigured
    URI FAILS, because a test that silently does not run reads as coverage.
    """
    uri = os.getenv(_POSTGRES_URI_ENV)
    if not uri:
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            pytest.fail(f"{_POSTGRES_URI_ENV} must be configured for PostgreSQL tests")
        pytest.skip(f"requires {_POSTGRES_URI_ENV}")
    return make_url(uri).set(drivername="postgresql+psycopg2")


@pytest.fixture
def postgres_session():
    """A throwaway DATABASE, not a schema in the configured one.

    A schema sharing ``search_path`` with ``public`` is not isolation:
    ``create_all`` finds the existing tables through the path, skips creating
    them, and every write lands in the developer's real database instead.
    Mirrors ``test_0066_celery_river_cutover_postgres``'s harness.
    """
    url = _require_postgres_test_url()
    database = f"chaos3465_{uuid.uuid4().hex}"
    admin = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    engine = None
    created = False
    try:
        with admin.connect() as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database}"')
            created = True
        engine = create_engine(url.set(database=database))
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            seed_sync_dispatch_transport_routes(session)
            yield session
    finally:
        if engine is not None:
            engine.dispose()
        if created:
            with admin.connect() as connection:
                connection.exec_driver_sql(f'DROP DATABASE "{database}"')
        admin.dispose()


def test_surplus_ordering_and_selection_hold_on_postgres(postgres_session, monkeypatch):
    """The same "longest-deferred wins a surplus that fits one" assertion, on
    the backend production runs.

    Two things this exercises that SQLite cannot: the advisory locks the
    surplus candidates' budget keys join, and the fact that candidate
    selection and ordering do not depend on backend-specific NULL ordering or
    JSON operators (both are done in Python for exactly that reason). A
    dialect divergence here would mean the surplus served a different unit in
    production than in every unit test.
    """
    from dev_health_ops.sync import budget_guard
    from dev_health_ops.sync.budget_guard import BudgetGuard

    run, alpha = _seed_run(postgres_session)
    alpha.dataset_key = "alpha"
    now = datetime.now(timezone.utc)
    deferred: dict[str, SyncRunUnit] = {}
    for name in ("new", "old"):
        unit = _sibling_unit(
            run, alpha, dataset_key=name, processor_flags={"sync_git": True}
        )
        postgres_session.add(unit)
        postgres_session.flush()
        deferred[name] = unit
    run.total_units = 3
    _mark_budget_deferred(deferred["old"], now=now, deferred_ago=timedelta(hours=3))
    _mark_budget_deferred(deferred["new"], now=now, deferred_ago=timedelta(minutes=1))
    postgres_session.flush()

    costs = {"alpha": 6, "old": 3, "new": 3}
    monkeypatch.setattr(
        budget_guard,
        "estimate_provider_budget",
        lambda ctx: _estimate_of(run.org_id, costs[ctx.dataset_key]),
    )
    monkeypatch.setenv("SYNC_BUDGET_BUCKET_LIMITS", '{"github:rest_core": 10}')
    monkeypatch.setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", "0")

    result = BudgetGuard.enforce_run(
        postgres_session,
        str(run.id),
        slot_headroom={(str(run.org_id), "github", "medium"): 4},
        now=now,
    )

    assert result.surplus_admitted_unit_ids == frozenset({str(deferred["old"].id)})
    postgres_session.refresh(deferred["new"])
    assert deferred["new"].budget_deferrals == 2
