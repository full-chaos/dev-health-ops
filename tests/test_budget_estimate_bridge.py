"""Tests for batch_estimate_provider_budget_for_units (CHAOS-4175 family 3).

This is the one function the /dispatch-budget-estimate bridge endpoint
wraps: bootstrap + estimate a BATCH of units in one pass, degrading a
per-unit failure to an empty estimate tuple rather than failing the whole
batch -- Python's OWN existing behavior in
enforce_run/observe_run/_active_budget_consumption's identical try/except,
preserved here rather than invented for the bridge.
"""

from __future__ import annotations

import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models import Base
from dev_health_ops.sync.budget_guard import batch_estimate_provider_budget_for_units
from tests._helpers import seed_sync_dispatch_transport_routes
from tests.test_sync_units import _seed_run


def _db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    seed_sync_dispatch_transport_routes(session)
    return session


def test_batch_estimate_returns_a_result_for_a_real_bootstrappable_unit() -> None:
    session = _db_session()
    run, unit = _seed_run(session)

    results = batch_estimate_provider_budget_for_units(
        session, str(run.id), [str(unit.id)]
    )

    assert set(results) == {str(unit.id)}
    # Not asserting a specific estimate shape here -- that is
    # GitHubBudgetEstimator's own contract, covered by
    # tests/providers/test_github_budget.py. This only pins that a real,
    # bootstrappable unit gets SOME tuple back (possibly empty, if the
    # estimator itself has nothing to report for this fixture's window),
    # not that it silently disappears from the result dict.
    assert isinstance(results[str(unit.id)], tuple)


def test_batch_estimate_degrades_an_unbootstrappable_unit_to_an_empty_tuple_without_failing_the_batch() -> (
    None
):
    """The core contract this function exists to preserve: one bad unit id
    must not take down the whole batch, and a GOOD sibling unit's estimate
    must still come back correctly -- matching enforce_run's own
    try/except/continue precedent exactly (budget_guard.py's existing
    per-unit degradation, not invented for this bridge).
    """
    session = _db_session()
    run, unit = _seed_run(session)
    missing_unit_id = str(uuid.uuid4())

    results = batch_estimate_provider_budget_for_units(
        session, str(run.id), [str(unit.id), missing_unit_id]
    )

    assert results[missing_unit_id] == ()
    assert set(results) == {str(unit.id), missing_unit_id}
    assert isinstance(results[str(unit.id)], tuple)


def test_batch_estimate_returns_an_empty_dict_for_an_empty_unit_list() -> None:
    session = _db_session()
    run, _unit = _seed_run(session)

    results = batch_estimate_provider_budget_for_units(session, str(run.id), [])

    assert results == {}
