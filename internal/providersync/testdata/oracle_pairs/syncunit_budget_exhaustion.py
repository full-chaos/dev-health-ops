"""sync-unit budget-deferral EXHAUSTION oracle pair (CHAOS-3427).

Registers "syncunit/budget/exhaustion": given one unit's budget episode
state -- ``budget_deferrals``, ``budget_first_deferred_at`` and the most
recently recorded ``result.error_category`` -- decide whether
``sync/budget_guard.py::_budget_deferral_exhausted`` would terminalize it.

WHY THIS PAIR EXISTS. The sync-unit state machine had ZERO oracle coverage
before this: 55 pairs, none touching it. That matters more than it sounds,
because the Go worker and the Python guard SHARE the ``sync_run_units`` row
and each writes columns the other reads. CHAOS-3412 landed the exhaustion
path in Python only; Go writes non-terminal stamps that must leave the
episode columns in a state this predicate reads correctly. Nothing forced
the two readings to agree until now.

WHAT IS LIVE. ``_budget_deferral_exhausted`` itself -- the real, unmodified
function from src/dev_health_ops/sync/budget_guard.py -- runs here. Only
budget_guard's module-level IMPORTS are stubbed (see
``_install_budget_guard_imports``), and none of them participate in the
predicate: it reads three attributes off the unit, two env-backed caps, and
one frozenset of accepted categories. Editing the predicate changes what
this oracle observes, with no separate pin to keep in sync.

TWO CLAUSES IT PINS, BOTH LOAD-BEARING FOR THE GO SIDE:

  1. A unit with NO budget history (deferrals 0 and no first-deferred stamp)
     is never exhausted. This is what makes Go's episode CLEARS
     (completeUnitSQL, markExpiredLeaseRetryingSQL) sufficient: a cleared
     unit cannot be terminalized on a resolved episode's counters.

  2. The DEFENCE-IN-DEPTH category gate. Stale nonzero counters carrying a
     NON-budget ``error_category`` are refused. That is precisely what makes
     Go's existing stamps safe today -- both OVERWRITE the category with
     their own cause -- and it is why a Go stamp that PRESERVED a prior
     category is a forbidden pattern rather than a style choice. The
     ``go_stamp_*`` cases below feed the exact post-stamp states Go's SQL
     produces through this predicate, so the two halves are measured
     together rather than asserted separately and hoped about.

REQUIREMENT, not a caveat: any Go test exercising this pair MUST run with
``go test -count=1`` (i.e. through ``ci/check_go.sh live-python-oracles``).
budget_guard.py lives outside internal/providersync/testdata/, so
``//go:embed`` cannot reach it and a bare ``go test`` can return a stale
cached PASS for a real regression in the live function.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_BUDGET_GUARD_SOURCE = REPO_ROOT / "src/dev_health_ops/sync/budget_guard.py"
_THIS_FILE = pathlib.Path(__file__)

#: The instant every case is evaluated at. Fixed so the wall-clock cap is a
#: function of the case's own offset and nothing else.
_NOW = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)


def _stub(name: str, **values: object) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    for key, value in values.items():
        setattr(module, key, value)
    sys.modules[name] = module


def _install_budget_guard_imports() -> None:
    """Satisfy budget_guard.py's module-level imports without dragging in
    SQLAlchemy, Celery bootstrap or the provider budget estimators.

    The hosted go-quality image is a no-deps Python: standard library plus
    the handful of packages provider code uses directly. budget_guard.py
    imports ``sqlalchemy`` and two worker modules at module level purely for
    code paths the exhaustion predicate never touches, so stubbing them is
    what lets the REAL predicate run there. Nothing stubbed here is reachable
    from ``_budget_deferral_exhausted``; if that ever stops being true the
    stub raises rather than silently returning a plausible value.
    """
    install_minimal_oracle_imports()

    def _unreachable(*_args: object, **_kwargs: object) -> object:
        raise AssertionError(
            "the budget exhaustion oracle reached a stubbed dependency; the "
            "predicate under test must not depend on SQLAlchemy or the worker "
            "bootstrap. Re-check what changed before trusting this oracle."
        )

    # install_minimal_oracle_imports gives dev_health_ops.models a real
    # namespace __path__ (so SUBMODULE imports stay live) but no aggregate
    # attributes, and budget_guard imports three names off the package
    # itself. They are ORM/enum types used only for annotations and for code
    # paths the predicate never enters, so placeholders keep the import
    # satisfiable without pulling in SQLAlchemy's declarative machinery.
    models = sys.modules["dev_health_ops.models"]
    for name in ("ProviderRateLimitObservation", "SyncRunUnit", "SyncRunUnitStatus"):
        if not hasattr(models, name):
            setattr(models, name, type(name, (), {}))

    _stub(
        "sqlalchemy",
        func=_unreachable,
        or_=_unreachable,
        text=_unreachable,
        update=_unreachable,
    )
    _stub(
        "dev_health_ops.sync.budget",
        BudgetEstimate=object,
        estimate_provider_budget=_unreachable,
    )
    _stub(
        "dev_health_ops.workers.rate_limit_defer",
        RATE_LIMIT_DEFAULT_COUNTDOWN_SECONDS=60,
        RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS=3600,
        plan_rate_limit_deferral=_unreachable,
    )
    _stub("dev_health_ops.workers.sync_bootstrap", SyncTaskBootstrap=object)


def _load_budget_guard() -> Any:
    if "dev_health_ops.sync.budget_guard" in sys.modules:
        return sys.modules["dev_health_ops.sync.budget_guard"]
    _install_budget_guard_imports()
    spec = importlib.util.spec_from_file_location(
        "dev_health_ops.sync.budget_guard", _BUDGET_GUARD_SOURCE
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {_BUDGET_GUARD_SOURCE}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    origin = pathlib.Path(module.__file__ or "").resolve(strict=True)
    if origin != _BUDGET_GUARD_SOURCE.resolve(strict=True):
        raise RuntimeError(f"oracle imported {origin}, expected {_BUDGET_GUARD_SOURCE}")
    return module


def _decide(case: dict[str, Any]) -> dict[str, Any]:
    budget_guard = _load_budget_guard()

    first_deferred_at = None
    offset = case.get("first_deferred_seconds_ago")
    if offset is not None:
        first_deferred_at = _NOW - timedelta(seconds=int(offset))
    result: dict[str, Any] | None = None
    category = case.get("error_category")
    if category is not None:
        # The whole document, not just the category: Go's release-for-retry
        # merges into the existing result, so the predicate must keep reading
        # the CURRENT category out of a document that carries other keys.
        result = {"error_category": category, "go_effect_ledger_v1": {"kept": True}}

    unit = SimpleNamespace(
        budget_deferrals=case["budget_deferrals"],
        budget_first_deferred_at=first_deferred_at,
        result=result,
    )
    return {
        "exhausted": bool(budget_guard._budget_deferral_exhausted(unit, now=_NOW)),
    }


def _reflected_fields() -> frozenset[str]:
    """The complete field set _decide's own return literal can emit. Derived
    from this file's AST rather than hand-listed, so a field added to the
    decision cannot be silently left out of the comparison."""
    return dict_literal_keys(_THIS_FILE.read_text(), "_decide", (RETURN_LITERAL,))


oracle_registry.register(
    oracle_registry.PairSpec(
        id="syncunit/budget/exhaustion",
        build_row=_decide,
        reflected_fields=_reflected_fields,
        excluded_fields={},
    )
)
