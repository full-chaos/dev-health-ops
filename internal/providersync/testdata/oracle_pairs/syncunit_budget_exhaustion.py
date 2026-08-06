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

WHAT THIS PAIR IS NOT. It is a PREDICATE-parity check, not a state-machine
oracle. The Go side of the comparison is a hand-written mirror of this
predicate (``budgetDeferralExhausted`` in
internal/providersync/syncunit_budget_exhaustion_oracle_test.go), because Go
owns no budget-admission decision to drive -- its only budget references in
non-test source are the two SQL clears. Nothing here executes a Go producer
or reaches a database. The Go stamps' effect on the columns this predicate
reads is covered by the SQL-string guards and, against real PostgreSQL, by
internal/providersync/budget_episode_integration_test.go and
internal/syncreconciler/lease_repair_integration_test.go; the Go test's
doc comment enumerates that split.

REQUIREMENT, not a caveat: any Go test exercising this pair MUST run with
``go test -count=1`` (i.e. through ``ci/check_go.sh live-python-oracles``).
budget_guard.py lives outside internal/providersync/testdata/, so
``//go:embed`` cannot reach it and a bare ``go test`` can return a stale
cached PASS for a real regression in the live function.
"""

from __future__ import annotations

import ast
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
_MODELS_PACKAGE_INIT = REPO_ROOT / "src/dev_health_ops/models/__init__.py"
_THIS_FILE = pathlib.Path(__file__)

#: The names budget_guard.py imports off ``dev_health_ops.models`` itself and
#: that _install_budget_guard_imports substitutes placeholders for. Every one
#: is checked against the real package before any placeholder is installed --
#: see _assert_models_still_export.
_PLACEHOLDER_MODEL_EXPORTS = (
    "ProviderRateLimitObservation",
    "SyncRunUnit",
    "SyncRunUnitStatus",
)

#: The instant every case is evaluated at. Fixed so the wall-clock cap is a
#: function of the case's own offset and nothing else.
_NOW = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)


def _module_level_bindings(source: str) -> frozenset[str]:
    """Every name ``source`` binds at module level, by AST rather than import.

    Reading the source is the point: importing ``dev_health_ops.models`` for
    real would drag in SQLAlchemy's declarative machinery, which is exactly
    what the placeholders exist to avoid. ``ast`` answers "does this name
    still exist" under the same stock interpreter everything else in this
    directory runs under.

    Descends into ``if``/``try`` bodies (a conditional or optional-dependency
    import still binds the name) but NOT into functions or classes, whose
    locals are not module attributes.
    """

    def collect(body: list[ast.stmt], bound: set[str]) -> None:
        for node in body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    if alias.name == "*":
                        continue
                    bound.add(alias.asname or alias.name.split(".")[0])
            elif isinstance(
                node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
            ):
                bound.add(node.name)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        bound.add(target.id)
            elif isinstance(node, ast.AnnAssign):
                if isinstance(node.target, ast.Name):
                    bound.add(node.target.id)
            elif isinstance(node, ast.If):
                collect(node.body, bound)
                collect(node.orelse, bound)
            elif isinstance(node, ast.Try):
                collect(node.body, bound)
                for handler in node.handlers:
                    collect(handler.body, bound)
                collect(node.orelse, bound)
                collect(node.finalbody, bound)

    bound: set[str] = set()
    collect(ast.parse(source).body, bound)
    return frozenset(bound)


def _assert_models_still_export(names: tuple[str, ...]) -> None:
    """Fail loudly if ``dev_health_ops.models`` no longer exports ``names``.

    WHY THIS PROBE EXISTS. ``install_minimal_oracle_imports`` registers
    ``dev_health_ops.models`` as a namespace package whose ``__path__`` points
    at the real directory -- which means the package's ``__init__.py`` is
    never executed, and the placeholder attributes installed below satisfy
    budget_guard.py's ``from dev_health_ops.models import (...)`` whatever the
    real package does or does not export. A rename or removal of any of these
    three would break production import and leave this oracle green: the
    placeholder IS the drift mask.

    So the export set is verified against the real source before any
    placeholder is installed. A probe that cannot read that source is itself
    a failure -- an unmeasured check must not read as a passing one.
    """
    if not _MODELS_PACKAGE_INIT.is_file():
        raise RuntimeError(
            f"cannot verify model exports: {_MODELS_PACKAGE_INIT} does not "
            "exist. The placeholders installed below would silently satisfy "
            "budget_guard.py's imports against a package that has moved."
        )
    exported = _module_level_bindings(_MODELS_PACKAGE_INIT.read_text())
    missing = [name for name in names if name not in exported]
    if missing:
        raise RuntimeError(
            "dev_health_ops.models no longer exports "
            f"{', '.join(missing)} ({_MODELS_PACKAGE_INIT}). budget_guard.py "
            "imports these off the package itself, so production import is "
            "broken -- but this oracle installs placeholders for exactly "
            "these names and would otherwise stay green. Update "
            "_PLACEHOLDER_MODEL_EXPORTS together with budget_guard.py's "
            "import list; do NOT delete this probe to make the oracle pass."
        )


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
    #
    # The placeholders are also a DRIFT MASK, so the real package's export
    # set is probed first and a rename or removal fails the oracle loudly.
    _assert_models_still_export(_PLACEHOLDER_MODEL_EXPORTS)
    models = sys.modules["dev_health_ops.models"]
    for name in _PLACEHOLDER_MODEL_EXPORTS:
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
