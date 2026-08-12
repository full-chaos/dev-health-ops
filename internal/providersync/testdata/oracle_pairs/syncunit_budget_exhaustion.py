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

A THIRD THING IT PINS, ADDED FOR CHAOS-3465. Surplus retry introduced two new
Python writers of the shared ``sync_run_units`` row, both of them landing
between a budget deferral and the clear that ends the episode. Neither writes
an episode column -- and the whole corpus below silently depends on that, since
every case is an authored snapshot and none would ever be built from a
laundered one. ``_assert_surplus_writes_leave_the_episode_alone`` reads the
live source and enforces it, so the guarantee fails the GO gate rather than
living in a Python code comment. See that function for the argument.

WHAT THIS PAIR IS NOT. It is a PREDICATE-parity check, not a state-machine
oracle. The Go side of the comparison is a hand-written mirror of this
predicate (``budgetDeferralExhausted`` in
internal/providersync/syncunit_budget_exhaustion_oracle_test.go), because Go
owns no intrinsic planner-budget admission decision to drive. Go does own
short-lived HTTP reservation contention, but records it under the distinct
``provider_budget_contention`` category and clears the intrinsic episode pair;
the ``go_stamp_*`` input binds that production stamp to this live predicate.
Nothing here executes a Go producer
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

#: The exhaustion predicates a surplus write must not be able to move. BOTH
#: are listed, not just the one this pair compares: ``_deferral_total_exhausted``
#: reads ``first_blocked_at``, the CHAOS-3412 aggregate clock that no episode
#: reset may move, and a surplus writer that reset it would launder that clock
#: without touching anything ``_budget_deferral_exhausted`` reads. Guarding one
#: predicate's columns and calling the row safe was the gap here.
_EXHAUSTION_PREDICATES = ("_budget_deferral_exhausted", "_deferral_total_exhausted")

#: The columns those predicates read off the unit row. Anything that writes one
#: of these can move an exhaustion verdict, so the corpus below is only as valid
#: as the set of writers that leave them alone.
#:
#: DERIVED, then checked: ``_assert_declared_columns_match_the_predicates``
#: reads the live predicates and fails if this tuple drifts from what they
#: actually read, in either direction. Hand-maintaining it is what let
#: ``first_blocked_at`` go missing, and a hand-maintained list that falls behind
#: a predicate does not announce itself -- the probe just stops covering the
#: column and keeps passing.
_EPISODE_COLUMNS_THE_PREDICATES_READ = (
    "budget_deferrals",
    "budget_first_deferred_at",
    "first_blocked_at",
    "result",
)

#: CHAOS-3465 added two NEW Python writers to the ``sync_run_units`` row the Go
#: worker shares -- surplus retry's promotion and its withdrawal. Both are
#: documented as writing ``available_at`` (plus ``updated_at``) and nothing
#: else, and that is load-bearing for the GO side rather than Python-internal
#: tidiness: budget_guard's own comment says a surplus admission that rewrote
#: ``result`` "would quietly disarm the exhaustion path for that unit", and
#: every ``go_stamp_*`` case in the Go test is built on that path still being
#: armed. Until now the guarantee was a code comment plus Python tests -- the
#: differential could not see it. The probe below makes it a measurement that
#: fails THIS oracle, i.e. the Go gate, on drift.
_SURPLUS_WRITE_PATHS = ("_admit_unit_from_surplus", "_withdraw_surplus_admission")

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


def _row_columns_written_by(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
) -> frozenset[str]:
    """Every unit-row column ``function`` can write.

    Two shapes, because the surplus writers use both and either one alone is
    an incomplete reading: the keyword arguments of a ``.values(...)`` call
    (the SQL UPDATE), and attribute assignment on the in-memory ORM mirror
    (``unit.available_at = now``). This repository already recorded a masked
    mutation against these exact statements -- corrupting the SQL alone left
    the test PASSING because the autoflush rewrote the row from the ORM
    object -- so a probe that read only one of the two would inherit the same
    blind spot.

    ``unit.result["error_category"] = ...`` is counted as a write of
    ``result``: mutating the JSON document in place is the cheapest way to
    disarm the defence-in-depth gate without ever naming the column in a
    ``.values()``.
    """
    written: set[str] = set()
    for node in ast.walk(function):
        if isinstance(node, ast.Call):
            callee = node.func
            if isinstance(callee, ast.Attribute) and callee.attr == "values":
                for keyword in node.keywords:
                    if keyword.arg is not None:
                        written.add(keyword.arg)
            continue
        if isinstance(node, ast.Assign):
            targets: list[ast.expr] = list(node.targets)
        elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
            targets = [node.target]
        else:
            continue
        for target in targets:
            if isinstance(target, ast.Attribute):
                written.add(target.attr)
            elif isinstance(target, ast.Subscript) and isinstance(
                target.value, ast.Attribute
            ):
                written.add(target.value.attr)
    return frozenset(written)


def _unit_columns_read_by(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
) -> frozenset[str]:
    """Every attribute ``function`` reads off its unit argument.

    Keyed on the function's OWN first parameter name rather than a hardcoded
    ``unit``, so a rename in budget_guard.py cannot silently make this return
    the empty set -- which would read as "this predicate touches no columns"
    and pass.
    """
    parameters = function.args.posonlyargs + function.args.args
    if not parameters:
        raise RuntimeError(
            f"{function.name} takes no positional unit argument; the column "
            "derivation below cannot identify what it reads."
        )
    unit_name = parameters[0].arg
    return frozenset(
        node.attr
        for node in ast.walk(function)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == unit_name
    )


def _assert_declared_columns_match_the_predicates(module: ast.Module) -> None:
    """Keep ``_EPISODE_COLUMNS_THE_PREDICATES_READ`` honest against the source.

    A hand-maintained column list is exactly the kind of thing that falls
    behind and says nothing: the probe simply stops covering the new column
    and keeps reporting success. So the list is checked against what the live
    predicates read, and drift in EITHER direction fails -- a column added to
    a predicate and not to the list leaves it unguarded, and a column in the
    list that no predicate reads makes the guard look wider than it is.
    """
    functions = {
        node.name: node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    read: set[str] = set()
    for name in _EXHAUSTION_PREDICATES:
        function = functions.get(name)
        if function is None:
            raise RuntimeError(
                f"{name} no longer exists in {_BUDGET_GUARD_SOURCE}. It is an "
                "exhaustion predicate whose input columns this probe derives; "
                "update _EXHAUSTION_PREDICATES rather than letting the "
                "derivation quietly narrow."
            )
        read |= _unit_columns_read_by(function)
    declared = frozenset(_EPISODE_COLUMNS_THE_PREDICATES_READ)
    if declared != read:
        raise RuntimeError(
            "_EPISODE_COLUMNS_THE_PREDICATES_READ has drifted from the live "
            f"predicates {list(_EXHAUSTION_PREDICATES)}. Declared "
            f"{sorted(declared)}, they read {sorted(read)}. Missing entries "
            f"({sorted(read - declared)}) are columns a surplus write could "
            "move unobserved; extra entries "
            f"({sorted(declared - read)}) make this guard look wider than it "
            "is."
        )


def _assert_surplus_writes_leave_the_episode_alone() -> None:
    """Fail loudly if a CHAOS-3465 surplus write path touches an episode column.

    WHY THIS PROBE EXISTS, in the Go test's terms. The ``go_stamp_*`` cases
    prove that Go's own non-terminal stamps cannot make stale budget counters
    terminalization-eligible, because each stamp overwrites
    ``result.error_category`` with its own cause. That argument is about the
    LAST recorded cause, so it holds only while every other writer of the row
    leaves that document alone between the deferral and the clear. CHAOS-3465
    put two new writers in exactly that window. A surplus promotion that reset
    ``budget_first_deferred_at`` would launder the wall-clock cap; one that
    rewrote ``result`` would disarm the category gate outright -- and in both
    cases every case below would keep passing, because the inputs are authored
    snapshots and no case would ever be built from the laundered state.

    A promotion that reset ``first_blocked_at`` is the same defect one level
    up: that is CHAOS-3412's AGGREGATE clock, the outer bound no per-episode
    reset may move, and it is read by ``_deferral_total_exhausted`` rather than
    by the predicate this pair compares. Guarding only the compared predicate's
    columns left it open -- so the column set is derived from BOTH predicates,
    not from the one that happens to be differenced here.

    So the guarantee is measured against the live source instead of trusted:
    the declared column set must still match what the predicates read, the two
    write paths must exist, and neither may write any of those columns.

    An empty ``_SURPLUS_WRITE_PATHS`` is itself a failure. A probe with
    nothing to check reads as a passing one, which is the state this whole
    file exists to refuse.
    """
    if not _SURPLUS_WRITE_PATHS:
        raise RuntimeError(
            "_SURPLUS_WRITE_PATHS is empty: this probe would measure nothing "
            "and report success. Name the surplus write paths, or delete the "
            "probe deliberately -- do not leave it vacuous."
        )
    module = ast.parse(_BUDGET_GUARD_SOURCE.read_text())
    _assert_declared_columns_match_the_predicates(module)
    functions = {
        node.name: node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    for name in _SURPLUS_WRITE_PATHS:
        function = functions.get(name)
        if function is None:
            raise RuntimeError(
                f"{name} no longer exists in {_BUDGET_GUARD_SOURCE}. It is a "
                "CHAOS-3465 writer of the sync_run_units row the Go worker "
                "shares; if it was renamed, update _SURPLUS_WRITE_PATHS so "
                "the probe keeps measuring the real write set. A rename must "
                "not silently retire this check."
            )
        touched = sorted(
            _row_columns_written_by(function).intersection(
                _EPISODE_COLUMNS_THE_PREDICATES_READ
            )
        )
        if touched:
            raise RuntimeError(
                f"{name} writes {', '.join(touched)} -- column(s) "
                f"{list(_EXHAUSTION_PREDICATES)} read. A surplus promotion or "
                "withdrawal that moves the budget episode changes an "
                "exhaustion verdict for a row the Go worker also stamps, and "
                "every case in this pair is authored from states that assume "
                "it does not. Either the write is wrong, or this oracle needs "
                "cases built from the new post-surplus state before the "
                "column list is relaxed."
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
    # Runs before the module is trusted, for the same reason
    # _assert_models_still_export does: a precondition checked after the fact
    # is a precondition that already let the cases run.
    _assert_surplus_writes_leave_the_episode_alone()
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
