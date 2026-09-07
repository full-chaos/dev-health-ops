"""CHAOS-5349: the never-shrink ledger for Python deleted from GO-SERVED GraphQL paths.

This is the QUERY-TIME counterpart to
``tests/api/internal/test_remaining_family_python_deletion_structural_guard.py``
(``metrics.remaining.*`` worker families) and
``tests/metrics/test_job_daily_skip_families_structural_guard.py``
(``job_daily.py``-resident families). Same contract as both: entries are added
one deletion PR at a time, and are NEVER removed.

# Why a query-time resolver cannot be deleted the way a worker family can

The two worker ledgers exist because chris's ruling is that once a family's
native Go executor is on main with no Python fallback, the Python compute is
DELETED rather than skip-gated. A GraphQL resolver looks like the same shape and
is not, for three reasons that were measured on this branch rather than assumed:

1. THE SDL IS A PYTHON ARTIFACT. ``contracts/graphql/v1/schema.graphql`` is
   EXPORTED from the Strawberry schema and pinned byte-for-byte by
   ``tests/api/graphql/test_schema_sdl_pinned.py``. Executed on this branch:
   removing the three ``@strawberry.field`` registrations from
   ``api/graphql/schema.py`` drops 116 lines from the export and moves the
   file's sha256 from ``29d509cd414cd957a7bcd73a1c0e78a07f17dd8a8794893233954aaa87241b88``
   to ``5e2150ef6b2b44975d83198b20d5b58cf72bca5a12cc51b9aed7304dad06545f``.

2. THAT FILE IS HALF THE ROUTING KEY. ``routeswitch.PostgresSwitch`` looks up
   ``go_api_routing_state`` by the 3-tuple (schema_digest, document_digest,
   selected_operation) (``cmd/query-api/internal/routeswitch/postgres_switch.go:46,122``),
   and the Python dispatcher computes the identical digest over the identical
   file (``go_api_dispatcher.py:121``). Changing it invalidates the routing row
   of EVERY registered operation at once, not just the ones being deleted. It is
   also gqlgen's input SDL, so the Go models for those types would vanish with
   it.

3. THE PYTHON RESOLVER IS THE FALLBACK. Routing is fail-closed: a digest miss, a
   missing registry row, a Go 5xx and a Go timeout all fall back to Python.

So the Strawberry FIELD DECLARATIONS and their input/output model classes are
load-bearing infrastructure, not leftovers, and this ledger deliberately does
NOT cover them. What it covers is Python that is genuinely dead.

# What this ledger currently holds

Only ``discover_team_scopes``. CHAOS-5336 deleted the capacity-forecast
scheduler; this function was its last survivor and had no caller in ``src/`` at
all. An unbounded sweep of the whole tree found exactly three references: its
own definition, its own test, and a doc comment in
``internal/jobs/metrics/remaining/capacity_native_clickhouse.go`` naming it as
the source of a query the Go worker already ports.

The larger deletion CHAOS-5349 was scoped for -- ``resolvers/capacity.py``,
``resolvers/forecast.py``, ``metrics/forecast.py``, ``metrics/compute_capacity.py``
and the rest of ``metrics/capacity_queries.py`` -- is NOT here, and must not be
added until the fallback question above is ruled on: with the routing rows
enabled those modules are only reached on the fallback paths, so deleting them
means those three operations lose their fallback entirely. That is a live
behaviour change and a separate decision from "enable the routing rows".

Widen this ledger as each subsequent deletion lands. Do not remove entries.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

CAPACITY_QUERIES_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "metrics" / "capacity_queries.py"
)

# module path -> the symbols that must no longer be defined in it.
#
# Symbol-level rather than file-level, because unlike the worker families this
# ticket's first deletion is a dead FUNCTION inside a module whose other two
# functions are still live and still serve the Python capacity resolver.
DELETED_GO_SERVED_RESOLVER_SYMBOLS: dict[Path, frozenset[str]] = {
    # CHAOS-5349: the capacity-forecast scheduler's last survivor. Its only
    # caller was run_capacity_forecast, deleted by CHAOS-5336 along with
    # job_capacity.py; nothing in src/ has called it since.
    CAPACITY_QUERIES_SOURCE: frozenset({"discover_team_scopes"}),
}

# module path -> a module that must not exist on disk AT ALL.
#
# Deliberately EMPTY today. See this module's docstring: the resolver and
# compute modules CHAOS-5349 was scoped to delete are blocked on a ruling about
# the fail-closed fallback, and an entry added here before that ruling would
# assert a deletion that has not been agreed.
DELETED_GO_SERVED_RESOLVER_MODULES: dict[str, Path] = {}

# The Strawberry declarations that MUST survive every deletion in this family,
# because they are what produces the SDL that both planes digest for routing.
#
# This is the ledger's other half, and the more important one: it is a
# never-DELETE list, not a never-restore list. A future PR that trims "unused"
# Python here would silently change the schema digest and disable every
# registered operation.
SDL_LOAD_BEARING_SOURCES: tuple[Path, ...] = (
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "schema.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "models" / "inputs.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "models" / "outputs.py",
)

SDL_LOAD_BEARING_SYMBOLS: dict[str, frozenset[str]] = {
    "schema.py": frozenset(
        {"capacity_forecast", "capacity_forecasts", "throughput_forecast"}
    ),
    "inputs.py": frozenset(
        {
            "CapacityForecastInput",
            "CapacityForecastFilterInput",
            "ThroughputForecastInput",
        }
    ),
    "outputs.py": frozenset(
        {
            "CapacityForecast",
            "CapacityForecastEdge",
            "CapacityForecastConnection",
            "ThroughputForecast",
            "ThroughputRollingWindow",
            "ThroughputRiskOverlay",
            "ThroughputStaleWip",
            "ThroughputEstimateCoverage",
        }
    ),
}


def _defined_names(source: Path) -> set[str]:
    """Every top-level and class-level name defined in ``source``.

    Parsed with ``ast`` rather than imported: importing the GraphQL schema pulls
    in Strawberry, the resolvers and a database layer, and this guard must be
    able to fail cleanly on a tree where one of those is exactly what is broken.
    A substring search over the text would be the other obvious shortcut and is
    worse -- it cannot tell a definition from a mention in a docstring, which is
    precisely the difference this file is asserting.
    """
    tree = ast.parse(source.read_text(encoding="utf-8"))
    names: set[str] = set()

    def visit(node: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(child.name)
                visit(child)

    visit(tree)
    return names


def test_deleted_symbols_are_not_redefined() -> None:
    """A deleted symbol must stay deleted, in the module it was deleted from."""
    for source, deleted in DELETED_GO_SERVED_RESOLVER_SYMBOLS.items():
        assert source.exists(), (
            f"{source} no longer exists. If it was deleted deliberately, move its "
            "entry from DELETED_GO_SERVED_RESOLVER_SYMBOLS to "
            "DELETED_GO_SERVED_RESOLVER_MODULES rather than dropping it -- this "
            "ledger never shrinks."
        )
        defined = _defined_names(source)
        restored = sorted(deleted & defined)
        assert not restored, (
            f"{', '.join(restored)} is defined again in {source.name}. These were "
            "deleted as dead code by CHAOS-5349 after an unbounded sweep proved "
            "zero callers. If a real caller now needs one, the sweep was wrong or "
            "the requirement is new: say which in the PR, and remove the ledger "
            "entry deliberately rather than as a side effect."
        )


def test_deleted_modules_do_not_exist() -> None:
    """A module in the ledger must not be back on disk."""
    for label, module in DELETED_GO_SERVED_RESOLVER_MODULES.items():
        assert not module.exists(), (
            f"{label}: {module} exists again. It was deleted because query-api "
            "serves its operation natively; restoring it re-creates the "
            "dual-implementation drift the deletion removed."
        )


def test_sdl_load_bearing_declarations_still_exist() -> None:
    """The Strawberry declarations that produce the SDL must NOT be deleted.

    The inverse assertion to the two above, and the reason this file is worth
    reading before writing the next deletion PR. These names are not leftovers
    from a completed port -- they generate
    ``contracts/graphql/v1/schema.graphql``, which is gqlgen's input SDL, web's
    codegen schema, and half of both planes' routing key. Deleting them is not a
    cleanup; it disables every registered Go operation at once.
    """
    for source in SDL_LOAD_BEARING_SOURCES:
        assert source.exists(), (
            f"{source} has been deleted. It defines part of the GraphQL schema "
            "that contracts/graphql/v1/schema.graphql is exported from -- see "
            "this module's docstring for the measured consequence."
        )
        defined = _defined_names(source)
        expected = SDL_LOAD_BEARING_SYMBOLS[source.name]
        missing = sorted(expected - defined)
        assert not missing, (
            f"{source.name} no longer defines {', '.join(missing)}. Removing a "
            "Strawberry field or one of its input/output models changes the "
            "exported SDL and therefore the canonical schema digest, which is "
            "part of go_api_routing_state's primary key -- so it disables the "
            "routing rows of EVERY registered operation, not just this one's. "
            "Measured on CHAOS-5349's branch: dropping these three fields "
            "removed 116 SDL lines and moved the digest from 29d509cd... to "
            "5e2150ef.... If the schema genuinely must change, that is a "
            "schema-change PR with a regenerated pin and a routing-row "
            "migration, not a Python cleanup."
        )
