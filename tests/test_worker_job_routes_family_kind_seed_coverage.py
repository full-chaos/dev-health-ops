"""Static (no-Postgres) proof that every remaining-metrics family's job kind
has a worker_job_routes seed row somewhere in the Alembic migration set.

CHAOS-5007 companion to
test_worker_job_routes_registry_coverage_postgres_migration.py: that test
proves the same fact by actually RUNNING every migration against a real
Postgres, which only executes under the Postgres test tier
(DEV_HEALTH_POSTGRES_TEST_URI). This test proves it statically, by parsing
the migration source with `ast` (not grepping for literal kind strings,
which would miss a kind assembled from a shared constant and would not
distinguish a genuine seed from a later retirement of the same kind), so the
guard runs in every test tier, including a plain unit-test pass with no
database at all.

Root cause this guards: internal/jobs/metrics/remaining/families.json is the
single source for which remaining-metrics families exist and what job kind
(route_key) each maps to (CHAOS-5007). A family whose route_key has no
worker_job_routes seed row makes internal/jobroute's Controller.DeferredKinds
fail with pgx.ErrNoRows -> ErrUnknownRoute -> joboutbox.ErrUnavailable on the
very first reconciler tick, with no Postgres-side error to find -- CHAOS-3092
PR-B's live incident (metrics.remaining.work_item_attribution, fixed by
alembic 0123, same pattern as 0094 and 0115).
"""

from __future__ import annotations

import ast
import json
import re
import textwrap
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parents[1]
_VERSIONS_DIR = _REPO_ROOT / "src" / "dev_health_ops" / "alembic" / "versions"
_FAMILIES_PATH = (
    _REPO_ROOT / "internal" / "jobs" / "metrics" / "remaining" / "families.json"
)

_ROUTES_TABLE_NAME = "worker_job_routes"


def _literal_str_or_strings(node: ast.expr) -> tuple[str, ...] | None:
    """literal_eval restricted to a bare str or a tuple/list/frozenset of str."""
    try:
        value = ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError):
        return None
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (tuple, list, frozenset, set)) and all(
        isinstance(item, str) for item in value
    ):
        return tuple(value)
    return None


_ROUTES_TABLE_CONSTRUCTORS = ("table", "Table")


def _is_routes_table_call(node: ast.AST) -> bool:
    """`sa.table("worker_job_routes", ...)` (the lightweight TableClause
    shape every migration in this history actually uses, usually inside a
    `_routes()` helper) OR `sa.Table("worker_job_routes", metadata, ...)`
    (the fuller ORM/metadata-bound form -- not used in this history today,
    but a plausible future migration shape team-lead's round-2 attack
    correctly predicted this parser would silently miss entirely, since
    BOTH constructors take the table name as their first positional arg)."""
    return bool(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _ROUTES_TABLE_CONSTRUCTORS
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == _ROUTES_TABLE_NAME
    )


class _MigrationModule:
    """Resolves module-level name -> literal str/tuple(str) for one migration
    file, finds which local functions are reachable (by direct call) from a
    given entry point -- this codebase's migrations routinely delegate
    upgrade()/downgrade() to _seed_*/_remove_*/_retarget-style helpers -- and
    tracks which local names actually refer to the worker_job_routes
    TableClause, as opposed to some other table the same file happens to
    touch (codex round 1, P1/P2: without this, an insert into an unrelated
    table could falsely satisfy seed coverage, and a delete against an
    unrelated table could falsely trip the fail-closed guard)."""

    def __init__(self, tree: ast.Module) -> None:
        self.constants: dict[str, tuple[str, ...]] = {}
        self.functions: dict[str, ast.FunctionDef] = {}
        self.local_aliases: dict[str, ast.expr] = {}
        self.routes_factory_functions: set[str] = set()
        self.routes_variable_names: set[str] = set()
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name):
                    literal = _literal_str_or_strings(node.value)
                    if literal is not None:
                        self.constants[target.id] = literal
            elif isinstance(node, ast.FunctionDef):
                self.functions[node.name] = node
        # A function is a "routes factory" if it directly returns the
        # worker_job_routes TableClause -- e.g. every migration's own
        # `def _routes(): return sa.table("worker_job_routes", ...)`.
        for name, func in self.functions.items():
            for stmt in ast.walk(func):
                if isinstance(stmt, ast.Return) and stmt.value is not None:
                    if _is_routes_table_call(stmt.value):
                        self.routes_factory_functions.add(name)
                        break
        # Passthrough-filter local aliases, e.g. 0064's
        # `missing = [kind for kind in _KINDS if kind not in existing]`.
        # Resolving through the `if` clause would mean simulating runtime
        # state, so this deliberately ignores filters and resolves to the
        # comprehension's full source iterable instead -- a safe
        # over-approximation for a seed-coverage guard: every kind a
        # migration COULD insert still counts as seeded. Scanned file-wide
        # (not per-function) since these migrations never shadow a name
        # across two different helpers.
        #
        # The same pass also collects routes-table variable bindings, e.g.
        # `routes = _routes()` or an inline `routes = sa.table("worker_job_routes", ...)`.
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                continue
            target_name = node.targets[0].id
            value = node.value
            if isinstance(value, ast.ListComp) and len(value.generators) == 1:
                generator = value.generators[0]
                if (
                    isinstance(value.elt, ast.Name)
                    and isinstance(generator.target, ast.Name)
                    and value.elt.id == generator.target.id
                ):
                    self.local_aliases[target_name] = generator.iter
            if _is_routes_table_call(value) or (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Name)
                and value.func.id in self.routes_factory_functions
            ):
                self.routes_variable_names.add(target_name)

    def resolve(self, node: ast.expr) -> tuple[str, ...] | None:
        if isinstance(node, ast.Name):
            if node.id in self.constants:
                return self.constants[node.id]
            if node.id in self.local_aliases:
                return self.resolve(self.local_aliases[node.id])
            return None
        return _literal_str_or_strings(node)

    def is_routes_table_expr(self, node: ast.expr) -> bool:
        """True when `node` provably refers to the worker_job_routes
        TableClause: an inline `sa.table("worker_job_routes", ...)`, a call
        to a known routes-factory function (`_routes()`), or a variable bound
        to either of those. False for anything else, INCLUDING a name this
        module simply doesn't recognize -- unresolvable receivers are not
        routes ops by definition, they're just unrelated code this parser
        correctly ignores."""
        if _is_routes_table_call(node):
            return True
        if isinstance(node, ast.Name) and node.id in self.routes_variable_names:
            return True
        return bool(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in self.routes_factory_functions
        )

    def _reachable_functions(self, entry_point: str) -> list[ast.FunctionDef]:
        if entry_point not in self.functions:
            return []
        seen: set[str] = set()
        queue = [entry_point]
        reached: list[ast.FunctionDef] = []
        while queue:
            name = queue.pop()
            if name in seen or name not in self.functions:
                continue
            seen.add(name)
            func = self.functions[name]
            reached.append(func)
            for node in ast.walk(func):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id in self.functions
                ):
                    queue.append(node.func.id)
        return reached

    def reachable_calls(self, entry_point: str) -> list[ast.Call]:
        return [
            node
            for func in self._reachable_functions(entry_point)
            for node in ast.walk(func)
            if isinstance(node, ast.Call)
        ]

    def reachable_string_literals(self, entry_point: str) -> list[str]:
        """Every string literal (plain or f-string constant segment)
        anywhere in the subtree reachable from `entry_point` -- used only to
        hunt for raw-SQL statements a structural AST match can't see."""
        strings: list[str] = []
        for func in self._reachable_functions(entry_point):
            for node in ast.walk(func):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    strings.append(node.value)
                elif isinstance(node, ast.JoinedStr):
                    strings.extend(
                        value.value
                        for value in node.values
                        if isinstance(value, ast.Constant)
                        and isinstance(value.value, str)
                    )
        return strings


def _file_targets_routes_table(tree: ast.Module) -> bool:
    return any(_is_routes_table_call(node) for node in ast.walk(tree))


# A raw-SQL DML statement naming the routes table directly, e.g.
# `op.execute(sa.text("DELETE FROM worker_job_routes WHERE ..."))` -- a shape
# no structural extractor below can verify (codex round 1, P1: a raw-SQL
# retirement was silently invisible to the parser, leaving a genuinely
# deleted route counted as still live). Detected as its own fail-closed
# check, independent of the structural insert/delete extractors.
_RAW_SQL_ROUTES_PATTERN = re.compile(
    r"\b(insert\s+into|delete\s+from|update)\s+"
    + re.escape(_ROUTES_TABLE_NAME)
    + r"\b",
    re.IGNORECASE,
)


def _dict_job_kind_value(node: ast.expr) -> ast.expr | None:
    if isinstance(node, ast.Dict):
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant) and key.value == "job_kind":
                return value
    return None


def _extract_bulk_list(module: _MigrationModule, node: ast.expr) -> tuple[str, ...]:
    """A literal list of `{"job_kind": ..., ...}` dicts, or a list
    comprehension `[{"job_kind": kind, ...} for kind in NAME]` -- the actual
    kinds come from NAME (the iterable), not the loop variable's own name."""
    found: list[str] = []
    if isinstance(node, ast.List):
        for element in node.elts:
            value = _dict_job_kind_value(element)
            if value is not None:
                resolved = module.resolve(value)
                if resolved:
                    found.extend(resolved)
    elif isinstance(node, ast.ListComp):
        value = _dict_job_kind_value(node.elt)
        if value is not None and isinstance(value, ast.Name):
            for generator in node.generators:
                resolved = module.resolve(generator.iter)
                if resolved:
                    found.extend(resolved)
    return tuple(found)


def _is_job_kind_column(node: ast.expr) -> bool:
    return isinstance(node, ast.Attribute) and node.attr == "job_kind"


class SeedPatternUnrecognized(RuntimeError):
    """A call shaped like a worker_job_routes insert/delete was found, but
    its job_kind value(s) could not be resolved to a literal string.

    This is a deliberate fail-CLOSED design choice, not a convenience: a
    migration author who writes a seed/retirement in a shape this parser
    does not yet understand (a job_kind built from string concatenation, an
    imported constant from another module, a computed value, ...) must see
    a loud, specific test failure naming the file and the unresolved
    expression -- not a silent zero-kind contribution that makes the
    coverage assertion below quietly weaker than it looks. Extend the
    extractors above for the new shape rather than loosening this.
    """


def _require_resolved(
    path: Path, call: ast.Call, node: ast.expr, resolved: tuple[str, ...] | None
) -> tuple[str, ...]:
    if resolved:
        return resolved
    raise SeedPatternUnrecognized(
        f"{path}:{call.lineno}: recognized a worker_job_routes insert/delete "
        f"call shape, but could not resolve its job_kind value "
        f"({ast.unparse(node)!r}) to a literal string or a module-level "
        f"name/alias -- extend the parser in "
        f"tests/test_worker_job_routes_family_kind_seed_coverage.py for this "
        f"shape rather than letting it silently contribute nothing"
    )


def _extract_insert_kinds(
    module: _MigrationModule, call: ast.Call, path: Path
) -> tuple[str, ...]:
    found: list[str] = []
    # X.insert().values(job_kind=_KIND, ...) -- the singular 0094/0115/0123 shape.
    if (
        isinstance(call.func, ast.Attribute)
        and call.func.attr == "values"
        and isinstance(call.func.value, ast.Call)
        and isinstance(call.func.value.func, ast.Attribute)
        and call.func.value.func.attr == "insert"
        and module.is_routes_table_expr(call.func.value.func.value)
    ):
        job_kind_keywords = [kw for kw in call.keywords if kw.arg == "job_kind"]
        for kw in job_kind_keywords:
            found.extend(
                _require_resolved(path, call, kw.value, module.resolve(kw.value))
            )
        if not job_kind_keywords and call.args:
            found.extend(
                _require_resolved(
                    path, call, call.args[0], _extract_bulk_list(module, call.args[0])
                )
            )
    # bind.execute(X.insert(), [...]) / op.execute(X.insert(), [...]) -- bulk form.
    if (
        isinstance(call.func, ast.Attribute)
        and call.func.attr == "execute"
        and len(call.args) >= 2
    ):
        first = call.args[0]
        if (
            isinstance(first, ast.Call)
            and isinstance(first.func, ast.Attribute)
            and first.func.attr == "insert"
            and module.is_routes_table_expr(first.func.value)
        ):
            found.extend(
                _require_resolved(
                    path, call, call.args[1], _extract_bulk_list(module, call.args[1])
                )
            )
    # op.bulk_insert(routes, [...]) -- 0055's shape.
    if (
        isinstance(call.func, ast.Attribute)
        and call.func.attr == "bulk_insert"
        and len(call.args) >= 2
        and module.is_routes_table_expr(call.args[0])
    ):
        found.extend(
            _require_resolved(
                path, call, call.args[1], _extract_bulk_list(module, call.args[1])
            )
        )
    return tuple(found)


def _extract_delete_kinds(
    module: _MigrationModule, call: ast.Call, path: Path
) -> tuple[str, ...]:
    """X.delete().where(routes.c.job_kind.in_(NAME)) or
    X.delete().where(routes.c.job_kind == NAME) -- a retirement/removal.
    Only counted when X is provably the worker_job_routes table (codex round
    1, P2): a delete against some OTHER table in a file that merely
    references worker_job_routes elsewhere is not a routes op at all, and
    must not trip the fail-closed guard below."""
    if not (
        isinstance(call.func, ast.Attribute)
        and call.func.attr == "where"
        and isinstance(call.func.value, ast.Call)
        and isinstance(call.func.value.func, ast.Attribute)
        and call.func.value.func.attr == "delete"
        and module.is_routes_table_expr(call.func.value.func.value)
    ):
        return ()
    found: list[str] = []
    matched_job_kind_condition = False
    for arg in call.args:
        if (
            isinstance(arg, ast.Call)
            and isinstance(arg.func, ast.Attribute)
            and arg.func.attr == "in_"
            and _is_job_kind_column(arg.func.value)
            and arg.args
        ):
            matched_job_kind_condition = True
            found.extend(
                _require_resolved(path, call, arg.args[0], module.resolve(arg.args[0]))
            )
        if (
            isinstance(arg, ast.Compare)
            and len(arg.ops) == 1
            and isinstance(arg.ops[0], ast.Eq)
        ):
            left, right = arg.left, arg.comparators[0]
            target = (
                right
                if _is_job_kind_column(left)
                else left
                if _is_job_kind_column(right)
                else None
            )
            if target is not None:
                matched_job_kind_condition = True
                found.extend(
                    _require_resolved(path, call, target, module.resolve(target))
                )
    if not matched_job_kind_condition:
        # A .delete().where(...) on the routes table whose condition isn't
        # keyed on job_kind at all (not seen in this history, but not
        # inherently invalid SQLAlchemy either) -- fail closed rather than
        # silently treat it as "deletes nothing".
        raise SeedPatternUnrecognized(
            f"{path}:{call.lineno}: recognized a worker_job_routes "
            f".delete().where(...) call, but its condition is not a "
            f"job_kind == / job_kind.in_(...) comparison this parser "
            f"understands ({ast.unparse(call)!r})"
        )
    return tuple(found)


def _collect_seed_state(path: Path) -> tuple[set[str], set[str]]:
    """Returns (seeded, retired) kinds this migration's upgrade() (and
    whatever local helpers it calls) reaches."""
    tree = ast.parse(path.read_text(), filename=str(path))
    if not _file_targets_routes_table(tree):
        return set(), set()
    module = _MigrationModule(tree)
    for text in module.reachable_string_literals("upgrade"):
        if _RAW_SQL_ROUTES_PATTERN.search(text):
            raise SeedPatternUnrecognized(
                f"{path}: found a raw-SQL statement naming "
                f"{_ROUTES_TABLE_NAME} directly ({text!r}) -- this parser "
                f"only understands the SQLAlchemy Core insert()/delete() "
                f"forms every other migration in this history uses, and "
                f"cannot verify a raw-SQL statement is a seed or a "
                f"retirement (codex round 1, P1: a raw-SQL retirement was "
                f"previously silently invisible, leaving a genuinely "
                f"deleted route counted as still live). Rewrite the "
                f"migration to use insert()/delete(), or extend this parser."
            )
    seeded: set[str] = set()
    retired: set[str] = set()
    for call in module.reachable_calls("upgrade"):
        seeded.update(_extract_insert_kinds(module, call, path))
        retired.update(_extract_delete_kinds(module, call, path))
    return seeded, retired


def _currently_seeded_kinds() -> set[str]:
    """The net set of job kinds with a live worker_job_routes row after every
    checked-in migration's upgrade() has run, in order, against a fresh
    database -- i.e. what a brand-new environment ends up with.

    This aggregates seed/retire pairs across the whole migration set rather
    than replaying strict revision order; that is exact for this history (no
    kind is ever re-seeded after being retired), which the self-check test
    below verifies against the one retirement (0110) actually on record.
    """
    files = sorted(_VERSIONS_DIR.glob("*.py"))
    assert files, f"no migration files found under {_VERSIONS_DIR}"
    all_seeded: set[str] = set()
    all_retired: set[str] = set()
    for path in files:
        seeded, retired = _collect_seed_state(path)
        all_seeded |= seeded
        all_retired |= retired
    return all_seeded - all_retired


def _remaining_route_keys() -> list[str]:
    inventory = json.loads(_FAMILIES_PATH.read_text())
    return [family["route_key"] for family in inventory["families"]]


def test_every_remaining_family_route_key_has_a_worker_job_routes_seed() -> None:
    seeded = _currently_seeded_kinds()
    route_keys = _remaining_route_keys()
    assert route_keys, "families.json produced no families -- test would pass vacuously"
    missing = [key for key in route_keys if key not in seeded]
    assert not missing, (
        f"remaining-metrics families with no worker_job_routes seed migration: "
        f"{missing} -- add a 0094/0115/0123-style seed migration before this "
        f"kind ships (CHAOS-3092 PR-B's exact failure mode: internal/jobroute's "
        f"Controller.DeferredKinds fails ErrUnknownRoute on the very first "
        f"reconciler tick for a kind with no seeded route)"
    )


def test_static_seed_parser_matches_known_baseline_and_retired_kinds() -> None:
    """Self-check for the AST parser above against known ground truth: a
    checked-in baseline seed (0064/0055), a singular 0094/0115/0123-style
    seed, and the one on-record retirement (0110). If this fails, the parser
    itself is broken (silently under- or over-counting) and the assertion
    above it is meaningless regardless of what it reports.
    """
    seeded = _currently_seeded_kinds()
    for kind in (
        "sync.provider_unit",  # 0061, singular values(job_kind=...)
        "system.sync_coverage_refresh",  # 0094, singular
        "sync.team_repo_ownership_derivation",  # 0115, singular
        "metrics.remaining.work_item_attribution",  # 0123, singular
        "investment.chunk",  # 0064, bulk insert() over a _KINDS tuple
        "operational.billing_notification",  # 0055, op.bulk_insert() over _KINDS
    ):
        assert kind in seeded, f"parser failed to find a known seed for {kind!r}"
    for kind in (
        "metrics.remaining.extra_metrics",
        "metrics.remaining.team_metrics",
    ):
        assert kind not in seeded, (
            f"parser failed to net out the 0110 retirement of {kind!r} "
            "against its 0064/0066 seed"
        )


def test_parser_fails_closed_on_an_unresolvable_job_kind_value(tmp_path: Path) -> None:
    """The parser must FAIL LOUDLY (SeedPatternUnrecognized), not silently
    skip, when it recognizes an insert/delete shape against the routes table
    but can't resolve the job_kind value to a literal -- e.g. a value built
    from a function call, an import, or anything else that isn't a plain
    string constant or a module-level name/alias this file's Load() already
    resolves. A migration author who writes a seed this parser doesn't
    understand must get a specific, actionable test failure, never a
    quietly-smaller seeded set that happens to still pass by coincidence.
    """
    synthetic = tmp_path / "9999_synthetic_unresolvable_seed.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def upgrade():
                kind = compute_dynamic_kind_from_somewhere_else()
                op.get_bind().execute(_routes().insert().values(job_kind=kind))
            """
        )
    )
    with pytest.raises(SeedPatternUnrecognized, match="job_kind"):
        _collect_seed_state(synthetic)


def test_insert_into_an_unrelated_table_does_not_satisfy_seed_coverage(
    tmp_path: Path,
) -> None:
    """Codex round 1, P1: a migration that references worker_job_routes
    somewhere (satisfying the file-level marker) but inserts a job_kind-
    shaped row into a DIFFERENT table must NOT count as seeding that kind --
    the old parser only checked the FILE, never the actual insert TARGET.
    """
    synthetic = tmp_path / "9998_false_seed_unrelated_table.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def _audit():
                return sa.table("some_audit_table", sa.column("job_kind", sa.String()))

            def upgrade():
                routes = _routes()
                op.get_bind().execute(
                    _audit().insert().values(job_kind="metrics.remaining.capacity")
                )
            """
        )
    )
    seeded, retired = _collect_seed_state(synthetic)
    assert "metrics.remaining.capacity" not in seeded, (
        "an insert into an unrelated table was wrongly counted as a "
        "worker_job_routes seed"
    )
    assert not retired


def test_raw_sql_retirement_fails_closed_instead_of_going_unnoticed(
    tmp_path: Path,
) -> None:
    """Codex round 1, P1: a route seeded via the recognized insert() shape
    and later retired via raw SQL (`op.execute(sa.text("DELETE FROM
    worker_job_routes ..."))`) used to leave the kind counted as still live
    -- the structural extractors have no way to see a raw-SQL statement, and
    silently seeing nothing is exactly the failure mode this guard exists to
    prevent. Must now fail closed instead.
    """
    synthetic = tmp_path / "9997_raw_sql_route_retirement.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def upgrade():
                op.get_bind().execute(
                    _routes().insert().values(job_kind="metrics.remaining.capacity")
                )
                op.execute(sa.text(
                    "DELETE FROM worker_job_routes WHERE job_kind = 'metrics.remaining.capacity'"
                ))
            """
        )
    )
    with pytest.raises(SeedPatternUnrecognized, match="raw-SQL"):
        _collect_seed_state(synthetic)


def test_delete_against_an_unrelated_table_does_not_trip_the_fail_closed_guard(
    tmp_path: Path,
) -> None:
    """Codex round 1, P2 (this round's own disclosed weakest point,
    confirmed real): a `.delete().where(...)` against some OTHER table, in a
    file that merely references worker_job_routes elsewhere, must not raise
    SeedPatternUnrecognized -- it is not a routes operation at all.
    """
    synthetic = tmp_path / "9996_unrelated_delete.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def _audit():
                return sa.table("unrelated_audit", sa.column("status", sa.String()))

            def upgrade():
                routes = _routes()
                op.get_bind().execute(
                    _audit().delete().where(_audit().c.status == "obsolete")
                )
            """
        )
    )
    seeded, retired = _collect_seed_state(synthetic)
    assert not seeded
    assert not retired


def test_sa_table_capital_t_construction_is_recognized_as_the_routes_table(
    tmp_path: Path,
) -> None:
    """Round 2 attack (team-lead): a migration could declare the routes
    table with the fuller `sa.Table("worker_job_routes", metadata, ...)`
    constructor instead of the lightweight `sa.table(...)` every real
    migration in this history actually uses -- both take the table name as
    their first positional arg, but the parser previously only recognized
    the lowercase form, so a file using `sa.Table(...)` bound to a
    module-level variable and consumed via `op.bulk_insert` was invisible
    end to end: `_file_targets_routes_table` returned False and the whole
    file was silently skipped.
    """
    synthetic = tmp_path / "9995_sa_table_capital_construction.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            metadata = sa.MetaData()
            routes = sa.Table(
                "worker_job_routes",
                metadata,
                sa.Column("job_kind", sa.String()),
            )

            def upgrade():
                op.bulk_insert(routes, [{"job_kind": "metrics.remaining.capacity"}])
            """
        )
    )
    seeded, retired = _collect_seed_state(synthetic)
    assert seeded == {"metrics.remaining.capacity"}
    assert not retired
