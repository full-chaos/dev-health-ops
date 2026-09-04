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

PARSER CONTRACT (written down explicitly per team-lead's request before
codex round 3, after two rounds of adversarial review already tightened
it -- read this before extending any extractor below, and keep it in sync
with the code):

  1. FILE-LOCAL ONLY. Each migration file is analyzed independently, using
     only that file's own AST. There is no cross-module resolution: a table
     object imported from another module, with no local `sa.table()`/
     `sa.Table()` call anywhere in the migration file itself, is invisible.
     This is a deliberate design boundary, not a bug (see
     _MigrationModule's docstring) -- verified to have zero occurrences in
     the current 124-file migration history.

  2. ROUTES-TABLE IDENTITY is recognized only as: an inline
     `sa.table("worker_job_routes", ...)` or
     `sa.Table("worker_job_routes", ...)` call; a call to a local function
     whose EVERY `return` statement resolves (directly or transitively
     through another such function) to one of those; or a variable whose
     EVERY assignment anywhere in the file resolves to one of those. A
     function or variable with a MIXED resolution (the routes table on one
     branch/assignment, something else on another) is AMBIGUOUS, not
     routes-identified -- this parser has no flow-sensitive/branch
     analysis, so it never guesses which one governs at a given call site.

  3. RECOGNIZED SEED (insert) SHAPES, only when the receiver is
     unambiguously the routes table (rule 2): `X.insert().values(job_kind=
     <value>)`; `X.insert().values(<bulk list/listcomp>)` (positional, no
     job_kind keyword); `bind_or_op.execute(X.insert(), <bulk
     list/listcomp>)`; `op.bulk_insert(X, <bulk list/listcomp>)`.

  4. RECOGNIZED RETIREMENT (delete) SHAPES, only when the receiver is
     unambiguously the routes table: `X.delete().where(<routes>.c.job_kind
     .in_(<value>))`; `X.delete().where(<routes>.c.job_kind == <value>)`.

  5. A job_kind VALUE resolves only to a literal string constant, or a
     module-level name assigned (exactly once) a literal str/tuple(str).

  6. FAILS CLOSED (raises SeedPatternUnrecognized -- a loud, specific test
     failure, never a silently smaller seeded set) when: a raw string this
     file's code could produce anywhere reachable from upgrade() (a
     literal, a `+`-concatenation, or an f-string with resolvable
     interpolations) matches a DML pattern naming the routes table
     directly; a recognized insert/delete shape's receiver IS the routes
     table but its job_kind value or delete condition can't be resolved to
     a literal; a `.delete().where(...)` on the routes table whose
     condition isn't job_kind-based at all; or a call/variable receiver is
     AMBIGUOUS per rule 2.

  7. Everything else -- a call whose receiver definitively resolves to
     something OTHER than the routes table, and is not ambiguous -- is
     SILENTLY not counted. This is correct, not a gap: it is genuinely
     unrelated code (an insert/delete against some other table), and the
     file-level pre-filter (rule 1's substring check) is deliberately
     broad, so most files it lets through will contain plenty of code this
     parser correctly has nothing to say about.

  The property this contract is meant to guarantee: this parser NEVER
  silently counts something as a worker_job_routes seed or retirement
  unless it can prove that structurally (rules 3-5); anything
  routes-adjacent it cannot fully resolve is a loud failure (rule 6), never
  a silent guess. A known, deliberately unhandled shape (rule 1's
  cross-module case; also multi-target assignment `a = b = sa.table(...)`;
  an aliased function reference `rt = _routes; rt()`) can only ever cause
  this parser to UNDER-count (a real seed goes unrecognized, surfacing as a
  false "missing" on the coverage assertion below -- annoying, but safe:
  it never hides a genuine gap) -- never to OVER-count. If a new extractor
  is ever added, preserve that asymmetry: err toward failing loud or
  missing silently, never toward a guessed accept.
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


def _walk_own_scope(node: ast.AST):  # noqa: ANN201 -- generator, see docstring
    """Like `ast.walk`, but does NOT descend into a NESTED
    FunctionDef/AsyncFunctionDef/Lambda's body -- codex round 3, P1: `ast.
    walk` recurses into every descendant regardless of scope, so a call
    inside `def upgrade(): def never_called(): _seed()` (a nested helper
    upgrade() never actually invokes) was found and counted as if it were
    upgrade()'s own directly-executed code -- a genuine false ACCEPT: the
    seed call never runs, but the parser credited it as if it had. A nested
    def's OWN name/call-sites in the ENCLOSING scope are still visible
    (this walker yields the nested FunctionDef node itself, just not its
    body), so `def helper(): ...; helper()` still resolves correctly
    through the normal `self.functions` machinery -- what's excluded is
    ONLY code lexically inside a definition that is never shown to be
    called.
    """
    stack = [node]
    root = node
    while stack:
        current = stack.pop()
        yield current
        if current is not root and isinstance(
            current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
        ):
            continue
        stack.extend(ast.iter_child_nodes(current))


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
        # Functions/names that are AMBIGUOUS with respect to the routes
        # table: a function with MULTIPLE distinct return shapes, at least
        # one of which is the routes table and at least one of which is not
        # (a CONDITIONAL factory, e.g. `def _routes(): return
        # sa.table("worker_job_routes",...) if FLAG else
        # sa.table("audit_log",...)` -- confirmed a real false-ACCEPT risk
        # while verifying round 2's fixes, not a codex finding: an
        # unconditional-factory assumption meant every call to such a
        # function was wrongly trusted as the routes table, even on the
        # branch that returns something else); or a variable rebound to a
        # non-routes value after being routes-bound elsewhere (codex round
        # 2, P1), or bound to an ambiguous factory's call. This parser has
        # no flow-sensitive/branch analysis, so any of these is inherently
        # unresolvable from here -- deliberately excluded from the
        # corresponding "known routes" set AND flagged so the extractors
        # below fail closed instead of guessing which shape governs at the
        # use site.
        self.ambiguous_factory_functions: set[str] = set()
        self.ambiguous_variable_names: set[str] = set()
        # A module-level name is trusted as a literal constant only if it has
        # EXACTLY ONE assignment, and that assignment is itself a literal
        # (codex round 3, P1: `KIND = "metrics.remaining.capacity"` followed
        # later by `KIND = dynamic()` kept the STALE literal in `constants`
        # forever, since only literal-resolving assignments ever wrote to
        # the dict -- a reassignment to a non-literal silently left the old
        # value in place instead of invalidating it). A name assigned more
        # than once, or assigned a non-literal at all, is excluded from
        # `constants` entirely rather than guessed at -- `resolve()` then
        # naturally returns None for it, which routes through the existing
        # `_require_resolved` fail-closed path, same mechanism as an
        # unresolvable value that was never a name at all.
        constant_assignment_counts: dict[str, int] = {}
        constant_literal_values: dict[str, tuple[str, ...]] = {}
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name):
                    constant_assignment_counts[target.id] = (
                        constant_assignment_counts.get(target.id, 0) + 1
                    )
                    literal = _literal_str_or_strings(node.value)
                    if literal is not None:
                        constant_literal_values[target.id] = literal
            elif isinstance(node, ast.FunctionDef):
                self.functions[node.name] = node
        for name, count in constant_assignment_counts.items():
            if count == 1 and name in constant_literal_values:
                self.constants[name] = constant_literal_values[name]
        # A function is a "routes factory" only if EVERY one of its `return`
        # statements resolves to the worker_job_routes TableClause (directly,
        # e.g. every migration's own `def _routes(): return
        # sa.table("worker_job_routes", ...)`, or transitively through
        # another already-known routes factory -- a "factory of a factory":
        # `def _r2(): return _routes()`, codex round 2, P1). A function with
        # ANY other return shape mixed in is ambiguous, not a factory --
        # see the class docstring's note above. Iterated to a fixed point
        # since a transitive factory's own dependency may be defined later
        # in the file, or may itself only become known-ambiguous once ITS
        # dependency resolves.
        changed = True
        while changed:
            changed = False
            for name, func in self.functions.items():
                if (
                    name in self.routes_factory_functions
                    or name in self.ambiguous_factory_functions
                ):
                    continue
                shapes: set[str] = set()
                for stmt in ast.walk(func):
                    if isinstance(stmt, ast.Return) and stmt.value is not None:
                        shapes.add(self._classify_routes_shape(stmt.value))
                if not shapes or shapes == {"other"}:
                    continue  # not routes-related at all; leave it alone
                if shapes == {"routes"}:
                    self.routes_factory_functions.add(name)
                    changed = True
                elif "other" in shapes or "ambiguous" in shapes:
                    self.ambiguous_factory_functions.add(name)
                    changed = True
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
        # `routes = _routes()` or an inline `routes = sa.table("worker_job_routes", ...)`,
        # using the same routes/ambiguous/other classification as above --
        # a name is only trusted as the routes table if EVERY assignment to
        # it, anywhere in the file, classifies as "routes".
        binding_shapes: dict[str, set[str]] = {}
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
            binding_shapes.setdefault(target_name, set()).add(
                self._classify_routes_shape(value)
            )
        for name, shapes in binding_shapes.items():
            if shapes == {"routes"}:
                self.routes_variable_names.add(name)
            elif "other" in shapes and "routes" in shapes:
                self.ambiguous_variable_names.add(name)
            elif "ambiguous" in shapes:
                self.ambiguous_variable_names.add(name)
        # `for X in ...:` and `with ... as X:` rebind X too, NOT just a plain
        # `X = ...` -- codex round 3, P1: the ordinary-reassignment ambiguity
        # tracking above only scans `ast.Assign` nodes, so a `for routes in
        # [_audit()]:` loop after `routes = _routes()` was invisible to it;
        # the parser kept trusting `routes` as the routes table throughout
        # the loop body, even though it is bound to a different table on
        # every iteration. Both binding forms are unconditionally ambiguous
        # here (their bound value can't be statically resolved the way a
        # plain Assign's RHS can, so there's no "shape" to classify) --
        # union them into ambiguous_variable_names and revoke any trust
        # routes_variable_names already granted the same name.
        loop_or_with_bound_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.For, ast.AsyncFor)) and isinstance(
                node.target, ast.Name
            ):
                loop_or_with_bound_names.add(node.target.id)
            elif isinstance(node, ast.With) or isinstance(node, ast.AsyncWith):
                for item in node.items:
                    if isinstance(item.optional_vars, ast.Name):
                        loop_or_with_bound_names.add(item.optional_vars.id)
        self.ambiguous_variable_names |= loop_or_with_bound_names
        self.routes_variable_names -= loop_or_with_bound_names

    def _classify_routes_shape(self, value: ast.expr) -> str:
        """ "routes" (provably the worker_job_routes TableClause), "ambiguous"
        (a call to an ambiguous factory function -- unresolvable, not
        provably one or the other), or "other" (anything else, including a
        call to a function unrelated to the routes table)."""
        if _is_routes_table_call(value):
            return "routes"
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
            if value.func.id in self.routes_factory_functions:
                return "routes"
            if value.func.id in self.ambiguous_factory_functions:
                return "ambiguous"
        return "other"

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
        to a known routes-factory function (`_routes()`, transitively), or a
        variable unambiguously bound to either of those. False for anything
        else, INCLUDING a name this module simply doesn't recognize --
        unresolvable receivers are not routes ops by definition, they're
        just unrelated code this parser correctly ignores. A NAME that is
        ambiguous (rebound to something else elsewhere in the file) is
        never True here -- callers must check `is_ambiguous_receiver` first
        and fail closed rather than silently trust a stale/guessed binding."""
        if _is_routes_table_call(node):
            return True
        if isinstance(node, ast.Name) and node.id in self.routes_variable_names:
            return True
        return bool(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in self.routes_factory_functions
        )

    def is_ambiguous_receiver(self, node: ast.expr) -> bool:
        if isinstance(node, ast.Name):
            return node.id in self.ambiguous_variable_names
        return bool(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in self.ambiguous_factory_functions
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
            for node in _walk_own_scope(func):
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
            for node in _walk_own_scope(func)
            if isinstance(node, ast.Call)
        ]

    def reachable_string_literals(self, entry_point: str) -> list[str]:
        """Every string value this file's code could produce anywhere in the
        subtree reachable from `entry_point`, constant-folding plain
        literals, `+` concatenation, and f-strings whose interpolated parts
        resolve to a known literal name -- used only to hunt for raw-SQL
        statements a structural AST match can't see. Walking every node
        (not just the outermost expression) is deliberate: it also resolves
        each piece of a concatenation individually, which costs nothing
        extra since duplicates are harmless for a substring search.

        codex round 2, P1: a raw-SQL DML statement assembled from MULTIPLE
        string literals (`"DELETE " + "FROM " + "worker_job_routes ..."`, or
        an f-string) was invisible to a per-literal-only scan -- no SINGLE
        AST string node contained the whole DML phrase, even though the
        file's actual runtime string did.

        Also resolves a bare NAME reference to a module-level string
        constant (codex round 3, P1: `SQL = "DELETE FROM
        worker_job_routes ..."` used via `op.execute(SQL)` was invisible --
        the scan only inspected literal/concatenation/f-string EXPRESSIONS
        directly present in the reachable code, never a Name pointing at
        one defined elsewhere).
        """
        strings: list[str] = []
        for func in self._reachable_functions(entry_point):
            for node in _walk_own_scope(func):
                if isinstance(node, (ast.Constant, ast.BinOp, ast.JoinedStr, ast.Name)):
                    resolved = self._resolve_string_expr(node)
                    if resolved is not None:
                        strings.append(resolved)
        return strings

    def _resolve_string_expr(self, node: ast.expr) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            resolved = self.resolve(node)
            if resolved and len(resolved) == 1:
                return resolved[0]
            return None
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = self._resolve_string_expr(node.left)
            right = self._resolve_string_expr(node.right)
            if left is not None and right is not None:
                return left + right
            return None
        if isinstance(node, ast.JoinedStr):
            parts: list[str] = []
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                    continue
                if isinstance(value, ast.FormattedValue):
                    resolved_name = self.resolve(value.value)
                    if resolved_name and len(resolved_name) == 1:
                        parts.append(resolved_name[0])
                        continue
                return None
            return "".join(parts)
        return None


def _file_targets_routes_table(source: str) -> bool:
    """A cheap, deliberately OVER-inclusive substring pre-filter on the raw
    source text, not the parsed AST: "does this file mention
    worker_job_routes ANYWHERE at all" (a `sa.table()`/`sa.Table()` call, a
    raw-SQL string, a comment, an import, anything). Gating on an AST-only
    check (only a `sa.table()`/`sa.Table()` CALL node) previously meant a
    file that touches the table ONLY via raw SQL, with no local table
    construction at all, skipped analysis entirely -- including the raw-SQL
    fail-closed check itself, silently defeating it. The real precision
    work (table identity, resolvability, DML detection) happens in the
    extractors below; this gate only needs to decide whether it's worth
    looking at all, so being broad here is free and safe."""
    return _ROUTES_TABLE_NAME in source


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


def _require_unambiguous_receiver(
    path: Path, call: ast.Call, module: _MigrationModule, receiver: ast.expr
) -> None:
    """codex round 2, P1: a name bound to the routes table at one point and
    rebound to something else later (`routes = _routes(); routes =
    _audit()`) is inherently ambiguous for a parser with no flow-sensitive
    analysis -- silently trusting EITHER binding risks either a false miss
    or, worse, a false accept of an unrelated table's insert as a routes
    seed. Fail closed instead of guessing."""
    if module.is_ambiguous_receiver(receiver):
        raise SeedPatternUnrecognized(
            f"{path}:{call.lineno}: {ast.unparse(receiver)!r} is bound to "
            f"the worker_job_routes table at one point in this file and to "
            f"something else at another -- this parser has no "
            f"flow-sensitive analysis and cannot tell which binding governs "
            f"this call. Give the routes table its own unique variable name "
            f"that is never reassigned, or extend the parser."
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
    ):
        receiver = call.func.value.func.value
        _require_unambiguous_receiver(path, call, module, receiver)
        if module.is_routes_table_expr(receiver):
            job_kind_keywords = [kw for kw in call.keywords if kw.arg == "job_kind"]
            for kw in job_kind_keywords:
                found.extend(
                    _require_resolved(path, call, kw.value, module.resolve(kw.value))
                )
            if not job_kind_keywords and call.args:
                found.extend(
                    _require_resolved(
                        path,
                        call,
                        call.args[0],
                        _extract_bulk_list(module, call.args[0]),
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
        ):
            receiver = first.func.value
            _require_unambiguous_receiver(path, call, module, receiver)
            if module.is_routes_table_expr(receiver):
                found.extend(
                    _require_resolved(
                        path,
                        call,
                        call.args[1],
                        _extract_bulk_list(module, call.args[1]),
                    )
                )
    # op.bulk_insert(routes, [...]) -- 0055's shape.
    if (
        isinstance(call.func, ast.Attribute)
        and call.func.attr == "bulk_insert"
        and len(call.args) >= 2
    ):
        receiver = call.args[0]
        _require_unambiguous_receiver(path, call, module, receiver)
        if module.is_routes_table_expr(receiver):
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
    ):
        return ()
    receiver = call.func.value.func.value
    _require_unambiguous_receiver(path, call, module, receiver)
    if not module.is_routes_table_expr(receiver):
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
    source = path.read_text()
    if not _file_targets_routes_table(source):
        return set(), set()
    tree = ast.parse(source, filename=str(path))
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


def test_fragmented_raw_sql_retirement_fails_closed(tmp_path: Path) -> None:
    """Codex round 2, P1: a raw-SQL retirement assembled from MULTIPLE
    string literals via `+` concatenation -- no SINGLE AST string node
    contains the whole "DELETE FROM worker_job_routes ..." phrase, so a
    scan that only checked one literal at a time missed it entirely.
    """
    synthetic = tmp_path / "9994_fragmented_raw_sql_retirement.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def upgrade():
                op.execute(
                    _routes().insert().values(job_kind="metrics.remaining.capacity")
                )
                op.execute(sa.text(
                    "DELETE " + "FROM " + "worker_job_routes WHERE job_kind = "
                    "'metrics.remaining.capacity'"
                ))
            """
        )
    )
    with pytest.raises(SeedPatternUnrecognized, match="raw-SQL"):
        _collect_seed_state(synthetic)


def test_pure_raw_sql_file_with_no_table_construction_is_still_detected(
    tmp_path: Path,
) -> None:
    """Codex round 2 follow-up (found by the lane while verifying the round
    2 fixes, not by codex itself): a file that touches worker_job_routes
    ONLY via a raw-SQL statement, with no local `sa.table()`/`sa.Table()`
    call anywhere, used to skip analysis entirely -- the file-level gate
    only looked for a table CONSTRUCTOR call, so a pure-raw-SQL file never
    even reached the raw-SQL check meant to catch exactly this.
    """
    synthetic = tmp_path / "9993_pure_raw_sql_no_table_construction.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            from alembic import op

            def upgrade():
                op.execute(
                    "DELETE FROM worker_job_routes WHERE job_kind = 'metrics.remaining.capacity'"
                )
            """
        )
    )
    with pytest.raises(SeedPatternUnrecognized, match="raw-SQL"):
        _collect_seed_state(synthetic)


def test_rebound_variable_name_fails_closed_instead_of_trusting_a_stale_binding(
    tmp_path: Path,
) -> None:
    """Codex round 2, P1: `routes = _routes(); routes = _audit()` rebinds
    the SAME name to a different table -- the parser has no flow-sensitive
    analysis, so trusting either binding is a guess. Must fail closed
    rather than silently accept an insert through the rebound name as a
    routes seed (the exact false-accept shape round 1's P1 already found
    once, recreated a different way).
    """
    synthetic = tmp_path / "9992_rebound_routes_variable.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def _audit():
                return sa.table("audit_log", sa.column("job_kind", sa.String()))

            def upgrade():
                routes = _routes()
                routes = _audit()
                op.execute(routes.insert().values(job_kind="metrics.remaining.capacity"))
            """
        )
    )
    with pytest.raises(SeedPatternUnrecognized, match="ambiguous|bound to"):
        _collect_seed_state(synthetic)


def test_factory_of_a_factory_is_recognized_as_the_routes_table(
    tmp_path: Path,
) -> None:
    """Codex round 2, P1: a helper function that returns ANOTHER
    routes-factory function's call (`def _r2(): return _routes()`), rather
    than the TableClause directly, must still be recognized as a routes
    reference -- the previous one-hop-only tracking silently treated an
    insert/delete through the second-level factory as unrelated code.
    """
    synthetic = tmp_path / "9991_factory_of_a_factory.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def _routes2():
                return _routes()

            def upgrade():
                op.execute(
                    _routes2().delete().where(
                        _routes2().c.job_kind == "metrics.remaining.capacity"
                    )
                )
            """
        )
    )
    seeded, retired = _collect_seed_state(synthetic)
    assert not seeded
    assert retired == {"metrics.remaining.capacity"}


def test_conditional_factory_fails_closed_instead_of_being_trusted_unconditionally(
    tmp_path: Path,
) -> None:
    """Found by the lane while self-auditing before round 3 (not a codex
    finding): a "factory" function that returns the routes table on only
    ONE branch (`def _routes(): return sa.table("worker_job_routes", ...)
    if FLAG else sa.table("audit_log", ...)`) used to be trusted as an
    UNCONDITIONAL routes factory the instant ANY of its return statements
    matched -- so every call to it was wrongly counted as the routes table,
    even on the branch that actually returns something else. This is a
    genuine false-ACCEPT (the dangerous direction, not merely a missed
    seed): a real insert into `audit_log` at runtime could have been
    silently counted as covering a `worker_job_routes` family. Must fail
    closed instead of guessing which branch governs at the call site.
    """
    synthetic = tmp_path / "9990_conditional_factory.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            FLAG = False

            def _routes():
                if FLAG:
                    return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))
                return sa.table("audit_log", sa.column("job_kind", sa.String()))

            def upgrade():
                op.execute(_routes().insert().values(job_kind="metrics.remaining.capacity"))
            """
        )
    )
    with pytest.raises(SeedPatternUnrecognized, match="ambiguous|bound to"):
        _collect_seed_state(synthetic)


def test_reassigned_module_constant_fails_closed_instead_of_using_a_stale_value(
    tmp_path: Path,
) -> None:
    """Codex round 3, P1: `KIND = "metrics.remaining.capacity"` followed by
    `KIND = dynamic()` (a non-literal reassignment) used to keep the STALE
    literal in `constants` forever -- only a literal-resolving assignment
    ever wrote to the dict, so the non-literal reassignment silently left
    the old value in place instead of invalidating it. A job_kind that
    resolves to a name assigned more than once (literal or not) must fail
    closed, not silently trust whichever assignment happened to be literal.
    """
    synthetic = tmp_path / "9989_reassigned_constant.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            KIND = "metrics.remaining.capacity"

            def dynamic():
                return "metrics.remaining.not_capacity"

            KIND = dynamic()

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def upgrade():
                op.execute(_routes().insert().values(job_kind=KIND))
            """
        )
    )
    with pytest.raises(SeedPatternUnrecognized, match="job_kind"):
        _collect_seed_state(synthetic)


def test_call_inside_a_never_invoked_nested_helper_is_not_counted(
    tmp_path: Path,
) -> None:
    """Codex round 3, P1: `ast.walk` recurses into a NESTED function
    definition's body regardless of whether that nested function is ever
    actually called -- `def upgrade(): def never_called(): _seed()` used to
    have `_seed()`'s call counted as upgrade()'s own reachable code, even
    though `never_called` is defined and never invoked. A genuine false
    ACCEPT: the seed call never runs at all, but the parser credited it.
    """
    synthetic = tmp_path / "9988_nested_uncalled_helper.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def _seed():
                op.execute(_routes().insert().values(job_kind="metrics.remaining.capacity"))

            def upgrade():
                def never_called():
                    _seed()
            """
        )
    )
    seeded, retired = _collect_seed_state(synthetic)
    assert not seeded
    assert not retired


def test_module_level_raw_sql_constant_used_by_name_is_still_detected(
    tmp_path: Path,
) -> None:
    """Codex round 3, P1: a raw-SQL statement stored in a MODULE-LEVEL
    string constant (`SQL = "DELETE FROM worker_job_routes ..."`) and used
    via `op.execute(SQL)` -- a bare Name reference at the call site, not a
    string literal/concatenation/f-string directly present in upgrade()'s
    own code -- was invisible to the raw-SQL scan, which only inspected
    literal-shaped expressions structurally, never resolved through a name.
    """
    synthetic = tmp_path / "9987_module_level_raw_sql_by_name.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            SQL = "DELETE FROM worker_job_routes WHERE job_kind = 'metrics.remaining.capacity'"

            def upgrade():
                op.execute(
                    _routes().insert().values(job_kind="metrics.remaining.capacity")
                )
                op.execute(SQL)
            """
        )
    )
    with pytest.raises(SeedPatternUnrecognized, match="raw-SQL"):
        _collect_seed_state(synthetic)


def test_for_loop_target_rebinding_fails_closed(tmp_path: Path) -> None:
    """Codex round 3, P1: the ordinary-reassignment ambiguity tracking only
    scans `ast.Assign` nodes -- a `for routes in [_audit()]:` loop rebinds
    `routes` too, but via a DIFFERENT AST node (`ast.For.target`), so it was
    invisible to that tracking. `routes = _routes()` once, followed by a
    `for routes in [...]:` loop, used to leave `routes` trusted as the
    routes table for the rest of the file, even though the loop rebinds it
    to a different table on every iteration.
    """
    synthetic = tmp_path / "9986_for_loop_rebind.py"
    synthetic.write_text(
        textwrap.dedent(
            """
            import sqlalchemy as sa
            from alembic import op

            def _routes():
                return sa.table("worker_job_routes", sa.column("job_kind", sa.String()))

            def _audit():
                return sa.table("audit_log", sa.column("job_kind", sa.String()))

            routes = _routes()

            def upgrade():
                for routes in [_audit()]:
                    op.execute(routes.insert().values(job_kind="metrics.remaining.capacity"))
            """
        )
    )
    with pytest.raises(SeedPatternUnrecognized, match="ambiguous|bound to"):
        _collect_seed_state(synthetic)
