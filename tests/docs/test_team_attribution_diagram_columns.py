"""Drift gate for team-attribution.md's mermaid ``erDiagram`` entity blocks.

``docs/contribute/architecture/team-attribution.md`` §3 documents the team-attribution
data model as a mermaid ``erDiagram``, with one ``{ ... }`` column block per entity
that makes a concrete "this is a table with these columns" claim (an entity that
appears only in a relationship line, like the query-time concepts
``investment_coverage`` / ``team_exchange_chord``, makes no such claim and is exempt).

chris (2026-08-28, on the diagram-accuracy ruling this test enforces): "The diagrams
should show these relationships in the graph; if they don't and we can't keep that
consistent we have an issue." A diagram that names a table or column that doesn't
exist is worse than no diagram -- it actively misleads. This test parses every
``erDiagram`` block in the page, extracts each declared entity's columns, and asserts
the entity is a real ClickHouse table (per the migration chain under
``src/dev_health_ops/migrations/clickhouse/``) and every declared column actually
exists on it -- unioning every ``CREATE TABLE`` and ``ALTER TABLE ... ADD COLUMN``
statement across the whole migration chain, .sql and .py alike (a table's live
schema is the union of every migration that has ever touched it, not just its
original ``CREATE TABLE``).

This intentionally parses the migration DDL directly rather than depending on
``full-chaos/dev-health-go``'s ``schema.go`` ``ProductionColumns`` (a sibling repo
this one has no checkout guarantee for in every environment this gate runs in) --
the migration chain is this repo's own source of truth for what ClickHouse actually
has, so the check is self-contained.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from dev_health_ops.migrations.clickhouse import (
    split_sql_statements,
    strip_line_comments,
)

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "contribute" / "architecture" / "team-attribution.md"
MIGRATIONS_DIR = ROOT / "src" / "dev_health_ops" / "migrations" / "clickhouse"

_MERMAID_FENCE_RE = re.compile(r"```mermaid\n(.*?)```", re.DOTALL)
# An entity's column block always opens with the entity name alone on its own
# line, followed by `{` with nothing else on that line -- e.g. `work_items {`.
# This deliberately does NOT use a blanket `(\w+)\s*\{...\}` regex: mermaid
# erDiagram relationship-cardinality tokens like `}o..o{` (a non-identifying
# many-to-many connector) also match `\w+\s*\{` in miniature (the `o` right
# before `{`), so a blanket regex misparses relationship lines as bogus
# one-letter entities. Requiring the opening line to be *exactly*
# `<name> {` and the closing line to be exactly `}` mirrors mermaid's own
# grammar for a column block and can't match inside a relationship line.
_ENTITY_OPEN_RE = re.compile(r"^(\w+)\s*\{\s*$")

_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:default\.)?`?(\w+)`?\s*\(",
    re.IGNORECASE,
)
_ALTER_TABLE_RE = re.compile(
    r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:default\.)?`?(\w+)`?",
    re.IGNORECASE,
)
_ADD_COLUMN_RE = re.compile(
    r"ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?",
    re.IGNORECASE,
)
_DROP_COLUMN_RE = re.compile(
    r"DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?`?(\w+)`?",
    re.IGNORECASE,
)
# The RMT-rebuild pattern this codebase uses everywhere a sorting key changes
# (migration 044's comment names it explicitly: CREATE `<t>_new` -> INSERT ...
# SELECT -> EXCHANGE TABLES <t> AND `<t>_new` -> DROP TABLE `<t>_new`) leaves a
# shadow `<t>_new`/`<t>_shadow` table CREATEd and then swapped away. Without
# handling DROP/EXCHANGE, that shadow table stays in the parsed schema forever
# as a phantom "real" table (caught in review: a stale ER block naming the
# dropped shadow table would otherwise pass both checks below).
_DROP_TABLE_RE = re.compile(
    r"^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:default\.)?`?(\w+)`?",
    re.IGNORECASE,
)
_RENAME_TABLE_PAIR_RE = re.compile(r"`?(\w+)`?\s+TO\s+`?(\w+)`?", re.IGNORECASE)
_EXCHANGE_TABLES_RE = re.compile(
    r"^EXCHANGE\s+TABLES\s+(?:default\.)?`?(\w+)`?\s+AND\s+(?:default\.)?`?(\w+)`?",
    re.IGNORECASE,
)

# Entities in the diagram that are query-time concepts, never a real ClickHouse
# table -- exempt because they never declare a `{ ... }` column block (asserted
# below), not hand-waved away.
_NO_COLUMN_BLOCK_ENTITIES = {"investment_coverage", "team_exchange_chord"}


def _find_matching_paren(text: str, open_idx: int) -> int:
    """Return the index of the ``)`` matching the ``(`` at ``open_idx``.

    Tracks single-quoted string literals (ClickHouse ``Enum8('a' = 1, ...)``
    values) so a stray paren-like character inside a string can't desync depth
    counting -- none of our DDL has one, but the guard is cheap and correct.
    """
    depth = 0
    in_single = False
    i = open_idx
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "'":
            in_single = not in_single
        elif not in_single:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    raise ValueError(f"unbalanced parentheses starting at index {open_idx}")


def _split_top_level_commas(body: str) -> list[str]:
    """Split a ``CREATE TABLE`` column-list body on top-level commas only."""
    parts: list[str] = []
    depth = 0
    in_single = False
    current: list[str] = []
    for ch in body:
        if ch == "'":
            in_single = not in_single
            current.append(ch)
            continue
        if not in_single:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append("".join(current))
                current = []
                continue
        current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _apply_statement(schema: dict[str, set[str]], statement: str) -> None:
    """Fold one SQL statement's effect into schema (mutated in place).

    Handles CREATE / ALTER ADD|DROP COLUMN, and the three table-identity
    operations the RMT-rebuild pattern uses (DROP TABLE, RENAME TABLE,
    EXCHANGE TABLES) so a shadow/`_new` table created mid-migration and later
    dropped or swapped away doesn't linger in the parsed schema as a phantom
    "real" table once the migration chain finishes.
    """
    drop_table_match = _DROP_TABLE_RE.match(statement)
    if drop_table_match:
        schema.pop(drop_table_match.group(1), None)
        return

    if re.match(r"^RENAME\s+TABLE\b", statement, re.IGNORECASE):
        for old_name, new_name in _RENAME_TABLE_PAIR_RE.findall(statement):
            schema[new_name] = schema.pop(old_name, set())
        return

    exchange_match = _EXCHANGE_TABLES_RE.match(statement)
    if exchange_match:
        left, right = exchange_match.group(1), exchange_match.group(2)
        left_cols = schema.get(left, set())
        right_cols = schema.get(right, set())
        schema[left] = right_cols
        schema[right] = left_cols
        return

    create_match = _CREATE_TABLE_RE.search(statement)
    if create_match:
        table = create_match.group(1)
        open_idx = create_match.end() - 1
        try:
            close_idx = _find_matching_paren(statement, open_idx)
        except ValueError:
            return
        body = statement[open_idx + 1 : close_idx]
        columns = schema.setdefault(table, set())
        for piece in _split_top_level_commas(body):
            tokens = piece.strip().split()
            if not tokens:
                continue
            col = tokens[0].strip("`")
            if col.upper() in {"PRIMARY", "INDEX", "CONSTRAINT", "KEY"}:
                continue
            columns.add(col)
        return

    alter_match = _ALTER_TABLE_RE.search(statement)
    if not alter_match:
        return
    table = alter_match.group(1)
    added = _ADD_COLUMN_RE.findall(statement)
    if added:
        columns = schema.setdefault(table, set())
        for col in added:
            columns.add(col)
    dropped = _DROP_COLUMN_RE.findall(statement)
    if dropped and table in schema:
        for col in dropped:
            schema[table].discard(col)


def _sql_strings_from_python_migration(path: Path) -> list[str]:
    """Every string constant in a .py migration (client.command/.query args and more).

    ``ast`` parses adjacent string-literal concatenation (``"a " "b"``) into a
    single ``Constant`` already, so this needs no manual joining. Non-SQL string
    constants are harmless: they never match the CREATE/ALTER regexes above.
    """
    try:
        tree = ast.parse(path.read_text(), filename=str(path))
    except SyntaxError:
        return []
    strings: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            strings.append(node.value)
    return strings


def _clickhouse_schema_from_migrations() -> dict[str, set[str]]:
    """Table name -> set of real column names, unioned across the migration chain."""
    schema: dict[str, set[str]] = {}
    paths = sorted(MIGRATIONS_DIR.glob("*.sql")) + sorted(MIGRATIONS_DIR.glob("*.py"))
    for path in sorted(paths):
        if path.name == "__init__.py":
            continue
        if path.suffix == ".sql":
            raw_chunks = [path.read_text()]
        else:
            raw_chunks = _sql_strings_from_python_migration(path)
        for raw in raw_chunks:
            cleaned = strip_line_comments(raw)
            for statement in split_sql_statements(cleaned):
                _apply_statement(schema, statement)
    return schema


def _entity_blocks_from_doc() -> dict[str, set[str]]:
    """entity_name -> set of declared column names, across every erDiagram block."""
    text = DOC.read_text()
    entities: dict[str, set[str]] = {}
    for fence in _MERMAID_FENCE_RE.findall(text):
        if "erDiagram" not in fence:
            continue
        lines = fence.splitlines()
        i = 0
        while i < len(lines):
            open_match = _ENTITY_OPEN_RE.match(lines[i].strip())
            if not open_match:
                i += 1
                continue
            name = open_match.group(1)
            columns = entities.setdefault(name, set())
            i += 1
            while i < len(lines) and lines[i].strip() != "}":
                tokens = lines[i].strip().split()
                if len(tokens) >= 2:
                    # mermaid attribute line: `<type> <name> [PK|FK|UK] ["comment"]`
                    columns.add(tokens[1])
                i += 1
            # i now sits on the closing `}` (or ran off the end -- malformed
            # fence, left for the "at least one entity" sanity test to catch
            # via a downstream assertion rather than silently swallowed here).
            i += 1
    return entities


def test_doc_has_at_least_one_er_diagram_with_entity_blocks():
    entities = _entity_blocks_from_doc()
    assert entities, (
        f"no mermaid erDiagram entity ({{...}}) blocks found in {DOC} -- "
        "the parser or the page's diagram markup drifted"
    )
    # Sanity: a handful of entities this page has always declared must still be there,
    # so a parser regression (matching nothing) can't masquerade as "nothing to check".
    for expected in ("work_items", "work_item_team_attributions", "teams"):
        assert expected in entities, (
            f"expected entity {expected!r} missing from parsed erDiagram"
        )


def test_no_column_block_entities_are_actually_columnless():
    """The `_NO_COLUMN_BLOCK_ENTITIES` exemption is valid only while it's true.

    The two tests below skip these entities *by name* rather than by property,
    for a clear failure message. That's only safe as long as this test also
    holds: if a future edit ever gives `investment_coverage` or
    `team_exchange_chord` a real `{ ... }` column block (making a "this is a
    table with these columns" claim after all), this test must fail loudly
    instead of the two column checks silently skipping it forever.
    """
    entities = _entity_blocks_from_doc()
    violations = {
        name: entities[name] for name in _NO_COLUMN_BLOCK_ENTITIES if entities.get(name)
    }
    assert not violations, (
        "these entities are exempt from the real-table/real-column checks on the "
        "assumption they are relationship-only (query-time concepts, not ClickHouse "
        f"tables), but the doc now declares columns for them: {violations}\n"
        "Either point them at a real ClickHouse table (and drop the exemption), or "
        "remove the erroneous column block."
    )


def test_every_diagram_entity_with_columns_is_a_real_clickhouse_table():
    entities = _entity_blocks_from_doc()
    real_schema = _clickhouse_schema_from_migrations()
    assert real_schema, (
        f"no ClickHouse tables parsed out of {MIGRATIONS_DIR} -- parser regression"
    )

    missing_tables = sorted(
        name
        for name, columns in entities.items()
        if columns and name not in real_schema and name not in _NO_COLUMN_BLOCK_ENTITIES
    )
    assert not missing_tables, (
        "team-attribution.md §3 erDiagram declares columns for entities that are not "
        f"real ClickHouse tables in {MIGRATIONS_DIR}: {missing_tables}\n"
        "Either the table was renamed/dropped and the diagram is stale, or the entity "
        "name in the diagram has a typo."
    )


def test_every_diagram_entity_column_exists_on_the_real_table():
    entities = _entity_blocks_from_doc()
    real_schema = _clickhouse_schema_from_migrations()

    offenders: list[str] = []
    for name, columns in entities.items():
        if not columns or name in _NO_COLUMN_BLOCK_ENTITIES:
            continue
        real_columns = real_schema.get(name)
        if real_columns is None:
            # Covered (and failed) by test_every_diagram_entity_with_columns_is_a_real_clickhouse_table;
            # don't double-report here.
            continue
        for col in sorted(columns - real_columns):
            offenders.append(f"{name}.{col}")

    assert not offenders, (
        "team-attribution.md §3 erDiagram declares columns that don't exist on the real "
        f"ClickHouse table (per {MIGRATIONS_DIR}): {offenders}\n"
        "Either the column was renamed/dropped and the diagram is stale, or it's a typo."
    )
