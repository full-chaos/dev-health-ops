"""Regression tests for the ClickHouse migration statement splitter (CHAOS-2430).

A stray ``;`` inside a ``-- ...`` comment in migration 046 previously split the
DDL: the comment text after the ``;`` became its own "statement" and ClickHouse
raised SYNTAX_ERROR in the live-e2e migration runner. ``split_sql_statements``
strips line comments before splitting, closing that class of footgun. We also
guard every committed migration so no future comment line carries a ``;``.
"""

from __future__ import annotations

from pathlib import Path

from dev_health_ops.migrations.clickhouse import (
    split_sql_statements,
    strip_line_comments,
)

_MIGRATIONS_DIR = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dev_health_ops"
    / "migrations"
    / "clickhouse"
)


def test_semicolon_inside_comment_does_not_split_statement():
    """A comment containing ';' must NOT break the statement into two."""
    sql = """
    -- This table is great; it does many things; really.
    CREATE TABLE IF NOT EXISTS t (
        a String,
        b String
    ) ENGINE = MergeTree() ORDER BY a;
    """
    statements = split_sql_statements(sql)
    assert len(statements) == 1
    assert statements[0].startswith("CREATE TABLE IF NOT EXISTS t")
    # No orphaned comment fragment leaked into a statement.
    assert "really" not in statements[0]


def test_comment_only_input_is_noop():
    """Comment-only / whitespace-only input yields zero statements."""
    assert split_sql_statements("-- just a comment; with a semicolon\n") == []
    assert split_sql_statements("   \n\n  ") == []
    assert split_sql_statements("") == []


def test_multiple_statements_still_split():
    sql = """
    -- first; comment
    CREATE TABLE a (x String) ENGINE = MergeTree() ORDER BY x;
    -- second; comment
    CREATE TABLE b (y String) ENGINE = MergeTree() ORDER BY y;
    """
    statements = split_sql_statements(sql)
    assert len(statements) == 2
    assert statements[0].startswith("CREATE TABLE a")
    assert statements[1].startswith("CREATE TABLE b")


def test_double_dash_inside_string_literal_preserved():
    """A '--' inside a quoted string is not treated as a comment."""
    sql = "INSERT INTO t (s) VALUES ('a--b');"
    statements = split_sql_statements(sql)
    assert len(statements) == 1
    assert "'a--b'" in statements[0]


def test_trailing_inline_comment_stripped_but_statement_kept():
    sql = "CREATE TABLE t (a String) ENGINE = MergeTree() ORDER BY a;  -- note; here"
    statements = split_sql_statements(sql)
    assert len(statements) == 1
    assert statements[0].startswith("CREATE TABLE t")
    assert "note" not in statements[0]


def test_strip_line_comments_keeps_code_drops_comment():
    assert strip_line_comments("SELECT 1 -- x; y").strip() == "SELECT 1"
    assert strip_line_comments("-- whole; line").strip() == ""


def test_no_committed_migration_comment_line_contains_semicolon():
    """Defense-in-depth: even though the splitter now tolerates ';' in comments,
    keep comment lines free of ';' so migrations stay readable and robust to any
    other tool that splits naively. Fails with a clear, actionable message."""
    offenders: list[str] = []
    for path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
        for lineno, raw in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            stripped = raw.strip()
            if stripped.startswith("--") and ";" in stripped:
                offenders.append(f"{path.name}:{lineno}: {stripped}")
    assert not offenders, (
        "Migration comment lines must not contain ';' (a stray ';' in a comment "
        "has broken naive SQL splitters). Offending lines:\n" + "\n".join(offenders)
    )


def test_all_committed_migrations_split_without_error():
    """Every committed .sql migration splits into >= 1 statement and produces no
    comment-only fragments."""
    for path in sorted(_MIGRATIONS_DIR.glob("*.sql")):
        statements = split_sql_statements(path.read_text(encoding="utf-8"))
        assert statements, f"{path.name} produced no statements"
        for stmt in statements:
            # Every emitted statement has at least one non-comment code line.
            code_lines = [
                line
                for line in stmt.splitlines()
                if line.strip() and not line.strip().startswith("--")
            ]
            assert code_lines, f"{path.name} emitted a comment-only statement: {stmt!r}"
