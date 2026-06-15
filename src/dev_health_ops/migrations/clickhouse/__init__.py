"""ClickHouse migration helpers shared by the migration runners.

``split_sql_statements`` is the single safe statement splitter used by both the
sink runner (``metrics/sinks/clickhouse/core.py``) and the storage runner
(``storage/clickhouse.py``). It strips SQL line comments (``-- ...``) BEFORE
splitting on ``;`` so a stray ``;`` inside a comment can never orphan bare text
into its own statement and break a migration with a ClickHouse SYNTAX_ERROR
(CHAOS-2430 hardening — that class of footgun previously broke 046).
"""

from __future__ import annotations

__all__ = ["strip_line_comments", "split_sql_statements"]


def strip_line_comments(sql: str) -> str:
    """Remove ``-- ...`` line comments, ignoring ``--`` inside string literals.

    Only ``--`` to end-of-line is treated as a comment (standard SQL line
    comment). A ``--`` that appears inside a single- or double-quoted string is
    preserved. Block comments (``/* */``) are not used in our migrations and are
    left untouched.
    """
    out: list[str] = []
    for line in sql.splitlines():
        in_single = False
        in_double = False
        i = 0
        n = len(line)
        cut = n
        while i < n:
            ch = line[i]
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif (
                ch == "-"
                and i + 1 < n
                and line[i + 1] == "-"
                and not in_single
                and not in_double
            ):
                cut = i
                break
            i += 1
        out.append(line[:cut])
    return "\n".join(out)


def split_sql_statements(sql: str) -> list[str]:
    """Split a migration .sql blob into individual statements.

    Line comments are stripped first (so a ``;`` inside a comment is harmless),
    then the remainder is split on ``;`` and stripped; empty fragments are
    dropped. Returns the list of executable statements in order. Comment-only or
    whitespace-only input yields an empty list (a no-op migration).
    """
    cleaned = strip_line_comments(sql)
    statements: list[str] = []
    for fragment in cleaned.split(";"):
        stmt = fragment.strip()
        if stmt:
            statements.append(stmt)
    return statements
