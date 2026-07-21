"""ClickHouse migration helpers shared by the migration runners.

``split_sql_statements`` is the single safe statement splitter used by both the
sink runner (``metrics/sinks/clickhouse/core.py``) and the storage runner
(``storage/clickhouse.py``). It strips SQL line comments (``-- ...``) BEFORE
splitting on ``;`` so a stray ``;`` inside a comment can never orphan bare text
into its own statement and break a migration with a ClickHouse SYNTAX_ERROR
(CHAOS-2430 hardening — that class of footgun previously broke 046).
"""

from __future__ import annotations

from collections.abc import Iterable

__all__ = [
    "strip_line_comments",
    "split_sql_statements",
    "all_migrations_applied",
    "MigrationDeferred",
]


class MigrationDeferred(RuntimeError):
    def __init__(self, migration: str, reason: str) -> None:
        self.migration = migration
        self.reason = reason
        super().__init__(migration, reason)

    def __str__(self) -> str:
        return f"migration {self.migration} deferred: {self.reason}"


def all_migrations_applied(
    migration_filenames: Iterable[str],
    applied_versions: Iterable[str],
) -> bool:
    """Return True iff every on-disk migration is already recorded as applied.

    This is the single source of truth for the migration runners' fast-path
    short-circuit (CHAOS-2440). It must be a FULL-SET completeness check, not a
    "latest filename applied" check: this repo has inserted / mixed-ordering
    migrations (e.g. ``023b_dora_metrics.sql`` sorts between ``023_*`` and
    ``024_*``, and duplicate numeric prefixes like ``002_phase2_metrics.sql`` /
    ``002_teams.sql`` exist). A database can hold the row for the
    lexicographically-latest migration while still missing an *intermediate*
    one — a latest-only check would falsely report "current" and silently skip
    the missing migration, causing schema drift.

    Comparing the full set of on-disk filenames against the applied set is still
    O(n) in memory over the single ``SELECT version FROM schema_migrations`` that
    the runner already issues — no per-file DB query, no logging loop — so the
    fast-path stays both fast and quiet while being correct.
    """
    return set(migration_filenames) <= set(applied_versions)


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
